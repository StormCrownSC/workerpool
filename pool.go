package pool

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/junxed/workerpool/worker"
)

var (
	ErrPoolClosed    = errors.New("worker pool is closed")
	ErrAlreadyClosed = errors.New("worker pool already is closed")
)

type Pool struct {
	workers      []*worker.Worker
	nextID       atomic.Int64
	scaleMutex   sync.Mutex
	taskQueue    chan *worker.Task
	workersCount atomic.Int64
	queueLength  atomic.Int64
	busyWorkers  atomic.Int64
	isClosed     atomic.Bool
	done         chan struct{}
	closeOnce    sync.Once
	workerWg     sync.WaitGroup
	monitorWg    sync.WaitGroup
	notify       chan struct{}
	config       Config
}

type Config struct {
	WorkersCount int64
	QueueSize    int64
	StopTimeout  time.Duration

	ScalingConfig ScalingConfig
}

type ScalingConfig struct {
	Enable                      bool
	MinWorkers                  int64
	MaxWorkers                  int64
	Type                        ScalingType
	MaxLoadPercent              float64
	MinLoadPercent              float64
	WorkersToAddRatioPercent    float64
	WorkersToRemoveRatioPercent float64
	Interval                    time.Duration

	Logging Logging
}

type Logging struct {
	Enable   bool
	Interval time.Duration
}

type Pooler interface {
	Submit(act func()) error
	SubmitContext(ctx context.Context, act func()) error
	SubmitWait(act func()) error
	SubmitWaitContext(ctx context.Context, act func()) error
	Stop() error
	Wait()
}

func NewPool(params Config) (*Pool, error) {
	var err error
	if params, err = checkParams(params); err != nil {
		return nil, err
	}

	p := &Pool{
		taskQueue: make(chan *worker.Task, params.QueueSize),
		config:    params,
		done:      make(chan struct{}),
		notify:    make(chan struct{}, 1),
	}

	p.scaleMutex.Lock()
	p.addWorker(params.WorkersCount)
	p.scaleMutex.Unlock()

	if params.ScalingConfig.Enable || params.ScalingConfig.Logging.Enable {
		p.monitorWg.Add(1)
		go p.monitorWorkers()
	}

	return p, nil
}

func (p *Pool) Submit(act func()) error {
	return p.SubmitContext(context.Background(), act)
}

func (p *Pool) SubmitContext(ctx context.Context, act func()) error {
	if p.isClosed.Load() {
		return ErrPoolClosed
	}

	p.queueLength.Add(1)
	task := &worker.Task{Action: act}
	select {
	case p.taskQueue <- task:
		return nil
	case <-ctx.Done():
		p.queueLength.Add(-1)
		return ctx.Err()
	case <-p.done:
		p.queueLength.Add(-1)
		return ErrPoolClosed
	}
}

func (p *Pool) SubmitWait(act func()) error {
	return p.SubmitWaitContext(context.Background(), act)
}

func (p *Pool) SubmitWaitContext(ctx context.Context, act func()) error {
	if p.isClosed.Load() {
		return ErrPoolClosed
	}

	taskDone := make(chan struct{})
	p.queueLength.Add(1)
	task := &worker.Task{Action: act, Done: taskDone}

	select {
	case p.taskQueue <- task:
	case <-ctx.Done():
		p.queueLength.Add(-1)
		return ctx.Err()
	case <-p.done:
		p.queueLength.Add(-1)
		return ErrPoolClosed
	}

	select {
	case <-taskDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrPoolClosed
	}
}

func (p *Pool) Wait() {
	for {
		if p.queueLength.Load() == 0 && p.busyWorkers.Load() == 0 {
			return
		}
		select {
		case <-p.notify:
		case <-p.done:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (p *Pool) Stop() error {
	if !p.isClosed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed
	}

	deadline := time.Now().Add(p.config.StopTimeout)
	for time.Now().Before(deadline) {
		if p.queueLength.Load() == 0 && p.busyWorkers.Load() == 0 {
			break
		}
		select {
		case <-p.notify:
		case <-time.After(20 * time.Millisecond):
		}
	}

	p.closeOnce.Do(func() {
		close(p.done)
	})

	p.scaleMutex.Lock()
	for _, w := range p.workers {
		w.Stop()
	}
	p.workers = nil
	p.scaleMutex.Unlock()

	p.workerWg.Wait()
	p.monitorWg.Wait()

	return nil
}

func (p *Pool) monitorWorkers() {
	defer p.monitorWg.Done()

	var scaleCh <-chan time.Time
	if p.config.ScalingConfig.Enable {
		t := time.NewTicker(p.config.ScalingConfig.Interval)
		defer t.Stop()
		scaleCh = t.C
	}

	var logCh <-chan time.Time
	if p.config.ScalingConfig.Logging.Enable {
		t := time.NewTicker(p.config.ScalingConfig.Logging.Interval)
		defer t.Stop()
		logCh = t.C
	}

	for {
		select {
		case <-p.done:
			return
		case <-scaleCh:
			p.checkScaling()
		case <-logCh:
			p.logging()
		}
	}
}

func (p *Pool) logging() {
	wTotal := p.workersCount.Load()
	wBusy := p.busyWorkers.Load()
	qLen := p.queueLength.Load()
	qCap := p.config.QueueSize

	safeDiv := func(a, b int64) float64 {
		if b == 0 {
			return 0
		}
		return float64(a) / float64(b) * percent
	}

	log.Printf("pool: workers=%d/%d(%.1f%%) queue=%d/%d(%.1f%%)",
		wBusy, wTotal, safeDiv(wBusy, wTotal),
		qLen, qCap, safeDiv(qLen, qCap),
	)
}

func (p *Pool) checkScaling() {
	p.scaleMutex.Lock()
	defer p.scaleMutex.Unlock()

	cw := p.workersCount.Load()
	if cw == 0 {
		return
	}

	cfg := p.config.ScalingConfig
	var load float64
	switch cfg.Type {
	case ScalingTypeWorker:
		load = float64(p.busyWorkers.Load()) / float64(cw)
	default:
		load = float64(p.queueLength.Load()) / float64(p.config.QueueSize)
	}

	if load > cfg.MaxLoadPercent && cw < cfg.MaxWorkers {
		p.scaleUp(cw)
		return
	}
	if load < cfg.MinLoadPercent && cw > cfg.MinWorkers {
		p.scaleDown(cw)
	}
}

func (p *Pool) scaleUp(currentWorkers int64) {
	cfg := p.config.ScalingConfig
	workersToAdd := max(int64(float64(currentWorkers)*cfg.WorkersToAddRatioPercent), 1)
	if currentWorkers+workersToAdd > cfg.MaxWorkers {
		workersToAdd = cfg.MaxWorkers - currentWorkers
	}
	p.addWorker(workersToAdd)
}

func (p *Pool) scaleDown(currentWorkers int64) {
	cfg := p.config.ScalingConfig
	workersToRemove := max(int64(float64(currentWorkers)*cfg.WorkersToRemoveRatioPercent), 1)
	if currentWorkers-workersToRemove < cfg.MinWorkers {
		workersToRemove = currentWorkers - cfg.MinWorkers
	}
	p.removeWorker(workersToRemove)
}

func (p *Pool) addWorker(count int64) {
	if p.isClosed.Load() {
		return
	}
	for range count {
		id := p.nextID.Add(1)
		w := worker.NewWorker(
			id,
			p.taskQueue,
			p.done,
			&p.queueLength,
			&p.busyWorkers,
			&p.workersCount,
			&p.workerWg,
			p.notify,
		)
		p.workers = append(p.workers, w)
		w.Start()
	}
}

func (p *Pool) removeWorker(count int64) {
	if count <= 0 {
		return
	}
	if count > int64(len(p.workers)) {
		count = int64(len(p.workers))
	}
	for i := range count {
		p.workers[i].Stop()
	}
	p.workers = p.workers[count:]
}
