package pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/junxed/workerpool/worker"
)

type pool struct {
	workers      []*worker.Worker
	scaleMutex   sync.Mutex
	taskQueue    chan *worker.Task
	workersCount *atomic.Int64
	config       Config
	queueLength  *atomic.Int64
	busyWorkers  *atomic.Int64
	isClosed     atomic.Bool
	closeCh      chan struct{}
}

type Config struct {
	WorkersCount int64
	QueueSize    int64
	StopTimeout  time.Duration // default 5s

	ScalingConfig ScalingConfig
}

type ScalingConfig struct {
	Enable                      bool
	MinWorkers                  int64
	MaxWorkers                  int64
	Type                        ScalingType   // default Queue
	MaxLoadPercent              float64       // default 70%
	MinLoadPercent              float64       // default 30%
	WorkersToAddRatioPercent    float64       // default 20%
	WorkersToRemoveRatioPercent float64       // default 10%
	Interval                    time.Duration //default 1 second

	Logging Logging
}

type Logging struct {
	Enable   bool          // debug log
	Interval time.Duration // default 1 minute
}

type Pooler interface {
	Submit(act func()) error
	SubmitWait(act func()) error
	Stop() error
	Wait()
}

func NewPool(params Config) (*pool, error) {
	var err error
	if params, err = checkParams(params); err != nil {
		return nil, err
	}

	pool := &pool{
		taskQueue:    make(chan *worker.Task, params.QueueSize),
		scaleMutex:   sync.Mutex{},
		workersCount: &atomic.Int64{},
		config:       params,
		queueLength:  &atomic.Int64{},
		busyWorkers:  &atomic.Int64{},
		isClosed:     atomic.Bool{},
		closeCh:      make(chan struct{}),
	}

	pool.scaleMutex.Lock()
	pool.addWorker(params.WorkersCount)
	pool.scaleMutex.Unlock()
	if params.ScalingConfig.Logging.Enable {
		go pool.monitorWorkers()
	}

	return pool, nil
}

func (pool *pool) Submit(act func()) error {
	if pool.isClosed.Load() {
		return errors.New("worker pool is closed")
	}

	task := &worker.Task{
		Action: act,
	}

	pool.taskQueue <- task
	pool.queueLength.Add(1)

	return nil
}

func (pool *pool) SubmitWait(act func()) error {
	if pool.isClosed.Load() {
		return errors.New("worker pool is closed")
	}

	done := make(chan struct{})
	task := &worker.Task{
		Action: act,
		Done:   &done,
	}

	pool.queueLength.Add(1)
	pool.taskQueue <- task

	_, ok := <-done
	if !ok {
		return errors.New("worker pool is closed")
	}

	return nil
}

func (pool *pool) Wait() {
	for {
		if pool.queueLength.Load() == 0 && pool.workersCount.Load() == 0 {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (pool *pool) Stop() error {
	if !pool.isClosed.CompareAndSwap(false, true) {
		return errors.New("worker pool already is closed")
	}

	timeout := time.After(pool.config.StopTimeout)
	if pool.config.ScalingConfig.Logging.Enable {
		if pool.config.ScalingConfig.Logging.Enable {
			select {
			case pool.closeCh <- struct{}{}:
			default:
			}
		}
	}

	for {
		select {
		case <-timeout:
			pool.scaleMutex.Lock()
			pool.removeWorker(pool.workersCount.Load())
			pool.scaleMutex.Unlock()
			close(pool.taskQueue)
			return nil
		default:
			if pool.queueLength.Load() == 0 {
				goto stopWorkers
			}
			time.Sleep(100 * time.Microsecond)
		}
	}

stopWorkers:
	pool.scaleMutex.Lock()
	pool.removeWorker(pool.workersCount.Load())
	pool.scaleMutex.Unlock()

	for {
		select {
		case <-timeout:
			close(pool.taskQueue)
			return nil
		default:
			if pool.workersCount.Load() == 0 {
				close(pool.taskQueue)
				return nil
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func (pool *pool) monitorWorkers() {
	monitorTicker := time.NewTicker(pool.config.ScalingConfig.Interval)
	defer monitorTicker.Stop()

	var logTicker *time.Ticker
	var logChannel <-chan time.Time

	if pool.config.ScalingConfig.Logging.Enable {
		logTicker = time.NewTicker(pool.config.ScalingConfig.Logging.Interval)
		defer logTicker.Stop()
		logChannel = logTicker.C
	}

	for {
		select {
		case <-monitorTicker.C:
			if err := pool.checkScaling(); err != nil {
				log.Printf("Logging error: %v", err)
				return
			}

		case <-logChannel:
			pool.logging()

		case <-pool.closeCh:
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (p *pool) logging() {
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
func (p *pool) checkScaling() error {
	p.scaleMutex.Lock()
	defer p.scaleMutex.Unlock()

	currentWorkers := p.workersCount.Load()
	currentQueue := p.queueLength.Load()
	currentBusy := p.busyWorkers.Load()

	if currentWorkers == 0 {
		return nil
	}

	var currentLoad float64
	switch p.config.ScalingConfig.Type {
	case ScalingTypeQueue:
		currentLoad = float64(currentQueue) / float64(p.config.QueueSize)
	case ScalingTypeWorker:
		currentLoad = float64(currentBusy) / float64(currentWorkers)
	default:
		currentLoad = float64(currentQueue) / float64(p.config.QueueSize)
	}

	cfg := p.config.ScalingConfig

	if currentLoad > cfg.MaxLoadPercent && currentWorkers < cfg.MaxWorkers {
		p.scaleUp(currentWorkers)
		return nil
	}

	if currentLoad < cfg.MinLoadPercent && currentWorkers > cfg.MinWorkers {
		p.scaleDown(currentWorkers)
	}

	return nil
}
func (pool *pool) scaleUp(currentWorkers int64) {
	workersToAdd := int64(float64(currentWorkers) * pool.config.ScalingConfig.WorkersToAddRatioPercent)
	if workersToAdd < 1 {
		workersToAdd = 1
	}

	if currentWorkers+workersToAdd > pool.config.ScalingConfig.MaxWorkers {
		workersToAdd = pool.config.ScalingConfig.MaxWorkers - currentWorkers
	}

	pool.addWorker(workersToAdd)
}

func (pool *pool) scaleDown(currentWorkers int64) {
	workersToRemove := int64(float64(currentWorkers) * pool.config.ScalingConfig.WorkersToRemoveRatioPercent)
	if workersToRemove < 1 {
		workersToRemove = 1
	}

	if currentWorkers-workersToRemove < pool.config.ScalingConfig.MinWorkers {
		workersToRemove = currentWorkers - pool.config.ScalingConfig.MinWorkers
	}
	pool.removeWorker(workersToRemove)
}

func (pool *pool) addWorker(workersCount int64) {
	if pool.isClosed.Load() {
		return
	}

	var actualWorkers = pool.workersCount.Load()
	for index := actualWorkers; index < workersCount+actualWorkers; index++ {
		newWorker := worker.NewWorker(index, pool.taskQueue, pool.queueLength, pool.busyWorkers, pool.workersCount)
		pool.workers = append(pool.workers, newWorker)
		newWorker.Start()
	}
}

func (pool *pool) removeWorker(workersCount int64) {
	for i := range workersCount {
		pool.workers[i].Stop()
	}
	pool.workers = pool.workers[workersCount:]
}
