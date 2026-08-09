package worker

import (
	"log"
	"sync"
	"sync/atomic"
)

type Task struct {
	Action func()
	Done   chan struct{}
}

type Worker struct {
	id           int64
	taskCh       chan *Task
	stopCh       chan struct{}
	stopOnce     sync.Once
	poolDone     <-chan struct{}
	queueLength  *atomic.Int64
	busyWorkers  *atomic.Int64
	workersCount *atomic.Int64
	wg           *sync.WaitGroup
	notify       chan<- struct{}
}

type Runner interface {
	Start()
	Stop()
}

func NewWorker(
	id int64,
	taskCh chan *Task,
	poolDone <-chan struct{},
	queueLength, busyWorkers, workersCount *atomic.Int64,
	wg *sync.WaitGroup,
	notify chan<- struct{},
) *Worker {
	return &Worker{
		id:           id,
		stopCh:       make(chan struct{}),
		taskCh:       taskCh,
		poolDone:     poolDone,
		queueLength:  queueLength,
		busyWorkers:  busyWorkers,
		workersCount: workersCount,
		wg:           wg,
		notify:       notify,
	}
}

func (w *Worker) Start() {
	w.workersCount.Add(1)
	w.wg.Add(1)
	go w.loop()
}

func (w *Worker) loop() {
	defer w.wg.Done()
	defer w.workersCount.Add(-1)
	defer w.signal()

	for {
		select {
		case <-w.stopCh:
			return
		case <-w.poolDone:
			return
		case task := <-w.taskCh:
			if task == nil {
				continue
			}
			w.queueLength.Add(-1)
			w.execute(task)
			w.signal()
		}
	}
}

func (w *Worker) execute(task *Task) {
	w.busyWorkers.Add(1)
	defer w.busyWorkers.Add(-1)

	defer func() {
		if task.Done != nil {
			close(task.Done)
		}
		if r := recover(); r != nil {
			log.Printf("worker %d recovered from panic: %v", w.id, r)
		}
	}()

	task.Action()
}

func (w *Worker) signal() {
	if w.notify == nil {
		return
	}
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
}
