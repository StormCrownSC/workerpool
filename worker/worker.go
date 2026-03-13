package worker

import (
	"log"
	"sync/atomic"
)

type Task struct {
	Action func()
	Done   *chan struct{}
}

type Worker struct {
	id           int64
	taskCh       chan *Task
	closeCh      chan struct{}
	queueLength  *atomic.Int64
	busyWorkers  *atomic.Int64
	workersCount *atomic.Int64
}

type Runner interface {
	Start()
	Stop()
}

func NewWorker(id int64, taskCh chan *Task, queueLength, busyWorkers, workersCount *atomic.Int64) *Worker {
	return &Worker{
		id:           id,
		closeCh:      make(chan struct{}, 1),
		taskCh:       taskCh,
		queueLength:  queueLength,
		busyWorkers:  busyWorkers,
		workersCount: workersCount,
	}
}

func (worker *Worker) Start() {
	go func() {
		worker.workersCount.Add(1)
		defer worker.workersCount.Add(-1)
		for {
			select {
			case <-worker.closeCh:
				close(worker.closeCh)
				return
			default:
				select {
				case <-worker.closeCh:
					close(worker.closeCh)
					return
				case task := <-worker.taskCh:
					worker.queueLength.Add(-1)
					if task != nil {
						worker.execute(task)
					}
				}
			}
		}
	}()
}

func (worker *Worker) execute(task *Task) {
	worker.busyWorkers.Add(1)
	defer worker.busyWorkers.Add(-1)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("worker recovered from panic: %v", r)
		}
	}()

	task.Action()
	if task.Done != nil {
		*task.Done <- struct{}{}
	}
}

func (worker *Worker) Stop() {
	worker.closeCh <- struct{}{}
}
