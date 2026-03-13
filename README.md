## workerpool

A highly configurable worker pool library for Go with optional dynamic scaling based on queue size or worker utilization. Supports synchronous and asynchronous task submission, logging, and graceful shutdown.

## Features
* Asynchronous (`Submit`) and synchronous (`SubmitWait`) task execution.
* Dynamic scaling of workers based on load.
* Queue and worker-based scaling strategies.
* Graceful shutdown with timeout support.
* Optional debug logging for monitoring pool and queue status.
* Panic recovery inside tasks.

## Installation
```text
go get github.com/junxed/workerpool
```
## Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/junxed/workerpool"
)

func main() {
	config := pool.Config{
		WorkersCount: 5,
		QueueSize:    20,
		ScalingConfig: pool.ScalingConfig{
			Enable:        true,
			MinWorkers:    2,
			MaxWorkers:    10,
			Type:          pool.ScalingTypeQueue,
			Logging:       pool.Logging{Enable: true, Interval: time.Second * 10},
			MaxLoadPercent: 70,
			MinLoadPercent: 30,
		},
	}

	p, err := pool.NewPool(config)
	if err != nil {
		panic(err)
	}

	// Asynchronous task submission
	for i := 0; i < 10; i++ {
		p.Submit(func() {
			fmt.Printf("Async Task %d executed\n", i)
			time.Sleep(time.Second)
		})
	}

	// Synchronous task submission
	for i := 0; i < 5; i++ {
		p.SubmitWait(func() {
			fmt.Printf("Sync Task %d executed\n", i)
			time.Sleep(time.Second)
		})
	}

	// Stop the pool gracefully
	if err := p.Stop(); err != nil {
		fmt.Println("Error stopping pool:", err)
	}
}
```

## Pool Configuration
**Field**	**Type**	**Description**
`WorkersCount`	int64	Initial number of workers in the pool.
`QueueSize`	int64	Maximum number of tasks that can be queued.
`StopTimeout`	time.Duration	Maximum time to wait when stopping the pool gracefully.
`ScalingConfig.Enable`	bool	Enable dynamic worker scaling.
`ScalingConfig.MinWorkers`	int64	Minimum number of workers when scaling.
`ScalingConfig.MaxWorkers`	int64	Maximum number of workers when scaling.
`ScalingConfig.Type`	ScalingType	Scaling strategy: ScalingTypeQueue or ScalingTypeWorker.
`ScalingConfig.Interval`	time.Duration	Interval between scaling checks.
`ScalingConfig.Logging`	Logging	Logging configuration for monitoring pool usage.

## Scaling Strategies
* Queue-based (ScalingTypeQueue): Scales workers according to the number of tasks in the queue.
* Worker-based (ScalingTypeWorker): Scales workers according to the ratio of busy workers to total workers.

## Logging

Enable logging in the scaling config:
```
Logging: pool.Logging{
    Enable:   true,
    Interval: time.Minute,
}
```

This will output stats such as:

```pool: workers=3/5(60.0%) queue=10/20(50.0%)```