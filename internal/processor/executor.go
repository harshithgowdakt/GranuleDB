package processor

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// PipelineExecutor drives the processor DAG to completion.
type PipelineExecutor struct {
	graph      *ExecutingGraph
	numWorkers int
	err        error
	errOnce    sync.Once
}

// NewPipelineExecutor creates an executor. numWorkers defaults to NumCPU if <= 0.
func NewPipelineExecutor(graph *ExecutingGraph, numWorkers int) *PipelineExecutor {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	return &PipelineExecutor{
		graph:      graph,
		numWorkers: numWorkers,
	}
}

// Execute runs the pipeline to completion.
func (ex *PipelineExecutor) Execute() error {
	graph := ex.graph
	n := len(graph.Processors)
	if n == 0 {
		return nil
	}

	// Work queue: buffered channel of processor indices to evaluate.
	queue := make(chan int, n*4)

	// Seed the queue with all processors.
	for i := 0; i < n; i++ {
		queue <- i
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	closeOnce := sync.Once{}

	var finishedCount atomic.Int32
	// Track which processors have been counted as finished to avoid double-counting.
	finishedFlags := make([]atomic.Bool, n)

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case procIdx, ok := <-queue:
				if !ok {
					return
				}
				ex.processOne(graph, procIdx, queue, &finishedCount, finishedFlags, n, done, &closeOnce)
			}
		}
	}

	wg.Add(ex.numWorkers)
	for i := 0; i < ex.numWorkers; i++ {
		go worker()
	}

	wg.Wait()

	return ex.err
}

func (ex *PipelineExecutor) processOne(
	graph *ExecutingGraph,
	procIdx int,
	queue chan int,
	finishedCount *atomic.Int32,
	finishedFlags []atomic.Bool,
	totalProcs int,
	done chan struct{},
	closeOnce *sync.Once,
) {
	// Try to claim. If another goroutine is working on it, skip.
	if !graph.TryClaim(procIdx) {
		return
	}
	defer graph.Release(procIdx)

	proc := graph.Processors[procIdx]
	status := proc.Prepare()

	switch status {
	case StatusReady:
		func() {
			defer func() {
				if r := recover(); r != nil {
					ex.errOnce.Do(func() {
						ex.err = fmt.Errorf("processor %s panicked: %v", proc.Name(), r)
					})
					closeOnce.Do(func() { close(done) })
				}
			}()
			proc.Work()
		}()

		// After work, re-evaluate self and neighbors.
		enqueueIfOpen(queue, done, procIdx)
		for _, up := range graph.Upstream(procIdx) {
			enqueueIfOpen(queue, done, up)
		}
		for _, down := range graph.Downstream(procIdx) {
			enqueueIfOpen(queue, done, down)
		}

	case StatusNeedData:
		for _, up := range graph.Upstream(procIdx) {
			enqueueIfOpen(queue, done, up)
		}

	case StatusPortFull:
		for _, down := range graph.Downstream(procIdx) {
			enqueueIfOpen(queue, done, down)
		}

	case StatusFinished:
		for _, down := range graph.Downstream(procIdx) {
			enqueueIfOpen(queue, done, down)
		}

		// Only count each processor's finish once.
		if finishedFlags[procIdx].CompareAndSwap(false, true) {
			if int(finishedCount.Add(1)) >= totalProcs {
				closeOnce.Do(func() { close(done) })
			}
		}
	}
}

func enqueueIfOpen(queue chan int, done chan struct{}, procIdx int) {
	for {
		select {
		case <-done:
			return
		case queue <- procIdx:
			return
		default:
			runtime.Gosched()
		}
	}
}
