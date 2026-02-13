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
		idleSpins := 0
		for {
			select {
			case <-done:
				return
			case procIdx, ok := <-queue:
				if !ok {
					return
				}
				ex.processOne(graph, procIdx, queue, &finishedCount, finishedFlags, n, done, &closeOnce)
				idleSpins = 0
			default:
				// Queue empty — sweep all processors to find unfinished work.
				// This prevents lost scheduling from non-blocking enqueue.
				idleSpins++
				if idleSpins > 2 {
					swept := false
					for i := 0; i < n; i++ {
						if !finishedFlags[i].Load() {
							select {
							case queue <- i:
								swept = true
							default:
							}
						}
					}
					if swept {
						idleSpins = 0
					} else {
						runtime.Gosched()
					}
				} else {
					runtime.Gosched()
				}
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
	// Skip already-finished processors immediately.
	if finishedFlags[procIdx].Load() {
		return
	}

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
		// Only enqueue downstream and count on the first finish.
		// Re-entries (from stale queue items) are no-ops to prevent
		// queue saturation in pipelines with multi-input processors.
		if finishedFlags[procIdx].CompareAndSwap(false, true) {
			for _, down := range graph.Downstream(procIdx) {
				enqueueIfOpen(queue, done, down)
			}
			if int(finishedCount.Add(1)) >= totalProcs {
				closeOnce.Do(func() { close(done) })
			}
		}
	}
}

func enqueueIfOpen(queue chan int, done chan struct{}, procIdx int) {
	select {
	case <-done:
		return
	case queue <- procIdx:
		return
	default:
		// Queue is full — skip. The processor will be rescheduled by
		// the idle-sweep mechanism or another worker processing a neighbor.
	}
}
