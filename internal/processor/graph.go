package processor

import "sync/atomic"

// Edge represents a connection from one processor's output to another's input.
type Edge struct {
	OutputProcessor int
	OutputPortIdx   int
	InputProcessor  int
	InputPortIdx    int
}

// processorState tracks per-processor scheduling state.
type processorState struct {
	claimed atomic.Int32
}

// ExecutingGraph holds the compiled processor DAG.
type ExecutingGraph struct {
	Processors   []Processor
	Edges        []Edge
	states       []processorState
	upstreamOf   [][]int // for each proc, list of upstream proc indices
	downstreamOf [][]int // for each proc, list of downstream proc indices
}

// NewExecutingGraph builds the graph from processors and edges.
func NewExecutingGraph(procs []Processor, edges []Edge) *ExecutingGraph {
	n := len(procs)
	g := &ExecutingGraph{
		Processors:   procs,
		Edges:        edges,
		states:       make([]processorState, n),
		upstreamOf:   make([][]int, n),
		downstreamOf: make([][]int, n),
	}

	for _, e := range edges {
		g.downstreamOf[e.OutputProcessor] = appendUnique(
			g.downstreamOf[e.OutputProcessor], e.InputProcessor)
		g.upstreamOf[e.InputProcessor] = appendUnique(
			g.upstreamOf[e.InputProcessor], e.OutputProcessor)
	}

	return g
}

func appendUnique(s []int, v int) []int {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

// TryClaim attempts to claim a processor for execution using CAS.
func (g *ExecutingGraph) TryClaim(procIdx int) bool {
	return g.states[procIdx].claimed.CompareAndSwap(0, 1)
}

// Release releases a previously claimed processor.
func (g *ExecutingGraph) Release(procIdx int) {
	g.states[procIdx].claimed.Store(0)
}

// Upstream returns indices of processors directly upstream of procIdx.
func (g *ExecutingGraph) Upstream(procIdx int) []int {
	return g.upstreamOf[procIdx]
}

// Downstream returns indices of processors directly downstream of procIdx.
func (g *ExecutingGraph) Downstream(procIdx int) []int {
	return g.downstreamOf[procIdx]
}
