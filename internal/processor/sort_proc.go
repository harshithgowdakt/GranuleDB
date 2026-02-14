package processor

import (
	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/parser"
)

// SortProcessor materializes all input, sorts, then emits one sorted chunk.
// Phase 0: accumulate, Phase 1: sort (Work), Phase 2: push, Phase 3: wait+finish.
type SortProcessor struct {
	BaseProcessor
	orderBy []parser.OrderByExpr

	accumulated *column.Block
	sorted      *Chunk
	pushed      bool
	phase       int
}

// NewSortProcessor creates a sort processor.
func NewSortProcessor(orderBy []parser.OrderByExpr) *SortProcessor {
	return &SortProcessor{
		BaseProcessor: NewBaseProcessor("Sort", 1, 1),
		orderBy:       orderBy,
	}
}

func (s *SortProcessor) Prepare() Status {
	switch s.phase {
	case 0: // Accumulate
		inp := s.Input(0)
		if inp.HasData() {
			chunk := inp.Pull()
			if s.accumulated == nil {
				s.accumulated = chunk.Block
			} else {
				s.accumulated.AppendBlock(chunk.Block)
			}
			return StatusNeedData
		}
		if inp.IsFinished() {
			if s.accumulated == nil || s.accumulated.NumRows() == 0 {
				s.phase = 4
				s.Output(0).SetFinished()
				return StatusFinished
			}
			s.phase = 1
			return StatusReady
		}
		return StatusNeedData

	case 1: // Sort (Work)
		return StatusReady

	case 2: // Push
		out := s.Output(0)
		if s.sorted != nil {
			if out.CanPush() {
				out.Push(s.sorted)
				s.sorted = nil
				s.pushed = true
				s.phase = 3
				return StatusPortFull // wait for downstream to consume
			}
			return StatusPortFull
		}
		s.phase = 3
		return StatusPortFull

	case 3: // Wait for consume then finish
		out := s.Output(0)
		if s.pushed && !out.CanPush() {
			return StatusPortFull
		}
		s.phase = 4
		out.SetFinished()
		return StatusFinished

	default:
		return StatusFinished
	}
}

func (s *SortProcessor) Work() {
	// Sort by ORDER BY columns with per-column direction.
	sortCols := make([]string, len(s.orderBy))
	desc := make([]bool, len(s.orderBy))
	for i, ob := range s.orderBy {
		sortCols[i] = ob.Column
		desc[i] = ob.Desc
	}

	s.accumulated.SortByColumnsWithDirection(sortCols, desc)

	s.sorted = NewChunk(s.accumulated)
	s.accumulated = nil
	s.phase = 2
}
