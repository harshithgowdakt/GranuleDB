package processor

import (
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/storage"
)

// SourceProcessor reads data from a single storage Part.
// One per part enables parallel reads across parts.
type SourceProcessor struct {
	BaseProcessor

	table     *storage.MergeTreeTable
	part      *storage.Part
	columns   []string
	keyRanges []engine.KeyRange

	chunk    *Chunk // produced chunk waiting to be pushed
	read     bool   // true after Work() has been called
	pushed   bool   // true after chunk has been pushed
	finished bool
}

// NewSourceProcessor creates a source for one part.
func NewSourceProcessor(
	table *storage.MergeTreeTable,
	part *storage.Part,
	columns []string,
	keyRanges []engine.KeyRange,
) *SourceProcessor {
	return &SourceProcessor{
		BaseProcessor: NewBaseProcessor("Source", 0, 1),
		table:         table,
		part:          part,
		columns:       columns,
		keyRanges:     keyRanges,
	}
}

func (s *SourceProcessor) Prepare() Status {
	if s.finished {
		return StatusFinished
	}

	out := s.Output(0)

	if out.IsFinished() {
		s.finished = true
		return StatusFinished
	}

	// If we already pushed, wait for downstream to consume then finish.
	if s.pushed {
		if out.CanPush() {
			s.finished = true
			out.SetFinished()
			return StatusFinished
		}
		return StatusPortFull
	}

	// If we have a chunk ready to push.
	if s.chunk != nil {
		if out.CanPush() {
			out.Push(s.chunk)
			s.chunk = nil
			s.pushed = true
			// Don't SetFinished yet — wait for downstream to consume.
			return StatusPortFull
		}
		return StatusPortFull
	}

	// If Work() ran but produced no chunk (pruned/empty part).
	if s.read {
		s.finished = true
		out.SetFinished()
		return StatusFinished
	}

	// Need to read the part.
	if !out.CanPush() {
		return StatusPortFull
	}

	return StatusReady
}

func (s *SourceProcessor) Work() {
	s.read = true

	reader := storage.NewPartReader(s.part, &s.table.Schema)

	granuleBegin := 0
	granuleEnd := s.part.NumGranules

	if len(s.keyRanges) > 0 && granuleEnd > 0 {
		idx, err := reader.LoadPrimaryIndex()
		if err == nil {
			for _, kr := range s.keyRanges {
				gb, ge := idx.FindGranuleRange(kr.KeyColumn, kr.MinVal, kr.MaxVal)
				if gb > granuleBegin {
					granuleBegin = gb
				}
				if ge < granuleEnd {
					granuleEnd = ge
				}
			}
		}
	}

	if granuleBegin >= granuleEnd {
		return
	}

	block, err := reader.ReadColumns(s.columns, granuleBegin, granuleEnd)
	if err != nil || block.NumRows() == 0 {
		return
	}

	s.chunk = NewChunk(block)
}
