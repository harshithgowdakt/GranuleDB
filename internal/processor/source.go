package processor

import (
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// SourceProcessor reads data from a single storage Part using morsel-driven
// streaming. Each Work() call reads one granule and pushes a small chunk,
// enabling pipeline parallelism with the work-stealing executor.
type SourceProcessor struct {
	BaseProcessor

	table        *storage.MergeTreeTable
	part         *storage.Part
	columns      []string
	keyCondition *storage.KeyCondition

	granuleBegin  int  // start of scan range (after index pruning)
	granuleEnd    int  // end of scan range
	currentGran   int  // next granule to read
	batchSize     int  // granules per Work() call
	rangeResolved bool // true after index pruning is done
	reader        *storage.PartReader

	chunk    *Chunk // produced chunk waiting to be pushed
	finished bool
}

// NewSourceProcessor creates a source for one part.
func NewSourceProcessor(
	table *storage.MergeTreeTable,
	part *storage.Part,
	columns []string,
	keyCondition *storage.KeyCondition,
) *SourceProcessor {
	return &SourceProcessor{
		BaseProcessor: NewBaseProcessor("Source", 0, 1),
		table:         table,
		part:          part,
		columns:       columns,
		keyCondition:  keyCondition,
		batchSize:     1, // one granule per Work() call
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

	// If we have a chunk ready to push.
	if s.chunk != nil {
		if out.CanPush() {
			out.Push(s.chunk)
			s.chunk = nil
			// After push, check if more granules remain.
			if s.currentGran >= s.granuleEnd {
				// No more granules — wait for downstream to consume, then finish.
				return StatusPortFull
			}
			// More granules to read — signal port full so executor re-schedules us.
			return StatusPortFull
		}
		return StatusPortFull
	}

	// Resolve granule range once via primary index pruning.
	if !s.rangeResolved {
		s.resolveRange()
		s.rangeResolved = true
		if s.currentGran >= s.granuleEnd {
			s.finished = true
			out.SetFinished()
			return StatusFinished
		}
	}

	// All granules read and last chunk pushed — finish.
	if s.currentGran >= s.granuleEnd {
		if out.CanPush() {
			s.finished = true
			out.SetFinished()
			return StatusFinished
		}
		return StatusPortFull
	}

	// Ready to read next granule batch.
	if !out.CanPush() {
		return StatusPortFull
	}

	return StatusReady
}

func (s *SourceProcessor) Work() {
	if s.reader == nil {
		s.reader = storage.NewPartReader(s.part, &s.table.Schema)
	}

	// Read a batch of granules [currentGran, currentGran+batchSize).
	end := s.currentGran + s.batchSize
	if end > s.granuleEnd {
		end = s.granuleEnd
	}

	block, err := s.reader.ReadColumns(s.columns, s.currentGran, end)
	s.currentGran = end

	if err != nil || block == nil || block.NumRows() == 0 {
		return
	}

	s.chunk = NewChunk(block)
}

// resolveRange uses the primary index to determine the granule scan range.
func (s *SourceProcessor) resolveRange() {
	s.granuleBegin = 0
	s.granuleEnd = s.part.NumGranules

	if s.keyCondition != nil && s.granuleEnd > 0 {
		reader := storage.NewPartReader(s.part, &s.table.Schema)
		idx, err := reader.LoadPrimaryIndex()
		if err == nil {
			s.granuleBegin, s.granuleEnd = s.keyCondition.CheckInPrimaryIndex(idx)
		}
	}

	s.currentGran = s.granuleBegin
}
