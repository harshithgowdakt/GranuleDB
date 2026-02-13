package processor

import "github.com/harshithgowda/goose-db/internal/column"

// OutputProcessor is the terminal sink of a pipeline.
// It collects all incoming chunks into a result slice.
type OutputProcessor struct {
	BaseProcessor

	Chunks   []*Chunk
	finished bool
}

// NewOutputProcessor creates an output sink.
func NewOutputProcessor() *OutputProcessor {
	return &OutputProcessor{
		BaseProcessor: NewBaseProcessor("Output", 1, 0),
	}
}

func (o *OutputProcessor) Prepare() Status {
	if o.finished {
		return StatusFinished
	}

	inp := o.Input(0)

	if inp.HasData() {
		chunk := inp.Pull()
		o.Chunks = append(o.Chunks, chunk)
		return StatusNeedData
	}

	if inp.IsFinished() {
		o.finished = true
		return StatusFinished
	}

	return StatusNeedData
}

func (o *OutputProcessor) Work() {
	// All work done in Prepare().
}

// ResultBlocks returns the collected blocks.
func (o *OutputProcessor) ResultBlocks() []*column.Block {
	blocks := make([]*column.Block, 0, len(o.Chunks))
	for _, c := range o.Chunks {
		if c != nil && c.Block != nil && c.Block.NumRows() > 0 {
			blocks = append(blocks, c.Block)
		}
	}
	return blocks
}
