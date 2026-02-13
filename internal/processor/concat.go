package processor

// ConcatProcessor merges N input ports into 1 output port.
// It forwards chunks from inputs in order.
type ConcatProcessor struct {
	BaseProcessor

	currentInput int
	chunk        *Chunk // chunk waiting to be pushed downstream
	allDrained   bool   // true when all inputs finished and drained
	finished     bool
}

// NewConcatProcessor creates a concat with numInputs input ports.
func NewConcatProcessor(numInputs int) *ConcatProcessor {
	return &ConcatProcessor{
		BaseProcessor: NewBaseProcessor("Concat", numInputs, 1),
	}
}

func (c *ConcatProcessor) Prepare() Status {
	if c.finished {
		return StatusFinished
	}

	out := c.Output(0)

	// If downstream cancelled.
	if out.IsFinished() {
		c.finished = true
		return StatusFinished
	}

	// All inputs drained — wait for downstream to consume last push before finishing.
	if c.allDrained {
		if out.CanPush() {
			c.finished = true
			out.SetFinished()
			return StatusFinished
		}
		return StatusPortFull
	}

	// If we have a pending chunk, try to push it.
	if c.chunk != nil {
		if out.CanPush() {
			out.Push(c.chunk)
			c.chunk = nil
			return c.checkInputs()
		}
		return StatusPortFull
	}

	return c.checkInputs()
}

func (c *ConcatProcessor) checkInputs() Status {
	out := c.Output(0)
	numInputs := len(c.Inputs())

	if numInputs == 0 {
		c.finished = true
		out.SetFinished()
		return StatusFinished
	}

	for i := 0; i < numInputs; i++ {
		idx := (c.currentInput + i) % numInputs
		inp := c.Input(idx)

		if inp.HasData() {
			c.currentInput = (idx + 1) % numInputs
			chunk := inp.Pull()
			if out.CanPush() {
				out.Push(chunk)
				return StatusNeedData // pushed; might need more
			}
			c.chunk = chunk
			return StatusPortFull
		}

		if !inp.IsFinished() {
			return StatusNeedData
		}
	}

	// All inputs are finished and drained.
	// Don't SetFinished yet — wait for downstream to consume last push.
	c.allDrained = true
	if out.CanPush() {
		c.finished = true
		out.SetFinished()
		return StatusFinished
	}
	return StatusPortFull
}

func (c *ConcatProcessor) Work() {
	// All work is done in Prepare() (just moving chunk pointers).
}
