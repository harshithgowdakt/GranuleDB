package processor

// LimitProcessor passes through up to `limit` rows, then finishes.
type LimitProcessor struct {
	BaseProcessor
	limit   int64
	emitted int64

	inputChunk  *Chunk
	outputChunk *Chunk
	finished    bool
}

// NewLimitProcessor creates a limit processor.
func NewLimitProcessor(limit int64) *LimitProcessor {
	return &LimitProcessor{
		BaseProcessor: NewBaseProcessor("Limit", 1, 1),
		limit:         limit,
	}
}

func (l *LimitProcessor) Prepare() Status {
	if l.finished {
		return StatusFinished
	}

	out := l.Output(0)
	inp := l.Input(0)

	if out.IsFinished() {
		l.finished = true
		inp.SetFinished()
		return StatusFinished
	}

	// Check if limit reached.
	if l.emitted >= l.limit && l.outputChunk == nil {
		// Wait for downstream to consume last push before finishing.
		if out.CanPush() {
			l.finished = true
			out.SetFinished()
			inp.SetFinished()
			return StatusFinished
		}
		return StatusPortFull
	}

	// Push pending output.
	if l.outputChunk != nil {
		if out.CanPush() {
			out.Push(l.outputChunk)
			l.outputChunk = nil
			if l.emitted >= l.limit {
				// Don't finish yet, wait for downstream to consume.
				return StatusPortFull
			}
			return StatusNeedData
		}
		return StatusPortFull
	}

	// Pull input.
	if l.inputChunk == nil {
		if inp.HasData() {
			l.inputChunk = inp.Pull()
		} else if inp.IsFinished() {
			if out.CanPush() {
				l.finished = true
				out.SetFinished()
				return StatusFinished
			}
			return StatusPortFull
		} else {
			return StatusNeedData
		}
	}

	return StatusReady
}

func (l *LimitProcessor) Work() {
	block := l.inputChunk.Block
	l.inputChunk = nil

	remaining := l.limit - l.emitted
	if int64(block.NumRows()) <= remaining {
		l.emitted += int64(block.NumRows())
		l.outputChunk = NewChunk(block)
		return
	}

	sliced := block.SliceRows(0, int(remaining))
	l.emitted += int64(sliced.NumRows())
	l.outputChunk = NewChunk(sliced)
}
