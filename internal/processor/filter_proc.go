package processor

import (
	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
)

// FilterProcessor evaluates a WHERE predicate on each row.
type FilterProcessor struct {
	BaseProcessor
	expr parser.Expression

	inputChunk  *Chunk
	outputChunk *Chunk
	finished    bool
}

// NewFilterProcessor creates a filter with the given WHERE expression.
func NewFilterProcessor(expr parser.Expression) *FilterProcessor {
	return &FilterProcessor{
		BaseProcessor: NewBaseProcessor("Filter", 1, 1),
		expr:          expr,
	}
}

func (f *FilterProcessor) Prepare() Status {
	if f.finished {
		return StatusFinished
	}

	out := f.Output(0)
	inp := f.Input(0)

	if out.IsFinished() {
		f.finished = true
		inp.SetFinished()
		return StatusFinished
	}

	// Push pending output.
	if f.outputChunk != nil {
		if out.CanPush() {
			out.Push(f.outputChunk)
			f.outputChunk = nil
			// Return so downstream can consume before we potentially finish.
			return StatusNeedData
		}
		return StatusPortFull
	}

	// Pull input.
	if f.inputChunk == nil {
		if inp.HasData() {
			f.inputChunk = inp.Pull()
		} else if inp.IsFinished() {
			// Only finish if output port has been consumed.
			if out.CanPush() {
				f.finished = true
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

func (f *FilterProcessor) Work() {
	block := f.inputChunk.Block
	f.inputChunk = nil

	// Vectorized: evaluate WHERE expression column-at-a-time.
	resultCol, resultDT, err := engine.EvalExprColumn(f.expr, block)
	if err != nil {
		// Fallback to row-by-row for unsupported expressions.
		f.workRowByRow(block)
		return
	}

	mask := engine.ColumnToBoolMask(resultCol, resultDT)
	hasMatch := false
	for _, m := range mask {
		if m {
			hasMatch = true
			break
		}
	}
	if !hasMatch {
		return
	}

	f.outputChunk = NewChunk(block.FilterRowsByMask(mask))
}

func (f *FilterProcessor) workRowByRow(block *column.Block) {
	var matchingRows []int
	for i := 0; i < block.NumRows(); i++ {
		v, _, err := engine.EvalExpr(f.expr, block, i)
		if err != nil {
			continue
		}
		b, err := engine.ToBool(v)
		if err != nil {
			continue
		}
		if b {
			matchingRows = append(matchingRows, i)
		}
	}
	if len(matchingRows) == 0 {
		return
	}
	cols := make([]column.Column, block.NumColumns())
	for c := 0; c < block.NumColumns(); c++ {
		srcCol := block.Columns[c]
		newCol := column.NewColumnWithCapacity(srcCol.DataType(), len(matchingRows))
		for _, rowIdx := range matchingRows {
			newCol.Append(srcCol.Value(rowIdx))
		}
		cols[c] = newCol
	}
	names := make([]string, len(block.ColumnNames))
	copy(names, block.ColumnNames)
	f.outputChunk = NewChunk(column.NewBlock(names, cols))
}
