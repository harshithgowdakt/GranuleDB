package processor

import (
	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

// ProjectionProcessor evaluates SELECT expressions.
type ProjectionProcessor struct {
	BaseProcessor
	selectExprs []parser.SelectExpr

	inputChunk  *Chunk
	outputChunk *Chunk
	finished    bool
}

// NewProjectionProcessor creates a projection with the given SELECT expressions.
func NewProjectionProcessor(selectExprs []parser.SelectExpr) *ProjectionProcessor {
	return &ProjectionProcessor{
		BaseProcessor: NewBaseProcessor("Projection", 1, 1),
		selectExprs:   selectExprs,
	}
}

func (p *ProjectionProcessor) Prepare() Status {
	if p.finished {
		return StatusFinished
	}

	out := p.Output(0)
	inp := p.Input(0)

	if out.IsFinished() {
		p.finished = true
		inp.SetFinished()
		return StatusFinished
	}

	// Push pending output.
	if p.outputChunk != nil {
		if out.CanPush() {
			out.Push(p.outputChunk)
			p.outputChunk = nil
			return StatusNeedData // let downstream consume
		}
		return StatusPortFull
	}

	// Pull input.
	if p.inputChunk == nil {
		if inp.HasData() {
			p.inputChunk = inp.Pull()
		} else if inp.IsFinished() {
			// Only finish if output has been consumed.
			if out.CanPush() {
				p.finished = true
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

func (p *ProjectionProcessor) Work() {
	block := p.inputChunk.Block
	p.inputChunk = nil

	// Handle SELECT *
	if len(p.selectExprs) == 1 {
		if _, ok := p.selectExprs[0].Expr.(*parser.StarExpr); ok {
			p.outputChunk = NewChunk(block)
			return
		}
	}

	outCols := make([]column.Column, len(p.selectExprs))
	outNames := make([]string, len(p.selectExprs))

	for i, se := range p.selectExprs {
		outName := se.Alias
		if outName == "" {
			outName = engine.ExprName(se.Expr)
		}
		outNames[i] = outName

		// Simple column reference: zero-copy.
		if ref, ok := se.Expr.(*parser.ColumnRef); ok {
			col, exists := block.GetColumn(ref.Name)
			if !exists {
				outCols[i] = column.NewColumn(types.TypeString)
				continue
			}
			outCols[i] = col
			continue
		}

		// Vectorized expression evaluation.
		vecCol, _, err := engine.EvalExprColumn(se.Expr, block)
		if err != nil {
			// Fallback to row-by-row for unsupported expressions.
			outCol := column.NewColumnWithCapacity(types.TypeFloat64, block.NumRows())
			for row := 0; row < block.NumRows(); row++ {
				v, _, err := engine.EvalExpr(se.Expr, block, row)
				if err != nil {
					outCol.Append(float64(0))
					continue
				}
				outCol.Append(v)
			}
			outCols[i] = outCol
			continue
		}
		outCols[i] = vecCol
	}

	p.outputChunk = NewChunk(column.NewBlock(outNames, outCols))
}
