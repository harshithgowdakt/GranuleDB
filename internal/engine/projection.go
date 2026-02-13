package engine

import (
	"fmt"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

// ProjectionOperator selects and computes output columns from SELECT expressions.
type ProjectionOperator struct {
	input      Operator
	selectExpr []parser.SelectExpr
	outNames   []string // computed output column names
}

func NewProjectionOperator(input Operator, selectExpr []parser.SelectExpr) *ProjectionOperator {
	return &ProjectionOperator{
		input:      input,
		selectExpr: selectExpr,
	}
}

// OutputNames returns the output column names.
func (p *ProjectionOperator) OutputNames() []string {
	return p.outNames
}

func (p *ProjectionOperator) Open() error {
	return p.input.Open()
}

func (p *ProjectionOperator) Next() (*column.Block, error) {
	block, err := p.input.Next()
	if err != nil || block == nil {
		return block, err
	}

	// Handle SELECT *
	if len(p.selectExpr) == 1 {
		if _, ok := p.selectExpr[0].Expr.(*parser.StarExpr); ok {
			p.outNames = block.ColumnNames
			return block, nil
		}
	}

	// Build output columns
	outCols := make([]column.Column, len(p.selectExpr))
	outNames := make([]string, len(p.selectExpr))

	for i, se := range p.selectExpr {
		if _, ok := se.Expr.(*parser.StarExpr); ok {
			// Star in multi-column select not supported
			return nil, fmt.Errorf("* must be the only select expression")
		}

		// Determine output name
		outName := se.Alias
		if outName == "" {
			outName = ExprName(se.Expr)
		}
		outNames[i] = outName

		// Check if this is a simple column reference
		if ref, ok := se.Expr.(*parser.ColumnRef); ok {
			col, exists := block.GetColumn(ref.Name)
			if !exists {
				return nil, fmt.Errorf("column %s not found", ref.Name)
			}
			outCols[i] = col
			continue
		}

		// Vectorized expression evaluation.
		vecCol, _, err := EvalExprColumn(se.Expr, block)
		if err != nil {
			// Fallback to row-by-row for unsupported expressions.
			outCol := column.NewColumnWithCapacity(types.TypeFloat64, block.NumRows())
			for row := range block.NumRows() {
				v, _, err := EvalExpr(se.Expr, block, row)
				if err != nil {
					return nil, err
				}
				outCol.Append(v)
			}
			outCols[i] = outCol
			continue
		}
		outCols[i] = vecCol
	}

	p.outNames = outNames
	return column.NewBlock(outNames, outCols), nil
}

func (p *ProjectionOperator) Close() error {
	return p.input.Close()
}

// ExprName returns a default name for an expression.
func ExprName(e parser.Expression) string {
	switch expr := e.(type) {
	case *parser.ColumnRef:
		return expr.Name
	case *parser.FunctionCall:
		if len(expr.Args) > 0 {
			return fmt.Sprintf("%s(%s)", expr.Name, ExprName(expr.Args[0]))
		}
		return expr.Name + "()"
	case *parser.StarExpr:
		return "*"
	default:
		return "expr"
	}
}
