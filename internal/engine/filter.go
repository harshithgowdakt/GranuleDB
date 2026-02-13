package engine

import (
	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
)

// FilterOperator evaluates a WHERE expression on each block, returning only matching rows.
type FilterOperator struct {
	input Operator
	expr  parser.Expression
}

func NewFilterOperator(input Operator, expr parser.Expression) *FilterOperator {
	return &FilterOperator{input: input, expr: expr}
}

func (f *FilterOperator) Open() error {
	return f.input.Open()
}

func (f *FilterOperator) Next() (*column.Block, error) {
	for {
		block, err := f.input.Next()
		if err != nil || block == nil {
			return block, err
		}

		// Vectorized: evaluate WHERE expression column-at-a-time.
		resultCol, resultDT, err := EvalExprColumn(f.expr, block)
		if err != nil {
			// Fallback to row-by-row for unsupported expressions.
			return f.filterRowByRow(block)
		}

		mask := ColumnToBoolMask(resultCol, resultDT)
		hasMatch := false
		for _, m := range mask {
			if m {
				hasMatch = true
				break
			}
		}
		if !hasMatch {
			continue
		}

		return block.FilterRowsByMask(mask), nil
	}
}

func (f *FilterOperator) filterRowByRow(block *column.Block) (*column.Block, error) {
	var matchingRows []int
	for i := range block.NumRows() {
		v, _, err := EvalExpr(f.expr, block, i)
		if err != nil {
			continue
		}
		b, err := ToBool(v)
		if err != nil {
			continue
		}
		if b {
			matchingRows = append(matchingRows, i)
		}
	}
	if len(matchingRows) == 0 {
		return nil, nil
	}
	cols := make([]column.Column, block.NumColumns())
	for c := range block.NumColumns() {
		srcCol := block.Columns[c]
		newCol := column.NewColumnWithCapacity(srcCol.DataType(), len(matchingRows))
		for _, rowIdx := range matchingRows {
			newCol.Append(srcCol.Value(rowIdx))
		}
		cols[c] = newCol
	}
	names := make([]string, len(block.ColumnNames))
	copy(names, block.ColumnNames)
	return column.NewBlock(names, cols), nil
}

func (f *FilterOperator) Close() error {
	return f.input.Close()
}
