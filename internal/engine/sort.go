package engine

import (
	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
)

// SortOperator sorts by ORDER BY columns. It materializes all input blocks first.
type SortOperator struct {
	input   Operator
	orderBy []parser.OrderByExpr

	result *column.Block
	done   bool
}

func NewSortOperator(input Operator, orderBy []parser.OrderByExpr) *SortOperator {
	return &SortOperator{input: input, orderBy: orderBy}
}

func (s *SortOperator) Open() error {
	return s.input.Open()
}

func (s *SortOperator) Next() (*column.Block, error) {
	if s.done {
		return nil, nil
	}
	s.done = true

	// Materialize all blocks
	var all *column.Block
	for {
		block, err := s.input.Next()
		if err != nil {
			return nil, err
		}
		if block == nil {
			break
		}
		if all == nil {
			all = block
		} else {
			if err := all.AppendBlock(block); err != nil {
				return nil, err
			}
		}
	}

	if all == nil || all.NumRows() == 0 {
		return nil, nil
	}

	// Sort by ORDER BY columns
	sortCols := make([]string, len(s.orderBy))
	for i, ob := range s.orderBy {
		sortCols[i] = ob.Column
	}

	if err := all.SortByColumns(sortCols); err != nil {
		return nil, err
	}

	// Handle DESC ordering by reversing
	hasDesc := false
	for _, ob := range s.orderBy {
		if ob.Desc {
			hasDesc = true
			break
		}
	}
	if hasDesc && len(s.orderBy) == 1 && s.orderBy[0].Desc {
		// Simple case: reverse the entire block
		n := all.NumRows()
		indices := make([]int, n)
		for i := range indices {
			indices[i] = n - 1 - i
		}
		newCols := make([]column.Column, len(all.Columns))
		for c, col := range all.Columns {
			newCols[c] = column.Gather(col, indices)
		}
		all = column.NewBlock(all.ColumnNames, newCols)
	}

	return all, nil
}

func (s *SortOperator) Close() error {
	return s.input.Close()
}
