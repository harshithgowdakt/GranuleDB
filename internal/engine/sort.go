package engine

import (
	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/parser"
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

	// Sort by ORDER BY columns with per-column direction.
	sortCols := make([]string, len(s.orderBy))
	desc := make([]bool, len(s.orderBy))
	for i, ob := range s.orderBy {
		sortCols[i] = ob.Column
		desc[i] = ob.Desc
	}

	if err := all.SortByColumnsWithDirection(sortCols, desc); err != nil {
		return nil, err
	}

	return all, nil
}

func (s *SortOperator) Close() error {
	return s.input.Close()
}
