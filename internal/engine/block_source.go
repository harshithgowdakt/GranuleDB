package engine

import "github.com/harshithgowdakt/granuledb/internal/column"

// BlockSourceOperator emits a single in-memory block.
type BlockSourceOperator struct {
	block   *column.Block
	emitted bool
}

// NewBlockSourceOperator creates a one-shot source for block execution.
func NewBlockSourceOperator(block *column.Block) *BlockSourceOperator {
	return &BlockSourceOperator{block: block}
}

func (s *BlockSourceOperator) Open() error {
	s.emitted = false
	return nil
}

func (s *BlockSourceOperator) Next() (*column.Block, error) {
	if s.emitted {
		return nil, nil
	}
	s.emitted = true
	return s.block, nil
}

func (s *BlockSourceOperator) Close() error { return nil }
