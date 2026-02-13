package engine

import "github.com/harshithgowda/goose-db/internal/column"

// Operator is a pull-based iterator producing blocks of data.
type Operator interface {
	Open() error
	// Next returns the next block, or nil when exhausted.
	Next() (*column.Block, error)
	Close() error
}
