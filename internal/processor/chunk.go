package processor

import "github.com/harshithgowdakt/granuledb/internal/column"

// Chunk is the unit of data flowing between processors through ports.
type Chunk struct {
	Block *column.Block
}

// NewChunk wraps a block into a chunk.
func NewChunk(block *column.Block) *Chunk {
	return &Chunk{Block: block}
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	if c == nil || c.Block == nil {
		return 0
	}
	return c.Block.NumRows()
}
