package engine

import "github.com/harshithgowda/goose-db/internal/column"

// LimitOperator limits the number of output rows.
type LimitOperator struct {
	input    Operator
	limit    int64
	emitted  int64
}

func NewLimitOperator(input Operator, limit int64) *LimitOperator {
	return &LimitOperator{input: input, limit: limit}
}

func (l *LimitOperator) Open() error {
	l.emitted = 0
	return l.input.Open()
}

func (l *LimitOperator) Next() (*column.Block, error) {
	if l.emitted >= l.limit {
		return nil, nil
	}

	block, err := l.input.Next()
	if err != nil || block == nil {
		return block, err
	}

	remaining := l.limit - l.emitted
	if int64(block.NumRows()) > remaining {
		block = block.SliceRows(0, int(remaining))
	}
	l.emitted += int64(block.NumRows())
	return block, nil
}

func (l *LimitOperator) Close() error {
	return l.input.Close()
}
