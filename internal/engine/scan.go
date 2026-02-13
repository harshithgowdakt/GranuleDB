package engine

import (
	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/storage"
	"github.com/harshithgowda/goose-db/internal/types"
)

// KeyRange specifies a range condition on a primary key column for index pruning.
type KeyRange struct {
	KeyColumn string
	MinVal    types.Value // nil = unbounded
	MaxVal    types.Value // nil = unbounded
}

// TableScanOperator reads from all active parts of a table.
type TableScanOperator struct {
	table     *storage.MergeTreeTable
	columns   []string
	keyRanges []KeyRange

	parts       []*storage.Part
	currentPart int
	done        bool
}

func NewTableScanOperator(table *storage.MergeTreeTable, columns []string, keyRanges []KeyRange) *TableScanOperator {
	return &TableScanOperator{
		table:     table,
		columns:   columns,
		keyRanges: keyRanges,
	}
}

func (s *TableScanOperator) Open() error {
	s.parts = s.table.GetActiveParts()
	s.currentPart = 0
	s.done = false
	return nil
}

func (s *TableScanOperator) Next() (*column.Block, error) {
	for s.currentPart < len(s.parts) {
		part := s.parts[s.currentPart]
		s.currentPart++

		reader := storage.NewPartReader(part, &s.table.Schema)

		// Try primary index pruning
		granuleBegin := 0
		granuleEnd := part.NumGranules

		if len(s.keyRanges) > 0 && granuleEnd > 0 {
			idx, err := reader.LoadPrimaryIndex()
			if err == nil {
				for _, kr := range s.keyRanges {
					gb, ge := idx.FindGranuleRange(kr.KeyColumn, kr.MinVal, kr.MaxVal)
					if gb > granuleBegin {
						granuleBegin = gb
					}
					if ge < granuleEnd {
						granuleEnd = ge
					}
				}
			}
		}

		if granuleBegin >= granuleEnd {
			continue // skip this part entirely
		}

		block, err := reader.ReadColumns(s.columns, granuleBegin, granuleEnd)
		if err != nil {
			return nil, err
		}
		if block.NumRows() == 0 {
			continue
		}
		return block, nil
	}

	return nil, nil // exhausted
}

func (s *TableScanOperator) Close() error {
	return nil
}
