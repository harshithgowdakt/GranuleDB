package engine

import (
	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/storage"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// KeyRange specifies a range condition on a primary key column for index pruning.
type KeyRange struct {
	KeyColumn string
	MinVal    types.Value // nil = unbounded
	MaxVal    types.Value // nil = unbounded
}

// PartitionRange specifies a comparison condition on a partition column for
// minmax-index-based partition pruning.
type PartitionRange struct {
	Column string
	Op     string      // "=", ">", ">=", "<", "<="
	Value  types.Value // already coerced to column type
}

// TableScanOperator reads from all active parts of a table.
type TableScanOperator struct {
	table           *storage.MergeTreeTable
	columns         []string
	keyRanges       []KeyRange
	partitionRanges []PartitionRange

	parts       []*storage.Part
	currentPart int
	done        bool
}

func NewTableScanOperator(table *storage.MergeTreeTable, columns []string, keyRanges []KeyRange, partitionRanges []PartitionRange) *TableScanOperator {
	return &TableScanOperator{
		table:           table,
		columns:         columns,
		keyRanges:       keyRanges,
		partitionRanges: partitionRanges,
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

		// Try partition pruning via minmax indexes
		if len(s.partitionRanges) > 0 {
			indexes, err := reader.LoadMinMaxIndexes()
			if err == nil && len(indexes) > 0 {
				skip := false
				for _, pr := range s.partitionRanges {
					for _, idx := range indexes {
						if idx.ColumnName == pr.Column {
							if idx.CanSkipByRange(pr.Op, pr.Value) {
								skip = true
								break
							}
						}
					}
					if skip {
						break
					}
				}
				if skip {
					continue
				}
			}
		}

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
