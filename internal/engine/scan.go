package engine

import (
	"log"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// TableScanOperator reads from pre-filtered active parts of a table.
// Partition pruning is done before construction via storage.FilterParts();
// this operator performs primary index granule pruning via KeyCondition.
type TableScanOperator struct {
	table        *storage.MergeTreeTable
	columns      []string
	keyCondition *storage.KeyCondition

	parts       []*storage.Part
	currentPart int
	done        bool
}

func NewTableScanOperator(table *storage.MergeTreeTable, columns []string, keyCondition *storage.KeyCondition, parts []*storage.Part) *TableScanOperator {
	return &TableScanOperator{
		table:        table,
		columns:      columns,
		keyCondition: keyCondition,
		parts:        parts,
	}
}

func (s *TableScanOperator) Open() error {
	s.currentPart = 0
	s.done = false
	log.Printf("[scan] opening table scan: %d parts, reading columns [%s]",
		len(s.parts), strings.Join(s.columns, ", "))
	return nil
}

func (s *TableScanOperator) Next() (*column.Block, error) {
	for s.currentPart < len(s.parts) {
		part := s.parts[s.currentPart]
		s.currentPart++

		reader := storage.NewPartReader(part, &s.table.Schema)

		// Primary index pruning (granule-level)
		granuleBegin := 0
		granuleEnd := part.NumGranules

		if s.keyCondition != nil && granuleEnd > 0 {
			idx, err := reader.LoadPrimaryIndex()
			if err == nil {
				granuleBegin, granuleEnd = s.keyCondition.CheckInPrimaryIndex(idx)
			}
		}

		if granuleBegin >= granuleEnd {
			log.Printf("[scan] part %s: all %d granules pruned — skipping entire part",
				part.Info.DirName(), part.NumGranules)
			continue // skip this part entirely
		}

		log.Printf("[scan] part %s: reading granules [%d, %d) of %d",
			part.Info.DirName(), granuleBegin, granuleEnd, part.NumGranules)

		block, err := reader.ReadColumns(s.columns, granuleBegin, granuleEnd)
		if err != nil {
			return nil, err
		}
		if block.NumRows() == 0 {
			log.Printf("[scan] part %s: block empty after read", part.Info.DirName())
			continue
		}
		log.Printf("[scan] part %s: read %d rows", part.Info.DirName(), block.NumRows())
		return block, nil
	}

	return nil, nil // exhausted
}

func (s *TableScanOperator) Close() error {
	return nil
}
