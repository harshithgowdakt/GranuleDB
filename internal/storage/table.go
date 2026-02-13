package storage

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/types"
)

// MergeTreeTable represents a single table with MergeTree engine.
type MergeTreeTable struct {
	Name    string
	Schema  TableSchema
	DataDir string // path: <db_data_dir>/<table_name>/

	mu           sync.RWMutex
	parts        []*Part
	blockCounter atomic.Uint64
}

// NewMergeTreeTable creates a new table.
func NewMergeTreeTable(name string, schema TableSchema, dataDir string) *MergeTreeTable {
	return &MergeTreeTable{
		Name:    name,
		Schema:  schema,
		DataDir: dataDir,
	}
}

// Insert splits a block by partition, sorts each sub-block by ORDER BY, and writes parts.
func (t *MergeTreeTable) Insert(block *column.Block) error {
	partitions, err := t.splitByPartition(block)
	if err != nil {
		return err
	}

	codec := &compression.LZ4Codec{}
	writer := NewPartWriter(&t.Schema, t.DataDir, codec)

	for partitionID, subBlock := range partitions {
		// Sort by ORDER BY columns
		if err := subBlock.SortByColumns(t.Schema.OrderBy); err != nil {
			return fmt.Errorf("sorting block: %w", err)
		}

		blockNum := t.blockCounter.Add(1)
		info := PartInfo{
			PartitionID: partitionID,
			MinBlock:    blockNum,
			MaxBlock:    blockNum,
			Level:       0,
		}

		part, err := writer.WritePart(subBlock, info)
		if err != nil {
			return fmt.Errorf("writing part: %w", err)
		}

		t.mu.Lock()
		t.parts = append(t.parts, part)
		t.mu.Unlock()
	}

	return nil
}

// splitByPartition splits a block into sub-blocks per partition.
func (t *MergeTreeTable) splitByPartition(block *column.Block) (map[string]*column.Block, error) {
	if t.Schema.PartitionBy == "" {
		return map[string]*column.Block{"all": block}, nil
	}

	partCol, ok := block.GetColumn(t.Schema.PartitionBy)
	if !ok {
		return nil, fmt.Errorf("partition column %s not found", t.Schema.PartitionBy)
	}

	// Group row indices by partition value
	partRows := make(map[string][]int)
	for i := 0; i < block.NumRows(); i++ {
		pid := types.ValueToString(partCol.DataType(), partCol.Value(i))
		partRows[pid] = append(partRows[pid], i)
	}

	result := make(map[string]*column.Block, len(partRows))
	for pid, rows := range partRows {
		cols := make([]column.Column, block.NumColumns())
		for c := range block.NumColumns() {
			srcCol := block.Columns[c]
			newCol := column.NewColumnWithCapacity(srcCol.DataType(), len(rows))
			for _, rowIdx := range rows {
				newCol.Append(srcCol.Value(rowIdx))
			}
			cols[c] = newCol
		}
		names := make([]string, len(block.ColumnNames))
		copy(names, block.ColumnNames)
		result[pid] = column.NewBlock(names, cols)
	}

	return result, nil
}

// GetActiveParts returns all parts with state == PartActive.
func (t *MergeTreeTable) GetActiveParts() []*Part {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var active []*Part
	for _, p := range t.parts {
		if p.State == PartActive {
			active = append(active, p)
		}
	}
	// Sort by partition then MinBlock for deterministic ordering
	sort.Slice(active, func(i, j int) bool {
		if active[i].Info.PartitionID != active[j].Info.PartitionID {
			return active[i].Info.PartitionID < active[j].Info.PartitionID
		}
		return active[i].Info.MinBlock < active[j].Info.MinBlock
	})
	return active
}

// GetActivePartsForPartition returns active parts for a specific partition.
func (t *MergeTreeTable) GetActivePartsForPartition(partitionID string) []*Part {
	parts := t.GetActiveParts()
	var result []*Part
	for _, p := range parts {
		if p.Info.PartitionID == partitionID {
			result = append(result, p)
		}
	}
	return result
}

// ReplaceParts atomically marks old parts as outdated and adds the new merged part.
func (t *MergeTreeTable) ReplaceParts(oldParts []*Part, newPart *Part) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Mark old parts as outdated
	oldSet := make(map[string]bool, len(oldParts))
	for _, p := range oldParts {
		oldSet[p.Info.DirName()] = true
	}
	for _, p := range t.parts {
		if oldSet[p.Info.DirName()] {
			p.State = PartOutdated
		}
	}

	// Add new part
	t.parts = append(t.parts, newPart)
}

// AddPart adds a part directly (used during metadata loading).
func (t *MergeTreeTable) AddPart(part *Part) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parts = append(t.parts, part)
	// Update block counter
	if part.Info.MaxBlock >= t.blockCounter.Load() {
		t.blockCounter.Store(part.Info.MaxBlock)
	}
}
