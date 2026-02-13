package merge

import (
	"fmt"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/storage"
	"github.com/harshithgowda/goose-db/internal/types"
)

// MergeExecutor performs the actual merge of multiple source parts into one.
type MergeExecutor struct {
	schema *storage.TableSchema
	codec  compression.Codec
}

// NewMergeExecutor creates a new merge executor.
func NewMergeExecutor(schema *storage.TableSchema, codec compression.Codec) *MergeExecutor {
	return &MergeExecutor{schema: schema, codec: codec}
}

// Merge reads all source parts, merge-sorts them, and writes a new merged part.
func (me *MergeExecutor) Merge(baseDir string, sourceParts []*storage.Part) (*storage.Part, error) {
	if len(sourceParts) == 0 {
		return nil, fmt.Errorf("no parts to merge")
	}

	colNames := me.schema.ColumnNames()

	// Read all source parts into blocks
	var blocks []*column.Block
	for _, src := range sourceParts {
		reader := storage.NewPartReader(src, me.schema)
		block, err := reader.ReadAll(colNames)
		if err != nil {
			return nil, fmt.Errorf("reading part %s: %w", src.Info.DirName(), err)
		}
		blocks = append(blocks, block)
	}

	// Merge all blocks into one
	merged := kWayMerge(blocks, me.schema.OrderBy, me.schema)
	if merged == nil || merged.NumRows() == 0 {
		return nil, fmt.Errorf("merged result is empty")
	}

	// Compute new PartInfo
	var minBlock, maxBlock uint64
	var maxLevel uint32
	minBlock = sourceParts[0].Info.MinBlock
	maxBlock = sourceParts[0].Info.MaxBlock
	maxLevel = sourceParts[0].Info.Level
	for _, p := range sourceParts[1:] {
		if p.Info.MinBlock < minBlock {
			minBlock = p.Info.MinBlock
		}
		if p.Info.MaxBlock > maxBlock {
			maxBlock = p.Info.MaxBlock
		}
		if p.Info.Level > maxLevel {
			maxLevel = p.Info.Level
		}
	}

	newInfo := storage.PartInfo{
		PartitionID: sourceParts[0].Info.PartitionID,
		MinBlock:    minBlock,
		MaxBlock:    maxBlock,
		Level:       maxLevel + 1,
	}

	// Write merged part
	writer := storage.NewPartWriter(me.schema, baseDir, me.codec)
	return writer.WritePart(merged, newInfo)
}

// kWayMerge merges multiple already-sorted blocks into one sorted block.
func kWayMerge(blocks []*column.Block, orderBy []string, schema *storage.TableSchema) *column.Block {
	if len(blocks) == 0 {
		return nil
	}
	if len(blocks) == 1 {
		return blocks[0]
	}

	// Resolve sort column types
	type sortKey struct {
		name string
		dt   types.DataType
	}
	keys := make([]sortKey, len(orderBy))
	for i, name := range orderBy {
		colDef, _ := schema.GetColumnDef(name)
		keys[i] = sortKey{name: name, dt: colDef.DataType}
	}

	colNames := schema.ColumnNames()

	// Simple approach: concatenate all blocks, then sort
	// For a production system you'd want a proper k-way merge with cursors
	result := blocks[0]
	for _, b := range blocks[1:] {
		newCols := make([]column.Column, len(colNames))
		for i, name := range colNames {
			col1, _ := result.GetColumn(name)
			col2, _ := b.GetColumn(name)
			merged := col1.Clone()
			column.AppendColumn(merged, col2)
			newCols[i] = merged
		}
		result = column.NewBlock(colNames, newCols)
	}

	// Sort by ORDER BY columns
	result.SortByColumns(orderBy)
	return result
}
