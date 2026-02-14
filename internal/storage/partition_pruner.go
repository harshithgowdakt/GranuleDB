package storage

import (
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// PartitionPruner wraps a KeyCondition and caches pruning results per partition
// ID, matching ClickHouse's PartitionPruner. The cache avoids re-loading and
// re-evaluating MinMax indexes for parts that share the same partition ID.
type PartitionPruner struct {
	condition *KeyCondition
	schema    *TableSchema
	cache     map[string]bool // partitionID -> canBePruned
}

// NewPartitionPruner creates a pruner for the given WHERE expression and schema.
// Returns nil when pruning is not applicable (no partition key, nil WHERE, or no
// usable conditions on partition columns).
func NewPartitionPruner(where parser.Expression, schema *TableSchema) *PartitionPruner {
	if where == nil || schema.PartitionBy == "" {
		return nil
	}

	partExpr, err := parser.ParseExpression(schema.PartitionBy)
	if err != nil {
		return nil
	}
	colNames := parser.ExtractColumnRefs(partExpr)
	if len(colNames) == 0 {
		return nil
	}

	keyTypes := make([]types.DataType, 0, len(colNames))
	validCols := make([]string, 0, len(colNames))
	for _, col := range colNames {
		colDef, ok := schema.GetColumnDef(col)
		if !ok {
			continue
		}
		validCols = append(validCols, col)
		keyTypes = append(keyTypes, colDef.DataType)
	}
	if len(validCols) == 0 {
		return nil
	}

	cond := NewKeyCondition(where, validCols, keyTypes)
	if cond == nil {
		return nil
	}

	return &PartitionPruner{
		condition: cond,
		schema:    schema,
		cache:     make(map[string]bool),
	}
}

// CanBePruned returns true if the part can be safely skipped — the WHERE
// condition is guaranteed to be false for all rows in the part based on its
// MinMax indexes.
func (pp *PartitionPruner) CanBePruned(part *Part) bool {
	pid := part.Info.PartitionID
	if result, ok := pp.cache[pid]; ok {
		return result
	}

	reader := NewPartReader(part, pp.schema)
	indexes, err := reader.LoadMinMaxIndexes()
	if err != nil || len(indexes) == 0 {
		pp.cache[pid] = false
		return false
	}

	// Build a Hyperrectangle from the MinMax indexes, one Range per key column.
	hr := make(Hyperrectangle, len(pp.condition.keyColumns))
	for i, col := range pp.condition.keyColumns {
		dt := pp.condition.keyTypes[i]
		// Default: unbounded range (if index not found, condition evaluates to MaskMaybe).
		hr[i] = Range{Left: nil, Right: nil, DataType: dt}
		for _, idx := range indexes {
			if idx.ColumnName == col {
				hr[i] = Range{
					Left:          idx.Min,
					Right:         idx.Max,
					LeftIncluded:  true,
					RightIncluded: true,
					DataType:      idx.DataType,
				}
				break
			}
		}
	}

	mask := pp.condition.CheckInHyperrectangle(hr)
	pruned := !mask.CanBeTrue
	pp.cache[pid] = pruned
	return pruned
}
