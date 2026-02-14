package merge

import (
	"fmt"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/aggstate"
	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/compression"
	"github.com/harshithgowdakt/granuledb/internal/storage"
	"github.com/harshithgowdakt/granuledb/internal/types"
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

	if strings.EqualFold(schema.Engine, "AggregatingMergeTree") {
		result = collapseAggregatingRows(result, orderBy)
	}
	return result
}

// collapseAggregatingRows merges rows with the same ORDER BY key.
// For non-key numeric columns, it sums values; for non-numeric columns, it keeps first value.
func collapseAggregatingRows(block *column.Block, orderBy []string) *column.Block {
	if block == nil || block.NumRows() <= 1 || len(orderBy) == 0 {
		return block
	}

	keySet := make(map[string]bool, len(orderBy))
	for _, k := range orderBy {
		keySet[k] = true
	}

	outCols := make([]column.Column, len(block.Columns))
	for i, c := range block.Columns {
		outCols[i] = column.NewColumnWithCapacity(c.DataType(), block.NumRows())
	}

	appendRange := func(start, end int) {
		for cIdx, c := range block.Columns {
			name := block.ColumnNames[cIdx]
			dt := c.DataType()

			if keySet[name] || !dt.IsNumeric() {
				if !keySet[name] && dt == types.TypeAggregateState {
					merged, ok := mergeAggregateStates(c, start, end)
					if ok {
						outCols[cIdx].Append(merged)
						continue
					}
				}
				outCols[cIdx].Append(c.Value(start))
				continue
			}

			// Use type-specific accumulators to avoid float64 precision loss.
			switch dt {
			case types.TypeUInt8:
				var sum uint8
				for r := start; r < end; r++ {
					sum += c.Value(r).(uint8)
				}
				outCols[cIdx].Append(sum)
			case types.TypeUInt16:
				var sum uint16
				for r := start; r < end; r++ {
					sum += c.Value(r).(uint16)
				}
				outCols[cIdx].Append(sum)
			case types.TypeUInt32:
				var sum uint32
				for r := start; r < end; r++ {
					sum += c.Value(r).(uint32)
				}
				outCols[cIdx].Append(sum)
			case types.TypeUInt64:
				var sum uint64
				for r := start; r < end; r++ {
					sum += c.Value(r).(uint64)
				}
				outCols[cIdx].Append(sum)
			case types.TypeInt8:
				var sum int8
				for r := start; r < end; r++ {
					sum += c.Value(r).(int8)
				}
				outCols[cIdx].Append(sum)
			case types.TypeInt16:
				var sum int16
				for r := start; r < end; r++ {
					sum += c.Value(r).(int16)
				}
				outCols[cIdx].Append(sum)
			case types.TypeInt32:
				var sum int32
				for r := start; r < end; r++ {
					sum += c.Value(r).(int32)
				}
				outCols[cIdx].Append(sum)
			case types.TypeInt64:
				var sum int64
				for r := start; r < end; r++ {
					sum += c.Value(r).(int64)
				}
				outCols[cIdx].Append(sum)
			case types.TypeFloat32:
				var sum float32
				for r := start; r < end; r++ {
					sum += c.Value(r).(float32)
				}
				outCols[cIdx].Append(sum)
			case types.TypeFloat64:
				var sum float64
				for r := start; r < end; r++ {
					sum += c.Value(r).(float64)
				}
				outCols[cIdx].Append(sum)
			case types.TypeDateTime:
				var sum uint32
				for r := start; r < end; r++ {
					sum += c.Value(r).(uint32)
				}
				outCols[cIdx].Append(sum)
			default:
				outCols[cIdx].Append(c.Value(start))
			}
		}
	}

	groupStart := 0
	for row := 1; row <= block.NumRows(); row++ {
		sameKey := false
		if row < block.NumRows() {
			sameKey = true
			for _, k := range orderBy {
				col, _ := block.GetColumn(k)
				if types.CompareValues(col.DataType(), col.Value(groupStart), col.Value(row)) != 0 {
					sameKey = false
					break
				}
			}
		}
		if sameKey {
			continue
		}
		appendRange(groupStart, row)
		groupStart = row
	}

	return column.NewBlock(block.ColumnNames, outCols)
}

func mergeAggregateStates(col column.Column, start, end int) ([]byte, bool) {
	first, ok := col.Value(start).([]byte)
	if !ok {
		return nil, false
	}
	kind := aggstate.StateKind(first)
	if kind == aggstate.KindUnknown {
		return nil, false
	}

	acc := make([]byte, len(first))
	copy(acc, first)

	for r := start + 1; r < end; r++ {
		cur, ok := col.Value(r).([]byte)
		if !ok || aggstate.StateKind(cur) != kind {
			return nil, false
		}
		switch kind {
		case aggstate.KindSum:
			a, okA := aggstate.DecodeSumState(acc)
			b, okB := aggstate.DecodeSumState(cur)
			if !okA || !okB {
				return nil, false
			}
			acc = aggstate.EncodeSumState(a + b)
		case aggstate.KindUniqHLL:
			ab, okA := aggstate.DecodeUniqHLLState(acc)
			bb, okB := aggstate.DecodeUniqHLLState(cur)
			if !okA || !okB {
				return nil, false
			}
			ah := aggstate.NewHLL12()
			bh := aggstate.NewHLL12()
			if !ah.UnmarshalBinary(ab) || !bh.UnmarshalBinary(bb) {
				return nil, false
			}
			ah.Merge(bh)
			acc = aggstate.EncodeUniqHLLState(ah.MarshalBinary())
		default:
			return nil, false
		}
	}
	return acc, true
}
