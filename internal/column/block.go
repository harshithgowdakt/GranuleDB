package column

import (
	"fmt"
	"sort"

	"github.com/harshithgowdakt/granuledb/internal/types"
)

// Block is a chunk of columnar data with named columns, all the same length.
type Block struct {
	ColumnNames []string
	Columns     []Column
	nameIndex   map[string]int
}

// NewBlock creates a block from parallel slices of names and columns.
func NewBlock(names []string, cols []Column) *Block {
	idx := make(map[string]int, len(names))
	for i, n := range names {
		idx[n] = i
	}
	return &Block{
		ColumnNames: names,
		Columns:     cols,
		nameIndex:   idx,
	}
}

// NumRows returns the number of rows in the block.
func (b *Block) NumRows() int {
	if len(b.Columns) == 0 {
		return 0
	}
	return b.Columns[0].Len()
}

// NumColumns returns the number of columns.
func (b *Block) NumColumns() int {
	return len(b.Columns)
}

// GetColumn returns the column with the given name.
func (b *Block) GetColumn(name string) (Column, bool) {
	if b.nameIndex == nil {
		b.rebuildIndex()
	}
	i, ok := b.nameIndex[name]
	if !ok {
		return nil, false
	}
	return b.Columns[i], true
}

// GetColumnIndex returns the index of a column by name.
func (b *Block) GetColumnIndex(name string) (int, bool) {
	if b.nameIndex == nil {
		b.rebuildIndex()
	}
	i, ok := b.nameIndex[name]
	return i, ok
}

// ColumnTypes returns the data types of all columns.
func (b *Block) ColumnTypes() []types.DataType {
	dts := make([]types.DataType, len(b.Columns))
	for i, c := range b.Columns {
		dts[i] = c.DataType()
	}
	return dts
}

func (b *Block) rebuildIndex() {
	b.nameIndex = make(map[string]int, len(b.ColumnNames))
	for i, n := range b.ColumnNames {
		b.nameIndex[n] = i
	}
}

// AppendBlock appends all rows from another block with the same schema.
func (b *Block) AppendBlock(other *Block) error {
	if len(b.Columns) != len(other.Columns) {
		return fmt.Errorf("column count mismatch: %d vs %d", len(b.Columns), len(other.Columns))
	}
	for i := range b.Columns {
		AppendColumn(b.Columns[i], other.Columns[i])
	}
	return nil
}

// SliceRows returns a new block with rows [from, to).
func (b *Block) SliceRows(from, to int) *Block {
	cols := make([]Column, len(b.Columns))
	for i, c := range b.Columns {
		cols[i] = c.Slice(from, to)
	}
	names := make([]string, len(b.ColumnNames))
	copy(names, b.ColumnNames)
	return NewBlock(names, cols)
}

// SortByColumns sorts the block by the given column names in ascending order.
func (b *Block) SortByColumns(sortCols []string) error {
	if b.NumRows() <= 1 {
		return nil
	}

	// Resolve sort column indices and types
	type sortKey struct {
		colIdx int
		dt     types.DataType
	}
	keys := make([]sortKey, len(sortCols))
	for i, name := range sortCols {
		idx, ok := b.GetColumnIndex(name)
		if !ok {
			return fmt.Errorf("sort column not found: %s", name)
		}
		keys[i] = sortKey{colIdx: idx, dt: b.Columns[idx].DataType()}
	}

	n := b.NumRows()
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, z int) bool {
		for _, k := range keys {
			va := b.Columns[k.colIdx].Value(indices[a])
			vz := b.Columns[k.colIdx].Value(indices[z])
			cmp := types.CompareValues(k.dt, va, vz)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	// Reorder all columns according to sorted indices
	newCols := make([]Column, len(b.Columns))
	for i, c := range b.Columns {
		newCols[i] = Gather(c, indices)
	}
	b.Columns = newCols
	return nil
}

// SelectColumns returns a new block with only the specified columns.
func (b *Block) SelectColumns(names []string) (*Block, error) {
	cols := make([]Column, len(names))
	for i, name := range names {
		c, ok := b.GetColumn(name)
		if !ok {
			return nil, fmt.Errorf("column not found: %s", name)
		}
		cols[i] = c
	}
	namesCopy := make([]string, len(names))
	copy(namesCopy, names)
	return NewBlock(namesCopy, cols), nil
}

// FilterRowsByMask returns a new block keeping only rows where mask[i] is true.
// Uses vectorized column operations — no per-row Value/Append calls.
func (b *Block) FilterRowsByMask(mask []bool) *Block {
	cols := make([]Column, len(b.Columns))
	for i, c := range b.Columns {
		cols[i] = FilterByMask(c, mask)
	}
	names := make([]string, len(b.ColumnNames))
	copy(names, b.ColumnNames)
	return NewBlock(names, cols)
}
