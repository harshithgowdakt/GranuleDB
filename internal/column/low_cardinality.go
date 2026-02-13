package column

import (
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// LowCardinalityColumn implements dictionary encoding for low-cardinality data.
// Unique values are stored once in Dict; rows hold uint32 indices into it.
type LowCardinalityColumn struct {
	Dict    Column                 // unique values
	Indices []uint32               // row[i] → Dict position
	lookup  map[interface{}]uint32 // value → dict index (for Append)
}

// NewLowCardinalityColumn creates an empty LC column wrapping the given inner type.
func NewLowCardinalityColumn(innerType types.DataType, capacity int) *LowCardinalityColumn {
	return &LowCardinalityColumn{
		Dict:    NewColumn(innerType),
		Indices: make([]uint32, 0, capacity),
		lookup:  make(map[interface{}]uint32),
	}
}

func (c *LowCardinalityColumn) DataType() types.DataType { return c.Dict.DataType() }
func (c *LowCardinalityColumn) Len() int                 { return len(c.Indices) }

func (c *LowCardinalityColumn) Value(i int) types.Value {
	return c.Dict.Value(int(c.Indices[i]))
}

func (c *LowCardinalityColumn) Append(v types.Value) {
	if idx, ok := c.lookup[v]; ok {
		c.Indices = append(c.Indices, idx)
		return
	}
	idx := uint32(c.Dict.Len())
	c.Dict.Append(v)
	c.lookup[v] = idx
	c.Indices = append(c.Indices, idx)
}

func (c *LowCardinalityColumn) Slice(from, to int) Column {
	newIndices := make([]uint32, to-from)
	copy(newIndices, c.Indices[from:to])
	return &LowCardinalityColumn{
		Dict:    c.Dict.Clone(),
		Indices: newIndices,
		lookup:  cloneLookup(c.lookup),
	}
}

func (c *LowCardinalityColumn) Clone() Column {
	newIndices := make([]uint32, len(c.Indices))
	copy(newIndices, c.Indices)
	return &LowCardinalityColumn{
		Dict:    c.Dict.Clone(),
		Indices: newIndices,
		lookup:  cloneLookup(c.lookup),
	}
}

// DictLen returns the number of unique values in the dictionary.
func (c *LowCardinalityColumn) DictLen() int {
	return c.Dict.Len()
}

// Materialize expands the LC column into a regular column with inline values.
func (c *LowCardinalityColumn) Materialize() Column {
	result := NewColumnWithCapacity(c.DataType(), len(c.Indices))
	for _, idx := range c.Indices {
		result.Append(c.Dict.Value(int(idx)))
	}
	return result
}

// RebuildLookup reconstructs the value→index lookup map from the dictionary.
// Used after deserialization.
func (c *LowCardinalityColumn) RebuildLookup() {
	c.lookup = make(map[interface{}]uint32, c.Dict.Len())
	for i := 0; i < c.Dict.Len(); i++ {
		c.lookup[c.Dict.Value(i)] = uint32(i)
	}
}

func cloneLookup(m map[interface{}]uint32) map[interface{}]uint32 {
	out := make(map[interface{}]uint32, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
