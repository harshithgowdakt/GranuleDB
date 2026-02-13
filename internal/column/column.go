package column

import (
	"github.com/harshithgowda/goose-db/internal/types"
)

// Column is an in-memory columnar array of a single type.
type Column interface {
	DataType() types.DataType
	Len() int
	Value(i int) types.Value
	Append(v types.Value)
	Slice(from, to int) Column
	Clone() Column
}

// NewColumn creates an empty column of the given type.
func NewColumn(dt types.DataType) Column {
	return NewColumnWithCapacity(dt, 0)
}

// NewColumnWithCapacity creates a column pre-allocated for n rows.
func NewColumnWithCapacity(dt types.DataType, n int) Column {
	switch dt {
	case types.TypeUInt8:
		return &UInt8Column{Data: make([]uint8, 0, n)}
	case types.TypeUInt16:
		return &UInt16Column{Data: make([]uint16, 0, n)}
	case types.TypeUInt32:
		return &UInt32Column{Data: make([]uint32, 0, n)}
	case types.TypeUInt64:
		return &UInt64Column{Data: make([]uint64, 0, n)}
	case types.TypeInt8:
		return &Int8Column{Data: make([]int8, 0, n)}
	case types.TypeInt16:
		return &Int16Column{Data: make([]int16, 0, n)}
	case types.TypeInt32:
		return &Int32Column{Data: make([]int32, 0, n)}
	case types.TypeInt64:
		return &Int64Column{Data: make([]int64, 0, n)}
	case types.TypeFloat32:
		return &Float32Column{Data: make([]float32, 0, n)}
	case types.TypeFloat64:
		return &Float64Column{Data: make([]float64, 0, n)}
	case types.TypeString:
		return &StringColumn{Data: make([]string, 0, n)}
	case types.TypeDateTime:
		return &DateTimeColumn{Data: make([]uint32, 0, n)}
	default:
		panic("unsupported data type")
	}
}

// --- UInt8Column ---

type UInt8Column struct{ Data []uint8 }

func (c *UInt8Column) DataType() types.DataType { return types.TypeUInt8 }
func (c *UInt8Column) Len() int                 { return len(c.Data) }
func (c *UInt8Column) Value(i int) types.Value  { return c.Data[i] }
func (c *UInt8Column) Append(v types.Value)     { c.Data = append(c.Data, v.(uint8)) }
func (c *UInt8Column) Slice(from, to int) Column {
	d := make([]uint8, to-from)
	copy(d, c.Data[from:to])
	return &UInt8Column{Data: d}
}
func (c *UInt8Column) Clone() Column {
	d := make([]uint8, len(c.Data))
	copy(d, c.Data)
	return &UInt8Column{Data: d}
}

// --- UInt16Column ---

type UInt16Column struct{ Data []uint16 }

func (c *UInt16Column) DataType() types.DataType { return types.TypeUInt16 }
func (c *UInt16Column) Len() int                 { return len(c.Data) }
func (c *UInt16Column) Value(i int) types.Value  { return c.Data[i] }
func (c *UInt16Column) Append(v types.Value)     { c.Data = append(c.Data, v.(uint16)) }
func (c *UInt16Column) Slice(from, to int) Column {
	d := make([]uint16, to-from)
	copy(d, c.Data[from:to])
	return &UInt16Column{Data: d}
}
func (c *UInt16Column) Clone() Column {
	d := make([]uint16, len(c.Data))
	copy(d, c.Data)
	return &UInt16Column{Data: d}
}

// --- UInt32Column ---

type UInt32Column struct{ Data []uint32 }

func (c *UInt32Column) DataType() types.DataType { return types.TypeUInt32 }
func (c *UInt32Column) Len() int                 { return len(c.Data) }
func (c *UInt32Column) Value(i int) types.Value  { return c.Data[i] }
func (c *UInt32Column) Append(v types.Value)     { c.Data = append(c.Data, v.(uint32)) }
func (c *UInt32Column) Slice(from, to int) Column {
	d := make([]uint32, to-from)
	copy(d, c.Data[from:to])
	return &UInt32Column{Data: d}
}
func (c *UInt32Column) Clone() Column {
	d := make([]uint32, len(c.Data))
	copy(d, c.Data)
	return &UInt32Column{Data: d}
}

// --- UInt64Column ---

type UInt64Column struct{ Data []uint64 }

func (c *UInt64Column) DataType() types.DataType { return types.TypeUInt64 }
func (c *UInt64Column) Len() int                 { return len(c.Data) }
func (c *UInt64Column) Value(i int) types.Value  { return c.Data[i] }
func (c *UInt64Column) Append(v types.Value)     { c.Data = append(c.Data, v.(uint64)) }
func (c *UInt64Column) Slice(from, to int) Column {
	d := make([]uint64, to-from)
	copy(d, c.Data[from:to])
	return &UInt64Column{Data: d}
}
func (c *UInt64Column) Clone() Column {
	d := make([]uint64, len(c.Data))
	copy(d, c.Data)
	return &UInt64Column{Data: d}
}

// --- Int8Column ---

type Int8Column struct{ Data []int8 }

func (c *Int8Column) DataType() types.DataType { return types.TypeInt8 }
func (c *Int8Column) Len() int                 { return len(c.Data) }
func (c *Int8Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Int8Column) Append(v types.Value)     { c.Data = append(c.Data, v.(int8)) }
func (c *Int8Column) Slice(from, to int) Column {
	d := make([]int8, to-from)
	copy(d, c.Data[from:to])
	return &Int8Column{Data: d}
}
func (c *Int8Column) Clone() Column {
	d := make([]int8, len(c.Data))
	copy(d, c.Data)
	return &Int8Column{Data: d}
}

// --- Int16Column ---

type Int16Column struct{ Data []int16 }

func (c *Int16Column) DataType() types.DataType { return types.TypeInt16 }
func (c *Int16Column) Len() int                 { return len(c.Data) }
func (c *Int16Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Int16Column) Append(v types.Value)     { c.Data = append(c.Data, v.(int16)) }
func (c *Int16Column) Slice(from, to int) Column {
	d := make([]int16, to-from)
	copy(d, c.Data[from:to])
	return &Int16Column{Data: d}
}
func (c *Int16Column) Clone() Column {
	d := make([]int16, len(c.Data))
	copy(d, c.Data)
	return &Int16Column{Data: d}
}

// --- Int32Column ---

type Int32Column struct{ Data []int32 }

func (c *Int32Column) DataType() types.DataType { return types.TypeInt32 }
func (c *Int32Column) Len() int                 { return len(c.Data) }
func (c *Int32Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Int32Column) Append(v types.Value)     { c.Data = append(c.Data, v.(int32)) }
func (c *Int32Column) Slice(from, to int) Column {
	d := make([]int32, to-from)
	copy(d, c.Data[from:to])
	return &Int32Column{Data: d}
}
func (c *Int32Column) Clone() Column {
	d := make([]int32, len(c.Data))
	copy(d, c.Data)
	return &Int32Column{Data: d}
}

// --- Int64Column ---

type Int64Column struct{ Data []int64 }

func (c *Int64Column) DataType() types.DataType { return types.TypeInt64 }
func (c *Int64Column) Len() int                 { return len(c.Data) }
func (c *Int64Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Int64Column) Append(v types.Value)     { c.Data = append(c.Data, v.(int64)) }
func (c *Int64Column) Slice(from, to int) Column {
	d := make([]int64, to-from)
	copy(d, c.Data[from:to])
	return &Int64Column{Data: d}
}
func (c *Int64Column) Clone() Column {
	d := make([]int64, len(c.Data))
	copy(d, c.Data)
	return &Int64Column{Data: d}
}

// --- Float32Column ---

type Float32Column struct{ Data []float32 }

func (c *Float32Column) DataType() types.DataType { return types.TypeFloat32 }
func (c *Float32Column) Len() int                 { return len(c.Data) }
func (c *Float32Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Float32Column) Append(v types.Value)     { c.Data = append(c.Data, v.(float32)) }
func (c *Float32Column) Slice(from, to int) Column {
	d := make([]float32, to-from)
	copy(d, c.Data[from:to])
	return &Float32Column{Data: d}
}
func (c *Float32Column) Clone() Column {
	d := make([]float32, len(c.Data))
	copy(d, c.Data)
	return &Float32Column{Data: d}
}

// --- Float64Column ---

type Float64Column struct{ Data []float64 }

func (c *Float64Column) DataType() types.DataType { return types.TypeFloat64 }
func (c *Float64Column) Len() int                 { return len(c.Data) }
func (c *Float64Column) Value(i int) types.Value  { return c.Data[i] }
func (c *Float64Column) Append(v types.Value)     { c.Data = append(c.Data, v.(float64)) }
func (c *Float64Column) Slice(from, to int) Column {
	d := make([]float64, to-from)
	copy(d, c.Data[from:to])
	return &Float64Column{Data: d}
}
func (c *Float64Column) Clone() Column {
	d := make([]float64, len(c.Data))
	copy(d, c.Data)
	return &Float64Column{Data: d}
}

// --- StringColumn ---

type StringColumn struct{ Data []string }

func (c *StringColumn) DataType() types.DataType { return types.TypeString }
func (c *StringColumn) Len() int                 { return len(c.Data) }
func (c *StringColumn) Value(i int) types.Value  { return c.Data[i] }
func (c *StringColumn) Append(v types.Value)     { c.Data = append(c.Data, v.(string)) }
func (c *StringColumn) Slice(from, to int) Column {
	d := make([]string, to-from)
	copy(d, c.Data[from:to])
	return &StringColumn{Data: d}
}
func (c *StringColumn) Clone() Column {
	d := make([]string, len(c.Data))
	copy(d, c.Data)
	return &StringColumn{Data: d}
}

// --- DateTimeColumn ---

type DateTimeColumn struct{ Data []uint32 }

func (c *DateTimeColumn) DataType() types.DataType { return types.TypeDateTime }
func (c *DateTimeColumn) Len() int                 { return len(c.Data) }
func (c *DateTimeColumn) Value(i int) types.Value  { return c.Data[i] }
func (c *DateTimeColumn) Append(v types.Value)     { c.Data = append(c.Data, v.(uint32)) }
func (c *DateTimeColumn) Slice(from, to int) Column {
	d := make([]uint32, to-from)
	copy(d, c.Data[from:to])
	return &DateTimeColumn{Data: d}
}
func (c *DateTimeColumn) Clone() Column {
	d := make([]uint32, len(c.Data))
	copy(d, c.Data)
	return &DateTimeColumn{Data: d}
}
