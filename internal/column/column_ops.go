package column

// FilterByMask returns a new column keeping only rows where mask[i] is true.
// Operates on raw typed slices — no Value/Append boxing.
func FilterByMask(col Column, mask []bool) Column {
	switch c := col.(type) {
	case *UInt8Column:
		return &UInt8Column{Data: filterSlice(c.Data, mask)}
	case *UInt16Column:
		return &UInt16Column{Data: filterSlice(c.Data, mask)}
	case *UInt32Column:
		return &UInt32Column{Data: filterSlice(c.Data, mask)}
	case *UInt64Column:
		return &UInt64Column{Data: filterSlice(c.Data, mask)}
	case *Int8Column:
		return &Int8Column{Data: filterSlice(c.Data, mask)}
	case *Int16Column:
		return &Int16Column{Data: filterSlice(c.Data, mask)}
	case *Int32Column:
		return &Int32Column{Data: filterSlice(c.Data, mask)}
	case *Int64Column:
		return &Int64Column{Data: filterSlice(c.Data, mask)}
	case *Float32Column:
		return &Float32Column{Data: filterSlice(c.Data, mask)}
	case *Float64Column:
		return &Float64Column{Data: filterSlice(c.Data, mask)}
	case *StringColumn:
		return &StringColumn{Data: filterSlice(c.Data, mask)}
	case *DateTimeColumn:
		return &DateTimeColumn{Data: filterSlice(c.Data, mask)}
	default:
		panic("FilterByMask: unsupported column type")
	}
}

// Gather returns a new column reordering rows by the given index array.
// Operates on raw typed slices — no Value/Append boxing.
func Gather(col Column, indices []int) Column {
	switch c := col.(type) {
	case *UInt8Column:
		return &UInt8Column{Data: gatherSlice(c.Data, indices)}
	case *UInt16Column:
		return &UInt16Column{Data: gatherSlice(c.Data, indices)}
	case *UInt32Column:
		return &UInt32Column{Data: gatherSlice(c.Data, indices)}
	case *UInt64Column:
		return &UInt64Column{Data: gatherSlice(c.Data, indices)}
	case *Int8Column:
		return &Int8Column{Data: gatherSlice(c.Data, indices)}
	case *Int16Column:
		return &Int16Column{Data: gatherSlice(c.Data, indices)}
	case *Int32Column:
		return &Int32Column{Data: gatherSlice(c.Data, indices)}
	case *Int64Column:
		return &Int64Column{Data: gatherSlice(c.Data, indices)}
	case *Float32Column:
		return &Float32Column{Data: gatherSlice(c.Data, indices)}
	case *Float64Column:
		return &Float64Column{Data: gatherSlice(c.Data, indices)}
	case *StringColumn:
		return &StringColumn{Data: gatherSlice(c.Data, indices)}
	case *DateTimeColumn:
		return &DateTimeColumn{Data: gatherSlice(c.Data, indices)}
	default:
		panic("Gather: unsupported column type")
	}
}

// AppendColumn bulk-appends all rows from src onto dst.
// Both must be the same concrete type. Operates on raw typed slices.
func AppendColumn(dst, src Column) {
	switch d := dst.(type) {
	case *UInt8Column:
		d.Data = appendSlice(d.Data, src.(*UInt8Column).Data)
	case *UInt16Column:
		d.Data = appendSlice(d.Data, src.(*UInt16Column).Data)
	case *UInt32Column:
		d.Data = appendSlice(d.Data, src.(*UInt32Column).Data)
	case *UInt64Column:
		d.Data = appendSlice(d.Data, src.(*UInt64Column).Data)
	case *Int8Column:
		d.Data = appendSlice(d.Data, src.(*Int8Column).Data)
	case *Int16Column:
		d.Data = appendSlice(d.Data, src.(*Int16Column).Data)
	case *Int32Column:
		d.Data = appendSlice(d.Data, src.(*Int32Column).Data)
	case *Int64Column:
		d.Data = appendSlice(d.Data, src.(*Int64Column).Data)
	case *Float32Column:
		d.Data = appendSlice(d.Data, src.(*Float32Column).Data)
	case *Float64Column:
		d.Data = appendSlice(d.Data, src.(*Float64Column).Data)
	case *StringColumn:
		d.Data = appendSlice(d.Data, src.(*StringColumn).Data)
	case *DateTimeColumn:
		d.Data = appendSlice(d.Data, src.(*DateTimeColumn).Data)
	default:
		panic("AppendColumn: unsupported column type")
	}
}
