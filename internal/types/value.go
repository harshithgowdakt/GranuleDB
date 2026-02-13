package types

import "fmt"

// Value represents a single database value. Concrete types use native Go types:
//   UInt8 -> uint8, UInt16 -> uint16, ..., String -> string, DateTime -> uint32
type Value = interface{}

// ToFloat64 converts a numeric value to float64 for arithmetic.
func ToFloat64(dt DataType, v Value) (float64, error) {
	switch dt {
	case TypeUInt8:
		return float64(v.(uint8)), nil
	case TypeUInt16:
		return float64(v.(uint16)), nil
	case TypeUInt32:
		return float64(v.(uint32)), nil
	case TypeUInt64:
		return float64(v.(uint64)), nil
	case TypeInt8:
		return float64(v.(int8)), nil
	case TypeInt16:
		return float64(v.(int16)), nil
	case TypeInt32:
		return float64(v.(int32)), nil
	case TypeInt64:
		return float64(v.(int64)), nil
	case TypeFloat32:
		return float64(v.(float32)), nil
	case TypeFloat64:
		return v.(float64), nil
	case TypeDateTime:
		return float64(v.(uint32)), nil
	default:
		return 0, fmt.Errorf("cannot convert %s to float64", dt.Name())
	}
}

// ToInt64 converts a numeric value to int64.
func ToInt64(dt DataType, v Value) (int64, error) {
	switch dt {
	case TypeUInt8:
		return int64(v.(uint8)), nil
	case TypeUInt16:
		return int64(v.(uint16)), nil
	case TypeUInt32:
		return int64(v.(uint32)), nil
	case TypeUInt64:
		return int64(v.(uint64)), nil
	case TypeInt8:
		return int64(v.(int8)), nil
	case TypeInt16:
		return int64(v.(int16)), nil
	case TypeInt32:
		return int64(v.(int32)), nil
	case TypeInt64:
		return v.(int64), nil
	case TypeFloat32:
		return int64(v.(float32)), nil
	case TypeFloat64:
		return int64(v.(float64)), nil
	case TypeDateTime:
		return int64(v.(uint32)), nil
	default:
		return 0, fmt.Errorf("cannot convert %s to int64", dt.Name())
	}
}

// CompareValues compares two values of the same DataType.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareValues(dt DataType, a, b Value) int {
	switch dt {
	case TypeUInt8:
		return cmpOrdered(a.(uint8), b.(uint8))
	case TypeUInt16:
		return cmpOrdered(a.(uint16), b.(uint16))
	case TypeUInt32:
		return cmpOrdered(a.(uint32), b.(uint32))
	case TypeUInt64:
		return cmpOrdered(a.(uint64), b.(uint64))
	case TypeInt8:
		return cmpOrdered(a.(int8), b.(int8))
	case TypeInt16:
		return cmpOrdered(a.(int16), b.(int16))
	case TypeInt32:
		return cmpOrdered(a.(int32), b.(int32))
	case TypeInt64:
		return cmpOrdered(a.(int64), b.(int64))
	case TypeFloat32:
		return cmpOrdered(a.(float32), b.(float32))
	case TypeFloat64:
		return cmpOrdered(a.(float64), b.(float64))
	case TypeString:
		return cmpOrdered(a.(string), b.(string))
	case TypeDateTime:
		return cmpOrdered(a.(uint32), b.(uint32))
	default:
		return 0
	}
}

type ordered interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~int8 | ~int16 | ~int32 | ~int64 |
		~float32 | ~float64 | ~string
}

func cmpOrdered[T ordered](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// ValueToString converts a value to its string representation.
func ValueToString(dt DataType, v Value) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
