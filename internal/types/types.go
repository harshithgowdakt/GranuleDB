package types

import (
	"fmt"
	"strings"
)

// DataType represents a column data type.
type DataType uint8

const (
	TypeUInt8 DataType = iota
	TypeUInt16
	TypeUInt32
	TypeUInt64
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeDateTime       // stored as uint32 unix timestamp
	TypeAggregateState // opaque binary aggregate state payload
)

// TypeInfo holds metadata about a data type.
type TypeInfo struct {
	Type      DataType
	Name      string
	FixedSize int // bytes per value; 0 for variable-length (String)
}

var typeInfoList = []TypeInfo{
	{TypeUInt8, "UInt8", 1},
	{TypeUInt16, "UInt16", 2},
	{TypeUInt32, "UInt32", 4},
	{TypeUInt64, "UInt64", 8},
	{TypeInt8, "Int8", 1},
	{TypeInt16, "Int16", 2},
	{TypeInt32, "Int32", 4},
	{TypeInt64, "Int64", 8},
	{TypeFloat32, "Float32", 4},
	{TypeFloat64, "Float64", 8},
	{TypeString, "String", 0},
	{TypeDateTime, "DateTime", 4},
	{TypeAggregateState, "AggregateState", 0},
}

// UniqStatePrefix is the marker used for serialized uniq aggregate state values.
const UniqStatePrefix = "__goose_uniq_state__:"

// TypeInfoMap maps DataType to its TypeInfo.
var TypeInfoMap map[DataType]TypeInfo

// typeNameMap maps lowercase type name to DataType for parsing.
var typeNameMap map[string]DataType

func init() {
	TypeInfoMap = make(map[DataType]TypeInfo, len(typeInfoList))
	typeNameMap = make(map[string]DataType, len(typeInfoList))
	for _, ti := range typeInfoList {
		TypeInfoMap[ti.Type] = ti
		typeNameMap[strings.ToLower(ti.Name)] = ti.Type
	}
}

// ParseDataType converts a type name string (case-insensitive) to DataType.
func ParseDataType(name string) (DataType, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "aggregatefunction" || strings.HasPrefix(n, "aggregatefunction(") {
		return TypeAggregateState, nil
	}
	dt, ok := typeNameMap[n]
	if !ok {
		return 0, fmt.Errorf("unknown data type: %s", name)
	}
	return dt, nil
}

// ParseColumnType parses a column type that may be wrapped in LowCardinality(...).
// Returns (innerType, isLowCardinality, error).
func ParseColumnType(name string) (DataType, bool, error) {
	lower := strings.ToLower(strings.TrimSpace(name))
	if strings.HasPrefix(lower, "lowcardinality(") && strings.HasSuffix(lower, ")") {
		inner := name[len("lowcardinality(") : len(name)-1]
		inner = strings.TrimSpace(inner)
		dt, err := ParseDataType(inner)
		if err != nil {
			return 0, false, fmt.Errorf("LowCardinality inner type: %w", err)
		}
		return dt, true, nil
	}
	dt, err := ParseDataType(name)
	return dt, false, err
}

// Name returns the string name of the DataType.
func (dt DataType) Name() string {
	if ti, ok := TypeInfoMap[dt]; ok {
		return ti.Name
	}
	return "Unknown"
}

// FixedSize returns the byte size for fixed-size types, 0 for variable-length.
func (dt DataType) FixedSize() int {
	if ti, ok := TypeInfoMap[dt]; ok {
		return ti.FixedSize
	}
	return 0
}

// IsNumeric returns true for integer and float types.
func (dt DataType) IsNumeric() bool {
	switch dt {
	case TypeUInt8, TypeUInt16, TypeUInt32, TypeUInt64,
		TypeInt8, TypeInt16, TypeInt32, TypeInt64,
		TypeFloat32, TypeFloat64, TypeDateTime:
		return true
	}
	return false
}

// IsInteger returns true for integer types (not float).
func (dt DataType) IsInteger() bool {
	switch dt {
	case TypeUInt8, TypeUInt16, TypeUInt32, TypeUInt64,
		TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeDateTime:
		return true
	}
	return false
}
