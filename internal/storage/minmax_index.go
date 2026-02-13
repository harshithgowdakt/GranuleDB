package storage

import (
	"bytes"
	"os"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/types"
)

// MinMaxIndex stores the min and max values for a column within a part.
type MinMaxIndex struct {
	ColumnName string
	DataType   types.DataType
	Min        types.Value
	Max        types.Value
}

// WriteMinMaxIndex writes the min-max index to a file.
func WriteMinMaxIndex(path string, idx *MinMaxIndex) error {
	var buf bytes.Buffer
	if err := column.EncodeValue(&buf, idx.DataType, idx.Min); err != nil {
		return err
	}
	if err := column.EncodeValue(&buf, idx.DataType, idx.Max); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

// ReadMinMaxIndex reads the min-max index from a file.
func ReadMinMaxIndex(path string, colName string, dt types.DataType) (*MinMaxIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	minVal, err := column.DecodeValue(r, dt)
	if err != nil {
		return nil, err
	}
	maxVal, err := column.DecodeValue(r, dt)
	if err != nil {
		return nil, err
	}
	return &MinMaxIndex{
		ColumnName: colName,
		DataType:   dt,
		Min:        minVal,
		Max:        maxVal,
	}, nil
}

// ComputeMinMax computes min and max values from a column.
func ComputeMinMax(col column.Column) (min, max types.Value) {
	if col.Len() == 0 {
		return nil, nil
	}
	dt := col.DataType()
	min = col.Value(0)
	max = col.Value(0)
	for i := 1; i < col.Len(); i++ {
		v := col.Value(i)
		if types.CompareValues(dt, v, min) < 0 {
			min = v
		}
		if types.CompareValues(dt, v, max) > 0 {
			max = v
		}
	}
	return min, max
}
