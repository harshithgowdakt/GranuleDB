package storage

import (
	"bytes"
	"fmt"
	"os"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// PrimaryIndex stores primary key values at each granule boundary.
// For each granule, we store the value of each ORDER BY column at the first row.
type PrimaryIndex struct {
	NumGranules int
	KeyColumns  []string
	KeyTypes    []types.DataType
	// Values[granuleIndex][keyColumnIndex] = value
	Values [][]types.Value
}

// WritePrimaryIndex writes the primary index to a file.
func WritePrimaryIndex(path string, idx *PrimaryIndex) error {
	var buf bytes.Buffer
	for _, granuleValues := range idx.Values {
		for k, v := range granuleValues {
			if err := column.EncodeValue(&buf, idx.KeyTypes[k], v); err != nil {
				return fmt.Errorf("encoding primary index value: %w", err)
			}
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

// ReadPrimaryIndex reads the primary index from a file.
func ReadPrimaryIndex(path string, keyColumns []string, keyTypes []types.DataType, numGranules int) (*PrimaryIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	idx := &PrimaryIndex{
		NumGranules: numGranules,
		KeyColumns:  keyColumns,
		KeyTypes:    keyTypes,
		Values:      make([][]types.Value, numGranules),
	}
	for g := range numGranules {
		vals := make([]types.Value, len(keyColumns))
		for k, dt := range keyTypes {
			v, err := column.DecodeValue(r, dt)
			if err != nil {
				return nil, fmt.Errorf("reading primary index granule %d key %d: %w", g, k, err)
			}
			vals[k] = v
		}
		idx.Values[g] = vals
	}
	return idx, nil
}

