package storage

import (
	"bytes"
	"fmt"
	"os"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/types"
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

// FindGranuleRange returns the range of granule indices [begin, end) that could
// match a condition: keyCol >= minVal AND keyCol < maxVal.
// If minVal is nil, no lower bound. If maxVal is nil, no upper bound.
// This does a simple scan (could be binary search for large indexes).
func (idx *PrimaryIndex) FindGranuleRange(keyCol string, minVal, maxVal types.Value) (int, int) {
	// Find which key column index this is
	keyIdx := -1
	var keyDt types.DataType
	for i, name := range idx.KeyColumns {
		if name == keyCol {
			keyIdx = i
			keyDt = idx.KeyTypes[i]
			break
		}
	}
	if keyIdx < 0 {
		// Column not in primary key, return all granules
		return 0, idx.NumGranules
	}

	begin := 0
	end := idx.NumGranules

	if minVal != nil {
		// Find first granule that could contain rows >= minVal.
		// Granule g could contain matching rows if the next granule's boundary > minVal
		// (or if it's the last granule).
		for g := 0; g < idx.NumGranules; g++ {
			// Check if the NEXT granule's boundary value is still <= minVal
			// If so, this granule can't contain any rows >= minVal
			if g+1 < idx.NumGranules {
				nextBoundary := idx.Values[g+1][keyIdx]
				if types.CompareValues(keyDt, nextBoundary, minVal) <= 0 {
					begin = g + 1
					continue
				}
			}
			begin = g
			break
		}
	}

	if maxVal != nil {
		// Find last granule that could contain rows < maxVal.
		for g := idx.NumGranules - 1; g >= 0; g-- {
			boundary := idx.Values[g][keyIdx]
			if types.CompareValues(keyDt, boundary, maxVal) >= 0 {
				end = g
				continue
			}
			end = g + 1
			break
		}
	}

	if begin > end {
		begin = end
	}
	return begin, end
}
