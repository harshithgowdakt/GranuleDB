package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/types"
)

// PartReader reads data from a part on disk.
type PartReader struct {
	part   *Part
	schema *TableSchema
}

// NewPartReader creates a new PartReader.
func NewPartReader(part *Part, schema *TableSchema) *PartReader {
	return &PartReader{part: part, schema: schema}
}

// ReadAll reads all columns for all granules.
func (pr *PartReader) ReadAll(columnNames []string) (*column.Block, error) {
	numGranules := pr.part.NumGranules
	if numGranules == 0 {
		// Try to compute from count.txt and granule size
		numRows, err := pr.readRowCount()
		if err != nil {
			return nil, err
		}
		granuleSize := pr.schema.EffectiveGranuleSize()
		numGranules = (numRows + granuleSize - 1) / granuleSize
	}
	return pr.ReadColumns(columnNames, 0, numGranules)
}

// ReadColumns reads specific columns for granules in range [granuleBegin, granuleEnd).
func (pr *PartReader) ReadColumns(columnNames []string, granuleBegin, granuleEnd int) (*column.Block, error) {
	if granuleBegin >= granuleEnd {
		// Return empty block
		cols := make([]column.Column, len(columnNames))
		for i, name := range columnNames {
			colDef, ok := pr.schema.GetColumnDef(name)
			if !ok {
				return nil, fmt.Errorf("column %s not in schema", name)
			}
			cols[i] = column.NewColumn(colDef.DataType)
		}
		return column.NewBlock(columnNames, cols), nil
	}

	cols := make([]column.Column, len(columnNames))
	for i, name := range columnNames {
		colDef, ok := pr.schema.GetColumnDef(name)
		if !ok {
			return nil, fmt.Errorf("column %s not in schema", name)
		}

		col, err := pr.readColumnGranules(name, colDef.DataType, granuleBegin, granuleEnd)
		if err != nil {
			return nil, fmt.Errorf("reading column %s: %w", name, err)
		}
		cols[i] = col
	}

	return column.NewBlock(columnNames, cols), nil
}

// readColumnGranules reads granules [begin, end) for a single column.
func (pr *PartReader) readColumnGranules(colName string, dt types.DataType, granuleBegin, granuleEnd int) (column.Column, error) {
	// Load mark file
	mrkPath := filepath.Join(pr.part.BasePath, colName+".mrk")
	marks, err := ReadMarksFromFile(mrkPath)
	if err != nil {
		return nil, fmt.Errorf("reading marks: %w", err)
	}

	// Load .bin file
	binPath := filepath.Join(pr.part.BasePath, colName+".bin")
	binData, err := os.ReadFile(binPath)
	if err != nil {
		return nil, fmt.Errorf("reading bin file: %w", err)
	}

	// Compute row count and granule sizes
	totalRows := int(pr.part.NumRows)
	granuleSize := pr.schema.EffectiveGranuleSize()

	// Read each granule
	result := column.NewColumn(dt)
	for g := granuleBegin; g < granuleEnd; g++ {
		if g >= len(marks) {
			break
		}

		mark := marks[g]
		offset := int(mark.OffsetInCompressedFile)

		if offset >= len(binData) {
			break
		}

		// Read the compressed block header to determine block size
		blockData := binData[offset:]
		compressedTotal, _, err := compression.ReadBlockHeader(blockData)
		if err != nil {
			return nil, fmt.Errorf("reading block header at granule %d: %w", g, err)
		}

		// Decompress the block
		decompressed, err := compression.DecompressBlock(blockData[:compressedTotal])
		if err != nil {
			return nil, fmt.Errorf("decompressing granule %d: %w", g, err)
		}

		// Calculate number of rows in this granule
		rowsInGranule := granuleSize
		startRow := g * granuleSize
		if startRow+rowsInGranule > totalRows {
			rowsInGranule = totalRows - startRow
		}

		// Decode column data
		var col column.Column
		colDef2, _ := pr.schema.GetColumnDef(colName)
		if colDef2.IsLowCardinality {
			col, err = column.DecodeLCColumn(dt, decompressed, rowsInGranule)
		} else {
			col, err = column.DecodeColumn(dt, decompressed, rowsInGranule)
		}
		if err != nil {
			return nil, fmt.Errorf("decoding granule %d: %w", g, err)
		}

		// Append to result
		for j := 0; j < col.Len(); j++ {
			result.Append(col.Value(j))
		}
	}

	return result, nil
}

// LoadPrimaryIndex loads the primary index for this part.
func (pr *PartReader) LoadPrimaryIndex() (*PrimaryIndex, error) {
	idxPath := filepath.Join(pr.part.BasePath, "primary.idx")

	keyTypes := make([]types.DataType, len(pr.schema.OrderBy))
	for i, keyName := range pr.schema.OrderBy {
		colDef, ok := pr.schema.GetColumnDef(keyName)
		if !ok {
			return nil, fmt.Errorf("ORDER BY column %s not in schema", keyName)
		}
		keyTypes[i] = colDef.DataType
	}

	numGranules := pr.part.NumGranules
	if numGranules == 0 {
		numRows, err := pr.readRowCount()
		if err != nil {
			return nil, err
		}
		granuleSize := pr.schema.EffectiveGranuleSize()
		numGranules = (numRows + granuleSize - 1) / granuleSize
	}

	return ReadPrimaryIndex(idxPath, pr.schema.OrderBy, keyTypes, numGranules)
}

// LoadMinMaxIndex loads the min-max index for the partition key column.
func (pr *PartReader) LoadMinMaxIndex() (*MinMaxIndex, error) {
	if pr.schema.PartitionBy == "" {
		return nil, nil
	}
	colDef, ok := pr.schema.GetColumnDef(pr.schema.PartitionBy)
	if !ok {
		return nil, fmt.Errorf("partition column %s not in schema", pr.schema.PartitionBy)
	}
	path := filepath.Join(pr.part.BasePath, "minmax_"+pr.schema.PartitionBy+".idx")
	return ReadMinMaxIndex(path, pr.schema.PartitionBy, colDef.DataType)
}

// readRowCount reads the row count from count.txt.
func (pr *PartReader) readRowCount() (int, error) {
	data, err := os.ReadFile(filepath.Join(pr.part.BasePath, "count.txt"))
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parsing count.txt: %w", err)
	}
	return n, nil
}
