package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/types"
)

// PartWriter creates a new part on disk from a Block.
type PartWriter struct {
	schema  *TableSchema
	baseDir string // parent directory (table data dir)
	codec   compression.Codec
}

// NewPartWriter creates a new PartWriter.
func NewPartWriter(schema *TableSchema, baseDir string, codec compression.Codec) *PartWriter {
	return &PartWriter{
		schema:  schema,
		baseDir: baseDir,
		codec:   codec,
	}
}

// WritePart writes a block as a new part directory.
// The block must already be sorted by ORDER BY columns and belong to a single partition.
func (pw *PartWriter) WritePart(block *column.Block, info PartInfo) (*Part, error) {
	tmpDir := filepath.Join(pw.baseDir, info.TmpDirName())
	finalDir := filepath.Join(pw.baseDir, info.DirName())

	// Create temporary directory
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("creating tmp dir: %w", err)
	}

	// Clean up on failure
	success := false
	defer func() {
		if !success {
			os.RemoveAll(tmpDir)
		}
	}()

	numRows := block.NumRows()
	granuleSize := pw.schema.EffectiveGranuleSize()
	granules := SplitIntoGranules(numRows, granuleSize)
	numGranules := len(granules)

	// Write each column's .bin and .mrk files
	for _, colDef := range pw.schema.Columns {
		col, ok := block.GetColumn(colDef.Name)
		if !ok {
			return nil, fmt.Errorf("column %s not found in block", colDef.Name)
		}

		if err := pw.writeColumn(tmpDir, colDef.Name, col, granules); err != nil {
			return nil, fmt.Errorf("writing column %s: %w", colDef.Name, err)
		}
	}

	// Write primary index
	if err := pw.writePrimaryIndex(tmpDir, block, granules); err != nil {
		return nil, fmt.Errorf("writing primary index: %w", err)
	}

	// Write min-max index for partition key column (only for simple column references).
	if pw.schema.PartitionBy != "" {
		if _, isCol := pw.schema.GetColumnDef(pw.schema.PartitionBy); isCol {
			if err := pw.writeMinMaxIndex(tmpDir, block); err != nil {
				return nil, fmt.Errorf("writing minmax index: %w", err)
			}
		}
	}

	// Write count.txt
	if err := os.WriteFile(filepath.Join(tmpDir, "count.txt"),
		[]byte(strconv.FormatInt(int64(numRows), 10)+"\n"), 0644); err != nil {
		return nil, err
	}

	// Write columns.txt
	if err := pw.writeColumnsFile(tmpDir); err != nil {
		return nil, err
	}

	// Atomic rename from tmp to final
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return nil, fmt.Errorf("renaming part dir: %w", err)
	}

	success = true

	// Calculate total size
	var totalSize uint64
	filepath.Walk(finalDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += uint64(info.Size())
		}
		return nil
	})

	return &Part{
		Info:        info,
		State:       PartActive,
		NumRows:     uint64(numRows),
		SizeBytes:   totalSize,
		CreatedAt:   time.Now(),
		BasePath:    finalDir,
		NumGranules: numGranules,
	}, nil
}

// writeColumn writes a single column's .bin and .mrk files.
func (pw *PartWriter) writeColumn(dir, colName string, col column.Column, granules []GranuleRange) error {
	binPath := filepath.Join(dir, colName+".bin")
	mrkPath := filepath.Join(dir, colName+".mrk")

	binFile, err := os.Create(binPath)
	if err != nil {
		return err
	}
	defer binFile.Close()

	marks := make([]Mark, 0, len(granules))
	var compressedOffset uint64

	for _, g := range granules {
		subCol := col.Slice(g.Start, g.End)

		// Encode column data to raw bytes
		rawBytes, err := column.EncodeColumn(subCol)
		if err != nil {
			return fmt.Errorf("encoding granule: %w", err)
		}

		// Record mark BEFORE writing compressed block
		marks = append(marks, Mark{
			OffsetInCompressedFile:    compressedOffset,
			OffsetInDecompressedBlock: 0,
		})

		// Compress and write
		compressedBlock, err := compression.CompressBlock(pw.codec, rawBytes)
		if err != nil {
			return fmt.Errorf("compressing granule: %w", err)
		}

		if _, err := binFile.Write(compressedBlock); err != nil {
			return err
		}
		compressedOffset += uint64(len(compressedBlock))
	}

	// Write mark file
	mrkFile, err := os.Create(mrkPath)
	if err != nil {
		return err
	}
	defer mrkFile.Close()

	return WriteMarks(mrkFile, marks)
}

// writePrimaryIndex writes the primary.idx file.
func (pw *PartWriter) writePrimaryIndex(dir string, block *column.Block, granules []GranuleRange) error {
	idx := &PrimaryIndex{
		NumGranules: len(granules),
		KeyColumns:  pw.schema.OrderBy,
		KeyTypes:    make([]types.DataType, len(pw.schema.OrderBy)),
		Values:      make([][]types.Value, len(granules)),
	}

	for k, keyName := range pw.schema.OrderBy {
		colDef, ok := pw.schema.GetColumnDef(keyName)
		if !ok {
			return fmt.Errorf("ORDER BY column %s not in schema", keyName)
		}
		idx.KeyTypes[k] = colDef.DataType
	}

	for g, gran := range granules {
		vals := make([]types.Value, len(pw.schema.OrderBy))
		for k, keyName := range pw.schema.OrderBy {
			col, _ := block.GetColumn(keyName)
			vals[k] = col.Value(gran.Start)
		}
		idx.Values[g] = vals
	}

	return WritePrimaryIndex(filepath.Join(dir, "primary.idx"), idx)
}

// writeMinMaxIndex writes minmax_<col>.idx for the partition key column.
func (pw *PartWriter) writeMinMaxIndex(dir string, block *column.Block) error {
	col, ok := block.GetColumn(pw.schema.PartitionBy)
	if !ok {
		return fmt.Errorf("partition column %s not found", pw.schema.PartitionBy)
	}
	colDef, _ := pw.schema.GetColumnDef(pw.schema.PartitionBy)
	minVal, maxVal := ComputeMinMax(col)
	idx := &MinMaxIndex{
		ColumnName: pw.schema.PartitionBy,
		DataType:   colDef.DataType,
		Min:        minVal,
		Max:        maxVal,
	}
	return WriteMinMaxIndex(filepath.Join(dir, "minmax_"+pw.schema.PartitionBy+".idx"), idx)
}

// writeColumnsFile writes columns.txt with column names and types.
func (pw *PartWriter) writeColumnsFile(dir string) error {
	var sb strings.Builder
	for _, c := range pw.schema.Columns {
		sb.WriteString(c.Name)
		sb.WriteByte('\t')
		sb.WriteString(c.DataType.Name())
		sb.WriteByte('\n')
	}
	return os.WriteFile(filepath.Join(dir, "columns.txt"), []byte(sb.String()), 0644)
}
