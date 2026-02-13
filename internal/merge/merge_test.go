package merge_test

import (
	"os"
	"testing"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/merge"
	"github.com/harshithgowda/goose-db/internal/storage"
	"github.com/harshithgowda/goose-db/internal/types"
)

func TestMergeExecution(t *testing.T) {
	dir, err := os.MkdirTemp("", "goosedb-merge-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema := &storage.TableSchema{
		Columns: []storage.ColumnDef{
			{Name: "id", DataType: types.TypeUInt64},
			{Name: "value", DataType: types.TypeInt64},
		},
		OrderBy: []string{"id"},
	}

	codec := &compression.LZ4Codec{}
	writer := storage.NewPartWriter(schema, dir, codec)

	// Create 3 small parts
	var parts []*storage.Part
	for i := 0; i < 3; i++ {
		idCol := &column.UInt64Column{Data: []uint64{uint64(i*3 + 1), uint64(i*3 + 2), uint64(i*3 + 3)}}
		valCol := &column.Int64Column{Data: []int64{int64((i + 1) * 100), int64((i + 1) * 200), int64((i + 1) * 300)}}
		block := column.NewBlock([]string{"id", "value"}, []column.Column{idCol, valCol})

		info := storage.PartInfo{
			PartitionID: "all",
			MinBlock:    uint64(i + 1),
			MaxBlock:    uint64(i + 1),
			Level:       0,
		}

		part, err := writer.WritePart(block, info)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}

	// Merge
	executor := merge.NewMergeExecutor(schema, codec)
	merged, err := executor.Merge(dir, parts)
	if err != nil {
		t.Fatal(err)
	}

	if merged.NumRows != 9 {
		t.Fatalf("expected 9 merged rows, got %d", merged.NumRows)
	}

	if merged.Info.Level != 1 {
		t.Fatalf("expected level 1, got %d", merged.Info.Level)
	}

	// Read merged part and verify data
	reader := storage.NewPartReader(merged, schema)
	block, err := reader.ReadAll([]string{"id", "value"})
	if err != nil {
		t.Fatal(err)
	}

	if block.NumRows() != 9 {
		t.Fatalf("expected 9 rows from merged part, got %d", block.NumRows())
	}

	// Verify data is sorted by id
	idCol, _ := block.GetColumn("id")
	for i := 1; i < idCol.Len(); i++ {
		prev := idCol.Value(i - 1).(uint64)
		curr := idCol.Value(i).(uint64)
		if prev > curr {
			t.Fatalf("rows not sorted: id[%d]=%d > id[%d]=%d", i-1, prev, i, curr)
		}
	}
}

func TestMergeSelector(t *testing.T) {
	selector := merge.NewSimpleMergeSelector()

	// Create mock parts (fewer than MinParts)
	parts := []*storage.Part{
		{Info: storage.PartInfo{PartitionID: "all", MinBlock: 1, MaxBlock: 1, Level: 0}, State: storage.PartActive, NumRows: 100},
		{Info: storage.PartInfo{PartitionID: "all", MinBlock: 2, MaxBlock: 2, Level: 0}, State: storage.PartActive, NumRows: 100},
	}
	result := selector.SelectPartsToMerge(parts)
	if result != nil {
		t.Fatal("should not select merge with only 2 parts")
	}

	// Add more parts
	parts = append(parts,
		&storage.Part{Info: storage.PartInfo{PartitionID: "all", MinBlock: 3, MaxBlock: 3, Level: 0}, State: storage.PartActive, NumRows: 100},
		&storage.Part{Info: storage.PartInfo{PartitionID: "all", MinBlock: 4, MaxBlock: 4, Level: 0}, State: storage.PartActive, NumRows: 100},
	)
	result = selector.SelectPartsToMerge(parts)
	if result == nil {
		t.Fatal("should select merge with 4 parts")
	}
	if len(result) < 3 {
		t.Fatalf("expected at least 3 parts to merge, got %d", len(result))
	}
}
