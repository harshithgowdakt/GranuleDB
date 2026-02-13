package column_test

import (
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

func TestLCBasicOperations(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("hello")
	lc.Append("world")
	lc.Append("hello")

	if lc.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", lc.Len())
	}
	if lc.DataType() != types.TypeString {
		t.Fatalf("expected TypeString, got %v", lc.DataType())
	}
	if lc.Value(0).(string) != "hello" {
		t.Fatalf("expected 'hello', got %v", lc.Value(0))
	}
	if lc.Value(1).(string) != "world" {
		t.Fatalf("expected 'world', got %v", lc.Value(1))
	}
	if lc.Value(2).(string) != "hello" {
		t.Fatalf("expected 'hello', got %v", lc.Value(2))
	}
}

func TestLCDeduplication(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	// 10 rows but only 3 unique values
	values := []string{"a", "b", "c", "a", "b", "c", "a", "b", "c", "a"}
	for _, v := range values {
		lc.Append(v)
	}

	if lc.Len() != 10 {
		t.Fatalf("expected 10 rows, got %d", lc.Len())
	}
	if lc.DictLen() != 3 {
		t.Fatalf("expected 3 unique values in dict, got %d", lc.DictLen())
	}
	for i, want := range values {
		if lc.Value(i).(string) != want {
			t.Fatalf("row %d: expected %q, got %v", i, want, lc.Value(i))
		}
	}
}

func TestLCFilterByMask(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("a")
	lc.Append("b")
	lc.Append("c")
	lc.Append("a")

	mask := []bool{true, false, true, false}
	filtered := column.FilterByMask(lc, mask)

	if filtered.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", filtered.Len())
	}
	if filtered.Value(0).(string) != "a" {
		t.Fatalf("expected 'a', got %v", filtered.Value(0))
	}
	if filtered.Value(1).(string) != "c" {
		t.Fatalf("expected 'c', got %v", filtered.Value(1))
	}

	// Should still be LC
	if _, ok := filtered.(*column.LowCardinalityColumn); !ok {
		t.Fatal("expected LowCardinalityColumn after filter")
	}
}

func TestLCGather(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("x")
	lc.Append("y")
	lc.Append("z")

	gathered := column.Gather(lc, []int{2, 0, 1, 0})

	if gathered.Len() != 4 {
		t.Fatalf("expected 4 rows, got %d", gathered.Len())
	}
	want := []string{"z", "x", "y", "x"}
	for i, w := range want {
		if gathered.Value(i).(string) != w {
			t.Fatalf("row %d: expected %q, got %v", i, w, gathered.Value(i))
		}
	}

	if _, ok := gathered.(*column.LowCardinalityColumn); !ok {
		t.Fatal("expected LowCardinalityColumn after gather")
	}
}

func TestLCAppendColumn(t *testing.T) {
	dst := column.NewLowCardinalityColumn(types.TypeString, 0)
	dst.Append("a")
	dst.Append("b")

	src := column.NewLowCardinalityColumn(types.TypeString, 0)
	src.Append("b")
	src.Append("c")

	column.AppendColumn(dst, src)

	if dst.Len() != 4 {
		t.Fatalf("expected 4 rows, got %d", dst.Len())
	}
	if dst.DictLen() != 3 {
		t.Fatalf("expected 3 unique values after merge, got %d", dst.DictLen())
	}
	want := []string{"a", "b", "b", "c"}
	for i, w := range want {
		if dst.Value(i).(string) != w {
			t.Fatalf("row %d: expected %q, got %v", i, w, dst.Value(i))
		}
	}
}

func TestLCSliceAndClone(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("a")
	lc.Append("b")
	lc.Append("c")
	lc.Append("d")

	// Slice
	sliced := lc.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Fatalf("expected 2 rows from slice, got %d", sliced.Len())
	}
	if sliced.Value(0).(string) != "b" || sliced.Value(1).(string) != "c" {
		t.Fatalf("unexpected slice values: %v, %v", sliced.Value(0), sliced.Value(1))
	}

	// Clone
	cloned := lc.Clone()
	if cloned.Len() != 4 {
		t.Fatalf("expected 4 rows from clone, got %d", cloned.Len())
	}
	for i := 0; i < 4; i++ {
		if cloned.Value(i) != lc.Value(i) {
			t.Fatalf("clone row %d mismatch: %v vs %v", i, cloned.Value(i), lc.Value(i))
		}
	}
}

func TestLCEncodeDecode(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("hello")
	lc.Append("world")
	lc.Append("hello")
	lc.Append("foo")
	lc.Append("world")

	encoded, err := column.EncodeColumn(lc)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := column.DecodeLCColumn(types.TypeString, encoded, 5)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Len() != 5 {
		t.Fatalf("expected 5 rows, got %d", decoded.Len())
	}
	for i := 0; i < 5; i++ {
		if decoded.Value(i) != lc.Value(i) {
			t.Fatalf("row %d: expected %v, got %v", i, lc.Value(i), decoded.Value(i))
		}
	}

	dlc, ok := decoded.(*column.LowCardinalityColumn)
	if !ok {
		t.Fatal("expected LowCardinalityColumn after decode")
	}
	if dlc.DictLen() != 3 {
		t.Fatalf("expected 3 dict entries, got %d", dlc.DictLen())
	}
}

func TestLCMaterialize(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeString, 0)
	lc.Append("a")
	lc.Append("b")
	lc.Append("a")

	mat := lc.Materialize()
	if mat.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", mat.Len())
	}
	sc, ok := mat.(*column.StringColumn)
	if !ok {
		t.Fatal("expected StringColumn after materialize")
	}
	want := []string{"a", "b", "a"}
	for i, w := range want {
		if sc.Data[i] != w {
			t.Fatalf("row %d: expected %q, got %q", i, w, sc.Data[i])
		}
	}
}

func TestLCNumericType(t *testing.T) {
	lc := column.NewLowCardinalityColumn(types.TypeUInt32, 0)
	lc.Append(uint32(100))
	lc.Append(uint32(200))
	lc.Append(uint32(100))
	lc.Append(uint32(300))

	if lc.Len() != 4 {
		t.Fatalf("expected 4 rows, got %d", lc.Len())
	}
	if lc.DictLen() != 3 {
		t.Fatalf("expected 3 unique values, got %d", lc.DictLen())
	}
	if lc.Value(0).(uint32) != 100 {
		t.Fatalf("expected 100, got %v", lc.Value(0))
	}
	if lc.DataType() != types.TypeUInt32 {
		t.Fatalf("expected TypeUInt32, got %v", lc.DataType())
	}
}
