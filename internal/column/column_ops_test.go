package column

import (
	"testing"
)

func TestFilterByMask_UInt64(t *testing.T) {
	col := &UInt64Column{Data: []uint64{10, 20, 30, 40, 50}}
	mask := []bool{true, false, true, false, true}
	result := FilterByMask(col, mask).(*UInt64Column)
	if len(result.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Data))
	}
	want := []uint64{10, 30, 50}
	for i, v := range want {
		if result.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, result.Data[i])
		}
	}
}

func TestFilterByMask_Int64(t *testing.T) {
	col := &Int64Column{Data: []int64{-1, 0, 1, 2, 3}}
	mask := []bool{false, false, true, true, true}
	result := FilterByMask(col, mask).(*Int64Column)
	if len(result.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Data))
	}
	want := []int64{1, 2, 3}
	for i, v := range want {
		if result.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, result.Data[i])
		}
	}
}

func TestFilterByMask_Float64(t *testing.T) {
	col := &Float64Column{Data: []float64{1.1, 2.2, 3.3}}
	mask := []bool{true, false, true}
	result := FilterByMask(col, mask).(*Float64Column)
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Data))
	}
	if result.Data[0] != 1.1 || result.Data[1] != 3.3 {
		t.Fatalf("unexpected values: %v", result.Data)
	}
}

func TestFilterByMask_String(t *testing.T) {
	col := &StringColumn{Data: []string{"a", "b", "c", "d"}}
	mask := []bool{false, true, false, true}
	result := FilterByMask(col, mask).(*StringColumn)
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Data))
	}
	if result.Data[0] != "b" || result.Data[1] != "d" {
		t.Fatalf("unexpected values: %v", result.Data)
	}
}

func TestFilterByMask_AllFalse(t *testing.T) {
	col := &UInt64Column{Data: []uint64{1, 2, 3}}
	mask := []bool{false, false, false}
	result := FilterByMask(col, mask).(*UInt64Column)
	if len(result.Data) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Data))
	}
}

func TestGather_UInt64(t *testing.T) {
	col := &UInt64Column{Data: []uint64{10, 20, 30, 40, 50}}
	indices := []int{4, 2, 0}
	result := Gather(col, indices).(*UInt64Column)
	if len(result.Data) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Data))
	}
	want := []uint64{50, 30, 10}
	for i, v := range want {
		if result.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, result.Data[i])
		}
	}
}

func TestGather_Int64(t *testing.T) {
	col := &Int64Column{Data: []int64{100, 200, 300}}
	indices := []int{2, 2, 1, 0}
	result := Gather(col, indices).(*Int64Column)
	want := []int64{300, 300, 200, 100}
	for i, v := range want {
		if result.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, result.Data[i])
		}
	}
}

func TestGather_String(t *testing.T) {
	col := &StringColumn{Data: []string{"alice", "bob", "charlie"}}
	indices := []int{2, 0, 1}
	result := Gather(col, indices).(*StringColumn)
	want := []string{"charlie", "alice", "bob"}
	for i, v := range want {
		if result.Data[i] != v {
			t.Fatalf("index %d: expected %s, got %s", i, v, result.Data[i])
		}
	}
}

func TestGather_Float64(t *testing.T) {
	col := &Float64Column{Data: []float64{1.1, 2.2, 3.3}}
	indices := []int{1, 0}
	result := Gather(col, indices).(*Float64Column)
	if result.Data[0] != 2.2 || result.Data[1] != 1.1 {
		t.Fatalf("unexpected values: %v", result.Data)
	}
}

func TestAppendColumn_UInt64(t *testing.T) {
	dst := &UInt64Column{Data: []uint64{1, 2}}
	src := &UInt64Column{Data: []uint64{3, 4, 5}}
	AppendColumn(dst, src)
	if len(dst.Data) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(dst.Data))
	}
	want := []uint64{1, 2, 3, 4, 5}
	for i, v := range want {
		if dst.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, dst.Data[i])
		}
	}
}

func TestAppendColumn_Int64(t *testing.T) {
	dst := &Int64Column{Data: []int64{10}}
	src := &Int64Column{Data: []int64{20, 30}}
	AppendColumn(dst, src)
	want := []int64{10, 20, 30}
	for i, v := range want {
		if dst.Data[i] != v {
			t.Fatalf("index %d: expected %d, got %d", i, v, dst.Data[i])
		}
	}
}

func TestAppendColumn_String(t *testing.T) {
	dst := &StringColumn{Data: []string{"hello"}}
	src := &StringColumn{Data: []string{"world"}}
	AppendColumn(dst, src)
	if len(dst.Data) != 2 || dst.Data[0] != "hello" || dst.Data[1] != "world" {
		t.Fatalf("unexpected: %v", dst.Data)
	}
}

func TestAppendColumn_EmptySrc(t *testing.T) {
	dst := &UInt64Column{Data: []uint64{1, 2}}
	src := &UInt64Column{Data: []uint64{}}
	AppendColumn(dst, src)
	if len(dst.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(dst.Data))
	}
}

func TestAppendColumn_Float64(t *testing.T) {
	dst := &Float64Column{Data: []float64{1.5}}
	src := &Float64Column{Data: []float64{2.5, 3.5}}
	AppendColumn(dst, src)
	if len(dst.Data) != 3 || dst.Data[2] != 3.5 {
		t.Fatalf("unexpected: %v", dst.Data)
	}
}
