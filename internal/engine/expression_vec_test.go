package engine

import (
	"testing"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

func makeTestBlock() *column.Block {
	return column.NewBlock(
		[]string{"id", "value", "name"},
		[]column.Column{
			&column.UInt64Column{Data: []uint64{1, 2, 3, 4, 5}},
			&column.Int64Column{Data: []int64{10, 20, 30, 40, 50}},
			&column.StringColumn{Data: []string{"alice", "bob", "charlie", "dave", "eve"}},
		},
	)
}

func TestEvalExprColumn_Literal(t *testing.T) {
	block := makeTestBlock()

	// int64 literal
	col, dt, err := EvalExprColumn(&parser.LiteralExpr{Value: int64(42)}, block)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeInt64 {
		t.Fatalf("expected Int64, got %v", dt)
	}
	if col.Len() != 5 {
		t.Fatalf("expected 5 rows, got %d", col.Len())
	}
	for i := 0; i < 5; i++ {
		if col.Value(i).(int64) != 42 {
			t.Fatalf("row %d: expected 42, got %v", i, col.Value(i))
		}
	}
}

func TestEvalExprColumn_ColumnRef(t *testing.T) {
	block := makeTestBlock()

	col, dt, err := EvalExprColumn(&parser.ColumnRef{Name: "id"}, block)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeUInt64 {
		t.Fatalf("expected UInt64, got %v", dt)
	}
	if col.Len() != 5 {
		t.Fatalf("expected 5 rows, got %d", col.Len())
	}
	if col.Value(2).(uint64) != 3 {
		t.Fatalf("expected id[2]=3, got %v", col.Value(2))
	}
}

func TestEvalExprColumn_Arithmetic(t *testing.T) {
	block := makeTestBlock()

	// value + 5
	expr := &parser.BinaryExpr{
		Op:    "+",
		Left:  &parser.ColumnRef{Name: "value"},
		Right: &parser.LiteralExpr{Value: int64(5)},
	}
	col, dt, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeFloat64 {
		t.Fatalf("expected Float64, got %v", dt)
	}
	expected := []float64{15, 25, 35, 45, 55}
	for i, want := range expected {
		got := col.Value(i).(float64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_Comparison(t *testing.T) {
	block := makeTestBlock()

	// id > 2
	expr := &parser.BinaryExpr{
		Op:    ">",
		Left:  &parser.ColumnRef{Name: "id"},
		Right: &parser.LiteralExpr{Value: int64(2)},
	}
	col, dt, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeInt64 {
		t.Fatalf("expected Int64 (bool encoded), got %v", dt)
	}
	// id: 1,2,3,4,5 → >2: false, false, true, true, true
	expected := []int64{0, 0, 1, 1, 1}
	for i, want := range expected {
		got := col.Value(i).(int64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_Boolean(t *testing.T) {
	block := makeTestBlock()

	// id > 1 AND id < 5
	expr := &parser.BinaryExpr{
		Op: "AND",
		Left: &parser.BinaryExpr{
			Op:    ">",
			Left:  &parser.ColumnRef{Name: "id"},
			Right: &parser.LiteralExpr{Value: int64(1)},
		},
		Right: &parser.BinaryExpr{
			Op:    "<",
			Left:  &parser.ColumnRef{Name: "id"},
			Right: &parser.LiteralExpr{Value: int64(5)},
		},
	}
	col, _, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	// id: 1,2,3,4,5 → >1: 0,1,1,1,1  <5: 1,1,1,1,0  AND: 0,1,1,1,0
	expected := []int64{0, 1, 1, 1, 0}
	for i, want := range expected {
		got := col.Value(i).(int64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_UnaryNegate(t *testing.T) {
	block := makeTestBlock()

	// -value
	expr := &parser.UnaryExpr{Op: "-", Expr: &parser.ColumnRef{Name: "value"}}
	col, _, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	expected := []float64{-10, -20, -30, -40, -50}
	for i, want := range expected {
		got := col.Value(i).(float64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_UnaryNot(t *testing.T) {
	block := makeTestBlock()

	// NOT (id > 3)
	expr := &parser.UnaryExpr{
		Op: "NOT",
		Expr: &parser.BinaryExpr{
			Op:    ">",
			Left:  &parser.ColumnRef{Name: "id"},
			Right: &parser.LiteralExpr{Value: int64(3)},
		},
	}
	col, _, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	// id >3: 0,0,0,1,1 → NOT: 1,1,1,0,0
	expected := []int64{1, 1, 1, 0, 0}
	for i, want := range expected {
		got := col.Value(i).(int64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_StringComparison(t *testing.T) {
	block := makeTestBlock()

	// name = 'bob'
	expr := &parser.BinaryExpr{
		Op:    "=",
		Left:  &parser.ColumnRef{Name: "name"},
		Right: &parser.LiteralExpr{Value: "bob"},
	}
	col, _, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}
	expected := []int64{0, 1, 0, 0, 0}
	for i, want := range expected {
		got := col.Value(i).(int64)
		if got != want {
			t.Fatalf("row %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestEvalExprColumn_MatchesRowByRow(t *testing.T) {
	block := makeTestBlock()

	// Complex: (value * 2) + id > 25
	expr := &parser.BinaryExpr{
		Op: ">",
		Left: &parser.BinaryExpr{
			Op: "+",
			Left: &parser.BinaryExpr{
				Op:    "*",
				Left:  &parser.ColumnRef{Name: "value"},
				Right: &parser.LiteralExpr{Value: int64(2)},
			},
			Right: &parser.ColumnRef{Name: "id"},
		},
		Right: &parser.LiteralExpr{Value: int64(25)},
	}

	// Vectorized result
	vecCol, _, err := EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}

	// Row-by-row result
	for i := 0; i < block.NumRows(); i++ {
		rv, _, err := EvalExpr(expr, block, i)
		if err != nil {
			t.Fatalf("row %d: EvalExpr error: %v", i, err)
		}
		vecVal := vecCol.Value(i).(int64)
		rowVal := rv.(int64)
		if vecVal != rowVal {
			t.Fatalf("row %d: vectorized=%d, row-by-row=%d", i, vecVal, rowVal)
		}
	}
}

func TestColumnToBoolMask(t *testing.T) {
	col := &column.Int64Column{Data: []int64{0, 1, 0, 1, 1}}
	mask := ColumnToBoolMask(col, types.TypeInt64)
	expected := []bool{false, true, false, true, true}
	for i, want := range expected {
		if mask[i] != want {
			t.Fatalf("index %d: expected %v, got %v", i, want, mask[i])
		}
	}
}
