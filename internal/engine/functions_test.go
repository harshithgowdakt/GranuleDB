package engine_test

import (
	"testing"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

func TestScalarToYYYYMM(t *testing.T) {
	// 2024-03-15 12:00:00 UTC = 1710504000
	block := column.NewBlock(
		[]string{"ts"},
		[]column.Column{&column.DateTimeColumn{Data: []uint32{1710504000}}},
	)
	expr := &parser.FunctionCall{Name: "toyyyymm", Args: []parser.Expression{&parser.ColumnRef{Name: "ts"}}}

	val, dt, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeUInt32 {
		t.Fatalf("expected UInt32, got %v", dt)
	}
	if val.(uint32) != 202403 {
		t.Fatalf("expected 202403, got %v", val)
	}
}

func TestScalarToYear(t *testing.T) {
	// 2024-01-01 00:00:00 UTC = 1704067200
	block := column.NewBlock(
		[]string{"ts"},
		[]column.Column{&column.DateTimeColumn{Data: []uint32{1704067200}}},
	)
	expr := &parser.FunctionCall{Name: "toyear", Args: []parser.Expression{&parser.ColumnRef{Name: "ts"}}}

	val, dt, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeUInt16 {
		t.Fatalf("expected UInt16, got %v", dt)
	}
	if val.(uint16) != 2024 {
		t.Fatalf("expected 2024, got %v", val)
	}
}

func TestScalarToMonth(t *testing.T) {
	// 2024-03-15 = 1710504000
	block := column.NewBlock(
		[]string{"ts"},
		[]column.Column{&column.DateTimeColumn{Data: []uint32{1710504000}}},
	)
	expr := &parser.FunctionCall{Name: "tomonth", Args: []parser.Expression{&parser.ColumnRef{Name: "ts"}}}

	val, _, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if val.(uint8) != 3 {
		t.Fatalf("expected 3 (March), got %v", val)
	}
}

func TestScalarToYYYYMMDD(t *testing.T) {
	// 2024-03-15 = 1710504000
	block := column.NewBlock(
		[]string{"ts"},
		[]column.Column{&column.DateTimeColumn{Data: []uint32{1710504000}}},
	)
	expr := &parser.FunctionCall{Name: "toyyyymmdd", Args: []parser.Expression{&parser.ColumnRef{Name: "ts"}}}

	val, _, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if val.(uint32) != 20240315 {
		t.Fatalf("expected 20240315, got %v", val)
	}
}

func TestScalarIntDiv(t *testing.T) {
	block := column.NewBlock(
		[]string{"id"},
		[]column.Column{&column.UInt64Column{Data: []uint64{1500}}},
	)
	expr := &parser.FunctionCall{
		Name: "intdiv",
		Args: []parser.Expression{
			&parser.ColumnRef{Name: "id"},
			&parser.LiteralExpr{Value: int64(1000)},
		},
	}

	val, dt, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeInt64 {
		t.Fatalf("expected Int64, got %v", dt)
	}
	if val.(int64) != 1 {
		t.Fatalf("expected 1, got %v", val)
	}
}

func TestScalarIntDivZero(t *testing.T) {
	block := column.NewBlock(
		[]string{"id"},
		[]column.Column{&column.UInt64Column{Data: []uint64{100}}},
	)
	expr := &parser.FunctionCall{
		Name: "intdiv",
		Args: []parser.Expression{
			&parser.ColumnRef{Name: "id"},
			&parser.LiteralExpr{Value: int64(0)},
		},
	}

	val, _, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if val.(int64) != 0 {
		t.Fatalf("expected 0 for division by zero, got %v", val)
	}
}

func TestScalarToString(t *testing.T) {
	block := column.NewBlock(
		[]string{"id"},
		[]column.Column{&column.UInt64Column{Data: []uint64{42}}},
	)
	expr := &parser.FunctionCall{
		Name: "tostring",
		Args: []parser.Expression{&parser.ColumnRef{Name: "id"}},
	}

	val, dt, err := engine.EvalExpr(expr, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dt != types.TypeString {
		t.Fatalf("expected String, got %v", dt)
	}
	if val.(string) != "42" {
		t.Fatalf("expected '42', got %v", val)
	}
}

func TestScalarVecMatchesRowByRow(t *testing.T) {
	// Test that vectorized toYYYYMM matches row-by-row
	block := column.NewBlock(
		[]string{"ts"},
		[]column.Column{&column.DateTimeColumn{Data: []uint32{
			1704067200, // 2024-01-01
			1706745600, // 2024-02-01
			1710504000, // 2024-03-15
		}}},
	)
	expr := &parser.FunctionCall{Name: "toyyyymm", Args: []parser.Expression{&parser.ColumnRef{Name: "ts"}}}

	vecCol, _, err := engine.EvalExprColumn(expr, block)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < block.NumRows(); i++ {
		rowVal, _, err := engine.EvalExpr(expr, block, i)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if vecCol.Value(i) != rowVal {
			t.Fatalf("row %d: vec=%v, row=%v", i, vecCol.Value(i), rowVal)
		}
	}
}
