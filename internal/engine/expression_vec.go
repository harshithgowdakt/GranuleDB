package engine

import (
	"fmt"
	"strings"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

// EvalExprColumn evaluates an expression against an entire block,
// returning one result value per row as a typed Column.
// This is the vectorized alternative to calling EvalExpr per row.
func EvalExprColumn(expr parser.Expression, block *column.Block) (column.Column, types.DataType, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return evalLiteralColumn(e, block.NumRows())

	case *parser.ColumnRef:
		col, ok := block.GetColumn(e.Name)
		if !ok {
			return nil, 0, fmt.Errorf("column %s not found", e.Name)
		}
		return col, col.DataType(), nil

	case *parser.BinaryExpr:
		return evalBinaryColumn(e, block)

	case *parser.UnaryExpr:
		return evalUnaryColumn(e, block)

	case *parser.FunctionCall:
		if isAggregateFunc(e.Name) {
			return nil, 0, fmt.Errorf("aggregate function %s cannot be evaluated as vectorized column expression", e.Name)
		}
		return evalScalarFuncColumn(e, block)

	case *parser.StarExpr:
		return nil, 0, fmt.Errorf("* cannot be evaluated as an expression")

	default:
		return nil, 0, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalLiteralColumn expands a scalar literal to a full column of n rows.
func evalLiteralColumn(e *parser.LiteralExpr, n int) (column.Column, types.DataType, error) {
	switch v := e.Value.(type) {
	case int64:
		data := make([]int64, n)
		for i := range data {
			data[i] = v
		}
		return &column.Int64Column{Data: data}, types.TypeInt64, nil
	case float64:
		data := make([]float64, n)
		for i := range data {
			data[i] = v
		}
		return &column.Float64Column{Data: data}, types.TypeFloat64, nil
	case string:
		data := make([]string, n)
		for i := range data {
			data[i] = v
		}
		return &column.StringColumn{Data: data}, types.TypeString, nil
	default:
		return nil, 0, fmt.Errorf("unknown literal type: %T", e.Value)
	}
}

func evalBinaryColumn(e *parser.BinaryExpr, block *column.Block) (column.Column, types.DataType, error) {
	lCol, ldt, err := EvalExprColumn(e.Left, block)
	if err != nil {
		return nil, 0, err
	}
	rCol, rdt, err := EvalExprColumn(e.Right, block)
	if err != nil {
		return nil, 0, err
	}

	op := strings.ToUpper(e.Op)

	// Boolean operators: AND, OR
	if op == "AND" || op == "OR" {
		lMask := ColumnToBoolMask(lCol, ldt)
		rMask := ColumnToBoolMask(rCol, rdt)
		var result []bool
		if op == "AND" {
			result = vecAND(lMask, rMask)
		} else {
			result = vecOR(lMask, rMask)
		}
		return boolMaskToInt64Column(result), types.TypeInt64, nil
	}

	// String comparison
	if ldt == types.TypeString || rdt == types.TypeString {
		ls := columnToStringSlice(lCol, ldt)
		rs := columnToStringSlice(rCol, rdt)
		mask, err := vecStringCmp(op, ls, rs)
		if err != nil {
			return nil, 0, err
		}
		return boolMaskToInt64Column(mask), types.TypeInt64, nil
	}

	// Numeric: promote both to float64 slices
	lf := columnToFloat64Slice(lCol, ldt)
	rf := columnToFloat64Slice(rCol, rdt)

	switch op {
	case "+":
		return &column.Float64Column{Data: vecAdd(lf, rf)}, types.TypeFloat64, nil
	case "-":
		return &column.Float64Column{Data: vecSub(lf, rf)}, types.TypeFloat64, nil
	case "*":
		return &column.Float64Column{Data: vecMul(lf, rf)}, types.TypeFloat64, nil
	case "/":
		return &column.Float64Column{Data: vecDiv(lf, rf)}, types.TypeFloat64, nil
	case "=":
		return boolMaskToInt64Column(vecEQ(lf, rf)), types.TypeInt64, nil
	case "!=", "<>":
		return boolMaskToInt64Column(vecNEQ(lf, rf)), types.TypeInt64, nil
	case "<":
		return boolMaskToInt64Column(vecLT(lf, rf)), types.TypeInt64, nil
	case ">":
		return boolMaskToInt64Column(vecGT(lf, rf)), types.TypeInt64, nil
	case "<=":
		return boolMaskToInt64Column(vecLTE(lf, rf)), types.TypeInt64, nil
	case ">=":
		return boolMaskToInt64Column(vecGTE(lf, rf)), types.TypeInt64, nil
	default:
		return nil, 0, fmt.Errorf("unknown binary operator: %s", op)
	}
}

func evalUnaryColumn(e *parser.UnaryExpr, block *column.Block) (column.Column, types.DataType, error) {
	col, dt, err := EvalExprColumn(e.Expr, block)
	if err != nil {
		return nil, 0, err
	}
	switch e.Op {
	case "-":
		f := columnToFloat64Slice(col, dt)
		return &column.Float64Column{Data: vecNegate(f)}, types.TypeFloat64, nil
	case "NOT":
		mask := ColumnToBoolMask(col, dt)
		return boolMaskToInt64Column(vecNOT(mask)), types.TypeInt64, nil
	default:
		return nil, 0, fmt.Errorf("unknown unary operator: %s", e.Op)
	}
}

// columnToFloat64Slice type-switches once then converts the entire typed slice to []float64.
func columnToFloat64Slice(col column.Column, dt types.DataType) []float64 {
	// Materialize LowCardinality columns first.
	if lc, ok := col.(*column.LowCardinalityColumn); ok {
		return columnToFloat64Slice(lc.Materialize(), dt)
	}
	switch c := col.(type) {
	case *column.UInt8Column:
		return convertSlice(c.Data)
	case *column.UInt16Column:
		return convertSlice(c.Data)
	case *column.UInt32Column:
		return convertSlice(c.Data)
	case *column.UInt64Column:
		return convertSlice(c.Data)
	case *column.Int8Column:
		return convertSlice(c.Data)
	case *column.Int16Column:
		return convertSlice(c.Data)
	case *column.Int32Column:
		return convertSlice(c.Data)
	case *column.Int64Column:
		return convertSlice(c.Data)
	case *column.Float32Column:
		return convertSlice(c.Data)
	case *column.Float64Column:
		// Already float64 — return a copy to avoid mutation.
		out := make([]float64, len(c.Data))
		copy(out, c.Data)
		return out
	case *column.DateTimeColumn:
		return convertSlice(c.Data)
	default:
		// Fallback: per-row conversion via interface.
		n := col.Len()
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			f, err := types.ToFloat64(dt, col.Value(i))
			if err != nil {
				out[i] = 0
			} else {
				out[i] = f
			}
		}
		return out
	}
}

// convertSlice converts a numeric typed slice to []float64.
func convertSlice[T numeric](data []T) []float64 {
	out := make([]float64, len(data))
	for i, v := range data {
		out[i] = float64(v)
	}
	return out
}

// ColumnToBoolMask converts a numeric column to []bool (nonzero = true).
func ColumnToBoolMask(col column.Column, dt types.DataType) []bool {
	// Materialize LowCardinality columns first.
	if lc, ok := col.(*column.LowCardinalityColumn); ok {
		return ColumnToBoolMask(lc.Materialize(), dt)
	}
	switch c := col.(type) {
	case *column.UInt8Column:
		return toBoolSlice(c.Data)
	case *column.UInt16Column:
		return toBoolSlice(c.Data)
	case *column.UInt32Column:
		return toBoolSlice(c.Data)
	case *column.UInt64Column:
		return toBoolSlice(c.Data)
	case *column.Int8Column:
		return toBoolSlice(c.Data)
	case *column.Int16Column:
		return toBoolSlice(c.Data)
	case *column.Int32Column:
		return toBoolSlice(c.Data)
	case *column.Int64Column:
		return toBoolSlice(c.Data)
	case *column.Float32Column:
		return toBoolSlice(c.Data)
	case *column.Float64Column:
		return toBoolSlice(c.Data)
	case *column.DateTimeColumn:
		return toBoolSlice(c.Data)
	case *column.StringColumn:
		out := make([]bool, len(c.Data))
		for i, s := range c.Data {
			out[i] = s != ""
		}
		return out
	default:
		n := col.Len()
		out := make([]bool, n)
		for i := 0; i < n; i++ {
			b, err := ToBool(col.Value(i))
			if err == nil {
				out[i] = b
			}
		}
		return out
	}
}

// toBoolSlice converts a numeric slice to []bool (nonzero = true).
func toBoolSlice[T numeric](data []T) []bool {
	out := make([]bool, len(data))
	for i, v := range data {
		out[i] = v != 0
	}
	return out
}

// boolMaskToInt64Column encodes []bool as Int64Column (1/0 values).
func boolMaskToInt64Column(mask []bool) *column.Int64Column {
	data := make([]int64, len(mask))
	for i, m := range mask {
		if m {
			data[i] = 1
		}
	}
	return &column.Int64Column{Data: data}
}

// columnToStringSlice extracts string representations from a column.
func columnToStringSlice(col column.Column, dt types.DataType) []string {
	// Optimize for LC wrapping String: resolve through dict.
	if lc, ok := col.(*column.LowCardinalityColumn); ok {
		if sc, ok := lc.Dict.(*column.StringColumn); ok {
			out := make([]string, len(lc.Indices))
			for i, idx := range lc.Indices {
				out[i] = sc.Data[idx]
			}
			return out
		}
		return columnToStringSlice(lc.Materialize(), dt)
	}
	if c, ok := col.(*column.StringColumn); ok {
		return c.Data
	}
	n := col.Len()
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%v", col.Value(i))
	}
	return out
}

// vecStringCmp performs vectorized string comparison.
func vecStringCmp(op string, a, b []string) ([]bool, error) {
	out := make([]bool, len(a))
	switch op {
	case "=":
		for i := range a {
			out[i] = a[i] == b[i]
		}
	case "!=", "<>":
		for i := range a {
			out[i] = a[i] != b[i]
		}
	case "<":
		for i := range a {
			out[i] = a[i] < b[i]
		}
	case ">":
		for i := range a {
			out[i] = a[i] > b[i]
		}
	case "<=":
		for i := range a {
			out[i] = a[i] <= b[i]
		}
	case ">=":
		for i := range a {
			out[i] = a[i] >= b[i]
		}
	default:
		return nil, fmt.Errorf("operator %s not supported for string comparison", op)
	}
	return out, nil
}
