package engine

import (
	"fmt"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// EvalExpr evaluates an expression for a single row of a block.
func EvalExpr(expr parser.Expression, block *column.Block, row int) (types.Value, types.DataType, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		switch v := e.Value.(type) {
		case int64:
			return v, types.TypeInt64, nil
		case float64:
			return v, types.TypeFloat64, nil
		case string:
			return v, types.TypeString, nil
		default:
			return nil, 0, fmt.Errorf("unknown literal type: %T", e.Value)
		}

	case *parser.ColumnRef:
		col, ok := block.GetColumn(e.Name)
		if !ok {
			return nil, 0, fmt.Errorf("column %s not found", e.Name)
		}
		return col.Value(row), col.DataType(), nil

	case *parser.BinaryExpr:
		return evalBinary(e, block, row)

	case *parser.UnaryExpr:
		return evalUnary(e, block, row)

	case *parser.FunctionCall:
		if isAggregateFunc(e.Name) {
			return nil, 0, fmt.Errorf("aggregate function %s cannot be evaluated row-by-row", e.Name)
		}
		return evalScalarFunc(e, block, row)

	case *parser.StarExpr:
		return nil, 0, fmt.Errorf("* cannot be evaluated as an expression")

	default:
		return nil, 0, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func evalBinary(e *parser.BinaryExpr, block *column.Block, row int) (types.Value, types.DataType, error) {
	lv, ldt, err := EvalExpr(e.Left, block, row)
	if err != nil {
		return nil, 0, err
	}
	rv, rdt, err := EvalExpr(e.Right, block, row)
	if err != nil {
		return nil, 0, err
	}

	op := strings.ToUpper(e.Op)

	// Boolean operators
	if op == "AND" || op == "OR" {
		lb, err := ToBool(lv)
		if err != nil {
			return nil, 0, err
		}
		rb, err := ToBool(rv)
		if err != nil {
			return nil, 0, err
		}
		var result bool
		if op == "AND" {
			result = lb && rb
		} else {
			result = lb || rb
		}
		if result {
			return int64(1), types.TypeInt64, nil
		}
		return int64(0), types.TypeInt64, nil
	}

	// String comparison
	if ldt == types.TypeString || rdt == types.TypeString {
		ls := fmt.Sprintf("%v", lv)
		rs := fmt.Sprintf("%v", rv)
		cmp := strings.Compare(ls, rs)
		return evalComparison(op, cmp)
	}

	// Numeric operations - promote to common type
	lf, err := types.ToFloat64(ldt, lv)
	if err != nil {
		return nil, 0, fmt.Errorf("left operand: %w", err)
	}
	rf, err := types.ToFloat64(rdt, rv)
	if err != nil {
		return nil, 0, fmt.Errorf("right operand: %w", err)
	}

	switch op {
	case "+":
		return lf + rf, types.TypeFloat64, nil
	case "-":
		return lf - rf, types.TypeFloat64, nil
	case "*":
		return lf * rf, types.TypeFloat64, nil
	case "/":
		if rf == 0 {
			return float64(0), types.TypeFloat64, nil
		}
		return lf / rf, types.TypeFloat64, nil
	case "=":
		return boolToInt(lf == rf), types.TypeInt64, nil
	case "!=", "<>":
		return boolToInt(lf != rf), types.TypeInt64, nil
	case "<":
		return boolToInt(lf < rf), types.TypeInt64, nil
	case ">":
		return boolToInt(lf > rf), types.TypeInt64, nil
	case "<=":
		return boolToInt(lf <= rf), types.TypeInt64, nil
	case ">=":
		return boolToInt(lf >= rf), types.TypeInt64, nil
	default:
		return nil, 0, fmt.Errorf("unknown binary operator: %s", op)
	}
}

func evalComparison(op string, cmp int) (types.Value, types.DataType, error) {
	switch op {
	case "=":
		return boolToInt(cmp == 0), types.TypeInt64, nil
	case "!=", "<>":
		return boolToInt(cmp != 0), types.TypeInt64, nil
	case "<":
		return boolToInt(cmp < 0), types.TypeInt64, nil
	case ">":
		return boolToInt(cmp > 0), types.TypeInt64, nil
	case "<=":
		return boolToInt(cmp <= 0), types.TypeInt64, nil
	case ">=":
		return boolToInt(cmp >= 0), types.TypeInt64, nil
	default:
		return nil, 0, fmt.Errorf("operator %s not supported for string comparison", op)
	}
}

func evalUnary(e *parser.UnaryExpr, block *column.Block, row int) (types.Value, types.DataType, error) {
	v, dt, err := EvalExpr(e.Expr, block, row)
	if err != nil {
		return nil, 0, err
	}
	switch e.Op {
	case "-":
		f, err := types.ToFloat64(dt, v)
		if err != nil {
			return nil, 0, err
		}
		return -f, types.TypeFloat64, nil
	case "NOT":
		b, err := ToBool(v)
		if err != nil {
			return nil, 0, err
		}
		return boolToInt(!b), types.TypeInt64, nil
	default:
		return nil, 0, fmt.Errorf("unknown unary operator: %s", e.Op)
	}
}

// ToBool converts a Value to a boolean.
func ToBool(v types.Value) (bool, error) {
	switch val := v.(type) {
	case int64:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case uint8:
		return val != 0, nil
	case uint16:
		return val != 0, nil
	case uint32:
		return val != 0, nil
	case uint64:
		return val != 0, nil
	case int8:
		return val != 0, nil
	case int16:
		return val != 0, nil
	case int32:
		return val != 0, nil
	case float32:
		return val != 0, nil
	case string:
		return val != "", nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// ExprReferencesColumns returns all column names referenced by an expression.
func ExprReferencesColumns(expr parser.Expression) []string {
	if expr == nil {
		return nil
	}
	var cols []string
	switch e := expr.(type) {
	case *parser.ColumnRef:
		cols = append(cols, e.Name)
	case *parser.BinaryExpr:
		cols = append(cols, ExprReferencesColumns(e.Left)...)
		cols = append(cols, ExprReferencesColumns(e.Right)...)
	case *parser.UnaryExpr:
		cols = append(cols, ExprReferencesColumns(e.Expr)...)
	case *parser.FunctionCall:
		for _, arg := range e.Args {
			cols = append(cols, ExprReferencesColumns(arg)...)
		}
	}
	return cols
}
