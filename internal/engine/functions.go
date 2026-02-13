package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/types"
)

// isAggregateFunc returns true for aggregate function names.
func isAggregateFunc(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "min", "max", "avg", "uniq", "quantiles", "topk":
		return true
	}
	return false
}

// evalScalarFunc evaluates a scalar function call for a single row.
func evalScalarFunc(fc *parser.FunctionCall, block *column.Block, row int) (types.Value, types.DataType, error) {
	name := strings.ToLower(fc.Name)

	args := make([]types.Value, len(fc.Args))
	argTypes := make([]types.DataType, len(fc.Args))
	for i, argExpr := range fc.Args {
		v, dt, err := EvalExpr(argExpr, block, row)
		if err != nil {
			return nil, 0, fmt.Errorf("evaluating arg %d of %s: %w", i, name, err)
		}
		args[i] = v
		argTypes[i] = dt
	}

	switch name {
	case "toyyyymm":
		return scalarToYYYYMM(args, argTypes)
	case "toyear":
		return scalarToYear(args, argTypes)
	case "tomonth":
		return scalarToMonth(args, argTypes)
	case "toyyyymmdd":
		return scalarToYYYYMMDD(args, argTypes)
	case "intdiv":
		return scalarIntDiv(args, argTypes)
	case "tostring":
		return scalarToString(args, argTypes)
	default:
		return nil, 0, fmt.Errorf("unknown scalar function: %s", name)
	}
}

func toTime(v types.Value, dt types.DataType) (time.Time, error) {
	n, err := types.ToInt64(dt, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected numeric datetime, got %T", v)
	}
	return time.Unix(n, 0).UTC(), nil
}

func scalarToYYYYMM(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 1 {
		return nil, 0, fmt.Errorf("toYYYYMM requires 1 argument, got %d", len(args))
	}
	t, err := toTime(args[0], argTypes[0])
	if err != nil {
		return nil, 0, err
	}
	return uint32(t.Year()*100 + int(t.Month())), types.TypeUInt32, nil
}

func scalarToYear(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 1 {
		return nil, 0, fmt.Errorf("toYear requires 1 argument, got %d", len(args))
	}
	t, err := toTime(args[0], argTypes[0])
	if err != nil {
		return nil, 0, err
	}
	return uint16(t.Year()), types.TypeUInt16, nil
}

func scalarToMonth(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 1 {
		return nil, 0, fmt.Errorf("toMonth requires 1 argument, got %d", len(args))
	}
	t, err := toTime(args[0], argTypes[0])
	if err != nil {
		return nil, 0, err
	}
	return uint8(t.Month()), types.TypeUInt8, nil
}

func scalarToYYYYMMDD(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 1 {
		return nil, 0, fmt.Errorf("toYYYYMMDD requires 1 argument, got %d", len(args))
	}
	t, err := toTime(args[0], argTypes[0])
	if err != nil {
		return nil, 0, err
	}
	return uint32(t.Year()*10000 + int(t.Month())*100 + t.Day()), types.TypeUInt32, nil
}

func scalarIntDiv(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 2 {
		return nil, 0, fmt.Errorf("intDiv requires 2 arguments, got %d", len(args))
	}
	a, err := types.ToInt64(argTypes[0], args[0])
	if err != nil {
		return nil, 0, err
	}
	b, err := types.ToInt64(argTypes[1], args[1])
	if err != nil {
		return nil, 0, err
	}
	if b == 0 {
		return int64(0), types.TypeInt64, nil
	}
	return a / b, types.TypeInt64, nil
}

func scalarToString(args []types.Value, argTypes []types.DataType) (types.Value, types.DataType, error) {
	if len(args) != 1 {
		return nil, 0, fmt.Errorf("toString requires 1 argument, got %d", len(args))
	}
	return fmt.Sprintf("%v", args[0]), types.TypeString, nil
}

// --- Vectorized scalar function dispatch ---

func evalScalarFuncColumn(fc *parser.FunctionCall, block *column.Block) (column.Column, types.DataType, error) {
	name := strings.ToLower(fc.Name)

	argCols := make([]column.Column, len(fc.Args))
	argTypes := make([]types.DataType, len(fc.Args))
	for i, argExpr := range fc.Args {
		col, dt, err := EvalExprColumn(argExpr, block)
		if err != nil {
			return nil, 0, fmt.Errorf("evaluating arg %d of %s: %w", i, name, err)
		}
		argCols[i] = col
		argTypes[i] = dt
	}

	n := block.NumRows()

	switch name {
	case "toyyyymm":
		return vecDateFunc(argCols, argTypes, n, func(t time.Time) uint32 {
			return uint32(t.Year()*100 + int(t.Month()))
		})
	case "toyear":
		out := make([]uint16, n)
		for i := 0; i < n; i++ {
			ts, _ := types.ToInt64(argTypes[0], argCols[0].Value(i))
			t := time.Unix(ts, 0).UTC()
			out[i] = uint16(t.Year())
		}
		return &column.UInt16Column{Data: out}, types.TypeUInt16, nil
	case "tomonth":
		out := make([]uint8, n)
		for i := 0; i < n; i++ {
			ts, _ := types.ToInt64(argTypes[0], argCols[0].Value(i))
			t := time.Unix(ts, 0).UTC()
			out[i] = uint8(t.Month())
		}
		return &column.UInt8Column{Data: out}, types.TypeUInt8, nil
	case "toyyyymmdd":
		return vecDateFunc(argCols, argTypes, n, func(t time.Time) uint32 {
			return uint32(t.Year()*10000 + int(t.Month())*100 + t.Day())
		})
	case "intdiv":
		return vecIntDiv(argCols, argTypes, n)
	case "tostring":
		return vecToString(argCols, n)
	default:
		return nil, 0, fmt.Errorf("unknown scalar function: %s", name)
	}
}

func vecDateFunc(argCols []column.Column, argTypes []types.DataType, n int, fn func(time.Time) uint32) (column.Column, types.DataType, error) {
	if len(argCols) != 1 {
		return nil, 0, fmt.Errorf("date function requires 1 argument")
	}
	out := make([]uint32, n)
	for i := 0; i < n; i++ {
		ts, _ := types.ToInt64(argTypes[0], argCols[0].Value(i))
		t := time.Unix(ts, 0).UTC()
		out[i] = fn(t)
	}
	return &column.UInt32Column{Data: out}, types.TypeUInt32, nil
}

func vecIntDiv(argCols []column.Column, argTypes []types.DataType, n int) (column.Column, types.DataType, error) {
	if len(argCols) != 2 {
		return nil, 0, fmt.Errorf("intDiv requires 2 arguments")
	}
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		a, _ := types.ToInt64(argTypes[0], argCols[0].Value(i))
		b, _ := types.ToInt64(argTypes[1], argCols[1].Value(i))
		if b != 0 {
			out[i] = a / b
		}
	}
	return &column.Int64Column{Data: out}, types.TypeInt64, nil
}

func vecToString(argCols []column.Column, n int) (column.Column, types.DataType, error) {
	if len(argCols) != 1 {
		return nil, 0, fmt.Errorf("toString requires 1 argument")
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%v", argCols[0].Value(i))
	}
	return &column.StringColumn{Data: out}, types.TypeString, nil
}
