package engine

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/storage"
	"github.com/harshithgowda/goose-db/internal/types"
)

// SelectExecutor is a hook for replacing the SELECT execution path.
// When set, executeSelect delegates to this function instead of using
// the pull-based operator tree. This is used to wire in the push-based
// processor pipeline without creating an import cycle.
var SelectExecutor func(stmt *parser.SelectStmt, db *storage.Database) (*ExecuteResult, error)

// ExecuteResult holds the result of executing a statement.
type ExecuteResult struct {
	Blocks     []*column.Block
	ColumnNames []string
	Message    string // for DDL statements
}

// Execute runs a parsed statement against the database.
func Execute(stmt parser.Statement, db *storage.Database) (*ExecuteResult, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return executeCreate(s, db)
	case *parser.InsertStmt:
		return executeInsert(s, db)
	case *parser.SelectStmt:
		return executeSelect(s, db)
	case *parser.DropTableStmt:
		return executeDrop(s, db)
	case *parser.ShowTablesStmt:
		return executeShowTables(db)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func executeCreate(stmt *parser.CreateTableStmt, db *storage.Database) (*ExecuteResult, error) {
	schema := storage.TableSchema{
		OrderBy:     stmt.OrderBy,
		PartitionBy: stmt.PartitionBy,
	}

	for _, col := range stmt.Columns {
		dt, err := types.ParseDataType(col.TypeName)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		schema.Columns = append(schema.Columns, storage.ColumnDef{Name: col.Name, DataType: dt})
	}

	err := db.CreateTable(stmt.TableName, schema)
	if err != nil {
		if stmt.IfNotExists {
			return &ExecuteResult{Message: "OK"}, nil
		}
		return nil, err
	}
	return &ExecuteResult{Message: "OK"}, nil
}

func executeInsert(stmt *parser.InsertStmt, db *storage.Database) (*ExecuteResult, error) {
	table, ok := db.GetTable(stmt.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Determine column order
	colNames := stmt.Columns
	if len(colNames) == 0 {
		colNames = table.Schema.ColumnNames()
	}

	// Validate column count
	for i, row := range stmt.Values {
		if len(row) != len(colNames) {
			return nil, fmt.Errorf("row %d: expected %d values, got %d", i, len(colNames), len(row))
		}
	}

	// Create columns
	cols := make([]column.Column, len(colNames))
	for i, name := range colNames {
		colDef, ok := table.Schema.GetColumnDef(name)
		if !ok {
			return nil, fmt.Errorf("column %s not found in table %s", name, stmt.TableName)
		}
		cols[i] = column.NewColumnWithCapacity(colDef.DataType, len(stmt.Values))
	}

	// Convert literal values and populate columns
	for rowIdx, row := range stmt.Values {
		for colIdx, expr := range row {
			colDef, _ := table.Schema.GetColumnDef(colNames[colIdx])
			val, err := convertLiteralToType(expr, colDef.DataType)
			if err != nil {
				return nil, fmt.Errorf("row %d, column %s: %w", rowIdx, colNames[colIdx], err)
			}
			cols[colIdx].Append(val)
		}
	}

	block := column.NewBlock(colNames, cols)
	if err := table.Insert(block); err != nil {
		return nil, err
	}

	return &ExecuteResult{Message: fmt.Sprintf("OK. %d rows inserted.", len(stmt.Values))}, nil
}

func executeSelect(stmt *parser.SelectStmt, db *storage.Database) (*ExecuteResult, error) {
	// Use push-based processor pipeline if wired.
	if SelectExecutor != nil {
		return SelectExecutor(stmt, db)
	}

	// Fallback: pull-based operator tree.
	op, outNames, err := PlanSelect(stmt, db)
	if err != nil {
		return nil, err
	}

	if err := op.Open(); err != nil {
		return nil, err
	}
	defer op.Close()

	var blocks []*column.Block
	for {
		block, err := op.Next()
		if err != nil {
			return nil, err
		}
		if block == nil {
			break
		}
		if block.NumColumns() > 0 && len(outNames) != block.NumColumns() {
			outNames = block.ColumnNames
		}
		blocks = append(blocks, block)
	}

	return &ExecuteResult{Blocks: blocks, ColumnNames: outNames}, nil
}

func executeDrop(stmt *parser.DropTableStmt, db *storage.Database) (*ExecuteResult, error) {
	err := db.DropTable(stmt.TableName)
	if err != nil {
		if stmt.IfExists {
			return &ExecuteResult{Message: "OK"}, nil
		}
		return nil, err
	}
	return &ExecuteResult{Message: "OK"}, nil
}

func executeShowTables(db *storage.Database) (*ExecuteResult, error) {
	names := db.TableNames()
	sort.Strings(names)

	col := &column.StringColumn{Data: names}
	block := column.NewBlock([]string{"name"}, []column.Column{col})
	return &ExecuteResult{
		Blocks:      []*column.Block{block},
		ColumnNames: []string{"name"},
	}, nil
}

// convertLiteralToType converts a parser expression (literal) to a typed value.
func convertLiteralToType(expr parser.Expression, dt types.DataType) (types.Value, error) {
	lit, ok := expr.(*parser.LiteralExpr)
	if !ok {
		// Could be a unary minus
		if unary, ok := expr.(*parser.UnaryExpr); ok && unary.Op == "-" {
			inner, err := convertLiteralToType(unary.Expr, dt)
			if err != nil {
				return nil, err
			}
			return negateValue(inner, dt)
		}
		return nil, fmt.Errorf("expected literal value, got %T", expr)
	}

	switch dt {
	case types.TypeUInt8:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return uint8(n), nil
	case types.TypeUInt16:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return uint16(n), nil
	case types.TypeUInt32:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return uint32(n), nil
	case types.TypeUInt64:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return uint64(n), nil
	case types.TypeInt8:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return int8(n), nil
	case types.TypeInt16:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return int16(n), nil
	case types.TypeInt32:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return int32(n), nil
	case types.TypeInt64:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case types.TypeFloat32:
		f, err := toFloat64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case types.TypeFloat64:
		f, err := toFloat64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return f, nil
	case types.TypeString:
		return fmt.Sprintf("%v", lit.Value), nil
	case types.TypeDateTime:
		n, err := toInt64FromLiteral(lit.Value)
		if err != nil {
			return nil, err
		}
		return uint32(n), nil
	default:
		return nil, fmt.Errorf("unsupported type conversion for %s", dt.Name())
	}
}

func toInt64FromLiteral(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64FromLiteral(v interface{}) (float64, error) {
	switch val := v.(type) {
	case int64:
		return float64(val), nil
	case float64:
		return val, nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func negateValue(v types.Value, dt types.DataType) (types.Value, error) {
	switch dt {
	case types.TypeInt8:
		return -v.(int8), nil
	case types.TypeInt16:
		return -v.(int16), nil
	case types.TypeInt32:
		return -v.(int32), nil
	case types.TypeInt64:
		return -v.(int64), nil
	case types.TypeFloat32:
		return -v.(float32), nil
	case types.TypeFloat64:
		return -v.(float64), nil
	default:
		return nil, fmt.Errorf("cannot negate %s type", dt.Name())
	}
}
