package engine

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

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
	Blocks      []*column.Block
	ColumnNames []string
	Message     string // for DDL statements
}

// Execute runs a parsed statement against the database.
func Execute(stmt parser.Statement, db *storage.Database) (*ExecuteResult, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return executeCreate(s, db)
	case *parser.CreateMaterializedViewStmt:
		return executeCreateMaterializedView(s, db)
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
		Engine:      stmt.Engine,
		OrderBy:     stmt.OrderBy,
		PartitionBy: parser.ExprToSQL(stmt.PartitionBy),
	}
	if schema.Engine == "" {
		schema.Engine = "MergeTree"
	}

	for _, col := range stmt.Columns {
		dt, isLC, err := types.ParseColumnType(col.TypeName)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		schema.Columns = append(schema.Columns, storage.ColumnDef{Name: col.Name, DataType: dt, IsLowCardinality: isLC})
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

func executeCreateMaterializedView(stmt *parser.CreateMaterializedViewStmt, db *storage.Database) (*ExecuteResult, error) {
	if stmt.Select == nil {
		return nil, fmt.Errorf("materialized view requires AS SELECT query")
	}
	mv := storage.MaterializedView{
		Name:        stmt.ViewName,
		SourceTable: stmt.Select.From,
		TargetTable: stmt.TargetTable,
		SelectSQL:   parser.SelectToSQL(stmt.Select),
	}
	if err := db.CreateMaterializedView(mv); err != nil {
		if stmt.IfNotExists && strings.Contains(err.Error(), "already exists") {
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
		if colDef.IsLowCardinality {
			cols[i] = column.NewLowCardinalityColumn(colDef.DataType, len(stmt.Values))
		} else {
			cols[i] = column.NewColumnWithCapacity(colDef.DataType, len(stmt.Values))
		}
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

	// Compute partition IDs and insert.
	partitions, err := computePartitions(block, table.Schema.PartitionBy)
	if err != nil {
		return nil, err
	}
	if err := table.InsertPartitioned(partitions); err != nil {
		return nil, err
	}

	if err := applyMaterializedViews(stmt.TableName, block, db); err != nil {
		return nil, err
	}

	return &ExecuteResult{Message: fmt.Sprintf("OK. %d rows inserted.", len(stmt.Values))}, nil
}

// computePartitions evaluates a partition expression per row and splits the block
// into sub-blocks keyed by the string representation of the partition value.
func computePartitions(block *column.Block, partitionBySQL string) (map[string]*column.Block, error) {
	if partitionBySQL == "" {
		return map[string]*column.Block{"all": block}, nil
	}

	partExpr, err := parser.ParseExpression(partitionBySQL)
	if err != nil {
		return nil, fmt.Errorf("parsing partition expression %q: %w", partitionBySQL, err)
	}

	partRows := make(map[string][]int)
	for i := 0; i < block.NumRows(); i++ {
		val, dt, err := EvalExpr(partExpr, block, i)
		if err != nil {
			return nil, fmt.Errorf("evaluating partition expr at row %d: %w", i, err)
		}
		pid := types.ValueToString(dt, val)
		partRows[pid] = append(partRows[pid], i)
	}

	result := make(map[string]*column.Block, len(partRows))
	for pid, rows := range partRows {
		cols := make([]column.Column, block.NumColumns())
		for c := range block.NumColumns() {
			srcCol := block.Columns[c]
			var newCol column.Column
			if _, isLC := srcCol.(*column.LowCardinalityColumn); isLC {
				newCol = column.NewLowCardinalityColumn(srcCol.DataType(), len(rows))
			} else {
				newCol = column.NewColumnWithCapacity(srcCol.DataType(), len(rows))
			}
			for _, rowIdx := range rows {
				newCol.Append(srcCol.Value(rowIdx))
			}
			cols[c] = newCol
		}
		names := make([]string, len(block.ColumnNames))
		copy(names, block.ColumnNames)
		result[pid] = column.NewBlock(names, cols)
	}

	return result, nil
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

func applyMaterializedViews(sourceTable string, inserted *column.Block, db *storage.Database) error {
	views := db.MaterializedViewsForSource(sourceTable)
	for _, mv := range views {
		stmt, err := parser.ParseSQL(mv.SelectSQL)
		if err != nil {
			return fmt.Errorf("materialized view %s parse error: %w", mv.Name, err)
		}
		selectStmt, ok := stmt.(*parser.SelectStmt)
		if !ok {
			return fmt.Errorf("materialized view %s is not a SELECT", mv.Name)
		}

		outBlock, outNames, err := executeSelectOnBlock(selectStmt, inserted)
		if err != nil {
			return fmt.Errorf("materialized view %s execution error: %w", mv.Name, err)
		}
		if outBlock == nil || outBlock.NumRows() == 0 {
			continue
		}
		if len(outNames) > 0 {
			outBlock.ColumnNames = outNames
		}

		target, ok := db.GetTable(mv.TargetTable)
		if !ok {
			return fmt.Errorf("materialized view %s target table %s not found", mv.Name, mv.TargetTable)
		}

		reordered, err := reorderBlockToSchema(outBlock, target.Schema.ColumnNames())
		if err != nil {
			return fmt.Errorf("materialized view %s output mismatch: %w", mv.Name, err)
		}

		partitions, err := computePartitions(reordered, target.Schema.PartitionBy)
		if err != nil {
			return err
		}
		if err := target.InsertPartitioned(partitions); err != nil {
			return err
		}
	}
	return nil
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
