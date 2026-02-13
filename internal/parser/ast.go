package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Statement is the top-level AST node.
type Statement interface {
	statementNode()
}

// --- Statements ---

// CreateTableStmt represents CREATE TABLE.
type CreateTableStmt struct {
	TableName   string
	IfNotExists bool
	Columns     []ColumnDefNode
	Engine      string     // "MergeTree", "AggregatingMergeTree", etc.
	OrderBy     []string   // primary key columns
	PartitionBy Expression // partition expression, or nil if not specified
}

func (*CreateTableStmt) statementNode() {}

// CreateMaterializedViewStmt represents CREATE MATERIALIZED VIEW ... TO ... AS SELECT ...
type CreateMaterializedViewStmt struct {
	ViewName    string
	IfNotExists bool
	TargetTable string
	Select      *SelectStmt
}

func (*CreateMaterializedViewStmt) statementNode() {}

// ColumnDefNode defines a column in a CREATE TABLE.
type ColumnDefNode struct {
	Name     string
	TypeName string
}

// InsertStmt represents INSERT INTO ... VALUES ...
type InsertStmt struct {
	TableName string
	Columns   []string       // explicit column list, or nil for all
	Values    [][]Expression // list of row-value-lists
}

func (*InsertStmt) statementNode() {}

// SelectStmt represents SELECT.
type SelectStmt struct {
	Columns []SelectExpr
	From    string // table name
	Where   Expression
	GroupBy []string
	OrderBy []OrderByExpr
	Limit   *int64
}

func (*SelectStmt) statementNode() {}

// SelectExpr represents a single item in the SELECT list.
type SelectExpr struct {
	Expr  Expression
	Alias string // AS alias, or empty
}

// OrderByExpr represents a single ORDER BY item.
type OrderByExpr struct {
	Column string
	Desc   bool
}

// DropTableStmt represents DROP TABLE.
type DropTableStmt struct {
	TableName string
	IfExists  bool
}

func (*DropTableStmt) statementNode() {}

// ShowTablesStmt represents SHOW TABLES.
type ShowTablesStmt struct{}

func (*ShowTablesStmt) statementNode() {}

// --- Expressions ---

// Expression is a node in an expression tree.
type Expression interface {
	exprNode()
}

// ColumnRef references a column by name.
type ColumnRef struct {
	Name string
}

func (*ColumnRef) exprNode() {}

// LiteralExpr is a literal value (int64, float64, or string).
type LiteralExpr struct {
	Value interface{} // int64, float64, or string
}

func (*LiteralExpr) exprNode() {}

// BinaryExpr is a binary operation.
type BinaryExpr struct {
	Op    string // +, -, *, /, =, !=, <, >, <=, >=, AND, OR
	Left  Expression
	Right Expression
}

func (*BinaryExpr) exprNode() {}

// UnaryExpr is a unary operation.
type UnaryExpr struct {
	Op   string // NOT, -
	Expr Expression
}

func (*UnaryExpr) exprNode() {}

// FunctionCall represents a function invocation.
type FunctionCall struct {
	Name string       // count, sum, min, max, avg, etc.
	Args []Expression // arguments
}

func (*FunctionCall) exprNode() {}

// StarExpr represents * in SELECT *.
type StarExpr struct{}

func (*StarExpr) exprNode() {}

// ExprToSQL converts an Expression AST back to its SQL text representation.
func ExprToSQL(expr Expression) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *ColumnRef:
		return e.Name
	case *LiteralExpr:
		switch v := e.Value.(type) {
		case string:
			return "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case int64:
			return strconv.FormatInt(v, 10)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		default:
			return fmt.Sprintf("%v", v)
		}
	case *FunctionCall:
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			args[i] = ExprToSQL(a)
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	case *BinaryExpr:
		return ExprToSQL(e.Left) + " " + e.Op + " " + ExprToSQL(e.Right)
	case *UnaryExpr:
		return e.Op + " " + ExprToSQL(e.Expr)
	case *StarExpr:
		return "*"
	default:
		return "?"
	}
}
