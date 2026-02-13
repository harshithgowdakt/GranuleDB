package engine

import (
	"fmt"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/parser"
)

// executeSelectOnBlock executes a SELECT against a single in-memory block.
// This powers insert-time materialized view transformations.
func executeSelectOnBlock(stmt *parser.SelectStmt, block *column.Block) (*column.Block, []string, error) {
	var op Operator = NewBlockSourceOperator(block)

	if stmt.Where != nil {
		op = NewFilterOperator(op, stmt.Where)
	}

	hasAggs := HasAggregates(stmt.Columns)
	if len(stmt.GroupBy) > 0 || hasAggs {
		aggs := ExtractAggregates(stmt.Columns)
		op = NewAggregateOperator(op, stmt.GroupBy, aggs)
		outNames := make([]string, 0, len(stmt.GroupBy)+len(aggs))
		outNames = append(outNames, stmt.GroupBy...)
		for _, a := range aggs {
			outNames = append(outNames, a.Alias)
		}
		return consumeSingleResult(op, stmt, outNames)
	}

	op = NewProjectionOperator(op, stmt.Columns)
	outNames := make([]string, len(stmt.Columns))
	for i, se := range stmt.Columns {
		if se.Alias != "" {
			outNames[i] = se.Alias
		} else if _, ok := se.Expr.(*parser.StarExpr); ok {
			outNames = block.ColumnNames
			break
		} else {
			outNames[i] = ExprName(se.Expr)
		}
	}
	return consumeSingleResult(op, stmt, outNames)
}

func consumeSingleResult(op Operator, stmt *parser.SelectStmt, outNames []string) (*column.Block, []string, error) {
	// Apply ORDER BY / LIMIT exactly like normal planning.
	var err error
	op, _, err = wrapSortLimit(op, stmt, outNames)
	if err != nil {
		return nil, nil, err
	}

	if err := op.Open(); err != nil {
		return nil, nil, err
	}
	defer op.Close()

	var out *column.Block
	for {
		b, err := op.Next()
		if err != nil {
			return nil, nil, err
		}
		if b == nil {
			break
		}
		if out == nil {
			out = b
		} else if err := out.AppendBlock(b); err != nil {
			return nil, nil, err
		}
	}

	if out == nil {
		return nil, outNames, nil
	}
	if out.NumColumns() != len(outNames) {
		return nil, nil, fmt.Errorf("output names/columns mismatch: %d vs %d", len(outNames), out.NumColumns())
	}
	return out, outNames, nil
}

func reorderBlockToSchema(block *column.Block, schemaCols []string) (*column.Block, error) {
	cols := make([]column.Column, len(schemaCols))
	for i, name := range schemaCols {
		col, ok := block.GetColumn(name)
		if !ok {
			return nil, fmt.Errorf("missing column %s", name)
		}
		cols[i] = col
	}
	names := make([]string, len(schemaCols))
	copy(names, schemaCols)
	return column.NewBlock(names, cols), nil
}
