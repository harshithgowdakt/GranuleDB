package engine

import (
	"fmt"
	"log"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// PlanSelect converts a SELECT AST into an operator tree.
// Returns the root operator and the output column names.
func PlanSelect(stmt *parser.SelectStmt, db *storage.Database) (Operator, []string, error) {
	table, ok := db.GetTable(stmt.From)
	if !ok {
		return nil, nil, fmt.Errorf("table %s not found", stmt.From)
	}

	// Determine all columns needed from the table
	neededCols := CollectNeededColumns(stmt, &table.Schema)
	log.Printf("[planner] table=%s, columns needed: [%s]", stmt.From, strings.Join(neededCols, ", "))

	// Build KeyCondition for primary index pruning
	keyCond := storage.NewKeyConditionForPrimaryKey(stmt.Where, &table.Schema)
	if keyCond != nil {
		log.Printf("[planner] primary key condition built for ORDER BY %v", table.Schema.OrderBy)
	} else {
		log.Printf("[planner] no primary key condition (WHERE doesn't reference key columns %v)", table.Schema.OrderBy)
	}

	// Centralized partition pruning via storage.FilterParts
	allParts := table.GetActiveParts()
	parts, counters := storage.FilterParts(stmt.Where, &table.Schema, allParts)
	log.Printf("[planner] partition pruning: %d/%d parts survived", counters.SurvivedParts, counters.InitialParts)

	// Count total granules across selected parts.
	totalGranules := 0
	for _, p := range parts {
		totalGranules += p.NumGranules
	}
	log.Printf("[planner] scanning %d parts, %d total granules", len(parts), totalGranules)

	// Build operator chain bottom-up
	var op Operator

	// 1. Table scan (receives pre-filtered parts)
	op = NewTableScanOperator(table, neededCols, keyCond, parts)

	// 2. Filter (WHERE)
	if stmt.Where != nil {
		op = NewFilterOperator(op, stmt.Where)
	}

	// 3. Aggregate (GROUP BY or aggregate functions without GROUP BY)
	hasAggs := HasAggregates(stmt.Columns)
	if len(stmt.GroupBy) > 0 || hasAggs {
		aggs := ExtractAggregates(stmt.Columns)
		op = NewAggregateOperator(op, stmt.GroupBy, aggs)

		// After aggregation, build output names from group-by + aggregates
		outNames := make([]string, 0, len(stmt.GroupBy)+len(aggs))
		outNames = append(outNames, stmt.GroupBy...)
		for _, a := range aggs {
			outNames = append(outNames, a.Alias)
		}
		return wrapSortLimit(op, stmt, outNames)
	}

	// 4. Projection (SELECT columns)
	proj := NewProjectionOperator(op, stmt.Columns)
	op = proj

	// Determine output names
	outNames := make([]string, len(stmt.Columns))
	for i, se := range stmt.Columns {
		if se.Alias != "" {
			outNames[i] = se.Alias
		} else if _, ok := se.Expr.(*parser.StarExpr); ok {
			// Will be resolved at runtime
			return wrapSortLimit(op, stmt, table.Schema.ColumnNames())
		} else {
			outNames[i] = ExprName(se.Expr)
		}
	}

	return wrapSortLimit(op, stmt, outNames)
}

func wrapSortLimit(op Operator, stmt *parser.SelectStmt, outNames []string) (Operator, []string, error) {
	// Sort (ORDER BY)
	if len(stmt.OrderBy) > 0 {
		op = NewSortOperator(op, stmt.OrderBy)
	}

	// Limit
	if stmt.Limit != nil {
		op = NewLimitOperator(op, *stmt.Limit)
	}

	return op, outNames, nil
}

// CollectNeededColumns determines which table columns are needed.
func CollectNeededColumns(stmt *parser.SelectStmt, schema *storage.TableSchema) []string {
	needed := make(map[string]bool)

	// From SELECT expressions
	for _, se := range stmt.Columns {
		if _, ok := se.Expr.(*parser.StarExpr); ok {
			// Need all columns
			return schema.ColumnNames()
		}
		for _, col := range ExprReferencesColumns(se.Expr) {
			needed[col] = true
		}
	}

	// From WHERE
	if stmt.Where != nil {
		for _, col := range ExprReferencesColumns(stmt.Where) {
			needed[col] = true
		}
	}

	// From GROUP BY
	for _, col := range stmt.GroupBy {
		needed[col] = true
	}

	// From ORDER BY
	for _, ob := range stmt.OrderBy {
		needed[ob.Column] = true
	}

	// Ensure we have at least the columns referenced
	result := make([]string, 0, len(needed))
	for col := range needed {
		result = append(result, col)
	}

	if len(result) == 0 {
		return schema.ColumnNames()
	}
	return result
}
