package parser

import (
	"strconv"
	"strings"
)

// SelectToSQL converts a SelectStmt AST back into SQL text.
func SelectToSQL(stmt *SelectStmt) string {
	if stmt == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, se := range stmt.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(ExprToSQL(se.Expr))
		if se.Alias != "" {
			sb.WriteString(" AS ")
			sb.WriteString(se.Alias)
		}
	}

	if stmt.From != "" {
		sb.WriteString(" FROM ")
		sb.WriteString(stmt.From)
	}

	if stmt.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(ExprToSQL(stmt.Where))
	}

	if len(stmt.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(stmt.GroupBy, ", "))
	}

	if len(stmt.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range stmt.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ob.Column)
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	if stmt.Limit != nil {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.FormatInt(*stmt.Limit, 10))
	}

	return sb.String()
}
