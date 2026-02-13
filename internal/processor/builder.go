package processor

import (
	"fmt"

	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/storage"
)

// PipelineResult holds the built pipeline and metadata.
type PipelineResult struct {
	Graph    *ExecutingGraph
	Output   *OutputProcessor
	OutNames []string
}

// BuildPipeline constructs a processor DAG from a SELECT statement.
//
// DAG structure:
//
//	[Source_0] ──┐
//	[Source_1] ──┼── [Concat] ── [Filter?] ── [Agg|Proj] ── [Sort?] ── [Limit?] ── [Output]
//	[Source_N] ──┘
func BuildPipeline(stmt *parser.SelectStmt, db *storage.Database) (*PipelineResult, error) {
	table, ok := db.GetTable(stmt.From)
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.From)
	}

	neededCols := engine.CollectNeededColumns(stmt, &table.Schema)
	keyRanges := engine.ExtractKeyRanges(stmt.Where, table.Schema.OrderBy)

	parts := table.GetActiveParts()

	var allProcs []Processor
	var allEdges []Edge

	addProc := func(p Processor) int {
		idx := len(allProcs)
		allProcs = append(allProcs, p)
		return idx
	}

	// 1. Create source processors (one per part).
	sourceIndices := make([]int, len(parts))
	for i, part := range parts {
		src := NewSourceProcessor(table, part, neededCols, keyRanges)
		sourceIndices[i] = addProc(src)
	}

	// 2. Concat or direct wire.
	var lastProcIdx int
	if len(parts) == 0 {
		concat := NewConcatProcessor(0)
		lastProcIdx = addProc(concat)
	} else if len(parts) == 1 {
		lastProcIdx = sourceIndices[0]
	} else {
		concat := NewConcatProcessor(len(parts))
		concatIdx := addProc(concat)
		for i, srcIdx := range sourceIndices {
			allEdges = append(allEdges, Edge{
				OutputProcessor: srcIdx,
				OutputPortIdx:   0,
				InputProcessor:  concatIdx,
				InputPortIdx:    i,
			})
		}
		lastProcIdx = concatIdx
	}

	// Helper: chain a processor to the current tail.
	chain := func(proc Processor) int {
		idx := addProc(proc)
		allEdges = append(allEdges, Edge{
			OutputProcessor: lastProcIdx,
			OutputPortIdx:   0,
			InputProcessor:  idx,
			InputPortIdx:    0,
		})
		lastProcIdx = idx
		return idx
	}

	// 3. Filter (WHERE).
	if stmt.Where != nil {
		chain(NewFilterProcessor(stmt.Where))
	}

	// 4. Aggregate or Projection.
	hasAggs := engine.HasAggregates(stmt.Columns)
	var outNames []string

	if len(stmt.GroupBy) > 0 || hasAggs {
		aggs := engine.ExtractAggregates(stmt.Columns)
		chain(NewAggregateProcessor(stmt.GroupBy, aggs))

		outNames = make([]string, 0, len(stmt.GroupBy)+len(aggs))
		outNames = append(outNames, stmt.GroupBy...)
		for _, a := range aggs {
			outNames = append(outNames, a.Alias)
		}
	} else {
		chain(NewProjectionProcessor(stmt.Columns))

		outNames = make([]string, len(stmt.Columns))
		for i, se := range stmt.Columns {
			if se.Alias != "" {
				outNames[i] = se.Alias
			} else if _, ok := se.Expr.(*parser.StarExpr); ok {
				outNames = table.Schema.ColumnNames()
				break
			} else {
				outNames[i] = engine.ExprName(se.Expr)
			}
		}
	}

	// 5. Sort (ORDER BY).
	if len(stmt.OrderBy) > 0 {
		chain(NewSortProcessor(stmt.OrderBy))
	}

	// 6. Limit.
	if stmt.Limit != nil {
		chain(NewLimitProcessor(*stmt.Limit))
	}

	// 7. Output sink.
	output := NewOutputProcessor()
	chain(output)

	// Wire all port connections.
	for _, e := range allEdges {
		outProc := allProcs[e.OutputProcessor]
		inProc := allProcs[e.InputProcessor]
		Connect(outProc.Outputs()[e.OutputPortIdx], inProc.Inputs()[e.InputPortIdx])
	}

	graph := NewExecutingGraph(allProcs, allEdges)

	return &PipelineResult{
		Graph:    graph,
		Output:   output,
		OutNames: outNames,
	}, nil
}
