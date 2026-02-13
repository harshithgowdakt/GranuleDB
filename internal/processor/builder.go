package processor

import (
	"fmt"

	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// PipelineResult holds the built pipeline and metadata.
type PipelineResult struct {
	Graph    *ExecutingGraph
	Output   *OutputProcessor
	OutNames []string
}

// BuildPipeline constructs a processor DAG from a SELECT statement.
//
// For non-aggregate queries (ClickHouse-like parallel scan):
//
//	[Source_0] ── [Filter_0] ── [Proj_0] ──┐
//	[Source_1] ── [Filter_1] ── [Proj_1] ──┼── [Concat] ── [Sort?] ── [Limit?] ── [Output]
//	[Source_N] ── [Filter_N] ── [Proj_N] ──┘
//
// For aggregate queries (two-phase parallel aggregation):
//
//	[Source_0] ── [Filter_0] ── [PartialAgg_0] ──┐
//	[Source_1] ── [Filter_1] ── [PartialAgg_1] ──┼── [MergeAgg] ── [Sort?] ── [Limit?] ── [Output]
//	[Source_N] ── [Filter_N] ── [PartialAgg_N] ──┘
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

	addEdge := func(outProc, outPort, inProc, inPort int) {
		allEdges = append(allEdges, Edge{
			OutputProcessor: outProc,
			OutputPortIdx:   outPort,
			InputProcessor:  inProc,
			InputPortIdx:    inPort,
		})
	}

	hasAggs := engine.HasAggregates(stmt.Columns)
	isAggQuery := len(stmt.GroupBy) > 0 || hasAggs
	aggs := engine.ExtractAggregates(stmt.Columns)
	useTwoPhaseAgg := isAggQuery && engine.CanUseTwoPhaseAggregation(aggs)

	var outNames []string
	var lastProcIdx int

	if len(parts) == 0 {
		// Empty table — build minimal pipeline.
		if isAggQuery {
			if useTwoPhaseAgg {
				// Single MergeAgg with 0 inputs handles empty-table aggregate.
				mergeIdx := addProc(NewMergeAggregateProcessor(0, stmt.GroupBy, aggs))
				lastProcIdx = mergeIdx
			} else {
				concatIdx := addProc(NewConcatProcessor(0))
				aggIdx := addProc(NewAggregateProcessor(stmt.GroupBy, aggs))
				addEdge(concatIdx, 0, aggIdx, 0)
				lastProcIdx = aggIdx
			}

			outNames = make([]string, 0, len(stmt.GroupBy)+len(aggs))
			outNames = append(outNames, stmt.GroupBy...)
			for _, a := range aggs {
				outNames = append(outNames, a.Alias)
			}
		} else {
			concat := NewConcatProcessor(0)
			concatIdx := addProc(concat)

			projIdx := addProc(NewProjectionProcessor(stmt.Columns))
			addEdge(concatIdx, 0, projIdx, 0)
			lastProcIdx = projIdx

			outNames = buildProjectionNames(stmt, table)
		}
	} else {
		// Build per-part parallel pipelines.
		perPartTails := make([]int, len(parts))

		for i, part := range parts {
			// Source for this part.
			sourceIdx := addProc(NewSourceProcessor(table, part, neededCols, keyRanges))
			tailIdx := sourceIdx

			// Per-part filter.
			if stmt.Where != nil {
				filterIdx := addProc(NewFilterProcessor(stmt.Where))
				addEdge(tailIdx, 0, filterIdx, 0)
				tailIdx = filterIdx
			}

			if isAggQuery {
				if useTwoPhaseAgg {
					// Per-part partial aggregation.
					partialIdx := addProc(NewPartialAggregateProcessor(stmt.GroupBy, aggs))
					addEdge(tailIdx, 0, partialIdx, 0)
					tailIdx = partialIdx
				}
			} else {
				// Per-part projection.
				projIdx := addProc(NewProjectionProcessor(stmt.Columns))
				addEdge(tailIdx, 0, projIdx, 0)
				tailIdx = projIdx
			}

			perPartTails[i] = tailIdx
		}

		if isAggQuery {
			if useTwoPhaseAgg {
				// Merge all partial aggregations.
				mergeIdx := addProc(NewMergeAggregateProcessor(len(parts), stmt.GroupBy, aggs))
				for i, tailIdx := range perPartTails {
					addEdge(tailIdx, 0, mergeIdx, i)
				}
				lastProcIdx = mergeIdx
			} else {
				concatIdx := addProc(NewConcatProcessor(len(parts)))
				for i, tailIdx := range perPartTails {
					addEdge(tailIdx, 0, concatIdx, i)
				}
				aggIdx := addProc(NewAggregateProcessor(stmt.GroupBy, aggs))
				addEdge(concatIdx, 0, aggIdx, 0)
				lastProcIdx = aggIdx
			}

			outNames = make([]string, 0, len(stmt.GroupBy)+len(aggs))
			outNames = append(outNames, stmt.GroupBy...)
			for _, a := range aggs {
				outNames = append(outNames, a.Alias)
			}
		} else {
			// Concat all per-part projection outputs.
			if len(parts) == 1 {
				lastProcIdx = perPartTails[0]
			} else {
				concatIdx := addProc(NewConcatProcessor(len(parts)))
				for i, tailIdx := range perPartTails {
					addEdge(tailIdx, 0, concatIdx, i)
				}
				lastProcIdx = concatIdx
			}

			outNames = buildProjectionNames(stmt, table)
		}
	}

	// Helper: chain a processor to the current tail.
	chain := func(proc Processor) int {
		idx := addProc(proc)
		addEdge(lastProcIdx, 0, idx, 0)
		lastProcIdx = idx
		return idx
	}

	// Sort (ORDER BY).
	if len(stmt.OrderBy) > 0 {
		chain(NewSortProcessor(stmt.OrderBy))
	}

	// Limit.
	if stmt.Limit != nil {
		chain(NewLimitProcessor(*stmt.Limit))
	}

	// Output sink.
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

// buildProjectionNames determines output column names for non-aggregate queries.
func buildProjectionNames(stmt *parser.SelectStmt, table *storage.MergeTreeTable) []string {
	outNames := make([]string, len(stmt.Columns))
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
	return outNames
}
