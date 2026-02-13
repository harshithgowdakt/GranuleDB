package processor

import (
	"fmt"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// AggregateProcessor implements hash-based GROUP BY aggregation.
// Phase 0: accumulate, Phase 1: build result, Phase 2: push, Phase 3: wait+finish.
type AggregateProcessor struct {
	BaseProcessor
	groupBy    []string
	aggregates []engine.AggregateFunc

	groups      map[string]*aggGroupState
	groupOrder  []string
	resultChunk *Chunk
	pushed      bool
	phase       int
}

type aggGroupState struct {
	keys   []types.Value
	accums []engine.Accumulator
}

// NewAggregateProcessor creates an aggregate processor.
func NewAggregateProcessor(groupBy []string, aggregates []engine.AggregateFunc) *AggregateProcessor {
	return &AggregateProcessor{
		BaseProcessor: NewBaseProcessor("Aggregate", 1, 1),
		groupBy:       groupBy,
		aggregates:    aggregates,
		groups:        make(map[string]*aggGroupState),
	}
}

func (a *AggregateProcessor) Prepare() Status {
	switch a.phase {
	case 0: // Accumulate
		inp := a.Input(0)
		if inp.HasData() {
			return StatusReady
		}
		if inp.IsFinished() {
			if len(a.groupBy) == 0 && len(a.groups) == 0 {
				accums := make([]engine.Accumulator, len(a.aggregates))
				for j, agg := range a.aggregates {
					accums[j] = engine.NewAccumulator(agg)
				}
				a.groups[""] = &aggGroupState{keys: nil, accums: accums}
				a.groupOrder = append(a.groupOrder, "")
			}
			a.phase = 1
			return StatusReady
		}
		return StatusNeedData

	case 1: // Build result (Work)
		return StatusReady

	case 2: // Push
		out := a.Output(0)
		if a.resultChunk != nil {
			if out.CanPush() {
				out.Push(a.resultChunk)
				a.resultChunk = nil
				a.pushed = true
				a.phase = 3
				return StatusPortFull // wait for downstream to consume
			}
			return StatusPortFull
		}
		a.phase = 3
		return StatusPortFull

	case 3: // Wait for consume then finish
		out := a.Output(0)
		if a.pushed && !out.CanPush() {
			return StatusPortFull
		}
		a.phase = 4
		out.SetFinished()
		return StatusFinished

	default:
		return StatusFinished
	}
}

func (a *AggregateProcessor) Work() {
	if a.phase == 0 {
		inp := a.Input(0)
		chunk := inp.Pull()
		if chunk == nil {
			return
		}
		block := chunk.Block

		// Pre-resolve group-by columns once per block.
		gbCols := make([]column.Column, len(a.groupBy))
		for i, gb := range a.groupBy {
			gbCols[i], _ = block.GetColumn(gb)
		}

		// Pre-resolve aggregate argument columns once per block.
		aggCols := make([]column.Column, len(a.aggregates))
		aggDTs := make([]types.DataType, len(a.aggregates))
		for j, agg := range a.aggregates {
			if agg.ArgCol != "" {
				col, ok := block.GetColumn(agg.ArgCol)
				if ok {
					aggCols[j] = col
					aggDTs[j] = col.DataType()
				}
			}
		}

		for row := 0; row < block.NumRows(); row++ {
			keyParts := make([]types.Value, len(a.groupBy))
			keyStr := ""
			for i := range a.groupBy {
				if gbCols[i] == nil {
					continue
				}
				v := gbCols[i].Value(row)
				keyParts[i] = v
				if i > 0 {
					keyStr += "|"
				}
				keyStr += fmt.Sprintf("%v", v)
			}

			gs, ok := a.groups[keyStr]
			if !ok {
				accums := make([]engine.Accumulator, len(a.aggregates))
				for j, agg := range a.aggregates {
					accums[j] = engine.NewAccumulator(agg)
				}
				gs = &aggGroupState{keys: keyParts, accums: accums}
				a.groups[keyStr] = gs
				a.groupOrder = append(a.groupOrder, keyStr)
			}

			for j, agg := range a.aggregates {
				if agg.Name == "count" && agg.ArgCol == "" {
					gs.accums[j].Add(int64(1), types.TypeInt64)
				} else if aggCols[j] != nil {
					gs.accums[j].Add(aggCols[j].Value(row), aggDTs[j])
				}
			}
		}
		return
	}

	if a.phase == 1 {
		numRows := len(a.groupOrder)
		numCols := len(a.groupBy) + len(a.aggregates)
		outNames := make([]string, numCols)
		outCols := make([]column.Column, numCols)

		for i, gb := range a.groupBy {
			outNames[i] = gb
		}
		for i, agg := range a.aggregates {
			outNames[len(a.groupBy)+i] = agg.Alias
		}

		if numRows > 0 {
			firstGs := a.groups[a.groupOrder[0]]

			for i := range a.groupBy {
				dt := engine.InferType(firstGs.keys[i])
				col := column.NewColumnWithCapacity(dt, numRows)
				for _, key := range a.groupOrder {
					gs := a.groups[key]
					col.Append(gs.keys[i])
				}
				outCols[i] = col
			}

			for j := range a.aggregates {
				dt := firstGs.accums[j].ResultType()
				col := column.NewColumnWithCapacity(dt, numRows)
				for _, key := range a.groupOrder {
					gs := a.groups[key]
					col.Append(gs.accums[j].Result())
				}
				outCols[len(a.groupBy)+j] = col
			}
		}

		a.resultChunk = NewChunk(column.NewBlock(outNames, outCols))
		a.phase = 2
	}
}
