package processor

import (
	"fmt"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/types"
)

// PartialAggregateProcessor performs per-part aggregation for parallel
// two-phase aggregation. Each instance builds its own private hash table
// and emits partial results when its input stream finishes.
//
// For the merge phase, partial results are encoded as raw accumulator state:
//   - count: uint64 (partial count)
//   - sum: float64 (partial sum)
//   - min: float64 (partial min)
//   - max: float64 (partial max)
//   - avg: two columns — sum (float64) and count (uint64)
//
// This processor has 1 input and 1 output.
type PartialAggregateProcessor struct {
	BaseProcessor
	groupBy    []string
	aggregates []engine.AggregateFunc

	groups      map[string]*partialGroupState
	groupOrder  []string
	resultChunk *Chunk
	pushed      bool
	phase       int // 0=accumulate, 1=build, 2=push, 3=wait+finish
}

type partialGroupState struct {
	keys   []types.Value
	accums []engine.Accumulator
}

// NewPartialAggregateProcessor creates a per-part partial aggregation processor.
func NewPartialAggregateProcessor(groupBy []string, aggregates []engine.AggregateFunc) *PartialAggregateProcessor {
	return &PartialAggregateProcessor{
		BaseProcessor: NewBaseProcessor("PartialAggregate", 1, 1),
		groupBy:       groupBy,
		aggregates:    aggregates,
		groups:        make(map[string]*partialGroupState),
	}
}

func (p *PartialAggregateProcessor) Prepare() Status {
	switch p.phase {
	case 0: // Accumulate
		inp := p.Input(0)
		if inp.HasData() {
			return StatusReady
		}
		if inp.IsFinished() {
			p.phase = 1
			return StatusReady
		}
		return StatusNeedData

	case 1: // Build result (Work)
		return StatusReady

	case 2: // Push
		out := p.Output(0)
		if p.resultChunk != nil {
			if out.CanPush() {
				out.Push(p.resultChunk)
				p.resultChunk = nil
				p.pushed = true
				p.phase = 3
				return StatusPortFull
			}
			return StatusPortFull
		}
		p.phase = 3
		return StatusPortFull

	case 3: // Wait for consume then finish
		out := p.Output(0)
		if p.pushed && !out.CanPush() {
			return StatusPortFull
		}
		p.phase = 4
		out.SetFinished()
		return StatusFinished

	default:
		return StatusFinished
	}
}

func (p *PartialAggregateProcessor) Work() {
	if p.phase == 0 {
		p.accumulate()
		return
	}

	if p.phase == 1 {
		p.buildPartialResult()
		return
	}
}

func (p *PartialAggregateProcessor) accumulate() {
	inp := p.Input(0)
	chunk := inp.Pull()
	if chunk == nil {
		return
	}
	block := chunk.Block

	// Pre-resolve group-by columns once per block.
	gbCols := make([]column.Column, len(p.groupBy))
	for i, gb := range p.groupBy {
		gbCols[i], _ = block.GetColumn(gb)
	}

	// Pre-resolve aggregate argument columns once per block.
	aggCols := make([]column.Column, len(p.aggregates))
	aggDTs := make([]types.DataType, len(p.aggregates))
	for j, agg := range p.aggregates {
		if agg.ArgCol != "" {
			col, ok := block.GetColumn(agg.ArgCol)
			if ok {
				aggCols[j] = col
				aggDTs[j] = col.DataType()
			}
		}
	}

	for row := 0; row < block.NumRows(); row++ {
		keyParts := make([]types.Value, len(p.groupBy))
		keyStr := ""
		for i := range p.groupBy {
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

		gs, ok := p.groups[keyStr]
		if !ok {
			accums := make([]engine.Accumulator, len(p.aggregates))
			for j, agg := range p.aggregates {
				accums[j] = engine.NewAccumulator(agg)
			}
			gs = &partialGroupState{keys: keyParts, accums: accums}
			p.groups[keyStr] = gs
			p.groupOrder = append(p.groupOrder, keyStr)
		}

		for j, agg := range p.aggregates {
			if agg.Name == "count" && agg.ArgCol == "" {
				gs.accums[j].Add(int64(1), types.TypeInt64)
			} else if aggCols[j] != nil {
				gs.accums[j].Add(aggCols[j].Value(row), aggDTs[j])
			}
		}
	}
}

// buildPartialResult emits raw accumulator state so MergeAggregateProcessor
// can combine partial results correctly. For avg, we emit sum and count
// as separate columns rather than the final average.
func (p *PartialAggregateProcessor) buildPartialResult() {
	numRows := len(p.groupOrder)

	// Count output columns: group-by cols + per-aggregate cols.
	// avg produces 2 columns (sum, count), all others produce 1.
	numAggCols := 0
	for _, agg := range p.aggregates {
		if agg.Name == "avg" {
			numAggCols += 2
		} else {
			numAggCols++
		}
	}
	numCols := len(p.groupBy) + numAggCols

	outNames := make([]string, 0, numCols)
	outCols := make([]column.Column, 0, numCols)

	// Group-by column names.
	for _, gb := range p.groupBy {
		outNames = append(outNames, gb)
	}

	// Aggregate column names — avg gets two columns.
	for _, agg := range p.aggregates {
		if agg.Name == "avg" {
			outNames = append(outNames, agg.Alias+"__sum")
			outNames = append(outNames, agg.Alias+"__count")
		} else {
			outNames = append(outNames, agg.Alias)
		}
	}

	if numRows > 0 {
		firstGs := p.groups[p.groupOrder[0]]

		// Build group-by columns.
		for i := range p.groupBy {
			dt := engine.InferType(firstGs.keys[i])
			col := column.NewColumnWithCapacity(dt, numRows)
			for _, key := range p.groupOrder {
				gs := p.groups[key]
				col.Append(gs.keys[i])
			}
			outCols = append(outCols, col)
		}

		// Build aggregate columns — emit raw accumulator state.
		for j, agg := range p.aggregates {
			switch agg.Name {
			case "avg":
				// Emit sum and count separately for merging.
				sumCol := column.NewColumnWithCapacity(types.TypeFloat64, numRows)
				cntCol := column.NewColumnWithCapacity(types.TypeUInt64, numRows)
				for _, key := range p.groupOrder {
					gs := p.groups[key]
					sum, count := gs.accums[j].(engine.AvgState).SumCount()
					sumCol.Append(sum)
					cntCol.Append(count)
				}
				outCols = append(outCols, sumCol, cntCol)
			default:
				// count, sum, min, max — emit Result() directly.
				dt := firstGs.accums[j].ResultType()
				col := column.NewColumnWithCapacity(dt, numRows)
				for _, key := range p.groupOrder {
					gs := p.groups[key]
					col.Append(gs.accums[j].Result())
				}
				outCols = append(outCols, col)
			}
		}
	}

	p.resultChunk = NewChunk(column.NewBlock(outNames, outCols))
	p.phase = 2
}
