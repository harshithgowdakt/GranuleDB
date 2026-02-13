package processor

import (
	"fmt"
	"math"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// MergeAggregateProcessor merges partial aggregate results from N
// PartialAggregateProcessors into a single final result.
//
// It has N input ports (one per partial aggregation stream) and 1 output port.
// It accumulates all partial results, then merges them by group key:
//   - count: sum partial counts
//   - sum: sum partial sums
//   - min: take min of partial mins
//   - max: take max of partial maxes
//   - avg: sum partial sums + sum partial counts, divide at end
type MergeAggregateProcessor struct {
	BaseProcessor
	groupBy    []string
	aggregates []engine.AggregateFunc
	numInputs  int

	// Merge state.
	groups      map[string]*mergeGroupState
	groupOrder  []string
	resultChunk *Chunk
	pushed      bool
	phase       int // 0=accumulate, 1=build, 2=push, 3=wait+finish
}

type mergeGroupState struct {
	keys   []types.Value
	accums []engine.Accumulator
}

// NewMergeAggregateProcessor creates a merge aggregation processor with
// numInputs input ports.
func NewMergeAggregateProcessor(numInputs int, groupBy []string, aggregates []engine.AggregateFunc) *MergeAggregateProcessor {
	return &MergeAggregateProcessor{
		BaseProcessor: NewBaseProcessor("MergeAggregate", numInputs, 1),
		groupBy:       groupBy,
		aggregates:    aggregates,
		numInputs:     numInputs,
		groups:        make(map[string]*mergeGroupState),
	}
}

func (m *MergeAggregateProcessor) Prepare() Status {
	switch m.phase {
	case 0: // Accumulate from all inputs
		// Check if any input has data.
		for i := 0; i < m.numInputs; i++ {
			inp := m.Input(i)
			if inp.HasData() {
				return StatusReady
			}
		}
		// Check if all inputs are finished.
		allFinished := true
		for i := 0; i < m.numInputs; i++ {
			if !m.Input(i).IsFinished() {
				allFinished = false
				break
			}
		}
		if allFinished {
			// Handle empty case: no GROUP BY and no groups → produce defaults.
			if len(m.groupBy) == 0 && len(m.groups) == 0 {
				accums := make([]engine.Accumulator, len(m.aggregates))
				for j, agg := range m.aggregates {
					accums[j] = engine.NewAccumulator(agg)
				}
				m.groups[""] = &mergeGroupState{keys: nil, accums: accums}
				m.groupOrder = append(m.groupOrder, "")
			}
			m.phase = 1
			return StatusReady
		}
		return StatusNeedData

	case 1: // Build result (Work)
		return StatusReady

	case 2: // Push
		out := m.Output(0)
		if m.resultChunk != nil {
			if out.CanPush() {
				out.Push(m.resultChunk)
				m.resultChunk = nil
				m.pushed = true
				m.phase = 3
				return StatusPortFull
			}
			return StatusPortFull
		}
		m.phase = 3
		return StatusPortFull

	case 3: // Wait for consume then finish
		out := m.Output(0)
		if m.pushed && !out.CanPush() {
			return StatusPortFull
		}
		m.phase = 4
		out.SetFinished()
		return StatusFinished

	default:
		return StatusFinished
	}
}

func (m *MergeAggregateProcessor) Work() {
	if m.phase == 0 {
		m.mergePartials()
		return
	}
	if m.phase == 1 {
		m.buildFinalResult()
		return
	}
}

// mergePartials pulls one available chunk from any input and merges it.
func (m *MergeAggregateProcessor) mergePartials() {
	for i := 0; i < m.numInputs; i++ {
		inp := m.Input(i)
		if !inp.HasData() {
			continue
		}
		chunk := inp.Pull()
		if chunk == nil || chunk.Block == nil || chunk.Block.NumRows() == 0 {
			continue
		}
		m.mergeBlock(chunk.Block)
		return // process one chunk per Work() call
	}
}

// mergeBlock merges a partial result block into the merge state.
// The block has group-by columns followed by aggregate columns.
// For avg, partial results have __sum and __count columns.
func (m *MergeAggregateProcessor) mergeBlock(block *column.Block) {
	// Resolve group-by columns.
	gbCols := make([]column.Column, len(m.groupBy))
	for i, gb := range m.groupBy {
		gbCols[i], _ = block.GetColumn(gb)
	}

	// Resolve aggregate columns. For avg, we need two columns.
	type aggColRef struct {
		col  column.Column
		col2 column.Column // second column for avg (__count)
	}
	aggRefs := make([]aggColRef, len(m.aggregates))
	for j, agg := range m.aggregates {
		if agg.Name == "avg" {
			aggRefs[j].col, _ = block.GetColumn(agg.Alias + "__sum")
			aggRefs[j].col2, _ = block.GetColumn(agg.Alias + "__count")
		} else {
			aggRefs[j].col, _ = block.GetColumn(agg.Alias)
		}
	}

	for row := 0; row < block.NumRows(); row++ {
		// Build group key.
		keyParts := make([]types.Value, len(m.groupBy))
		keyStr := ""
		for i := range m.groupBy {
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

		gs, ok := m.groups[keyStr]
		if !ok {
			accums := make([]engine.Accumulator, len(m.aggregates))
			for j, agg := range m.aggregates {
				accums[j] = engine.NewAccumulator(agg)
			}
			gs = &mergeGroupState{keys: keyParts, accums: accums}
			m.groups[keyStr] = gs
			m.groupOrder = append(m.groupOrder, keyStr)
		}

		// Merge each aggregate's partial value.
		for j, agg := range m.aggregates {
			if agg.Name == "avg" {
				// Merge sum and count into the avg accumulator.
				if aggRefs[j].col != nil && aggRefs[j].col2 != nil {
					sumVal := aggRefs[j].col.Value(row)
					cntVal := aggRefs[j].col2.Value(row)
					if avgAcc, ok := gs.accums[j].(engine.AvgState); ok {
						s, _ := types.ToFloat64(types.TypeFloat64, sumVal)
						var c uint64
						switch cv := cntVal.(type) {
						case uint64:
							c = cv
						case float64:
							c = uint64(cv)
						case int64:
							c = uint64(cv)
						}
						avgAcc.MergePartial(s, c)
					}
				}
			} else {
				// For count, sum, min, max: use MergeValue.
				if aggRefs[j].col != nil {
					v := aggRefs[j].col.Value(row)
					dt := aggRefs[j].col.DataType()
					if ma, ok := gs.accums[j].(engine.MergeableAccumulator); ok {
						ma.MergeValue(v, dt)
					}
				}
			}
		}
	}
}

// buildFinalResult constructs the final output block with computed aggregates.
func (m *MergeAggregateProcessor) buildFinalResult() {
	numRows := len(m.groupOrder)
	numCols := len(m.groupBy) + len(m.aggregates)

	outNames := make([]string, numCols)
	outCols := make([]column.Column, numCols)

	for i, gb := range m.groupBy {
		outNames[i] = gb
	}
	for i, agg := range m.aggregates {
		outNames[len(m.groupBy)+i] = agg.Alias
	}

	if numRows > 0 {
		firstGs := m.groups[m.groupOrder[0]]

		for i := range m.groupBy {
			dt := engine.InferType(firstGs.keys[i])
			col := column.NewColumnWithCapacity(dt, numRows)
			for _, key := range m.groupOrder {
				gs := m.groups[key]
				col.Append(gs.keys[i])
			}
			outCols[i] = col
		}

		for j := range m.aggregates {
			dt := firstGs.accums[j].ResultType()
			col := column.NewColumnWithCapacity(dt, numRows)
			for _, key := range m.groupOrder {
				gs := m.groups[key]
				result := gs.accums[j].Result()
				// Ensure NaN doesn't propagate for avg with 0 count.
				if f, ok := result.(float64); ok && math.IsNaN(f) {
					col.Append(float64(0))
				} else {
					col.Append(result)
				}
			}
			outCols[len(m.groupBy)+j] = col
		}
	}

	m.resultChunk = NewChunk(column.NewBlock(outNames, outCols))
	m.phase = 2
}
