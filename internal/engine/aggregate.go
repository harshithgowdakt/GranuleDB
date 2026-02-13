package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/aggstate"
	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// AggregateFunc describes an aggregate function to compute.
type AggregateFunc struct {
	Name   string // "count", "sum", "sumState", "sumMerge", "min", "max", "avg", ...
	ArgCol string // column name argument (empty for count(*))
	Alias  string // output column name
	ParamF float64
	ParamI int64
}

// AggregateOperator implements GROUP BY with aggregation functions.
type AggregateOperator struct {
	input      Operator
	groupBy    []string
	aggregates []AggregateFunc
	done       bool
}

func NewAggregateOperator(input Operator, groupBy []string, aggregates []AggregateFunc) *AggregateOperator {
	return &AggregateOperator{
		input:      input,
		groupBy:    groupBy,
		aggregates: aggregates,
	}
}

func (a *AggregateOperator) Open() error {
	a.done = false
	return a.input.Open()
}

func (a *AggregateOperator) Next() (*column.Block, error) {
	if a.done {
		return nil, nil
	}
	a.done = true

	// Accumulate all rows
	type groupState struct {
		keys   []types.Value
		accums []Accumulator
	}

	groups := make(map[string]*groupState) // groupKey string -> state
	var groupOrder []string                // preserve insertion order

	for {
		block, err := a.input.Next()
		if err != nil {
			return nil, err
		}
		if block == nil {
			break
		}

		// Pre-resolve group-by columns once per block.
		gbCols := make([]column.Column, len(a.groupBy))
		for i, gb := range a.groupBy {
			col, ok := block.GetColumn(gb)
			if !ok {
				return nil, fmt.Errorf("GROUP BY column %s not found", gb)
			}
			gbCols[i] = col
		}

		// Pre-resolve aggregate argument columns once per block.
		aggCols := make([]column.Column, len(a.aggregates))
		aggDTs := make([]types.DataType, len(a.aggregates))
		for j, agg := range a.aggregates {
			if agg.ArgCol != "" {
				col, ok := block.GetColumn(agg.ArgCol)
				if !ok {
					return nil, fmt.Errorf("aggregate column %s not found", agg.ArgCol)
				}
				aggCols[j] = col
				aggDTs[j] = col.DataType()
			}
		}

		for row := range block.NumRows() {
			// Compute group key
			keyParts := make([]types.Value, len(a.groupBy))
			keyStr := ""
			for i := range a.groupBy {
				v := gbCols[i].Value(row)
				keyParts[i] = v
				if i > 0 {
					keyStr += "|"
				}
				keyStr += fmt.Sprintf("%v", v)
			}

			gs, ok := groups[keyStr]
			if !ok {
				accums := make([]Accumulator, len(a.aggregates))
				for j, agg := range a.aggregates {
					accums[j] = NewAccumulator(agg)
				}
				gs = &groupState{keys: keyParts, accums: accums}
				groups[keyStr] = gs
				groupOrder = append(groupOrder, keyStr)
			}

			// Feed values to accumulators
			for j, agg := range a.aggregates {
				if agg.Name == "count" && agg.ArgCol == "" {
					gs.accums[j].Add(int64(1), types.TypeInt64)
				} else if aggCols[j] != nil {
					gs.accums[j].Add(aggCols[j].Value(row), aggDTs[j])
				}
			}
		}
	}

	if len(a.groupBy) == 0 && len(groups) == 0 {
		// No GROUP BY and no rows: still produce one row for aggregates
		accums := make([]Accumulator, len(a.aggregates))
		for j, agg := range a.aggregates {
			accums[j] = NewAccumulator(agg)
		}
		gs := &groupState{keys: nil, accums: accums}
		groups[""] = gs
		groupOrder = append(groupOrder, "")
	}

	// Build result block
	numRows := len(groupOrder)
	numCols := len(a.groupBy) + len(a.aggregates)
	outNames := make([]string, numCols)
	outCols := make([]column.Column, numCols)

	// Group-by columns
	for i, gb := range a.groupBy {
		outNames[i] = gb
	}
	// Aggregate columns
	for i, agg := range a.aggregates {
		outNames[len(a.groupBy)+i] = agg.Alias
	}

	// Determine types from first group
	if numRows > 0 {
		firstGs := groups[groupOrder[0]]

		// Group-by column types - infer from values
		for i := range a.groupBy {
			dt := InferType(firstGs.keys[i])
			col := column.NewColumnWithCapacity(dt, numRows)
			for _, key := range groupOrder {
				gs := groups[key]
				col.Append(gs.keys[i])
			}
			outCols[i] = col
		}

		// Aggregate result columns
		for j := range a.aggregates {
			dt := firstGs.accums[j].ResultType()
			col := column.NewColumnWithCapacity(dt, numRows)
			for _, key := range groupOrder {
				gs := groups[key]
				col.Append(gs.accums[j].Result())
			}
			outCols[len(a.groupBy)+j] = col
		}
	}

	return column.NewBlock(outNames, outCols), nil
}

func (a *AggregateOperator) Close() error {
	return a.input.Close()
}

// --- Accumulator ---

// Accumulator tracks incremental state for an aggregate function.
type Accumulator interface {
	Add(v types.Value, dt types.DataType)
	Result() types.Value
	ResultType() types.DataType
}

// NewAccumulator creates an Accumulator by function name.
func NewAccumulator(agg AggregateFunc) Accumulator {
	switch strings.ToLower(agg.Name) {
	case "count":
		return &countAccum{}
	case "sum":
		return &sumAccum{}
	case "sumstate":
		return &sumStateAccum{}
	case "summerge":
		return &sumMergeAccum{}
	case "min":
		return &minAccum{}
	case "max":
		return &maxAccum{}
	case "avg":
		return &avgAccum{}
	case "uniq":
		return &uniqAccum{seen: make(map[string]struct{})}
	case "uniqstate":
		return &uniqStateAccum{hll: aggstate.NewHLL12()}
	case "uniqmerge":
		return &uniqMergeAccum{hll: aggstate.NewHLL12()}
	case "quantiles":
		q := agg.ParamF
		if q <= 0 || q >= 1 {
			q = 0.5
		}
		return &quantileAccum{q: q}
	case "topk":
		k := int(agg.ParamI)
		if k <= 0 {
			k = 10
		}
		return &topKAccum{k: k, counts: make(map[string]uint64)}
	default:
		return &countAccum{} // fallback
	}
}

type countAccum struct{ n uint64 }

func (a *countAccum) Add(_ types.Value, _ types.DataType) { a.n++ }
func (a *countAccum) Result() types.Value                 { return uint64(a.n) }
func (a *countAccum) ResultType() types.DataType          { return types.TypeUInt64 }

type sumAccum struct{ sum float64 }

func (a *sumAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.sum += f
	}
}
func (a *sumAccum) Result() types.Value        { return a.sum }
func (a *sumAccum) ResultType() types.DataType { return types.TypeFloat64 }

type sumStateAccum struct{ sum float64 }

func (a *sumStateAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.sum += f
	}
}
func (a *sumStateAccum) Result() types.Value        { return aggstate.EncodeSumState(a.sum) }
func (a *sumStateAccum) ResultType() types.DataType { return types.TypeAggregateState }

type sumMergeAccum struct{ sum float64 }

func (a *sumMergeAccum) Add(v types.Value, dt types.DataType) {
	if dt == types.TypeAggregateState {
		if s, ok := aggstate.DecodeSumState(v.([]byte)); ok {
			a.sum += s
			return
		}
	}
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.sum += f
	}
}
func (a *sumMergeAccum) Result() types.Value        { return a.sum }
func (a *sumMergeAccum) ResultType() types.DataType { return types.TypeFloat64 }

type minAccum struct {
	val   float64
	first bool
}

func (a *minAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		if !a.first || f < a.val {
			a.val = f
			a.first = true
		}
	}
}
func (a *minAccum) Result() types.Value {
	if !a.first {
		return float64(0)
	}
	return a.val
}
func (a *minAccum) ResultType() types.DataType { return types.TypeFloat64 }

type maxAccum struct {
	val   float64
	first bool
}

func (a *maxAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		if !a.first || f > a.val {
			a.val = f
			a.first = true
		}
	}
}
func (a *maxAccum) Result() types.Value {
	if !a.first {
		return float64(0)
	}
	return a.val
}
func (a *maxAccum) ResultType() types.DataType { return types.TypeFloat64 }

type avgAccum struct {
	sum   float64
	count float64
}

func (a *avgAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.sum += f
		a.count++
	}
}
func (a *avgAccum) Result() types.Value {
	if a.count == 0 {
		return math.NaN()
	}
	return a.sum / a.count
}
func (a *avgAccum) ResultType() types.DataType  { return types.TypeFloat64 }
func (a *avgAccum) SumCount() (float64, uint64) { return a.sum, uint64(a.count) }
func (a *avgAccum) MergePartial(sum float64, count uint64) {
	a.sum += sum
	a.count += float64(count)
}

// AvgState exposes raw sum/count for two-phase aggregation merging.
type AvgState interface {
	SumCount() (float64, uint64)
	MergePartial(sum float64, count uint64)
}

// MergeableAccumulator extends Accumulator with merge support for
// two-phase parallel aggregation.
type MergeableAccumulator interface {
	Accumulator
	// MergeValue merges a partial aggregate result value into this accumulator.
	MergeValue(v types.Value, dt types.DataType)
}

func (a *countAccum) MergeValue(v types.Value, _ types.DataType) {
	switch n := v.(type) {
	case uint64:
		a.n += n
	case int64:
		a.n += uint64(n)
	case float64:
		a.n += uint64(n)
	}
}

func (a *sumAccum) MergeValue(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.sum += f
	}
}

func (a *minAccum) MergeValue(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		if !a.first || f < a.val {
			a.val = f
			a.first = true
		}
	}
}

func (a *maxAccum) MergeValue(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		if !a.first || f > a.val {
			a.val = f
			a.first = true
		}
	}
}

type uniqAccum struct {
	seen map[string]struct{}
}

func (a *uniqAccum) Add(v types.Value, _ types.DataType) {
	a.seen[fmt.Sprintf("%v", v)] = struct{}{}
}
func (a *uniqAccum) Result() types.Value        { return uint64(len(a.seen)) }
func (a *uniqAccum) ResultType() types.DataType { return types.TypeUInt64 }

type uniqStateAccum struct{ hll *aggstate.HLL12 }

func (a *uniqStateAccum) Add(v types.Value, _ types.DataType) {
	a.hll.AddBytes([]byte(fmt.Sprintf("%v", v)))
}
func (a *uniqStateAccum) Result() types.Value {
	return aggstate.EncodeUniqHLLState(a.hll.MarshalBinary())
}
func (a *uniqStateAccum) ResultType() types.DataType { return types.TypeAggregateState }

type uniqMergeAccum struct {
	hll *aggstate.HLL12
}

func (a *uniqMergeAccum) Add(v types.Value, dt types.DataType) {
	if dt == types.TypeAggregateState {
		if b, ok := aggstate.DecodeUniqHLLState(v.([]byte)); ok {
			tmp := aggstate.NewHLL12()
			if tmp.UnmarshalBinary(b) {
				a.hll.Merge(tmp)
				return
			}
		}
	}
	a.hll.AddBytes([]byte(fmt.Sprintf("%v", v)))
}
func (a *uniqMergeAccum) Result() types.Value        { return a.hll.Estimate() }
func (a *uniqMergeAccum) ResultType() types.DataType { return types.TypeUInt64 }

type quantileAccum struct {
	q      float64
	values []float64
}

func (a *quantileAccum) Add(v types.Value, dt types.DataType) {
	f, err := types.ToFloat64(dt, v)
	if err == nil {
		a.values = append(a.values, f)
	}
}
func (a *quantileAccum) Result() types.Value {
	n := len(a.values)
	if n == 0 {
		return float64(0)
	}
	cp := make([]float64, n)
	copy(cp, a.values)
	sort.Float64s(cp)
	pos := int(a.q * float64(n-1))
	if pos < 0 {
		pos = 0
	}
	if pos >= n {
		pos = n - 1
	}
	return cp[pos]
}
func (a *quantileAccum) ResultType() types.DataType { return types.TypeFloat64 }

type topKAccum struct {
	k      int
	counts map[string]uint64
}

func (a *topKAccum) Add(v types.Value, _ types.DataType) {
	a.counts[fmt.Sprintf("%v", v)]++
}
func (a *topKAccum) Result() types.Value {
	type kv struct {
		key string
		cnt uint64
	}
	items := make([]kv, 0, len(a.counts))
	for k, c := range a.counts {
		items = append(items, kv{key: k, cnt: c})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].cnt != items[j].cnt {
			return items[i].cnt > items[j].cnt
		}
		return items[i].key < items[j].key
	})
	limit := a.k
	if limit > len(items) {
		limit = len(items)
	}
	out := make([]string, limit)
	for i := 0; i < limit; i++ {
		out[i] = items[i].key
	}
	return strings.Join(out, ",")
}
func (a *topKAccum) ResultType() types.DataType { return types.TypeString }

// InferType determines the DataType from a Go value.
func InferType(v types.Value) types.DataType {
	switch v.(type) {
	case uint8:
		return types.TypeUInt8
	case uint16:
		return types.TypeUInt16
	case uint32:
		return types.TypeUInt32
	case uint64:
		return types.TypeUInt64
	case int8:
		return types.TypeInt8
	case int16:
		return types.TypeInt16
	case int32:
		return types.TypeInt32
	case int64:
		return types.TypeInt64
	case float32:
		return types.TypeFloat32
	case float64:
		return types.TypeFloat64
	case string:
		return types.TypeString
	default:
		return types.TypeString
	}
}

// ExtractAggregates extracts aggregate functions from SELECT expressions.
func ExtractAggregates(selectExprs []parser.SelectExpr) []AggregateFunc {
	var aggs []AggregateFunc
	for _, se := range selectExprs {
		fc, ok := se.Expr.(*parser.FunctionCall)
		if !ok {
			continue
		}
		name := strings.ToLower(fc.Name)
		switch name {
		case "count", "sum", "sumstate", "summerge", "min", "max", "avg", "uniq", "uniqstate", "uniqmerge", "quantiles", "topk":
			argCol := ""
			paramF := float64(0)
			paramI := int64(0)

			// Defaults / arg conventions:
			// - quantiles(col) or quantiles(level, col)
			// - topK(col) or topK(k, col)
			if name == "quantiles" {
				paramF = 0.5
			}
			if name == "topk" {
				paramI = 10
			}

			switch len(fc.Args) {
			case 1:
				if ref, ok := fc.Args[0].(*parser.ColumnRef); ok {
					argCol = ref.Name
				}
			case 2:
				if lit, ok := fc.Args[0].(*parser.LiteralExpr); ok {
					switch name {
					case "quantiles":
						switch v := lit.Value.(type) {
						case int64:
							paramF = float64(v)
						case float64:
							paramF = v
						}
					case "topk":
						switch v := lit.Value.(type) {
						case int64:
							paramI = v
						case float64:
							paramI = int64(v)
						}
					}
				}
				if ref, ok := fc.Args[1].(*parser.ColumnRef); ok {
					argCol = ref.Name
				}
			}
			alias := se.Alias
			if alias == "" {
				alias = ExprName(se.Expr)
			}
			aggs = append(aggs, AggregateFunc{Name: name, ArgCol: argCol, Alias: alias, ParamF: paramF, ParamI: paramI})
		}
	}
	return aggs
}

// HasAggregates checks if any SELECT expression contains an aggregate function.
func HasAggregates(selectExprs []parser.SelectExpr) bool {
	for _, se := range selectExprs {
		if hasAggregateExpr(se.Expr) {
			return true
		}
	}
	return false
}

func hasAggregateExpr(e parser.Expression) bool {
	switch expr := e.(type) {
	case *parser.FunctionCall:
		name := strings.ToLower(expr.Name)
		switch name {
		case "count", "sum", "sumstate", "summerge", "min", "max", "avg", "uniq", "uniqstate", "uniqmerge", "quantiles", "topk":
			return true
		}
	case *parser.BinaryExpr:
		return hasAggregateExpr(expr.Left) || hasAggregateExpr(expr.Right)
	}
	return false
}

// CanUseTwoPhaseAggregation reports whether all aggregate functions are safe
// for the partial+merge aggregation pipeline.
func CanUseTwoPhaseAggregation(aggs []AggregateFunc) bool {
	for _, a := range aggs {
		switch strings.ToLower(a.Name) {
		case "count", "sum", "min", "max", "avg":
			// supported
		default:
			return false
		}
	}
	return true
}
