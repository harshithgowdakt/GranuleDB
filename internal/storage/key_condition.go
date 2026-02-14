package storage

import (
	"fmt"
	"log"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// Range represents a single-dimension range with optional inclusive bounds.
// A nil Left means unbounded below; a nil Right means unbounded above.
type Range struct {
	Left          types.Value
	Right         types.Value
	LeftIncluded  bool
	RightIncluded bool
	DataType      types.DataType
}

// Hyperrectangle is a multi-dimensional range — one Range per key column.
type Hyperrectangle []Range

// RPN element types for KeyCondition evaluation.
type rpnFunction uint8

const (
	rpnFunctionInRange    rpnFunction = iota // condition: column value is in range
	rpnFunctionNotInRange                    // condition: column value is NOT in range
	rpnFunctionUnknown                       // unknown condition — evaluates to MaskMaybe
	rpnFunctionAnd                           // logical AND of top two stack items
	rpnFunctionOr                            // logical OR of top two stack items
	rpnFunctionNot                           // logical NOT of top stack item
	rpnAlwaysTrue                            // constant true
	rpnAlwaysFalse                           // constant false
)

// rpnNode is a single element in the RPN (Reverse Polish Notation) program.
type rpnNode struct {
	function   rpnFunction
	keyColumn  int   // index into keyColumns (for InRange/NotInRange)
	rangeValue Range // the condition range (for InRange/NotInRange)
}

// KeyCondition compiles a WHERE expression into an RPN program that can be
// evaluated against index ranges (Hyperrectangles). This matches ClickHouse's
// KeyCondition class used for both partition and primary-key pruning.
type KeyCondition struct {
	rpn        []rpnNode
	keyColumns []string
	keyTypes   []types.DataType
}

// NewKeyCondition compiles a WHERE expression into an RPN-based KeyCondition.
// keyColumns are the column names (e.g., partition columns), keyTypes their
// respective DataTypes. Returns nil if where is nil or keyColumns is empty.
func NewKeyCondition(where parser.Expression, keyColumns []string, keyTypes []types.DataType) *KeyCondition {
	if where == nil || len(keyColumns) == 0 {
		return nil
	}

	kc := &KeyCondition{
		keyColumns: keyColumns,
		keyTypes:   keyTypes,
	}
	kc.compileExpr(where)

	if len(kc.rpn) == 0 {
		return nil
	}
	return kc
}

// CheckInHyperrectangle evaluates the compiled RPN condition against the given
// hyperrectangle (one Range per key column). Returns a BoolMask indicating
// whether the condition can be true/false for values within the rectangle.
func (kc *KeyCondition) CheckInHyperrectangle(hr Hyperrectangle) BoolMask {
	if len(kc.rpn) == 0 {
		return MaskMaybe
	}

	stack := make([]BoolMask, 0, len(kc.rpn))

	for _, node := range kc.rpn {
		switch node.function {
		case rpnFunctionInRange:
			if node.keyColumn < len(hr) {
				mask := checkRangeIntersection(node.rangeValue, hr[node.keyColumn])
				stack = append(stack, mask)
			} else {
				stack = append(stack, MaskMaybe)
			}
		case rpnFunctionNotInRange:
			if node.keyColumn < len(hr) {
				mask := checkRangeIntersection(node.rangeValue, hr[node.keyColumn])
				stack = append(stack, mask.Not())
			} else {
				stack = append(stack, MaskMaybe)
			}
		case rpnFunctionUnknown:
			stack = append(stack, MaskMaybe)
		case rpnAlwaysTrue:
			stack = append(stack, MaskAlwaysTrue)
		case rpnAlwaysFalse:
			stack = append(stack, MaskAlwaysFalse)
		case rpnFunctionAnd:
			if len(stack) >= 2 {
				b := stack[len(stack)-1]
				a := stack[len(stack)-2]
				stack = stack[:len(stack)-2]
				stack = append(stack, a.And(b))
			}
		case rpnFunctionOr:
			if len(stack) >= 2 {
				b := stack[len(stack)-1]
				a := stack[len(stack)-2]
				stack = stack[:len(stack)-2]
				stack = append(stack, a.Or(b))
			}
		case rpnFunctionNot:
			if len(stack) >= 1 {
				top := stack[len(stack)-1]
				stack[len(stack)-1] = top.Not()
			}
		}
	}

	if len(stack) == 0 {
		return MaskMaybe
	}
	return stack[len(stack)-1]
}

// compileExpr recursively walks the AST in post-order, building the RPN program.
func (kc *KeyCondition) compileExpr(expr parser.Expression) {
	if expr == nil {
		kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
		return
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		op := strings.ToUpper(e.Op)
		if op == "AND" {
			kc.compileExpr(e.Left)
			kc.compileExpr(e.Right)
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionAnd})
			return
		}
		if op == "OR" {
			kc.compileExpr(e.Left)
			kc.compileExpr(e.Right)
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionOr})
			return
		}

		// Try to extract column OP literal pattern.
		col, lit, flipped := extractColLit(e)
		if col == "" {
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
			return
		}

		actualOp := e.Op
		if flipped {
			actualOp = flipOperator(actualOp)
		}

		keyIdx := kc.findKeyColumn(col)
		if keyIdx < 0 {
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
			return
		}

		dt := kc.keyTypes[keyIdx]
		coerced, ok := types.CoerceValue(dt, lit)
		if !ok {
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
			return
		}

		r := opValueToRange(actualOp, coerced, dt)
		if r == nil {
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
			return
		}

		fn := rpnFunctionInRange
		if actualOp == "!=" {
			fn = rpnFunctionNotInRange
		}

		kc.rpn = append(kc.rpn, rpnNode{
			function:   fn,
			keyColumn:  keyIdx,
			rangeValue: *r,
		})

	case *parser.UnaryExpr:
		if strings.ToUpper(e.Op) == "NOT" {
			kc.compileExpr(e.Expr)
			kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionNot})
			return
		}
		kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})

	default:
		kc.rpn = append(kc.rpn, rpnNode{function: rpnFunctionUnknown})
	}
}

// findKeyColumn returns the index of the column in keyColumns, or -1.
func (kc *KeyCondition) findKeyColumn(name string) int {
	for i, col := range kc.keyColumns {
		if col == name {
			return i
		}
	}
	return -1
}

// opValueToRange converts an operator + literal into a Range.
func opValueToRange(op string, val types.Value, dt types.DataType) *Range {
	switch op {
	case "=":
		return &Range{Left: val, Right: val, LeftIncluded: true, RightIncluded: true, DataType: dt}
	case "!=":
		// For != we use InRange with the equality range, but the caller wraps it in rpnFunctionNotInRange.
		return &Range{Left: val, Right: val, LeftIncluded: true, RightIncluded: true, DataType: dt}
	case ">":
		return &Range{Left: val, Right: nil, LeftIncluded: false, RightIncluded: false, DataType: dt}
	case ">=":
		return &Range{Left: val, Right: nil, LeftIncluded: true, RightIncluded: false, DataType: dt}
	case "<":
		return &Range{Left: nil, Right: val, LeftIncluded: false, RightIncluded: false, DataType: dt}
	case "<=":
		return &Range{Left: nil, Right: val, LeftIncluded: false, RightIncluded: true, DataType: dt}
	default:
		return nil
	}
}

// checkRangeIntersection determines whether the condition range intersects with
// the index range. Returns a BoolMask:
//   - CanBeTrue:  the condition range overlaps with the index range
//   - CanBeFalse: the condition range does NOT fully cover the index range
func checkRangeIntersection(condRange, indexRange Range) BoolMask {
	dt := condRange.DataType
	cmp := types.CompareValues

	// Check if the condition range and index range overlap.
	// They DON'T overlap if condRange is entirely above or below indexRange.
	canBeTrue := true

	// If condRange has a lower bound, check if it's above the index's upper bound.
	if condRange.Left != nil && indexRange.Right != nil {
		c := cmp(dt, condRange.Left, indexRange.Right)
		if c > 0 {
			canBeTrue = false
		} else if c == 0 && !condRange.LeftIncluded && !indexRange.RightIncluded {
			// condRange starts strictly above the last index value.
			canBeTrue = false
		} else if c == 0 && (!condRange.LeftIncluded || !indexRange.RightIncluded) {
			canBeTrue = false
		}
	}

	// If condRange has an upper bound, check if it's below the index's lower bound.
	if condRange.Right != nil && indexRange.Left != nil {
		c := cmp(dt, condRange.Right, indexRange.Left)
		if c < 0 {
			canBeTrue = false
		} else if c == 0 && (!condRange.RightIncluded || !indexRange.LeftIncluded) {
			canBeTrue = false
		}
	}

	// Check if the condition range fully covers the index range.
	// If so, the condition is always true for all values in the index range.
	fullyCovered := true

	// condRange must start at or below the index lower bound.
	if condRange.Left != nil && indexRange.Left != nil {
		c := cmp(dt, condRange.Left, indexRange.Left)
		if c > 0 {
			fullyCovered = false
		} else if c == 0 && !condRange.LeftIncluded && indexRange.LeftIncluded {
			fullyCovered = false
		}
	} else if condRange.Left != nil && indexRange.Left == nil {
		// Index is unbounded below but condition isn't — can't fully cover.
		fullyCovered = false
	}

	// condRange must end at or above the index upper bound.
	if condRange.Right != nil && indexRange.Right != nil {
		c := cmp(dt, condRange.Right, indexRange.Right)
		if c < 0 {
			fullyCovered = false
		} else if c == 0 && !condRange.RightIncluded && indexRange.RightIncluded {
			fullyCovered = false
		}
	} else if condRange.Right != nil && indexRange.Right == nil {
		// Index is unbounded above but condition isn't — can't fully cover.
		fullyCovered = false
	}

	if !canBeTrue {
		return MaskAlwaysFalse
	}
	if fullyCovered {
		return MaskAlwaysTrue
	}
	return MaskMaybe
}

// extractColLit extracts a (columnName, literalValue, flipped) triple from a
// binary expression. Returns ("", nil, false) if the pattern doesn't match.
func extractColLit(e *parser.BinaryExpr) (colName string, litVal interface{}, flipped bool) {
	if ref, ok := e.Left.(*parser.ColumnRef); ok {
		if lit, ok := e.Right.(*parser.LiteralExpr); ok {
			return ref.Name, lit.Value, false
		}
	}
	if ref, ok := e.Right.(*parser.ColumnRef); ok {
		if lit, ok := e.Left.(*parser.LiteralExpr); ok {
			return ref.Name, lit.Value, true
		}
	}
	return "", nil, false
}

// NewKeyConditionForPrimaryKey builds a KeyCondition for primary key pruning.
// It extracts key column names and types from schema.OrderBy, then compiles
// the WHERE expression. Returns nil when no usable conditions exist.
func NewKeyConditionForPrimaryKey(where parser.Expression, schema *TableSchema) *KeyCondition {
	if where == nil || len(schema.OrderBy) == 0 {
		return nil
	}

	keyColumns := make([]string, 0, len(schema.OrderBy))
	keyTypes := make([]types.DataType, 0, len(schema.OrderBy))
	for _, col := range schema.OrderBy {
		colDef, ok := schema.GetColumnDef(col)
		if !ok {
			continue
		}
		keyColumns = append(keyColumns, col)
		keyTypes = append(keyTypes, colDef.DataType)
	}
	if len(keyColumns) == 0 {
		return nil
	}

	return NewKeyCondition(where, keyColumns, keyTypes)
}

// CheckInPrimaryIndex evaluates the RPN condition against each granule's range
// in the primary index, returning the pruned [begin, end) granule interval.
// It scans forward to find the first granule where the condition can be true,
// and backward to find the last such granule + 1.
func (kc *KeyCondition) CheckInPrimaryIndex(idx *PrimaryIndex) (begin, end int) {
	begin = 0
	end = idx.NumGranules

	if idx.NumGranules == 0 {
		log.Printf("[primary-key] index has 0 granules, nothing to prune")
		return 0, 0
	}

	log.Printf("[primary-key] evaluating %d granules against key columns %v", idx.NumGranules, kc.keyColumns)

	// Log granule boundaries for visibility.
	for g := 0; g < idx.NumGranules; g++ {
		vals := make([]string, len(idx.KeyColumns))
		for k := range idx.KeyColumns {
			vals[k] = fmt.Sprintf("%v", idx.Values[g][k])
		}
		var nextBound string
		if g+1 < idx.NumGranules {
			nvals := make([]string, len(idx.KeyColumns))
			for k := range idx.KeyColumns {
				nvals[k] = fmt.Sprintf("%v", idx.Values[g+1][k])
			}
			nextBound = fmt.Sprintf("[%s)", strings.Join(nvals, ","))
		} else {
			nextBound = "+∞)"
		}
		log.Printf("[primary-key]   granule %d: [%s, %s", g, strings.Join(vals, ","), nextBound)
	}

	// Forward scan: find first granule where condition CanBeTrue.
	for g := 0; g < idx.NumGranules; g++ {
		hr := kc.granuleToHyperrectangle(idx, g)
		mask := kc.CheckInHyperrectangle(hr)
		if mask.CanBeTrue {
			begin = g
			break
		}
		log.Printf("[primary-key]   granule %d: skipped (condition always false)", g)
		if g == idx.NumGranules-1 {
			// All granules pruned.
			log.Printf("[primary-key] all %d granules pruned — no matching data", idx.NumGranules)
			return idx.NumGranules, idx.NumGranules
		}
	}

	// Backward scan: find last granule where condition CanBeTrue.
	for g := idx.NumGranules - 1; g >= begin; g-- {
		hr := kc.granuleToHyperrectangle(idx, g)
		mask := kc.CheckInHyperrectangle(hr)
		if mask.CanBeTrue {
			end = g + 1
			break
		}
		log.Printf("[primary-key]   granule %d: skipped from tail (condition always false)", g)
	}

	selected := end - begin
	skipped := idx.NumGranules - selected
	log.Printf("[primary-key] selected granules [%d, %d) — %d/%d granules, %d skipped",
		begin, end, selected, idx.NumGranules, skipped)

	return begin, end
}

// granuleToHyperrectangle builds a Hyperrectangle for granule g from the
// primary index. For each key column k:
//   - Left  = idx.Values[g][k]   (granule boundary = first row)
//   - Right = idx.Values[g+1][k] if g+1 < NumGranules, else nil (unbounded)
//   - [Left, Right) — half-open interval; last granule is unbounded above.
func (kc *KeyCondition) granuleToHyperrectangle(idx *PrimaryIndex, g int) Hyperrectangle {
	hr := make(Hyperrectangle, len(kc.keyColumns))
	for i, col := range kc.keyColumns {
		dt := kc.keyTypes[i]

		// Find this key column's position in the primary index.
		keyIdx := -1
		for j, idxCol := range idx.KeyColumns {
			if idxCol == col {
				keyIdx = j
				break
			}
		}
		if keyIdx < 0 {
			// Column not in the primary index — unbounded range.
			hr[i] = Range{Left: nil, Right: nil, DataType: dt}
			continue
		}

		left := idx.Values[g][keyIdx]
		var right types.Value
		rightIncluded := false

		if g+1 < idx.NumGranules {
			right = idx.Values[g+1][keyIdx]
			// Half-open: [boundary, nextBoundary)
		} else {
			// Last granule: unbounded above.
			right = nil
		}

		hr[i] = Range{
			Left:          left,
			Right:         right,
			LeftIncluded:  true,
			RightIncluded: rightIncluded,
			DataType:      dt,
		}
	}
	return hr
}

// flipOperator inverts comparison operators when the literal is on the left.
func flipOperator(op string) string {
	switch op {
	case "<":
		return ">"
	case ">":
		return "<"
	case "<=":
		return ">="
	case ">=":
		return "<="
	default:
		return op
	}
}
