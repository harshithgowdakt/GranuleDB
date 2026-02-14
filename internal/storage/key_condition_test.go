package storage

import (
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// --- BoolMask tests ---

func TestBoolMaskAnd(t *testing.T) {
	tests := []struct {
		name string
		a, b BoolMask
		want BoolMask
	}{
		{"true AND true", MaskAlwaysTrue, MaskAlwaysTrue, MaskAlwaysTrue},
		{"true AND false", MaskAlwaysTrue, MaskAlwaysFalse, MaskAlwaysFalse},
		{"false AND true", MaskAlwaysFalse, MaskAlwaysTrue, MaskAlwaysFalse},
		{"false AND false", MaskAlwaysFalse, MaskAlwaysFalse, MaskAlwaysFalse},
		{"maybe AND true", MaskMaybe, MaskAlwaysTrue, MaskMaybe},
		{"maybe AND false", MaskMaybe, MaskAlwaysFalse, MaskAlwaysFalse},
		{"true AND maybe", MaskAlwaysTrue, MaskMaybe, MaskMaybe},
		{"maybe AND maybe", MaskMaybe, MaskMaybe, MaskMaybe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.And(tt.b)
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBoolMaskOr(t *testing.T) {
	tests := []struct {
		name string
		a, b BoolMask
		want BoolMask
	}{
		{"true OR true", MaskAlwaysTrue, MaskAlwaysTrue, MaskAlwaysTrue},
		{"true OR false", MaskAlwaysTrue, MaskAlwaysFalse, MaskAlwaysTrue},
		{"false OR true", MaskAlwaysFalse, MaskAlwaysTrue, MaskAlwaysTrue},
		{"false OR false", MaskAlwaysFalse, MaskAlwaysFalse, MaskAlwaysFalse},
		{"maybe OR true", MaskMaybe, MaskAlwaysTrue, MaskAlwaysTrue},
		{"maybe OR false", MaskMaybe, MaskAlwaysFalse, MaskMaybe},
		{"false OR maybe", MaskAlwaysFalse, MaskMaybe, MaskMaybe},
		{"maybe OR maybe", MaskMaybe, MaskMaybe, MaskMaybe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Or(tt.b)
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBoolMaskNot(t *testing.T) {
	tests := []struct {
		name string
		m    BoolMask
		want BoolMask
	}{
		{"NOT true", MaskAlwaysTrue, MaskAlwaysFalse},
		{"NOT false", MaskAlwaysFalse, MaskAlwaysTrue},
		{"NOT maybe", MaskMaybe, MaskMaybe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.m.Not()
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- checkRangeIntersection tests ---

func TestCheckRangeIntersectionDisjoint(t *testing.T) {
	// Condition: col = 100 (range [100,100])
	// Index range: [1, 50]
	// Expected: no intersection → AlwaysFalse
	cond := Range{Left: int64(100), Right: int64(100), LeftIncluded: true, RightIncluded: true, DataType: types.TypeInt64}
	idx := Range{Left: int64(1), Right: int64(50), LeftIncluded: true, RightIncluded: true, DataType: types.TypeInt64}

	mask := checkRangeIntersection(cond, idx)
	if mask.CanBeTrue {
		t.Error("expected CanBeTrue=false for disjoint ranges")
	}
	if !mask.CanBeFalse {
		t.Error("expected CanBeFalse=true for disjoint ranges")
	}
}

func TestCheckRangeIntersectionPartialOverlap(t *testing.T) {
	// Condition: col > 5 (range (5, +inf))
	// Index range: [1, 10]
	// Expected: partial overlap → MaskMaybe
	cond := Range{Left: int64(5), Right: nil, LeftIncluded: false, RightIncluded: false, DataType: types.TypeInt64}
	idx := Range{Left: int64(1), Right: int64(10), LeftIncluded: true, RightIncluded: true, DataType: types.TypeInt64}

	mask := checkRangeIntersection(cond, idx)
	if !mask.CanBeTrue {
		t.Error("expected CanBeTrue=true for partial overlap")
	}
	if !mask.CanBeFalse {
		t.Error("expected CanBeFalse=true for partial overlap")
	}
}

func TestCheckRangeIntersectionFullContainment(t *testing.T) {
	// Condition: col > 0 (range (0, +inf))
	// Index range: [5, 10]
	// Expected: fully covered → AlwaysTrue
	cond := Range{Left: int64(0), Right: nil, LeftIncluded: false, RightIncluded: false, DataType: types.TypeInt64}
	idx := Range{Left: int64(5), Right: int64(10), LeftIncluded: true, RightIncluded: true, DataType: types.TypeInt64}

	mask := checkRangeIntersection(cond, idx)
	if !mask.CanBeTrue {
		t.Error("expected CanBeTrue=true for full containment")
	}
	if mask.CanBeFalse {
		t.Error("expected CanBeFalse=false for full containment")
	}
}

// --- KeyCondition tests ---

// Helper: parse an expression from SQL.
func mustParseExpr(t *testing.T, sql string) parser.Expression {
	t.Helper()
	expr, err := parser.ParseExpression(sql)
	if err != nil {
		t.Fatalf("failed to parse expression %q: %v", sql, err)
	}
	return expr
}

// Helper: build a Hyperrectangle from min/max int64 values for a single column.
func int64HR(min, max int64, dt types.DataType) Hyperrectangle {
	return Hyperrectangle{
		{Left: min, Right: max, LeftIncluded: true, RightIncluded: true, DataType: dt},
	}
}

func TestKeyConditionEquality(t *testing.T) {
	// col = 5
	expr := mustParseExpr(t, "col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [1, 10] — contains 5 → Maybe (not full coverage)
	mask := kc.CheckInHyperrectangle(int64HR(1, 10, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[1,10] should CanBeTrue for col=5")
	}

	// Index [5, 5] — exactly 5 → AlwaysTrue
	mask = kc.CheckInHyperrectangle(int64HR(5, 5, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[5,5] should CanBeTrue for col=5")
	}
	if mask.CanBeFalse {
		t.Error("[5,5] should not CanBeFalse for col=5")
	}

	// Index [10, 20] — 5 not in range → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[10,20] should not CanBeTrue for col=5")
	}

	// Index [1, 3] — 5 not in range → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(1, 3, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,3] should not CanBeTrue for col=5")
	}
}

func TestKeyConditionGreaterThan(t *testing.T) {
	// col > 5
	expr := mustParseExpr(t, "col > 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [10, 20] — all values > 5 → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[10,20] should CanBeTrue for col>5")
	}
	if mask.CanBeFalse {
		t.Error("[10,20] should not CanBeFalse for col>5")
	}

	// Index [1, 3] — all values <=5 → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(1, 3, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,3] should not CanBeTrue for col>5")
	}

	// Index [1, 10] — partial overlap → Maybe
	mask = kc.CheckInHyperrectangle(int64HR(1, 10, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[1,10] should CanBeTrue for col>5")
	}
	if !mask.CanBeFalse {
		t.Error("[1,10] should CanBeFalse for col>5")
	}

	// Index [1, 5] — max=5, strictly greater → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(1, 5, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,5] should not CanBeTrue for col>5")
	}
}

func TestKeyConditionGreaterOrEqual(t *testing.T) {
	// col >= 5
	expr := mustParseExpr(t, "col >= 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})

	// Index [5, 10] — all >= 5 → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(5, 10, types.TypeInt64))
	if !mask.CanBeTrue || mask.CanBeFalse {
		t.Error("[5,10] should be AlwaysTrue for col>=5")
	}

	// Index [1, 4] — all < 5 → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(1, 4, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,4] should not CanBeTrue for col>=5")
	}
}

func TestKeyConditionLessThan(t *testing.T) {
	// col < 10
	expr := mustParseExpr(t, "col < 10")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})

	// Index [1, 5] — all < 10 → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(1, 5, types.TypeInt64))
	if !mask.CanBeTrue || mask.CanBeFalse {
		t.Error("[1,5] should be AlwaysTrue for col<10")
	}

	// Index [15, 20] — all >= 10 → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(15, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[15,20] should not CanBeTrue for col<10")
	}

	// Index [10, 20] — min=10, strictly less → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[10,20] should not CanBeTrue for col<10")
	}
}

func TestKeyConditionLessOrEqual(t *testing.T) {
	// col <= 10
	expr := mustParseExpr(t, "col <= 10")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})

	// Index [1, 10] — all <= 10 → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(1, 10, types.TypeInt64))
	if !mask.CanBeTrue || mask.CanBeFalse {
		t.Error("[1,10] should be AlwaysTrue for col<=10")
	}

	// Index [11, 20] — all > 10 → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(11, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[11,20] should not CanBeTrue for col<=10")
	}
}

func TestKeyConditionAND(t *testing.T) {
	// col > 5 AND col < 10
	expr := mustParseExpr(t, "col > 5 AND col < 10")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [6, 9] — fully within (5,10) → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(6, 9, types.TypeInt64))
	if !mask.CanBeTrue || mask.CanBeFalse {
		t.Error("[6,9] should be AlwaysTrue for col>5 AND col<10")
	}

	// Index [1, 3] — all <= 5 → AlwaysFalse (col>5 fails)
	mask = kc.CheckInHyperrectangle(int64HR(1, 3, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,3] should not CanBeTrue for col>5 AND col<10")
	}

	// Index [15, 20] — all >= 10 → AlwaysFalse (col<10 fails)
	mask = kc.CheckInHyperrectangle(int64HR(15, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[15,20] should not CanBeTrue for col>5 AND col<10")
	}

	// Index [3, 8] — partial overlap → Maybe
	mask = kc.CheckInHyperrectangle(int64HR(3, 8, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[3,8] should CanBeTrue for col>5 AND col<10")
	}
}

func TestKeyConditionOR(t *testing.T) {
	// col = 1 OR col = 5
	expr := mustParseExpr(t, "col = 1 OR col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [1, 1] — col=1 is true → CanBeTrue
	mask := kc.CheckInHyperrectangle(int64HR(1, 1, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[1,1] should CanBeTrue for col=1 OR col=5")
	}

	// Index [5, 5] — col=5 is true → CanBeTrue
	mask = kc.CheckInHyperrectangle(int64HR(5, 5, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[5,5] should CanBeTrue for col=1 OR col=5")
	}

	// Index [10, 20] — neither 1 nor 5 in range → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[10,20] should not CanBeTrue for col=1 OR col=5")
	}

	// Index [1, 5] — both values in range → CanBeTrue
	mask = kc.CheckInHyperrectangle(int64HR(1, 5, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[1,5] should CanBeTrue for col=1 OR col=5")
	}

	// Index [3, 3] — neither value → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(3, 3, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[3,3] should not CanBeTrue for col=1 OR col=5")
	}
}

func TestKeyConditionNOT(t *testing.T) {
	// NOT col = 5
	expr := mustParseExpr(t, "NOT col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [5, 5] — col=5 is always true, so NOT col=5 is always false
	mask := kc.CheckInHyperrectangle(int64HR(5, 5, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[5,5] should not CanBeTrue for NOT col=5")
	}

	// Index [10, 20] — col=5 is always false, so NOT col=5 is always true
	mask = kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if !mask.CanBeTrue {
		t.Error("[10,20] should CanBeTrue for NOT col=5")
	}
	if mask.CanBeFalse {
		t.Error("[10,20] should not CanBeFalse for NOT col=5")
	}

	// Index [1, 10] — col=5 is maybe, so NOT col=5 is maybe
	mask = kc.CheckInHyperrectangle(int64HR(1, 10, types.TypeInt64))
	if !mask.CanBeTrue || !mask.CanBeFalse {
		t.Error("[1,10] should be Maybe for NOT col=5")
	}
}

func TestKeyConditionUnknown(t *testing.T) {
	// non_key_col = 5 — not a key column, should produce MaskMaybe
	expr := mustParseExpr(t, "non_key_col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	mask := kc.CheckInHyperrectangle(int64HR(1, 100, types.TypeInt64))
	if !mask.CanBeTrue || !mask.CanBeFalse {
		t.Error("non-indexed column should produce MaskMaybe")
	}
}

func TestKeyConditionNilWhere(t *testing.T) {
	kc := NewKeyCondition(nil, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc != nil {
		t.Error("nil WHERE should produce nil KeyCondition")
	}
}

func TestKeyConditionEmptyColumns(t *testing.T) {
	expr := mustParseExpr(t, "col = 5")
	kc := NewKeyCondition(expr, nil, nil)
	if kc != nil {
		t.Error("empty key columns should produce nil KeyCondition")
	}
}

func TestKeyConditionUInt32(t *testing.T) {
	// Test with UInt32 type (used by DateTime)
	expr := mustParseExpr(t, "ts = 1000")
	kc := NewKeyCondition(expr, []string{"ts"}, []types.DataType{types.TypeUInt32})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [500, 1500] — 1000 in range → CanBeTrue
	hr := Hyperrectangle{{
		Left: uint32(500), Right: uint32(1500),
		LeftIncluded: true, RightIncluded: true,
		DataType: types.TypeUInt32,
	}}
	mask := kc.CheckInHyperrectangle(hr)
	if !mask.CanBeTrue {
		t.Error("1000 in [500,1500] should CanBeTrue")
	}

	// Index [2000, 3000] — 1000 not in range → AlwaysFalse
	hr = Hyperrectangle{{
		Left: uint32(2000), Right: uint32(3000),
		LeftIncluded: true, RightIncluded: true,
		DataType: types.TypeUInt32,
	}}
	mask = kc.CheckInHyperrectangle(hr)
	if mask.CanBeTrue {
		t.Error("1000 not in [2000,3000] should not CanBeTrue")
	}
}

func TestKeyConditionFlippedOperator(t *testing.T) {
	// 5 < col → col > 5
	expr := mustParseExpr(t, "5 < col")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	// Index [10, 20] — all > 5 → AlwaysTrue
	mask := kc.CheckInHyperrectangle(int64HR(10, 20, types.TypeInt64))
	if !mask.CanBeTrue || mask.CanBeFalse {
		t.Error("[10,20] should be AlwaysTrue for 5 < col (i.e., col > 5)")
	}

	// Index [1, 3] — all <= 5 → AlwaysFalse
	mask = kc.CheckInHyperrectangle(int64HR(1, 3, types.TypeInt64))
	if mask.CanBeTrue {
		t.Error("[1,3] should not CanBeTrue for 5 < col")
	}
}

// --- CheckInPrimaryIndex tests ---

// Helper: build a PrimaryIndex with one key column and given boundary values.
func makeIndex(col string, dt types.DataType, boundaries ...types.Value) *PrimaryIndex {
	idx := &PrimaryIndex{
		NumGranules: len(boundaries),
		KeyColumns:  []string{col},
		KeyTypes:    []types.DataType{dt},
		Values:      make([][]types.Value, len(boundaries)),
	}
	for i, v := range boundaries {
		idx.Values[i] = []types.Value{v}
	}
	return idx
}

func TestCheckInPrimaryIndex_Equality(t *testing.T) {
	// Index boundaries: [1, 5, 10, 15] → 4 granules
	// Granule 0: [1, 5)   — does NOT contain 5
	// Granule 1: [5, 10)  — contains 5
	// Granule 2: [10, 15) — does NOT contain 5
	// Granule 3: [15, +∞) — does NOT contain 5
	idx := makeIndex("col", types.TypeInt64, int64(1), int64(5), int64(10), int64(15))

	expr := mustParseExpr(t, "col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin != 1 || end != 2 {
		t.Errorf("col=5: got [%d,%d), want [1,2)", begin, end)
	}
}

func TestCheckInPrimaryIndex_Range(t *testing.T) {
	// Index boundaries: [1, 5, 10, 15] → 4 granules
	// col > 5 AND col < 15
	// Granule 0: [1, 5)   — max 4, all <=5 → col>5 false → prune
	// Granule 1: [5, 10)  — contains values >5 and <15 → maybe
	// Granule 2: [10, 15) — contains values >5 and <15 → maybe
	// Granule 3: [15, +∞) — min=15, col<15 false → prune
	idx := makeIndex("col", types.TypeInt64, int64(1), int64(5), int64(10), int64(15))

	expr := mustParseExpr(t, "col > 5 AND col < 15")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin != 1 || end != 3 {
		t.Errorf("col>5 AND col<15: got [%d,%d), want [1,3)", begin, end)
	}
}

func TestCheckInPrimaryIndex_OR(t *testing.T) {
	// Index boundaries: [1, 5, 10, 15] → 4 granules
	// col = 1 OR col = 10
	// Granule 0: [1, 5)   — contains 1 → maybe
	// Granule 1: [5, 10)  — does not contain 1 or 10 → prune
	// Granule 2: [10, 15) — contains 10 → maybe
	// Granule 3: [15, +∞) — does not contain 1 or 10 → prune
	// Expected: [0, 3) — first match at g=0, last match at g=2
	idx := makeIndex("col", types.TypeInt64, int64(1), int64(5), int64(10), int64(15))

	expr := mustParseExpr(t, "col = 1 OR col = 10")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition")
	}

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin != 0 || end != 3 {
		t.Errorf("col=1 OR col=10: got [%d,%d), want [0,3)", begin, end)
	}
}

func TestCheckInPrimaryIndex_NoKeyCondition(t *testing.T) {
	// nil condition should return full range
	idx := makeIndex("col", types.TypeInt64, int64(1), int64(5), int64(10))

	// When there's no key condition, the caller checks for nil before calling.
	// But if we call with a condition that references a non-key column, it
	// should return the full range since all evaluations produce MaskMaybe.
	expr := mustParseExpr(t, "other_col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})
	if kc == nil {
		t.Fatal("expected non-nil KeyCondition for unknown column")
	}

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin != 0 || end != 3 {
		t.Errorf("non-key condition: got [%d,%d), want [0,3)", begin, end)
	}
}

func TestCheckInPrimaryIndex_SingleGranule(t *testing.T) {
	// Single granule: boundary=[5], range [5, +∞)
	idx := makeIndex("col", types.TypeInt64, int64(5))

	// col = 5 → should match granule 0
	expr := mustParseExpr(t, "col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin != 0 || end != 1 {
		t.Errorf("single granule col=5: got [%d,%d), want [0,1)", begin, end)
	}

	// col = 1 → value below boundary, but single granule is [5, +∞)
	// so col=1 is NOT in [5, +∞) → should prune
	expr2 := mustParseExpr(t, "col = 1")
	kc2 := NewKeyCondition(expr2, []string{"col"}, []types.DataType{types.TypeInt64})

	begin, end = kc2.CheckInPrimaryIndex(idx)
	if begin != 1 || end != 1 {
		t.Errorf("single granule col=1: got [%d,%d), want [1,1) (all pruned)", begin, end)
	}
}

func TestCheckInPrimaryIndex_AllPruned(t *testing.T) {
	// All granules should be pruned when condition can never be true.
	// Index boundaries: [10, 20, 30]
	// col = 5 → value 5 is below all boundaries
	idx := makeIndex("col", types.TypeInt64, int64(10), int64(20), int64(30))

	expr := mustParseExpr(t, "col = 5")
	kc := NewKeyCondition(expr, []string{"col"}, []types.DataType{types.TypeInt64})

	begin, end := kc.CheckInPrimaryIndex(idx)
	if begin < end {
		t.Errorf("col=5 with boundaries [10,20,30]: got [%d,%d), expected empty range", begin, end)
	}
}
