package storage

// BoolMask represents three-state logic for partition pruning, matching
// ClickHouse's BoolMask{can_be_true, can_be_false}. When evaluating a
// condition against a range (e.g., a minmax index), the result is not simply
// true/false — we need to express "the condition might be true for some rows"
// and "the condition might be false for some rows".
type BoolMask struct {
	CanBeTrue  bool
	CanBeFalse bool
}

// Pre-defined constants for common masks.
var (
	// MaskAlwaysTrue means the condition is definitely true for all rows.
	MaskAlwaysTrue = BoolMask{CanBeTrue: true, CanBeFalse: false}
	// MaskAlwaysFalse means the condition is definitely false for all rows.
	MaskAlwaysFalse = BoolMask{CanBeTrue: false, CanBeFalse: true}
	// MaskMaybe means some rows may satisfy the condition, some may not.
	MaskMaybe = BoolMask{CanBeTrue: true, CanBeFalse: true}
)

// And combines two masks with AND logic.
func (m BoolMask) And(other BoolMask) BoolMask {
	return BoolMask{
		CanBeTrue:  m.CanBeTrue && other.CanBeTrue,
		CanBeFalse: m.CanBeFalse || other.CanBeFalse,
	}
}

// Or combines two masks with OR logic.
func (m BoolMask) Or(other BoolMask) BoolMask {
	return BoolMask{
		CanBeTrue:  m.CanBeTrue || other.CanBeTrue,
		CanBeFalse: m.CanBeFalse && other.CanBeFalse,
	}
}

// Not inverts the mask.
func (m BoolMask) Not() BoolMask {
	return BoolMask{
		CanBeTrue:  m.CanBeFalse,
		CanBeFalse: m.CanBeTrue,
	}
}
