package storage

import "github.com/harshithgowdakt/granuledb/internal/parser"

// PartFilterCounters records observability metrics for part-level pruning.
type PartFilterCounters struct {
	InitialParts  int
	SurvivedParts int
}

// FilterParts is the single centralized entry point for part-level partition
// pruning. Both the operator-based (engine) and processor-based execution paths
// call this instead of duplicating pruning logic.
//
// It creates a PartitionPruner from the WHERE expression and schema, then
// iterates over parts calling CanBePruned(). Parts that cannot be pruned are
// returned along with observability counters.
func FilterParts(where parser.Expression, schema *TableSchema, parts []*Part) ([]*Part, PartFilterCounters) {
	counters := PartFilterCounters{
		InitialParts: len(parts),
	}

	pruner := NewPartitionPruner(where, schema)
	if pruner == nil {
		// No pruning applicable — return all parts.
		counters.SurvivedParts = len(parts)
		return parts, counters
	}

	filtered := make([]*Part, 0, len(parts))
	for _, part := range parts {
		if !pruner.CanBePruned(part) {
			filtered = append(filtered, part)
		}
	}

	counters.SurvivedParts = len(filtered)
	return filtered, counters
}
