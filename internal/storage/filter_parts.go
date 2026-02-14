package storage

import (
	"log"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/parser"
)

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
		if len(parts) > 0 {
			log.Printf("[partition] no partition pruning applicable, passing all %d parts through", len(parts))
		}
		return parts, counters
	}

	log.Printf("[partition] evaluating %d parts against partition key (%s)", len(parts), schema.PartitionBy)

	filtered := make([]*Part, 0, len(parts))
	var prunedNames []string
	var selectedNames []string
	for _, part := range parts {
		if pruner.CanBePruned(part) {
			prunedNames = append(prunedNames, part.Info.DirName())
		} else {
			filtered = append(filtered, part)
			selectedNames = append(selectedNames, part.Info.DirName())
		}
	}

	counters.SurvivedParts = len(filtered)

	if len(prunedNames) > 0 {
		log.Printf("[partition] pruned %d parts: [%s]", len(prunedNames), strings.Join(prunedNames, ", "))
	}
	if len(selectedNames) > 0 {
		log.Printf("[partition] selected %d/%d parts: [%s]", len(selectedNames), len(parts), strings.Join(selectedNames, ", "))
	} else {
		log.Printf("[partition] all %d parts pruned — no data to scan", len(parts))
	}

	return filtered, counters
}
