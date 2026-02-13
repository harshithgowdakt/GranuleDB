package merge

import "github.com/harshithgowda/goose-db/internal/storage"

// SimpleMergeSelector picks adjacent parts in the same partition to merge.
type SimpleMergeSelector struct {
	MaxPartsToMerge int // default 10
	MinPartsToMerge int // default 3
}

// NewSimpleMergeSelector creates a merge selector with defaults.
func NewSimpleMergeSelector() *SimpleMergeSelector {
	return &SimpleMergeSelector{
		MaxPartsToMerge: 10,
		MinPartsToMerge: 3,
	}
}

// SelectPartsToMerge finds the best set of adjacent parts to merge within a partition.
// Returns nil if no merge is worthwhile.
func (s *SimpleMergeSelector) SelectPartsToMerge(parts []*storage.Part) []*storage.Part {
	if len(parts) < s.MinPartsToMerge {
		return nil
	}

	// Group by partition
	partsByPartition := make(map[string][]*storage.Part)
	for _, p := range parts {
		if p.State == storage.PartActive {
			partsByPartition[p.Info.PartitionID] = append(partsByPartition[p.Info.PartitionID], p)
		}
	}

	// For each partition, find the best range to merge
	var bestRange []*storage.Part
	var bestScore float64

	for _, partList := range partsByPartition {
		if len(partList) < s.MinPartsToMerge {
			continue
		}

		// Try all contiguous ranges of MinParts..MaxParts
		maxLen := s.MaxPartsToMerge
		if maxLen > len(partList) {
			maxLen = len(partList)
		}

		for rangeLen := s.MinPartsToMerge; rangeLen <= maxLen; rangeLen++ {
			for start := 0; start+rangeLen <= len(partList); start++ {
				candidate := partList[start : start+rangeLen]
				score := scoreMergeRange(candidate)
				if score > bestScore {
					bestScore = score
					bestRange = candidate
				}
			}
		}
	}

	return bestRange
}

// scoreMergeRange scores a merge candidate. Higher is better.
// Prefers merging many small parts over few large parts.
func scoreMergeRange(parts []*storage.Part) float64 {
	if len(parts) == 0 {
		return 0
	}

	var totalSize uint64
	var maxSize uint64
	for _, p := range parts {
		size := p.SizeBytes
		if size == 0 {
			size = p.NumRows // fallback if SizeBytes not set
		}
		totalSize += size
		if size > maxSize {
			maxSize = size
		}
	}

	if maxSize == 0 {
		maxSize = 1
	}

	// Score: prefer many parts with similar sizes
	// ratio close to numParts means all parts are similar size
	ratio := float64(totalSize) / float64(maxSize)
	return ratio * float64(len(parts))
}
