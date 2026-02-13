package storage

const DefaultGranuleSize = 8192

// GranuleRange represents a range of rows [Start, End).
type GranuleRange struct {
	Start int
	End   int
}

// SplitIntoGranules splits totalRows into granule boundaries.
func SplitIntoGranules(totalRows, granuleSize int) []GranuleRange {
	if granuleSize <= 0 {
		granuleSize = DefaultGranuleSize
	}
	var result []GranuleRange
	for start := 0; start < totalRows; start += granuleSize {
		end := start + granuleSize
		if end > totalRows {
			end = totalRows
		}
		result = append(result, GranuleRange{Start: start, End: end})
	}
	return result
}
