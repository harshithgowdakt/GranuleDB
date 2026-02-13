package column

// filterSlice returns elements of data where mask[i] is true.
func filterSlice[T any](data []T, mask []bool) []T {
	// Count matches for exact allocation.
	n := 0
	for _, m := range mask {
		if m {
			n++
		}
	}
	out := make([]T, 0, n)
	for i, m := range mask {
		if m {
			out = append(out, data[i])
		}
	}
	return out
}

// gatherSlice reorders data by the given index array.
func gatherSlice[T any](data []T, indices []int) []T {
	out := make([]T, len(indices))
	for i, idx := range indices {
		out[i] = data[idx]
	}
	return out
}

// appendSlice appends src onto dst and returns the extended slice.
func appendSlice[T any](dst, src []T) []T {
	return append(dst, src...)
}
