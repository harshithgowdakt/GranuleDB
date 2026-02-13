package engine

// numeric is the type constraint for vectorized arithmetic/comparison kernels.
type numeric interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~int8 | ~int16 | ~int32 | ~int64 |
		~float32 | ~float64
}

func vecAdd[T numeric](a, b []T) []T {
	out := make([]T, len(a))
	for i := range a {
		out[i] = a[i] + b[i]
	}
	return out
}

func vecSub[T numeric](a, b []T) []T {
	out := make([]T, len(a))
	for i := range a {
		out[i] = a[i] - b[i]
	}
	return out
}

func vecMul[T numeric](a, b []T) []T {
	out := make([]T, len(a))
	for i := range a {
		out[i] = a[i] * b[i]
	}
	return out
}

func vecDiv[T numeric](a, b []T) []T {
	out := make([]T, len(a))
	for i := range a {
		if b[i] == 0 {
			out[i] = 0
		} else {
			out[i] = a[i] / b[i]
		}
	}
	return out
}

func vecGT[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] > b[i]
	}
	return out
}

func vecLT[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] < b[i]
	}
	return out
}

func vecEQ[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] == b[i]
	}
	return out
}

func vecNEQ[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] != b[i]
	}
	return out
}

func vecGTE[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] >= b[i]
	}
	return out
}

func vecLTE[T numeric](a, b []T) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] <= b[i]
	}
	return out
}

func vecAND(a, b []bool) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] && b[i]
	}
	return out
}

func vecOR(a, b []bool) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = a[i] || b[i]
	}
	return out
}

func vecNOT(a []bool) []bool {
	out := make([]bool, len(a))
	for i := range a {
		out[i] = !a[i]
	}
	return out
}

func vecNegate[T numeric](a []T) []T {
	out := make([]T, len(a))
	for i := range a {
		out[i] = -a[i]
	}
	return out
}
