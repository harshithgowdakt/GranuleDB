package aggstate

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	hllP = 12
	hllM = 1 << hllP
)

// HLL12 is a fixed-precision HyperLogLog sketch.
type HLL12 struct {
	Regs [hllM]uint8
}

func NewHLL12() *HLL12 {
	return &HLL12{}
}

func (h *HLL12) AddBytes(b []byte) {
	// 64-bit FNV-1a hash.
	hasher := fnv.New64a()
	_, _ = hasher.Write(b)
	x := hasher.Sum64()

	idx := x & ((1 << hllP) - 1)
	w := x >> hllP
	rho := uint8(bits.LeadingZeros64(w) + 1)
	if rho > 64 {
		rho = 64
	}
	if rho > h.Regs[idx] {
		h.Regs[idx] = rho
	}
}

func (h *HLL12) Merge(other *HLL12) {
	for i := 0; i < hllM; i++ {
		if other.Regs[i] > h.Regs[i] {
			h.Regs[i] = other.Regs[i]
		}
	}
}

func (h *HLL12) Estimate() uint64 {
	m := float64(hllM)
	alpha := 0.7213 / (1.0 + 1.079/m)

	sum := 0.0
	zeros := 0
	for i := 0; i < hllM; i++ {
		r := h.Regs[i]
		sum += math.Pow(2.0, -float64(r))
		if r == 0 {
			zeros++
		}
	}
	est := alpha * m * m / sum
	// Small-range correction.
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}
	if est < 0 {
		est = 0
	}
	return uint64(est + 0.5)
}

func (h *HLL12) MarshalBinary() []byte {
	out := make([]byte, 2+hllM)
	binary.LittleEndian.PutUint16(out[:2], hllP)
	copy(out[2:], h.Regs[:])
	return out
}

func (h *HLL12) UnmarshalBinary(data []byte) bool {
	if len(data) != 2+hllM {
		return false
	}
	p := binary.LittleEndian.Uint16(data[:2])
	if p != hllP {
		return false
	}
	copy(h.Regs[:], data[2:])
	return true
}
