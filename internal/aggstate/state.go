package aggstate

import (
	"encoding/binary"
	"math"
)

const (
	magic0 = 'A'
	magic1 = 'G'
	magic2 = 'S'
	magic3 = '1'
)

const (
	KindUnknown uint8 = 0
	KindSum     uint8 = 1
	KindUniqHLL uint8 = 2
)

// EncodeSumState serializes a sum state.
func EncodeSumState(sum float64) []byte {
	out := make([]byte, 13)
	out[0], out[1], out[2], out[3] = magic0, magic1, magic2, magic3
	out[4] = KindSum
	binary.LittleEndian.PutUint64(out[5:13], math.Float64bits(sum))
	return out
}

// DecodeSumState deserializes a sum state.
func DecodeSumState(data []byte) (float64, bool) {
	if len(data) < 13 {
		return 0, false
	}
	if !hasMagic(data) || data[4] != KindSum {
		return 0, false
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(data[5:13])), true
}

// EncodeUniqHLLState serializes a HLL sketch as aggregate state.
func EncodeUniqHLLState(sketch []byte) []byte {
	out := make([]byte, 5+len(sketch))
	out[0], out[1], out[2], out[3] = magic0, magic1, magic2, magic3
	out[4] = KindUniqHLL
	copy(out[5:], sketch)
	return out
}

// DecodeUniqHLLState extracts HLL sketch bytes from aggregate state.
func DecodeUniqHLLState(data []byte) ([]byte, bool) {
	if len(data) < 5 {
		return nil, false
	}
	if !hasMagic(data) || data[4] != KindUniqHLL {
		return nil, false
	}
	out := make([]byte, len(data)-5)
	copy(out, data[5:])
	return out, true
}

// StateKind returns the state kind encoded in data.
func StateKind(data []byte) uint8 {
	if len(data) < 5 || !hasMagic(data) {
		return KindUnknown
	}
	return data[4]
}

func hasMagic(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == magic0 &&
		data[1] == magic1 &&
		data[2] == magic2 &&
		data[3] == magic3
}
