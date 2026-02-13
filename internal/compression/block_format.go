package compression

import (
	"encoding/binary"
	"fmt"
)

// Compressed block format (matching ClickHouse, minus the 16-byte CityHash checksum):
//   [method_byte (1)] [compressed_size_with_header (4 LE)] [uncompressed_size (4 LE)] [payload...]
//
// compressed_size_with_header includes the 9-byte header itself.

const HeaderSize = 9

// CompressBlock compresses data and returns the full block (header + compressed payload).
func CompressBlock(codec Codec, data []byte) ([]byte, error) {
	compressed, err := codec.Compress(data)
	if err != nil {
		return nil, err
	}

	totalSize := HeaderSize + len(compressed)
	block := make([]byte, totalSize)

	// Write header
	block[0] = codec.MethodByte()
	binary.LittleEndian.PutUint32(block[1:5], uint32(totalSize))
	binary.LittleEndian.PutUint32(block[5:9], uint32(len(data)))

	// Write compressed payload
	copy(block[HeaderSize:], compressed)

	return block, nil
}

// DecompressBlock reads a compressed block, validates header, and decompresses.
func DecompressBlock(data []byte) ([]byte, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("compressed block too small: %d bytes", len(data))
	}

	methodByte := data[0]
	compressedSizeWithHeader := binary.LittleEndian.Uint32(data[1:5])
	uncompressedSize := binary.LittleEndian.Uint32(data[5:9])

	if int(compressedSizeWithHeader) > len(data) {
		return nil, fmt.Errorf("compressed block size mismatch: header says %d, have %d",
			compressedSizeWithHeader, len(data))
	}

	payload := data[HeaderSize:compressedSizeWithHeader]

	var codec Codec
	switch methodByte {
	case MethodLZ4:
		codec = &LZ4Codec{}
	case MethodNone:
		codec = &NoneCodec{}
	default:
		return nil, fmt.Errorf("unknown compression method: 0x%02x", methodByte)
	}

	return codec.Decompress(payload, int(uncompressedSize))
}

// ReadBlockHeader reads the header from a compressed block and returns
// (compressedSizeWithHeader, uncompressedSize, error).
func ReadBlockHeader(data []byte) (compressedTotal uint32, uncompressed uint32, err error) {
	if len(data) < HeaderSize {
		return 0, 0, fmt.Errorf("not enough data for block header")
	}
	compressedTotal = binary.LittleEndian.Uint32(data[1:5])
	uncompressed = binary.LittleEndian.Uint32(data[5:9])
	return compressedTotal, uncompressed, nil
}
