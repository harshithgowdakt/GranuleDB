package compression

import (
	"fmt"

	"github.com/pierrec/lz4/v4"
)

// LZ4Codec implements LZ4 block compression.
type LZ4Codec struct{}

func (c *LZ4Codec) MethodByte() byte { return MethodLZ4 }

func (c *LZ4Codec) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return []byte{}, nil
	}
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return nil, fmt.Errorf("lz4 compress: %w", err)
	}
	if n == 0 {
		// Data is incompressible, store as-is
		dst = make([]byte, len(src))
		copy(dst, src)
		return dst, nil
	}
	return dst[:n], nil
}

func (c *LZ4Codec) Decompress(src []byte, decompressedSize int) ([]byte, error) {
	if decompressedSize == 0 {
		return []byte{}, nil
	}
	dst := make([]byte, decompressedSize)
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	if n != decompressedSize {
		return nil, fmt.Errorf("lz4 decompress: expected %d bytes, got %d", decompressedSize, n)
	}
	return dst, nil
}
