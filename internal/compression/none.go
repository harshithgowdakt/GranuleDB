package compression

// NoneCodec is a no-op codec (no compression).
type NoneCodec struct{}

func (c *NoneCodec) MethodByte() byte { return MethodNone }

func (c *NoneCodec) Compress(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, nil
}

func (c *NoneCodec) Decompress(src []byte, decompressedSize int) ([]byte, error) {
	dst := make([]byte, decompressedSize)
	copy(dst, src)
	return dst, nil
}
