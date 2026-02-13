package compression

// Codec compresses and decompresses data blocks.
type Codec interface {
	// MethodByte returns the single-byte codec identifier.
	MethodByte() byte
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte, decompressedSize int) ([]byte, error)
}

// Method byte constants matching ClickHouse format.
const (
	MethodNone byte = 0x02
	MethodLZ4  byte = 0x82
)
