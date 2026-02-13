package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Mark represents a position within a compressed column file.
// Each mark stores the byte offset to the compressed block and the offset within
// the decompressed block (always 0 in our simplified format where each granule
// is one compressed block).
type Mark struct {
	OffsetInCompressedFile    uint64
	OffsetInDecompressedBlock uint64
}

const markSize = 16 // 2 x uint64

// WriteMarks writes a slice of marks to a writer.
func WriteMarks(w io.Writer, marks []Mark) error {
	for _, m := range marks {
		if err := binary.Write(w, binary.LittleEndian, m.OffsetInCompressedFile); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, m.OffsetInDecompressedBlock); err != nil {
			return err
		}
	}
	return nil
}

// ReadMarks reads count marks from a reader.
func ReadMarks(r io.Reader, count int) ([]Mark, error) {
	marks := make([]Mark, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &marks[i].OffsetInCompressedFile); err != nil {
			return nil, fmt.Errorf("reading mark %d offset: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &marks[i].OffsetInDecompressedBlock); err != nil {
			return nil, fmt.Errorf("reading mark %d decompressed offset: %w", i, err)
		}
	}
	return marks, nil
}

// ReadMarksFromFile reads all marks from a .mrk file.
func ReadMarksFromFile(path string) ([]Mark, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	count := len(data) / markSize
	if len(data)%markSize != 0 {
		return nil, fmt.Errorf("mark file size %d is not a multiple of %d", len(data), markSize)
	}
	marks := make([]Mark, count)
	for i := range count {
		offset := i * markSize
		marks[i].OffsetInCompressedFile = binary.LittleEndian.Uint64(data[offset:])
		marks[i].OffsetInDecompressedBlock = binary.LittleEndian.Uint64(data[offset+8:])
	}
	return marks, nil
}
