package column

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/harshithgowda/goose-db/internal/types"
)

// WriteVarUInt writes a variable-length unsigned integer (same encoding as protobuf varint).
func WriteVarUInt(w io.Writer, v uint64) error {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

// ReadVarUInt reads a variable-length unsigned integer.
func ReadVarUInt(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// EncodeColumn encodes a column to binary format.
// Fixed-size types: raw little-endian contiguous bytes.
// String: VarInt(length) + raw bytes per string.
func EncodeColumn(col Column) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeColumnTo(&buf, col); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeColumnTo(w io.Writer, col Column) error {
	n := col.Len()
	switch c := col.(type) {
	case *UInt8Column:
		_, err := w.Write(c.Data)
		return err
	case *UInt16Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *UInt32Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *UInt64Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Int8Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Int16Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Int32Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Int64Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Float32Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *Float64Column:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	case *StringColumn:
		for i := 0; i < n; i++ {
			s := c.Data[i]
			if err := WriteVarUInt(w, uint64(len(s))); err != nil {
				return err
			}
			if _, err := w.Write([]byte(s)); err != nil {
				return err
			}
		}
		return nil
	case *DateTimeColumn:
		for i := 0; i < n; i++ {
			if err := binary.Write(w, binary.LittleEndian, c.Data[i]); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported column type for encoding: %T", col)
	}
}

// DecodeColumn decodes a column from binary data.
func DecodeColumn(dt types.DataType, data []byte, numRows int) (Column, error) {
	r := bytes.NewReader(data)
	return decodeColumnFrom(dt, r, numRows)
}

func decodeColumnFrom(dt types.DataType, r io.Reader, numRows int) (Column, error) {
	switch dt {
	case types.TypeUInt8:
		col := &UInt8Column{Data: make([]uint8, numRows)}
		_, err := io.ReadFull(r, col.Data)
		return col, err
	case types.TypeUInt16:
		col := &UInt16Column{Data: make([]uint16, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeUInt32:
		col := &UInt32Column{Data: make([]uint32, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeUInt64:
		col := &UInt64Column{Data: make([]uint64, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeInt8:
		col := &Int8Column{Data: make([]int8, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeInt16:
		col := &Int16Column{Data: make([]int16, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeInt32:
		col := &Int32Column{Data: make([]int32, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeInt64:
		col := &Int64Column{Data: make([]int64, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeFloat32:
		col := &Float32Column{Data: make([]float32, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeFloat64:
		col := &Float64Column{Data: make([]float64, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	case types.TypeString:
		col := &StringColumn{Data: make([]string, 0, numRows)}
		br, ok := r.(io.ByteReader)
		if !ok {
			// Wrap in a bufio-like reader
			br = newByteReaderWrapper(r)
		}
		for i := 0; i < numRows; i++ {
			length, err := ReadVarUInt(br)
			if err != nil {
				return nil, fmt.Errorf("reading string length at row %d: %w", i, err)
			}
			buf := make([]byte, length)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, fmt.Errorf("reading string data at row %d: %w", i, err)
			}
			col.Data = append(col.Data, string(buf))
		}
		return col, nil
	case types.TypeDateTime:
		col := &DateTimeColumn{Data: make([]uint32, numRows)}
		for i := 0; i < numRows; i++ {
			if err := binary.Read(r, binary.LittleEndian, &col.Data[i]); err != nil {
				return nil, err
			}
		}
		return col, nil
	default:
		return nil, fmt.Errorf("unsupported data type for decoding: %d", dt)
	}
}

// EncodeValue encodes a single value to binary format.
func EncodeValue(w io.Writer, dt types.DataType, v types.Value) error {
	switch dt {
	case types.TypeUInt8:
		return binary.Write(w, binary.LittleEndian, v.(uint8))
	case types.TypeUInt16:
		return binary.Write(w, binary.LittleEndian, v.(uint16))
	case types.TypeUInt32:
		return binary.Write(w, binary.LittleEndian, v.(uint32))
	case types.TypeUInt64:
		return binary.Write(w, binary.LittleEndian, v.(uint64))
	case types.TypeInt8:
		return binary.Write(w, binary.LittleEndian, v.(int8))
	case types.TypeInt16:
		return binary.Write(w, binary.LittleEndian, v.(int16))
	case types.TypeInt32:
		return binary.Write(w, binary.LittleEndian, v.(int32))
	case types.TypeInt64:
		return binary.Write(w, binary.LittleEndian, v.(int64))
	case types.TypeFloat32:
		return binary.Write(w, binary.LittleEndian, v.(float32))
	case types.TypeFloat64:
		return binary.Write(w, binary.LittleEndian, v.(float64))
	case types.TypeString:
		s := v.(string)
		if err := WriteVarUInt(w, uint64(len(s))); err != nil {
			return err
		}
		_, err := w.Write([]byte(s))
		return err
	case types.TypeDateTime:
		return binary.Write(w, binary.LittleEndian, v.(uint32))
	default:
		return fmt.Errorf("unsupported type for EncodeValue: %d", dt)
	}
}

// DecodeValue decodes a single value from binary format.
func DecodeValue(r io.Reader, dt types.DataType) (types.Value, error) {
	switch dt {
	case types.TypeUInt8:
		var v uint8
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeUInt16:
		var v uint16
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeUInt32:
		var v uint32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeUInt64:
		var v uint64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeInt8:
		var v int8
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeInt16:
		var v int16
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeInt32:
		var v int32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeInt64:
		var v int64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeFloat32:
		var v float32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeFloat64:
		var v float64
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	case types.TypeString:
		br, ok := r.(io.ByteReader)
		if !ok {
			br = newByteReaderWrapper(r)
		}
		length, err := ReadVarUInt(br)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	case types.TypeDateTime:
		var v uint32
		err := binary.Read(r, binary.LittleEndian, &v)
		return v, err
	default:
		return nil, fmt.Errorf("unsupported type for DecodeValue: %d", dt)
	}
}

// byteReaderWrapper wraps an io.Reader to implement io.ByteReader.
type byteReaderWrapper struct {
	r   io.Reader
	buf [1]byte
}

func newByteReaderWrapper(r io.Reader) *byteReaderWrapper {
	return &byteReaderWrapper{r: r}
}

func (b *byteReaderWrapper) ReadByte() (byte, error) {
	_, err := io.ReadFull(b.r, b.buf[:])
	return b.buf[0], err
}

func (b *byteReaderWrapper) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

// Helper: encode fixed-size numeric values more efficiently using unsafe-like batch writes.
// For simplicity we use binary.Write per element above. For performance, we could
// use math.Float32bits/Float64bits + manual LE encoding, but this is cleaner.

// Float32ToBytes converts a float32 to 4 LE bytes.
func Float32ToBytes(v float32) [4]byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(v))
	return b
}

// Float64ToBytes converts a float64 to 8 LE bytes.
func Float64ToBytes(v float64) [8]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	return b
}
