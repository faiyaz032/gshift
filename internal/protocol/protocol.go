package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Magic           uint32 = 0x47534846
	Version         uint8  = 1
	FixedHeaderSize        = 4 + 1 + 2 + 8
	MaxNameLen             = 1024
	MaxFileSize            = 1 << 40
)

var (
	ErrBadMagic     = errors.New("protocol: not a gshift stream")
	ErrBadVersion   = errors.New("protocol: unsupported version")
	ErrBadName      = errors.New("protocol: invalid file name")
	ErrSizeTooLarge = errors.New("protocol: declared size exceeds limit")
)

type Header struct {
	Name string
	Size int64
}

func writeHeader(w io.Writer, h Header) error {
	name := []byte(h.Name)
	nameLen := len(name)
	if nameLen == 0 || nameLen > MaxNameLen {
		return fmt.Errorf("%w length %d", ErrBadName, nameLen)
	}

	if h.Size == 0 || h.Size > MaxFileSize {
		return fmt.Errorf("%w length %d", ErrSizeTooLarge, h.Size)
	}

	buf := make([]byte, 0, FixedHeaderSize+nameLen)
	buf = binary.BigEndian.AppendUint32(buf, Magic)
	buf = append(buf, Version)
	buf = binary.BigEndian.AppendUint16(buf, uint16(nameLen))
	buf = binary.BigEndian.AppendUint64(buf, uint64(h.Size))
	buf = append(buf, name...)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("protocol: write header: %w", err)
	}

	return nil
}
