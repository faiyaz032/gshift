package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	Magic uint32 = 0x47534846

	Version uint8 = 2

	FixedHeaderSize = 4 + 1 + 2 + 8 + 8 + 8

	MaxNameLen = 1024

	MaxFileSize = 1 << 40
)

var (
	ErrBadMagic = errors.New("protocol: not a gshift stream")

	ErrBadVersion = errors.New("protocol: unsupported version")

	ErrBadName = errors.New("protocol: invalid file name")

	ErrSizeTooLarge = errors.New("protocol: declared size exceeds limit")

	ErrBadRange = errors.New("protocol: invalid chunk range")
)

type Header struct {
	Name      string
	TotalSize int64
	Offset    int64
	Length    int64
}

func WriteHeader(w io.Writer, h Header) error {
	name := []byte(h.Name)
	nameLen := len(name)
	if nameLen == 0 || nameLen > MaxNameLen {
		return fmt.Errorf("%w: length %d", ErrBadName, nameLen)
	}
	if !utf8.Valid(name) {
		return fmt.Errorf("%w: not valid UTF-8", ErrBadName)
	}

	if h.TotalSize < 0 || h.TotalSize > MaxFileSize {
		return fmt.Errorf("%w: %d", ErrSizeTooLarge, h.TotalSize)
	}
	if h.Offset < 0 || h.Length < 0 || h.Offset+h.Length > h.TotalSize {
		return fmt.Errorf("%w: offset %d, length %d, total %d", ErrBadRange, h.Offset, h.Length, h.TotalSize)
	}

	buf := make([]byte, 0, FixedHeaderSize+nameLen)
	buf = binary.BigEndian.AppendUint32(buf, Magic)
	buf = append(buf, Version)
	buf = binary.BigEndian.AppendUint16(buf, uint16(nameLen))
	buf = binary.BigEndian.AppendUint64(buf, uint64(h.TotalSize))
	buf = binary.BigEndian.AppendUint64(buf, uint64(h.Offset))
	buf = binary.BigEndian.AppendUint64(buf, uint64(h.Length))
	buf = append(buf, name...)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("protocol: write header: %w", err)
	}

	return nil
}

func ReadHeader(r io.Reader) (Header, error) {
	var fixed [FixedHeaderSize]byte

	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Header{}, fmt.Errorf("protocol: read header: %w", err)
	}

	if got := binary.BigEndian.Uint32(fixed[0:4]); got != Magic {
		return Header{}, fmt.Errorf("%w (got %#08x)", ErrBadMagic, got)
	}

	if v := fixed[4]; v != Version {
		return Header{}, fmt.Errorf("%w: peer speaks v%d, we speak v%d",
			ErrBadVersion, v, Version)
	}

	nameLen := binary.BigEndian.Uint16(fixed[5:7])
	totalSize := binary.BigEndian.Uint64(fixed[7:15])
	offset := binary.BigEndian.Uint64(fixed[15:23])
	length := binary.BigEndian.Uint64(fixed[23:31])

	if nameLen == 0 || nameLen > MaxNameLen {
		return Header{}, fmt.Errorf("%w: length %d", ErrBadName, nameLen)
	}

	if totalSize > MaxFileSize {
		return Header{}, fmt.Errorf("%w: %d", ErrSizeTooLarge, totalSize)
	}

	if offset > totalSize || length > totalSize-offset {
		return Header{}, fmt.Errorf("%w: offset %d, length %d, total %d", ErrBadRange, offset, length, totalSize)
	}

	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return Header{}, fmt.Errorf("protocol: read name: %w", err)
	}
	if !utf8.Valid(name) {
		return Header{}, fmt.Errorf("%w: not valid UTF-8", ErrBadName)
	}

	return Header{
		Name:      string(name),
		TotalSize: int64(totalSize),
		Offset:    int64(offset),
		Length:    int64(length),
	}, nil
}

func SafeName(name string) (string, error) {
	base := filepath.Base(name)
	switch {
	case base == "." || base == ".." || base == string(filepath.Separator):
		return "", fmt.Errorf("%w: %q", ErrBadName, name)
	case strings.ContainsRune(base, 0):
		return "", fmt.Errorf("%w: contains NUL", ErrBadName)
	}
	return base, nil
}
