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

func WriteHeader(w io.Writer, h Header) error {
	name := []byte(h.Name)
	nameLen := len(name)
	if nameLen == 0 || nameLen > MaxNameLen {
		return fmt.Errorf("%w: length %d", ErrBadName, nameLen)
	}
	// NAME is UTF-8 on the wire and ReadHeader enforces that, so refuse here
	// too rather than emit a header no compliant peer would accept.
	if !utf8.Valid(name) {
		return fmt.Errorf("%w: not valid UTF-8", ErrBadName)
	}

	if h.Size < 0 || h.Size > MaxFileSize {
		return fmt.Errorf("%w: %d", ErrSizeTooLarge, h.Size)
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

func ReadHeader(r io.Reader) (Header, error) {
	// A fixed size array rather than a slice, so it stays on the stack and
	// costs no allocation.
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
	size := binary.BigEndian.Uint64(fixed[7:15])

	if nameLen == 0 || nameLen > MaxNameLen {
		return Header{}, fmt.Errorf("%w: length %d", ErrBadName, nameLen)
	}

	if size > MaxFileSize {
		return Header{}, fmt.Errorf("%w: %d", ErrSizeTooLarge, size)
	}

	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return Header{}, fmt.Errorf("protocol: read name: %w", err)
	}
	if !utf8.Valid(name) {
		return Header{}, fmt.Errorf("%w: not valid UTF-8", ErrBadName)
	}

	return Header{Name: string(name), Size: int64(size)}, nil
}

func SafeName(name string) (string, error) {
	// filepath.Base strips every directory component: "a/b/c.txt" -> "c.txt".
	base := filepath.Base(name)
	switch {
	case base == "." || base == ".." || base == string(filepath.Separator):
		return "", fmt.Errorf("%w: %q", ErrBadName, name)
	case strings.ContainsRune(base, 0):
		// A NUL byte can truncate the name inside C-based syscalls.
		return "", fmt.Errorf("%w: contains NUL", ErrBadName)
	}
	return base, nil
}
