// Package protocol defines the gshift wire format.
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
	// Magic is the first four bytes of every gshift stream: ASCII "GSHF".
	Magic uint32 = 0x47534846

	// Version is the wire format version this build speaks.
	Version uint8 = 1

	// FixedHeaderSize is the header size up to, but not including, the name.
	FixedHeaderSize = 4 + 1 + 2 + 8

	// MaxNameLen is the largest file name, in bytes, that a header may carry.
	MaxNameLen = 1024

	// MaxFileSize is the largest payload, in bytes, that a header may declare.
	MaxFileSize = 1 << 40
)

var (
	// ErrBadMagic reports a stream that does not begin with Magic.
	ErrBadMagic = errors.New("protocol: not a gshift stream")

	// ErrBadVersion reports a wire format version this build does not speak.
	ErrBadVersion = errors.New("protocol: unsupported version")

	// ErrBadName reports a file name that is empty, too long, not valid UTF-8,
	// or unusable as a single path element.
	ErrBadName = errors.New("protocol: invalid file name")

	// ErrSizeTooLarge reports a payload length outside 0..MaxFileSize.
	ErrSizeTooLarge = errors.New("protocol: declared size exceeds limit")
)

// Header describes a single file transfer.
type Header struct {
	// Name is the file name, with no directory components.
	Name string

	// Size is the payload length in bytes.
	Size int64
}

// WriteHeader encodes h onto w in a single Write. It reports ErrBadName or
// ErrSizeTooLarge, without writing anything, for a header it cannot encode.
func WriteHeader(w io.Writer, h Header) error {
	name := []byte(h.Name)
	nameLen := len(name)
	if nameLen == 0 || nameLen > MaxNameLen {
		return fmt.Errorf("%w: length %d", ErrBadName, nameLen)
	}
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

// ReadHeader decodes one header from r, consuming exactly its bytes and leaving
// the payload in the stream. It validates every field before returning, and
// reports the zero Header alongside any error.
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

// SafeName reduces a peer-supplied name to a single path element that cannot
// escape the destination directory. It reports ErrBadName for a name that
// reduces to nothing usable, or that contains a NUL byte.
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
