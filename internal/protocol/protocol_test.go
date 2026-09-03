package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// decodedHeader mirrors the wire format that writeHeader produces, so tests can
// assert on individual fields instead of one opaque byte slice.
type decodedHeader struct {
	magic   uint32
	version uint8
	nameLen uint16
	size    uint64
	name    string
}

// decodeHeader parses a buffer written by writeHeader and fails the test if the
// buffer is not internally consistent.
func decodeHeader(t *testing.T, b []byte) decodedHeader {
	t.Helper()

	if len(b) < FixedHeaderSize {
		t.Fatalf("encoded header is %d bytes, want at least FixedHeaderSize (%d)", len(b), FixedHeaderSize)
	}

	d := decodedHeader{
		magic:   binary.BigEndian.Uint32(b[0:4]),
		version: b[4],
		nameLen: binary.BigEndian.Uint16(b[5:7]),
		size:    binary.BigEndian.Uint64(b[7:FixedHeaderSize]),
	}

	if got, want := len(b), FixedHeaderSize+int(d.nameLen); got != want {
		t.Fatalf("encoded header is %d bytes, want %d (FixedHeaderSize + nameLen %d)", got, want, d.nameLen)
	}
	d.name = string(b[FixedHeaderSize:])

	return d
}

// errWriter fails every Write with a fixed error.
type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// countingWriter records how many times Write was called and how many bytes it
// received, so tests can assert that rejected headers emit nothing at all.
type countingWriter struct {
	calls int
	n     int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.calls++
	w.n += len(p)
	return len(p), nil
}

func TestWriteHeaderGoldenBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHeader(&buf, Header{Name: "a.txt", Size: 1}); err != nil {
		t.Fatalf("writeHeader() error = %v, want nil", err)
	}

	want := []byte{
		'G', 'S', 'H', 'F', // magic, big endian
		0x01,       // version
		0x00, 0x05, // name length
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // size
		'a', '.', 't', 'x', 't', // name
	}
	if got := buf.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("writeHeader() wrote\n %#v\nwant\n %#v", got, want)
	}
}

func TestWriteHeaderMagicIsGSHF(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHeader(&buf, Header{Name: "x", Size: 1}); err != nil {
		t.Fatalf("writeHeader() error = %v, want nil", err)
	}

	if got, want := buf.Bytes()[:4], []byte("GSHF"); !bytes.Equal(got, want) {
		t.Errorf("magic = %q, want %q", got, want)
	}
}

func TestWriteHeaderEncodesFields(t *testing.T) {
	tests := []struct {
		name        string
		header      Header
		wantNameLen uint16
	}{
		{
			name:        "short ascii name",
			header:      Header{Name: "a.txt", Size: 1},
			wantNameLen: 5,
		},
		{
			name:        "path separators are not special",
			header:      Header{Name: "dir/sub/file.bin", Size: 4096},
			wantNameLen: 16,
		},
		{
			name:        "name length counts bytes not runes",
			header:      Header{Name: "héllo.txt", Size: 12},
			wantNameLen: 10, // é is two bytes in UTF-8
		},
		{
			name:        "single byte name",
			header:      Header{Name: "x", Size: 1},
			wantNameLen: 1,
		},
		{
			name:        "name at MaxNameLen",
			header:      Header{Name: strings.Repeat("n", MaxNameLen), Size: 1},
			wantNameLen: MaxNameLen,
		},
		{
			name:        "size at MaxFileSize",
			header:      Header{Name: "big.bin", Size: MaxFileSize},
			wantNameLen: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeHeader(&buf, tt.header); err != nil {
				t.Fatalf("writeHeader() error = %v, want nil", err)
			}

			got := decodeHeader(t, buf.Bytes())

			if got.magic != Magic {
				t.Errorf("magic = %#x, want %#x", got.magic, Magic)
			}
			if got.version != Version {
				t.Errorf("version = %d, want %d", got.version, Version)
			}
			if got.nameLen != tt.wantNameLen {
				t.Errorf("nameLen = %d, want %d", got.nameLen, tt.wantNameLen)
			}
			if got.size != uint64(tt.header.Size) {
				t.Errorf("size = %d, want %d", got.size, tt.header.Size)
			}
			if got.name != tt.header.Name {
				t.Errorf("name = %q, want %q", got.name, tt.header.Name)
			}
		})
	}
}

func TestWriteHeaderRejectsInvalidHeaders(t *testing.T) {
	tests := []struct {
		name    string
		header  Header
		wantErr error
	}{
		{
			name:    "empty name",
			header:  Header{Name: "", Size: 1},
			wantErr: ErrBadName,
		},
		{
			name:    "name one byte over MaxNameLen",
			header:  Header{Name: strings.Repeat("n", MaxNameLen+1), Size: 1},
			wantErr: ErrBadName,
		},
		{
			name:    "multi byte name over MaxNameLen in bytes",
			header:  Header{Name: strings.Repeat("é", MaxNameLen/2+1), Size: 1},
			wantErr: ErrBadName,
		},
		{
			name:    "zero size",
			header:  Header{Name: "empty.txt", Size: 0},
			wantErr: ErrSizeTooLarge,
		},
		{
			name:    "size one byte over MaxFileSize",
			header:  Header{Name: "big.bin", Size: MaxFileSize + 1},
			wantErr: ErrSizeTooLarge,
		},
		{
			name:    "name checked before size",
			header:  Header{Name: "", Size: MaxFileSize + 1},
			wantErr: ErrBadName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w countingWriter

			err := writeHeader(&w, tt.header)
			if err == nil {
				t.Fatalf("writeHeader() error = nil, want %v", tt.wantErr)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("writeHeader() error = %v, want it to wrap %v", err, tt.wantErr)
			}
			if w.calls != 0 {
				t.Errorf("writer received %d Write call(s) and %d byte(s); a rejected header must write nothing", w.calls, w.n)
			}
		})
	}
}

func TestWriteHeaderPropagatesWriteError(t *testing.T) {
	sentinel := errors.New("disk on fire")

	err := writeHeader(errWriter{err: sentinel}, Header{Name: "a.txt", Size: 1})
	if err == nil {
		t.Fatal("writeHeader() error = nil, want the writer's error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("writeHeader() error = %v, want it to wrap %v", err, sentinel)
	}
	if !strings.Contains(err.Error(), "write header") {
		t.Errorf("writeHeader() error = %q, want it to mention %q for context", err, "write header")
	}
}

func TestWriteHeaderWritesInASingleCall(t *testing.T) {
	// The header is assembled in one buffer before it reaches the writer, which
	// keeps it atomic for writers that frame each Write (sockets, pipes).
	var w countingWriter

	name := "a.txt"
	if err := writeHeader(&w, Header{Name: name, Size: 1}); err != nil {
		t.Fatalf("writeHeader() error = %v, want nil", err)
	}

	if w.calls != 1 {
		t.Errorf("writer received %d Write calls, want 1", w.calls)
	}
	if want := FixedHeaderSize + len(name); w.n != want {
		t.Errorf("writer received %d bytes, want %d", w.n, want)
	}
}

func TestWriteHeaderAppendsToExistingBuffer(t *testing.T) {
	// writeHeader must not clobber bytes already in the writer.
	var buf bytes.Buffer
	buf.WriteString("PRELUDE")

	if err := writeHeader(&buf, Header{Name: "a.txt", Size: 1}); err != nil {
		t.Fatalf("writeHeader() error = %v, want nil", err)
	}

	if got := buf.String(); !strings.HasPrefix(got, "PRELUDE") {
		t.Errorf("buffer = %q, want it to still start with %q", got, "PRELUDE")
	}
	if got, want := buf.Len(), len("PRELUDE")+FixedHeaderSize+len("a.txt"); got != want {
		t.Errorf("buffer length = %d, want %d", got, want)
	}
}

func TestFixedHeaderSizeMatchesEncoding(t *testing.T) {
	// Guards against FixedHeaderSize drifting away from the fields actually written.
	var buf bytes.Buffer
	if err := writeHeader(&buf, Header{Name: "x", Size: 1}); err != nil {
		t.Fatalf("writeHeader() error = %v, want nil", err)
	}

	if got, want := buf.Len()-1, FixedHeaderSize; got != want {
		t.Errorf("fixed portion of header is %d bytes, want FixedHeaderSize (%d)", got, want)
	}
}

func FuzzWriteHeader(f *testing.F) {
	f.Add("a.txt", int64(1))
	f.Add("héllo.txt", int64(4096))
	f.Add(strings.Repeat("n", MaxNameLen), int64(MaxFileSize))

	f.Fuzz(func(t *testing.T, name string, size int64) {
		// Negative sizes are out of scope: writeHeader does not reject them yet.
		if size < 1 || size > MaxFileSize {
			t.Skip()
		}
		if n := len(name); n == 0 || n > MaxNameLen {
			t.Skip()
		}

		var buf bytes.Buffer
		if err := writeHeader(&buf, Header{Name: name, Size: size}); err != nil {
			t.Fatalf("writeHeader(%q, %d) error = %v, want nil", name, size, err)
		}

		got := decodeHeader(t, buf.Bytes())
		if got.magic != Magic || got.version != Version {
			t.Errorf("magic/version = %#x/%d, want %#x/%d", got.magic, got.version, Magic, Version)
		}
		if got.name != name {
			t.Errorf("name = %q, want %q", got.name, name)
		}
		if got.size != uint64(size) {
			t.Errorf("size = %d, want %d", got.size, size)
		}
	})
}

var _ io.Writer = errWriter{}
