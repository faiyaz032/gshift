// These tests are the specification of the gshift wire format.
//
// Every test name states one rule the package guarantees, so the verbose test
// output IS the documentation:
//
//	go test -v ./internal/protocol
//	go test -v ./internal/protocol | grep -- '--- PASS'
//
// Layout, in reading order:
//
//  1. The wire format      - the exact bytes, as a fixture both sides share
//  2. WriteHeader          - what it produces, then what it refuses
//  3. ReadHeader           - what it accepts, what it refuses, how it behaves
//     on a stream it shares with the payload
//  4. SafeName             - the untrusted-name rule
//  5. Fuzz targets         - the invariants that must hold for ALL inputs
//  6. Test helpers         - the plumbing, last, so the rules come first
//
// See example_test.go for runnable examples of how the package is used.
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

// ---------------------------------------------------------------------------
// 1. The wire format
// ---------------------------------------------------------------------------

// goldenHeader and goldenBytes are the same header in both representations:
// the Go value on the left of the encoder, the bytes on the right of it.
//
// Hard coding the bytes is the point. If they were computed by the code under
// test, the test would only prove the encoder agrees with itself; spelled out
// like this it proves the encoder agrees with the *documented format*, and any
// accidental change to the layout breaks the build for every peer at once.
var (
	goldenHeader = Header{Name: "a.txt", TotalSize: 1, Offset: 0, Length: 1}

	goldenBytes = []byte{
		'G', 'S', 'H', 'F', // MAGIC     uint32, big endian
		0x02,       // VERSION   uint8
		0x00, 0x05, // NAMELEN   uint16, big endian: len("a.txt")
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // TOTALSIZE uint64, big endian
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // OFFSET    uint64, big endian
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // LENGTH    uint64, big endian
		'a', '.', 't', 'x', 't', // NAME      NAMELEN bytes, UTF-8
	}
)

func TestWriteHeader_ProducesTheDocumentedBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteHeader(&buf, goldenHeader); err != nil {
		t.Fatalf("WriteHeader(%+v) error = %v, want nil", goldenHeader, err)
	}

	if got := buf.Bytes(); !bytes.Equal(got, goldenBytes) {
		t.Errorf("WriteHeader(%+v) wrote\n %#v\nwant\n %#v", goldenHeader, got, goldenBytes)
	}
}

func TestReadHeader_ParsesTheDocumentedBytes(t *testing.T) {
	got, err := ReadHeader(bytes.NewReader(goldenBytes))
	if err != nil {
		t.Fatalf("ReadHeader() error = %v, want nil", err)
	}

	if got != goldenHeader {
		t.Errorf("ReadHeader() = %+v, want %+v", got, goldenHeader)
	}
}

func TestWriteHeader_StartsEveryStreamWithTheAsciiMagicGSHF(t *testing.T) {
	// The magic is why a browser, a port scanner or a stray HTTP request gets
	// a clear rejection instead of being parsed as a file transfer.
	var buf bytes.Buffer
	if err := WriteHeader(&buf, Header{Name: "x", TotalSize: 1, Length: 1}); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}

	if got, want := buf.Bytes()[:4], []byte("GSHF"); !bytes.Equal(got, want) {
		t.Errorf("first 4 bytes = %q, want %q", got, want)
	}
}

func TestFixedHeaderSize_MatchesTheBytesActuallyWritten(t *testing.T) {
	// Readers size their buffer from FixedHeaderSize before they know the name
	// length, so the constant drifting away from the encoder would desynchronize
	// every peer. One byte of name makes the arithmetic below exact.
	var buf bytes.Buffer
	if err := WriteHeader(&buf, Header{Name: "x", TotalSize: 1, Length: 1}); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}

	if got, want := buf.Len()-len("x"), FixedHeaderSize; got != want {
		t.Errorf("fixed portion of the header is %d bytes, want FixedHeaderSize (%d)", got, want)
	}
}

// ---------------------------------------------------------------------------
// 2. WriteHeader
// ---------------------------------------------------------------------------

func TestWriteHeader_EncodesEveryFieldOfTheHeader(t *testing.T) {
	tests := []struct {
		name        string
		header      Header
		wantNameLen uint16
	}{
		{
			name:        "a short ascii name",
			header:      Header{Name: "a.txt", TotalSize: 1, Length: 1},
			wantNameLen: 5,
		},
		{
			name:        "path separators are just bytes to the encoder, SafeName cleans them on receipt",
			header:      Header{Name: "dir/sub/file.bin", TotalSize: 4096, Length: 4096},
			wantNameLen: 16,
		},
		{
			name:        "NAMELEN counts bytes, not runes",
			header:      Header{Name: "héllo.txt", TotalSize: 12, Length: 12},
			wantNameLen: 10, // é is two bytes in UTF-8
		},
		{
			name:        "SIZE 0, an empty file",
			header:      Header{Name: "empty.txt", TotalSize: 0, Length: 0},
			wantNameLen: 9,
		},
		{
			name:        "the shortest legal name is one byte",
			header:      Header{Name: "x", TotalSize: 1, Length: 1},
			wantNameLen: 1,
		},
		{
			name:        "the longest legal name is MaxNameLen bytes",
			header:      Header{Name: strings.Repeat("n", MaxNameLen), TotalSize: 1, Length: 1},
			wantNameLen: MaxNameLen,
		},
		{
			name:        "the largest legal size is MaxFileSize",
			header:      Header{Name: "big.bin", TotalSize: MaxFileSize, Length: MaxFileSize},
			wantNameLen: 7,
		},
		{
			name:        "a chunk that starts mid file",
			header:      Header{Name: "big.bin", TotalSize: 100, Offset: 40, Length: 60},
			wantNameLen: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteHeader(&buf, tt.header); err != nil {
				t.Fatalf("WriteHeader(%+v) error = %v, want nil", tt.header, err)
			}

			got := decodeHeader(t, buf.Bytes())

			if got.magic != Magic {
				t.Errorf("MAGIC = %#x, want %#x", got.magic, Magic)
			}
			if got.version != Version {
				t.Errorf("VERSION = %d, want %d", got.version, Version)
			}
			if got.nameLen != tt.wantNameLen {
				t.Errorf("NAMELEN = %d, want %d", got.nameLen, tt.wantNameLen)
			}
			if got.totalSize != uint64(tt.header.TotalSize) {
				t.Errorf("TOTALSIZE = %d, want %d", got.totalSize, tt.header.TotalSize)
			}
			if got.offset != uint64(tt.header.Offset) {
				t.Errorf("OFFSET = %d, want %d", got.offset, tt.header.Offset)
			}
			if got.length != uint64(tt.header.Length) {
				t.Errorf("LENGTH = %d, want %d", got.length, tt.header.Length)
			}
			if got.name != tt.header.Name {
				t.Errorf("NAME = %q, want %q", got.name, tt.header.Name)
			}
		})
	}
}

func TestWriteHeader_RejectsInvalidHeadersWithoutWritingAnything(t *testing.T) {
	// Two rules at once, and the second one matters more than it looks: a
	// rejected header must not leave a few stray bytes on the connection, or
	// the peer's next read would start mid-header and never resynchronize.
	tests := []struct {
		name    string
		header  Header
		wantErr error
	}{
		{
			name:    "a name must not be empty",
			header:  Header{Name: "", TotalSize: 1, Length: 1},
			wantErr: ErrBadName,
		},
		{
			name:    "a name must not exceed MaxNameLen",
			header:  Header{Name: strings.Repeat("n", MaxNameLen+1), TotalSize: 1, Length: 1},
			wantErr: ErrBadName,
		},
		{
			name:    "the MaxNameLen limit is in bytes, so multi byte names hit it sooner",
			header:  Header{Name: strings.Repeat("é", MaxNameLen/2+1), TotalSize: 1, Length: 1},
			wantErr: ErrBadName,
		},
		{
			// Go strings can hold arbitrary bytes, so this is reachable from a
			// caller. ReadHeader would reject the result, so we refuse to send it.
			name:    "a name must be valid UTF-8",
			header:  Header{Name: "\xff\xfe", TotalSize: 1, Length: 1},
			wantErr: ErrBadName,
		},
		{
			// Size is signed on the way in but unsigned on the wire, so a
			// negative size that slipped through would be encoded as an
			// enormous one. It is refused, and reported as a size limit error.
			name:    "a total size must not be negative",
			header:  Header{Name: "a.txt", TotalSize: -1},
			wantErr: ErrSizeTooLarge,
		},
		{
			name:    "the most negative size, which would wrap to 2^63 on the wire",
			header:  Header{Name: "a.txt", TotalSize: math.MinInt64},
			wantErr: ErrSizeTooLarge,
		},
		{
			name:    "a total size must not exceed MaxFileSize",
			header:  Header{Name: "big.bin", TotalSize: MaxFileSize + 1, Length: MaxFileSize + 1},
			wantErr: ErrSizeTooLarge,
		},
		{
			name:    "the name is validated before the size",
			header:  Header{Name: "", TotalSize: -1},
			wantErr: ErrBadName,
		},
		{
			name:    "an offset must not be negative",
			header:  Header{Name: "a.txt", TotalSize: 10, Offset: -1, Length: 5},
			wantErr: ErrBadRange,
		},
		{
			name:    "a length must not be negative",
			header:  Header{Name: "a.txt", TotalSize: 10, Length: -1},
			wantErr: ErrBadRange,
		},
		{
			name:    "offset plus length must not exceed the total size",
			header:  Header{Name: "a.txt", TotalSize: 10, Offset: 5, Length: 6},
			wantErr: ErrBadRange,
		},
		{
			name:    "the size is validated before the range",
			header:  Header{Name: "a.txt", TotalSize: -1, Offset: -1},
			wantErr: ErrSizeTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w countingWriter

			err := WriteHeader(&w, tt.header)
			if err == nil {
				t.Fatalf("WriteHeader(%+v) error = nil, want %v", tt.header, tt.wantErr)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("WriteHeader() error = %v, want it to wrap %v", err, tt.wantErr)
			}
			if w.calls != 0 {
				t.Errorf("writer received %d Write call(s) carrying %d byte(s); a rejected header must write nothing", w.calls, w.n)
			}
		})
	}
}

func TestWriteHeader_EmitsTheWholeHeaderInASingleWriteCall(t *testing.T) {
	// The header is assembled in one buffer before it reaches the writer. Four
	// small Writes would mean four syscalls, and on a socket possibly four TCP
	// segments for a 20 byte header.
	var w countingWriter

	const name = "a.txt"
	if err := WriteHeader(&w, Header{Name: name, TotalSize: 1, Length: 1}); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}

	if w.calls != 1 {
		t.Errorf("writer received %d Write calls, want exactly 1", w.calls)
	}
	if want := FixedHeaderSize + len(name); w.n != want {
		t.Errorf("writer received %d bytes, want %d", w.n, want)
	}
}

func TestWriteHeader_AppendsToTheWriterAndNeverRewindsIt(t *testing.T) {
	// Callers write headers onto a stream that already carries earlier bytes
	// (in phase 2, many headers share one connection).
	var buf bytes.Buffer
	buf.WriteString("PRELUDE")

	if err := WriteHeader(&buf, Header{Name: "a.txt", TotalSize: 1, Length: 1}); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}

	if got := buf.String(); !strings.HasPrefix(got, "PRELUDE") {
		t.Errorf("buffer = %q, want it to still start with %q", got, "PRELUDE")
	}
	if got, want := buf.Len(), len("PRELUDE")+FixedHeaderSize+len("a.txt"); got != want {
		t.Errorf("buffer length = %d, want %d", got, want)
	}
}

func TestWriteHeader_WrapsTheWritersErrorAndAddsContext(t *testing.T) {
	// %w keeps the cause reachable, so a caller can still ask
	// errors.Is(err, net.ErrClosed) three layers up the stack.
	sentinel := errors.New("disk on fire")

	err := WriteHeader(errWriter{err: sentinel}, Header{Name: "a.txt", TotalSize: 1, Length: 1})
	if err == nil {
		t.Fatal("WriteHeader() error = nil, want the writer's error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("WriteHeader() error = %v, want it to wrap %v", err, sentinel)
	}
	if !strings.Contains(err.Error(), "write header") {
		t.Errorf("WriteHeader() error = %q, want it to say %q so the reader knows which stage failed", err, "write header")
	}
}

// ---------------------------------------------------------------------------
// 3. ReadHeader
// ---------------------------------------------------------------------------

func TestReadHeader_RoundTripsEveryHeaderWriteHeaderAccepts(t *testing.T) {
	// The property that matters between two peers: decode(encode(h)) == h.
	headers := []Header{
		{Name: "a.txt", TotalSize: 1, Length: 1},
		{Name: "dir/sub/file.bin", TotalSize: 4096, Length: 4096},
		{Name: "héllo.txt", TotalSize: 12, Length: 12},
		{Name: "x", TotalSize: 1, Length: 1},
		{Name: strings.Repeat("n", MaxNameLen), TotalSize: 1, Length: 1},
		{Name: "big.bin", TotalSize: MaxFileSize, Length: MaxFileSize},
		{Name: "big.bin", TotalSize: 100, Offset: 40, Length: 60},
		{Name: "empty.txt", TotalSize: 0},
	}

	for _, want := range headers {
		t.Run(want.Name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteHeader(&buf, want); err != nil {
				t.Fatalf("WriteHeader(%+v) error = %v, want nil", want, err)
			}

			got, err := ReadHeader(&buf)
			if err != nil {
				t.Fatalf("ReadHeader() error = %v, want nil", err)
			}
			if got != want {
				t.Errorf("round trip = %+v, want %+v", got, want)
			}
		})
	}
}

func TestReadHeader_RejectsStreamsThatDoNotStartWithGSHF(t *testing.T) {
	tests := []struct {
		name  string
		magic uint32
	}{
		{name: "all zero bytes", magic: 0},
		{name: "all one bits", magic: 0xFFFFFFFF},
		{name: "one bit off from the real magic", magic: Magic + 1},
		{name: "the right bytes in the wrong byte order", magic: 0x46485347},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRaw()
			r.magic = tt.magic

			_, err := ReadHeader(bytes.NewReader(r.encode()))
			if !errors.Is(err, ErrBadMagic) {
				t.Errorf("ReadHeader() error = %v, want it to wrap %v", err, ErrBadMagic)
			}
		})
	}
}

func TestReadHeader_RejectsVersionsItDoesNotSpeak(t *testing.T) {
	// Version 2 is the only format this build understands. A newer or older peer
	// gets a clear mismatch rather than a misparse.
	for _, version := range []uint8{0, 1, Version + 1, 255} {
		r := validRaw()
		r.version = version

		_, err := ReadHeader(bytes.NewReader(r.encode()))
		if !errors.Is(err, ErrBadVersion) {
			t.Errorf("VERSION %d: ReadHeader() error = %v, want it to wrap %v", version, err, ErrBadVersion)
		}
	}
}

func TestReadHeader_RejectsNameLengthsOutsideOneToMaxNameLen(t *testing.T) {
	tests := []struct {
		name    string
		nameLen uint16
	}{
		{name: "zero, there is no such thing as an unnamed file", nameLen: 0},
		{name: "one byte over MaxNameLen", nameLen: MaxNameLen + 1},
		{name: "the largest value NAMELEN can hold", nameLen: ^uint16(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRaw()
			r.nameLen = tt.nameLen
			// Supply the bytes the header claims, so the rejection is provably
			// about the declared length and not about hitting EOF first.
			r.name = bytes.Repeat([]byte("n"), int(tt.nameLen))

			_, err := ReadHeader(bytes.NewReader(r.encode()))
			if !errors.Is(err, ErrBadName) {
				t.Errorf("ReadHeader() error = %v, want it to wrap %v", err, ErrBadName)
			}
		})
	}
}

func TestReadHeader_AcceptsAnySizeUpToMaxFileSizeIncludingZero(t *testing.T) {
	// MaxFileSize is a sanity cap, not a capability claim: a peer announcing
	// 2^63 bytes must be refused before the receiver acts on that number.
	//
	// A zero byte file is a legitimate thing to announce, and WriteHeader emits
	// one, so the two halves agree on the accepted range: 0..MaxFileSize.
	tests := []struct {
		name    string
		size    uint64
		wantErr error
	}{
		{name: "zero, an empty file", size: 0},
		{name: "one byte", size: 1},
		{name: "exactly MaxFileSize", size: MaxFileSize},
		{name: "one byte over MaxFileSize", size: MaxFileSize + 1, wantErr: ErrSizeTooLarge},
		{name: "the largest value TOTALSIZE can hold", size: ^uint64(0), wantErr: ErrSizeTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRaw()
			r.totalSize = tt.size
			r.offset = 0
			r.length = 0

			got, err := ReadHeader(bytes.NewReader(r.encode()))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("ReadHeader() error = %v, want it to wrap %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadHeader() error = %v, want nil", err)
			}
			if got.TotalSize != int64(tt.size) {
				t.Errorf("TotalSize = %d, want %d", got.TotalSize, tt.size)
			}
		})
	}
}

func TestReadHeader_RejectsARangeThatDoesNotFitWithinTheTotalSize(t *testing.T) {
	tests := []struct {
		name            string
		total, off, len uint64
	}{
		{name: "offset alone exceeds the total", total: 10, off: 11, len: 0},
		{name: "offset equals the total but length is not zero", total: 10, off: 10, len: 1},
		{name: "offset plus length overflows past the total", total: 10, off: 5, len: 6},
		{name: "length alone exceeds the total", total: 10, off: 0, len: 11},
		{name: "offset and length would overflow uint64 if added directly", total: 10, off: ^uint64(0), len: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRaw()
			r.totalSize = tt.total
			r.offset = tt.off
			r.length = tt.len

			_, err := ReadHeader(bytes.NewReader(r.encode()))
			if !errors.Is(err, ErrBadRange) {
				t.Errorf("ReadHeader() error = %v, want it to wrap %v", err, ErrBadRange)
			}
		})
	}
}

func TestReadHeader_AcceptsAChunkThatExactlyFillsTheTotalSize(t *testing.T) {
	r := validRaw()
	r.totalSize = 100
	r.offset = 40
	r.length = 60

	got, err := ReadHeader(bytes.NewReader(r.encode()))
	if err != nil {
		t.Fatalf("ReadHeader() error = %v, want nil", err)
	}
	if got.Offset != 40 || got.Length != 60 || got.TotalSize != 100 {
		t.Errorf("Offset/Length/TotalSize = %d/%d/%d, want 40/60/100", got.Offset, got.Length, got.TotalSize)
	}
}

func TestReadHeader_RejectsNamesThatAreNotValidUTF8(t *testing.T) {
	// Header.Name is a Go string, and a string built from invalid UTF-8 breaks
	// every downstream assumption (logging, path handling, JSON encoding).
	r := validRaw()
	r.name = []byte{0xff, 0xfe, 0xfd}
	r.nameLen = uint16(len(r.name))

	_, err := ReadHeader(bytes.NewReader(r.encode()))
	if !errors.Is(err, ErrBadName) {
		t.Fatalf("ReadHeader() error = %v, want it to wrap %v", err, ErrBadName)
	}
	if !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("ReadHeader() error = %q, want it to say why the name was rejected", err)
	}
}

func TestReadHeader_ReportsTruncatedStreamsInsteadOfGuessing(t *testing.T) {
	// A connection can die at any byte offset. Every prefix of a valid header
	// must fail loudly, and the message must say which stage ran out of bytes.
	full := validRaw().encode()

	tests := []struct {
		name    string
		input   []byte
		wantErr error
		wantCtx string
	}{
		{
			name:    "nothing at all, the peer connected and hung up",
			input:   nil,
			wantErr: io.EOF,
			wantCtx: "read header",
		},
		{
			name:    "half of the magic",
			input:   full[:2],
			wantErr: io.ErrUnexpectedEOF,
			wantCtx: "read header",
		},
		{
			name:    "one byte short of the fixed header",
			input:   full[:FixedHeaderSize-1],
			wantErr: io.ErrUnexpectedEOF,
			wantCtx: "read header",
		},
		{
			// io.ReadFull returns a plain io.EOF when it reads nothing at all,
			// and io.ErrUnexpectedEOF only once some bytes have arrived. The
			// name read starts fresh, so a missing name is a plain EOF.
			name:    "a complete fixed header with the name missing",
			input:   full[:FixedHeaderSize],
			wantErr: io.EOF,
			wantCtx: "read name",
		},
		{
			name:    "the name one byte short",
			input:   full[:len(full)-1],
			wantErr: io.ErrUnexpectedEOF,
			wantCtx: "read name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadHeader(bytes.NewReader(tt.input))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadHeader() error = %v, want it to wrap %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantCtx) {
				t.Errorf("ReadHeader() error = %q, want it to say %q so the reader knows which stage failed", err, tt.wantCtx)
			}
		})
	}
}

func TestReadHeader_NeverReturnsAPartiallyFilledHeader(t *testing.T) {
	// A hostile peer declares MaxNameLen and then sends five bytes. The failure
	// must be an error, not a Header whose Name is padded with NUL bytes.
	r := validRaw()
	r.nameLen = MaxNameLen
	r.name = []byte("a.txt")

	got, err := ReadHeader(bytes.NewReader(r.encode()))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadHeader() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}
	if got != (Header{}) {
		t.Errorf("ReadHeader() = %+v, want the zero Header alongside an error", got)
	}
}

func TestReadHeader_WrapsTheReadersErrorAndAddsContext(t *testing.T) {
	sentinel := errors.New("connection reset by peer")

	_, err := ReadHeader(errReader{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Errorf("ReadHeader() error = %v, want it to wrap %v", err, sentinel)
	}
}

func TestReadHeader_StopsAtTheEndOfTheNameAndLeavesThePayloadInTheStream(t *testing.T) {
	// The header and the file body share one stream, so over-reading by even
	// one byte would silently corrupt every transfer. This is the test that
	// pins the boundary between the two.
	const payload = "PAYLOAD"

	var buf bytes.Buffer
	if err := WriteHeader(&buf, Header{Name: "a.txt", TotalSize: int64(len(payload)), Length: int64(len(payload))}); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}
	buf.WriteString(payload)

	r := bytes.NewReader(buf.Bytes())
	if _, err := ReadHeader(r); err != nil {
		t.Fatalf("ReadHeader() error = %v, want nil", err)
	}

	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v, want nil", err)
	}
	if string(rest) != payload {
		t.Errorf("bytes left in the stream = %q, want %q", rest, payload)
	}
}

func TestReadHeader_DecodesBackToBackHeadersWithoutResynchronizing(t *testing.T) {
	// Framing: because every header carries its own lengths, many of them can
	// share one connection. Chunked transfers depend entirely on this property,
	// even though each chunk here happens to dial its own connection.
	want := []Header{
		{Name: "first.txt", TotalSize: 1, Length: 1},
		{Name: "second.bin", TotalSize: 4096, Length: 4096},
		{Name: "héllo.txt", TotalSize: 12, Length: 12},
	}

	var buf bytes.Buffer
	for _, h := range want {
		if err := WriteHeader(&buf, h); err != nil {
			t.Fatalf("WriteHeader(%+v) error = %v, want nil", h, err)
		}
	}

	for i, w := range want {
		got, err := ReadHeader(&buf)
		if err != nil {
			t.Fatalf("ReadHeader() #%d error = %v, want nil", i, err)
		}
		if got != w {
			t.Errorf("header #%d = %+v, want %+v", i, got, w)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("%d byte(s) left over after reading every header, want 0", buf.Len())
	}
}

func TestReadHeader_ReassemblesAHeaderDeliveredOneByteAtATime(t *testing.T) {
	// TCP is a byte stream, not a message stream: a Read can return 1 byte with
	// no error. Code that assumes "one Read is one message" works on localhost
	// and fails on a real network. iotest.OneByteReader simulates the worst case
	// so the guarantee is checked on every run, not hoped for.
	want := Header{Name: "héllo.txt", TotalSize: 4096, Length: 4096}

	var buf bytes.Buffer
	if err := WriteHeader(&buf, want); err != nil {
		t.Fatalf("WriteHeader() error = %v, want nil", err)
	}

	got, err := ReadHeader(iotest.OneByteReader(&buf))
	if err != nil {
		t.Fatalf("ReadHeader() error = %v, want nil", err)
	}
	if got != want {
		t.Errorf("ReadHeader() = %+v, want %+v", got, want)
	}
}

func TestReadHeader_ChecksFieldsInWireOrder(t *testing.T) {
	// Every field in these headers is invalid; the error names whichever one is
	// checked first. Magic before version matters most: a stream that is not
	// gshift at all should be reported as such, never as a version mismatch.
	tests := []struct {
		name    string
		mutate  func(*rawHeader)
		wantErr error
	}{
		{
			name: "the magic is checked before the version",
			mutate: func(r *rawHeader) {
				r.magic = 0
				r.version = 99
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "the version is checked before the name length",
			mutate: func(r *rawHeader) {
				r.version = 99
				r.nameLen = 0
			},
			wantErr: ErrBadVersion,
		},
		{
			name: "the name length is checked before the total size",
			mutate: func(r *rawHeader) {
				r.nameLen = 0
				r.totalSize = MaxFileSize + 1
			},
			wantErr: ErrBadName,
		},
		{
			name: "the total size is checked before the range",
			mutate: func(r *rawHeader) {
				r.totalSize = MaxFileSize + 1
				r.offset = ^uint64(0) // would also fail the range check
			},
			wantErr: ErrSizeTooLarge,
		},
		{
			name: "the range is checked before any name bytes are read",
			mutate: func(r *rawHeader) {
				r.offset = 11
				r.totalSize = 10
				r.name = nil // would be an unexpected EOF if we got that far
			},
			wantErr: ErrBadRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRaw()
			tt.mutate(&r)

			_, err := ReadHeader(bytes.NewReader(r.encode()))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ReadHeader() error = %v, want it to wrap %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. SafeName
// ---------------------------------------------------------------------------

func TestSafeName_ReducesAnyPathToItsFinalElement(t *testing.T) {
	// The rule in one line: whatever a peer sends, the server writes into its
	// own directory and nowhere else. Without this, a name like
	// "../../.ssh/authorized_keys" would own the machine.
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "a plain file name is unchanged", input: "a.txt", want: "a.txt"},
		{name: "one directory is stripped", input: "dir/a.txt", want: "a.txt"},
		{name: "nested directories are stripped", input: "a/b/c/d.txt", want: "d.txt"},
		{name: "an absolute path is stripped", input: "/etc/passwd", want: "passwd"},
		{name: "a traversal is stripped", input: "../../etc/passwd", want: "passwd"},
		{name: "a traversal hidden mid path is stripped", input: "a/../../b.txt", want: "b.txt"},
		{name: "a trailing slash yields the last element", input: "dir/sub/", want: "sub"},
		{name: "a single element with a trailing slash", input: "dir/", want: "dir"},
		{name: "a dotfile is a legitimate name", input: ".bashrc", want: ".bashrc"},
		{name: "a dotfile under a directory", input: "/home/u/.ssh/config", want: "config"},
		{name: "three dots is an ordinary name, not a traversal", input: "...", want: "..."},
		{name: "a dot dot prefix is an ordinary name", input: "..hidden", want: "..hidden"},
		{name: "multi byte names survive intact", input: "dir/héllo.txt", want: "héllo.txt"},
		{name: "spaces are preserved", input: "dir/my file.txt", want: "my file.txt"},
		{name: "a newline is preserved", input: "dir/a\nb.txt", want: "a\nb.txt"},
		{name: "a NUL before the last separator is discarded with the directory", input: "a\x00b/c.txt", want: "c.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeName(tt.input)
			if err != nil {
				t.Fatalf("SafeName(%q) error = %v, want nil", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("SafeName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeName_RejectsAnythingThatIsNotAUsableFileName(t *testing.T) {
	// Stripping directories is not enough on its own: some paths reduce to
	// something that names a directory rather than a file, and a NUL byte can
	// truncate a name inside the C based syscalls underneath os.OpenFile.
	tests := []struct {
		name  string
		input string
	}{
		{name: "the empty string", input: ""},
		{name: "the current directory", input: "."},
		{name: "the parent directory", input: ".."},
		{name: "the filesystem root", input: "/"},
		{name: "a doubled separator, which is still the root", input: "//"},
		{name: "a path ending in dot dot", input: "a/b/.."},
		{name: "a path ending in dot", input: "a/b/."},
		{name: "a lone NUL byte", input: "\x00"},
		{name: "a NUL inside the base name", input: "a\x00b.txt"},
		{name: "a NUL in the base name after a directory", input: "dir/a\x00b.txt"},
		{name: "a trailing NUL", input: "a.txt\x00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeName(tt.input)
			if err == nil {
				t.Fatalf("SafeName(%q) = %q, want an error", tt.input, got)
			}
			if !errors.Is(err, ErrBadName) {
				t.Errorf("SafeName(%q) error = %v, want it to wrap %v", tt.input, err, ErrBadName)
			}
			if got != "" {
				t.Errorf("SafeName(%q) = %q, want the empty string alongside an error", tt.input, got)
			}
		})
	}
}

func TestSafeName_IsIdempotent(t *testing.T) {
	// Sanitizing twice must equal sanitizing once, so a caller who is unsure
	// whether a name has already been cleaned can just call it again.
	inputs := []string{"a.txt", "dir/a.txt", "../../etc/passwd", ".bashrc", "dir/sub/", "...", "héllo.txt"}

	for _, in := range inputs {
		once, err := SafeName(in)
		if err != nil {
			t.Fatalf("SafeName(%q) error = %v, want nil", in, err)
		}

		twice, err := SafeName(once)
		if err != nil {
			t.Fatalf("SafeName(%q) error = %v, want nil on the second pass", once, err)
		}
		if twice != once {
			t.Errorf("SafeName(SafeName(%q)) = %q, want %q", in, twice, once)
		}
	}
}

func TestSafeName_TreatsBackslashesAsOrdinaryCharactersOnUnix(t *testing.T) {
	// A Windows peer may send a backslash separated path. On Unix a backslash
	// is not a separator, so the whole thing stays one (ugly, but harmless)
	// file name that still cannot escape the destination directory.
	if filepath.Separator != '/' {
		t.Skip("this documents Unix path semantics")
	}

	const windowsPath = `..\..\windows\system32\evil.dll`

	got, err := SafeName(windowsPath)
	if err != nil {
		t.Fatalf("SafeName(%q) error = %v, want nil", windowsPath, err)
	}
	if got != windowsPath {
		t.Errorf("SafeName(%q) = %q, want the whole string as one file name", windowsPath, got)
	}
}

// ---------------------------------------------------------------------------
// 5. Fuzz targets
//
// The tests above pin down named cases. These state the invariants that must
// hold for EVERY input, including ones nobody thought to write down. The seed
// corpus doubles as a list of the interesting shapes.
//
//	go test -run xxx -fuzz FuzzReadHeader ./internal/protocol
// ---------------------------------------------------------------------------

// FuzzWriteHeader asserts: anything WriteHeader accepts, it encodes faithfully.
func FuzzWriteHeader(f *testing.F) {
	f.Add("a.txt", int64(1), int64(0), int64(1))
	f.Add("héllo.txt", int64(4096), int64(0), int64(4096))
	f.Add(strings.Repeat("n", MaxNameLen), int64(MaxFileSize), int64(0), int64(MaxFileSize))
	f.Add("empty.txt", int64(0), int64(0), int64(0))
	f.Add("chunk.bin", int64(100), int64(40), int64(60))

	f.Fuzz(func(t *testing.T, name string, totalSize, offset, length int64) {
		// Only inputs WriteHeader is documented to accept are in scope here;
		// the rejections are covered by the table tests above.
		if totalSize < 0 || totalSize > MaxFileSize {
			t.Skip()
		}
		if offset < 0 || length < 0 || offset > totalSize-length {
			t.Skip()
		}
		if n := len(name); n == 0 || n > MaxNameLen {
			t.Skip()
		}
		if !utf8.ValidString(name) {
			t.Skip()
		}

		h := Header{Name: name, TotalSize: totalSize, Offset: offset, Length: length}

		var buf bytes.Buffer
		if err := WriteHeader(&buf, h); err != nil {
			t.Fatalf("WriteHeader(%+v) error = %v, want nil", h, err)
		}

		got := decodeHeader(t, buf.Bytes())
		if got.magic != Magic || got.version != Version {
			t.Errorf("MAGIC/VERSION = %#x/%d, want %#x/%d", got.magic, got.version, Magic, Version)
		}
		if got.name != name {
			t.Errorf("NAME = %q, want %q", got.name, name)
		}
		if got.totalSize != uint64(totalSize) {
			t.Errorf("TOTALSIZE = %d, want %d", got.totalSize, totalSize)
		}
		if got.offset != uint64(offset) {
			t.Errorf("OFFSET = %d, want %d", got.offset, offset)
		}
		if got.length != uint64(length) {
			t.Errorf("LENGTH = %d, want %d", got.length, length)
		}
	})
}

// FuzzReadHeader asserts three things about arbitrary, hostile bytes:
// ReadHeader never panics; anything it accepts is within the documented limits;
// and anything it accepts re-encodes to the exact bytes it consumed.
func FuzzReadHeader(f *testing.F) {
	f.Add(validRaw().encode())
	f.Add([]byte("GSHF"))
	f.Add([]byte{})

	invalidUTF8Name := validRaw()
	invalidUTF8Name.name = []byte{0xff}
	invalidUTF8Name.nameLen = 1
	f.Add(invalidUTF8Name.encode())

	chunk := validRaw()
	chunk.totalSize = 100
	chunk.offset = 40
	chunk.length = 60
	f.Add(chunk.encode())

	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := ReadHeader(bytes.NewReader(data))
		if err != nil {
			return // any rejection is fine; surviving the input is the point
		}

		if n := len(h.Name); n == 0 || n > MaxNameLen {
			t.Errorf("accepted a name of %d bytes, want 1..%d", n, MaxNameLen)
		}
		if h.TotalSize < 0 || h.TotalSize > MaxFileSize {
			t.Errorf("accepted total size %d, want 0..%d", h.TotalSize, MaxFileSize)
		}
		if h.Offset < 0 || h.Length < 0 || h.Offset+h.Length > h.TotalSize {
			t.Errorf("accepted offset %d, length %d outside 0..TotalSize (%d)", h.Offset, h.Length, h.TotalSize)
		}

		var buf bytes.Buffer
		if err := WriteHeader(&buf, h); err != nil {
			t.Fatalf("WriteHeader(%+v) error = %v, want nil for a header ReadHeader accepted", h, err)
		}
		if got, want := buf.Bytes(), data[:buf.Len()]; !bytes.Equal(got, want) {
			t.Errorf("re-encoded header =\n %#v\nwant\n %#v", got, want)
		}
	})
}

// FuzzSafeName asserts the security property itself: for any input at all, the
// result is either an error or a single path element that cannot walk out of
// the destination directory.
func FuzzSafeName(f *testing.F) {
	f.Add("a.txt")
	f.Add("../../etc/passwd")
	f.Add("dir/sub/")
	f.Add("a\x00b.txt")
	f.Add("")

	f.Fuzz(func(t *testing.T, name string) {
		got, err := SafeName(name)
		if err != nil {
			if got != "" {
				t.Errorf("SafeName(%q) = %q alongside error %v, want the empty string", name, got, err)
			}
			return
		}

		if got == "" || got == "." || got == ".." {
			t.Errorf("SafeName(%q) = %q, want a usable file name", name, got)
		}
		if strings.ContainsRune(got, 0) {
			t.Errorf("SafeName(%q) = %q, want no NUL byte", name, got)
		}
		if strings.ContainsRune(got, filepath.Separator) {
			t.Errorf("SafeName(%q) = %q, want no path separator", name, got)
		}
		if got != filepath.Base(got) {
			t.Errorf("SafeName(%q) = %q, want a bare path element", name, got)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. Test helpers
//
// Deliberately last. A reader opening this file should meet the rules first and
// the plumbing only if they need it.
// ---------------------------------------------------------------------------

// decodedHeader mirrors the wire format field by field, so a failing test can
// say "NAMELEN = 6, want 5" instead of printing two opaque byte slices.
type decodedHeader struct {
	magic     uint32
	version   uint8
	nameLen   uint16
	totalSize uint64
	offset    uint64
	length    uint64
	name      string
}

// decodeHeader splits a buffer written by WriteHeader into its fields, failing
// the test if the buffer is not internally consistent.
//
// It is a second, independent decoder: using ReadHeader here instead would let
// a matching pair of bugs in the encoder and decoder cancel out and pass.
func decodeHeader(t *testing.T, b []byte) decodedHeader {
	t.Helper() // failures are reported at the caller's line, not this one

	if len(b) < FixedHeaderSize {
		t.Fatalf("encoded header is %d bytes, want at least FixedHeaderSize (%d)", len(b), FixedHeaderSize)
	}

	d := decodedHeader{
		magic:     binary.BigEndian.Uint32(b[0:4]),
		version:   b[4],
		nameLen:   binary.BigEndian.Uint16(b[5:7]),
		totalSize: binary.BigEndian.Uint64(b[7:15]),
		offset:    binary.BigEndian.Uint64(b[15:23]),
		length:    binary.BigEndian.Uint64(b[23:31]),
	}

	if got, want := len(b), FixedHeaderSize+int(d.nameLen); got != want {
		t.Fatalf("encoded header is %d bytes, want %d (FixedHeaderSize + NAMELEN %d)", got, want, d.nameLen)
	}
	d.name = string(b[FixedHeaderSize:])

	return d
}

// rawHeader builds header bytes field by field, bypassing WriteHeader's
// validation, so tests can hand ReadHeader wire data no honest peer would send.
type rawHeader struct {
	magic     uint32
	version   uint8
	nameLen   uint16
	totalSize uint64
	offset    uint64
	length    uint64
	name      []byte
}

// validRaw returns a well formed header. Each test mutates exactly the one
// field it is about, which is what makes the test names true.
func validRaw() rawHeader {
	return rawHeader{
		magic:     Magic,
		version:   Version,
		nameLen:   5,
		totalSize: 1,
		offset:    0,
		length:    1,
		name:      []byte("a.txt"),
	}
}

// encode serializes r. NAMELEN is written exactly as set, independently of
// len(name), so a header can lie about how many name bytes follow.
func (r rawHeader) encode() []byte {
	buf := make([]byte, 0, FixedHeaderSize+len(r.name))
	buf = binary.BigEndian.AppendUint32(buf, r.magic)
	buf = append(buf, r.version)
	buf = binary.BigEndian.AppendUint16(buf, r.nameLen)
	buf = binary.BigEndian.AppendUint64(buf, r.totalSize)
	buf = binary.BigEndian.AppendUint64(buf, r.offset)
	buf = binary.BigEndian.AppendUint64(buf, r.length)
	buf = append(buf, r.name...)
	return buf
}

// errWriter fails every Write with a fixed error, standing in for a dead socket.
type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// errReader fails every Read with a fixed error, standing in for a reset peer.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// countingWriter accepts everything and records what it was given, so a test
// can assert on how many Write calls happened, not just on the bytes.
type countingWriter struct {
	calls int
	n     int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.calls++
	w.n += len(p)
	return len(p), nil
}

// Compile time assertions that the fakes really do satisfy the interfaces they
// are passed as. A var _ T = x line is the idiomatic way to say "this must
// implement that" without any runtime cost.
var (
	_ io.Writer = errWriter{}
	_ io.Writer = (*countingWriter)(nil)
	_ io.Reader = errReader{}
)
