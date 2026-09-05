// These tests are the specification of the sending half of a transfer.
//
// Layout, in reading order:
//
//  1. Helpers      - a fake server that reads one or more transfers
//  2. Send         - one file over a single connection, from open to last byte
//  3. Send, split  - one file split across several connections
//  4. splitRanges  - how a file size is divided into chunk ranges
//  5. rate         - the throughput line
//
// The fake server is not internal/server on purpose. a client test must not
// fail because of a bug in the receiver.
package client

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// 1. Helpers
// ---------------------------------------------------------------------------

// transfer is what the fake server made of one connection.
type transfer struct {
	hdr     protocol.Header
	payload []byte
	err     error
}

// acceptOne serves exactly one connection and reports what arrived on it.
func acceptOne(t *testing.T) (addr string, received <-chan transfer) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	t.Cleanup(func() { ln.Close() })

	// buffered so the goroutine still finishes if the test fails early
	out := make(chan transfer, 1)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			out <- transfer{err: err}
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

		var tr transfer
		if tr.hdr, tr.err = protocol.ReadHeader(conn); tr.err == nil {
			// read to EOF, so a client that sent too much would be caught
			tr.payload, tr.err = io.ReadAll(conn)
		}
		out <- tr
	}()

	return ln.Addr().String(), out
}

// acceptN serves exactly n connections and reports what arrived on each, in
// whatever order they were accepted.
func acceptN(t *testing.T, n int) (addr string, received <-chan transfer) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	t.Cleanup(func() { ln.Close() })

	out := make(chan transfer, n)

	for range n {
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				out <- transfer{err: err}
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

			var tr transfer
			if tr.hdr, tr.err = protocol.ReadHeader(conn); tr.err == nil {
				tr.payload, tr.err = io.ReadAll(conn)
			}
			out <- tr
		}()
	}

	return ln.Addr().String(), out
}

// mustReceive bounds the wait so a bug fails in seconds, not at the test timeout.
func mustReceive(t *testing.T, received <-chan transfer) transfer {
	t.Helper()

	select {
	case tr := <-received:
		if tr.err != nil {
			t.Fatalf("the server could not read the transfer: %v", tr.err)
		}
		return tr
	case <-time.After(5 * time.Second):
		t.Fatal("the server never finished reading the transfer")
		return transfer{}
	}
}

func tempFile(t *testing.T, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return path
}

// deadAddr is an address nothing is listening on.
func deadAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// ---------------------------------------------------------------------------
// 2. Send
// ---------------------------------------------------------------------------

func TestSend_WritesTheHeaderThenExactlyTheFileBytes(t *testing.T) {
	content := []byte("hello there")
	path := tempFile(t, "notes.txt", content)

	addr, received := acceptOne(t)
	if err := Send(addr, path, 1); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if want := (protocol.Header{Name: "notes.txt", TotalSize: int64(len(content)), Length: int64(len(content))}); got.hdr != want {
		t.Errorf("header = %+v, want %+v", got.hdr, want)
	}
	if string(got.payload) != string(content) {
		t.Errorf("payload = %q, want %q", got.payload, content)
	}
}

func TestSend_SendsOnlyTheBaseName(t *testing.T) {
	// the receiver must not learn the sender's directory layout
	dir := filepath.Join(t.TempDir(), "deep", "nested")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	path := filepath.Join(dir, "report.pdf")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("writing report.pdf: %v", err)
	}

	addr, received := acceptOne(t)
	if err := Send(addr, path, 1); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	if got := mustReceive(t, received); got.hdr.Name != "report.pdf" {
		t.Errorf("NAME = %q, want %q", got.hdr.Name, "report.pdf")
	}
}

func TestSend_SendsAnEmptyFile(t *testing.T) {
	path := tempFile(t, "empty.bin", nil)

	addr, received := acceptOne(t)
	if err := Send(addr, path, 1); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if got.hdr.TotalSize != 0 {
		t.Errorf("SIZE = %d, want 0", got.hdr.TotalSize)
	}
	if len(got.payload) != 0 {
		t.Errorf("payload = %q, want empty", got.payload)
	}
}

func TestSend_SendsAPayloadLargerThanOneCopyBuffer(t *testing.T) {
	// past io.Copy's 32 KiB buffer, so the loop runs more than once
	content := make([]byte, 320*1024)
	for i := range content {
		content[i] = byte(i)
	}
	path := tempFile(t, "big.bin", content)

	addr, received := acceptOne(t)
	if err := Send(addr, path, 1); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if got.hdr.TotalSize != int64(len(content)) {
		t.Errorf("SIZE = %d, want %d", got.hdr.TotalSize, len(content))
	}
	if string(got.payload) != string(content) {
		t.Errorf("payload is %d bytes and differs, want %d bytes identical", len(got.payload), len(content))
	}
}

func TestSend_SendsAUnicodeName(t *testing.T) {
	path := tempFile(t, "héllo.txt", []byte("x"))

	addr, received := acceptOne(t)
	if err := Send(addr, path, 1); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	if got := mustReceive(t, received); got.hdr.Name != "héllo.txt" {
		t.Errorf("NAME = %q, want %q", got.hdr.Name, "héllo.txt")
	}
}

func TestSend_RejectsAnythingThatIsNotARegularFile(t *testing.T) {
	// all of these fail before a socket is opened, which is why the address is dead
	tests := []struct {
		name    string
		path    func(t *testing.T) string
		wantMsg string
	}{
		{
			name:    "a directory",
			path:    func(t *testing.T) string { return t.TempDir() },
			wantMsg: "not a regular file",
		},
		{
			name:    "a file that does not exist",
			path:    func(t *testing.T) string { return filepath.Join(t.TempDir(), "nope.txt") },
			wantMsg: "open",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Send("127.0.0.1:1", tt.path(t), 1)
			if err == nil {
				t.Fatal("Send() error = nil, want a local validation error")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("Send() error = %q, want it to mention %q", err, tt.wantMsg)
			}
		})
	}
}

func TestSend_ReturnsAnErrorWhenNothingIsListening(t *testing.T) {
	path := tempFile(t, "a.txt", []byte("x"))

	err := Send(deadAddr(t), path, 1)
	if err == nil {
		t.Fatal("Send() error = nil, want a dial error")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("Send() error = %q, want it to mention dial", err)
	}
}

func TestSend_ReturnsAnErrorForAnUnparseableAddress(t *testing.T) {
	path := tempFile(t, "a.txt", []byte("x"))

	if err := Send("definitely not an address", path, 1); err == nil {
		t.Fatal("Send() error = nil, want a dial error")
	}
}

func TestSend_ReportsTheServerHangingUpMidTransfer(t *testing.T) {
	// enough bytes that the write cannot all fit in the socket buffer
	content := make([]byte, 8<<20)
	path := tempFile(t, "big.bin", content)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = protocol.ReadHeader(conn)
		conn.Close()
	}()

	if err := Send(ln.Addr().String(), path, 1); err == nil {
		t.Error("Send() error = nil, want the broken connection to be reported")
	}
}

// ---------------------------------------------------------------------------
// 3. Send, split across several connections
// ---------------------------------------------------------------------------

func TestSend_SplitsALargeFileAcrossParallelConnections(t *testing.T) {
	const n = 4
	content := make([]byte, minChunkSize*n)
	for i := range content {
		content[i] = byte(i)
	}
	path := tempFile(t, "big.bin", content)

	addr, received := acceptN(t, n)
	if err := Send(addr, path, n); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	transfers := make([]transfer, n)
	for i := range transfers {
		transfers[i] = mustReceive(t, received)
	}
	sort.Slice(transfers, func(i, j int) bool { return transfers[i].hdr.Offset < transfers[j].hdr.Offset })

	var got bytes.Buffer
	var offset int64
	for _, tr := range transfers {
		if tr.hdr.Name != "big.bin" {
			t.Errorf("NAME = %q, want %q", tr.hdr.Name, "big.bin")
		}
		if tr.hdr.TotalSize != int64(len(content)) {
			t.Errorf("TOTALSIZE = %d, want %d", tr.hdr.TotalSize, len(content))
		}
		if tr.hdr.Offset != offset {
			t.Errorf("chunk offset = %d, want %d: chunks must be contiguous, no gaps or overlaps", tr.hdr.Offset, offset)
		}
		if tr.hdr.Length != int64(len(tr.payload)) {
			t.Errorf("chunk declared length %d, want it to match the %d bytes actually sent", tr.hdr.Length, len(tr.payload))
		}
		got.Write(tr.payload)
		offset += tr.hdr.Length
	}
	if offset != int64(len(content)) {
		t.Errorf("chunks covered %d bytes total, want %d", offset, len(content))
	}
	if !bytes.Equal(got.Bytes(), content) {
		t.Error("reassembling the chunks in offset order does not reproduce the original content")
	}
}

func TestSend_ClampsParallelismForASmallFile(t *testing.T) {
	// requesting 8 connections for a 5 byte file must still send it whole,
	// over a single connection.
	path := tempFile(t, "small.txt", []byte("hello"))

	addr, received := acceptOne(t)
	if err := Send(addr, path, 8); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if got.hdr.Length != 5 || got.hdr.Offset != 0 {
		t.Errorf("offset/length = %d/%d, want 0/5 (one connection carrying the whole file)", got.hdr.Offset, got.hdr.Length)
	}
}

// ---------------------------------------------------------------------------
// 4. splitRanges
// ---------------------------------------------------------------------------

func TestSplitRanges_DividesEvenlyWhenSizeIsAnExactMultipleOfN(t *testing.T) {
	got := splitRanges(4*minChunkSize, 4)
	want := []chunkRange{
		{offset: 0, length: minChunkSize},
		{offset: minChunkSize, length: minChunkSize},
		{offset: 2 * minChunkSize, length: minChunkSize},
		{offset: 3 * minChunkSize, length: minChunkSize},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitRanges() = %+v, want %+v", got, want)
	}
}

func TestSplitRanges_SpreadsTheRemainderOverTheEarliestRanges(t *testing.T) {
	// 3 chunk sized ranges plus a remainder of 10: the remainder must land on
	// the earliest ranges, not be dropped or piled onto the last one.
	const n = 3
	size := int64(n)*minChunkSize + 10

	got := splitRanges(size, n)
	want := []chunkRange{
		{offset: 0, length: minChunkSize + 4},
		{offset: minChunkSize + 4, length: minChunkSize + 3},
		{offset: 2*minChunkSize + 7, length: minChunkSize + 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitRanges(%d, %d) = %+v, want %+v", size, n, got, want)
	}
}

func TestSplitRanges_ClampsToOneRangeWhenNIsLessThanTwo(t *testing.T) {
	for _, n := range []int{1, 0, -1} {
		got := splitRanges(100, n)
		want := []chunkRange{{offset: 0, length: 100}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("splitRanges(100, %d) = %+v, want %+v", n, got, want)
		}
	}
}

func TestSplitRanges_ReturnsOneEmptyRangeForAnEmptyFile(t *testing.T) {
	got := splitRanges(0, 8)
	want := []chunkRange{{offset: 0, length: 0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitRanges(0, 8) = %+v, want %+v", got, want)
	}
}

func TestSplitRanges_ClampsParallelismSoNoChunkFallsBelowMinChunkSize(t *testing.T) {
	// only 1.5x minChunkSize exists, so 4 requested connections must collapse
	// down to the 1 that can actually be filled.
	size := int64(minChunkSize) + int64(minChunkSize)/2

	got := splitRanges(size, 4)
	want := []chunkRange{{offset: 0, length: size}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitRanges(%d, 4) = %+v, want %+v", size, got, want)
	}
}

func TestSplitRanges_RangesAreContiguousAndCoverTheWholeFile(t *testing.T) {
	// the property that matters regardless of the exact split: no gaps, no
	// overlaps, and the ranges cover exactly size bytes.
	tests := []struct {
		size int64
		n    int
	}{
		{size: 1, n: 1},
		{size: minChunkSize*5 + 7, n: 5},
		{size: minChunkSize*5 + 7, n: 3},
		{size: minChunkSize * 100, n: 16},
	}

	for _, tt := range tests {
		ranges := splitRanges(tt.size, tt.n)
		var offset int64
		for i, r := range ranges {
			if r.offset != offset {
				t.Errorf("size=%d n=%d: range %d starts at %d, want %d", tt.size, tt.n, i, r.offset, offset)
			}
			offset += r.length
		}
		if offset != tt.size {
			t.Errorf("size=%d n=%d: ranges cover %d bytes, want %d", tt.size, tt.n, offset, tt.size)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. rate
// ---------------------------------------------------------------------------

func TestRate_GuardsAgainstAZeroDuration(t *testing.T) {
	// a coarse clock can report 0 for a small file, and dividing by it prints +Inf
	tests := []struct {
		name string
		n    int64
		d    time.Duration
		want string
	}{
		{name: "zero duration", n: 1024, d: 0, want: "instant"},
		{name: "negative duration", n: 1024, d: -time.Second, want: "instant"},
		{name: "one MiB in one second", n: 1 << 20, d: time.Second, want: "1.0 MiB/s"},
		{name: "an empty file", n: 0, d: time.Second, want: "0.0 MiB/s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rate(tt.n, tt.d); got != tt.want {
				t.Errorf("rate(%d, %v) = %q, want %q", tt.n, tt.d, got, tt.want)
			}
		})
	}
}
