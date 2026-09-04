// These tests are the specification of the sending half of a transfer.
//
// Layout, in reading order:
//
//  1. Helpers   - a fake server that reads one transfer
//  2. Send      - one file, from open to last byte on the wire
//  3. rate      - the throughput line
//
// The fake server is not internal/server on purpose. a client test must not
// fail because of a bug in the receiver.
package client

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
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
	if err := Send(addr, path); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if want := (protocol.Header{Name: "notes.txt", Size: int64(len(content))}); got.hdr != want {
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
	if err := Send(addr, path); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	if got := mustReceive(t, received); got.hdr.Name != "report.pdf" {
		t.Errorf("NAME = %q, want %q", got.hdr.Name, "report.pdf")
	}
}

func TestSend_SendsAnEmptyFile(t *testing.T) {
	path := tempFile(t, "empty.bin", nil)

	addr, received := acceptOne(t)
	if err := Send(addr, path); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if got.hdr.Size != 0 {
		t.Errorf("SIZE = %d, want 0", got.hdr.Size)
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
	if err := Send(addr, path); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	got := mustReceive(t, received)
	if got.hdr.Size != int64(len(content)) {
		t.Errorf("SIZE = %d, want %d", got.hdr.Size, len(content))
	}
	if string(got.payload) != string(content) {
		t.Errorf("payload is %d bytes and differs, want %d bytes identical", len(got.payload), len(content))
	}
}

func TestSend_SendsAUnicodeName(t *testing.T) {
	path := tempFile(t, "héllo.txt", []byte("x"))

	addr, received := acceptOne(t)
	if err := Send(addr, path); err != nil {
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
			err := Send("127.0.0.1:1", tt.path(t))
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

	err := Send(deadAddr(t), path)
	if err == nil {
		t.Fatal("Send() error = nil, want a dial error")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Errorf("Send() error = %q, want it to mention dial", err)
	}
}

func TestSend_ReturnsAnErrorForAnUnparseableAddress(t *testing.T) {
	path := tempFile(t, "a.txt", []byte("x"))

	if err := Send("definitely not an address", path); err == nil {
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

	if err := Send(ln.Addr().String(), path); err == nil {
		t.Error("Send() error = nil, want the broken connection to be reported")
	}
}

// ---------------------------------------------------------------------------
// 3. rate
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
