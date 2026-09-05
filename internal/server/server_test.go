// these tests say what the receiving side must do.
//
// order:
//
//  1. helpers          fixtures for the tests
//  2. receive          one file, start to finish
//  3. Serve            the accept loop
//  4. ListenAndServe   address handling
package server

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

func TestMain(m *testing.M) {
	// the server logs every transfer. turn it off so test output stays readable.
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// 1. helpers
// ---------------------------------------------------------------------------

// wire builds the bytes a sender puts on the connection for a file sent whole,
// in a single chunk. size goes into the header as given, so it can differ
// from len(payload) on purpose.
func wire(t *testing.T, name string, size int64, payload []byte) []byte {
	t.Helper()

	b, err := encode(name, size, payload)
	if err != nil {
		t.Fatalf("WriteHeader(%q, %d) error = %v, want nil", name, size, err)
	}
	return b
}

// encode is wire without a *testing.T, for callers that must not call FailNow.
func encode(name string, size int64, payload []byte) ([]byte, error) {
	return chunk(name, size, 0, size, payload)
}

// chunk builds the bytes for one chunk of a (possibly multi connection)
// transfer: name and total are the same on every chunk of one file, offset
// and length describe the slice this chunk carries.
func chunk(name string, total, offset, length int64, payload []byte) ([]byte, error) {
	var b bytes.Buffer
	if err := protocol.WriteHeader(&b, protocol.Header{Name: name, TotalSize: total, Offset: offset, Length: length}); err != nil {
		return nil, err
	}
	b.Write(payload)
	return b.Bytes(), nil
}

// chunkWire is chunk, failing the test instead of returning an error.
func chunkWire(t *testing.T, name string, total, offset, length int64, payload []byte) []byte {
	t.Helper()

	b, err := chunk(name, total, offset, length, payload)
	if err != nil {
		t.Fatalf("WriteHeader(%q, total=%d, offset=%d, length=%d) error = %v, want nil", name, total, offset, length, err)
	}
	return b
}

// deliverChunks runs receive concurrently, once per entry in wires, against
// the same server, and returns every call's error in the same order. Use it
// to drive several connections that carry chunks of the same file.
func deliverChunks(t *testing.T, s *Server, wires [][]byte) []error {
	t.Helper()

	errs := make([]error, len(wires))
	var wg sync.WaitGroup
	for i, w := range wires {
		wg.Add(1)
		go func(i int, w []byte) {
			defer wg.Done()
			errs[i] = deliver(t, s, w)
		}(i, w)
	}
	wg.Wait()
	return errs
}

// deliver runs one receive over an in memory connection and returns its error.
// the write needs its own goroutine because net.Pipe has no buffer. if receive
// gives up early nobody is reading, so the deadline stops the write hanging.
func deliver(t *testing.T, s *Server, wire []byte) error {
	t.Helper()

	client, srv := net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	_ = client.SetDeadline(deadline)
	_ = srv.SetDeadline(deadline)

	done := make(chan error, 1)
	go func() { done <- s.receive(srv) }()

	go func() {
		defer client.Close()
		_, _ = client.Write(wire)
	}()

	err := <-done
	srv.Close()
	return err
}

// newServer returns a Server that writes into a fresh temp dir.
func newServer(t *testing.T) *Server {
	t.Helper()
	return &Server{OutDir: t.TempDir()}
}

// entries lists the file names in dir so a test can check what was left behind.
func entries(t *testing.T, dir string) []string {
	t.Helper()

	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v, want nil", dir, err)
	}

	names := make([]string, 0, len(des))
	for _, de := range des {
		names = append(names, de.Name())
	}
	return names
}

// wantFile checks that dir/name exists and holds exactly this content.
func wantFile(t *testing.T, dir, name, content string) {
	t.Helper()

	got, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("reading %s: %v", name, err)
	}
	if string(got) != content {
		t.Errorf("%s = %q, want %q", name, got, content)
	}
}

// wantOnly checks the out dir holds these names and nothing else. this is how
// the tests prove no .gshift-part file was left.
func wantOnly(t *testing.T, dir string, names ...string) {
	t.Helper()

	got := entries(t, dir)
	if len(got) != len(names) {
		t.Fatalf("output dir holds %v, want exactly %v", got, names)
	}
	for _, want := range names {
		if !slices.Contains(got, want) {
			t.Errorf("output dir holds %v, want it to contain %q", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. receive
// ---------------------------------------------------------------------------

func TestReceive_WritesTheFileAndLeavesNoPartFileBehind(t *testing.T) {
	s := newServer(t)
	const payload = "hello there"

	if err := deliver(t, s, wire(t, "notes.txt", int64(len(payload)), []byte(payload))); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "notes.txt", payload)
	wantOnly(t, s.OutDir, "notes.txt")
}

func TestReceive_AcceptsAnEmptyFile(t *testing.T) {
	// SIZE 0 is a valid header, so we should get a real empty file, not nothing.
	s := newServer(t)

	if err := deliver(t, s, wire(t, "empty.txt", 0, nil)); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "empty.txt", "")
	wantOnly(t, s.OutDir, "empty.txt")
}

func TestReceive_WritesALargePayloadIntact(t *testing.T) {
	// bigger than the 32 KiB copy buffer, so the copy loop runs more than once.
	s := newServer(t)
	payload := bytes.Repeat([]byte("abcdefgh"), 40*1024) // 320 KiB

	if err := deliver(t, s, wire(t, "big.bin", int64(len(payload)), payload)); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	got, err := os.ReadFile(filepath.Join(s.OutDir, "big.bin"))
	if err != nil {
		t.Fatalf("reading big.bin: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("big.bin is %d bytes, want %d, and the contents differ", len(got), len(payload))
	}
}

func TestReceive_StripsDirectoriesFromTheName(t *testing.T) {
	// the security rule end to end. a name with .. in it still lands inside OutDir.
	tests := []struct {
		name     string
		sent     string
		wantFile string
	}{
		{name: "traversal", sent: "../../etc/passwd", wantFile: "passwd"},
		{name: "absolute path", sent: "/etc/shadow", wantFile: "shadow"},
		{name: "nested directories", sent: "a/b/c/deep.txt", wantFile: "deep.txt"},
		{name: "traversal hidden mid path", sent: "ok/../../../evil.sh", wantFile: "evil.sh"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer(t)
			const payload = "x"

			if err := deliver(t, s, wire(t, tt.sent, int64(len(payload)), []byte(payload))); err != nil {
				t.Fatalf("receive() error = %v, want nil", err)
			}

			wantFile(t, s.OutDir, tt.wantFile, payload)
			wantOnly(t, s.OutDir, tt.wantFile)
		})
	}
}

func TestReceive_RejectsANameThatIsNotAFile(t *testing.T) {
	tests := []struct {
		name string
		sent string
	}{
		{name: "dot dot", sent: ".."},
		{name: "dot", sent: "."},
		{name: "root", sent: "/"},
		{name: "NUL byte", sent: "evil\x00.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer(t)

			err := deliver(t, s, wire(t, tt.sent, 1, []byte("x")))
			if !errors.Is(err, protocol.ErrBadName) {
				t.Fatalf("receive() error = %v, want it to wrap %v", err, protocol.ErrBadName)
			}
			if got := entries(t, s.OutDir); len(got) != 0 {
				t.Errorf("output dir holds %v, want it empty", got)
			}
		})
	}
}

func TestReceive_StopsAtTheDeclaredSize(t *testing.T) {
	// bytes past SIZE must not be written. this is what stops a bad peer from
	// filling our disk.
	s := newServer(t)

	w := wire(t, "a.txt", 5, []byte("hello"))
	w = append(w, []byte("AND MUCH MORE THAT WAS NEVER PROMISED")...)

	if err := deliver(t, s, w); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "a.txt", "hello")
}

func TestReceive_RejectsATruncatedPayload(t *testing.T) {
	// the header promised 20 bytes and only 5 came. nothing should be saved and
	// no .part left behind.
	s := newServer(t)

	err := deliver(t, s, wire(t, "a.txt", 20, []byte("short")))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}
	if !strings.Contains(err.Error(), "copied 5 of 20 bytes") {
		t.Errorf("receive() error = %q, want it to say how far it got", err)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

// deadlineErrConn is a conn that refuses deadlines. the embedded nil Conn is
// fine because receive returns before it touches anything else.
type deadlineErrConn struct {
	net.Conn
	err error
}

func (c deadlineErrConn) SetReadDeadline(time.Time) error { return c.err }

func TestReceive_FailsWhenTheHeaderDeadlineCannotBeSet(t *testing.T) {
	// no deadline means an unbounded read, so refusing to go on is the point.
	s := newServer(t)
	sentinel := errors.New("no deadlines here")

	err := s.receive(deadlineErrConn{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, sentinel)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_GivesUpOnAPeerThatSendsNothing(t *testing.T) {
	// without a deadline a silent peer holds a goroutine forever, so a few
	// thousand of them is enough to kill the process.
	s := newServer(t)
	s.HeaderTimeout = 50 * time.Millisecond

	client, srv := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		srv.Close()
	})

	start := time.Now()
	err := s.receive(srv)
	elapsed := time.Since(start)

	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, os.ErrDeadlineExceeded)
	}
	if elapsed > 5*time.Second {
		t.Errorf("receive() took %v, want it to give up after HeaderTimeout", elapsed)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_DoesNotApplyTheHeaderTimeoutToTheBody(t *testing.T) {
	// a big transfer takes longer than any header ever should, so the deadline
	// has to be cleared once the header is in.
	s := newServer(t)
	s.HeaderTimeout = 50 * time.Millisecond

	const payload = "late"
	w := wire(t, "slow.txt", int64(len(payload)), nil)

	client, srv := net.Pipe()
	t.Cleanup(func() { srv.Close() })

	go func() {
		defer client.Close()
		_, _ = client.Write(w)
		time.Sleep(4 * s.HeaderTimeout)
		_, _ = client.Write([]byte(payload))
	}()

	if err := s.receive(srv); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "slow.txt", payload)
}

func TestReceive_RejectsAStreamThatIsNotGshift(t *testing.T) {
	s := newServer(t)

	err := deliver(t, s, []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	if !errors.Is(err, protocol.ErrBadMagic) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, protocol.ErrBadMagic)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_RejectsAHeaderThatEndsEarly(t *testing.T) {
	s := newServer(t)

	full := wire(t, "a.txt", 1, []byte("x"))

	err := deliver(t, s, full[:4])
	if err == nil {
		t.Fatal("receive() error = nil, want a truncated header to fail")
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_OverwritesAStalePartFile(t *testing.T) {
	// a crashed run leaves a .part behind. the next try reuses it and O_TRUNC
	// means none of the old bytes stay.
	s := newServer(t)

	stale := filepath.Join(s.OutDir, "a.txt.gshift-part")
	if err := os.WriteFile(stale, []byte("JUNK FROM A CRASHED TRANSFER"), 0o644); err != nil {
		t.Fatalf("seeding a stale part file: %v", err)
	}

	if err := deliver(t, s, wire(t, "a.txt", 2, []byte("ok"))); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "a.txt", "ok")
	wantOnly(t, s.OutDir, "a.txt")
}

func TestReceive_ReplacesAFileThatAlreadyExists(t *testing.T) {
	s := newServer(t)

	existing := filepath.Join(s.OutDir, "a.txt")
	if err := os.WriteFile(existing, []byte("OLD CONTENT, LONGER THAN THE NEW"), 0o644); err != nil {
		t.Fatalf("seeding an existing file: %v", err)
	}

	if err := deliver(t, s, wire(t, "a.txt", 3, []byte("new"))); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "a.txt", "new")
	wantOnly(t, s.OutDir, "a.txt")
}

func TestReceive_FailsWhenTheNameCollidesWithADirectory(t *testing.T) {
	// the bytes arrive fine but the rename cannot work. nothing is saved and the
	// .part file is still cleaned up.
	s := newServer(t)

	if err := os.Mkdir(filepath.Join(s.OutDir, "a.txt"), 0o755); err != nil {
		t.Fatalf("seeding the blocking directory: %v", err)
	}

	err := deliver(t, s, wire(t, "a.txt", 2, []byte("ok")))
	if err == nil {
		t.Fatal("receive() error = nil, want the rename to fail")
	}
	if !strings.Contains(err.Error(), "commit") {
		t.Errorf("receive() error = %q, want it to mention the commit step", err)
	}
	wantOnly(t, s.OutDir, "a.txt") // just the directory, no leftover .part
}

func TestReceive_FailsWhenTheOutputDirDoesNotExist(t *testing.T) {
	// receive does not make OutDir, Serve does. point it at a missing dir and the
	// create error should come back.
	s := &Server{OutDir: filepath.Join(t.TempDir(), "does-not-exist")}

	err := deliver(t, s, wire(t, "a.txt", 1, []byte("x")))
	if err == nil {
		t.Fatal("receive() error = nil, want a create failure")
	}
	if !strings.Contains(err.Error(), "create") {
		t.Errorf("receive() error = %q, want it to mention the create step", err)
	}
}

// ---------------------------------------------------------------------------
// 2b. chunked transfers: several connections carrying one file
// ---------------------------------------------------------------------------

func TestReceive_CommitsOnlyOnceEveryChunkHasArrived(t *testing.T) {
	s := newServer(t)
	const total = 10

	first := chunkWire(t, "split.bin", total, 0, 4, []byte("ABCD"))
	second := chunkWire(t, "split.bin", total, 4, 6, []byte("EFGHIJ"))

	for i, err := range deliverChunks(t, s, [][]byte{first, second}) {
		if err != nil {
			t.Fatalf("chunk %d: receive() error = %v, want nil", i, err)
		}
	}

	wantFile(t, s.OutDir, "split.bin", "ABCDEFGHIJ")
	wantOnly(t, s.OutDir, "split.bin")
}

func TestReceive_HandlesManyConcurrentChunksOfTheSameFileWithoutCorruption(t *testing.T) {
	// each chunk writes to a different slice of the same shared file
	// concurrently. run this under -race.
	s := newServer(t)

	const chunkSize = 4096
	const numChunks = 8

	var want bytes.Buffer
	wires := make([][]byte, numChunks)
	for i := range numChunks {
		part := bytes.Repeat([]byte{byte('A' + i)}, chunkSize)
		want.Write(part)
		wires[i] = chunkWire(t, "big.bin", chunkSize*numChunks, int64(i*chunkSize), chunkSize, part)
	}

	for i, err := range deliverChunks(t, s, wires) {
		if err != nil {
			t.Fatalf("chunk %d: receive() error = %v, want nil", i, err)
		}
	}

	wantFile(t, s.OutDir, "big.bin", want.String())
	wantOnly(t, s.OutDir, "big.bin")
}

func TestReceive_OneFailingChunkAbortsTheWholeFileEvenIfOthersSucceeded(t *testing.T) {
	// "bad" always independently reports io.ErrUnexpectedEOF, since it is the
	// one violating its own declared length; "good" may additionally report
	// an error too, if it happens to run after "bad" has already poisoned the
	// name (see TestReceive_RejectsAChunkForANameWhoseEarlierTransferFailed).
	// Either way, nothing is ever left on disk.
	s := newServer(t)
	const total = 10

	good := chunkWire(t, "abort.bin", total, 0, 4, []byte("ABCD"))
	// promises 6 bytes at offset 4, but only 2 are ever put on the wire
	bad := chunkWire(t, "abort.bin", total, 4, 6, []byte("EF"))

	errs := deliverChunks(t, s, [][]byte{good, bad})

	sawTruncation := false
	for _, err := range errs {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			sawTruncation = true
		}
	}
	if !sawTruncation {
		t.Fatal("want the truncated chunk to be reported with io.ErrUnexpectedEOF")
	}

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty: no partial file under any name, even though the other chunk may have finished", got)
	}
}

func TestReceive_RejectsAChunkForANameWhoseEarlierTransferFailed(t *testing.T) {
	// once a transfer under a name has failed, the name stays rejected rather
	// than silently letting a late chunk start a fresh, doomed transfer under
	// the same temp file (which would otherwise leak a .gshift-part forever).
	s := newServer(t)
	const total = 10

	bad := chunkWire(t, "poisoned.bin", total, 0, 6, []byte("AB"))
	if err := deliver(t, s, bad); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}

	late := chunkWire(t, "poisoned.bin", total, 6, 4, []byte("CDEF"))
	if err := deliver(t, s, late); err == nil {
		t.Fatal("receive() error = nil, want the poisoned name to be rejected")
	}

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_ASuccessfulTransferDoesNotPoisonTheNameForLaterUse(t *testing.T) {
	s := newServer(t)

	if err := deliver(t, s, wire(t, "reuse.txt", 5, []byte("first"))); err != nil {
		t.Fatalf("first receive() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "reuse.txt", "first")

	if err := deliver(t, s, wire(t, "reuse.txt", 6, []byte("second"))); err != nil {
		t.Fatalf("second receive() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "reuse.txt", "second")
}

func TestReceive_RejectsChunksThatOverlapAndAbortsTheTransfer(t *testing.T) {
	// two chunks each covering the same range of the file: whichever one's
	// bytes land second is claiming a range that has already arrived.
	s := newServer(t)
	const total = 10

	first := chunkWire(t, "overlap.bin", total, 0, 6, []byte("ABCDEF"))
	dup := chunkWire(t, "overlap.bin", total, 0, 6, []byte("ABCDEF"))

	errs := deliverChunks(t, s, [][]byte{first, dup})

	wantRejected(t, errs, "overlaps")

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty after an aborted transfer", got)
	}
}

func TestReceive_RejectsOverlappingChunksEvenWhenTheirLengthsAddUpToTheTotal(t *testing.T) {
	// [0,6) and [2,6) are 10 bytes between them, the same as the total, but
	// they cover only the first 6 bytes of the file. counting bytes instead of
	// ranges would call this done and commit a file with a hole in it.
	s := newServer(t)
	const total = 10

	first := chunkWire(t, "gap.bin", total, 0, 6, []byte("ABCDEF"))
	second := chunkWire(t, "gap.bin", total, 2, 4, []byte("CDEF"))

	errs := deliverChunks(t, s, [][]byte{first, second})

	wantRejected(t, errs, "overlaps")

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty: the file was never fully covered", got)
	}
}

func TestReceive_RejectsAChunkThatDisagreesAboutTheTotalSize(t *testing.T) {
	// the two senders do not have the same file in mind, so neither of them
	// can be trusted with it.
	s := newServer(t)

	first := chunkWire(t, "disagree.bin", 10, 0, 4, []byte("ABCD"))
	if err := deliver(t, s, first); err != nil {
		t.Fatalf("first receive() error = %v, want nil", err)
	}

	odd := chunkWire(t, "disagree.bin", 12, 4, 8, []byte("EFGHIJKL"))
	err := deliver(t, s, odd)
	if err == nil {
		t.Fatal("receive() error = nil, want the mismatched total to be rejected")
	}
	if !strings.Contains(err.Error(), "total") {
		t.Errorf("receive() error = %q, want it to mention the totals", err)
	}

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty after an aborted transfer", got)
	}
}

func TestReceive_ForgetsAFailedTransferOnceItsTTLHasPassed(t *testing.T) {
	// a failed name is rejected for a while, not forever: otherwise one bad
	// chunk per name would both poison every name and grow the map without
	// bound.
	s := newServer(t)
	s.TransferTTL = 100 * time.Millisecond

	bad := chunkWire(t, "retry.bin", 10, 0, 6, []byte("AB"))
	if err := deliver(t, s, bad); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receive() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}

	time.Sleep(3 * s.TransferTTL)

	if err := deliver(t, s, wire(t, "retry.bin", 5, []byte("again"))); err != nil {
		t.Fatalf("receive() after the ttl error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "retry.bin", "again")
	wantOnly(t, s.OutDir, "retry.bin")
}

func TestSweep_DropsAStalledTransferAndItsPartFile(t *testing.T) {
	// a sender that opens a transfer and then goes quiet must not hold an
	// entry, an open file and a .gshift-part forever.
	s := newServer(t)
	s.TransferTTL = 100 * time.Millisecond

	half := chunkWire(t, "stalled.bin", 10, 0, 4, []byte("ABCD"))
	if err := deliver(t, s, half); err != nil {
		t.Fatalf("receive() error = %v, want nil", err)
	}
	if got := entries(t, s.OutDir); len(got) != 1 {
		t.Fatalf("output dir holds %v, want the part file while the transfer is under way", got)
	}

	time.Sleep(3 * s.TransferTTL)
	s.sweep(time.Now())

	if _, ok := s.chunks.Load("stalled.bin"); ok {
		t.Error("the stalled transfer is still tracked, want it swept")
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want the part file gone with it", got)
	}
}

// wantRejected checks that at least one of the chunks was turned away, and that
// every error blames the same thing.
func wantRejected(t *testing.T, errs []error, reason string) {
	t.Helper()

	rejected := false
	for _, err := range errs {
		if err == nil {
			continue
		}
		rejected = true
		if !strings.Contains(err.Error(), reason) {
			t.Errorf("receive() error = %q, want it to mention %q", err, reason)
		}
	}
	if !rejected {
		t.Fatalf("want a chunk to be rejected with %q, all of them succeeded", reason)
	}
}

// ---------------------------------------------------------------------------
// 3. Serve
// ---------------------------------------------------------------------------

// startServer runs Serve on a local listener and returns its address. closing
// the listener at the end of the test is what makes Serve return.
func startServer(t *testing.T, s *Server) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()

	t.Cleanup(func() {
		ln.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Serve() did not return within 5s of the listener closing")
		}
	})

	return ln.Addr().String()
}

// send does one client side transfer to addr.
// t.Errorf not t.Fatalf, the concurrent test calls this from goroutines.
func send(t *testing.T, addr, name string, payload []byte) {
	t.Helper()

	b, err := encode(name, int64(len(payload)), payload)
	if err != nil {
		t.Errorf("WriteHeader(%q, %d) error = %v, want nil", name, len(payload), err)
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Errorf("Dial(%s) error = %v, want nil", addr, err)
		return
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(b); err != nil {
		t.Errorf("writing to %s: %v", addr, err)
	}
}

// waitForFile waits for dir/name to show up. Serve handles each connection in
// its own goroutine, so the file lands a moment after the client is done.
func waitForFile(t *testing.T, dir, name string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never appeared in %s, dir holds %v", name, dir, entries(t, dir))
}

func TestServe_ReceivesAFileOverTheNetwork(t *testing.T) {
	s := newServer(t)
	addr := startServer(t, s)

	send(t, addr, "notes.txt", []byte("hello there"))
	waitForFile(t, s.OutDir, "notes.txt")

	wantFile(t, s.OutDir, "notes.txt", "hello there")
}

func TestServe_HandlesSeveralConnectionsInSequence(t *testing.T) {
	// the accept loop must keep going after a transfer finishes.
	s := newServer(t)
	addr := startServer(t, s)

	for _, name := range []string{"one.txt", "two.txt", "three.txt"} {
		send(t, addr, name, []byte(name))
		waitForFile(t, s.OutDir, name)
		wantFile(t, s.OutDir, name, name)
	}
}

func TestServe_HandlesConcurrentConnections(t *testing.T) {
	// one goroutine per connection, so they must not mix up each other's files.
	// run this under -race.
	s := newServer(t)
	addr := startServer(t, s)

	names := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "f.txt", "g.txt", "h.txt"}
	payload := func(n string) []byte { return bytes.Repeat([]byte(n), 4096) }

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Go(func() { send(t, addr, name, payload(name)) })
	}
	wg.Wait()

	for _, name := range names {
		waitForFile(t, s.OutDir, name)
		wantFile(t, s.OutDir, name, string(payload(name)))
	}
}

func TestServe_SurvivesAConnectionThatSendsGarbage(t *testing.T) {
	// a bad peer only loses its own transfer. the loop stays up and the next
	// client still works.
	s := newServer(t)
	addr := startServer(t, s)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Dial() error = %v, want nil", err)
	}
	_, _ = conn.Write([]byte("not a gshift stream"))
	conn.Close()

	send(t, addr, "after.txt", []byte("still up"))
	waitForFile(t, s.OutDir, "after.txt")
	wantFile(t, s.OutDir, "after.txt", "still up")
}

func TestServe_SurvivesAConnectionThatHangsUpMidTransfer(t *testing.T) {
	s := newServer(t)
	addr := startServer(t, s)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Dial() error = %v, want nil", err)
	}
	// promise 100 bytes, send 4, then vanish.
	_, _ = conn.Write(wire(t, "half.txt", 100, []byte("abcd")))
	conn.Close()

	send(t, addr, "after.txt", []byte("still up"))
	waitForFile(t, s.OutDir, "after.txt")

	// the dead transfer leaves nothing behind, not even a .part file.
	wantOnly(t, s.OutDir, "after.txt")
}

func TestServe_CreatesTheOutputDir(t *testing.T) {
	s := &Server{OutDir: filepath.Join(t.TempDir(), "nested", "incoming")}
	addr := startServer(t, s)

	send(t, addr, "a.txt", []byte("x"))
	waitForFile(t, s.OutDir, "a.txt")
}

func TestServe_FailsWhenTheOutputDirCannotBeCreated(t *testing.T) {
	// a file sits where a dir should be. MkdirAll cannot fix that, so Serve fails
	// before it accepts anything.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seeding the blocking file: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}

	s := &Server{OutDir: filepath.Join(blocker, "incoming")}
	err = s.Serve(ln)
	if err == nil {
		t.Fatal("Serve() error = nil, want a failure preparing the output dir")
	}
	if !strings.Contains(err.Error(), "output dir") {
		t.Errorf("Serve() error = %q, want it to mention the output dir", err)
	}
}

func TestServe_ReturnsWhenTheListenerCloses(t *testing.T) {
	s := newServer(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()

	// let Serve reach Accept, then take the listener away.
	time.Sleep(50 * time.Millisecond)
	ln.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve() error = nil, want the accept failure")
		}
		if !strings.Contains(err.Error(), "accept") {
			t.Errorf("Serve() error = %q, want it to mention accept", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return within 5s of the listener closing")
	}
}

func TestServe_ClosesTheListenerOnTheWayOut(t *testing.T) {
	// Serve owns the listener it is given, so the same port must be free again
	// once it returns.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seeding the blocking file: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	addr := ln.Addr().String()

	// this fails right away on the out dir, but it still has to close ln.
	s := &Server{OutDir: filepath.Join(blocker, "incoming")}
	if err := s.Serve(ln); err == nil {
		t.Fatal("Serve() error = nil, want a failure preparing the output dir")
	}

	reopened, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("rebinding %s error = %v, want nil, so Serve left the listener open", addr, err)
	}
	reopened.Close()
}

// ---------------------------------------------------------------------------
// 4. ListenAndServe
// ---------------------------------------------------------------------------

func TestListenAndServe_ReportsAnUnusableAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{name: "port out of range", addr: "127.0.0.1:99999"},
		{name: "not an address", addr: "definitely not an address"},
		{name: "unassignable ip", addr: "192.0.2.1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{Addr: tt.addr, OutDir: t.TempDir()}

			err := s.ListenAndServe()
			if err == nil {
				t.Fatalf("ListenAndServe() error = nil, want a listen failure for %q", tt.addr)
			}
			if !strings.Contains(err.Error(), "listen") {
				t.Errorf("ListenAndServe() error = %q, want it to mention listen", err)
			}
			if !strings.Contains(err.Error(), tt.addr) {
				t.Errorf("ListenAndServe() error = %q, want it to name the address %q", err, tt.addr)
			}
		})
	}
}

func TestListenAndServe_BindsThenHandsOffToServe(t *testing.T) {
	// port 0 lets the kernel pick a free port, so this binds for real. a bad
	// OutDir then makes Serve return at once, which shows we got past listen and
	// passed Serve's error back.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seeding the blocking file: %v", err)
	}

	s := &Server{Addr: "127.0.0.1:0", OutDir: filepath.Join(blocker, "incoming")}

	err := s.ListenAndServe()
	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want Serve's output dir failure")
	}
	if !strings.Contains(err.Error(), "output dir") {
		t.Errorf("ListenAndServe() error = %q, want Serve's error, not a listen error", err)
	}
}

func TestListenAndServe_DoesNotCreateTheOutputDirWhenListenFails(t *testing.T) {
	// listen runs first, so a bad address should not leave a dir behind.
	dir := filepath.Join(t.TempDir(), "incoming")
	s := &Server{Addr: "127.0.0.1:99999", OutDir: dir}

	if err := s.ListenAndServe(); err == nil {
		t.Fatal("ListenAndServe() error = nil, want a listen failure")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat(%q) error = %v, want the dir to not exist", dir, err)
	}
}
