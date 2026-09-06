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
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

func wire(t *testing.T, name string, size int64, payload []byte) []byte {
	t.Helper()

	b, err := encode(name, size, payload)
	if err != nil {
		t.Fatalf("WriteHeader(%q, %d) error = %v, want nil", name, size, err)
	}
	return b
}

func encode(name string, size int64, payload []byte) ([]byte, error) {
	return chunk(name, size, 0, size, payload)
}

func chunk(name string, total, offset, length int64, payload []byte) ([]byte, error) {
	var b bytes.Buffer
	if err := protocol.WriteHeader(&b, protocol.Header{Name: name, TotalSize: total, Offset: offset, Length: length}); err != nil {
		return nil, err
	}
	b.Write(payload)
	return b.Bytes(), nil
}

func chunkWire(t *testing.T, name string, total, offset, length int64, payload []byte) []byte {
	t.Helper()

	b, err := chunk(name, total, offset, length, payload)
	if err != nil {
		t.Fatalf("WriteHeader(%q, total=%d, offset=%d, length=%d) error = %v, want nil", name, total, offset, length, err)
	}
	return b
}

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

func deliver(t *testing.T, s *Server, wire []byte) error {
	t.Helper()

	client, srv := net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	_ = client.SetDeadline(deadline)
	_ = srv.SetDeadline(deadline)

	done := make(chan error, 1)
	go func() { done <- s.receiveChunk(srv) }()

	go func() {
		defer client.Close()
		_, _ = client.Write(wire)
	}()

	err := <-done
	srv.Close()
	return err
}

func newServer(t *testing.T) *Server {
	t.Helper()
	return &Server{OutDir: t.TempDir()}
}

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

func TestReceive_WritesTheFileAndLeavesNoPartFileBehind(t *testing.T) {
	s := newServer(t)
	const payload = "hello there"

	if err := deliver(t, s, wire(t, "notes.txt", int64(len(payload)), []byte(payload))); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "notes.txt", payload)
	wantOnly(t, s.OutDir, "notes.txt")
}

func TestReceive_AcceptsAnEmptyFile(t *testing.T) {
	s := newServer(t)

	if err := deliver(t, s, wire(t, "empty.txt", 0, nil)); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "empty.txt", "")
	wantOnly(t, s.OutDir, "empty.txt")
}

func TestReceive_WritesALargePayloadIntact(t *testing.T) {
	s := newServer(t)
	payload := bytes.Repeat([]byte("abcdefgh"), 40*1024)

	if err := deliver(t, s, wire(t, "big.bin", int64(len(payload)), payload)); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
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
				t.Fatalf("receiveChunk() error = %v, want nil", err)
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
				t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, protocol.ErrBadName)
			}
			if got := entries(t, s.OutDir); len(got) != 0 {
				t.Errorf("output dir holds %v, want it empty", got)
			}
		})
	}
}

func TestReceive_StopsAtTheDeclaredSize(t *testing.T) {
	s := newServer(t)

	w := wire(t, "a.txt", 5, []byte("hello"))
	w = append(w, []byte("AND MUCH MORE THAT WAS NEVER PROMISED")...)

	if err := deliver(t, s, w); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "a.txt", "hello")
}

func TestReceive_RejectsATruncatedPayload(t *testing.T) {
	s := newServer(t)

	err := deliver(t, s, wire(t, "a.txt", 20, []byte("short")))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}
	if !strings.Contains(err.Error(), "copied 5 of 20 bytes") {
		t.Errorf("receiveChunk() error = %q, want it to say how far it got", err)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

type deadlineErrConn struct {
	net.Conn
	err error
}

func (c deadlineErrConn) SetReadDeadline(time.Time) error { return c.err }

func TestReceive_FailsWhenTheHeaderDeadlineCannotBeSet(t *testing.T) {
	s := newServer(t)
	sentinel := errors.New("no deadlines here")

	err := s.receiveChunk(deadlineErrConn{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, sentinel)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_GivesUpOnAPeerThatSendsNothing(t *testing.T) {
	s := newServer(t)
	s.HeaderTimeout = 50 * time.Millisecond

	client, srv := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		srv.Close()
	})

	start := time.Now()
	err := s.receiveChunk(srv)
	elapsed := time.Since(start)

	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, os.ErrDeadlineExceeded)
	}
	if elapsed > 5*time.Second {
		t.Errorf("receiveChunk() took %v, want it to give up after HeaderTimeout", elapsed)
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_DoesNotApplyTheHeaderTimeoutToTheBody(t *testing.T) {
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

	if err := s.receiveChunk(srv); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "slow.txt", payload)
}

func TestReceive_RejectsAStreamThatIsNotGshift(t *testing.T) {
	s := newServer(t)

	err := deliver(t, s, []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	if !errors.Is(err, protocol.ErrBadMagic) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, protocol.ErrBadMagic)
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
		t.Fatal("receiveChunk() error = nil, want a truncated header to fail")
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_OverwritesAStalePartFile(t *testing.T) {
	s := newServer(t)

	stale := filepath.Join(s.OutDir, "a.txt.gshift-part")
	if err := os.WriteFile(stale, []byte("JUNK FROM A CRASHED TRANSFER"), 0o644); err != nil {
		t.Fatalf("seeding a stale part file: %v", err)
	}

	if err := deliver(t, s, wire(t, "a.txt", 2, []byte("ok"))); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
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
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "a.txt", "new")
	wantOnly(t, s.OutDir, "a.txt")
}

func TestReceive_FailsWhenTheNameCollidesWithADirectory(t *testing.T) {
	s := newServer(t)

	if err := os.Mkdir(filepath.Join(s.OutDir, "a.txt"), 0o755); err != nil {
		t.Fatalf("seeding the blocking directory: %v", err)
	}

	err := deliver(t, s, wire(t, "a.txt", 2, []byte("ok")))
	if err == nil {
		t.Fatal("receiveChunk() error = nil, want the rename to fail")
	}
	if !strings.Contains(err.Error(), "commit") {
		t.Errorf("receiveChunk() error = %q, want it to mention the commit step", err)
	}
	wantOnly(t, s.OutDir, "a.txt")
}

func TestReceive_FailsWhenTheOutputDirDoesNotExist(t *testing.T) {
	s := &Server{OutDir: filepath.Join(t.TempDir(), "does-not-exist")}

	err := deliver(t, s, wire(t, "a.txt", 1, []byte("x")))
	if err == nil {
		t.Fatal("receiveChunk() error = nil, want a create failure")
	}
	if !strings.Contains(err.Error(), "create") {
		t.Errorf("receiveChunk() error = %q, want it to mention the create step", err)
	}
}

func TestReceive_CommitsOnlyOnceEveryChunkHasArrived(t *testing.T) {
	s := newServer(t)
	const total = 10

	first := chunkWire(t, "split.bin", total, 0, 4, []byte("ABCD"))
	second := chunkWire(t, "split.bin", total, 4, 6, []byte("EFGHIJ"))

	for i, err := range deliverChunks(t, s, [][]byte{first, second}) {
		if err != nil {
			t.Fatalf("chunk %d: receiveChunk() error = %v, want nil", i, err)
		}
	}

	wantFile(t, s.OutDir, "split.bin", "ABCDEFGHIJ")
	wantOnly(t, s.OutDir, "split.bin")
}

func TestReceive_HandlesManyConcurrentChunksOfTheSameFileWithoutCorruption(t *testing.T) {
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
			t.Fatalf("chunk %d: receiveChunk() error = %v, want nil", i, err)
		}
	}

	wantFile(t, s.OutDir, "big.bin", want.String())
	wantOnly(t, s.OutDir, "big.bin")
}

func TestReceive_OneFailingChunkAbortsTheWholeFileEvenIfOthersSucceeded(t *testing.T) {
	s := newServer(t)
	const total = 10

	good := chunkWire(t, "abort.bin", total, 0, 4, []byte("ABCD"))
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
	s := newServer(t)
	const total = 10

	bad := chunkWire(t, "poisoned.bin", total, 0, 6, []byte("AB"))
	if err := deliver(t, s, bad); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}

	late := chunkWire(t, "poisoned.bin", total, 6, 4, []byte("CDEF"))
	if err := deliver(t, s, late); err == nil {
		t.Fatal("receiveChunk() error = nil, want the poisoned name to be rejected")
	}

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty", got)
	}
}

func TestReceive_ASuccessfulTransferDoesNotPoisonTheNameForLaterUse(t *testing.T) {
	s := newServer(t)

	if err := deliver(t, s, wire(t, "reuse.txt", 5, []byte("first"))); err != nil {
		t.Fatalf("first receiveChunk() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "reuse.txt", "first")

	if err := deliver(t, s, wire(t, "reuse.txt", 6, []byte("second"))); err != nil {
		t.Fatalf("second receiveChunk() error = %v, want nil", err)
	}
	wantFile(t, s.OutDir, "reuse.txt", "second")
}

func TestReceive_RejectsChunksThatOverlapAndAbortsTheTransfer(t *testing.T) {
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
	s := newServer(t)

	first := chunkWire(t, "disagree.bin", 10, 0, 4, []byte("ABCD"))
	if err := deliver(t, s, first); err != nil {
		t.Fatalf("first receiveChunk() error = %v, want nil", err)
	}

	odd := chunkWire(t, "disagree.bin", 12, 4, 8, []byte("EFGHIJKL"))
	err := deliver(t, s, odd)
	if err == nil {
		t.Fatal("receiveChunk() error = nil, want the mismatched total to be rejected")
	}
	if !strings.Contains(err.Error(), "total") {
		t.Errorf("receiveChunk() error = %q, want it to mention the totals", err)
	}

	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want it empty after an aborted transfer", got)
	}
}

func TestReceive_ForgetsAFailedTransferOnceItsTTLHasPassed(t *testing.T) {
	s := newServer(t)
	s.TransferTTL = 100 * time.Millisecond

	bad := chunkWire(t, "retry.bin", 10, 0, 6, []byte("AB"))
	if err := deliver(t, s, bad); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("receiveChunk() error = %v, want it to wrap %v", err, io.ErrUnexpectedEOF)
	}

	time.Sleep(3 * s.TransferTTL)

	if err := deliver(t, s, wire(t, "retry.bin", 5, []byte("again"))); err != nil {
		t.Fatalf("receiveChunk() after the ttl error = %v, want nil", err)
	}

	wantFile(t, s.OutDir, "retry.bin", "again")
	wantOnly(t, s.OutDir, "retry.bin")
}

func TestSweep_DropsAStalledTransferAndItsPartFile(t *testing.T) {
	s := newServer(t)
	s.TransferTTL = 100 * time.Millisecond

	half := chunkWire(t, "stalled.bin", 10, 0, 4, []byte("ABCD"))
	if err := deliver(t, s, half); err != nil {
		t.Fatalf("receiveChunk() error = %v, want nil", err)
	}
	if got := entries(t, s.OutDir); len(got) != 1 {
		t.Fatalf("output dir holds %v, want the part file while the transfer is under way", got)
	}

	time.Sleep(3 * s.TransferTTL)
	s.sweep(time.Now())

	if _, ok := s.assemblies.Load("stalled.bin"); ok {
		t.Error("the stalled transfer is still tracked, want it swept")
	}
	if got := entries(t, s.OutDir); len(got) != 0 {
		t.Errorf("output dir holds %v, want the part file gone with it", got)
	}
}

func wantRejected(t *testing.T, errs []error, reason string) {
	t.Helper()

	rejected := false
	for _, err := range errs {
		if err == nil {
			continue
		}
		rejected = true
		if !strings.Contains(err.Error(), reason) {
			t.Errorf("receiveChunk() error = %q, want it to mention %q", err, reason)
		}
	}
	if !rejected {
		t.Fatalf("want a chunk to be rejected with %q, all of them succeeded", reason)
	}
}

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
	s := newServer(t)
	addr := startServer(t, s)

	for _, name := range []string{"one.txt", "two.txt", "three.txt"} {
		send(t, addr, name, []byte(name))
		waitForFile(t, s.OutDir, name)
		wantFile(t, s.OutDir, name, name)
	}
}

func TestServe_HandlesConcurrentConnections(t *testing.T) {
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
	_, _ = conn.Write(wire(t, "half.txt", 100, []byte("abcd")))
	conn.Close()

	send(t, addr, "after.txt", []byte("still up"))
	waitForFile(t, s.OutDir, "after.txt")

	wantOnly(t, s.OutDir, "after.txt")
}

func TestServe_CreatesTheOutputDir(t *testing.T) {
	s := &Server{OutDir: filepath.Join(t.TempDir(), "nested", "incoming")}
	addr := startServer(t, s)

	send(t, addr, "a.txt", []byte("x"))
	waitForFile(t, s.OutDir, "a.txt")
}

func TestServe_FailsWhenTheOutputDirCannotBeCreated(t *testing.T) {
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
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seeding the blocking file: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v, want nil", err)
	}
	addr := ln.Addr().String()

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
	dir := filepath.Join(t.TempDir(), "incoming")
	s := &Server{Addr: "127.0.0.1:99999", OutDir: dir}

	if err := s.ListenAndServe(); err == nil {
		t.Fatal("ListenAndServe() error = nil, want a listen failure")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat(%q) error = %v, want the dir to not exist", dir, err)
	}
}
