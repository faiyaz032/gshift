// Package server receives gshift transfers and writes them to disk.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

const defaultHeaderTimeout = 30 * time.Second

// how long a failed or stalled transfer is remembered, both to keep its name
// rejected and to keep the entry around at all.
const defaultTransferTTL = 5 * time.Minute

type Server struct {
	Addr string

	OutDir string

	HeaderTimeout time.Duration

	// TransferTTL is how long a failed or stalled transfer is remembered before
	// it is swept and its name is free again. zero means defaultTransferTTL.
	TransferTTL time.Duration

	chunks sync.Map

	// unix nanos of the earliest next sweep
	sweepAt atomic.Int64
}

// span is a half open byte range [start, end) of the file that has arrived.
type span struct {
	start, end int64
}

type inflight struct {
	mu       sync.Mutex
	file     *os.File
	tmp, dst string
	total    int64

	// sorted, disjoint, and never touching, so covering the whole file leaves
	// exactly one span
	spans []span

	// chunks still writing. an entry is only swept once this is zero.
	active  int
	updated time.Time
	closed  bool
}

// open returns the entry for name, creating and preallocating the part file on
// the first chunk. the caller must release the entry once its chunk is done.
func (s *Server) open(name, dst, tmp string, total int64) (*inflight, error) {
	actual, _ := s.chunks.LoadOrStore(name, &inflight{})
	entry := actual.(*inflight)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.closed {
		return nil, fmt.Errorf("server: %s: an earlier transfer under this name already failed", name)
	}

	switch {
	case entry.file == nil:
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, fmt.Errorf("server: create %s: %w", tmp, err)
		}
		if err := f.Truncate(total); err != nil {
			f.Close()
			os.Remove(tmp)
			return nil, fmt.Errorf("server: preallocate %s: %w", tmp, err)
		}
		entry.file = f
		entry.tmp = tmp
		entry.dst = dst
		entry.total = total

	case entry.total != total:
		// the senders disagree about the file, so neither of them can be
		// trusted with it.
		entry.abortLocked()
		return nil, fmt.Errorf("server: %s: chunk declares a total of %d bytes, the transfer under way declares %d",
			name, total, entry.total)
	}

	entry.active++
	entry.updated = time.Now()

	return entry, nil
}

// release marks this connection's chunk finished, however it went.
func (e *inflight) release() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.active--
	e.updated = time.Now()
}

// addSpan records that [offset, offset+n) has been written. it reports an error
// if that range overlaps one already recorded, and done once the recorded spans
// cover the whole file. counting bytes alone would not do: two overlapping
// chunks can add up to the total while leaving a hole.
func (e *inflight) addSpan(offset, n int64) (done bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := span{start: offset, end: offset + n}

	i := sort.Search(len(e.spans), func(i int) bool { return e.spans[i].start >= s.start })
	if i > 0 && e.spans[i-1].end > s.start {
		return false, overlap(s, e.spans[i-1])
	}
	if i < len(e.spans) && e.spans[i].start < s.end {
		return false, overlap(s, e.spans[i])
	}

	// insert in order, then close up the seam with either neighbour
	e.spans = slices.Insert(e.spans, i, s)
	if i+1 < len(e.spans) && e.spans[i].end == e.spans[i+1].start {
		e.spans[i].end = e.spans[i+1].end
		e.spans = slices.Delete(e.spans, i+1, i+2)
	}
	if i > 0 && e.spans[i-1].end == e.spans[i].start {
		e.spans[i-1].end = e.spans[i].end
		e.spans = slices.Delete(e.spans, i, i+1)
	}

	covered := e.spans[0]
	return len(e.spans) == 1 && covered.start == 0 && covered.end == e.total, nil
}

func overlap(got, have span) error {
	return fmt.Errorf("chunk [%d,%d) overlaps [%d,%d), which already arrived",
		got.start, got.end, have.start, have.end)
}

// stale reports whether the entry has been idle since cutoff with no chunk
// still writing, and gives up on it if so.
func (e *inflight) stale(cutoff time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.active > 0 || e.updated.After(cutoff) {
		return false
	}
	e.abortLocked()

	return true
}

func (e *inflight) abort() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.abortLocked()
}

func (e *inflight) abortLocked() {
	if e.closed {
		return
	}
	e.closed = true
	if e.file != nil {
		e.file.Close()
		os.Remove(e.tmp)
	}
}

func (e *inflight) commit() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("server: %s: transfer already finished", e.dst)
	}
	e.closed = true
	f, tmp, dst := e.file, e.tmp, e.dst
	e.mu.Unlock()

	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("server: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("server: commit %s: %w", dst, err)
	}
	return nil
}

// sweep drops entries for transfers that failed or stalled more than a ttl ago,
// so one bad chunk cannot poison a name forever and the map cannot grow without
// bound. it does the walk at most once per ttl.
func (s *Server) sweep(now time.Time) {
	ttl := s.transferTTL()

	next := s.sweepAt.Load()
	if now.UnixNano() < next || !s.sweepAt.CompareAndSwap(next, now.Add(ttl).UnixNano()) {
		return
	}

	cutoff := now.Add(-ttl)
	s.chunks.Range(func(key, value any) bool {
		if entry := value.(*inflight); entry.stale(cutoff) {
			s.chunks.Delete(key)
		}
		return true
	})
}

func (s *Server) headerTimeout() time.Duration {
	if s.HeaderTimeout > 0 {
		return s.HeaderTimeout
	}
	return defaultHeaderTimeout
}

func (s *Server) transferTTL() time.Duration {
	if s.TransferTTL > 0 {
		return s.TransferTTL
	}
	return defaultTransferTTL
}

// ListenAndServe binds Addr and serves until the listener fails.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.Addr, err)
	}
	return s.Serve(ln)
}

// Serve makes OutDir, then takes connections on ln until it fails. each one is
// handled in its own goroutine. Serve closes ln before it returns.
func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()

	if err := os.MkdirAll(s.OutDir, 0o755); err != nil {
		return fmt.Errorf("server: prepare output dir: %w", err)
	}

	log.Printf("listening on %s, writing to %s", ln.Addr().String(), s.OutDir)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("server: accept: %w", err)
		}

		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	if err := s.receive(conn); err != nil {
		log.Printf("transfer from %s failed: %v", conn.RemoteAddr(), err)
	}
}

func (s *Server) receive(conn net.Conn) error {
	s.sweep(time.Now())

	if err := conn.SetReadDeadline(time.Now().Add(s.headerTimeout())); err != nil {
		return fmt.Errorf("server: set header deadline: %w", err)
	}

	hdr, err := protocol.ReadHeader(conn)
	if err != nil {
		return err
	}

	_ = conn.SetReadDeadline(time.Time{})

	name, err := protocol.SafeName(hdr.Name)
	if err != nil {
		return err
	}

	dst := filepath.Join(s.OutDir, name)
	tmp := dst + ".gshift-part"

	entry, err := s.open(name, dst, tmp, hdr.TotalSize)
	if err != nil {
		return err
	}
	defer entry.release()

	n, err := io.CopyN(io.NewOffsetWriter(entry.file, hdr.Offset), conn, hdr.Length)
	if err != nil {
		entry.abort()
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("server: %s: copied %d of %d bytes: %w", name, n, hdr.Length, err)
	}

	done, err := entry.addSpan(hdr.Offset, n)
	if err != nil {
		entry.abort()
		return fmt.Errorf("server: %s: %w", name, err)
	}
	if !done {
		return nil
	}

	if err := entry.commit(); err != nil {
		return err
	}
	s.chunks.Delete(name)

	log.Printf("received %s (%d bytes) from %s", name, entry.total, conn.RemoteAddr())
	return nil
}
