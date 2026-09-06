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

const defaultTransferTTL = 5 * time.Minute

type Server struct {
	Addr          string
	OutDir        string
	HeaderTimeout time.Duration
	TransferTTL   time.Duration
	assemblies    sync.Map
	nextSweepAt   atomic.Int64
}

type span struct {
	start, end int64
}

type fileAssembly struct {
	mu               sync.Mutex
	file             *os.File
	tmpPath, dstPath string
	totalSize        int64

	spans []span

	activeChunks int
	updated      time.Time
	closed       bool
}

func (s *Server) acquireEntry(name, dst, tmp string, total int64) (*fileAssembly, error) {
	actual, _ := s.assemblies.LoadOrStore(name, &fileAssembly{})
	entry := actual.(*fileAssembly)

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
		entry.tmpPath = tmp
		entry.dstPath = dst
		entry.totalSize = total

	case entry.totalSize != total:
		entry.abortLocked()
		return nil, fmt.Errorf("server: %s: chunk declares a total of %d bytes, the transfer under way declares %d",
			name, total, entry.totalSize)
	}

	entry.activeChunks++
	entry.updated = time.Now()

	return entry, nil
}

func (e *fileAssembly) markChunkFinished() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.activeChunks--
	e.updated = time.Now()
}

func (e *fileAssembly) recordSpan(offset, n int64) (done bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := span{start: offset, end: offset + n}

	i := sort.Search(len(e.spans), func(i int) bool { return e.spans[i].start >= s.start })
	if i > 0 && e.spans[i-1].end > s.start {
		return false, overlapError(s, e.spans[i-1])
	}
	if i < len(e.spans) && e.spans[i].start < s.end {
		return false, overlapError(s, e.spans[i])
	}

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
	return len(e.spans) == 1 && covered.start == 0 && covered.end == e.totalSize, nil
}

func overlapError(got, have span) error {
	return fmt.Errorf("chunk [%d,%d) overlaps [%d,%d), which already arrived",
		got.start, got.end, have.start, have.end)
}

func (e *fileAssembly) expireIfStale(cutoff time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.activeChunks > 0 || e.updated.After(cutoff) {
		return false
	}
	e.abortLocked()

	return true
}

func (e *fileAssembly) abort() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.abortLocked()
}

func (e *fileAssembly) abortLocked() {
	if e.closed {
		return
	}
	e.closed = true
	if e.file != nil {
		e.file.Close()
		os.Remove(e.tmpPath)
	}
}

func (e *fileAssembly) commit() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("server: %s: transfer already finished", e.dstPath)
	}
	e.closed = true
	f, tmp, dst := e.file, e.tmpPath, e.dstPath
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

func (s *Server) sweep(now time.Time) {
	ttl := s.effectiveTransferTTL()

	next := s.nextSweepAt.Load()
	if now.UnixNano() < next || !s.nextSweepAt.CompareAndSwap(next, now.Add(ttl).UnixNano()) {
		return
	}

	cutoff := now.Add(-ttl)
	s.assemblies.Range(func(key, value any) bool {
		if entry := value.(*fileAssembly); entry.expireIfStale(cutoff) {
			s.assemblies.Delete(key)
		}
		return true
	})
}

func (s *Server) effectiveHeaderTimeout() time.Duration {
	if s.HeaderTimeout > 0 {
		return s.HeaderTimeout
	}
	return defaultHeaderTimeout
}

func (s *Server) effectiveTransferTTL() time.Duration {
	if s.TransferTTL > 0 {
		return s.TransferTTL
	}
	return defaultTransferTTL
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.Addr, err)
	}
	return s.Serve(ln)
}

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

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	if err := s.receiveChunk(conn); err != nil {
		log.Printf("transfer from %s failed: %v", conn.RemoteAddr(), err)
	}
}

func (s *Server) receiveChunk(conn net.Conn) error {
	s.sweep(time.Now())

	if err := conn.SetReadDeadline(time.Now().Add(s.effectiveHeaderTimeout())); err != nil {
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

	entry, err := s.acquireEntry(name, dst, tmp, hdr.TotalSize)
	if err != nil {
		return err
	}
	defer entry.markChunkFinished()

	n, err := io.CopyN(io.NewOffsetWriter(entry.file, hdr.Offset), conn, hdr.Length)
	if err != nil {
		entry.abort()
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("server: %s: copied %d of %d bytes: %w", name, n, hdr.Length, err)
	}

	done, err := entry.recordSpan(hdr.Offset, n)
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
	s.assemblies.Delete(name)

	log.Printf("received %s (%d bytes) from %s", name, entry.totalSize, conn.RemoteAddr())
	return nil
}
