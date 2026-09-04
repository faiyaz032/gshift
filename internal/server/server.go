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
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

const defaultHeaderTimeout = 30 * time.Second

// Server receives files into one output directory.
type Server struct {
	// Addr is the listen address like ":9000". Serve does not use it.
	Addr string

	// OutDir is where received files are written.
	OutDir string

	// HeaderTimeout bounds the wait for a header. zero means 30 seconds.
	HeaderTimeout time.Duration
}

func (s *Server) headerTimeout() time.Duration {
	if s.HeaderTimeout > 0 {
		return s.HeaderTimeout
	}
	return defaultHeaderTimeout
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
	// a peer that connects and says nothing would hold this goroutine forever
	if err := conn.SetReadDeadline(time.Now().Add(s.headerTimeout())); err != nil {
		return fmt.Errorf("server: set header deadline: %w", err)
	}

	hdr, err := protocol.ReadHeader(conn)
	if err != nil {
		return err
	}

	// a big transfer is allowed to take as long as it takes.
	// only a broken conn can fail here and the copy below reports that better.
	_ = conn.SetReadDeadline(time.Time{})

	name, err := protocol.SafeName(hdr.Name)
	if err != nil {
		return err
	}

	dst := filepath.Join(s.OutDir, name)
	tmp := dst + ".gshift-part"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("server: create %s: %w", tmp, err)
	}

	committed := false
	defer func() {
		f.Close()
		if !committed {
			os.Remove(tmp)
		}
	}()

	n, err := io.CopyN(f, conn, hdr.Size)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("server: %s: copied %d of %d bytes: %w", name, n, hdr.Size, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("server: close %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("server: commit %s: %w", dst, err)
	}
	committed = true

	log.Printf("received %s (%d bytes) from %s", name, n, conn.RemoteAddr())
	return nil
}
