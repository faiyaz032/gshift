// Package client sends files to a gshift server.
package client

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

const dialTimeout = 10 * time.Second

// Send transfers the file at path to a gshift server at addr.
func Send(addr, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	// a fifo reports size 0 then streams forever, which the header cannot describe
	if !info.Mode().IsRegular() {
		return fmt.Errorf("client: %s is not a regular file", path)
	}

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return fmt.Errorf("client: dial %s: %w", addr, err)
	}
	defer conn.Close()

	hdr := protocol.Header{Name: filepath.Base(path), Size: info.Size()}
	if err := protocol.WriteHeader(conn, hdr); err != nil {
		return err
	}

	start := time.Now()

	n, err := io.Copy(conn, f)
	if err != nil {
		return fmt.Errorf("client: send body: %w", err)
	}

	// the file can shrink between stat and copy, and the peer is still waiting for hdr.Size
	if n != info.Size() {
		return fmt.Errorf("client: %s changed while sending, sent %d of %d bytes", path, n, info.Size())
	}

	elapsed := time.Since(start)
	log.Printf("sent %s (%d bytes) in %s (%s)", hdr.Name, n, elapsed.Round(time.Millisecond), rate(n, elapsed))
	return nil
}

func rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "instant"
	}
	return fmt.Sprintf("%.1f MiB/s", float64(n)/(1<<20)/d.Seconds())
}
