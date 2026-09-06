package client

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/faiyaz032/gshift/internal/protocol"
)

const dialTimeout = 10 * time.Second

const minChunkSize = 4 << 20

type chunkRange struct {
	offset, length int64
}

func splitRanges(size int64, n int) []chunkRange {
	if n < 1 {
		n = 1
	}
	if size > 0 {
		if max := int(size / minChunkSize); max < n {
			if max < 1 {
				max = 1
			}
			n = max
		}
	} else {
		n = 1
	}

	base := size / int64(n)
	rem := size % int64(n)

	ranges := make([]chunkRange, n)
	var offset int64
	for i := range ranges {
		length := base
		if int64(i) < rem {
			length++
		}
		ranges[i] = chunkRange{offset: offset, length: length}
		offset += length
	}
	return ranges
}

func SendFile(addr, path string, parallel int) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("client: %s is not a regular file", path)
	}

	size := info.Size()
	name := filepath.Base(path)
	ranges := splitRanges(size, parallel)

	start := time.Now()

	var wg sync.WaitGroup
	errs := make([]error, len(ranges))
	for i, r := range ranges {
		wg.Add(1)
		go func(i int, r chunkRange) {
			defer wg.Done()
			errs[i] = sendChunk(addr, name, size, r, f)
		}(i, r)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return err
	}

	elapsed := time.Since(start)
	log.Printf("sent %s (%d bytes) in %s over %d connection(s) (%s)",
		name, size, elapsed.Round(time.Millisecond), len(ranges), formatThroughput(size, elapsed))
	return nil
}

func sendChunk(addr, name string, totalSize int64, r chunkRange, f *os.File) error {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return fmt.Errorf("client: dial %s: %w", addr, err)
	}
	defer conn.Close()

	hdr := protocol.Header{Name: name, TotalSize: totalSize, Offset: r.offset, Length: r.length}
	if err := protocol.WriteHeader(conn, hdr); err != nil {
		return err
	}

	section := io.NewSectionReader(f, r.offset, r.length)
	n, err := io.Copy(conn, section)
	if err != nil {
		return fmt.Errorf("client: send chunk [%d,%d) of %s: %w", r.offset, r.offset+r.length, name, err)
	}
	if n != r.length {
		return fmt.Errorf("client: %s changed while sending, sent %d of %d bytes for chunk at offset %d",
			name, n, r.length, r.offset)
	}

	return nil
}

func formatThroughput(n int64, d time.Duration) string {
	if d <= 0 {
		return "instant"
	}
	return fmt.Sprintf("%.1f MiB/s", float64(n)/(1<<20)/d.Seconds())
}
