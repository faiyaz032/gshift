// Read this file first: it shows what the protocol package is FOR. Read
// protocol_test.go next: it states, rule by rule, what the package GUARANTEES.
package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// Example_wireFormat prints the exact bytes a gshift peer puts on the wire for
// a 1 byte file named "a.txt". This is the spec, rendered by the code itself.
//
//	MAGIC   4 bytes  "GSHF"
//	VERSION 1 byte
//	NAMELEN 2 bytes  big endian
//	SIZE    8 bytes  big endian, payload length
//	NAME    NAMELEN bytes, UTF-8
func Example_wireFormat() {
	var wire bytes.Buffer
	if err := WriteHeader(&wire, Header{Name: "a.txt", Size: 1}); err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Print(hex.Dump(wire.Bytes()))
	// Output:
	// 00000000  47 53 48 46 01 00 05 00  00 00 00 00 00 00 01 61  |GSHF...........a|
	// 00000010  2e 74 78 74                                       |.txt|
}

// ExampleReadHeader shows the receiving half of a transfer: one stream carries
// the header immediately followed by the file bytes, and the Size field in the
// header is what tells the receiver where the payload ends.
func ExampleReadHeader() {
	// What the sender put on the connection.
	var stream bytes.Buffer
	if err := WriteHeader(&stream, Header{Name: "notes.txt", Size: 11}); err != nil {
		fmt.Println("error:", err)
		return
	}
	stream.WriteString("hello there")

	// What the receiver does with it.
	hdr, err := ReadHeader(&stream)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("name=%s size=%d\n", hdr.Name, hdr.Size)

	// ReadHeader stops at the last byte of the name, so the stream is now
	// positioned on the first payload byte. CopyN, not Copy: the header
	// declared the length, and taking more than that would be a protocol
	// violation (and, on a real socket, an unbounded read).
	var payload strings.Builder
	if _, err := io.CopyN(&payload, &stream, hdr.Size); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("payload=%q\n", payload.String())

	// Output:
	// name=notes.txt size=11
	// payload="hello there"
}

// ExampleReadHeader_notAGshiftStream shows why the header starts with a magic
// number: anything else connecting to the port is rejected on its first 4
// bytes, with an error the caller can identify using errors.Is.
func ExampleReadHeader_notAGshiftStream() {
	_, err := ReadHeader(strings.NewReader("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))

	fmt.Println("rejected as not a gshift stream:", errors.Is(err, ErrBadMagic))

	// Output:
	// rejected as not a gshift stream: true
}

// ExampleSafeName shows the one job SafeName has: turn a name chosen by a
// remote peer into something that cannot escape the destination directory.
func ExampleSafeName() {
	const incoming = "/srv/incoming"

	for _, sent := range []string{"report.pdf", "../../etc/passwd", ".."} {
		safe, err := SafeName(sent)
		if err != nil {
			fmt.Printf("%-16s rejected\n", sent)
			continue
		}
		// ToSlash only so this example prints the same on every OS.
		fmt.Printf("%-16s written to %s\n", sent, filepath.ToSlash(filepath.Join(incoming, safe)))
	}

	// Output:
	// report.pdf       written to /srv/incoming/report.pdf
	// ../../etc/passwd written to /srv/incoming/passwd
	// ..               rejected
}
