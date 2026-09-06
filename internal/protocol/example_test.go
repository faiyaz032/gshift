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

func Example_wireFormat() {
	var wire bytes.Buffer
	if err := WriteHeader(&wire, Header{Name: "a.txt", TotalSize: 1, Length: 1}); err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Print(hex.Dump(wire.Bytes()))
	// Output:
	// 00000000  47 53 48 46 02 00 05 00  00 00 00 00 00 00 01 00  |GSHF............|
	// 00000010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 01 61  |...............a|
	// 00000020  2e 74 78 74                                       |.txt|
}

func ExampleReadHeader() {
	var stream bytes.Buffer
	if err := WriteHeader(&stream, Header{Name: "notes.txt", TotalSize: 11, Length: 11}); err != nil {
		fmt.Println("error:", err)
		return
	}
	stream.WriteString("hello there")

	hdr, err := ReadHeader(&stream)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("name=%s total=%d offset=%d length=%d\n", hdr.Name, hdr.TotalSize, hdr.Offset, hdr.Length)

	var payload strings.Builder
	if _, err := io.CopyN(&payload, &stream, hdr.Length); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("payload=%q\n", payload.String())

	// Output:
	// name=notes.txt total=11 offset=0 length=11
	// payload="hello there"
}

func ExampleReadHeader_notAGshiftStream() {
	_, err := ReadHeader(strings.NewReader("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))

	fmt.Println("rejected as not a gshift stream:", errors.Is(err, ErrBadMagic))

	// Output:
	// rejected as not a gshift stream: true
}

func ExampleSafeName() {
	const incoming = "/srv/incoming"

	for _, sent := range []string{"report.pdf", "../../etc/passwd", ".."} {
		safe, err := SafeName(sent)
		if err != nil {
			fmt.Printf("%-16s rejected\n", sent)
			continue
		}
		fmt.Printf("%-16s written to %s\n", sent, filepath.ToSlash(filepath.Join(incoming, safe)))
	}

	// Output:
	// report.pdf       written to /srv/incoming/report.pdf
	// ../../etc/passwd written to /srv/incoming/passwd
	// ..               rejected
}
