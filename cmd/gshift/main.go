// Command gshift is a fast file transfer tool.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/faiyaz032/gshift/internal/client"
	"github.com/faiyaz032/gshift/internal/server"
)

const usage = `gshift - fast file transfer

usage:
  gshift serve [-addr <address>] [-out <dir>]
  gshift send [-parallel <n>] <file> <host:port>

serve flags:
  -addr       address to listen on            (default ":9000")
  -out        directory to write files into   (default "./received")

send flags:
  -parallel   connections to split the file across   (default 4)
`

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

const defaultParallel = 4

var (
	errUsage = errors.New("usage")
	errHelp  = errors.New("help requested")
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("gshift: ")
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch err := execute(cmd, args); {
	case err == nil:
		return exitOK
	case errors.Is(err, errHelp):
		fmt.Fprint(stdout, usage)
		return exitOK
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, err)
		fmt.Fprint(stderr, usage)
		return exitUsage
	default:
		fmt.Fprintln(stderr, "gshift:", err)
		return exitFailure
	}
}

func execute(cmd string, args []string) error {
	switch cmd {
	case "serve":
		srv, err := parseServe(args)
		if err != nil {
			return err
		}
		return srv.ListenAndServe()

	case "send":
		src, addr, parallel, err := parseSend(args)
		if err != nil {
			return err
		}
		return client.Send(addr, src, parallel)

	case "":
		return fmt.Errorf("%w: no command given", errUsage)

	default:
		return fmt.Errorf("%w: unknown command %q", errUsage, cmd)
	}
}

func parseServe(args []string) (*server.Server, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	addr := fs.String("addr", ":9000", "address to listen on")
	out := fs.String("out", "./received", "directory to write received files into")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, errHelp
		}
		return nil, fmt.Errorf("%w: serve: %w", errUsage, err)
	}
	if fs.NArg() != 0 {
		return nil, fmt.Errorf("%w: serve takes no arguments, got %q", errUsage, fs.Arg(0))
	}

	return &server.Server{Addr: *addr, OutDir: *out}, nil
}

func parseSend(args []string) (src, addr string, parallel int, err error) {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	p := fs.Int("parallel", defaultParallel, "connections to split the file across")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", "", 0, errHelp
		}
		return "", "", 0, fmt.Errorf("%w: send: %w", errUsage, err)
	}
	if fs.NArg() != 2 {
		return "", "", 0, fmt.Errorf("%w: send wants <file> <host:port>, got %d arguments", errUsage, fs.NArg())
	}

	return fs.Arg(0), fs.Arg(1), *p, nil
}
