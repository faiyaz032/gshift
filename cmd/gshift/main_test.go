package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func runAndCapture(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	var out, errOut bytes.Buffer
	code = runCLI(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestRun_WithNoArgumentsPrintsUsageToStderrAndExitsTwo(t *testing.T) {
	code, stdout, stderr := runAndCapture(t)

	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "no command given") {
		t.Errorf("stderr = %q, want it to say no command was given", stderr)
	}
	if !strings.Contains(stderr, "gshift serve") {
		t.Errorf("stderr = %q, want it to include the usage text", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing on stdout for a usage error", stdout)
	}
}

func TestRun_WithAnUnknownCommandNamesItAndExitsTwo(t *testing.T) {
	code, stdout, stderr := runAndCapture(t, "bogus")

	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, `"bogus"`) {
		t.Errorf("stderr = %q, want it to name the command it did not recognise", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing on stdout for a usage error", stdout)
	}
}

func TestRun_WithHelpPrintsUsageToStdoutAndExitsZero(t *testing.T) {
	for _, arg := range []string{"-h", "-help", "--help"} {
		t.Run(arg, func(t *testing.T) {
			code, stdout, stderr := runAndCapture(t, "serve", arg)

			if code != exitOK {
				t.Errorf("exit code = %d, want %d", code, exitOK)
			}
			if !strings.Contains(stdout, "gshift serve") {
				t.Errorf("stdout = %q, want the usage text", stdout)
			}
			if stderr != "" {
				t.Errorf("stderr = %q, want nothing on stderr when help was asked for", stderr)
			}
		})
	}
}

func TestRun_WithABadFlagReportsItAndExitsTwo(t *testing.T) {
	code, _, stderr := runAndCapture(t, "serve", "-nope")

	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "-nope") {
		t.Errorf("stderr = %q, want it to name the offending flag", stderr)
	}
}

func TestRun_WithAStrayArgumentReportsItAndExitsTwo(t *testing.T) {
	code, _, stderr := runAndCapture(t, "serve", "received")

	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "takes no arguments") {
		t.Errorf("stderr = %q, want it to say serve takes no arguments", stderr)
	}
}

func TestRun_WhenTheAddressCannotBeBoundExitsOne(t *testing.T) {
	code, stdout, stderr := runAndCapture(t, "serve", "-addr", "definitely not an address", "-out", t.TempDir())

	if code != exitFailure {
		t.Errorf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, "listen") {
		t.Errorf("stderr = %q, want it to report the listen failure", stderr)
	}
	if strings.Contains(stderr, "gshift serve [-addr") {
		t.Errorf("stderr = %q, want no usage text for a runtime failure", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing on stdout", stdout)
	}
}

func TestParseServe_Defaults(t *testing.T) {
	srv, err := parseServe(nil)
	if err != nil {
		t.Fatalf("parseServe(nil) error = %v, want nil", err)
	}

	if srv.Addr != ":9000" {
		t.Errorf("Addr = %q, want %q", srv.Addr, ":9000")
	}
	if srv.OutDir != "./received" {
		t.Errorf("OutDir = %q, want %q", srv.OutDir, "./received")
	}
}

func TestParseServe_ReadsBothFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantAddr string
		wantOut  string
	}{
		{
			name:     "addr only",
			args:     []string{"-addr", "127.0.0.1:0"},
			wantAddr: "127.0.0.1:0",
			wantOut:  "./received",
		},
		{
			name:     "out only",
			args:     []string{"-out", "/srv/incoming"},
			wantAddr: ":9000",
			wantOut:  "/srv/incoming",
		},
		{
			name:     "both, in either order",
			args:     []string{"-out", "/srv/incoming", "-addr", ":1234"},
			wantAddr: ":1234",
			wantOut:  "/srv/incoming",
		},
		{
			name:     "the double dash and equals forms are equivalent",
			args:     []string{"--addr=:1234", "--out=/srv/incoming"},
			wantAddr: ":1234",
			wantOut:  "/srv/incoming",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := parseServe(tt.args)
			if err != nil {
				t.Fatalf("parseServe(%q) error = %v, want nil", tt.args, err)
			}
			if srv.Addr != tt.wantAddr {
				t.Errorf("Addr = %q, want %q", srv.Addr, tt.wantAddr)
			}
			if srv.OutDir != tt.wantOut {
				t.Errorf("OutDir = %q, want %q", srv.OutDir, tt.wantOut)
			}
		})
	}
}

func TestParseServe_RejectsBadArgumentsAsUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "an unknown flag", args: []string{"-nope"}},
		{name: "a flag with no value", args: []string{"-addr"}},
		{name: "a stray positional argument", args: []string{"received"}},
		{name: "a positional argument after valid flags", args: []string{"-addr", ":1", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := parseServe(tt.args)
			if err == nil {
				t.Fatalf("parseServe(%q) = %+v, want a usage error", tt.args, srv)
			}
			if !errors.Is(err, errUsage) {
				t.Errorf("parseServe(%q) error = %v, want it to wrap %v", tt.args, err, errUsage)
			}
			if srv != nil {
				t.Errorf("parseServe(%q) = %+v, want a nil Server alongside an error", tt.args, srv)
			}
		})
	}
}

func TestParseServe_TreatsHelpAsARequestNotAMistake(t *testing.T) {
	for _, arg := range []string{"-h", "-help", "--help"} {
		_, err := parseServe([]string{arg})
		if !errors.Is(err, errHelp) {
			t.Errorf("parseServe(%q) error = %v, want it to wrap %v", arg, err, errHelp)
		}
		if errors.Is(err, errUsage) {
			t.Errorf("parseServe(%q) error = %v, want it NOT to be a usage error", arg, err)
		}
	}
}

func TestParseSend_ReturnsTheArgumentsInTheOrderTyped(t *testing.T) {
	src, addr, parallel, err := parseSend([]string{"blob.bin", "localhost:9000"})
	if err != nil {
		t.Fatalf("parseSend() error = %v, want nil", err)
	}
	if src != "blob.bin" {
		t.Errorf("src = %q, want %q", src, "blob.bin")
	}
	if addr != "localhost:9000" {
		t.Errorf("addr = %q, want %q", addr, "localhost:9000")
	}
	if parallel != defaultParallel {
		t.Errorf("parallel = %d, want the default %d", parallel, defaultParallel)
	}
}

func TestParseSend_ReadsTheParallelFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		{name: "explicit value before the positionals", args: []string{"-parallel", "8", "blob.bin", "localhost:9000"}, want: 8},
		{name: "the equals form", args: []string{"--parallel=1", "blob.bin", "localhost:9000"}, want: 1},
		{name: "not given at all", args: []string{"blob.bin", "localhost:9000"}, want: defaultParallel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, parallel, err := parseSend(tt.args)
			if err != nil {
				t.Fatalf("parseSend(%q) error = %v, want nil", tt.args, err)
			}
			if parallel != tt.want {
				t.Errorf("parallel = %d, want %d", parallel, tt.want)
			}
		})
	}
}

func TestParseSend_RejectsTheWrongNumberOfArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "no arguments", args: nil},
		{name: "only a file", args: []string{"blob.bin"}},
		{name: "a stray third argument", args: []string{"blob.bin", "localhost:9000", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, addr, _, err := parseSend(tt.args)
			if !errors.Is(err, errUsage) {
				t.Fatalf("parseSend(%q) error = %v, want it to wrap %v", tt.args, err, errUsage)
			}
			if src != "" || addr != "" {
				t.Errorf("parseSend(%q) = %q, %q, want empty strings alongside an error", tt.args, src, addr)
			}
		})
	}
}

func TestParseSend_TreatsHelpAsARequestNotAMistake(t *testing.T) {
	_, _, _, err := parseSend([]string{"-h"})
	if !errors.Is(err, errHelp) {
		t.Errorf("parseSend() error = %v, want it to wrap %v", err, errHelp)
	}
}

func TestRun_SendReportsAMissingFileAndExitsOne(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.bin")

	code, _, stderr := runAndCapture(t, "send", missing, "127.0.0.1:1")
	if code != exitFailure {
		t.Errorf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr, "no such file") {
		t.Errorf("stderr = %q, want it to report the missing file", stderr)
	}
}
