package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestBuildInjectedMetadata verifies Makefile build-linux ldflags embed commit
// and builtAt into the binary.
func TestBuildInjectedMetadata(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping ldflags build test on windows")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	bin := filepath.Join(t.TempDir(), "tc-repro")
	const (
		wantCommit  = "deadbeefcafe"
		wantBuiltAt = "2026-05-21T12:00:00Z"
	)
	ldflags := "-s -w -X main.commit=" + wantCommit + " -X main.builtAt=" + wantBuiltAt
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", ldflags, "-o", bin, "./cmd/tc-repro")
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read binary: %v", err)
	}
	if !bytes.Contains(data, []byte(wantCommit)) {
		t.Fatalf("binary missing injected commit %q", wantCommit)
	}
	if !bytes.Contains(data, []byte(wantBuiltAt)) {
		t.Fatalf("binary missing injected builtAt %q", wantBuiltAt)
	}

	badCfg := filepath.Join(t.TempDir(), "missing.json")
	run := exec.Command(bin, "--config", badCfg, "start")
	run.Stdout = discardWriter{}
	run.Stderr = discardWriter{}
	if err := run.Run(); err == nil {
		t.Fatal("expected non-zero exit for missing config")
	}
}
