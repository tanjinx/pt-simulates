package artifacts

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestLatestDirPicksNewest(t *testing.T) {
	root := t.TempDir()
	for _, id := range []string{"20260520T100000.000000000Z", "20260521T120000.000000000Z", "20260520T230000.000000000Z"} {
		if err := os.MkdirAll(filepath.Join(root, id), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := LatestDir(root)
	if err != nil {
		t.Fatalf("LatestDir: %v", err)
	}
	if dir.RunID != "20260521T120000.000000000Z" {
		t.Fatalf("got %q want newest", dir.RunID)
	}
}

func TestPrintStatusRendersSummary(t *testing.T) {
	root := t.TempDir()
	h := Header("c", "b", "h")
	d, err := Open(root, h)
	if err != nil {
		t.Fatal(err)
	}
	s := PhaseSummary{
		Phase: "run", Duration: time.Second, Rows: 42, Batches: 4,
		Tenants: []TenantRow{{TenantID: 1, Rows: 42, Batches: 4, Duration: time.Second}},
	}
	if err := d.WriteSummary(s); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := PrintStatus(root, &buf); err != nil {
		t.Fatalf("PrintStatus: %v", err)
	}
	for _, want := range []string{"run_id", "run", "42"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Fatalf("output missing %q: %s", want, buf.String())
		}
	}
}

func TestLogFilePrefersRun(t *testing.T) {
	root := t.TempDir()
	d, err := Open(root, Header("c", "b", "h"))
	if err != nil {
		t.Fatal(err)
	}
	initPath := filepath.Join(d.Path, "init.jsonl")
	runPath := filepath.Join(d.Path, "run.jsonl")
	if err := os.WriteFile(initPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(runPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := d.LogFile()
	if err != nil {
		t.Fatal(err)
	}
	if got != runPath {
		t.Fatalf("got %q want %q", got, runPath)
	}
}

func TestReadPID(t *testing.T) {
	root := t.TempDir()
	d, err := Open(root, Header("c", "b", "h"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := ReadPID(d)
	if err != nil {
		t.Fatalf("ReadPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("pid %d want %d", pid, os.Getpid())
	}
}

func TestSignalPIDRejectsDeadProcess(t *testing.T) {
	// Pick a PID unlikely to exist; ESRCH on Unix means "not running".
	const deadPID = 4000000000
	if err := SignalPID(deadPID, syscall.SIGTERM); err == nil {
		t.Fatal("expected error signaling dead pid")
	}
}
