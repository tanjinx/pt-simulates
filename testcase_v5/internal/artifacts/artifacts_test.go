package artifacts

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenWritesProvenanceAndPID(t *testing.T) {
	root := t.TempDir()
	h := Header("commit-hash", "2026-05-20T12:00:00Z", "deadbeef")
	d, err := Open(root, h)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !strings.HasPrefix(d.RunID, "20") {
		t.Fatalf("RunID looks wrong: %q", d.RunID)
	}
	prov := filepath.Join(d.Path, "provenance.json")
	pid := filepath.Join(d.Path, "pid")
	for _, p := range []string{prov, pid} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s: %v", p, err)
		}
	}
	raw, err := os.ReadFile(prov)
	if err != nil {
		t.Fatalf("read prov: %v", err)
	}
	var got ProvenanceHeader
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Commit != "commit-hash" {
		t.Fatalf("commit: got %q want commit-hash", got.Commit)
	}
}

func TestRenderTable(t *testing.T) {
	s := PhaseSummary{
		Phase: "init", Duration: 2 * time.Second, Rows: 1000, Batches: 10,
		Tenants: []TenantRow{{TenantID: 1, Rows: 1000, Batches: 10, Duration: 2 * time.Second}},
	}
	var buf bytes.Buffer
	if err := RenderTable(&buf, s); err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{"phase", "init", "rows", "1000"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("table missing %q: %s", want, buf.String())
		}
	}
}

func TestWithEndedAt(t *testing.T) {
	h := Header("c", "b", "h")
	end := time.Now().UTC().Add(time.Minute)
	h2 := h.WithEndedAt(end)
	if !h2.EndedAt.Equal(end) {
		t.Fatalf("EndedAt not set; got %v want %v", h2.EndedAt, end)
	}
	if !h.EndedAt.IsZero() {
		t.Fatal("WithEndedAt should not mutate receiver")
	}
}
