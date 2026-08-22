package config

import (
	"encoding/json"
	"os"
	"testing"
)

func mustWriteJSON(t *testing.T, path string, cfg *Config) {
	t.Helper()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
