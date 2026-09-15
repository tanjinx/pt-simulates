package singlehost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(path, []byte(`{
		"database": {
			"defaults_file": "/etc/mysql.cnf",
			"host": "127.0.0.1",
			"port": 3306,
			"schema": "repro_db",
			"table": "files"
		},
		"workload": {
			"tenant_id": 42,
			"id_min": 100,
			"id_max": 199,
			"read_workers": 2,
			"write_workers": 1,
			"read_batch_size": 10,
			"read_iterations": 20,
			"write_iterations": 5
		}
	}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Database.Host != "127.0.0.1" {
		t.Fatalf("host = %q, want 127.0.0.1", cfg.Database.Host)
	}
	if cfg.Workload.ReadIterations != 20 || cfg.Workload.WriteIterations != 5 {
		t.Fatalf("unexpected iterations: %+v", cfg.Workload)
	}
}

func TestConfigRejectsInvalidRange(t *testing.T) {
	cfg := Config{
		Database: DatabaseConfig{
			DefaultsFile: "/etc/mysql.cnf",
			Host:         "127.0.0.1",
			Schema:       "repro_db",
			Table:        "files",
		},
		Workload: WorkloadConfig{
			TenantID:        42,
			IDMin:           200,
			IDMax:           100,
			ReadWorkers:     1,
			WriteWorkers:    1,
			ReadBatchSize:   10,
			ReadIterations:  1,
			WriteIterations: 1,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate succeeded with id_min > id_max")
	}
}
