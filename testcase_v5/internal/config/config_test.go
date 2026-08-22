package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadAccept reads testdata/accept-minimal.json fresh per test so mutations
// in one case do not leak into another.
func loadAccept(t *testing.T) *Config {
	t.Helper()
	cfg, err := Load("testdata/accept-minimal.json")
	if err != nil {
		t.Fatalf("accept-minimal failed to load: %v", err)
	}
	return cfg
}

func TestAcceptMinimal(t *testing.T) {
	cfg := loadAccept(t)
	if cfg.Run.BatchSize != 10 {
		t.Fatalf("batch size: got %d want 10", cfg.Run.BatchSize)
	}
	if cfg.Database.Write.Schema != "repro_db" {
		t.Fatalf("schema: got %q want repro_db", cfg.Database.Write.Schema)
	}
}

func TestRejectUnknownField(t *testing.T) {
	_, err := Load("testdata/reject-unknown-field.json")
	if err == nil {
		t.Fatal("expected unknown-field rejection")
	}
	if !strings.Contains(err.Error(), "ghost_field") {
		t.Fatalf("expected unknown-field error to mention key; got %v", err)
	}
}

func TestRejectMissingFile(t *testing.T) {
	_, err := Load("testdata/does-not-exist.json")
	if err == nil {
		t.Fatal("expected open error for missing file")
	}
}

// goldenRejects are committed JSON fixtures under testdata/. Each file
// violates exactly one validation rule relative to accept-minimal.json.
func TestRejectGoldenFiles(t *testing.T) {
	cases := []struct {
		file string
		want string
	}{
		{"reject-batch-size.json", "run.batch_size"},
		{"reject-blob-sum.json", "blob_profile percentages must sum to 100"},
		{"reject-schema-mismatch.json", "schema"},
		{"reject-table-mismatch.json", "table"},
		{"reject-write-host-socket.json", "database.write requires host or socket"},
		{"reject-read-host-socket.json", "database.read requires host or socket"},
		{"reject-tenants-zero.json", "init.tenants"},
		{"reject-max-parallel.json", "max_parallel_tenants"},
		{"reject-encryption-mode.json", "encryption.mode"},
		{"reject-stop-condition.json", "stop_condition.mode"},
		{"reject-replication-check.json", "replication_check.mode"},
		{"reject-negative-lag.json", "max_replication_lag_seconds"},
		{"reject-missing-write-user.json", "database.write.user"},
		{"reject-artifacts-dir.json", "artifacts.dir"},
		{"reject-negative-max-runtime.json", "safety.max_runtime_seconds"},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("testdata", tc.file)
			cfg, err := Load(path)
			if err == nil {
				// Load validates; expect failure wrapped in ErrConfigInvalid.
				t.Fatalf("expected Load to reject %s", tc.file)
			}
			if !errors.Is(err, ErrConfigInvalid) {
				t.Fatalf("expected ErrConfigInvalid for %s, got %v", tc.file, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error for %s missing %q: %v", tc.file, tc.want, err)
			}
			_ = cfg
		})
	}
}

// rejectCases walks the validator over the accept config mutated to violate
// exactly one rule each. Kept alongside golden files for fast iteration.
func TestRejectCases(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(c *Config)
		needs string
	}{
		{"BatchSize", func(c *Config) { c.Run.BatchSize = 9 }, "run.batch_size"},
		{"BlobSum", func(c *Config) { c.Init.BlobProfile.SmallPct = 31 }, "blob_profile percentages must sum to 100"},
		{"SchemaMismatch", func(c *Config) { c.Database.Read.Schema = "other_db" }, "schema"},
		{"TableMismatch", func(c *Config) { c.Database.Read.Table = "other_tbl" }, "table"},
		{"WriteHostAndSocketMissing", func(c *Config) {
			c.Database.Write.Host = ""
			c.Database.Write.Socket = ""
		}, "database.write requires host or socket"},
		{"ReadHostMissing", func(c *Config) {
			c.Database.Read.Host = ""
			c.Database.Read.Socket = ""
		}, "database.read requires host or socket"},
		{"TenantsZero", func(c *Config) { c.Init.Tenants = 0 }, "init.tenants"},
		{"MaxParallelTooMany", func(c *Config) {
			c.Init.Tenants = 1
			c.Init.MaxParallelTenants = 5
		}, "max_parallel_tenants"},
		{"EncryptionMode", func(c *Config) { c.Run.Encryption.Mode = "aes" }, "encryption.mode"},
		{"StopCondition", func(c *Config) { c.Run.StopCondition.Mode = "n_rows" }, "stop_condition.mode"},
		{"ReplicationCheck", func(c *Config) { c.Run.ReplicationCheck.Mode = "ping" }, "replication_check.mode"},
		{"NegativeLag", func(c *Config) { c.Init.MaxReplicationLagSeconds = -1 }, "max_replication_lag_seconds"},
		{"MissingWriteUser", func(c *Config) { c.Database.Write.User = "" }, "database.write.user"},
		{"ArtifactsDirMissing", func(c *Config) { c.Artifacts.Dir = "" }, "artifacts.dir"},
		{"NegativeMaxRuntime", func(c *Config) { c.Safety.MaxRuntimeSeconds = -10 }, "safety.max_runtime_seconds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadAccept(t)
			tc.mut(cfg)
			err := Validate(cfg)
			if err == nil {
				t.Fatalf("expected validation error mentioning %q", tc.needs)
			}
			if !strings.Contains(err.Error(), tc.needs) {
				t.Fatalf("missing %q in error: %v", tc.needs, err)
			}
		})
	}
}

func TestValidateCollectsMultipleErrors(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Run.BatchSize = 9
	cfg.Init.Tenants = 0
	cfg.Run.Encryption.Mode = "aes"
	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected joined validation error")
	}
	for _, want := range []string{"batch_size", "init.tenants", "encryption.mode"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error missing %q: %v", want, err)
		}
	}
}

func TestLoadErrorIsErrConfigInvalid(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Run.BatchSize = 9
	tmp := filepath.Join(t.TempDir(), "bad.json")
	mustWriteJSON(t, tmp, cfg)
	_, err := Load(tmp)
	if err == nil {
		t.Fatal("expected Load to reject the mutated config")
	}
	if !errors.Is(err, ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid sentinel, got %v", err)
	}
}

func TestHashDifferentiatesContent(t *testing.T) {
	h1, err := Hash("testdata/accept-minimal.json")
	if err != nil {
		t.Fatalf("hash accept: %v", err)
	}
	tmp := filepath.Join(t.TempDir(), "cfg.json")
	raw, err := os.ReadFile("testdata/accept-minimal.json")
	if err != nil {
		t.Fatalf("read accept: %v", err)
	}
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	h2, err := Hash(tmp)
	if err != nil {
		t.Fatalf("hash tmp: %v", err)
	}
	if h1 == h2 {
		t.Fatalf("expected differing hashes for differing bytes; both = %s", h1)
	}
}
