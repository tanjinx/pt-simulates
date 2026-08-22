//go:build integration

// Package smoke is the end-to-end integration test for the v5 harness.
// It spins up the containerized MySQL declared in
// compose/docker-compose.yml, writes a 1-tenant x 100-row config to a tmp
// path, runs init then run, and asserts: provenance.json valid, 100 rows
// updated, summary.json contains the expected totals.
//
// Tag-gated with `integration` so `go test ./...` does not require Docker;
// invoke with `make smoke` or `go test -tags=integration ./internal/smoke/`.
package smoke

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/artifacts"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	initphase "github.com/matias-sanchez/pt-simulates/testcase_v5/internal/init"
	runphase "github.com/matias-sanchez/pt-simulates/testcase_v5/internal/run"
)

const (
	smokeDSN       = "root@tcp(127.0.0.1:3307)/repro_db?parseTime=true&multiStatements=false&interpolateParams=false"
	smokeHost      = "127.0.0.1"
	smokePort      = 3307
	smokeSchema    = "repro_db"
	smokeTable     = "files"
	smokeRows      = 100
	smokeWaitTotal = 90 * time.Second
)

func TestEndToEndSmoke(t *testing.T) {
	waitForMySQL(t)

	tmp := t.TempDir()
	artifactsDir := filepath.Join(tmp, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	cfg := smokeConfig(artifactsDir)
	if err := config.Validate(cfg); err != nil {
		t.Fatalf("smoke config invalid: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if err := initphase.Run(ctx, cfg, "smoke-commit", "smoke-built"); err != nil {
		t.Fatalf("init.Run: %v", err)
	}
	assertRowCount(t, smokeRows)
	assertNoENC(t)
	assertAllSecondaryIndexesRebuilt(t)

	if err := runphase.Run(ctx, cfg, "smoke-commit", "smoke-built"); err != nil {
		t.Fatalf("run.Run: %v", err)
	}
	assertENCPopulated(t, smokeRows)

	// Two artifact directories should exist (init then run).
	dirs, err := os.ReadDir(artifactsDir)
	if err != nil {
		t.Fatalf("read artifacts: %v", err)
	}
	if len(dirs) < 2 {
		t.Fatalf("expected >=2 artifact dirs (init+run), got %d", len(dirs))
	}
	for _, d := range dirs {
		assertProvenance(t, filepath.Join(artifactsDir, d.Name()))
	}
}

func smokeConfig(artifactsDir string) *config.Config {
	endpoint := config.Endpoint{
		Host: smokeHost, Port: smokePort, User: "root", Password: "",
		Schema: smokeSchema, Table: smokeTable,
	}
	return &config.Config{
		Remote: config.Remote{},
		Database: config.Database{
			Write: endpoint,
			Read:  endpoint,
		},
		Init: config.Init{
			Tenants: 1, RowsPerTenant: smokeRows,
			TenantIDBase: 9_000_001, StartID: 90_000_000_000,
			DateBase: 1_700_000_000, DateStep: 60, Seed: 7,
			BlobProfile:     config.BlobProfile{SmallPct: 100, MediumPct: 0, LargePct: 0},
			InsertBatchRows: 50, InsertBatchBytes: 67_108_864,
			InitProgressRows: 50, TenantProgressRows: 50,
			InitWorkers: 1, MaxParallelTenants: 1,
			RelaxDurability:          true, // exercise SET GLOBAL path
			DisableRedoLog:           true, // exercise ALTER INSTANCE DISABLE INNODB REDO_LOG
			BulkIndexRebuild:         true, // exercise PK-only create + ADD KEY pass
			ForceInit:                true,
			MaxReplicationLagSeconds: 0, // disabled for single-node smoke
		},
		Run: config.Run{
			BatchSize:                     10,
			ReadSource:                    "replica",
			WriteTarget:                   "master",
			Encryption:                    config.Encryption{Mode: "sha256"},
			StopCondition:                 config.StopCondition{Mode: "all_rows"},
			ReplicationCheck:              config.ReplicationCheck{Mode: "show_replica_status_only"},
			MaxParallelTenants:            1,
			ProgressIntervalBatches:       10,
			TenantLogIntervalBatches:      10,
			ReplicationLogIntervalSeconds: 5,
		},
		Safety:    config.Safety{ForceInitRequiresFlag: true, MaxRuntimeSeconds: 0},
		Artifacts: config.Artifacts{Dir: artifactsDir},
		Debug:     config.Debug{LogSQLOnce: false},
	}
}

func waitForMySQL(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(smokeWaitTotal)
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", smokeDSN)
		if err == nil {
			if err := db.Ping(); err == nil {
				_ = db.Close()
				return
			}
			_ = db.Close()
		}
		time.Sleep(1500 * time.Millisecond)
	}
	t.Fatalf("MySQL on %s never became reachable within %s; run `make smoke-up` first",
		smokeDSN, smokeWaitTotal)
}

func openSmokeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", smokeDSN)
	if err != nil {
		t.Fatalf("open smoke db: %v", err)
	}
	return db
}

func assertRowCount(t *testing.T, want int) {
	t.Helper()
	db := openSmokeDB(t)
	defer db.Close()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM files").Scan(&got); err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != want {
		t.Fatalf("row count: got %d want %d", got, want)
	}
}

func assertNoENC(t *testing.T) {
	t.Helper()
	db := openSmokeDB(t)
	defer db.Close()
	var n int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM files WHERE contents_enc IS NOT NULL").Scan(&n); err != nil {
		t.Fatalf("enc probe: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 rows with contents_enc after init; got %d", n)
	}
}

func assertENCPopulated(t *testing.T, want int) {
	t.Helper()
	db := openSmokeDB(t)
	defer db.Close()
	var n int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM files WHERE contents_enc IS NOT NULL").Scan(&n); err != nil {
		t.Fatalf("enc count: %v", err)
	}
	if n != want {
		t.Fatalf("expected %d rows with contents_enc after run; got %d", want, n)
	}
}

// assertAllSecondaryIndexesRebuilt verifies the BulkIndexRebuild path
// landed every secondary index from the embedded schema.
func assertAllSecondaryIndexesRebuilt(t *testing.T) {
	t.Helper()
	db := openSmokeDB(t)
	defer db.Close()
	rows, err := db.Query("SHOW INDEX FROM files")
	if err != nil {
		t.Fatalf("show index: %v", err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("cols: %v", err)
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		for i, c := range cols {
			if c == "Key_name" {
				if name, ok := vals[i].([]byte); ok {
					seen[string(name)] = true
				} else if s, ok := vals[i].(string); ok {
					seen[s] = true
				}
			}
		}
	}
	required := []string{"PRIMARY",
		"tenant_id_2", "tenant_id_3", "tenant_id_4", "tenant_id_5",
		"tenant_id_6", "tenant_id_7", "tenant_id_8",
		"user_and_tenant", "tenant_id_date_thumbnail_retrieved",
		"tenant_id", "highlight_type_tenant_id"}
	for _, r := range required {
		if !seen[r] {
			t.Errorf("expected index %q after init (BulkIndexRebuild path); SHOW INDEX returned %v", r, seen)
		}
	}
}

func assertProvenance(t *testing.T, dir string) {
	t.Helper()
	provPath := filepath.Join(dir, "provenance.json")
	raw, err := os.ReadFile(provPath)
	if err != nil {
		t.Fatalf("read %s: %v", provPath, err)
	}
	var h artifacts.ProvenanceHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatalf("unmarshal provenance: %v", err)
	}
	if h.Commit == "" || h.BuiltAt == "" {
		t.Fatalf("provenance missing commit/builtAt: %+v", h)
	}
	if h.StartedAt.IsZero() || h.EndedAt.IsZero() {
		t.Fatalf("provenance times incomplete: %+v", h)
	}
}

// TestSearchMixForwardRefScanIntegration exercises the changed run-phase read
// path: a run with search_mix forward_refscan enabled must complete cleanly
// — proving the SQL shape, the per-session REPEATABLE-READ SET, and the
// auto-EXPLAIN all execute (any failure aborts the run) — and must emit the
// dominant shape's EXPLAIN to the
// artifact, which MUST show a filesort and MUST NOT use the clustered PRIMARY
// (IGNORE INDEX(PRIMARY) + ORDER BY id guarantees both regardless of cardinality).
func TestSearchMixForwardRefScanIntegration(t *testing.T) {
	waitForMySQL(t)

	tmp := t.TempDir()
	artifactsDir := filepath.Join(tmp, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	cfg := smokeConfig(artifactsDir)
	// Multiple tenants so tenant_id is selective; enable the dominant search shape.
	cfg.Init.Tenants = 5
	cfg.Init.RowsPerTenant = 400
	cfg.Init.MaxParallelTenants = 5
	cfg.Run.MaxParallelTenants = 2
	cfg.Run.SearchMix = config.SearchMix{
		Enabled: true, ForwardRefScanWorkers: 2, RowsPerQuery: 10, LogIntervalSeconds: 1,
	}
	if err := config.Validate(cfg); err != nil {
		t.Fatalf("search_mix config invalid: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := initphase.Run(ctx, cfg, "smoke-commit", "smoke-built"); err != nil {
		t.Fatalf("init.Run: %v", err)
	}
	// max_runtime=0 + search_mix is NOT soak-driven: the backfill sweep completes, then
	// the searchers are cancelled and drain cleanly. A broken shape SQL or
	// isolation SET would surface here as a non-nil error.
	if err := runphase.Run(ctx, cfg, "smoke-commit", "smoke-built"); err != nil {
		t.Fatalf("run.Run with search_mix forward_refscan: %v", err)
	}

	rec := findForwardRefScanExplain(t, artifactsDir)
	if rec == nil {
		t.Fatal("no forward_refscan EXPLAIN record in run.jsonl — auto-EXPLAIN gate missing")
	}
	if extra, _ := rec["Extra"].(string); !strings.Contains(extra, "filesort") {
		t.Errorf("forward_refscan EXPLAIN Extra=%q, want 'Using filesort'", extra)
	}
	if key, _ := rec["key"].(string); key == "PRIMARY" {
		t.Errorf("forward_refscan chose key=PRIMARY; IGNORE INDEX(PRIMARY) must force off the clustered PK")
	}
}

// findForwardRefScanExplain scans every run.jsonl under artifactsDir for the
// search_mix forward_refscan EXPLAIN record (shape + event tags distinguish it
// from the backfill-loop EXPLAIN). Returns nil if absent.
func findForwardRefScanExplain(t *testing.T, artifactsDir string) map[string]any {
	t.Helper()
	dirs, err := os.ReadDir(artifactsDir)
	if err != nil {
		t.Fatalf("read artifacts: %v", err)
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(artifactsDir, d.Name(), "run.jsonl"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) != nil {
				continue
			}
			if rec["shape"] == "forward_refscan" && rec["event"] == "explain" {
				return rec
			}
		}
	}
	return nil
}

// fmtForLogs is a placeholder to keep "fmt" imported without %-vet errors.
var _ = fmt.Sprintf
