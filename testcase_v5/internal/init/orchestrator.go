package initphase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/db"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/debugsql"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/repllag"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/schema"
)

// Orchestrate seeds every tenant in cfg.Init using a write-pool shared across
// workers. Caller owns the logger.
func Orchestrate(ctx context.Context, cfg *config.Config, logger *slog.Logger) (Summary, error) {
	summary := Summary{Started: time.Now().UTC()}
	if err := bootstrapDatabase(ctx, cfg, logger); err != nil {
		return summary, err
	}
	writePool, err := db.Open(cfg.Database.Write, cfg.Init.MaxParallelTenants)
	if err != nil {
		return summary, fmt.Errorf("open write pool: %w", err)
	}
	defer func() {
		_ = writePool.Close()
	}()

	var readPool *sql.DB
	if cfg.Init.MaxReplicationLagSeconds > 0 {
		readPool, err = db.Open(cfg.Database.Read, 1)
		if err != nil {
			return summary, fmt.Errorf("open read pool for lag sampler: %w", err)
		}
		defer func() { _ = readPool.Close() }()
	}

	sqlOnce := debugsql.New(cfg.Debug.LogSQLOnce)

	initCtx, cancelInit := context.WithCancel(ctx)
	defer cancelInit()
	g, gctx := errgroup.WithContext(initCtx)
	var lagCoord repllag.Coordinator

	if readPool != nil {
		interval := time.Duration(cfg.Run.ReplicationLogIntervalSeconds) * time.Second
		g.Go(lagCoord.GoWatch(gctx, readPool, interval,
			int64(cfg.Init.MaxReplicationLagSeconds),
			logger.With(slog.String("component", "repllag"))))
	}

	abortInit := func(err error) (Summary, error) {
		cancelInit()
		_ = g.Wait()
		err = lagCoord.Prefer(err)
		if err != nil && errors.Is(err, repllag.ErrLagExceeded) {
			err = fmt.Errorf("%w: %w", ErrPhaseAborted, err)
		}
		summary.Ended = time.Now().UTC()
		summary.Duration = summary.Ended.Sub(summary.Started)
		summary.Err = err
		return summary, err
	}

	if err := probeAndCheck(gctx, writePool, cfg); err != nil {
		return abortInit(err)
	}

	redoToggled := false
	if cfg.Init.DisableRedoLog {
		if err := disableRedoLog(gctx, writePool, logger); err != nil {
			return abortInit(err)
		}
		redoToggled = true
	}
	defer func() {
		if !redoToggled {
			return
		}
		if rerr := enableRedoLog(context.Background(), writePool, logger); rerr != nil {
			logger.Error("redo log restore failed",
				slog.String("event", "restore_failed"),
				slog.String("err", rerr.Error()),
			)
		}
	}()

	if err := applySchema(gctx, writePool, cfg, logger); err != nil {
		return abortInit(err)
	}

	previous, err := maybeRelax(gctx, writePool, cfg, logger)
	if err != nil {
		return abortInit(err)
	}
	defer func() {
		if len(previous) == 0 {
			return
		}
		if rerr := restoreDurability(context.Background(), writePool, previous); rerr != nil {
			logger.Error("durability restore failed",
				slog.String("event", "restore_failed"),
				slog.String("err", rerr.Error()),
			)
		}
	}()

	cycle := blobgen.NewCycle(blobgen.Profile{
		SmallPct:  cfg.Init.BlobProfile.SmallPct,
		MediumPct: cfg.Init.BlobProfile.MediumPct,
		LargePct:  cfg.Init.BlobProfile.LargePct,
	})

	jobs := planJobs(cfg)
	summary.TenantCount = len(jobs)
	logger.Info("init plan",
		slog.Int("tenants", len(jobs)),
		slog.Int("rows_per_tenant", cfg.Init.RowsPerTenant),
		slog.Int("parallel_tenants", cfg.Init.MaxParallelTenants),
		slog.Bool("relax_durability", cfg.Init.RelaxDurability),
		slog.Int("max_replication_lag_seconds", cfg.Init.MaxReplicationLagSeconds),
	)

	var rowsSeeded atomic.Int64
	jobCh := make(chan tenantJob, cfg.Init.MaxParallelTenants)
	resultCh := make(chan tenantCounters, len(jobs))
	var workWg sync.WaitGroup
	workWg.Add(cfg.Init.MaxParallelTenants + 1)

	for w := 0; w < cfg.Init.MaxParallelTenants; w++ {
		workerID := w + 1
		g.Go(func() error {
			defer workWg.Done()
			workerLogger := logger.With(slog.Int("worker_id", workerID))
			for job := range jobCh {
				tc, err := seedTenant(gctx, writePool, cfg, cycle, job, sqlOnce, workerLogger)
				if err != nil {
					return err
				}
				resultCh <- tc
				rowsSeeded.Add(int64(tc.rows))
			}
			return nil
		})
	}

	g.Go(func() error {
		defer workWg.Done()
		defer close(jobCh)
		for _, j := range jobs {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case jobCh <- j:
			}
		}
		return nil
	})

	g.Go(func() error {
		defer cancelInit()
		workWg.Wait()
		if !cfg.Init.BulkIndexRebuild {
			return nil
		}
		idxStart := time.Now()
		logger.Info("secondary index rebuild start",
			slog.Int("indexes", len(schema.SecondaryKeys())),
		)
		if err := rebuildSecondaryIndexes(gctx, writePool, cfg, logger); err != nil {
			return err
		}
		logger.Info("secondary index rebuild complete",
			slog.Duration("elapsed", time.Since(idxStart)),
		)
		return nil
	})

	go func() {
		workWg.Wait()
		if !cfg.Init.BulkIndexRebuild {
			cancelInit()
		}
	}()

	waitErr := g.Wait()
	close(resultCh)

	for tc := range resultCh {
		summary.Tenants = append(summary.Tenants, tc)
		summary.Rows += tc.rows
		summary.Bytes += tc.bytes
		summary.Batches += tc.batches
	}
	waitErr = lagCoord.Prefer(waitErr)
	if waitErr != nil {
		summary.Ended = time.Now().UTC()
		summary.Duration = summary.Ended.Sub(summary.Started)
		summary.Err = waitErr
		if errors.Is(waitErr, repllag.ErrLagExceeded) {
			waitErr = fmt.Errorf("%w: %w", ErrPhaseAborted, waitErr)
			summary.Err = waitErr
		}
		return summary, waitErr
	}

	summary.Ended = time.Now().UTC()
	summary.Duration = summary.Ended.Sub(summary.Started)

	if err := finalCountVerify(ctx, writePool, cfg, summary, logger); err != nil {
		summary.Err = err
		return summary, err
	}
	return summary, nil
}

func planJobs(cfg *config.Config) []tenantJob {
	jobs := make([]tenantJob, cfg.Init.Tenants)
	for i := 0; i < cfg.Init.Tenants; i++ {
		jobs[i] = tenantJob{
			idx:      i,
			tenantID: cfg.Init.TenantIDBase + int64(i),
			startID:  cfg.Init.StartID + int64(i)*int64(cfg.Init.RowsPerTenant),
			rowCount: cfg.Init.RowsPerTenant,
		}
	}
	return jobs
}

func probeAndCheck(ctx context.Context, pool *sql.DB, cfg *config.Config) error {
	max, err := db.ProbeMaxAllowedPacket(ctx, pool)
	if err != nil {
		return err
	}
	return config.ValidateInsertBatchBytes(max, cfg)
}

func applySchema(ctx context.Context, pool *sql.DB, cfg *config.Config, logger *slog.Logger) error {
	if !cfg.Init.ForceInit {
		exists, rows, err := tableState(ctx, pool, cfg)
		if err != nil {
			return err
		}
		if exists && rows > 0 {
			return fmt.Errorf(
				"target `%s`.`%s` already has %d rows; set init.force_init=true to drop",
				cfg.Database.Write.Schema, cfg.Database.Write.Table, rows)
		}
	}
	if _, err := pool.ExecContext(ctx, schema.DropDDL()); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	createDDL := schema.CreateDDL()
	if cfg.Init.BulkIndexRebuild {
		createDDL = schema.CreateDDLPKOnly()
	}
	if _, err := pool.ExecContext(ctx, createDDL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	logger.Info("schema applied",
		slog.String("table", schema.Table),
		slog.Bool("pk_only", cfg.Init.BulkIndexRebuild),
	)
	return nil
}

// rebuildSecondaryIndexes is the bulk-load phase 2: with all rows seeded,
// issue ALTER TABLE ADD KEY for each secondary index parsed out of the
// embedded schema. End-state matches the target schema byte-identically.
//
// Runs AFTER all tenant workers complete, BEFORE finalCountVerify so the
// final state is the target schema state.
func rebuildSecondaryIndexes(ctx context.Context, pool *sql.DB, cfg *config.Config,
	logger *slog.Logger) error {

	keys := schema.SecondaryKeys()
	for i, k := range keys {
		stmt := k.AddKeyStmt(cfg.Database.Write.Schema, cfg.Database.Write.Table)
		start := time.Now()
		if _, err := pool.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("rebuild secondary index %d/%d (%s): %w",
				i+1, len(keys), k.Name, err)
		}
		logger.Info("index rebuilt",
			slog.String("event", "index_rebuilt"),
			slog.Int("idx", i+1),
			slog.Int("of", len(keys)),
			slog.String("name", k.Name),
			slog.Duration("elapsed", time.Since(start)),
		)
	}
	return nil
}

func tableState(ctx context.Context, pool *sql.DB, cfg *config.Config) (bool, int64, error) {
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		cfg.Database.Write.Schema, cfg.Database.Write.Table)
	var present int
	if err := pool.QueryRowContext(ctx, q).Scan(&present); err != nil {
		return false, 0, fmt.Errorf("probe table presence: %w", err)
	}
	if present == 0 {
		return false, 0, nil
	}
	var rows int64
	if err := pool.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`.`%s`",
		cfg.Database.Write.Schema, cfg.Database.Write.Table)).Scan(&rows); err != nil {
		return true, 0, fmt.Errorf("count existing rows: %w", err)
	}
	return true, rows, nil
}

func maybeRelax(ctx context.Context, pool *sql.DB, cfg *config.Config, logger *slog.Logger) (map[string]string, error) {
	if !cfg.Init.RelaxDurability {
		return nil, nil
	}
	previous, err := relaxDurability(ctx, pool)
	if err != nil {
		return nil, err
	}
	logger.Info("durability relaxed", slog.Int("vars_changed", len(previous)))
	return previous, nil
}

func finalCountVerify(ctx context.Context, pool *sql.DB, cfg *config.Config,
	summary Summary, logger *slog.Logger) error {

	want := summary.TenantCount * cfg.Init.RowsPerTenant
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`",
		cfg.Database.Write.Schema, cfg.Database.Write.Table)
	var got int
	if err := pool.QueryRowContext(ctx, q).Scan(&got); err != nil {
		return fmt.Errorf("final count: %w", err)
	}
	if got != want {
		return fmt.Errorf("final count mismatch: got %d want %d", got, want)
	}
	logger.Info("init complete",
		slog.String("event", "init_complete"),
		slog.Int("rows", got),
		slog.Float64("rows_per_sec", float64(got)/summary.Duration.Seconds()),
		slog.Duration("elapsed", summary.Duration),
	)
	return nil
}

// Summary captures the per-run totals + per-tenant breakdown emitted to
// artifacts/<run-id>/init.json.
type Summary struct {
	Started     time.Time
	Ended       time.Time
	Duration    time.Duration
	TenantCount int
	Rows        int
	Bytes       int64
	Batches     int
	Tenants     []tenantCounters
	Err         error
}

// ErrPhaseAborted is returned when init aborts early (replication lag,
// context cancellation, etc.). Callers use errors.Is to branch.
var ErrPhaseAborted = errors.New("init phase aborted")
