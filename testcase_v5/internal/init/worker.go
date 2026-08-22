package initphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/debugsql"
)

// tenantJob describes the rows one worker is responsible for seeding.
type tenantJob struct {
	idx      int   // 0-based index in the tenant list
	tenantID int64 // tenant_id value to write
	startID  int64 // first row's id (StartID + idx * RowsPerTenant)
	rowCount int
}

// tenantCounters records the per-tenant timing/throughput numbers the summary
// table consumes.
type tenantCounters struct {
	tenantID int64
	rows     int
	bytes    int64
	batches  int
	duration time.Duration
}

// seedTenant runs one tenant's seeding pass: generate, batch, INSERT, count-
// verify. db is the shared write pool. logger has component=init bound.
func seedTenant(ctx context.Context, db *sql.DB, cfg *config.Config, cycle blobgen.Cycle,
	job tenantJob, sqlOnce *debugsql.Once, logger *slog.Logger) (tenantCounters, error) {

	tc := tenantCounters{tenantID: job.tenantID}
	start := time.Now()
	tenantLogger := logger.With(
		slog.Int64("tenant_id", job.tenantID),
		slog.Int("tenant_idx", job.idx),
		slog.Int("rows_target", job.rowCount),
	)
	tenantLogger.Info("tenant start", slog.String("event", "tenant_start"))

	in := blobgen.Inputs{
		TenantID: job.tenantID,
		StartID:  job.startID,
		DateBase: cfg.Init.DateBase,
		DateStep: cfg.Init.DateStep,
		Seed:     cfg.Init.Seed + uint64(job.idx), // v4 mirror
	}

	buf := make([]blobgen.Row, 0, cfg.Init.InsertBatchRows)
	var bufBytes int
	batchIdx := 0

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		stmt, params := BuildInsert(
			cfg.Database.Write.Schema, cfg.Database.Write.Table, buf)
		sqlOnce.Log(logger, "init", "insert", stmt)
		if _, err := db.ExecContext(ctx, stmt, params...); err != nil {
			return fmt.Errorf("tenant %d batch %d insert: %w", job.tenantID, batchIdx, err)
		}
		batchIdx++
		tc.batches = batchIdx
		tc.rows += len(buf)
		tc.bytes += int64(bufBytes)
		tenantLogger.Info("batch flushed",
			slog.Int("batch", batchIdx),
			slog.Int("rows_in_batch", len(buf)),
			slog.Int("bytes_in_batch", bufBytes),
			slog.Int("rows_seeded", tc.rows),
		)
		buf = buf[:0]
		bufBytes = 0
		return nil
	}

	progressEvery := max(1, cfg.Init.TenantProgressRows)
	nextProgress := progressEvery

	for rowN := uint64(0); rowN < uint64(job.rowCount); rowN++ {
		if err := ctx.Err(); err != nil {
			return tc, err
		}
		in.Class = cycle.Pick(rowN)
		row := blobgen.Generate(rowN, in)
		rowBytes := RowBytes(row)
		if len(buf) > 0 &&
			(len(buf) >= cfg.Init.InsertBatchRows ||
				bufBytes+rowBytes > cfg.Init.InsertBatchBytes) {
			if err := flush(); err != nil {
				return tc, err
			}
		}
		buf = append(buf, row)
		bufBytes += rowBytes
		if tc.rows+len(buf) >= nextProgress {
			tenantLogger.Info("tenant progress",
				slog.Int("rows_generated", tc.rows+len(buf)),
				slog.Int("rows_target", job.rowCount),
			)
			nextProgress += progressEvery
		}
	}
	if err := flush(); err != nil {
		return tc, err
	}

	// Count-verify the seeded rows for this tenant.
	if err := verifyTenantCount(ctx, db, cfg, job, tenantLogger); err != nil {
		return tc, err
	}
	tc.duration = time.Since(start)
	tenantLogger.Info("tenant complete",
		slog.String("event", "tenant_complete"),
		slog.Int("rows", tc.rows),
		slog.Int64("bytes", tc.bytes),
		slog.Int("batches", tc.batches),
		slog.Float64("rows_per_sec", float64(tc.rows)/tc.duration.Seconds()),
		slog.Duration("elapsed", tc.duration),
	)
	return tc, nil
}

func verifyTenantCount(ctx context.Context, db *sql.DB, cfg *config.Config,
	job tenantJob, logger *slog.Logger) error {

	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE tenant_id = ?",
		cfg.Database.Write.Schema, cfg.Database.Write.Table)
	var got int
	if err := db.QueryRowContext(ctx, q, job.tenantID).Scan(&got); err != nil {
		return fmt.Errorf("count verify tenant %d: %w", job.tenantID, err)
	}
	if got != job.rowCount {
		return fmt.Errorf(
			"count verify tenant %d: got %d want %d", job.tenantID, got, job.rowCount)
	}
	logger.Info("tenant count verified", slog.Int("count", got))
	return nil
}
