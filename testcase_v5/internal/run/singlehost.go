package runphase

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

type (
	singleHostRowScanner interface {
		Scan(...any) error
	}

	singleHostBinlogConn interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
		QueryRowContext(context.Context, string, ...any) singleHostRowScanner
	}

	singleHostSQLConn struct {
		conn *sql.Conn
	}

	singleHostReadSpec struct {
		kind        string
		query       string
		mrr         bool
		count       int
		externalIDs []string
	}
)

func (c singleHostSQLConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

func (c singleHostSQLConn) QueryRowContext(ctx context.Context, query string, args ...any) singleHostRowScanner {
	return c.conn.QueryRowContext(ctx, query, args...)
}

func disableSingleHostBinlog(ctx context.Context, conn singleHostBinlogConn) error {
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin = 0"); err != nil {
		return fmt.Errorf("disable binary logging: %w", err)
	}
	var enabled int
	if err := conn.QueryRowContext(ctx, "SELECT @@session.sql_log_bin").Scan(&enabled); err != nil {
		return fmt.Errorf("verify binary logging: %w", err)
	}
	if enabled != 0 {
		return fmt.Errorf("verify binary logging: sql_log_bin=%d, want 0", enabled)
	}
	return nil
}

func singleHostWriteSQL(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET contents_enc = ?, contents_highlight_enc = ?, "+
			"metadata_enc = ?, version = version + 1 "+
			"WHERE id = ? AND tenant_id = ? AND @@session.sql_log_bin = 0",
		schema, table)
}

func singleHostBackfillSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE tenant_id = ? AND date_create > ? "+
			"AND id BETWEEN ? AND ? ORDER BY date_create ASC, id ASC LIMIT %d",
		columnList(), schema, table, batchSize)
}

func orchestrateSingleHost(ctx context.Context, readPool, writePool *sql.DB,
	cfg *config.Config) (Summary, error) {
	summary := Summary{Started: time.Now().UTC(), TenantCount: 1}
	s := cfg.Run.SingleHost
	schema, table := cfg.Database.Read.Schema, cfg.Database.Read.Table
	rows := cfg.Run.SearchMix.RowsPerQuery
	if rows < 1 {
		rows = cfg.Run.BatchSize
	}
	externalIDs := s.ExternalIDs
	if len(externalIDs) > rows {
		externalIDs = externalIDs[:rows]
	}
	rangeWorkers := 0
	mrrWorkers := 0
	forwardWorkers := 0
	if cfg.Run.SearchMix.Enabled {
		rangeWorkers = cfg.Run.SearchMix.RangeEstimateWorkers
		mrrWorkers = cfg.Run.SearchMix.MRRWorkers
		forwardWorkers = cfg.Run.SearchMix.ForwardRefScanWorkers
	}

	specs := []singleHostReadSpec{
		{kind: "backfill", query: singleHostBackfillSQL(schema, table, cfg.Run.BatchSize), count: s.BackfillReadWorkers},
		{kind: "range_estimate", query: rangeEstimateSQL(schema, table, rows), count: rangeWorkers},
		{kind: "mrr", query: mrrSQL(schema, table, len(externalIDs)), mrr: true, count: mrrWorkers, externalIDs: externalIDs},
		{kind: "forward_refscan", query: forwardRefScanSQL(schema, table, rows), count: forwardWorkers},
	}

	var selects atomic.Int64
	var updates atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for _, spec := range specs {
		for worker := 1; worker <= spec.count; worker++ {
			spec := spec
			worker := worker
			g.Go(func() error {
				return runSingleHostReader(gctx, readPool, cfg, spec, worker, &selects)
			})
		}
	}
	for worker := 1; worker <= s.WriteWorkers; worker++ {
		worker := worker
		g.Go(func() error {
			return runSingleHostWriter(gctx, writePool, cfg, worker, &updates)
		})
	}

	err := g.Wait()
	summary.Selects = int(selects.Load())
	summary.Updates = int(updates.Load())
	summary.Rows = summary.Updates
	summary.Batches = summary.Updates
	summary.Ended = time.Now().UTC()
	summary.Duration = summary.Ended.Sub(summary.Started)
	summary.Err = err
	return summary, err
}

func runSingleHostReader(ctx context.Context, pool *sql.DB, cfg *config.Config,
	spec singleHostReadSpec, worker int, selects *atomic.Int64) error {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s reader %d connection: %w", spec.kind, worker, err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET SESSION transaction_isolation = 'REPEATABLE-READ'"); err != nil {
		return fmt.Errorf("%s reader %d isolation: %w", spec.kind, worker, err)
	}
	if spec.mrr {
		if _, err := conn.ExecContext(ctx, "SET SESSION optimizer_switch = 'mrr=on,mrr_cost_based=off'"); err != nil {
			return fmt.Errorf("%s reader %d optimizer: %w", spec.kind, worker, err)
		}
	}

	s := cfg.Run.SingleHost
	span := s.IDMax - s.IDMin + 1
	for iteration := 0; iteration < s.ReadIterations; iteration++ {
		var args []any
		switch spec.kind {
		case "backfill":
			args = []any{s.TenantID, s.DateCreateMin, s.IDMin, s.IDMax}
		case "range_estimate":
			args = []any{s.TenantID, 0, 0, s.DateCreateMin, s.DateCreateMax}
		case "mrr":
			args = []any{s.TenantID, 0}
			for _, externalID := range spec.externalIDs {
				args = append(args, externalID)
			}
		default:
			cursor := s.IDMin
			if span > 1 {
				cursor += int64(iteration) % span
			}
			args = []any{s.TenantID, cursor}
		}
		if err := runOneSearch(ctx, conn, spec.query, args); err != nil {
			return fmt.Errorf("%s reader %d iteration %d: %w", spec.kind, worker, iteration+1, err)
		}
		selects.Add(1)
	}
	return nil
}

func runSingleHostWriter(ctx context.Context, pool *sql.DB, cfg *config.Config,
	worker int, updates *atomic.Int64) error {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("writer %d connection: %w", worker, err)
	}
	defer conn.Close()

	if err := disableSingleHostBinlog(ctx, singleHostSQLConn{conn: conn}); err != nil {
		return fmt.Errorf("writer %d: %w", worker, err)
	}
	s := cfg.Run.SingleHost
	span := s.IDMax - s.IDMin + 1
	query := singleHostWriteSQL(cfg.Database.Write.Schema, cfg.Database.Write.Table)
	for iteration := 0; iteration < s.WriteIterations; iteration++ {
		id := s.IDMin + (int64(worker-1+s.WriteWorkers*iteration) % span)
		_, err := conn.ExecContext(ctx, query,
			singleHostHash(id, 1, iteration),
			singleHostHash(id, 2, iteration),
			singleHostHash(id, 3, iteration),
			id, s.TenantID,
		)
		if err != nil {
			return fmt.Errorf("writer %d iteration %d: %w", worker, iteration+1, err)
		}
		updates.Add(1)
	}
	return nil
}

func singleHostHash(id int64, label byte, iteration int) []byte {
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%d:%d", id, label, iteration))
	return sum[:]
}
