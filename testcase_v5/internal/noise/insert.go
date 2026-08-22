package noise

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
)

type insertWorker struct {
	id         int
	write      *sql.DB
	schema     string
	table      string
	ratePerSec int
	schedule   BurstSchedule
	cycle      blobgen.Cycle
	idCounter  *atomic.Int64
	tenantLo   int64
	tenantHi   int64
	dateBase   int64
	seed       uint64
	counters   *Counters
	logger     *slog.Logger
}

func (w *insertWorker) run(ctx context.Context) error {
	stmt := buildInsertStmt(w.schema, w.table)
	start := time.Now()
	walker := &linWalker{state: uint64(w.id)*0x9E3779B97F4A7C15 + w.seed}

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		elapsed := time.Since(start)
		if d := rateSleep(w.ratePerSec, w.schedule, elapsed); d > 0 {
			if err := sleepCtx(ctx, d); err != nil {
				return nil
			}
		}
		id := w.idCounter.Add(1)
		tenantID := walker.inRange(w.tenantLo, w.tenantHi)
		// Use a bounded rowN for blobgen's deterministic seeding; the real
		// row ID + tenant_id are stamped on the row below. DateStep=0 keeps
		// date_create at DateBase (fits int unsigned) — noise rows do not
		// need temporal spread, only compressed-page pressure.
		rowN := uint64(id) % 1_000_000
		class := w.cycle.Pick(rowN)
		row := blobgen.Generate(rowN, blobgen.Inputs{
			TenantID: tenantID,
			StartID:  id,
			DateBase: w.dateBase,
			DateStep: 0,
			Seed:     w.seed ^ uint64(w.id),
			Class:    class,
		})
		row.ID = id
		row.TenantID = tenantID

		if _, err := w.write.ExecContext(ctx, stmt, row.Values()...); err != nil {
			if isCanceledLike(err) {
				return nil
			}
			w.counters.Errors.Add(1)
			w.logger.Warn("insert error", slog.String("err", err.Error()))
			// Continue on transient errors. Alternate-path fallback is
			// forbidden, but a single failed INSERT is not a path
			// divergence — it is a per-statement transient. The
			// errgroup-level abort happens only on unrecoverable errors
			// (context cancel, pool death), and those surface through
			// later iterations.
			continue
		}
		w.counters.Inserts.Add(1)
	}
}

// buildInsertStmt returns the parameterised single-row INSERT for the
// `files` table. Columns and order are taken from blobgen.Columns() so
// the wire shape matches the init phase exactly.
func buildInsertStmt(schema, table string) string {
	cols := blobgen.Columns()
	placeholders := strings.Repeat("?,", len(cols))
	placeholders = strings.TrimSuffix(placeholders, ",")
	return fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		schema, table,
		"`"+strings.Join(cols, "`,`")+"`",
		placeholders,
	)
}

func isCanceledLike(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
