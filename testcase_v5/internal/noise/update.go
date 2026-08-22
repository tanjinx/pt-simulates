package noise

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

type updateWorker struct {
	id               int
	write            *sql.DB
	schema           string
	table            string
	ratePerSec       int
	schedule         BurstSchedule
	tenantLo         int64
	tenantHi         int64
	startID          int64
	rowsPerTenant    int64
	hotRowsPerTenant int64 // 0 = full tenant range
	counters         *Counters
	logger           *slog.Logger
}

func (w *updateWorker) run(ctx context.Context) error {
	stmt := buildUpdateStmt(w.schema, w.table)
	start := time.Now()
	walker := &linWalker{state: uint64(w.id)*0xBF58476D1CE4E5B9 + uint64(w.startID)}

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
		// Pick (tenant_id, id) coherently from v5's seeding scheme:
		// tenant N (where N = tenant_id - tenantLo) seeds row IDs in
		// [startID + N*rowsPerTenant, startID + (N+1)*rowsPerTenant).
		tenantID := walker.inRange(w.tenantLo, w.tenantHi)
		tenantOffset := tenantID - w.tenantLo
		idLo := w.startID + tenantOffset*w.rowsPerTenant
		span := w.rowsPerTenant
		if w.hotRowsPerTenant > 0 && w.hotRowsPerTenant < span {
			span = w.hotRowsPerTenant
		}
		idHi := idLo + span - 1
		id := walker.inRange(idLo, idHi)

		// Recompression-pressure payload: small but non-empty SHA-256-derived
		// blobs (same shape as the backfill loop's UPDATE: writes the three _enc
		// columns + bumps version).
		enc1 := sha256Bytes(id, 1)
		enc2 := sha256Bytes(id, 2)
		enc3 := sha256Bytes(id, 3)

		if _, err := w.write.ExecContext(ctx, stmt, enc1, enc2, enc3, id, tenantID); err != nil {
			if isCanceledLike(err) {
				return nil
			}
			w.counters.Errors.Add(1)
			w.logger.Warn("update error", slog.String("err", err.Error()))
			continue
		}
		w.counters.Updates.Add(1)
	}
}

// buildUpdateStmt matches the backfill run-phase UPDATE shape so the
// replica sees the same statement family v5 already replicates: three _enc
// columns + version bump.
func buildUpdateStmt(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET contents_enc=?, contents_highlight_enc=?, "+
			"metadata_enc=?, version=version+1 WHERE id=? AND tenant_id=?",
		schema, table,
	)
}

func sha256Bytes(seed int64, label byte) []byte {
	h := sha256.New()
	h.Write([]byte{label})
	for i := 0; i < 8; i++ {
		h.Write([]byte{byte(seed >> (8 * i))})
	}
	return h.Sum(nil)
}
