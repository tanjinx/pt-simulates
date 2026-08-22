package runphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/debugsql"
)

// tenantRowResult is the minimal scan-target we pull out of the SELECT — we
// only need the three plaintext blobs + id + tenant_id + cursor field. The
// remaining 37 columns are pulled to match the v4 wire shape but discarded.
type tenantRowResult struct {
	id                int64
	tenantID          int64
	dateCreate        int64
	contents          []byte
	contentsHighlight []byte
	metadata          string
}

// scanTargets returns 43 pointers in the same order as blobgen.Columns().
// Throwaway columns land in sinks; the six we keep land in r.
func (r *tenantRowResult) scanTargets(sink *rowSinks) []any {
	return []any{
		&r.id,                     // id
		&r.tenantID,               // tenant_id
		&sink.userID,              // user_id
		&r.dateCreate,             // date_create
		&sink.token, &sink.pubSec, // token, pub_token
		&sink.size, &sink.isStored, // size, is_stored
		&sink.origName, &sink.storedName, &sink.title, &sink.mimetype,
		&sink.parentID,
		&r.contents, &sink.contentsENC,
		&r.contentsHighlight, &sink.contentsHighlightENC,
		&sink.highlightType,
		&r.metadata, &sink.metadataENC,
		&sink.externalURL,
		&sink.isDeleted, &sink.dateDelete,
		&sink.isPublic, &sink.pubShared,
		&sink.lastIndexed,
		&sink.source, &sink.externalID,
		&sink.serviceTypeID, &sink.serviceID,
		&sink.isMultitenant, &sink.originalTenantID, &sink.tenantsSharedWith,
		&sink.serviceTenantID,
		&sink.isTombstoned, &sink.thumbnailVersion, &sink.dateThumbnailRetrieved,
		&sink.externalPtr,
		&sink.md5,
		&sink.metadataVersion, &sink.unencryptedMetadata,
		&sink.restrictionType, &sink.version,
	}
}

// rowSinks holds throwaway destinations for the 37 columns we read on the
// wire but do not use in the update. SQL NULLs need NullX types.
type rowSinks struct {
	userID                                                 sql.NullInt64
	token, pubSec                                          sql.NullString
	size                                                   sql.NullInt64
	isStored                                               sql.NullInt64
	origName, storedName, title, mimetype                  sql.NullString
	parentID                                               sql.NullInt64
	contentsENC, contentsHighlightENC                      []byte
	highlightType                                          sql.NullString
	metadataENC                                            []byte
	externalURL                                            sql.NullString
	isDeleted                                              sql.NullInt64
	dateDelete                                             sql.NullInt64
	isPublic, pubShared                                    sql.NullInt64
	lastIndexed                                            sql.NullInt64
	source, externalID                                     sql.NullString
	serviceTypeID, serviceID                               sql.NullInt64
	isMultitenant, originalTenantID                        sql.NullInt64
	tenantsSharedWith                                      sql.NullString
	serviceTenantID                                        sql.NullInt64
	isTombstoned, thumbnailVersion, dateThumbnailRetrieved sql.NullInt64
	externalPtr                                            []byte
	md5                                                    sql.NullString
	metadataVersion                                        sql.NullInt64
	unencryptedMetadata                                    sql.NullString
	restrictionType, version                               sql.NullInt64
}

// tenantSummary captures one tenant's run-phase totals for the artifact table.
type tenantSummary struct {
	tenantID int64
	workerID int
	rows     int
	batches  int
	selects  int
	updates  int
	elapsed  time.Duration
}

// processTenant drives the inner loop for one tenant. read and write
// are the shared pools; ctx cancellation aborts the loop between batches.
// First batch logs EXPLAIN once and the per-batch timings once (then the
// regular tenant_log_interval cadence takes over).
func processTenant(ctx context.Context, read, write *sql.DB, cfg *config.Config,
	tenantID int64, workerID int, sqlOnce *debugsql.Once, logger *slog.Logger) (tenantSummary, error) {

	tenantLogger := logger.With(
		slog.Int64("tenant_id", tenantID),
		slog.Int("worker_id", workerID),
	)
	sum := tenantSummary{tenantID: tenantID, workerID: workerID}
	start := time.Now()
	tenantLogger.Info("tenant start", slog.String("event", "tenant_start"))

	sel := selectSQL(cfg.Database.Read.Schema, cfg.Database.Read.Table, cfg.Run.BatchSize)
	upd := updateSQL(cfg.Database.Write.Schema, cfg.Database.Write.Table)

	loggedExplain := false
	var cursor int64
	logEvery := max(1, cfg.Run.TenantLogIntervalBatches)
	loggedFirstBatch := false

	for {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		if !loggedExplain {
			sqlOnce.Log(tenantLogger, "run", "select", sel)
			if err := logExplainOnce(ctx, read, sel, tenantID, tenantLogger); err != nil {
				return sum, fmt.Errorf("explain tenant %d: %w", tenantID, err)
			}
			loggedExplain = true
		}
		selStart := time.Now()
		rows, err := fetchBatch(ctx, read, sel, tenantID, cursor)
		if err != nil {
			return sum, fmt.Errorf("select tenant %d: %w", tenantID, err)
		}
		selMs := time.Since(selStart).Milliseconds()
		sum.selects++
		if len(rows) == 0 {
			break
		}
		cursor = rows[len(rows)-1].dateCreate

		encStart := time.Now()
		updates := buildENCUpdates(rows)
		encMs := time.Since(encStart).Milliseconds()

		updStart := time.Now()
		if err := applyUpdates(ctx, write, upd, updates, sqlOnce, tenantLogger); err != nil {
			return sum, err
		}
		updMs := time.Since(updStart).Milliseconds()

		sum.rows += len(rows)
		sum.batches++
		sum.updates += len(rows)

		if !loggedFirstBatch || sum.batches%logEvery == 0 {
			tenantLogger.Info("batch",
				slog.Int("batch", sum.batches),
				slog.Int("rows_in_batch", len(rows)),
				slog.Int64("cursor", cursor),
				slog.Int64("select_ms", selMs),
				slog.Int64("encrypt_ms", encMs),
				slog.Int64("update_ms", updMs),
			)
			loggedFirstBatch = true
		}
	}
	sum.elapsed = time.Since(start)
	tenantLogger.Info("tenant complete",
		slog.String("event", "tenant_complete"),
		slog.Int("rows", sum.rows),
		slog.Int("batches", sum.batches),
		slog.Duration("elapsed", sum.elapsed),
	)
	return sum, nil
}

func fetchBatch(ctx context.Context, db *sql.DB, sel string,
	tenantID, cursor int64) ([]tenantRowResult, error) {

	rs, err := db.QueryContext(ctx, sel, tenantID, cursor)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var out []tenantRowResult
	sink := &rowSinks{}
	for rs.Next() {
		var r tenantRowResult
		if err := rs.Scan(r.scanTargets(sink)...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type updatePlan struct {
	id, tenantID                           int64
	contentsENC, highlightENC, metadataENC []byte
}

func buildENCUpdates(rows []tenantRowResult) []updatePlan {
	out := make([]updatePlan, len(rows))
	for i, r := range rows {
		out[i] = updatePlan{
			id:           r.id,
			tenantID:     r.tenantID,
			contentsENC:  encryptBlob(r.contents, contentsENCLabel(r.id)),
			highlightENC: encryptBlob(r.contentsHighlight, highlightENCLabel(r.id)),
			metadataENC:  encryptText(r.metadata, metadataENCLabel(r.id)),
		}
	}
	return out
}

func applyUpdates(ctx context.Context, db *sql.DB, stmt string,
	plans []updatePlan, sqlOnce *debugsql.Once, logger *slog.Logger) error {

	for _, p := range plans {
		sqlOnce.Log(logger, "run", "update", stmt)
		if _, err := db.ExecContext(ctx, stmt,
			p.contentsENC, p.highlightENC, p.metadataENC,
			p.id, p.tenantID); err != nil {
			return fmt.Errorf("update id=%d tenant=%d: %w", p.id, p.tenantID, err)
		}
	}
	return nil
}
