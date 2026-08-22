package runphase

import (
	"context"
	"database/sql"
	"log/slog"
)

// logExplainOnce runs EXPLAIN on the per-tenant SELECT and writes one
// structured record. The plan is expected to use tenant_id_4; we just
// log what the optimizer chose and let the operator confirm — failing the
// run on an unexpected index would mask a real diagnostic signal.
func logExplainOnce(ctx context.Context, db *sql.DB, sel string,
	tenantID int64, logger *slog.Logger) error {

	rs, err := db.QueryContext(ctx, "EXPLAIN "+sel, tenantID, int64(0))
	if err != nil {
		return err
	}
	defer rs.Close()

	cols, err := rs.Columns()
	if err != nil {
		return err
	}
	row := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range row {
		ptrs[i] = &row[i]
	}
	for rs.Next() {
		if err := rs.Scan(ptrs...); err != nil {
			return err
		}
		attrs := make([]slog.Attr, 0, len(cols)+1)
		attrs = append(attrs, slog.String("event", "explain"))
		for i, c := range cols {
			v := ""
			if row[i].Valid {
				v = row[i].String
			}
			attrs = append(attrs, slog.String(c, v))
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "EXPLAIN", attrs...)
	}
	return rs.Err()
}
