package repllag

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrLagExceeded is returned when the replica is too far behind. The
// orchestrator's errgroup propagates this up to abort the phase.
var ErrLagExceeded = errors.New("replication lag exceeded threshold")

// Sample reads SHOW REPLICA STATUS once and reports lag/IO/SQL state.
func Sample(ctx context.Context, db *sql.DB) (Status, error) {
	rs, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return Status{}, fmt.Errorf("show replica status: %w", err)
	}
	defer rs.Close()

	cols, err := rs.Columns()
	if err != nil {
		return Status{}, err
	}
	if !rs.Next() {
		return Status{}, errors.New("SHOW REPLICA STATUS returned no rows; replica not configured?")
	}
	row := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range row {
		ptrs[i] = &row[i]
	}
	if err := rs.Scan(ptrs...); err != nil {
		return Status{}, fmt.Errorf("scan replica status: %w", err)
	}
	st := Status{Fields: make(map[string]string, len(cols))}
	for i, c := range cols {
		if row[i].Valid {
			st.Fields[c] = row[i].String
		}
	}
	st.IORunning = st.Fields["Replica_IO_Running"]
	st.SQLRunning = st.Fields["Replica_SQL_Running"]
	if v := st.Fields["Seconds_Behind_Source"]; v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			st.LagSeconds = n
		}
	}
	return st, nil
}

// Status is the cleaned-up subset of SHOW REPLICA STATUS we depend on plus
// the raw key/value map for completeness in the artifact log.
type Status struct {
	LagSeconds int64
	IORunning  string
	SQLRunning string
	Fields     map[string]string
}

// Healthy reports whether IO and SQL threads are both running with no
// trailing error.
func (s Status) Healthy() bool {
	if s.IORunning != "Yes" || s.SQLRunning != "Yes" {
		return false
	}
	if s.Fields["Last_IO_Error"] != "" || s.Fields["Last_SQL_Error"] != "" {
		return false
	}
	return true
}

// Watch loops Sample on an interval until ctx is cancelled. If lagCeiling > 0
// and the observed lag exceeds it, Watch returns ErrLagExceeded immediately
// (no warn-and-continue).
func Watch(ctx context.Context, db *sql.DB, interval time.Duration,
	lagCeiling int64, logger *slog.Logger) error {

	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			st, err := Sample(ctx, db)
			if err != nil {
				logger.Error("repl sample failed",
					slog.String("event", "repl_sample_err"),
					slog.String("err", err.Error()),
				)
				continue
			}
			logger.Info("repl sample",
				slog.String("event", "repl_sample"),
				slog.Int64("lag_seconds", st.LagSeconds),
				slog.String("io_running", st.IORunning),
				slog.String("sql_running", st.SQLRunning),
			)
			if lagCeiling > 0 && st.LagSeconds > lagCeiling {
				return fmt.Errorf("%w: %d > %d", ErrLagExceeded, st.LagSeconds, lagCeiling)
			}
		}
	}
}
