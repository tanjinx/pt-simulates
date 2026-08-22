package debugsql

import (
	"log/slog"
	"sync"
)

// Once logs the first SQL statement per phase/kind when enabled
// (debug.log_sql_once).
type Once struct {
	enabled bool
	seen    sync.Map
}

func New(enabled bool) *Once {
	return &Once{enabled: enabled}
}

func (o *Once) Log(logger *slog.Logger, phase, kind, sql string) {
	if o == nil || !o.enabled || logger == nil || sql == "" {
		return
	}
	key := phase + ":" + kind
	if _, loaded := o.seen.LoadOrStore(key, true); loaded {
		return
	}
	logger.Info("sql once",
		slog.String("event", "sql_once"),
		slog.String("phase", phase),
		slog.String("kind", kind),
		slog.String("sql", sql),
	)
}
