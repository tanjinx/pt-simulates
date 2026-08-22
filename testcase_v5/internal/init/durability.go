package initphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// dynamicGlobalVars enumerates the MySQL GLOBAL variables init may relax
// for throughput. innodb_doublewrite is only touched when
// cfg.Init.RelaxDoublewrite=true. Both flush-log and sync-binlog are
// touched when cfg.Init.RelaxDurability=true.
//
// Note: although these variables are sometimes set per session, in
// MySQL 8.0 they are GLOBAL-scope only, so this uses SET GLOBAL to
// match the v4 harness. Documented in README.md "Notable deviations".
const (
	varFlushAtCommit = "innodb_flush_log_at_trx_commit"
	varSyncBinlog    = "sync_binlog"
)

// relaxDurability switches the relevant GLOBAL knobs to their bulk-load
// values and returns the previous values for the deferred restore.
// innodb_doublewrite is intentionally NOT included — MySQL 8.0 forbids
// flipping it to OFF on a running server; the operator must set
// innodb_doublewrite=OFF in my.cnf and restart before init.
func relaxDurability(ctx context.Context, db *sql.DB) (map[string]string, error) {
	desired := map[string]string{
		varFlushAtCommit: "2",
		varSyncBinlog:    "0",
	}
	previous := map[string]string{}
	for v, want := range desired {
		current, err := readVar(ctx, db, v)
		if err != nil {
			return nil, err
		}
		if current == want {
			continue
		}
		previous[v] = current
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL %s = %s", v, want)); err != nil {
			return nil, fmt.Errorf("set global %s: %w", v, err)
		}
	}
	return previous, nil
}

// restoreDurability undoes relaxDurability.
func restoreDurability(ctx context.Context, db *sql.DB, previous map[string]string) error {
	for v, val := range previous {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL %s = %s", v, val)); err != nil {
			return fmt.Errorf("restore %s: %w", v, err)
		}
	}
	return nil
}

// disableRedoLog issues ALTER INSTANCE DISABLE INNODB REDO_LOG (MySQL
// 8.0.21+). This is INSTANCE-scope and survives the connection; the
// deferred enableRedoLog restores it. The privilege required is
// INNODB_REDO_LOG_ENABLE.
//
// SAFETY: a server crash while redo log is disabled leaves InnoDB
// unrecoverable. Only enable on throw-away test clusters.
func disableRedoLog(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if _, err := db.ExecContext(ctx, "ALTER INSTANCE DISABLE INNODB REDO_LOG"); err != nil {
		return fmt.Errorf("ALTER INSTANCE DISABLE INNODB REDO_LOG: %w", err)
	}
	logger.Warn("redo log disabled",
		slog.String("event", "redo_log_disabled"),
		slog.String("safety", "server crash while disabled = unrecoverable; rerun init"),
	)
	return nil
}

// enableRedoLog re-engages the redo log. Best-effort during deferred
// cleanup — error logged but does not stop the orchestrator's normal
// completion.
func enableRedoLog(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if _, err := db.ExecContext(ctx, "ALTER INSTANCE ENABLE INNODB REDO_LOG"); err != nil {
		return fmt.Errorf("ALTER INSTANCE ENABLE INNODB REDO_LOG: %w", err)
	}
	logger.Info("redo log re-enabled", slog.String("event", "redo_log_enabled"))
	return nil
}

func readVar(ctx context.Context, db *sql.DB, name string) (string, error) {
	// SHOW VARIABLES LIKE does not accept prepared placeholders under the
	// binary protocol (interpolateParams=false). Names come from the
	// hardcoded list above — pure identifiers, no injection risk.
	stmt := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", name)
	row := db.QueryRowContext(ctx, stmt)
	var k, v string
	if err := row.Scan(&k, &v); err != nil {
		return "", fmt.Errorf("read variable %s: %w", name, err)
	}
	return v, nil
}
