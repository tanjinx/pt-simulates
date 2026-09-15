package singlehost

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	rowScanner interface {
		Scan(...any) error
	}

	binlogConn interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
		QueryRowContext(context.Context, string, ...any) rowScanner
	}

	sqlBinlogConn struct {
		conn *sql.Conn
	}
)

func (c sqlBinlogConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

func (c sqlBinlogConn) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return c.conn.QueryRowContext(ctx, query, args...)
}

func disableBinlog(ctx context.Context, conn binlogConn) error {
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
