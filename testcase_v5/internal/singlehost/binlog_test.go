package singlehost

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type fakeBinlogConn struct {
	execErr error
	value   int
}

func (c *fakeBinlogConn) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, c.execErr
}

func (c *fakeBinlogConn) QueryRowContext(context.Context, string, ...any) rowScanner {
	return fakeRow{value: c.value}
}

type fakeRow struct {
	value int
}

func (r fakeRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = r.value
	return nil
}

func TestDisableBinlog(t *testing.T) {
	if err := disableBinlog(context.Background(), &fakeBinlogConn{value: 0}); err != nil {
		t.Fatalf("disableBinlog: %v", err)
	}
}

func TestDisableBinlogRejectsEnabledSession(t *testing.T) {
	err := disableBinlog(context.Background(), &fakeBinlogConn{value: 1})
	if err == nil {
		t.Fatal("disableBinlog succeeded with sql_log_bin=1")
	}
}

func TestDisableBinlogWrapsSetError(t *testing.T) {
	err := disableBinlog(context.Background(), &fakeBinlogConn{execErr: errors.New("denied")})
	if err == nil || !strings.Contains(err.Error(), "disable binary logging") {
		t.Fatalf("error = %v", err)
	}
}
