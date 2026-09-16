package runphase

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestSingleHostWriteSQLRequiresDisabledBinlog(t *testing.T) {
	got := singleHostWriteSQL("repro_db", "files")
	for _, want := range []string{
		"UPDATE `repro_db`.`files`",
		"WHERE id = ? AND tenant_id = ?",
		"@@session.sql_log_bin = 0",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("write SQL missing %q: %s", want, got)
		}
	}
}

type fakeSingleHostBinlogConn struct {
	execErr error
	value   int
}

func (c *fakeSingleHostBinlogConn) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, c.execErr
}

func (c *fakeSingleHostBinlogConn) QueryRowContext(context.Context, string, ...any) singleHostRowScanner {
	return fakeSingleHostRow{value: c.value}
}

type fakeSingleHostRow struct {
	value int
}

func (r fakeSingleHostRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = r.value
	return nil
}

func TestDisableSingleHostBinlog(t *testing.T) {
	if err := disableSingleHostBinlog(context.Background(), &fakeSingleHostBinlogConn{value: 0}); err != nil {
		t.Fatalf("disableSingleHostBinlog: %v", err)
	}
}

func TestDisableSingleHostBinlogRejectsEnabledSession(t *testing.T) {
	err := disableSingleHostBinlog(context.Background(), &fakeSingleHostBinlogConn{value: 1})
	if err == nil {
		t.Fatal("disableSingleHostBinlog succeeded with sql_log_bin=1")
	}
}

func TestDisableSingleHostBinlogWrapsSetError(t *testing.T) {
	err := disableSingleHostBinlog(context.Background(), &fakeSingleHostBinlogConn{execErr: errors.New("denied")})
	if err == nil || !strings.Contains(err.Error(), "disable binary logging") {
		t.Fatalf("error = %v", err)
	}
}
