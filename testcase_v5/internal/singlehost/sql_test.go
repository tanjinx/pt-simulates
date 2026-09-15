package singlehost

import (
	"strings"
	"testing"
)

func TestReadSQL(t *testing.T) {
	got := readSQL("repro_db", "files", 10)
	for _, want := range []string{
		"SELECT * FROM `repro_db`.`files` IGNORE INDEX (`PRIMARY`)",
		"WHERE tenant_id = ? AND id > ?",
		"ORDER BY id ASC LIMIT 10",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("read SQL missing %q: %s", want, got)
		}
	}
}

func TestWriteSQLRequiresDisabledBinlog(t *testing.T) {
	got := writeSQL("repro_db", "files")
	for _, want := range []string{
		"UPDATE `repro_db`.`files`",
		"version = version + 1",
		"tenant_id = ?",
		"id BETWEEN ? AND ?",
		"@@session.sql_log_bin = 0",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("write SQL missing %q: %s", want, got)
		}
	}
}
