package debugsql

import (
	"bytes"
	"log/slog"
	"testing"
)

func TestOnceLogsEachKindOnce(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	o := New(true)

	o.Log(logger, "init", "insert", "INSERT INTO t VALUES (?)")
	o.Log(logger, "init", "insert", "INSERT INTO t VALUES (?)")
	o.Log(logger, "init", "select", "SELECT 1")

	out := buf.String()
	if n := bytes.Count([]byte(out), []byte("sql_once")); n != 2 {
		t.Fatalf("expected 2 sql_once logs, got %d in %q", n, out)
	}
}

func TestOnceDisabled(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	o := New(false)
	o.Log(logger, "init", "insert", "INSERT INTO t VALUES (?)")
	if buf.Len() != 0 {
		t.Fatalf("expected no logs when disabled, got %q", buf.String())
	}
}
