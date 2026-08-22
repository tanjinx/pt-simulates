package initphase

import (
	"strings"
	"testing"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
)

func makeRow(seed uint64, rowN uint64, class blobgen.SizeClass) blobgen.Row {
	return blobgen.Generate(rowN, blobgen.Inputs{
		TenantID: 1, StartID: 1, DateBase: 1, DateStep: 1, Seed: seed, Class: class,
	})
}

func TestBuildInsertShape(t *testing.T) {
	rows := []blobgen.Row{makeRow(1, 0, blobgen.Small), makeRow(1, 1, blobgen.Small)}
	stmt, params := BuildInsert("repro_db", "files", rows)

	if !strings.Contains(stmt, "INSERT INTO `repro_db`.`files`") {
		t.Errorf("expected qualified table; got %q", stmt[:80])
	}
	if !strings.Contains(stmt, ") VALUES (") {
		t.Errorf("expected VALUES clause: %s", stmt[:120])
	}

	tuples := strings.Count(stmt, "(?,")
	if tuples != len(rows)+1 {
		// +1 because the column list also starts with "(?," shape... actually
		// only VALUES tuples start that way. Re-count via "?)":
		closeCount := strings.Count(stmt, "?)")
		if closeCount != len(rows) {
			t.Fatalf("expected %d row tuples, counted %d close-paren placeholder matches", len(rows), closeCount)
		}
	}

	if got, want := len(params), len(rows)*len(blobgen.Columns()); got != want {
		t.Fatalf("params=%d want %d (rows=%d cols=%d)",
			got, want, len(rows), len(blobgen.Columns()))
	}
}

func TestBuildInsertSingleRow(t *testing.T) {
	rows := []blobgen.Row{makeRow(1, 0, blobgen.Small)}
	stmt, _ := BuildInsert("s", "t", rows)
	if strings.Contains(stmt, "), (") {
		t.Errorf("single-row INSERT must have no row separator: %s", stmt)
	}
}

func TestRowBytesIncludesAllBlobs(t *testing.T) {
	row := makeRow(1, 0, blobgen.Small)
	got := RowBytes(row)
	min := len(row.Contents) + len(row.ContentsHighlight) + len(row.ExternalPtr)
	if got <= min {
		t.Fatalf("RowBytes (%d) should exceed the sum of three BLOBs (%d)", got, min)
	}
}
