package initphase

import (
	"strings"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
)

// BuildInsert renders the multi-row INSERT statement and the flat parameter
// slice for a batch. The SQL shape is:
//
//	INSERT INTO `<schema>`.`<table>` (col1, col2, ...) VALUES
//	(?,?,?,...), (?,?,?,...), ...
//
// The worker decides when to flush; this function just renders.
func BuildInsert(schema, table string, rows []blobgen.Row) (string, []any) {
	cols := blobgen.Columns()
	placeholders := "(" + strings.Repeat("?,", len(cols)-1) + "?)"

	var sb strings.Builder
	sb.Grow(64 + len(cols)*16 + len(rows)*(len(placeholders)+2))
	sb.WriteString("INSERT INTO `")
	sb.WriteString(schema)
	sb.WriteString("`.`")
	sb.WriteString(table)
	sb.WriteString("` (")
	for i, c := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('`')
		sb.WriteString(c)
		sb.WriteByte('`')
	}
	sb.WriteString(") VALUES ")
	for i := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(placeholders)
	}

	params := make([]any, 0, len(rows)*len(cols))
	for i := range rows {
		params = append(params, rows[i].Values()...)
	}
	return sb.String(), params
}

// RowBytes returns a rough byte estimate of a Row for the
// insert_batch_bytes ceiling. Includes the wire-overhead of BLOBs +
// strings but is not exact (binary protocol framing isn't counted) — that
// is fine for batching decisions because operators set
// insert_batch_bytes well below max_allowed_packet.
func RowBytes(r blobgen.Row) int {
	n := 0
	n += len(r.Contents)
	n += len(r.ContentsHighlight)
	n += len(r.ExternalPtr)
	n += len(r.Metadata)
	n += len(r.UnencryptedMetadata)
	n += len(r.TenantsSharedWith)
	n += len(r.Token) + len(r.PubToken)
	n += len(r.OriginalName) + len(r.StoredName) + len(r.Title) + len(r.Mimetype)
	n += len(r.HighlightType)
	n += len(r.ExternalURL)
	n += len(r.Source) + len(r.ExternalID)
	n += len(r.MD5)
	// Roughly account for 16 integer/bool columns at 8 bytes each.
	n += 16 * 8
	return n
}
