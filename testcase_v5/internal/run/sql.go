package runphase

import (
	"fmt"
	"strings"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
)

// columnList renders the 43 backtick-quoted column names in blobgen.Columns()
// order, comma-separated. Shared by every read shape so the SELECT projection
// stays byte-identical to the v4 wire shape.
func columnList() string {
	cols := blobgen.Columns()
	var sb strings.Builder
	for i, c := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('`')
		sb.WriteString(c)
		sb.WriteByte('`')
	}
	return sb.String()
}

// selectSQL renders the 43-column SELECT statement from the v4 read shape.
// Order matches blobgen.Columns(); the LIMIT is the mandated
// run.batch_size==10 (config validator enforces). This is the frozen backfill
// loop shape and MUST NOT change (default-off parity guard).
func selectSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE tenant_id = ? AND date_create > ? "+
			"ORDER BY date_create ASC, id ASC LIMIT %d",
		columnList(), schema, table, batchSize)
}

// rangeEstimateSQL drives the optimizer-side crash caller. Predicates that
// several secondary indexes (tenant_id_2/3/6/7) can
// serve force the optimizer to run records_in_range ->
// btr_estimate_n_rows_in_range_low across candidate indexes at plan time.
// Bind order: tenant_id, is_deleted, is_public, date_create_lo, date_create_hi.
func rangeEstimateSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE tenant_id = ? AND is_deleted = ? "+
			"AND is_public = ? AND date_create BETWEEN ? AND ? LIMIT %d",
		columnList(), schema, table, batchSize)
}

// mrrSQL drives the MRR execution caller. A multi-value external_id IN-list
// scoped by (tenant_id, is_deleted) resolves through the tenant_id_5 secondary
// index then fetches the clustered rows — planned "Using MRR" when the searcher
// sets optimizer_switch='mrr=on,mrr_cost_based=off' on its session. Bind order:
// tenant_id, is_deleted, then inCount external_id values.
func mrrSQL(schema, table string, inCount int) string {
	if inCount < 1 {
		inCount = 1
	}
	ph := strings.TrimSuffix(strings.Repeat("?, ", inCount), ", ")
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE tenant_id = ? AND is_deleted = ? "+
			"AND `external_id` IN (%s)",
		columnList(), schema, table, ph)
}

// forwardRefScanSQL drives the DOMINANT observed crash caller (the canonical
// backfill query shape). A forward
// `tenant_id = const` ref scan whose `IGNORE INDEX(PRIMARY)` pushes the optimizer
// off the clustered PK onto a tenant_id secondary (tenant_id_4 family); the `id > ?`
// keyset cursor + `ORDER BY id ASC` then forces a filesort because no tenant_id
// secondary is id-ordered. The 43-column projection forces a clustered fetch per
// row, so the B-tree descent S-latches the clustered-index ROOT (page 4,
// index_name=PRIMARY) — the read path identical across all observed
// execution-ref crashes, via ha_index_next_same / RefIterator. Bind order:
// tenant_id, id_cursor.
//
// NOTE: faithful to the recovered query, which uses IGNORE INDEX(PRIMARY) only.
// If a local EXPLAIN check shows the optimizer choosing a full scan rather
// than a tenant_id secondary, add FORCE INDEX(tenant_id_4) here to pin the documented
// index — the InnoDB descent is unchanged either way.
func forwardRefScanSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` IGNORE INDEX (`PRIMARY`) "+
			"WHERE tenant_id = ? AND id > ? ORDER BY id ASC LIMIT %d",
		columnList(), schema, table, batchSize)
}

// updateSQL renders the per-row UPDATE statement for the run phase.
func updateSQL(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET contents_enc = ?, contents_highlight_enc = ?, "+
			"metadata_enc = ?, version = version + 1 "+
			"WHERE id = ? AND tenant_id = ?", schema, table)
}
