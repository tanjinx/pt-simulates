package runphase

import "testing"

// TestSearchMixMRRArgsMatchSeededExternalID guards against a previously-found
// bug: the
// MRR IN-list values MUST match blobgen's seeded external_id format
// ("ext-<id>", id = StartID + tenantIdx*RowsPerTenant + rowN) so the IN-list
// resolves real keys and MRR actually performs the clustered fetch. is_deleted
// must bind 0 (every seeded row has is_deleted=0).
func TestSearchMixMRRArgsMatchSeededExternalID(t *testing.T) {
	const startID = int64(1_000_000)
	const rowsPerTenant = int64(100)
	const tenantLo = int64(463855761557)
	spec := searchSpec{
		kind: "mrr", rows: 2, tenantLo: tenantLo,
		startID: startID, rowsPerTenant: rowsPerTenant, dateSpan: 1000, dateLo: 1,
	}
	// Tenant index 1 -> tenantStartID = startID + 1*rowsPerTenant = 1_000_100.
	tenant := tenantLo + 1
	args := spec.argsFor(tenant, 0)
	if len(args) != 4 { // tenant_id, is_deleted, + 2 external_id values
		t.Fatalf("expected 4 MRR args, got %d: %v", len(args), args)
	}
	if args[0] != tenant {
		t.Errorf("arg0 tenant_id = %v, want %d", args[0], tenant)
	}
	if args[1] != 0 {
		t.Errorf("arg1 is_deleted = %v, want 0 (seeded rows are never deleted)", args[1])
	}
	// rowN 0 -> id = 1_000_100 -> "ext-1000100"; rowN 1 -> "ext-1000101".
	if got, want := args[2].(string), "ext-1000100"; got != want {
		t.Errorf("external_id[0] = %q, want %q (must match blobgen seed)", got, want)
	}
	if got, want := args[3].(string), "ext-1000101"; got != want {
		t.Errorf("external_id[1] = %q, want %q (must match blobgen seed)", got, want)
	}
}

// TestSearchMixRangeEstimateBindsExistingValues: range-estimate must bind
// is_deleted=0 and is_public=0 (the only seeded values) so the range matches.
func TestSearchMixRangeEstimateBindsExistingValues(t *testing.T) {
	spec := searchSpec{kind: "range_estimate", tenantLo: 1, startID: 0, rowsPerTenant: 100, dateSpan: 1000, dateLo: 1}
	args := spec.argsFor(1, 3)
	if args[1] != 0 || args[2] != 0 {
		t.Errorf("is_deleted/is_public must bind 0 (seeded values); got %v, %v", args[1], args[2])
	}
}

// TestSearchMixForwardRefScanArgs: the dominant shape binds (tenant_id, id_cursor)
// where the cursor walks the tenant's seeded id range (id = startID +
// tenantIdx*rowsPerTenant + rowN), so the ref scan + filesort materialises rows.
func TestSearchMixForwardRefScanArgs(t *testing.T) {
	const startID = int64(1_000_000)
	const rowsPerTenant = int64(100)
	const tenantLo = int64(1_000) // arbitrary synthetic base; test exercises cursor arithmetic only
	spec := searchSpec{
		kind: "forward_refscan", rows: 10, tenantLo: tenantLo,
		startID: startID, rowsPerTenant: rowsPerTenant, dateSpan: 1000, dateLo: 1,
	}
	tenant := tenantLo + 1 // tenantStartID = startID + 1*rowsPerTenant = 1_000_100
	const tenantStartID = int64(1_000_100)
	args := spec.argsFor(tenant, 5)
	if len(args) != 2 {
		t.Fatalf("expected 2 forward_refscan args (tenant_id, id_cursor), got %d: %v", len(args), args)
	}
	if args[0] != tenant {
		t.Errorf("arg0 tenant_id = %v, want %d", args[0], tenant)
	}
	// idMod = rowsPerTenant - rows = 90; n=5 -> cursor = 1_000_100 + (5 % 90) = 1_000_105.
	if got, want := args[1].(int64), int64(1_000_105); got != want {
		t.Errorf("arg1 id cursor = %d, want %d", got, want)
	}
	// The cursor must stay in the lower (rowsPerTenant - rows) band so >= `rows` ids
	// always satisfy id>cursor — the LIMIT-N clustered fetch (clustered-root
	// descent) must fire every iteration, including for large n.
	maxCursor := tenantStartID + (rowsPerTenant - int64(spec.rows) - 1) // 1_000_189
	for _, nn := range []int64{0, 50, 89, 95, 1_000, 1_000_000} {
		c := spec.argsFor(tenant, nn)[1].(int64)
		if c < tenantStartID || c > maxCursor {
			t.Errorf("n=%d cursor %d out of [%d,%d] (must leave >= rows rows above)",
				nn, c, tenantStartID, maxCursor)
		}
	}
}
