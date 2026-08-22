package config

// Config mirrors tc_config.json. The field shape stays drop-in
// compatible with the v4 config plus three v5-only additions: artifacts,
// init.max_replication_lag_seconds, and debug.log_sql_once.
//
// Fields that v4 used but v5 ignores at runtime (e.g. remote.screen_sessions)
// are kept here so DisallowUnknownFields stays strict; v5 logs a warning on
// startup that they are inert.
type Config struct {
	Remote    Remote    `json:"remote"`
	Database  Database  `json:"database"`
	Init      Init      `json:"init"`
	Run       Run       `json:"run"`
	Noise     Noise     `json:"noise"`
	Safety    Safety    `json:"safety"`
	Artifacts Artifacts `json:"artifacts"`
	Debug     Debug     `json:"debug"`
}

// Noise carries the optional sibling-DML workers that run alongside the
// backfill scan-update loop. Intra-table noise on `files` is in scope as a
// config-gated opt-in; cross-table workloads remain out of scope.
// See internal/noise/doc.go.
type Noise struct {
	Enabled          bool  `json:"enabled"`
	InsertWorkers    int   `json:"insert_workers"`
	InsertRatePerSec int   `json:"insert_rate_per_sec"`
	InsertStartID    int64 `json:"insert_start_id"`
	UpdateWorkers    int   `json:"update_workers"`
	UpdateRatePerSec int   `json:"update_rate_per_sec"`
	// UpdateHotRowsPerTenant restricts UPDATE noise to the first N rows of
	// each tenant's seeded range. Concentrates same-page contention to amplify
	// the compressed-page recompression pressure observed in the crash-window
	// workload pattern. 0 = use full per-tenant range.
	UpdateHotRowsPerTenant int `json:"update_hot_rows_per_tenant"`
	// PoolMaxConns overrides the noise-side write pool size. 0 = falls back
	// to the backfill run pool size. Set higher (e.g. update_workers + insert_workers
	// + 4) to avoid contention with the backfill loop.
	PoolMaxConns       int      `json:"pool_max_conns"`
	LogIntervalSeconds int      `json:"log_interval_seconds"`
	Burst              BurstCfg `json:"burst"`
}

// BurstCfg is the time-shaped DML rate multiplier matching the observed
// crash-window pattern (~20s spike + ~50s baseline, 3-4x multiplier).
type BurstCfg struct {
	Enabled         bool    `json:"enabled"`
	Multiplier      float64 `json:"multiplier"`
	BurstSeconds    int     `json:"burst_seconds"`
	IntervalSeconds int     `json:"interval_seconds"`
}

// Remote captures the v4 remote.* block. v5 ignores every field at runtime
// (sysbench-style invocation) but parses them to keep the JSON shape
// v4-compatible.
type Remote struct {
	RemoteDir      string            `json:"remote_dir"`
	LogsDir        string            `json:"logs_dir"`
	ScreenSessions map[string]string `json:"screen_sessions"`
}

// Database wraps the two endpoint definitions. Write targets the source/master
// (typically a local Unix socket); read targets the replica TCP endpoint.
type Database struct {
	Write Endpoint `json:"write"`
	Read  Endpoint `json:"read"`
}

// Endpoint captures one side of the master/replica pair. Either Host or
// Socket must be set; Socket wins when both are present.
type Endpoint struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Socket   string `json:"socket"`
}

// Init carries the seeding knobs.
type Init struct {
	Tenants                  int         `json:"tenants"`
	RowsPerTenant            int         `json:"rows_per_tenant"`
	TenantIDBase             int64       `json:"tenant_id_base"`
	StartID                  int64       `json:"start_id"`
	DateBase                 int64       `json:"date_base"`
	DateStep                 int64       `json:"date_step"`
	Seed                     uint64      `json:"seed"`
	BlobProfile              BlobProfile `json:"blob_profile"`
	InsertBatchRows          int         `json:"insert_batch_rows"`
	InsertBatchBytes         int         `json:"insert_batch_bytes"`
	InitProgressRows         int         `json:"init_progress_rows"`
	TenantProgressRows       int         `json:"tenant_progress_rows"`
	InitWorkers              int         `json:"init_workers"`
	MaxParallelTenants       int         `json:"max_parallel_tenants"`
	RelaxDurability          bool        `json:"relax_durability"`
	ForceInit                bool        `json:"force_init"`
	MaxReplicationLagSeconds int         `json:"max_replication_lag_seconds"`
	// Opt-in performance levers — default off to preserve the wire
	// shape. Each one is documented in the per-field comments below.
	//
	// DisableRedoLog: ALTER INSTANCE DISABLE INNODB REDO_LOG before init,
	// re-enable after. Skips redo log writes entirely. CRITICAL: a server
	// crash during init leaves InnoDB unrecoverable — only enable on
	// throw-away test clusters where force_init can be replayed.
	DisableRedoLog bool `json:"disable_redo_log"`
	// BulkIndexRebuild: create the table with PRIMARY KEY only, seed all
	// rows, then ALTER TABLE ADD KEY for every secondary index parsed
	// out of the embedded schema. End-state matches the byte-identical
	// schema; intermediate timing differs (replica sees DDL events).
	BulkIndexRebuild bool `json:"bulk_index_rebuild"`
}

// BlobProfile is the size-class mix; percentages must sum to
// 100 (enforced by Validate).
type BlobProfile struct {
	SmallPct  int `json:"small_pct"`
	MediumPct int `json:"medium_pct"`
	LargePct  int `json:"large_pct"`
}

// Run carries the workload knobs.
type Run struct {
	BatchSize                     int              `json:"batch_size"`
	ReadSource                    string           `json:"read_source"`
	WriteTarget                   string           `json:"write_target"`
	Encryption                    Encryption       `json:"encryption"`
	StopCondition                 StopCondition    `json:"stop_condition"`
	ReplicationCheck              ReplicationCheck `json:"replication_check"`
	MaxParallelTenants            int              `json:"max_parallel_tenants"`
	ProgressIntervalBatches       int              `json:"progress_interval_batches"`
	TenantLogIntervalBatches      int              `json:"tenant_log_interval_batches"`
	ReplicationLogIntervalSeconds int              `json:"replication_log_interval_seconds"`
	SearchMix                     SearchMix        `json:"search_mix"`
}

// SearchMix carries the optional read-path diversification searchers that run
// alongside the backfill scan-update loop on the replica. Diversifying the *read*
// access paths on `repro_db.files` is in scope as a config-gated opt-in so the
// harness drives the observed crash callers — optimizer range-estimation, MRR
// execution, and the dominant forward `tenant_id = const` ref scan + filesort,
// which all converge on
// btr_cur_search_to_nth_level -> buf_page_get_gen -> single_page ->
// rw_lock_s_lock_low. (The earlier backward `ha_index_prev` shape was replaced:
// it appears in no core dump.) Searchers are read-only.
// Default Enabled=false is a no-op and preserves the wire shape and the
// validation gates bit-for-bit. See internal/run/searcher.go.
type SearchMix struct {
	Enabled bool `json:"enabled"`
	// RangeEstimateWorkers issue many distinct non-cached BETWEEN ranges over
	// multiple candidate indexes -> records_in_range /
	// btr_estimate_n_rows_in_range_low (the optimizer-side crash caller).
	RangeEstimateWorkers int `json:"range_estimate_workers"`
	// MRRWorkers issue a multi-value external_id IN-list over the tenant_id_5 /
	// tenant_id_8 secondary indexes, planned "Using MRR" + clustered fetch ->
	// ha_multi_range_read_next. The searcher sets optimizer_switch
	// 'mrr=on,mrr_cost_based=off' on its own read session to force the plan.
	MRRWorkers int `json:"mrr_workers"`
	// ForwardRefScanWorkers issue the DOMINANT shape: a forward `tenant_id = const`
	// ref scan with `IGNORE INDEX(PRIMARY)` + an `id > ?` keyset cursor +
	// `ORDER BY id ASC` -> a tenant_id secondary ref (ha_index_next_same /
	// RefIterator) + filesort + clustered fetch, S-latching the clustered ROOT
	// (page 4). This is the canonical backfill crash query.
	ForwardRefScanWorkers int `json:"forward_refscan_workers"`
	// RowsPerQuery bounds each searcher query (LIMIT, and the IN-list width for
	// MRR). 0 falls back to the default LIMIT of 10.
	RowsPerQuery int `json:"rows_per_query"`
	// LogIntervalSeconds throttles per-shape progress logs. 0 falls back to 60.
	LogIntervalSeconds int `json:"log_interval_seconds"`
}

// Encryption captures the run.encryption block. Mode is fixed to
// "sha256"; Validate refuses any other value.
type Encryption struct {
	Mode string `json:"mode"`
}

// StopCondition captures run.stop_condition. Only "all_rows" is in scope.
type StopCondition struct {
	Mode string `json:"mode"`
}

// ReplicationCheck captures run.replication_check. Only
// "show_replica_status_only" is in scope.
type ReplicationCheck struct {
	Mode string `json:"mode"`
}

// Safety carries the run-wide safety net knobs. MaxRuntimeSeconds=0 means
// "no deadline" (mirrors v4's null).
type Safety struct {
	ForceInitRequiresFlag bool `json:"force_init_requires_flag"`
	MaxRuntimeSeconds     int  `json:"max_runtime_seconds"`
}

// Artifacts carries the per-run artifact directory.
type Artifacts struct {
	Dir string `json:"dir"`
}

// Debug carries the wire-level debugging toggle.
type Debug struct {
	LogSQLOnce bool `json:"log_sql_once"`
}
