package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Validate enforces every documented config rule. It collects ALL
// errors before returning a single joined error so the operator sees the
// full list at startup, not one violation per retry.
func Validate(cfg *Config) error {
	var errs []error
	errs = append(errs, validateDatabase(cfg)...)
	errs = append(errs, validateInit(cfg)...)
	errs = append(errs, validateRun(cfg)...)
	errs = append(errs, validateSearchMix(cfg)...)
	errs = append(errs, validateNoise(cfg)...)
	errs = append(errs, validateSafety(cfg)...)
	errs = append(errs, validateArtifacts(cfg)...)
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func validateSearchMix(cfg *Config) []error {
	var errs []error
	s := cfg.Run.SearchMix
	if !s.Enabled {
		return nil
	}
	if s.RangeEstimateWorkers < 0 {
		errs = append(errs, fmt.Errorf(
			"run.search_mix.range_estimate_workers must be >= 0 (got %d)", s.RangeEstimateWorkers))
	}
	if s.MRRWorkers < 0 {
		errs = append(errs, fmt.Errorf("run.search_mix.mrr_workers must be >= 0 (got %d)", s.MRRWorkers))
	}
	if s.ForwardRefScanWorkers < 0 {
		errs = append(errs, fmt.Errorf(
			"run.search_mix.forward_refscan_workers must be >= 0 (got %d)", s.ForwardRefScanWorkers))
	}
	if s.RangeEstimateWorkers == 0 && s.MRRWorkers == 0 && s.ForwardRefScanWorkers == 0 {
		errs = append(errs, errors.New("run.search_mix.enabled=true but no searchers configured"))
	}
	if s.RowsPerQuery < 0 {
		errs = append(errs, fmt.Errorf("run.search_mix.rows_per_query must be >= 0 (got %d)", s.RowsPerQuery))
	}
	if s.LogIntervalSeconds < 0 {
		errs = append(errs, fmt.Errorf(
			"run.search_mix.log_interval_seconds must be >= 0 (got %d)", s.LogIntervalSeconds))
	}
	return errs
}

func validateNoise(cfg *Config) []error {
	var errs []error
	n := cfg.Noise
	if !n.Enabled {
		return nil
	}
	if n.InsertWorkers < 0 {
		errs = append(errs, fmt.Errorf("noise.insert_workers must be >= 0 (got %d)", n.InsertWorkers))
	}
	if n.UpdateWorkers < 0 {
		errs = append(errs, fmt.Errorf("noise.update_workers must be >= 0 (got %d)", n.UpdateWorkers))
	}
	if n.InsertWorkers == 0 && n.UpdateWorkers == 0 {
		errs = append(errs, errors.New("noise.enabled=true but no insert/update workers configured"))
	}
	if n.InsertRatePerSec < 0 {
		errs = append(errs, fmt.Errorf("noise.insert_rate_per_sec must be >= 0 (got %d)", n.InsertRatePerSec))
	}
	if n.UpdateRatePerSec < 0 {
		errs = append(errs, fmt.Errorf("noise.update_rate_per_sec must be >= 0 (got %d)", n.UpdateRatePerSec))
	}
	if n.InsertWorkers > 0 && n.InsertStartID <= 0 {
		errs = append(errs, errors.New("noise.insert_start_id must be > 0 when insert_workers > 0"))
	}
	if n.InsertWorkers > 0 && n.InsertStartID > 0 {
		seededHi := cfg.Init.StartID + int64(cfg.Init.RowsPerTenant)*int64(cfg.Init.Tenants)
		if n.InsertStartID <= seededHi {
			errs = append(errs, fmt.Errorf(
				"noise.insert_start_id (%d) must be greater than seeded range end (%d)",
				n.InsertStartID, seededHi))
		}
	}
	if n.LogIntervalSeconds < 0 {
		errs = append(errs, fmt.Errorf("noise.log_interval_seconds must be >= 0 (got %d)", n.LogIntervalSeconds))
	}
	if n.UpdateHotRowsPerTenant < 0 {
		errs = append(errs, fmt.Errorf("noise.update_hot_rows_per_tenant must be >= 0 (got %d)", n.UpdateHotRowsPerTenant))
	}
	if n.UpdateHotRowsPerTenant > 0 && cfg.Init.RowsPerTenant > 0 && n.UpdateHotRowsPerTenant > cfg.Init.RowsPerTenant {
		errs = append(errs, fmt.Errorf(
			"noise.update_hot_rows_per_tenant (%d) must be <= init.rows_per_tenant (%d)",
			n.UpdateHotRowsPerTenant, cfg.Init.RowsPerTenant))
	}
	if n.PoolMaxConns < 0 {
		errs = append(errs, fmt.Errorf("noise.pool_max_conns must be >= 0 (got %d)", n.PoolMaxConns))
	}
	if n.Burst.Enabled {
		if n.Burst.Multiplier <= 0 {
			errs = append(errs, fmt.Errorf("noise.burst.multiplier must be > 0 (got %v)", n.Burst.Multiplier))
		}
		if n.Burst.BurstSeconds < 1 {
			errs = append(errs, fmt.Errorf("noise.burst.burst_seconds must be >= 1 (got %d)", n.Burst.BurstSeconds))
		}
		if n.Burst.IntervalSeconds < 0 {
			errs = append(errs, fmt.Errorf(
				"noise.burst.interval_seconds must be >= 0 (got %d)", n.Burst.IntervalSeconds))
		}
	}
	return errs
}

func validateDatabase(cfg *Config) []error {
	var errs []error
	w, r := cfg.Database.Write, cfg.Database.Read
	if w.Schema == "" {
		errs = append(errs, errors.New("database.write.schema is required"))
	}
	if w.Table == "" {
		errs = append(errs, errors.New("database.write.table is required"))
	}
	if r.Schema == "" {
		errs = append(errs, errors.New("database.read.schema is required"))
	}
	if r.Table == "" {
		errs = append(errs, errors.New("database.read.table is required"))
	}
	if w.Schema != "" && r.Schema != "" && w.Schema != r.Schema {
		errs = append(errs, fmt.Errorf(
			"database.write.schema (%q) must equal database.read.schema (%q)",
			w.Schema, r.Schema))
	}
	if w.Table != "" && r.Table != "" && w.Table != r.Table {
		errs = append(errs, fmt.Errorf(
			"database.write.table (%q) must equal database.read.table (%q)",
			w.Table, r.Table))
	}
	if w.Socket == "" && w.Host == "" {
		errs = append(errs, errors.New("database.write requires host or socket"))
	}
	if r.Host == "" && r.Socket == "" {
		errs = append(errs, errors.New("database.read requires host or socket"))
	}
	if w.User == "" {
		errs = append(errs, errors.New("database.write.user is required"))
	}
	if r.User == "" {
		errs = append(errs, errors.New("database.read.user is required"))
	}
	return errs
}

func validateInit(cfg *Config) []error {
	var errs []error
	i := cfg.Init
	if i.Tenants < 1 {
		errs = append(errs, fmt.Errorf("init.tenants must be >= 1 (got %d)", i.Tenants))
	}
	if i.RowsPerTenant < 1 {
		errs = append(errs, fmt.Errorf("init.rows_per_tenant must be >= 1 (got %d)", i.RowsPerTenant))
	}
	if i.MaxParallelTenants < 1 {
		errs = append(errs, fmt.Errorf("init.max_parallel_tenants must be >= 1 (got %d)", i.MaxParallelTenants))
	}
	if i.Tenants >= 1 && i.MaxParallelTenants > i.Tenants {
		errs = append(errs, fmt.Errorf(
			"init.max_parallel_tenants (%d) must be <= init.tenants (%d)",
			i.MaxParallelTenants, i.Tenants))
	}
	if i.InsertBatchRows < 1 {
		errs = append(errs, fmt.Errorf(
			"init.insert_batch_rows must be >= 1 (got %d)", i.InsertBatchRows))
	}
	if i.InsertBatchBytes < 1 {
		errs = append(errs, fmt.Errorf(
			"init.insert_batch_bytes must be >= 1 (got %d)", i.InsertBatchBytes))
	}
	if i.DateStep < 1 {
		errs = append(errs, fmt.Errorf("init.date_step must be >= 1 (got %d)", i.DateStep))
	}
	if i.MaxReplicationLagSeconds < 0 {
		errs = append(errs, fmt.Errorf(
			"init.max_replication_lag_seconds must be >= 0 (got %d)",
			i.MaxReplicationLagSeconds))
	}
	errs = append(errs, validateBlobProfile(i.BlobProfile)...)
	return errs
}

func validateBlobProfile(p BlobProfile) []error {
	var errs []error
	if p.SmallPct < 0 || p.MediumPct < 0 || p.LargePct < 0 {
		errs = append(errs, errors.New("init.blob_profile percentages must be >= 0"))
	}
	if total := p.SmallPct + p.MediumPct + p.LargePct; total != 100 {
		errs = append(errs, fmt.Errorf(
			"init.blob_profile percentages must sum to 100 (got %d)", total))
	}
	return errs
}

func validateRun(cfg *Config) []error {
	var errs []error
	r := cfg.Run
	if r.BatchSize != 10 {
		errs = append(errs, fmt.Errorf(
			"run.batch_size must be 10 (got %d) — matches production backfill cursor",
			r.BatchSize))
	}
	if r.Encryption.Mode != "sha256" {
		errs = append(errs, fmt.Errorf(
			"run.encryption.mode must be \"sha256\" (got %q)", r.Encryption.Mode))
	}
	if r.StopCondition.Mode != "all_rows" {
		errs = append(errs, fmt.Errorf(
			"run.stop_condition.mode must be \"all_rows\" (got %q)",
			r.StopCondition.Mode))
	}
	if r.ReplicationCheck.Mode != "show_replica_status_only" {
		errs = append(errs, fmt.Errorf(
			"run.replication_check.mode must be \"show_replica_status_only\" (got %q)",
			r.ReplicationCheck.Mode))
	}
	if r.MaxParallelTenants < 1 {
		errs = append(errs, fmt.Errorf(
			"run.max_parallel_tenants must be >= 1 (got %d)", r.MaxParallelTenants))
	}
	if r.ReplicationLogIntervalSeconds < 1 {
		errs = append(errs, fmt.Errorf(
			"run.replication_log_interval_seconds must be >= 1 (got %d)",
			r.ReplicationLogIntervalSeconds))
	}
	if r.ProgressIntervalBatches < 1 {
		errs = append(errs, fmt.Errorf(
			"run.progress_interval_batches must be >= 1 (got %d)",
			r.ProgressIntervalBatches))
	}
	if r.TenantLogIntervalBatches < 1 {
		errs = append(errs, fmt.Errorf(
			"run.tenant_log_interval_batches must be >= 1 (got %d)",
			r.TenantLogIntervalBatches))
	}
	return errs
}

func validateSafety(cfg *Config) []error {
	var errs []error
	if cfg.Safety.MaxRuntimeSeconds < 0 {
		errs = append(errs, fmt.Errorf(
			"safety.max_runtime_seconds must be >= 0 (got %d; 0 disables)",
			cfg.Safety.MaxRuntimeSeconds))
	}
	return errs
}

func validateArtifacts(cfg *Config) []error {
	var errs []error
	if cfg.Artifacts.Dir == "" {
		errs = append(errs, errors.New("artifacts.dir is required"))
		return errs
	}
	parent := filepath.Dir(cfg.Artifacts.Dir)
	info, err := os.Stat(parent)
	if err != nil {
		errs = append(errs, fmt.Errorf(
			"artifacts.dir parent %q is not stat-able: %w", parent, err))
		return errs
	}
	if !info.IsDir() {
		errs = append(errs, fmt.Errorf(
			"artifacts.dir parent %q is not a directory", parent))
	}
	return errs
}
