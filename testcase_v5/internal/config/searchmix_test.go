package config

import "testing"

// TestSearchMixDisabledIsNoop: the default zero value is disabled and produces
// no validation errors (default-off parity).
func TestSearchMixDisabledIsNoop(t *testing.T) {
	cfg := &Config{}
	if cfg.Run.SearchMix.Enabled {
		t.Fatal("SearchMix must default to disabled")
	}
	if errs := validateSearchMix(cfg); errs != nil {
		t.Fatalf("disabled search_mix must produce no errors, got %v", errs)
	}
}

// TestSearchMixEnabledRequiresWorkers: enabled with all worker counts zero is a
// configuration error.
func TestSearchMixEnabledRequiresWorkers(t *testing.T) {
	cfg := &Config{}
	cfg.Run.SearchMix.Enabled = true
	if errs := validateSearchMix(cfg); len(errs) == 0 {
		t.Fatal("enabled search_mix with no workers must error")
	}
}

// TestSearchMixNegativeWorkersRejected: negative worker counts are rejected.
func TestSearchMixNegativeWorkersRejected(t *testing.T) {
	cfg := &Config{}
	cfg.Run.SearchMix.Enabled = true
	cfg.Run.SearchMix.MRRWorkers = -1
	if errs := validateSearchMix(cfg); len(errs) == 0 {
		t.Fatal("negative mrr_workers must error")
	}
}

// TestSearchMixValidConfig: a sane enabled config validates clean.
func TestSearchMixValidConfig(t *testing.T) {
	cfg := &Config{}
	cfg.Run.SearchMix = SearchMix{
		Enabled:               true,
		RangeEstimateWorkers:  2,
		MRRWorkers:            2,
		ForwardRefScanWorkers: 2,
		RowsPerQuery:          10,
		LogIntervalSeconds:    60,
	}
	if errs := validateSearchMix(cfg); errs != nil {
		t.Fatalf("valid search_mix must produce no errors, got %v", errs)
	}
}
