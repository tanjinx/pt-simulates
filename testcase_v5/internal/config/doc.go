// Package config loads, validates, and exposes the tc_config.json contract.
// It is the sole knob surface of tc-repro and runs fail-fast at startup — every
// validation error is collected before Load returns, and no DB connection is
// attempted until validation succeeds.
package config
