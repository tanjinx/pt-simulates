// Package noise drives concurrent DML against the `files` table while
// the run-phase backfill scan-update loop runs.
//
// # Why this exists
//
// Analysis of the pre-crash captures observed three workload
// conditions in both application crashes:
//
//  1. Range-scan SELECT on a ROW_FORMAT=COMPRESSED table (v5 already does this).
//  2. Heavy concurrent DML on the same compressed-table family during the
//     SELECT: ~55k UPDATEs on the SELECT'd table plus a 10x surge on a
//     related table. v5 does this only weakly (the backfill UPDATEs).
//  3. Pre-crash burst: 3-4x DML rate spikes lasting ~20s, separated by ~50s
//     of baseline, with two such spikes inside the 90s before the crash.
//
// This package adds (2) and (3) inside v5's single-table scope: concurrent
// INSERT + UPDATE workers against `files` (sized via config), with an
// optional burst schedule that periodically multiplies the rate.
//
// # Scope
//
// Cross-table noise remains out of scope; intra-table noise on `files` is
// in scope as a config-gated opt-in.
//
// # Default behaviour
//
// Default config: noise.enabled=false. With the field absent or false the
// entire package is a no-op and the harness behaves exactly as if this
// package were not present.
package noise
