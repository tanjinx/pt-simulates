// Package artifacts owns the per-run artifact directory layout: a
// provenance.json header
// ({commit, builtAt, configHash, hostname, startedAt, endedAt}), the
// JSON-formatted log, per-tenant and per-worker summary tables, and the pid
// file that `tc-repro stop` consults.
package artifacts
