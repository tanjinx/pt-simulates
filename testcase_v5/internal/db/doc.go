// Package db owns the shared *sql.DB pools used by every phase:
// one pool per endpoint, explicit SetMaxOpenConns sizing, DSN params
// parseTime=true, multiStatements=false, interpolateParams=false (binary
// protocol on the wire). Probes that inform config validation (e.g.
// max_allowed_packet) live here too.
package db
