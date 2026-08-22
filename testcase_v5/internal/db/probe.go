package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// ProbeMaxAllowedPacket reads @@max_allowed_packet from the endpoint so
// config validation can enforce
// init.insert_batch_bytes <= probed. Errors propagate; callers must NOT
// silently fall back.
func ProbeMaxAllowedPacket(ctx context.Context, db *sql.DB) (int64, error) {
	var raw string
	row := db.QueryRowContext(ctx, "SELECT @@max_allowed_packet")
	if err := row.Scan(&raw); err != nil {
		return 0, fmt.Errorf("probe max_allowed_packet: %w", err)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse max_allowed_packet %q: %w", raw, err)
	}
	return v, nil
}
