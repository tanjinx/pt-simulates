package config

import "fmt"

// ValidateInsertBatchBytes enforces init.insert_batch_bytes against a probed
// server max_allowed_packet (the runtime half of validation).
func ValidateInsertBatchBytes(max int64, cfg *Config) error {
	if int64(cfg.Init.InsertBatchBytes) > max {
		return fmt.Errorf(
			"init.insert_batch_bytes (%d) exceeds server max_allowed_packet (%d)",
			cfg.Init.InsertBatchBytes, max)
	}
	return nil
}
