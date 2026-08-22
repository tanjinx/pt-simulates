package config

import "testing"

func TestValidateInsertBatchBytes(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Init.InsertBatchBytes = 2_097_152

	if err := ValidateInsertBatchBytes(1_048_576, cfg); err == nil {
		t.Fatal("expected oversize batch bytes to fail")
	}
	if err := ValidateInsertBatchBytes(134_217_728, cfg); err != nil {
		t.Fatalf("expected within limit, got %v", err)
	}
}
