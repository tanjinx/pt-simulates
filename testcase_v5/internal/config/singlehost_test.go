package config

import (
	"strings"
	"testing"
)

func TestSingleHostModeAcceptsSameServer(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Run.Mode = "single_host"
	cfg.Run.ReplicationCheck.Mode = "disabled"
	cfg.Database.Read = cfg.Database.Write
	cfg.Noise.Enabled = false
	cfg.Run.SingleHost = SingleHost{
		TenantID:            42,
		IDMin:               100,
		IDMax:               199,
		DateCreateMin:       1000,
		DateCreateMax:       2000,
		ExternalIDs:         []string{"one"},
		BackfillReadWorkers: 1,
		WriteWorkers:        1,
		ReadIterations:      10,
		WriteIterations:     10,
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestSingleHostModeRejectsDifferentServers(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Run.Mode = "single_host"
	cfg.Run.ReplicationCheck.Mode = "disabled"
	cfg.Run.SingleHost = SingleHost{
		TenantID:            42,
		IDMin:               100,
		IDMax:               199,
		BackfillReadWorkers: 1,
		WriteWorkers:        1,
		ReadIterations:      1,
		WriteIterations:     1,
	}

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "same server") {
		t.Fatalf("error = %v, want same-server rejection", err)
	}
}

func TestSingleHostModeRejectsNoise(t *testing.T) {
	cfg := loadAccept(t)
	cfg.Run.Mode = "single_host"
	cfg.Run.ReplicationCheck.Mode = "disabled"
	cfg.Database.Read = cfg.Database.Write
	cfg.Noise.Enabled = true
	cfg.Run.SingleHost = SingleHost{
		TenantID:            42,
		IDMin:               100,
		IDMax:               199,
		BackfillReadWorkers: 1,
		WriteWorkers:        1,
		ReadIterations:      1,
		WriteIterations:     1,
	}

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "noise.enabled") {
		t.Fatalf("error = %v, want noise rejection", err)
	}
}
