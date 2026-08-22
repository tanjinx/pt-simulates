package main

import (
	"context"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	initphase "github.com/matias-sanchez/pt-simulates/testcase_v5/internal/init"
	runphase "github.com/matias-sanchez/pt-simulates/testcase_v5/internal/run"
)

// initEntrypoint runs the seeding phase. Indirected through a function var
// so that cmd_test.go can dispatch without touching real DB endpoints.
var initEntrypoint = func(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	return initphase.Run(ctx, cfg, commit, builtAt)
}

// runEntrypoint runs the scan-encrypt-update workload (same indirection
// pattern as initEntrypoint).
var runEntrypoint = func(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	return runphase.Run(ctx, cfg, commit, builtAt)
}
