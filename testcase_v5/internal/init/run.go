package initphase

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/artifacts"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/obs"
)

// Run is the CLI's entrypoint for the init verb. It builds the
// artifact directory + provenance header, opens the dual-handler logger,
// runs the orchestrator, then writes summary tables.
func Run(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	configHash, _ := configFingerprint(cfg)
	header := artifacts.Header(commit, builtAt, configHash)
	dir, err := artifacts.Open(cfg.Artifacts.Dir, header)
	if err != nil {
		return err
	}

	logPath := filepath.Join(dir.Path, "init.jsonl")
	logger, closer, err := obs.NewLoggers(logPath)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()
	logger = logger.With(
		slog.String("component", "init"),
		slog.String("phase", "init"),
		slog.String("run_id", dir.RunID),
		slog.String("commit", commit),
		slog.String("built_at", builtAt),
	)

	summary, runErr := Orchestrate(ctx, cfg, logger)
	tenants := make([]artifacts.TenantRow, len(summary.Tenants))
	for i, t := range summary.Tenants {
		tenants[i] = artifacts.TenantRow{
			TenantID: t.tenantID, Rows: t.rows, Batches: t.batches,
			Bytes: t.bytes, Duration: t.duration,
		}
	}
	phase := artifacts.PhaseSummary{
		Phase: "init", Started: summary.Started, Ended: summary.Ended,
		Duration: summary.Duration, TenantCount: summary.TenantCount,
		Rows: summary.Rows, Bytes: summary.Bytes, Batches: summary.Batches,
		Tenants: tenants,
	}
	if werr := dir.WriteSummary(phase); werr != nil {
		logger.Error("write summary failed", slog.String("err", werr.Error()))
	}
	if werr := dir.WriteHeader(header.WithEndedAt(time.Now())); werr != nil {
		logger.Error("write header end failed", slog.String("err", werr.Error()))
	}
	if runErr != nil {
		logger.Error("init failed",
			slog.String("event", "abort"),
			slog.String("err", runErr.Error()),
			slog.Duration("elapsed", summary.Duration),
		)
		return runErr
	}
	return nil
}

func configFingerprint(_ *config.Config) (string, error) {
	// The Hash function operates on the on-disk JSON bytes; the loader path
	// is not propagated here, so the fingerprint is best-effort empty when
	// called from Run. The artifact still records commit/builtAt/hostname.
	return "", fmt.Errorf("config hash propagation handled at CLI boundary")
}
