package noise

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/blobgen"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

// Run starts the configured set of insert and update workers and blocks
// until ctx is cancelled. With cfg.Noise.Enabled=false this is a no-op.
//
// Errors from any worker abort the whole noise group via the embedded
// errgroup: no per-worker fallback.
func Run(ctx context.Context, write *sql.DB, cfg *config.Config, logger *slog.Logger) error {
	n := cfg.Noise
	if !n.Enabled {
		logger.Info("noise disabled; skipping")
		return nil
	}
	if write == nil {
		return errors.New("noise: write pool is nil")
	}
	// Optionally bump the pool size so noise workers do not contend with
	// the backfill tenant workers. db.Open in run.Orchestrate caps the pool at
	// cfg.Run.MaxParallelTenants; here we lift it for the lifetime of the
	// run when the noise config asks for more.
	if n.PoolMaxConns > 0 {
		write.SetMaxOpenConns(n.PoolMaxConns)
		write.SetMaxIdleConns(n.PoolMaxConns)
		logger.Info("noise pool sized", slog.Int("pool_max_conns", n.PoolMaxConns))
	}

	logger.Info("noise start",
		slog.Int("insert_workers", n.InsertWorkers),
		slog.Int("update_workers", n.UpdateWorkers),
		slog.Int("insert_rate_per_sec", n.InsertRatePerSec),
		slog.Int("update_rate_per_sec", n.UpdateRatePerSec),
		slog.Bool("burst", n.Burst.Enabled),
	)

	cycle := blobgen.NewCycle(blobgen.Profile{
		SmallPct:  cfg.Init.BlobProfile.SmallPct,
		MediumPct: cfg.Init.BlobProfile.MediumPct,
		LargePct:  cfg.Init.BlobProfile.LargePct,
	})
	idCounter := &atomic.Int64{}
	idCounter.Store(n.InsertStartID)

	tenantLo := cfg.Init.TenantIDBase
	tenantHi := cfg.Init.TenantIDBase + int64(cfg.Init.Tenants) - 1
	if tenantHi < tenantLo {
		tenantHi = tenantLo
	}

	schedule := BurstSchedule{
		Enabled:         n.Burst.Enabled,
		Multiplier:      n.Burst.Multiplier,
		BurstSeconds:    n.Burst.BurstSeconds,
		IntervalSeconds: n.Burst.IntervalSeconds,
	}

	counters := &Counters{}
	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < n.InsertWorkers; i++ {
		workerID := i + 1
		w := &insertWorker{
			id:         workerID,
			write:      write,
			schema:     cfg.Database.Write.Schema,
			table:      cfg.Database.Write.Table,
			ratePerSec: n.InsertRatePerSec,
			schedule:   schedule,
			cycle:      cycle,
			idCounter:  idCounter,
			tenantLo:   tenantLo,
			tenantHi:   tenantHi,
			dateBase:   cfg.Init.DateBase,
			seed:       cfg.Init.Seed,
			counters:   counters,
			logger:     logger.With(slog.String("noise_worker", "insert"), slog.Int("worker_id", workerID)),
		}
		g.Go(func() error { return w.run(gctx) })
	}
	for i := 0; i < n.UpdateWorkers; i++ {
		workerID := i + 1
		w := &updateWorker{
			id:               workerID,
			write:            write,
			schema:           cfg.Database.Write.Schema,
			table:            cfg.Database.Write.Table,
			ratePerSec:       n.UpdateRatePerSec,
			schedule:         schedule,
			tenantLo:         tenantLo,
			tenantHi:         tenantHi,
			startID:          cfg.Init.StartID,
			rowsPerTenant:    int64(cfg.Init.RowsPerTenant),
			hotRowsPerTenant: int64(n.UpdateHotRowsPerTenant),
			counters:         counters,
			logger:           logger.With(slog.String("noise_worker", "update"), slog.Int("worker_id", workerID)),
		}
		g.Go(func() error { return w.run(gctx) })
	}

	// Periodic counters log.
	if n.LogIntervalSeconds > 0 {
		interval := time.Duration(n.LogIntervalSeconds) * time.Second
		g.Go(func() error {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-gctx.Done():
					return nil
				case <-t.C:
					logger.Info("noise tick",
						slog.Int64("inserts_total", counters.Inserts.Load()),
						slog.Int64("updates_total", counters.Updates.Load()),
						slog.Int64("errors_total", counters.Errors.Load()),
					)
				}
			}
		})
	}

	err := g.Wait()
	logger.Info("noise stop",
		slog.Int64("inserts_total", counters.Inserts.Load()),
		slog.Int64("updates_total", counters.Updates.Load()),
		slog.Int64("errors_total", counters.Errors.Load()),
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Counters carries the run-wide totals across noise workers, exposed to
// the orchestrator for periodic logging.
type Counters struct {
	Inserts atomic.Int64
	Updates atomic.Int64
	Errors  atomic.Int64
}

// rateSleep computes the sleep between operations for the configured rate
// adjusted by the current burst multiplier. Off-state (rate<=0) returns
// 0 and the caller skips sleeping.
func rateSleep(ratePerSec int, sched BurstSchedule, elapsed time.Duration) time.Duration {
	if ratePerSec <= 0 {
		return 0
	}
	mult := sched.MultiplierAt(elapsed)
	effective := float64(ratePerSec) * mult
	if effective <= 0 {
		return time.Second
	}
	return time.Duration(float64(time.Second) / effective)
}

// sleepCtx sleeps d or returns ctx.Err on early cancel. Returns nil on
// graceful sleep completion.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// tenantPicker / idPicker are tiny deterministic-ish helpers — the noise
// workers do not need cryptographic randomness, only well-spread coverage
// of the seeded range. The single-pass single-algorithm choice is a
// linear-congruential walk seeded per worker.
type linWalker struct {
	state uint64
}

func (w *linWalker) next() uint64 {
	w.state = w.state*6364136223846793005 + 1442695040888963407
	return w.state >> 32
}

func (w *linWalker) inRange(lo, hi int64) int64 {
	span := hi - lo + 1
	if span <= 0 {
		return lo
	}
	return lo + int64(w.next())%span
}
