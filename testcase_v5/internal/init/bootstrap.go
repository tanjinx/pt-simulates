package initphase

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/db"
)

// bootstrapDatabase ensures the target schema exists before the main write
// pool's DSN tries to USE it (which would fail Ping otherwise). It opens a
// short-lived no-schema pool, issues CREATE DATABASE IF NOT EXISTS with
// the same charset/collation v4 used, and closes.
//
// Idempotent: re-running over an existing database is a no-op.
func bootstrapDatabase(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	noSchema := cfg.Database.Write
	noSchema.Schema = ""

	pool, err := db.Open(noSchema, 1)
	if err != nil {
		return fmt.Errorf("bootstrap connect: %w", err)
	}
	defer func() { _ = pool.Close() }()

	stmt := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` "+
			"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
		cfg.Database.Write.Schema)
	if _, err := pool.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("bootstrap CREATE DATABASE: %w", err)
	}
	logger.Info("database bootstrapped",
		slog.String("event", "db_bootstrapped"),
		slog.String("schema", cfg.Database.Write.Schema),
	)
	return nil
}
