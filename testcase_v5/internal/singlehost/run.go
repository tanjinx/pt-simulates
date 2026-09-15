package singlehost

import (
	"context"
	"database/sql"
	"fmt"

	"golang.org/x/sync/errgroup"
)

func Run(ctx context.Context, cfg *Config) error {
	driverCfg, err := mysqlConfig(cfg.Database)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", driverCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(cfg.Workload.ReadWorkers + cfg.Workload.WriteWorkers)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect database: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	for worker := 1; worker <= cfg.Workload.ReadWorkers; worker++ {
		worker := worker
		g.Go(func() error {
			if err := runReader(gctx, db, cfg); err != nil {
				return fmt.Errorf("read worker %d: %w", worker, err)
			}
			return nil
		})
	}
	for worker := 1; worker <= cfg.Workload.WriteWorkers; worker++ {
		worker := worker
		g.Go(func() error {
			if err := runWriter(gctx, db, cfg, worker); err != nil {
				return fmt.Errorf("write worker %d: %w", worker, err)
			}
			return nil
		})
	}
	return g.Wait()
}

func runReader(ctx context.Context, db *sql.DB, cfg *Config) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET SESSION transaction_isolation = 'REPEATABLE-READ'"); err != nil {
		return fmt.Errorf("set isolation: %w", err)
	}
	query := readSQL(cfg.Database.Schema, cfg.Database.Table, cfg.Workload.ReadBatchSize)
	for range cfg.Workload.ReadIterations {
		rows, err := conn.QueryContext(ctx, query, cfg.Workload.TenantID, cfg.Workload.IDMin)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		if err := drainRows(rows); err != nil {
			return err
		}
	}
	return nil
}

func runWriter(ctx context.Context, db *sql.DB, cfg *Config, worker int) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()

	if err := disableBinlog(ctx, sqlBinlogConn{conn: conn}); err != nil {
		return err
	}
	query := writeSQL(cfg.Database.Schema, cfg.Database.Table)
	for iteration := 1; iteration <= cfg.Workload.WriteIterations; iteration++ {
		_, err := conn.ExecContext(ctx, query,
			worker, iteration,
			worker, iteration,
			worker, iteration,
			cfg.Workload.TenantID, cfg.Workload.IDMin, cfg.Workload.IDMax,
		)
		if err != nil {
			return fmt.Errorf("iteration %d: %w", iteration, err)
		}
	}
	return nil
}

func drainRows(rows *sql.Rows) error {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}
	values := make([]any, len(cols))
	for i := range values {
		values[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	return nil
}
