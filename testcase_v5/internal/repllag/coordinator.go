package repllag

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

// Coordinator preserves lag-breach errors when errgroup peers return
// context.Canceled first.
type Coordinator struct {
	lagErr atomic.Pointer[error]
}

// GoWatch returns an errgroup func that runs Watch and records non-cancel errors.
func (c *Coordinator) GoWatch(ctx context.Context, db *sql.DB,
	interval time.Duration, lagCeiling int64, logger *slog.Logger) func() error {

	return func() error {
		err := Watch(ctx, db, interval, lagCeiling, logger)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		c.lagErr.Store(&err)
		return err
	}
}

// Prefer returns a stored lag error over a generic context cancellation.
func (c *Coordinator) Prefer(err error) error {
	if p := c.lagErr.Load(); p != nil && *p != nil {
		return *p
	}
	if err != nil && errors.Is(err, context.Canceled) {
		if p := c.lagErr.Load(); p != nil && *p != nil {
			return *p
		}
	}
	return err
}
