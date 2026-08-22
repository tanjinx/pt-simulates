// Package obs constructs the dual-handler slog logger.
package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// NewLoggers opens jsonPath for append and returns a logger that fans out to
// a JSON handler (file) and a text handler (stdout). The returned io.Closer
// is the underlying *os.File; callers MUST close it when the run ends so the
// final batched records flush to disk.
func NewLoggers(jsonPath string) (*slog.Logger, io.Closer, error) {
	f, err := os.OpenFile(jsonPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open jsonl log %s: %w", jsonPath, err)
	}
	jsonH := slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelInfo})
	textH := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(&fanout{handlers: []slog.Handler{jsonH, textH}}), f, nil
}

type fanout struct {
	handlers []slog.Handler
}

func (f *fanout) Enabled(ctx context.Context, lvl slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, lvl) {
			return true
		}
	}
	return false
}

func (f *fanout) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range f.handlers {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		if err := h.Handle(ctx, r.Clone()); err != nil {
			return err
		}
	}
	return nil
}

func (f *fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		out[i] = h.WithAttrs(attrs)
	}
	return &fanout{handlers: out}
}

func (f *fanout) WithGroup(name string) slog.Handler {
	out := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		out[i] = h.WithGroup(name)
	}
	return &fanout{handlers: out}
}
