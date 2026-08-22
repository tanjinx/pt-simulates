package runphase

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"
)

// ErrMaxRuntime is returned when the run-wide deadline expires (the
// safety.max_runtime_seconds stop condition).
var ErrMaxRuntime = errors.New("run: max_runtime_seconds exceeded")

// withCancellation derives a context that cancels on SIGINT/SIGTERM and on
// the safety.max_runtime_seconds deadline. The returned cleanup function
// MUST be called when the run ends.
func withCancellation(parent context.Context, maxRuntimeSecs int) (context.Context, func()) {
	sigCtx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	if maxRuntimeSecs <= 0 {
		return sigCtx, stop
	}
	deadlineCtx, cancel := context.WithTimeout(sigCtx, time.Duration(maxRuntimeSecs)*time.Second)
	return deadlineCtx, func() {
		cancel()
		stop()
	}
}
