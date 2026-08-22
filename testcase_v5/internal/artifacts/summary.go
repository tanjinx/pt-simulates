package artifacts

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

// PhaseSummary captures totals for one phase (init or run). Numeric fields
// are plain ints; durations are nanoseconds so JSON round-trips are
// lossless.
type PhaseSummary struct {
	Phase       string        `json:"phase"`
	Started     time.Time     `json:"started"`
	Ended       time.Time     `json:"ended"`
	Duration    time.Duration `json:"duration_ns"`
	TenantCount int           `json:"tenant_count"`
	Rows        int           `json:"rows"`
	Bytes       int64         `json:"bytes,omitzero"`
	Batches     int           `json:"batches"`
	Selects     int           `json:"selects,omitzero"`
	Updates     int           `json:"updates,omitzero"`
	Tenants     []TenantRow   `json:"tenants"`
}

// TenantRow is one row in the per-tenant summary table.
type TenantRow struct {
	TenantID int64         `json:"tenant_id"`
	WorkerID int           `json:"worker_id,omitzero"`
	Rows     int           `json:"rows"`
	Batches  int           `json:"batches"`
	Selects  int           `json:"selects,omitzero"`
	Updates  int           `json:"updates,omitzero"`
	Bytes    int64         `json:"bytes,omitzero"`
	Duration time.Duration `json:"duration_ns"`
}

// WriteSummary serializes the summary to <dir>/<phase>.summary.json and
// renders a human-readable table to stdout.
func (d *Dir) WriteSummary(s PhaseSummary) error {
	path := filepath.Join(d.Path, s.Phase+".summary.json")
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return RenderTable(os.Stdout, s)
}

// RenderTable writes a tabwriter-formatted table to w. Useful for the
// `tc-repro status` verb later.
func RenderTable(w io.Writer, s PhaseSummary) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintf(tw, "phase\trows\tbatches\tduration\trows/s\n"); err != nil {
		return err
	}
	rps := 0.0
	if s.Duration > 0 {
		rps = float64(s.Rows) / s.Duration.Seconds()
	}
	if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%.1f\n",
		s.Phase, s.Rows, s.Batches, s.Duration.Round(time.Millisecond), rps); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(tw, "tenant\tworker\trows\tbatches\tduration"); err != nil {
		return err
	}
	for _, t := range s.Tenants {
		if _, err := fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%s\n",
			t.TenantID, t.WorkerID, t.Rows, t.Batches,
			t.Duration.Round(time.Millisecond)); err != nil {
			return err
		}
	}
	return tw.Flush()
}
