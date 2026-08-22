package artifacts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Dir is one run's artifact directory: <artifacts.dir>/<run-id>/. Holds the
// provenance header, the JSON log, and the pid file.
type Dir struct {
	Path  string
	RunID string
}

// Open creates <root>/<run-id>/ (mode 0o755) and writes pid + initial
// provenance header to disk. Subsequent phases append to the JSON log; the
// header is updated at run end via WriteHeader.
func Open(root string, header ProvenanceHeader) (*Dir, error) {
	// Sub-second resolution so init + run called back-to-back land in
	// distinct directories. The format is filesystem-safe (no colons).
	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	full := filepath.Join(root, runID)
	if err := os.MkdirAll(full, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir artifact dir: %w", err)
	}
	d := &Dir{Path: full, RunID: runID}
	if err := d.WriteHeader(header); err != nil {
		return nil, err
	}
	if err := d.writePID(); err != nil {
		return nil, err
	}
	return d, nil
}

// WriteHeader (re)writes provenance.json. Safe to call at run start and run
// end (with EndedAt populated).
func (d *Dir) WriteHeader(h ProvenanceHeader) error {
	path := filepath.Join(d.Path, "provenance.json")
	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal provenance: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write provenance: %w", err)
	}
	return nil
}

func (d *Dir) writePID() error {
	path := filepath.Join(d.Path, "pid")
	pid := strconv.Itoa(os.Getpid())
	return os.WriteFile(path, []byte(pid), 0o644)
}
