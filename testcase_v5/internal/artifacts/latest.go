package artifacts

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// LatestDir returns the most recently created run directory under root.
// Run IDs are UTC timestamps formatted by Open(), so lexicographic sort
// matches chronological order.
func LatestDir(root string) (*Dir, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read artifacts root %q: %w", root, err)
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no artifact runs under %q", root)
	}
	sort.Strings(ids)
	runID := ids[len(ids)-1]
	return &Dir{Path: filepath.Join(root, runID), RunID: runID}, nil
}

// LoadPhaseSummary reads <phase>.summary.json from dir.
func LoadPhaseSummary(dir *Dir, phase string) (PhaseSummary, error) {
	path := filepath.Join(dir.Path, phase+".summary.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return PhaseSummary{}, fmt.Errorf("read %s summary: %w", phase, err)
	}
	var s PhaseSummary
	if err := json.Unmarshal(raw, &s); err != nil {
		return PhaseSummary{}, fmt.Errorf("parse %s summary: %w", phase, err)
	}
	return s, nil
}

// ReadPID returns the process ID recorded when the run started.
func ReadPID(dir *Dir) (int, error) {
	raw, err := os.ReadFile(filepath.Join(dir.Path, "pid"))
	if err != nil {
		return 0, fmt.Errorf("read pid file: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, fmt.Errorf("parse pid %q: %w", strings.TrimSpace(string(raw)), err)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	return pid, nil
}

// PrintStatus writes the latest artifact run metadata and any available
// phase summaries to stdout (the status verb).
func PrintStatus(root string, w io.Writer) error {
	dir, err := LatestDir(root)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "run_id\t%s\npath\t%s\n", dir.RunID, dir.Path); err != nil {
		return err
	}
	if prov, err := os.ReadFile(filepath.Join(dir.Path, "provenance.json")); err == nil {
		if _, err := fmt.Fprintf(w, "provenance\t%s\n", strings.TrimSpace(string(prov))); err != nil {
			return err
		}
	}
	for _, phase := range []string{"init", "run"} {
		s, err := LoadPhaseSummary(dir, phase)
		if err != nil {
			continue
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := RenderTable(w, s); err != nil {
			return err
		}
	}
	return nil
}

// LogFile returns run.jsonl if present, otherwise init.jsonl.
func (d *Dir) LogFile() (string, error) {
	for _, name := range []string{"run.jsonl", "init.jsonl"} {
		path := filepath.Join(d.Path, name)
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no run.jsonl or init.jsonl in %s", d.Path)
}

// FollowLog prints existing log lines then polls for new ones until ctx
// is canceled (the tail verb).
func FollowLog(ctx context.Context, path string, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	// bufio.Scanner cannot follow appended bytes; re-open a reader at the
	// current offset once the initial snapshot is printed.
	var offset int64
	for sc.Scan() {
		if _, err := fmt.Fprintln(w, sc.Text()); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read log: %w", err)
	}
	offset, err = f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("seek log: %w", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := f.Stat()
			if err != nil {
				return fmt.Errorf("stat log: %w", err)
			}
			if info.Size() <= offset {
				continue
			}
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				return fmt.Errorf("seek log: %w", err)
			}
			chunk := make([]byte, info.Size()-offset)
			n, err := io.ReadFull(f, chunk)
			if err != nil && err != io.ErrUnexpectedEOF {
				return fmt.Errorf("read log tail: %w", err)
			}
			if n > 0 {
				if _, err := w.Write(chunk[:n]); err != nil {
					return err
				}
				offset += int64(n)
			}
		}
	}
}

// SignalPID sends sig to the recorded process. Returns an error when the
// process is not running.
func SignalPID(pid int, sig syscall.Signal) error {
	if err := syscall.Kill(pid, 0); err != nil {
		return fmt.Errorf("pid %d not running: %w", pid, err)
	}
	if err := syscall.Kill(pid, sig); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	return nil
}
