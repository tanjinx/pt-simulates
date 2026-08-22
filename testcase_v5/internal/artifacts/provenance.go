package artifacts

import (
	"fmt"
	"os"
	"time"
)

// ProvenanceHeader is the unambiguous run-identity record. Every
// artifact directory carries
// one as provenance.json. StartedAt is set at Header time; EndedAt is set
// later by the run completion path via WithEndedAt.
type ProvenanceHeader struct {
	Commit     string    `json:"commit"`
	BuiltAt    string    `json:"builtAt"`
	ConfigHash string    `json:"configHash"`
	Hostname   string    `json:"hostname"`
	StartedAt  time.Time `json:"startedAt"`
	EndedAt    time.Time `json:"endedAt,omitzero"`
}

// Header constructs a header from the build-time identity (commit, builtAt
// injected by ldflags into main.go and passed in here) and the on-disk
// configHash (config.Hash). StartedAt is captured at call time.
func Header(commit, builtAt, configHash string) ProvenanceHeader {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return ProvenanceHeader{
		Commit:     fallback(commit, "unknown"),
		BuiltAt:    fallback(builtAt, "unknown"),
		ConfigHash: configHash,
		Hostname:   host,
		StartedAt:  time.Now().UTC(),
	}
}

// WithEndedAt returns a copy of h with EndedAt set to t (UTC).
func (h ProvenanceHeader) WithEndedAt(t time.Time) ProvenanceHeader {
	h.EndedAt = t.UTC()
	return h
}

// String renders a one-line human summary, useful in startup logs.
func (h ProvenanceHeader) String() string {
	return fmt.Sprintf(
		"commit=%s builtAt=%s configHash=%s host=%s",
		h.Commit, h.BuiltAt, h.ConfigHash, h.Hostname)
}

func fallback(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
