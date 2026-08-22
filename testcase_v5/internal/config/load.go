package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// ErrConfigInvalid is returned by Load when validation fails; the wrapped
// error carries the joined list of every violation.
var ErrConfigInvalid = errors.New("tc_config.json is invalid")

// Load reads, decodes (strict — unknown fields are rejected), and validates
// the config at path. The returned *Config is safe to use; Load never returns
// a partially-validated config.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	cfg, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfigInvalid, err)
	}
	return cfg, nil
}

// Hash returns a SHA-256 hex digest of the on-disk config bytes (for
// provenance). Errors from re-reading the file are
// propagated; the digest is over raw bytes, not the parsed struct, so two
// configs that differ only in whitespace would hash differently — which is
// intentional, because a difference like that should be unambiguous in run
// comparisons.
func Hash(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return hashBytes(raw), nil
}

func decode(raw []byte) (*Config, error) {
	cfg := &Config{}
	dec := json.NewDecoder(byteReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
