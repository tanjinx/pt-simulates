package singlehost

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type (
	Config struct {
		Database DatabaseConfig `json:"database"`
		Workload WorkloadConfig `json:"workload"`
	}

	DatabaseConfig struct {
		DefaultsFile string `json:"defaults_file"`
		User         string `json:"user"`
		Password     string `json:"password"`
		Host         string `json:"host"`
		Port         int    `json:"port"`
		Socket       string `json:"socket"`
		Schema       string `json:"schema"`
		Table        string `json:"table"`
	}

	WorkloadConfig struct {
		TenantID        uint64 `json:"tenant_id"`
		IDMin           uint64 `json:"id_min"`
		IDMax           uint64 `json:"id_max"`
		ReadWorkers     int    `json:"read_workers"`
		WriteWorkers    int    `json:"write_workers"`
		ReadBatchSize   int    `json:"read_batch_size"`
		ReadIterations  int    `json:"read_iterations"`
		WriteIterations int    `json:"write_iterations"`
	}
)

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg Config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.Database.DefaultsFile == "" && c.Database.User == "" {
		errs = append(errs, errors.New("database.defaults_file or database.user is required"))
	}
	if c.Database.Host == "" && c.Database.Socket == "" {
		errs = append(errs, errors.New("database.host or database.socket is required"))
	}
	if !identifierPattern.MatchString(c.Database.Schema) {
		errs = append(errs, fmt.Errorf("invalid database.schema %q", c.Database.Schema))
	}
	if !identifierPattern.MatchString(c.Database.Table) {
		errs = append(errs, fmt.Errorf("invalid database.table %q", c.Database.Table))
	}
	if c.Workload.IDMin > c.Workload.IDMax {
		errs = append(errs, errors.New("workload.id_min must be <= workload.id_max"))
	}
	if c.Workload.ReadWorkers < 1 {
		errs = append(errs, errors.New("workload.read_workers must be >= 1"))
	}
	if c.Workload.WriteWorkers < 1 {
		errs = append(errs, errors.New("workload.write_workers must be >= 1"))
	}
	if c.Workload.ReadBatchSize < 1 {
		errs = append(errs, errors.New("workload.read_batch_size must be >= 1"))
	}
	if c.Workload.ReadIterations < 1 {
		errs = append(errs, errors.New("workload.read_iterations must be >= 1"))
	}
	if c.Workload.WriteIterations < 1 {
		errs = append(errs, errors.New("workload.write_iterations must be >= 1"))
	}
	return errors.Join(errs...)
}
