package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

// newRootCmd builds the cobra tree. Only --config is registered as a flag;
// every verb is positional. The verbs themselves take no flags — Cobra's
// persistent flag inheritance still counts toward the "exactly one flag"
// requirement, so do not add any.
func newRootCmd(commit, builtAt string) *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:           "tc-repro --config <path> <verb>",
		Short:         "Backfill workload for mysqld SIGSEGV reproduction",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "path to tc_config.json (required)")
	_ = root.MarkPersistentFlagRequired("config")

	loader := func() (*config.Config, error) {
		if configPath == "" {
			return nil, errors.New("--config <path> is required")
		}
		return config.Load(configPath)
	}

	root.AddCommand(
		newInitCmd(loader, commit, builtAt),
		newRunCmd(loader, commit, builtAt),
		newStartCmd(loader, commit, builtAt),
		newStatusCmd(loader),
		newTailCmd(loader),
		newStopCmd(loader),
	)
	return root
}

func newInitCmd(load loaderFn, commit, builtAt string) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Drop+create table and seed init data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			return runInit(cmd.Context(), cfg, commit, builtAt)
		},
	}
}

func newRunCmd(load loaderFn, commit, builtAt string) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Execute the scan-encrypt-update workload",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			return runWorkload(cmd.Context(), cfg, commit, builtAt)
		},
	}
}

func newStartCmd(load loaderFn, commit, builtAt string) *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Run init then run, sequentially",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			if err := runInit(cmd.Context(), cfg, commit, builtAt); err != nil {
				return fmt.Errorf("init: %w", err)
			}
			return runWorkload(cmd.Context(), cfg, commit, builtAt)
		},
	}
}

func newStatusCmd(load loaderFn) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print the latest run's summary",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			return printLatestStatus(cfg)
		},
	}
}

func newTailCmd(load loaderFn) *cobra.Command {
	return &cobra.Command{
		Use:   "tail",
		Short: "Tail the latest run's JSON log",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			return tailLatestLog(cmd.Context(), cfg)
		},
	}
}

func newStopCmd(load loaderFn) *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Signal the PID recorded for the latest run",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			return signalLatest(cfg)
		},
	}
}

type loaderFn func() (*config.Config, error)
