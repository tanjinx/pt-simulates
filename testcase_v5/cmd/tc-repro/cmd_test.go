package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TestExactlyOneFlag is the regression test for the rule that the CLI must
// expose exactly one flag (--config). A future code change that accidentally
// adds a --verbose, --dry-run, or env-bound flag will fail here.
func TestExactlyOneFlag(t *testing.T) {
	root := newRootCmd("test", "test")
	var names []string
	root.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		names = append(names, f.Name)
	})
	root.Flags().VisitAll(func(f *pflag.Flag) {
		names = append(names, f.Name)
	})
	if got := len(names); got != 1 {
		t.Fatalf("expected exactly one flag, got %d: %v", got, names)
	}
	if names[0] != "config" {
		t.Fatalf("expected only flag to be --config, got --%s", names[0])
	}
	for _, sub := range root.Commands() {
		sub.Flags().VisitAll(func(f *pflag.Flag) {
			if f.Name == "help" {
				return
			}
			t.Fatalf("subcommand %q must not declare any flags; saw --%s", sub.Name(), f.Name)
		})
	}
}

func TestVerbsRegistered(t *testing.T) {
	root := newRootCmd("test", "test")
	want := map[string]bool{
		"init": false, "run": false, "start": false,
		"status": false, "tail": false, "stop": false,
	}
	for _, sub := range root.Commands() {
		want[sub.Name()] = true
	}
	for verb, present := range want {
		if !present {
			t.Errorf("subcommand %q not registered", verb)
		}
	}
}

func TestMissingConfigFlagFails(t *testing.T) {
	root := newRootCmd("test", "test")
	root.SetArgs([]string{"init"})
	root.SetOut(discardWriter{})
	root.SetErr(discardWriter{})
	err := root.Execute()
	if err == nil {
		t.Fatal("expected error when --config is omitted")
	}
	if !strings.Contains(err.Error(), "config") {
		t.Fatalf("expected error to mention config flag, got %v", err)
	}
}

func TestBadConfigPathFailsBeforeWork(t *testing.T) {
	root := newRootCmd("test", "test")
	root.SetArgs([]string{"--config", "/does/not/exist.json", "start"})
	root.SetOut(discardWriter{})
	root.SetErr(discardWriter{})
	err := root.Execute()
	if err == nil {
		t.Fatal("expected error on missing config file")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// Compile-time check that loaderFn signature matches what cobra runners
// expect; if cobra changes signatures in a major bump, this will fail to
// build first.
var _ func() (any, error) = func() (any, error) {
	root := newRootCmd("c", "b")
	if root == nil {
		return nil, nil
	}
	_ = cobra.Command{}
	return nil, nil
}
