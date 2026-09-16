package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

func TestParseDefaultsClient(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantUser string
		wantPass string
	}{
		{
			name:     "basic",
			content:  "[client]\nuser=root\npassword=secret\n",
			wantUser: "root",
			wantPass: "secret",
		},
		{
			name:     "spaces around equals",
			content:  "[client]\nuser = dba\npassword = p@ss\n",
			wantUser: "dba",
			wantPass: "p@ss",
		},
		{
			name:     "quoted values",
			content:  "[client]\nuser=\"app_user\"\npassword='s3cret'\n",
			wantUser: "app_user",
			wantPass: "s3cret",
		},
		{
			name:     "comments and other sections",
			content:  "# top comment\n[mysqld]\nuser=daemon\n[client]\n; inline comment\nuser=reader\npassword=rpass\n[other]\nuser=nope\n",
			wantUser: "reader",
			wantPass: "rpass",
		},
		{
			name:     "no password",
			content:  "[client]\nuser=nopass\n",
			wantUser: "nopass",
			wantPass: "",
		},
		{
			name:     "case insensitive section",
			content:  "[CLIENT]\nuser=upper\npassword=pw\n",
			wantUser: "upper",
			wantPass: "pw",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "my.cnf")
			if err := os.WriteFile(path, []byte(tt.content), 0600); err != nil {
				t.Fatal(err)
			}
			user, pass, err := parseDefaultsClient(path)
			if err != nil {
				t.Fatal(err)
			}
			if user != tt.wantUser {
				t.Errorf("user = %q, want %q", user, tt.wantUser)
			}
			if pass != tt.wantPass {
				t.Errorf("password = %q, want %q", pass, tt.wantPass)
			}
		})
	}
}

func TestResolveDefaultsConfigOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "my.cnf")
	if err := os.WriteFile(path, []byte("[client]\nuser=file_user\npassword=file_pass\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := config.Endpoint{
		DefaultsFile: path,
		User:         "explicit_user",
		Schema:       "test",
	}
	if err := resolveDefaults(&e); err != nil {
		t.Fatal(err)
	}
	if e.User != "explicit_user" {
		t.Errorf("user = %q, want explicit_user (config should win)", e.User)
	}
	if e.Password != "file_pass" {
		t.Errorf("password = %q, want file_pass", e.Password)
	}
}

func TestResolveDefaultsNoFile(t *testing.T) {
	e := config.Endpoint{User: "u", Schema: "s"}
	if err := resolveDefaults(&e); err != nil {
		t.Errorf("no defaults_file should be a no-op, got %v", err)
	}
}

func TestResolveDefaultsMissingUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "my.cnf")
	if err := os.WriteFile(path, []byte("[client]\npassword=only\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := config.Endpoint{DefaultsFile: path, Schema: "test"}
	if err := resolveDefaults(&e); err == nil {
		t.Error("expected error when defaults file has no user and config has no user")
	}
}
