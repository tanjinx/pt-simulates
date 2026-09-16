package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

// resolveDefaults reads a MySQL defaults file (--defaults-file style) and
// fills in User and Password on the endpoint from the [client] section.
// Fields already set on the endpoint take precedence.
func resolveDefaults(e *config.Endpoint) error {
	if e.DefaultsFile == "" {
		return nil
	}
	user, password, err := parseDefaultsClient(e.DefaultsFile)
	if err != nil {
		return fmt.Errorf("defaults_file %q: %w", e.DefaultsFile, err)
	}
	if e.User == "" {
		e.User = user
	}
	if e.Password == "" {
		e.Password = password
	}
	if e.User == "" {
		return fmt.Errorf("defaults_file %q: no user in [client] section and none in config", e.DefaultsFile)
	}
	return nil
}

// parseDefaultsClient extracts user and password from the [client] section
// of a MySQL defaults file. Handles key=value and key = value forms.
// Lines starting with # or ; are comments. Values may be optionally quoted.
func parseDefaultsClient(path string) (user, password string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	inClient := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' {
			inClient = strings.EqualFold(line, "[client]")
			continue
		}
		if !inClient {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		val = unquoteINI(val)
		switch strings.ToLower(key) {
		case "user":
			user = val
		case "password":
			password = val
		}
	}
	return user, password, scanner.Err()
}

func unquoteINI(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
