package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	// MySQL driver registered for database/sql (pinned via go.mod). This
	// driver is the only acceptable choice.
	_ "github.com/go-sql-driver/mysql"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
)

// Open returns a configured *sql.DB pool for an endpoint. The pool shape is
// fixed: SetMaxOpenConns = maxConns + 1, SetMaxIdleConns = maxConns,
// SetConnMaxLifetime = 0 (let server-side timeouts decide). DSN params
// parseTime=true (so date_create lands as time.Time when we add a typed
// scan later), multiStatements=false, interpolateParams=false (binary
// protocol on the wire — the whole reason for the language choice).
func Open(endpoint config.Endpoint, maxConns int) (*sql.DB, error) {
	dsn := DSN(endpoint)
	driver, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql endpoint: %w", err)
	}
	driver.SetMaxOpenConns(maxConns + 1)
	driver.SetMaxIdleConns(maxConns)
	driver.SetConnMaxLifetime(0)
	driver.SetConnMaxIdleTime(0)
	// Wake up one connection so misconfigured endpoints fail at startup, not
	// at first per-tenant query (fail-fast).
	if err := driver.Ping(); err != nil {
		_ = driver.Close()
		return nil, fmt.Errorf("ping endpoint: %w (dsn redacted)", err)
	}
	// Sanity bound — DB drivers cap conn lifetime quickly when the server
	// closes idle conns; SetConnMaxLifetime(15m) is the standard safe default
	// even with server-side wait_timeout in the hour range.
	driver.SetConnMaxLifetime(15 * time.Minute)
	return driver, nil
}

// DSN renders the go-sql-driver/mysql DSN. Required params:
// parseTime=true, multiStatements=false, interpolateParams=false. Credentials
// come from the config; we never read them from environment variables.
func DSN(e config.Endpoint) string {
	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("multiStatements", "false")
	params.Set("interpolateParams", "false")
	params.Set("charset", "utf8mb4")
	params.Set("collation", "utf8mb4_general_ci")
	params.Set("loc", "UTC")

	var auth strings.Builder
	auth.WriteString(e.User)
	if e.Password != "" {
		auth.WriteString(":")
		auth.WriteString(e.Password)
	}
	var addr string
	if e.Socket != "" {
		addr = "unix(" + e.Socket + ")"
	} else {
		port := e.Port
		if port == 0 {
			port = 3306
		}
		addr = fmt.Sprintf("tcp(%s:%d)", e.Host, port)
	}
	return fmt.Sprintf("%s@%s/%s?%s", auth.String(), addr, e.Schema, params.Encode())
}
