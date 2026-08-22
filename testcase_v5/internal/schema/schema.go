package schema

import (
	_ "embed"
	"errors"
	"strings"
)

//go:embed embedded.sql
var raw string

// Table is the single table name this harness operates on. It is
// derived from the embedded DDL once at package init so the rest of the
// code can refer to schema.Table without re-parsing.
var Table = parseTableName(raw)

// CreateDDL returns the byte-identical v4 schema.sql contents.
func CreateDDL() string { return raw }

// DropDDL returns the DROP statement complementing CreateDDL. Schema embeds
// `CREATE TABLE IF NOT EXISTS files` — DropDDL mirrors that table name so
// init's force_init=true path can wipe a previous run cleanly.
func DropDDL() string { return "DROP TABLE IF EXISTS `" + Table + "`" }

// Verify panics on package init if the embedded DDL is empty or does not
// match the expected table name. The Makefile's schema-check target also
// verifies byte-identical parity with v4 — this is the in-binary backstop.
func Verify() error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("schema: embedded DDL is empty")
	}
	if Table == "" {
		return errors.New("schema: unable to parse table name from embedded DDL")
	}
	return nil
}

func parseTableName(ddl string) string {
	const marker = "CREATE TABLE IF NOT EXISTS `"
	i := strings.Index(ddl, marker)
	if i < 0 {
		return ""
	}
	rest := ddl[i+len(marker):]
	j := strings.Index(rest, "`")
	if j < 0 {
		return ""
	}
	return rest[:j]
}
