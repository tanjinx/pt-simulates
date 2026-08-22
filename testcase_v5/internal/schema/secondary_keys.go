package schema

import (
	"fmt"
	"strings"
)

// SecondaryKey is one secondary index parsed out of the embedded schema.
// Name is the bare index name (no backticks); ColumnExpr is the column
// list verbatim from the DDL, including the parentheses and any prefix
// lengths (e.g. `tenant_id_5` (`tenant_id`,`is_deleted`,`external_id`(191)) ).
type SecondaryKey struct {
	Name       string
	ColumnExpr string
}

// SecondaryKeys returns every secondary KEY definition in the embedded
// schema, preserving the DDL order. PRIMARY KEY is NOT included — it
// stays on the table for the bulk-load path.
func SecondaryKeys() []SecondaryKey {
	return parseSecondaryKeys(raw)
}

// CreateDDLPKOnly returns the embedded CREATE TABLE statement with every
// secondary KEY line removed and the trailing comma after PRIMARY KEY
// cleaned up. The end-state DDL is structurally identical to running
// CreateDDL() then DROP INDEX for each secondary key.
func CreateDDLPKOnly() string {
	return stripSecondaryKeys(raw)
}

// AddKeyStmt renders an ALTER TABLE ADD KEY statement for `schema`.`table`.
func (k SecondaryKey) AddKeyStmt(schema, table string) string {
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD KEY `%s` %s",
		schema, table, k.Name, k.ColumnExpr)
}

// parseSecondaryKeys walks the DDL line-by-line and extracts each
// `  KEY \`<name>\` (<cols>),` line. The trailing comma is dropped from
// ColumnExpr.
func parseSecondaryKeys(ddl string) []SecondaryKey {
	var out []SecondaryKey
	for _, line := range strings.Split(ddl, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "KEY `") {
			continue
		}
		// trimmed looks like: KEY `<name>` (<cols>),  or no trailing comma on last
		rest := strings.TrimPrefix(trimmed, "KEY `")
		closeQuote := strings.Index(rest, "`")
		if closeQuote < 0 {
			continue
		}
		name := rest[:closeQuote]
		expr := strings.TrimSpace(rest[closeQuote+1:])
		expr = strings.TrimSuffix(expr, ",")
		out = append(out, SecondaryKey{Name: name, ColumnExpr: expr})
	}
	return out
}

// stripSecondaryKeys returns the CREATE TABLE DDL with every `  KEY \`<name>\“
// line removed. The last surviving line inside the column list (PRIMARY KEY)
// must lose its trailing comma so the CREATE TABLE remains valid.
func stripSecondaryKeys(ddl string) string {
	lines := strings.Split(ddl, "\n")
	var kept []string
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "KEY `") {
			continue
		}
		kept = append(kept, line)
	}
	// Find the closing `)` line (table-options line). The line immediately
	// before it is the last column-list line; ensure it ends without a
	// trailing comma.
	for i := len(kept) - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(kept[i]), ") ENGINE=") {
			if i == 0 {
				break
			}
			prev := strings.TrimRight(kept[i-1], " \t")
			kept[i-1] = strings.TrimSuffix(prev, ",")
			break
		}
	}
	return strings.Join(kept, "\n")
}
