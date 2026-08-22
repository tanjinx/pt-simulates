package schema

import (
	"strings"
	"testing"
)

func TestSecondaryKeysParsesAllElevenIndexes(t *testing.T) {
	keys := SecondaryKeys()
	// The files table has 11 secondary
	// indexes (tenant_id_2 through highlight_type_tenant_id).
	if got, want := len(keys), 11; got != want {
		names := make([]string, len(keys))
		for i, k := range keys {
			names[i] = k.Name
		}
		t.Fatalf("secondary key count: got %d want %d (%v)", got, want, names)
	}
	wantNames := []string{
		"tenant_id_2", "tenant_id_3", "tenant_id_4", "tenant_id_5",
		"tenant_id_6", "tenant_id_7", "tenant_id_8",
		"user_and_tenant", "tenant_id_date_thumbnail_retrieved",
		"tenant_id", "highlight_type_tenant_id",
	}
	for i, want := range wantNames {
		if keys[i].Name != want {
			t.Errorf("keys[%d]=%q want %q", i, keys[i].Name, want)
		}
		if !strings.HasPrefix(keys[i].ColumnExpr, "(") {
			t.Errorf("keys[%d].ColumnExpr should start with `(`: %q",
				i, keys[i].ColumnExpr)
		}
	}
}

func TestCreateDDLPKOnlyHasNoSecondaryKeys(t *testing.T) {
	ddl := CreateDDLPKOnly()
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Fatal("PRIMARY KEY missing from PK-only DDL")
	}
	for _, k := range SecondaryKeys() {
		if strings.Contains(ddl, "KEY `"+k.Name+"`") {
			t.Errorf("secondary key %q leaked into PK-only DDL", k.Name)
		}
	}
	// Critical: no trailing comma on PRIMARY KEY line — the CREATE TABLE
	// would otherwise have a syntax error.
	if strings.Contains(ddl, "PRIMARY KEY (`id`),") {
		t.Fatal("PRIMARY KEY line still carries trailing comma")
	}
	// Round-trip sanity: every required schema marker still present.
	for _, marker := range []string{
		"CREATE TABLE IF NOT EXISTS `files`",
		"ENGINE=InnoDB",
		"ROW_FORMAT=COMPRESSED",
	} {
		if !strings.Contains(ddl, marker) {
			t.Errorf("PK-only DDL missing %q", marker)
		}
	}
}

func TestAddKeyStmtShape(t *testing.T) {
	keys := SecondaryKeys()
	if len(keys) == 0 {
		t.Fatal("no keys parsed; cannot test ADD KEY shape")
	}
	got := keys[0].AddKeyStmt("repro_db", "files")
	for _, want := range []string{
		"ALTER TABLE `repro_db`.`files`",
		"ADD KEY `tenant_id_2`",
		"(`tenant_id`,`is_deleted`,`is_public`,`date_create`)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ADD KEY stmt missing %q: %s", want, got)
		}
	}
}
