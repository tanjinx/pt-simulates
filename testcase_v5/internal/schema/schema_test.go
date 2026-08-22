package schema

import (
	"strings"
	"testing"
)

func TestVerifyEmbed(t *testing.T) {
	if err := Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestCreateDDLShape(t *testing.T) {
	ddl := CreateDDL()
	if Table != "files" {
		t.Fatalf("parsed table = %q; want files", Table)
	}
	for _, marker := range []string{
		"CREATE TABLE IF NOT EXISTS `files`",
		"ENGINE=InnoDB",
		"ROW_FORMAT=COMPRESSED",
		"KEY `tenant_id_4` (`tenant_id`,`date_create`)",
		"PRIMARY KEY (`id`)",
	} {
		if !strings.Contains(ddl, marker) {
			t.Errorf("embedded DDL missing %q", marker)
		}
	}
}

func TestDropDDL(t *testing.T) {
	if got, want := DropDDL(), "DROP TABLE IF EXISTS `files`"; got != want {
		t.Fatalf("DropDDL=%q want %q", got, want)
	}
}
