package singlehost

import "fmt"

func readSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT * FROM `%s`.`%s` IGNORE INDEX (`PRIMARY`) "+
			"WHERE tenant_id = ? AND id > ? ORDER BY id ASC LIMIT %d",
		schema, table, batchSize)
}

func writeSQL(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET "+
			"contents_enc = UNHEX(SHA2(CONCAT(id, ':contents:', ?, ':', ?), 256)), "+
			"contents_highlight_enc = UNHEX(SHA2(CONCAT(id, ':highlight:', ?, ':', ?), 256)), "+
			"metadata_enc = UNHEX(SHA2(CONCAT(id, ':metadata:', ?, ':', ?), 256)), "+
			"version = version + 1 "+
			"WHERE tenant_id = ? AND id BETWEEN ? AND ? AND @@session.sql_log_bin = 0",
		schema, table)
}
