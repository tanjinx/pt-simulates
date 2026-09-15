package singlehost

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

func mysqlConfig(cfg DatabaseConfig) (*mysql.Config, error) {
	values := map[string]string{}
	if cfg.DefaultsFile != "" {
		var err error
		values, err = readClientDefaults(cfg.DefaultsFile)
		if err != nil {
			return nil, err
		}
	}

	user := cfg.User
	if user == "" {
		user = values["user"]
	}
	password := cfg.Password
	if password == "" {
		password = values["password"]
	}
	if user == "" {
		return nil, fmt.Errorf("database user is empty")
	}

	driverCfg := mysql.NewConfig()
	driverCfg.User = user
	driverCfg.Passwd = password
	driverCfg.DBName = cfg.Schema
	driverCfg.MultiStatements = false

	if cfg.Socket != "" {
		driverCfg.Net = "unix"
		driverCfg.Addr = cfg.Socket
	} else {
		port := cfg.Port
		if port == 0 {
			port = 3306
		}
		driverCfg.Net = "tcp"
		driverCfg.Addr = cfg.Host + ":" + strconv.Itoa(port)
	}
	return driverCfg, nil
}

func readClientDefaults(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open defaults file: %w", err)
	}
	defer f.Close()

	values := map[string]string{}
	inClient := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inClient = strings.EqualFold(strings.TrimSpace(line[1:len(line)-1]), "client")
			continue
		}
		if !inClient {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if ok {
			values[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read defaults file: %w", err)
	}
	return values, nil
}
