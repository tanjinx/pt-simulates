# Single-host workload

`tc-single-host` runs read and write workers in parallel against an existing
`files` table on one MySQL host. It does not create or seed the table.

Each write connection executes and verifies:

```sql
SET SESSION sql_log_bin = 0;
```

The writes modify the local table but are not written to the binary log.

## Build

From `testcase_v5`:

```bash
go build -o build/tc-single-host ./cmd/tc-single-host
```

To build a Linux binary elsewhere:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o build/tc-single-host-linux-amd64 ./cmd/tc-single-host
```

## Select a test range

Choose a tenant and a deliberately bounded ID range. The selected rows will be
updated repeatedly.

```bash
sudo mysql \
  --defaults-file=/etc/slack.d/msql_vt_dba_a.ini \
  -h 127.0.0.1 -BNe "
SELECT tenant_id, MIN(id), MAX(id), COUNT(*)
FROM vt_qareshard1.files
GROUP BY tenant_id
ORDER BY COUNT(*) DESC
LIMIT 10;"
```

## Configure

Copy the example:

```bash
cp single-host/config.example.json single-host/config.json
```

Edit `single-host/config.json`:

```json
{
  "database": {
    "defaults_file": "/etc/slack.d/msql_vt_dba_a.ini",
    "host": "127.0.0.1",
    "port": 3306,
    "socket": "",
    "schema": "vt_qareshard1",
    "table": "files"
  },
  "workload": {
    "tenant_id": 12345,
    "id_min": 100000,
    "id_max": 100100,
    "read_workers": 1,
    "write_workers": 1,
    "read_batch_size": 10,
    "read_iterations": 1,
    "write_iterations": 1
  }
}
```

Use either `host` and `port` for TCP or `socket` for a Unix socket. When
`socket` is set, it takes precedence over `host` and `port`.

The defaults file must contain a `[client]` section with the MySQL username and
password. The account must be able to update the target table and disable
binary logging for its session.

## Run

Start with a one-iteration smoke test:

```bash
sudo ./build/tc-single-host --config single-host/config.json
```

After verifying the selected range and server behavior, increase the worker and
iteration counts in the config and run the same command again.

The total operation counts are:

```text
read queries  = read_workers  × read_iterations
write queries = write_workers × write_iterations
```

Each read returns at most `read_batch_size` rows. Each write updates every row
matching `tenant_id` and the inclusive `id_min` to `id_max` range.

Press `Ctrl-C` to cancel the run.
