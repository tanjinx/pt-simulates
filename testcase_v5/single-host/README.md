# Single-host workload

Single-host mode runs the v5 workload against an existing `files` table on one
MySQL server. Reads and writes execute in parallel on separate connection
pools, but both pools must point to the same server and table.

This mode:

- does not create, drop, alter, or seed the table;
- does not require replication;
- does not monitor replication lag;
- does not run the optional noise sidecar (validation rejects `noise.enabled=true`);
- restricts writes to one tenant and an inclusive ID range; and
- disables and verifies session binary logging on every write connection.

The selected rows are modified repeatedly. Use this only on an isolated host
where the selected data is disposable or recoverable.

## Query shapes

Single-host mode runs the four v5 read shapes:

1. Backfill scan by `tenant_id`, `date_create`, and the configured ID range.
2. Range-estimation query using `tenant_id`, deletion/public flags, and
   `date_create`.
3. MRR query using real `external_id` values.
4. Forward secondary-index scan using `IGNORE INDEX(PRIMARY)`.

Each writer repeatedly runs the original per-row update shape:

```sql
UPDATE files
SET contents_enc = ?,
    contents_highlight_enc = ?,
    metadata_enc = ?,
    version = version + 1
WHERE id = ?
  AND tenant_id = ?
  AND @@session.sql_log_bin = 0;
```

Before writing, every writer connection runs:

```sql
SET SESSION sql_log_bin = 0;
SELECT @@session.sql_log_bin;
```

The worker stops unless the returned value is `0`.

## Configure

From `testcase_v5`, copy the template:

```bash
cp tc_config.single_host.json tc_config.single_host.local.json
```

The template sets `run.mode` to `"single_host"` and `replication_check.mode`
to `"disabled"`. Both are required; validation rejects any other values for
this mode.

Set both database endpoints to the same MySQL server and table. The accounts
may differ, but the write account must be able to update the table and execute
`SET SESSION sql_log_bin = 0`. Validation enforces that the read and write
endpoints share the same host, port (or socket), schema, and table.

### Credentials via defaults file (recommended)

Point `defaults_file` at a MySQL option file containing a `[client]` section
with `user` and `password`. This avoids storing passwords in the JSON config:

```json
"database": {
  "write": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "",
    "password": "",
    "defaults_file": "/etc/slack.d/msql_vt_app_a.ini",
    "schema": "vt_qareshard1",
    "table": "files",
    "socket": ""
  },
  "read": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "",
    "password": "",
    "defaults_file": "/etc/slack.d/msql_vt_app_a.ini",
    "schema": "vt_qareshard1",
    "table": "files",
    "socket": ""
  }
}
```

The defaults file is the same format used by `mysql --defaults-file=...`:

```ini
[client]
user=vt_app
password=secret
```

When both `defaults_file` and explicit `user`/`password` are set, the explicit
values take precedence.

### Inline credentials

Alternatively, set `user` and `password` directly and leave `defaults_file`
empty.

For a local TCP connection, use `127.0.0.1`. Using the machine hostname can
select a different MySQL `user@host` account.

Set the same Unix socket on both endpoints to use a socket connection. A
configured socket takes precedence over host and port.

## Select real values

Select a tenant with enough rows and choose a deliberately small ID range for
the initial test:

```sql
SELECT tenant_id, MIN(id), MAX(id), COUNT(*)
FROM vt_qareshard1.files
GROUP BY tenant_id
ORDER BY COUNT(*) DESC
LIMIT 10;
```

Find the date range and real external IDs inside the selected range:

```sql
SELECT MIN(date_create), MAX(date_create)
FROM vt_qareshard1.files
WHERE tenant_id = ?
  AND id BETWEEN ? AND ?;

SELECT external_id
FROM vt_qareshard1.files
WHERE tenant_id = ?
  AND id BETWEEN ? AND ?
  AND external_id <> ''
LIMIT 10;
```

Configure those values under `run.single_host`:

```json
"single_host": {
  "tenant_id": 12345,
  "id_min": 100000,
  "id_max": 100100,
  "date_create_min": 1700000000,
  "date_create_max": 1800000000,
  "external_ids": ["existing-external-id"],
  "backfill_read_workers": 1,
  "write_workers": 1,
  "read_iterations": 1000,
  "write_iterations": 1000
}
```

`backfill_read_workers` controls the backfill query. The existing search-mix
worker counts control the other three read shapes:

- `range_estimate_workers`
- `mrr_workers`
- `forward_refscan_workers`

Set `search_mix.enabled` to `true` to run these workers. MRR workers require
real `external_id` values from the selected tenant.

## Build and run

```bash
make build
```

For Linux:

```bash
make build-linux
```

Start with one worker and one iteration for every enabled shape:

```bash
./build/tc-repro --config tc_config.single_host.local.json run
```

After verifying the target range and MySQL behavior, increase the worker and
iteration counts and run the same command again.

Press `Ctrl-C` to stop an active run.

## Operation counts

The total read count is the sum of all enabled read workers multiplied by
`read_iterations`:

```text
reads = (
  backfill_read_workers
  + range_estimate_workers
  + mrr_workers
  + forward_refscan_workers
) × read_iterations
```

The write count is:

```text
writes = write_workers × write_iterations
```

Each write targets one row selected round-robin from the configured tenant and
ID range. Write payloads are deterministic SHA-256 hashes derived from the row
ID, a label byte, and the iteration number.

## Results

Run logs and summaries are written beneath the configured `artifacts.dir`.
A clean run means the workload completed; it does not prove that the server
cannot encounter the investigated crash.
