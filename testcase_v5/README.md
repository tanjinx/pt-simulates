# testcase_v5

A small Go program that drives a read-heavy InnoDB workload against a MySQL or
Percona Server source and replica, to try to reproduce a `mysqld` SIGSEGV we have been
investigating.

The crash happens on the replica, during an ordinary read-only `SELECT`: while
descending a B-tree, InnoDB takes a shared latch on a buffer-pool page
(`btr_cur_search_to_nth_level` -> `buf_page_get_gen` -> `rw_lock_s_lock`) and faults
at `pthread_self`. The program reproduces that read path, a scan, transform, and
update loop with concurrent range-scan SELECTs, reading from the replica and writing
to the source.

It replaces the older Python versions (v1 to v4) with a single static binary, so it
seeds faster and drives the load more steadily.

## Build

```bash
make build          # builds ./build/tc-repro for the current platform
```

Or just use the prebuilt Linux binary in `bin/tc-repro-linux-amd64`, no Go toolchain needed.

## Configure

Copy a template and fill in your own endpoints and credentials:

```bash
cp tc_config.json tc_config.local.json
$EDITOR tc_config.local.json     # set the read (replica) and write (source) connections
```

The templates ship with placeholder values only. Keep your filled-in copy out of git.

## Run

```bash
./bin/tc-repro-linux-amd64 --config tc_config.local.json init    # create and seed the test table
./bin/tc-repro-linux-amd64 --config tc_config.local.json run     # run the scan / encrypt / update loop
```

`tc-repro` takes one flag, `--config <path>`, plus a verb: `init`, `run`, `start`
(init then run), `status`, `tail`, or `stop`. Everything else lives in the config file.

To drive the read side harder, use `tc_config.search_mix.json`, which runs the query
shapes seen at crash time in parallel.

## Background

The comparison of the crash cores behind this test is in
[docs/cross-core-comparison.md](docs/cross-core-comparison.md).

## A note on results

This program reproduces the workload, not the crash itself. A clean run does not prove a
server is safe; the crash needs a condition we cannot trigger from software. Treat a
clean run as the workload running as expected, nothing more.
