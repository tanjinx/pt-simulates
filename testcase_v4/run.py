#!/usr/bin/env python3
"""BackfillJob scan/update workload mimic."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
import json
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Set

try:
    import pymysql
    from pymysql.cursors import DictCursor
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "PyMySQL is required."
    ) from exc


BASE_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = BASE_DIR / "artifacts"
DEFAULT_CONFIG_PATH = BASE_DIR / "tc_config.json"
SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "sql" / "schema.sql"
DEFAULT_SCHEMA = os.getenv("WORKLOAD_DB", "repro_tc")
DEFAULT_TABLE = "files"

PROFILE_CYCLE: Tuple[str, ...] = (
    "small",
    "small",
    "small",
    "medium",
    "medium",
    "medium",
    "large",
    "large",
    "large",
    "large",
)

PROFILE_SPECS = {
    "small": {
        "contents": (2 * 1024, 8 * 1024),
        "highlight": (1 * 1024, 4 * 1024),
        "metadata_text": 2048,
        "external_ptr": (512, 2 * 1024),
    },
    "medium": {
        "contents": (64 * 1024, 256 * 1024),
        "highlight": (32 * 1024, 128 * 1024),
        "metadata_text": 4096,
        "external_ptr": (4 * 1024, 16 * 1024),
    },
    "large": {
        "contents": (1 * 1024 * 1024, 2 * 1024 * 1024),
        "highlight": (512 * 1024, 1536 * 1024),
        "metadata_text": 8192,
        # external_ptr column is a BLOB (64 KiB max), so cap below that ceiling.
        "external_ptr": (32 * 1024, 60 * 1024),
    },
}

INSERT_COLUMNS: Tuple[str, ...] = (
    "id",
    "tenant_id",
    "user_id",
    "date_create",
    "token",
    "pub_token",
    "size",
    "is_stored",
    "original_name",
    "stored_name",
    "title",
    "mimetype",
    "parent_id",
    "contents",
    "contents_enc",
    "contents_highlight",
    "contents_highlight_enc",
    "highlight_type",
    "metadata",
    "metadata_enc",
    "external_url",
    "is_deleted",
    "date_delete",
    "is_public",
    "pub_shared",
    "last_indexed",
    "source",
    "external_id",
    "service_type_id",
    "service_id",
    "is_multitenant",
    "original_tenant_id",
    "tenants_shared_with",
    "service_tenant_id",
    "is_tombstoned",
    "thumbnail_version",
    "date_thumbnail_retrieved",
    "external_ptr",
    "md5",
    "metadata_version",
    "unencrypted_metadata",
    "restriction_type",
    "version",
)


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: Optional[str]
    socket: Optional[str]
    db: Optional[str] = None

    def with_db(self, database: str) -> "DBConfig":
        return replace(self, db=database)


@dataclass
class RunStats:
    batches: int = 0
    rows: int = 0
    selects: int = 0
    updates: int = 0
    total_batch_ms: float = 0.0
    start_ts: float = 0.0

    def mark_batch(self, rows_in_batch: int, batch_ms: float) -> None:
        self.batches += 1
        self.rows += rows_in_batch
        self.updates += rows_in_batch
        self.total_batch_ms += batch_ms

    @property
    def avg_batch_ms(self) -> float:
        if not self.batches:
            return 0.0
        return self.total_batch_ms / self.batches


@dataclass
class TenantCounters:
    tenant_id: int
    worker_id: int
    start_ts: float = 0.0
    batches: int = 0
    select_count: int = 0
    update_count: int = 0
    rows_selected: int = 0
    rows_updated: int = 0
    end_ts: float = 0.0


@dataclass
class WorkerCounters:
    worker_id: int
    start_ts: float = 0.0
    end_ts: float = 0.0
    tenants_completed: int = 0
    batches: int = 0
    select_count: int = 0
    update_count: int = 0
    rows_selected: int = 0
    rows_updated: int = 0


PRINT_LOCK = threading.Lock()


def log_line(message: str, *, stream: Optional[object] = None) -> None:
    target = stream or sys.stdout
    with PRINT_LOCK:
        print(message, file=target, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BackfillJob workload mimic.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--init", action="store_true", help="Create schema/table and seed data.")
    mode.add_argument("--run", action="store_true", help="Execute the scan/update workload.")
    parser.add_argument("--config", help="Path to JSON config (default: tc_config.json).")
    parser.add_argument(
        "--allow-env-overrides",
        action="store_true",
        help="Allow environment variables to override JSON config keys (default: off).",
    )
    parser.add_argument("--schema", help="Schema name (overrides config).")
    parser.add_argument("--table", help="Table name (overrides config).")
    parser.add_argument("--tenants", type=int, help="Number of tenants (overrides config).")
    parser.add_argument("--rows-per-tenant", type=int, help="Rows per tenant (overrides config).")
    parser.add_argument("--batch-size", type=int, help="Batch size for SELECT/UPDATE (must stay 10).")
    parser.add_argument("--tenant-id-base", type=int, help="Starting tenant_id (overrides config).")
    parser.add_argument("--start-id", type=int, help="Starting row id (overrides config).")
    parser.add_argument("--date-base", type=int, help="Base unix timestamp (overrides config).")
    parser.add_argument("--date-step", type=int, help="Seconds added per row (overrides config).")
    parser.add_argument("--seed", type=int, help="RNG seed (overrides config).")
    parser.add_argument("--insert-chunk", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--insert-batch-rows",
        type=int,
        help="Max rows per INSERT batch during --init (overrides config).",
    )
    parser.add_argument(
        "--insert-batch-bytes",
        type=int,
        help="Max bytes per INSERT batch during --init (overrides config).",
    )
    parser.add_argument(
        "--init-progress-rows",
        type=int,
        help="Print --init progress every N inserted rows (overrides config).",
    )
    parser.add_argument(
        "--tenant-progress-rows",
        type=int,
        help="Print per-tenant --init progress every N rows (overrides config).",
    )
    parser.add_argument(
        "--init-workers",
        type=int,
        default=None,
        help="Parallel workers for --init seeding (overrides config).",
    )
    parser.add_argument(
        "--max-parallel-tenants",
        type=int,
        help="Maximum number of tenants to seed in parallel (overrides config).",
    )
    parser.add_argument(
        "--relax-durability",
        dest="relax_durability",
        action="store_true",
        help="During --init set innodb_flush_log_at_trx_commit=2 and sync_binlog=0.",
    )
    parser.add_argument(
        "--no-relax-durability",
        dest="relax_durability",
        action="store_false",
        help="Keep durability settings untouched during --init.",
    )
    parser.set_defaults(relax_durability=None)
    parser.add_argument("--force", action="store_true", help="Drop table during --init.")
    parser.add_argument("--progress-interval", type=int, default=100, help="Print progress every N batches (default: %(default)s).")

    parser.add_argument("--write-host", help="Master host override.")
    parser.add_argument("--write-port", type=int, help="Master port override.")
    parser.add_argument("--write-user", help="Master username override.")
    parser.add_argument("--write-password", help="Master password override.")
    parser.add_argument("--write-socket", help="Master socket path override.")

    parser.add_argument("--read-host", help="Replica host override.")
    parser.add_argument("--read-port", type=int, help="Replica port override.")
    parser.add_argument("--read-user", help="Replica username override.")
    parser.add_argument("--read-password", help="Replica password override.")
    parser.add_argument("--read-socket", help="Replica socket path override.")
    parser.add_argument(
        "--run-max-parallel-tenants",
        type=int,
        help="Maximum number of tenants to process concurrently during --run (overrides config).",
    )
    parser.add_argument(
        "--continue-on-tenant-failure",
        action="store_true",
        help="Continue seeding other tenants even if one fails (default: abort immediately).",
    )
    parser.add_argument(
        "--tenant-log-interval",
        type=int,
        help="Per-tenant batch interval for detailed logs during --run (overrides config).",
    )
    parser.add_argument(
        "--replication-log-interval",
        type=int,
        help="Seconds between replication health snapshots during --run (overrides config).",
    )

    args = parser.parse_args()
    if args.insert_chunk is not None:
        args.insert_batch_rows = args.insert_chunk
    return args


def load_json_config(path: Optional[str]) -> Tuple[Path, Dict[str, object]]:
    config_path = Path(path).expanduser() if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid config
        raise SystemExit(f"Invalid JSON in {config_path}: {exc}") from exc
    return config_path, data


def validate_config_schema(config: Dict[str, object]) -> None:
    def require(mapping: Dict[str, object], key: str) -> Dict[str, object]:
        if key not in mapping:
            raise SystemExit(f"Config missing required key: {key}")
        value = mapping[key]
        if not isinstance(value, dict):
            raise SystemExit(f"Config key {key} must be an object.")
        return value

    database = require(config, "database")
    write_db = require(database, "write")
    read_db = require(database, "read")
    for subkey in ("host", "schema", "table"):
        if subkey not in write_db:
            raise SystemExit(f"Config missing database.write.{subkey}")
    for subkey in ("host", "schema", "table"):
        if subkey not in read_db:
            raise SystemExit(f"Config missing database.read.{subkey}")
    init_cfg = require(config, "init")
    for key in ("tenants", "rows_per_tenant", "start_id", "date_base", "date_step"):
        if key not in init_cfg:
            raise SystemExit(f"Config missing init.{key}")
    run_cfg = require(config, "run")
    batch_size = run_cfg.get("batch_size")
    if batch_size != 10:
        raise SystemExit(f"Config run.batch_size must be 10 (got {batch_size})")


def build_profile_cycle(blob_profile: Dict[str, int]) -> Tuple[str, ...]:
    entries: List[str] = []
    ordered = (("small", "small_pct"), ("medium", "medium_pct"), ("large", "large_pct"))
    for label, key in ordered:
        count = int(blob_profile.get(key, 0))
        if count > 0:
            entries.extend([label] * count)
    return tuple(entries) if entries else PROFILE_CYCLE


def apply_config_overrides(
    args: argparse.Namespace, config: Dict[str, object], config_path: Path
) -> Tuple[List[str], List[str]]:
    env_overrides: List[str] = []
    ignored_env: List[str] = []
    env_allowed = bool(getattr(args, "allow_env_overrides", False))

    def pick(
        cli_value: Optional[object],
        config_value: Optional[object],
        env_name: Optional[str] = None,
        cast: Optional[callable] = None,
    ) -> Optional[object]:
        if cli_value is not None:
            return cli_value
        if env_name:
            env_val = os.getenv(env_name)
            if env_val not in (None, ""):
                if env_allowed:
                    env_overrides.append(env_name)
                    return cast(env_val) if cast else env_val
                ignored_env.append(env_name)
        return config_value

    database = config.get("database", {})
    write_db = database.get("write", {})
    read_db = database.get("read", {})

    args.schema = pick(args.schema, write_db.get("schema"))
    args.table = pick(args.table, write_db.get("table", DEFAULT_TABLE))

    args.write_host = pick(args.write_host, write_db.get("host"), "WRITE_HOST")
    args.write_port = pick(args.write_port, write_db.get("port"), "WRITE_PORT", int)
    args.write_user = pick(args.write_user, write_db.get("user"), "WRITE_USER")
    args.write_password = pick(args.write_password, write_db.get("password"), "WRITE_PASSWORD")
    args.write_socket = pick(args.write_socket, write_db.get("socket"), "WRITE_SOCKET")

    args.read_host = pick(args.read_host, read_db.get("host"), "READ_HOST")
    args.read_port = pick(args.read_port, read_db.get("port"), "READ_PORT", int)
    args.read_user = pick(args.read_user, read_db.get("user"), "READ_USER")
    args.read_password = pick(args.read_password, read_db.get("password"), "READ_PASSWORD")
    args.read_socket = pick(args.read_socket, read_db.get("socket"), "READ_SOCKET")

    init_cfg = config.get("init", {})
    args.tenants = pick(args.tenants, init_cfg.get("tenants"), "TENANTS", int)
    args.rows_per_tenant = pick(args.rows_per_tenant, init_cfg.get("rows_per_tenant"), "ROWS_PER_TENANT", int)
    args.tenant_id_base = pick(args.tenant_id_base, init_cfg.get("tenant_id_base"), "TENANT_ID_BASE", int)
    args.start_id = pick(args.start_id, init_cfg.get("start_id"), "START_ID", int)
    args.date_base = pick(args.date_base, init_cfg.get("date_base"), "DATE_BASE", int)
    args.date_step = pick(args.date_step, init_cfg.get("date_step"), "DATE_STEP", int)
    args.seed = pick(args.seed, init_cfg.get("seed"), "INIT_SEED", int)
    args.insert_batch_rows = pick(args.insert_batch_rows, init_cfg.get("insert_batch_rows"), None, int)
    args.insert_batch_bytes = pick(
        args.insert_batch_bytes, init_cfg.get("insert_batch_bytes"), None, int
    )
    args.init_progress_rows = pick(
        args.init_progress_rows, init_cfg.get("init_progress_rows"), None, int
    )
    args.tenant_progress_rows = pick(
        args.tenant_progress_rows, init_cfg.get("tenant_progress_rows"), None, int
    )
    args.init_workers = pick(args.init_workers, init_cfg.get("init_workers"), "INIT_WORKERS", int)
    args.max_parallel_tenants = pick(
        args.max_parallel_tenants, init_cfg.get("max_parallel_tenants"), "MAX_PARALLEL_TENANTS", int
    )
    if not args.continue_on_tenant_failure:
        cof = init_cfg.get("continue_on_tenant_failure")
        if cof:
            args.continue_on_tenant_failure = bool(cof)
    if args.relax_durability is None:
        rd = init_cfg.get("relax_durability")
        args.relax_durability = True if rd is None else bool(rd)
    blob_profile = init_cfg.get("blob_profile", {})
    args.profile_cycle = build_profile_cycle(blob_profile)

    run_cfg = config.get("run", {})
    args.batch_size = pick(args.batch_size, run_cfg.get("batch_size"), "BATCH_SIZE", int)
    args.run_max_parallel_tenants = pick(
        args.run_max_parallel_tenants,
        run_cfg.get("max_parallel_tenants"),
        "RUN_MAX_PARALLEL_TENANTS",
        int,
    )
    args.progress_interval = pick(
        args.progress_interval,
        run_cfg.get("progress_interval_batches"),
        "RUN_PROGRESS_INTERVAL",
        int,
    )
    args.tenant_log_interval = pick(
        args.tenant_log_interval,
        run_cfg.get("tenant_log_interval_batches"),
        "RUN_TENANT_LOG_INTERVAL",
        int,
    )
    args.replication_log_interval = pick(
        args.replication_log_interval,
        run_cfg.get("replication_log_interval_seconds"),
        "RUN_REPLICATION_LOG_INTERVAL",
        int,
    )

    safety_cfg = config.get("safety", {}) or {}
    args.force_required = bool(safety_cfg.get("force_init_requires_flag"))
    args.max_runtime_seconds = safety_cfg.get("max_runtime_seconds")

    args.config_path = str(config_path)
    return env_overrides, ignored_env


def ensure_effective_args(args: argparse.Namespace) -> None:
    if not args.schema or not args.table:
        raise SystemExit("Schema/table must be defined (config is missing database.write settings).")
    if not (args.write_socket or args.write_host):
        raise SystemExit("Write connection requires host or socket.")
    if args.write_host and args.write_port is None:
        args.write_port = 3306
    if args.read_host and args.read_port is None:
        args.read_port = 3306
    if not args.read_host and not args.read_socket:
        raise SystemExit("Read connection requires host or socket.")
    if args.write_password is None:
        args.write_password = ""
    if args.read_password is None:
        args.read_password = ""
    if args.init_progress_rows is None:
        args.init_progress_rows = 100000
    if args.tenant_progress_rows is None:
        args.tenant_progress_rows = 10000
    required_ints = {
        "tenants": args.tenants,
        "rows_per_tenant": args.rows_per_tenant,
        "tenant_id_base": args.tenant_id_base,
        "start_id": args.start_id,
        "date_base": args.date_base,
        "date_step": args.date_step,
        "insert_batch_rows": args.insert_batch_rows,
        "insert_batch_bytes": args.insert_batch_bytes,
        "init_progress_rows": args.init_progress_rows,
        "tenant_progress_rows": args.tenant_progress_rows,
        "batch_size": args.batch_size,
        "seed": args.seed,
    }
    for name, value in required_ints.items():
        if value is None:
            raise SystemExit(f"Config missing value for {name}.")
    if args.batch_size != 10:
        raise SystemExit(f"Batch size must be 10 (got {args.batch_size}).")
    if args.init_workers is None or args.init_workers < 1:
        args.init_workers = 1
    if args.max_parallel_tenants is None:
        args.max_parallel_tenants = args.init_workers
    if args.max_parallel_tenants < 1:
        args.max_parallel_tenants = 1
    if args.insert_batch_rows <= 0:
        raise SystemExit("insert_batch_rows must be > 0.")
    if args.insert_batch_bytes <= 0:
        raise SystemExit("insert_batch_bytes must be > 0.")
    if args.init_progress_rows <= 0:
        args.init_progress_rows = 100000
    if args.tenant_progress_rows <= 0:
        args.tenant_progress_rows = 10000
    if not getattr(args, "profile_cycle", None):
        args.profile_cycle = PROFILE_CYCLE
    if args.run_max_parallel_tenants is None or args.run_max_parallel_tenants < 1:
        args.run_max_parallel_tenants = 1
    if args.tenant_log_interval is None or args.tenant_log_interval < 1:
        args.tenant_log_interval = 100
    if args.replication_log_interval is None or args.replication_log_interval < 1:
        args.replication_log_interval = 60


def print_config_summary(
    args: argparse.Namespace, env_overrides: Sequence[str], ignored_env: Sequence[str]
) -> None:
    print(f"[config] Loaded {args.config_path}")
    write_target = (
        args.write_socket or f"{args.write_host}:{args.write_port}"
    )
    read_target = (
        args.read_socket or f"{args.read_host}:{args.read_port}"
    )
    print(
        f"[config] write={write_target} read={read_target} "
        f"schema=`{args.schema}`.`{args.table}`"
    )
    print(
        "[config] tenants=%s rows_per_tenant=%s max_parallel_tenants=%s init_workers=%s "
        "batch_rows=%s batch_bytes=%s tenant_progress=%s global_progress=%s relax_durability=%s"
        % (
            args.tenants,
            args.rows_per_tenant,
            args.max_parallel_tenants,
            args.init_workers,
            args.insert_batch_rows,
            args.insert_batch_bytes,
            args.tenant_progress_rows,
            args.init_progress_rows,
            args.relax_durability,
        )
    )
    if env_overrides:
        print(f"[config] Environment overrides applied: {', '.join(env_overrides)}")
    elif ignored_env:
        print(
            "[config] Ignored env overrides: %s (use --allow-env-overrides to enable)"
            % ", ".join(ignored_env)
        )
    if args.run:
        print(
            "[config] run_max_parallel_tenants=%s progress_interval=%s "
            "tenant_log_interval=%s repl_log_interval=%ss"
            % (
                args.run_max_parallel_tenants,
                args.progress_interval,
                args.tenant_log_interval,
                args.replication_log_interval,
            )
        )


def connect_mysql(
    cfg: DBConfig,
    autocommit: bool,
    *,
    connect_timeout: Optional[int] = None,
) -> pymysql.connections.Connection:
    params: Dict[str, object] = {
        "user": cfg.user,
        "password": cfg.password or "",
        "charset": "utf8mb4",
        "autocommit": autocommit,
        "cursorclass": DictCursor,
    }
    if connect_timeout is not None:
        params["connect_timeout"] = connect_timeout
    if cfg.socket:
        params["unix_socket"] = cfg.socket
    else:
        params["host"] = cfg.host
        params["port"] = cfg.port
    conn = pymysql.connect(**params)
    if cfg.db:
        conn.select_db(cfg.db)
    return conn


def ensure_schema(conn: pymysql.connections.Connection, schema: str, table: str, force: bool) -> None:
    schema_sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{schema}` "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
        )
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
        exists = bool(cur.fetchone()["cnt"])
        cur.execute(f"USE `{schema}`")
        if exists and not force:
            raise SystemExit(
                f"Table `{schema}`.`{table}` already exists. Re-run with --force to drop it."
            )
        if exists and force:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(schema_sql)
    conn.commit()


def generate_tenant_ids(args: argparse.Namespace) -> List[int]:
    return [args.tenant_id_base + offset for offset in range(args.tenants)]


def choose_profile(index: int, cycle: Sequence[str]) -> str:
    return cycle[index % len(cycle)]


def expand_digest(label: str, size: int) -> bytes:
    if size <= 0:
        return b""
    digest = hashlib.sha256(label.encode("utf-8")).digest()
    repeats = -(-size // len(digest))
    return (digest * repeats)[:size]


def text_payload(label: str, length: int) -> str:
    if length <= 0:
        return ""
    pattern = hashlib.sha256(label.encode("utf-8")).hexdigest()
    repeats = -(-length // len(pattern))
    return (pattern * repeats)[:length]


def metadata_block(label: str, length: int) -> str:
    return text_payload(label, length)


def value_size(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value)
    if isinstance(value, int):
        return 8
    return len(str(value))


def row_payload_size(values: Sequence[object]) -> int:
    return sum(value_size(value) for value in values)


def deterministic_range(row_id: int, label: str, bounds: Tuple[int, int], seed: int) -> int:
    low, high = bounds
    if low >= high:
        return low
    span = high - low
    digest = hashlib.sha256(f"{seed}:{row_id}:{label}".encode("utf-8")).digest()
    window = int.from_bytes(digest[:4], "big")
    return low + (window % (span + 1))


def apply_relaxed_durability(
    cursor: pymysql.cursors.Cursor,
) -> Dict[str, str]:
    desired = {
        "innodb_flush_log_at_trx_commit": "2",
        "sync_binlog": "0",
    }
    previous: Dict[str, str] = {}
    for var, new_value in desired.items():
        cursor.execute("SHOW VARIABLES LIKE %s", (var,))
        row = cursor.fetchone()
        if not row:
            continue
        current_value = str(row["Value"])
        if current_value == new_value:
            continue
        previous[var] = current_value
        cursor.execute(f"SET GLOBAL {var} = {new_value}")
    return previous


def restore_durability(
    cursor: pymysql.cursors.Cursor, previous: Dict[str, str]
) -> None:
    for var, old_value in previous.items():
        cursor.execute(f"SET GLOBAL {var} = {old_value}")


def seed_data(args: argparse.Namespace, cfg: DBConfig) -> None:
    conn = connect_mysql(cfg, autocommit=False)
    ensure_schema(conn, args.schema, args.table, args.force)
    conn.select_db(args.schema)

    tenant_ids = generate_tenant_ids(args)
    total_rows = args.tenants * args.rows_per_tenant
    max_parallel = max(1, min(args.max_parallel_tenants, len(tenant_ids)))

    write_location = cfg.host or cfg.socket or "local"
    print(
        f"[init] Seeding {total_rows:,} rows across {args.tenants} tenants "
        f"into `{args.schema}`.`{args.table}` via {write_location} "
        f"(max_parallel_tenants: {max_parallel})"
    )
    print("[init] Tenant ID ranges:")
    for tenant_idx, tenant_id in enumerate(tenant_ids):
        tenant_start_id = args.start_id + tenant_idx * args.rows_per_tenant
        tenant_end_id = tenant_start_id + args.rows_per_tenant - 1
        print(
            f"[init]   tenant {tenant_id}: id_range={tenant_start_id}-{tenant_end_id} "
            f"rows={args.rows_per_tenant:,}"
        )
    if total_rows >= 1_000_000:
        print(
            "[init] Heads-up: seeding >=1M rows creates massive payloads "
            "— lower --max-parallel-tenants if memory pressure or swap usage grows."
        )

    columns_sql = ", ".join(f"`{col}`" for col in INSERT_COLUMNS)
    single_values_sql = "(" + ", ".join(["%s"] * len(INSERT_COLUMNS)) + ")"
    insert_sql = f"INSERT INTO `{args.schema}`.`{args.table}` ({columns_sql}) VALUES {single_values_sql}"

    start = time.perf_counter()
    progress_step = max(1, args.init_progress_rows)
    progress_next = progress_step
    tenant_progress_step = max(1, args.tenant_progress_rows)
    progress_lock = threading.Lock()
    inserted_total = 0
    generation_time = 0.0
    insert_time = 0.0
    durability_overrides: Dict[str, str] = {}
    profile_cycle = args.profile_cycle or PROFILE_CYCLE
    base_seed = args.seed or 0

    def report_progress(
        rows_added: int,
        gen_delta: float,
        insert_delta: float,
        last_batch_rows: int,
        last_batch_bytes: int,
    ) -> None:
        nonlocal inserted_total, generation_time, insert_time, progress_next
        with progress_lock:
            inserted_total += rows_added
            generation_time += gen_delta
            insert_time += insert_delta
            while inserted_total >= progress_next:
                elapsed = time.perf_counter() - start
                rows_per_sec = inserted_total / elapsed if elapsed else 0.0
                last_batch_mib = last_batch_bytes / (1024 * 1024)
                print(
                    f"[init] Inserted {inserted_total:,}/{total_rows:,} rows in {elapsed:,.1f}s "
                    f"({rows_per_sec:,.1f} rows/s, gen {generation_time:,.1f}s, insert {insert_time:,.1f}s, "
                    f"last batch: {last_batch_rows} rows, {last_batch_mib:,.2f} MiB)"
                )
                progress_next += progress_step

    try:
        with conn.cursor() as cur:
            if args.relax_durability:
                try:
                    durability_overrides = apply_relaxed_durability(cur)
                    if durability_overrides:
                        print(
                            "[init] Relaxed durability settings for seeding "
                            f"(overrode: {', '.join(f'{k}={v}' for k, v in durability_overrides.items())})"
                        )
                except pymysql.MySQLError as exc:
                    print(f"[init] Unable to relax durability settings: {exc}")
                    durability_overrides = {}

        def seed_tenant(tenant_idx: int, tenant_id: int) -> None:
            local_conn = connect_mysql(cfg, autocommit=False)
            local_conn.select_db(args.schema)
            batch_rows: List[Tuple[object, ...]] = []
            batch_bytes = 0
            generation_since_flush = 0.0
            rows_committed_tenant = 0
            tenant_next_progress = tenant_progress_step
            tenant_seed = base_seed + tenant_idx
            tenant_start_id = args.start_id + tenant_idx * args.rows_per_tenant
            tenant_end_id = tenant_start_id + args.rows_per_tenant - 1
            tenant_start_ts = time.perf_counter()
            print(
                f"[init][tenant {tenant_id}] starting rows={args.rows_per_tenant:,} "
                f"id_range={tenant_start_id}-{tenant_end_id}"
            )

            def flush_local(cursor: pymysql.cursors.Cursor) -> None:
                nonlocal batch_rows, batch_bytes, generation_since_flush, rows_committed_tenant
                nonlocal tenant_next_progress
                if not batch_rows:
                    return
                t0 = time.perf_counter()
                rows_committed = len(batch_rows)
                batch_payload_bytes = batch_bytes
                cursor.executemany(insert_sql, batch_rows)
                local_conn.commit()
                duration = time.perf_counter() - t0
                report_progress(
                    rows_committed,
                    generation_since_flush,
                    duration,
                    rows_committed,
                    batch_payload_bytes,
                )
                rows_committed_tenant += rows_committed
                while rows_committed_tenant >= tenant_next_progress and tenant_next_progress <= args.rows_per_tenant:
                    elapsed_tenant = time.perf_counter() - tenant_start_ts
                    rows_sec = rows_committed_tenant / elapsed_tenant if elapsed_tenant else 0.0
                    print(
                        f"[init][tenant {tenant_id}] progress {rows_committed_tenant:,}/{args.rows_per_tenant:,} "
                        f"rows ({rows_sec:,.1f} rows/s)"
                    )
                    tenant_next_progress += tenant_progress_step
                batch_rows = []
                batch_bytes = 0
                generation_since_flush = 0.0

            try:
                with local_conn.cursor() as cur:
                    for row_index in range(args.rows_per_tenant):
                        row_id = tenant_start_id + row_index
                        gen_t0 = time.perf_counter()
                        profile = choose_profile(row_index, profile_cycle)
                        spec = PROFILE_SPECS[profile]
                        contents_size = deterministic_range(
                            row_id, "contents", spec["contents"], tenant_seed
                        )
                        highlight_size = deterministic_range(
                            row_id, "highlight", spec["highlight"], tenant_seed
                        )
                        external_ptr_size = deterministic_range(
                            row_id, "external_ptr", spec["external_ptr"], tenant_seed
                        )
                        metadata_len = spec["metadata_text"]

                        date_create = args.date_base + (row_index * args.date_step)
                        contents = expand_digest(f"{tenant_seed}-{row_id}-contents", contents_size)
                        contents_highlight = expand_digest(
                            f"{tenant_seed}-{row_id}-highlight", highlight_size
                        )
                        metadata_text = metadata_block(
                            f"{tenant_seed}-{row_id}-metadata", metadata_len
                        )
                        external_ptr = expand_digest(
                            f"{tenant_seed}-{row_id}-extptr", external_ptr_size
                        )

                        values: Tuple[object, ...] = (
                            row_id,
                            tenant_id,
                            800_000_000_000 + row_id % 10_000_000,
                            date_create,
                            f"sec-{row_id}",
                            f"pub-{row_id}",
                            contents_size,
                            0,
                            f"orig_{row_id}.dat",
                            f"stored_{row_id}.dat",
                            f"tenant_{tenant_id}_file_{row_index}",
                            "application/octet-stream",
                            tenant_id,
                            contents,
                            None,
                            contents_highlight,
                            None,
                            "backfill",
                            metadata_text,
                            None,
                            f"https://files.example/{row_id}",
                            0,
                            0,
                            0,
                            0,
                            date_create,
                            "HARNESS_TC",
                            f"ext-{row_id}",
                            0,
                            tenant_id,
                            0,
                            tenant_id,
                            text_payload(f"{tenant_seed}-{row_id}-tenants", 256),
                            tenant_id,
                            0,
                            0,
                            date_create,
                            external_ptr,
                            hashlib.md5(contents).hexdigest(),
                            0,
                            metadata_block(f"{tenant_seed}-{row_id}-unencrypted", 1024),
                            0,
                            0,
                        )
                        generation_since_flush += time.perf_counter() - gen_t0
                        row_bytes = row_payload_size(values)

                        if batch_rows and (
                            len(batch_rows) >= args.insert_batch_rows
                            or batch_bytes + row_bytes > args.insert_batch_bytes
                        ):
                            flush_local(cur)

                        batch_rows.append(values)
                        batch_bytes += row_bytes
                    flush_local(cur)
                    elapsed_tenant = time.perf_counter() - tenant_start_ts
                    rows_per_sec = rows_committed_tenant / elapsed_tenant if elapsed_tenant else 0.0
                    print(
                        f"[init][tenant {tenant_id}] completed rows={rows_committed_tenant:,} "
                        f"in {elapsed_tenant:,.1f}s ({rows_per_sec:,.1f} rows/s)"
                    )
            finally:
                local_conn.close()

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures: List[Tuple[int, Future[None]]] = []
            for idx, tenant_id in enumerate(tenant_ids):
                futures.append((tenant_id, executor.submit(seed_tenant, idx, tenant_id)))
            failed: List[Tuple[int, Exception]] = []
            for tenant_id, future in futures:
                try:
                    future.result()
                except Exception as exc:  # pragma: no cover - relies on runtime errors
                    failed.append((tenant_id, exc))
                    print(f"[init][tenant {tenant_id}] failed: {exc}")
                    if not args.continue_on_tenant_failure:
                        raise
            if failed:
                failed_ids = ", ".join(str(tid) for tid, _ in failed)
                raise SystemExit(f"Seeding failed for tenant(s): {failed_ids}")

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM `{args.schema}`.`{args.table}`"
            )
            total = cur.fetchone()["cnt"]
            print(f"[init] Row count check: {total:,} (expected {total_rows:,})")
            cur.execute(
                f"SELECT tenant_id, COUNT(*) AS cnt FROM `{args.schema}`.`{args.table}` "
                "GROUP BY tenant_id ORDER BY tenant_id"
            )
            for row in cur.fetchall():
                print(f"[init] tenant {row['tenant_id']}: {row['cnt']:,} rows")
    finally:
        if durability_overrides:
            with conn.cursor() as cur:
                restore_durability(cur, durability_overrides)
        conn.close()

    elapsed = time.perf_counter() - start
    rows_per_sec = inserted_total / elapsed if elapsed else 0.0
    print(
        f"[init] Completed {inserted_total:,} rows in {elapsed:,.1f}s "
        f"(rows/s: {rows_per_sec:,.1f}, generation {generation_time:,.1f}s, inserts {insert_time:,.1f}s)"
    )


def build_select_sql(args: argparse.Namespace) -> str:
    return (
        "SELECT id, tenant_id, user_id, date_create, token, pub_token, size, "
        "is_stored, original_name, stored_name, title, mimetype, parent_id, "
        "contents, contents_enc, contents_highlight, contents_highlight_enc, "
        "highlight_type, metadata, metadata_enc, external_url, is_deleted, "
        "date_delete, is_public, pub_shared, last_indexed, source, external_id, "
        "service_type_id, service_id, is_multitenant, original_tenant_id, "
        "tenants_shared_with, service_tenant_id, is_tombstoned, thumbnail_version, "
        "date_thumbnail_retrieved, external_ptr, md5, metadata_version, "
        "unencrypted_metadata, restriction_type, version "
        f"FROM `{args.schema}`.`{args.table}` "
        "WHERE tenant_id = %s AND date_create > %s "
        "ORDER BY date_create ASC, id ASC "
        f"LIMIT {args.batch_size}"
    )


def build_update_sql(args: argparse.Namespace) -> str:
    return (
        f"UPDATE `{args.schema}`.`{args.table}` "
        "SET contents_enc=%s, contents_highlight_enc=%s, metadata_enc=%s, "
        "version = version + 1 "
        "WHERE id=%s AND tenant_id=%s"
    )


def encrypt_blob(source: Optional[bytes], label: str) -> bytes:
    source_bytes = source or b""
    target_len = len(source_bytes) or 32
    digest = hashlib.sha256(source_bytes + label.encode("utf-8")).digest()
    repeats = -(-target_len // len(digest))
    return (digest * repeats)[:target_len]


def encrypt_metadata(source: Optional[str], label: str) -> bytes:
    payload = (source or "").encode("utf-8")
    return encrypt_blob(payload, label)


def fetch_batch(
    conn: pymysql.connections.Connection,
    select_sql: str,
    tenant_id: int,
    cursor_date: int,
) -> List[Dict[str, object]]:
    with conn.cursor() as cur:
        cur.execute(select_sql, (tenant_id, cursor_date))
        return cur.fetchall()


def explain_plan(conn: pymysql.connections.Connection, select_sql: str, tenant_id: int) -> None:
    explain_sql = f"EXPLAIN {select_sql}"
    with conn.cursor() as cur:
        cur.execute(explain_sql, (tenant_id, 0))
        plan = cur.fetchall()
    log_line("[run] EXPLAIN (expect tenant_id_4):")
    for row in plan:
        log_line(f"  id={row.get('id')} table={row.get('table')} key={row.get('key')} rows={row.get('rows')}")


def apply_updates(
    conn: pymysql.connections.Connection,
    update_sql: str,
    rows: Sequence[Dict[str, object]],
) -> Tuple[int, float, float]:
    encrypt_seconds = 0.0
    update_seconds = 0.0
    with conn.cursor() as cur:
        for row in rows:
            enc_start = time.perf_counter()
            enc_contents = encrypt_blob(row.get("contents"), f"{row['id']}-contents_enc")
            enc_highlight = encrypt_blob(row.get("contents_highlight"), f"{row['id']}-highlight_enc")
            enc_metadata = encrypt_metadata(row.get("metadata"), f"{row['id']}-metadata_enc")
            encrypt_seconds += time.perf_counter() - enc_start
            update_start = time.perf_counter()
            cur.execute(
                update_sql,
                (
                    enc_contents,
                    enc_highlight,
                    enc_metadata,
                    row["id"],
                    row["tenant_id"],
                ),
            )
            update_seconds += time.perf_counter() - update_start
    commit_start = time.perf_counter()
    conn.commit()
    update_seconds += time.perf_counter() - commit_start
    return len(rows), encrypt_seconds * 1000.0, update_seconds * 1000.0


def run_workload(args: argparse.Namespace, read_cfg: DBConfig, write_cfg: DBConfig) -> None:
    select_sql = build_select_sql(args)
    update_sql = build_update_sql(args)
    tenant_ids = generate_tenant_ids(args)
    if not tenant_ids:
        log_line("[run] No tenant IDs configured; exiting.")
        return

    read_target = read_cfg.socket or f"{read_cfg.host}:{read_cfg.port}"
    write_target = write_cfg.socket or f"{write_cfg.host}:{write_cfg.port}"
    log_line(
        f"[run] READ from {read_target}, WRITE to {write_target}, "
        f"target `{args.schema}`.`{args.table}`"
    )

    try:
        explain_conn = connect_mysql(
            read_cfg.with_db(args.schema),
            autocommit=True,
            connect_timeout=5,
        )
    except pymysql.MySQLError as exc:
        hint = (
            f"Unable to reach replica at {read_cfg.host}:{read_cfg.port}. "
            "Verify the SSH tunnel (mysql-read-tunnel.service), firewall rules, "
            "and READ_USER/READ_PASSWORD credentials."
        )
        raise SystemExit(f"[run] Failed to connect to replica: {exc}. {hint}") from exc
    try:
        explain_plan(explain_conn, select_sql, tenant_ids[0])
    finally:
        explain_conn.close()

    stats = RunStats(start_ts=time.perf_counter())
    stats_lock = threading.Lock()
    tenants_lock = threading.Lock()
    stop_event = threading.Event()
    runtime_event = threading.Event()
    total_tenants = len(tenant_ids)
    completed_tenants = 0
    active_tenants: Set[int] = set()
    max_runtime = args.max_runtime_seconds or 0
    global_progress_interval = max(1, args.progress_interval)
    tenant_log_interval = max(1, args.tenant_log_interval)
    replication_interval = max(1, args.replication_log_interval)
    max_workers = max(1, min(args.run_max_parallel_tenants, total_tenants))
    log_line(f"[run] Using up to {max_workers} parallel tenant worker(s)")

    tenant_counters: Dict[int, TenantCounters] = {}
    worker_counters: Dict[int, WorkerCounters] = {}
    thread_worker_ids: Dict[int, int] = {}
    worker_id_lock = threading.Lock()

    def acquire_worker_slot() -> Tuple[int, WorkerCounters]:
        tid = threading.get_ident()
        with worker_id_lock:
            worker_id = thread_worker_ids.get(tid)
            if worker_id is None:
                worker_id = len(thread_worker_ids) + 1
                thread_worker_ids[tid] = worker_id
                worker_counters[worker_id] = WorkerCounters(
                    worker_id=worker_id,
                    start_ts=time.perf_counter(),
                )
            return worker_id, worker_counters[worker_id]

    replication_stop = threading.Event()

    def replication_logger() -> None:
        while not stop_event.is_set():
            if replication_stop.wait(replication_interval):
                break
            try:
                conn = connect_mysql(
                    read_cfg.with_db(args.schema),
                    autocommit=True,
                    connect_timeout=5,
                )
            except pymysql.MySQLError as exc:
                log_line(f"[run][repl] snapshot failed: {exc}")
                continue
            try:
                status = fetch_replica_status_fields(conn)
            except Exception as exc:  # pragma: no cover
                log_line(f"[run][repl] snapshot failed: {exc}")
            else:
                log_line(
                    "[run][repl] sbm=%s io=%s sql=%s"
                    % (
                        status.get("Seconds_Behind_Source", ""),
                        status.get("Replica_IO_Running", ""),
                        status.get("Replica_SQL_Running", ""),
                    )
                )
            finally:
                conn.close()

    replication_thread = threading.Thread(
        target=replication_logger,
        name="replication-logger",
        daemon=True,
    )
    replication_thread.start()

    def process_tenant(tenant_idx: int, tenant_id: int) -> None:
        nonlocal completed_tenants
        if stop_event.is_set():
            return
        worker_id, worker_counter = acquire_worker_slot()
        try:
            read_conn = connect_mysql(
                read_cfg.with_db(args.schema),
                autocommit=True,
                connect_timeout=5,
            )
        except pymysql.MySQLError as exc:
            raise RuntimeError(f"tenant {tenant_id} failed to connect to replica: {exc}") from exc
        try:
            write_conn = connect_mysql(write_cfg.with_db(args.schema), autocommit=False)
        except pymysql.MySQLError as exc:
            read_conn.close()
            raise RuntimeError(f"tenant {tenant_id} failed to connect to master: {exc}") from exc

        tenant_counter = TenantCounters(
            tenant_id=tenant_id,
            worker_id=worker_id,
            start_ts=time.perf_counter(),
        )
        tenant_counters[tenant_id] = tenant_counter

        with tenants_lock:
            active_tenants.add(tenant_id)

        cursor_date = 0
        start_ts = tenant_counter.start_ts
        log_line(f"[run][tenant {tenant_id}][w{worker_id}] starting ({tenant_idx}/{total_tenants})")
        logged_first_batch = False
        try:
            while not stop_event.is_set():
                select_start = time.perf_counter()
                rows = fetch_batch(read_conn, select_sql, tenant_id, cursor_date)
                select_ms = (time.perf_counter() - select_start) * 1000.0
                tenant_counter.select_count += 1
                worker_counter.select_count += 1
                with stats_lock:
                    stats.selects += 1
                if not rows:
                    break
                cursor_date = rows[-1]["date_create"]
                tenant_counter.rows_selected += len(rows)
                worker_counter.rows_selected += len(rows)
                batch_start = time.perf_counter()
                processed, encrypt_ms, update_ms = apply_updates(write_conn, update_sql, rows)
                tenant_counter.batches += 1
                worker_counter.batches += 1
                tenant_counter.rows_updated += processed
                tenant_counter.update_count += processed
                worker_counter.rows_updated += processed
                worker_counter.update_count += processed
                batch_ms = (time.perf_counter() - batch_start) * 1000.0
                log_progress = False
                rows_snapshot = 0
                batches_snapshot = 0
                selects_snapshot = 0
                updates_snapshot = 0
                with stats_lock:
                    stats.mark_batch(len(rows), batch_ms)
                    rows_snapshot = stats.rows
                    batches_snapshot = stats.batches
                    selects_snapshot = stats.selects
                    updates_snapshot = stats.updates
                    if stats.batches % global_progress_interval == 0:
                        log_progress = True
                if not logged_first_batch:
                    log_line(
                        f"[run][tenant {tenant_id}][w{worker_id}] first_batch rows={len(rows)} "
                        f"select={select_ms:.1f}ms encrypt={encrypt_ms:.1f}ms "
                        f"update={update_ms:.1f}ms"
                    )
                    logged_first_batch = True
                if tenant_counter.batches % tenant_log_interval == 0:
                    elapsed = time.perf_counter() - start_ts
                    log_line(
                        f"[run][tenant {tenant_id}][w{worker_id}] batch={tenant_counter.batches:,} "
                        f"rows_sel={tenant_counter.rows_selected:,} rows_upd={tenant_counter.rows_updated:,} "
                        f"selects={tenant_counter.select_count:,} updates={tenant_counter.update_count:,} "
                        f"elapsed={elapsed:,.1f}s select={select_ms:.1f}ms "
                        f"encrypt={encrypt_ms:.1f}ms update={update_ms:.1f}ms"
                    )
                if log_progress:
                    with tenants_lock:
                        completed_snapshot = completed_tenants
                        active_snapshot = len(active_tenants)
                    elapsed_global = time.perf_counter() - stats.start_ts
                    log_line(
                        f"[run][global] rows={rows_snapshot:,} batches={batches_snapshot:,} "
                        f"selects={selects_snapshot:,} updates={updates_snapshot:,} "
                        f"tenants_done={completed_snapshot}/{total_tenants} "
                        f"active={active_snapshot} elapsed={elapsed_global:,.1f}s"
                    )
                if max_runtime and (time.perf_counter() - stats.start_ts) > max_runtime:
                    runtime_event.set()
                    stop_event.set()
                    log_line(f"[run] Max runtime {max_runtime}s exceeded; signalling stop.")
                    break
        except Exception:
            stop_event.set()
            raise
        finally:
            write_conn.close()
            read_conn.close()

        tenant_counter.end_ts = time.perf_counter()
        elapsed = tenant_counter.end_ts - tenant_counter.start_ts
        worker_counter.tenants_completed += 1
        worker_counter.end_ts = max(worker_counter.end_ts, tenant_counter.end_ts)
        with tenants_lock:
            active_tenants.discard(tenant_id)
            completed_tenants += 1
            completed_snapshot = completed_tenants
            active_snapshot = len(active_tenants)
        log_line(
            f"[run][tenant {tenant_id}][w{worker_id}] complete rows_sel={tenant_counter.rows_selected:,} "
            f"rows_upd={tenant_counter.rows_updated:,} batches={tenant_counter.batches:,} "
            f"selects={tenant_counter.select_count:,} updates={tenant_counter.update_count:,} "
            f"elapsed={elapsed:,.1f}s ({completed_snapshot}/{total_tenants} tenants, "
            f"active={active_snapshot})"
        )
        if tenant_counter.batches == 0:
            log_line(f"[run][tenant {tenant_id}][w{worker_id}] no rows found")

    futures: Dict[Future[None], int] = {}
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, tenant_id in enumerate(tenant_ids, start=1):
                futures[executor.submit(process_tenant, idx, tenant_id)] = tenant_id
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # pragma: no cover - runtime errors
                    stop_event.set()
                    tenant_id = futures[future]
                    raise SystemExit(f"[run] Tenant {tenant_id} failed: {exc}") from exc
    finally:
        replication_stop.set()
        replication_thread.join(timeout=5)

    total_runtime = time.perf_counter() - stats.start_ts
    if tenant_counters:
        log_line("[run] Tenant summaries:")
        for tenant_id in sorted(tenant_counters):
            tenant_stats = tenant_counters[tenant_id]
            elapsed = (
                (tenant_stats.end_ts or time.perf_counter()) - tenant_stats.start_ts
                if tenant_stats.start_ts
                else 0.0
            )
            log_line(
                f"  tenant {tenant_id} [w{tenant_stats.worker_id}] batches={tenant_stats.batches:,} "
                f"selects={tenant_stats.select_count:,} updates={tenant_stats.update_count:,} "
                f"rows_sel={tenant_stats.rows_selected:,} rows_upd={tenant_stats.rows_updated:,} "
                f"elapsed={elapsed:,.1f}s"
            )
    if worker_counters:
        log_line("[run] Worker summaries:")
        for worker_id in sorted(worker_counters):
            worker_stats = worker_counters[worker_id]
            elapsed = (
                (worker_stats.end_ts or time.perf_counter()) - worker_stats.start_ts
                if worker_stats.start_ts
                else 0.0
            )
            log_line(
                f"  worker w{worker_id}: tenants={worker_stats.tenants_completed}, "
                f"batches={worker_stats.batches:,} selects={worker_stats.select_count:,} "
                f"updates={worker_stats.update_count:,} rows_sel={worker_stats.rows_selected:,} "
                f"rows_upd={worker_stats.rows_updated:,} elapsed={elapsed:,.1f}s"
            )
    log_line(
        f"[run] COMPLETE: {stats.rows:,} rows, {stats.batches:,} batches, "
        f"{stats.selects:,} SELECTs, {stats.updates:,} UPDATEs in {total_runtime:,.1f}s"
    )
    try:
        read_conn = connect_mysql(
            read_cfg.with_db(args.schema),
            autocommit=True,
            connect_timeout=5,
        )
    except pymysql.MySQLError as exc:
        raise SystemExit(f"[run] Unable to re-connect to replica for status check: {exc}") from exc
    try:
        status = check_replication(read_conn)
    finally:
        read_conn.close()
    write_artifacts(stats, status, args, total_runtime)
    if not replication_healthy(status):
        log_line("[run] Replication unhealthy — see artifacts for details.", stream=sys.stderr)
        sys.exit(2)
    if runtime_event.is_set():
        log_line("[run] Aborted due to max_runtime_seconds limit.", stream=sys.stderr)
        sys.exit(3)


def fetch_replica_status_fields(conn: pymysql.connections.Connection) -> Dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SHOW REPLICA STATUS")
        row = cur.fetchone()
    if not row:
        raise SystemExit("SHOW REPLICA STATUS returned no rows; replica not configured?")
    fields = {
        "Replica_IO_Running": row.get("Replica_IO_Running"),
        "Replica_SQL_Running": row.get("Replica_SQL_Running"),
        "Seconds_Behind_Source": row.get("Seconds_Behind_Source"),
        "Last_IO_Error": row.get("Last_IO_Error"),
        "Last_SQL_Error": row.get("Last_SQL_Error"),
    }
    return {k: ("" if v is None else str(v)) for k, v in fields.items()}


def check_replication(conn: pymysql.connections.Connection) -> Dict[str, str]:
    fields = fetch_replica_status_fields(conn)
    log_line("[run] Replica status summary:")
    for key, value in fields.items():
        log_line(f"  {key}: {value}")
    return fields


def replication_healthy(status: Dict[str, str]) -> bool:
    if status["Replica_IO_Running"] != "Yes":
        return False
    if status["Replica_SQL_Running"] != "Yes":
        return False
    if status["Last_IO_Error"] or status["Last_SQL_Error"]:
        return False
    return True


def write_artifacts(
    stats: RunStats,
    status: Dict[str, str],
    args: argparse.Namespace,
    runtime_seconds: float,
) -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    summary_path = ARTIFACTS_DIR / f"run_{timestamp}.log"
    with summary_path.open("w", encoding="utf-8") as fh:
        fh.write(f"Schema: {args.schema}.{args.table}\n")
        fh.write(f"Tenants processed: {args.tenants}\n")
        fh.write(f"Rows processed: {stats.rows}\n")
        fh.write(f"Batches: {stats.batches}\n")
        fh.write(f"SELECTs: {stats.selects}\n")
        fh.write(f"UPDATEs: {stats.updates}\n")
        fh.write(f"Average batch ms: {stats.avg_batch_ms:.2f}\n")
        fh.write(f"Total runtime seconds: {runtime_seconds:.2f}\n")
        fh.write("Replica status summary:\n")
        for key, value in status.items():
            fh.write(f"  {key}: {value}\n")

    status_path = ARTIFACTS_DIR / f"replica_status_{timestamp}.txt"
    with status_path.open("w", encoding="utf-8") as fh:
        fh.write("SHOW REPLICA STATUS summary\n")
        for key, value in status.items():
            fh.write(f"{key}: {value}\n")
    log_line(f"[run] Summary written to {summary_path}")


def main() -> None:
    args = parse_args()
    config_path, config_data = load_json_config(args.config)
    validate_config_schema(config_data)
    env_overrides, ignored_env = apply_config_overrides(args, config_data, config_path)
    ensure_effective_args(args)
    print_config_summary(args, env_overrides, ignored_env)
    write_cfg = DBConfig(
        host=args.write_host,
        port=args.write_port,
        user=args.write_user,
        password=args.write_password,
        socket=args.write_socket,
    )
    read_cfg = DBConfig(
        host=args.read_host,
        port=args.read_port,
        user=args.read_user,
        password=args.read_password,
        socket=args.read_socket,
    )
    if args.init:
        if getattr(args, "force_required", False) and not args.force:
            raise SystemExit(
                "This config requires --force for init runs. Re-run with --force to confirm."
            )
        seed_data(args, write_cfg)
    elif args.run:
        run_workload(args, read_cfg, write_cfg)
    else:  # pragma: no cover
        raise SystemExit("Either --init or --run must be supplied.")


if __name__ == "__main__":
    main()
