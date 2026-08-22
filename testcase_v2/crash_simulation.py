#!/usr/bin/env python3
"""Stress-test key repro_db.files query patterns.

The runner loops through representative queries with randomized bounds built from
20 dataset slices, allowing large worker counts (e.g., 40 threads) to round-robin
through the dataset. Run ``python3 crash_simulation.py --help`` for the available options.
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_default_source_candidates = [
    Path(os.getenv('WORKLOAD_SOURCE_ROOT')) if os.getenv('WORKLOAD_SOURCE_ROOT') else None,
    Path.home() / 'Documents' / 'Incidents' / 'code' / 'percona-server',
    Path.home() / 'Documents' / 'Incidents' / 'percona-server',
]
SOURCE_INFO = {'percona_server_root': 'unknown'}
for candidate in _default_source_candidates:
    if candidate and candidate.exists():
        SOURCE_INFO['percona_server_root'] = str(candidate.resolve())
        break

# MySQL client setup

try:  # Try mysql-connector-python first
    import mysql.connector as mysql_driver  # type: ignore
    USING_MYSQL_CONNECTOR = True
except ImportError:  # Fall back to PyMySQL
    mysql_driver = None
    USING_MYSQL_CONNECTOR = False
    try:
        import pymysql as pymysql_driver  # type: ignore
    except ImportError:  # pragma: no cover - guidance for the reader
        pymysql_driver = None

if not USING_MYSQL_CONNECTOR and "pymysql_driver" not in globals():
    raise SystemExit(
        "No MySQL Python driver available. Install 'mysql-connector-python' or 'PyMySQL'."
    )


def open_connection(*, host: str, port: int, user: str, password: Optional[str]) -> Any:
    """Open a MySQL connection using whichever driver we managed to import."""

    if USING_MYSQL_CONNECTOR:
        return mysql_driver.connect(  # type: ignore[union-attr]
            host=host,
            port=port,
            user=user,
            password=password or "",
            autocommit=True,
        )

    # Use PyMySQL if that's the driver we found
    return pymysql_driver.connect(  # type: ignore[union-attr]
        host=host,
        port=port,
        user=user,
        password=password or "",
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql_driver.cursors.DictCursor,
    )


# Workload definitions

HIGHLIGHT_FILTER = "('quip','list','list_template','huddle_transcript')"
BANNED_HIGHLIGHT_TYPES = ("quip", "list", "list_template", "huddle_transcript")

RECENT_WINDOW_DATE_THRESHOLD = 1_483_713_425
CLEANUP_DATE_CUTOFF_MAX = 1_756_722_478
PER_QUERY_TIMEOUT_MS = 30_000

RECENT_WINDOW_SLICES: Tuple[Dict[str, int], ...] = (
    {"tenant_id": 2981626498, "date_min": 1483714204, "date_max": 1510258804, "row_count": 36084},
    {"tenant_id": 75899570098, "date_min": 1483713788, "date_max": 1510260005, "row_count": 29958},
    {"tenant_id": 10736132949, "date_min": 1483715553, "date_max": 1510251314, "row_count": 13743},
    {"tenant_id": 2151557247, "date_min": 1483713636, "date_max": 1510225178, "row_count": 10139},
    {"tenant_id": 10181337062, "date_min": 1483753276, "date_max": 1510194917, "row_count": 8481},
    {"tenant_id": 181878234354, "date_min": 1483730955, "date_max": 1510247702, "row_count": 7138},
    {"tenant_id": 405881099296, "date_min": 1483746632, "date_max": 1510204734, "row_count": 6765},
    {"tenant_id": 43110286786, "date_min": 1484153697, "date_max": 1510258227, "row_count": 6737},
    {"tenant_id": 799070240290, "date_min": 1483733245, "date_max": 1510252339, "row_count": 5384},
    {"tenant_id": 265193767125, "date_min": 1483716066, "date_max": 1510255771, "row_count": 5243},
    {"tenant_id": 6252741026, "date_min": 1483728253, "date_max": 1510219539, "row_count": 4703},
    {"tenant_id": 218854112738, "date_min": 1483714620, "date_max": 1510220864, "row_count": 4635},
    {"tenant_id": 2176659155, "date_min": 1483715192, "date_max": 1510225297, "row_count": 4595},
    {"tenant_id": 9160916583, "date_min": 1483725303, "date_max": 1510251069, "row_count": 4458},
    {"tenant_id": 2662667302, "date_min": 1484062853, "date_max": 1510226141, "row_count": 4010},
    {"tenant_id": 110817556278, "date_min": 1503267146, "date_max": 1510094207, "row_count": 3758},
    {"tenant_id": 2317099954, "date_min": 1483713445, "date_max": 1497916869, "row_count": 3690},
    {"tenant_id": 220462725429, "date_min": 1483717814, "date_max": 1510159252, "row_count": 3641},
    {"tenant_id": 3866669863, "date_min": 1493307866, "date_max": 1510259545, "row_count": 3602},
    {"tenant_id": 71864288151, "date_min": 1483727507, "date_max": 1510203574, "row_count": 3552},
)

SEQUENTIAL_SLICES: Tuple[Dict[str, int], ...] = (
    {"tenant_id": 75899570098, "id_min": 2377636008, "id_max": 269279195810, "row_count": 61598},
    {"tenant_id": 2981626498, "id_min": 92256644502, "id_max": 269280872211, "row_count": 42331},
    {"tenant_id": 2151557247, "id_min": 4586750407, "id_max": 269033608865, "row_count": 27190},
    {"tenant_id": 147557470051, "id_min": 2305882893, "id_max": 269279481904, "row_count": 23943},
    {"tenant_id": 10736132949, "id_min": 18964520038, "id_max": 269252725537, "row_count": 22754},
    {"tenant_id": 181878234354, "id_min": 2199848365, "id_max": 269277487697, "row_count": 21928},
    {"tenant_id": 265193767125, "id_min": 2152150267, "id_max": 269284062085, "row_count": 20199},
    {"tenant_id": 71864288151, "id_min": 2152043829, "id_max": 269281004389, "row_count": 16435},
    {"tenant_id": 799070240290, "id_min": 2476772060, "id_max": 269239341990, "row_count": 14877},
    {"tenant_id": 220462725429, "id_min": 2511812381, "id_max": 269278800102, "row_count": 14719},
    {"tenant_id": 809797175120, "id_min": 4145669972, "id_max": 269259370595, "row_count": 14044},
    {"tenant_id": 15786212499, "id_min": 16170825186, "id_max": 269142841799, "row_count": 12515},
    {"tenant_id": 218854112738, "id_min": 2560076763, "id_max": 269212757877, "row_count": 12026},
    {"tenant_id": 3831568043, "id_min": 127684114192, "id_max": 269223615889, "row_count": 11883},
    {"tenant_id": 265127040497, "id_min": 2374475666, "id_max": 269277643010, "row_count": 10948},
    {"tenant_id": 306417793632, "id_min": 2152225226, "id_max": 269238970705, "row_count": 10491},
    {"tenant_id": 304429294326, "id_min": 2604298798, "id_max": 269284410165, "row_count": 9009},
    {"tenant_id": 405881099296, "id_min": 11240545430, "id_max": 269257541204, "row_count": 8963},
    {"tenant_id": 10181337062, "id_min": 11539373587, "id_max": 269141782613, "row_count": 8687},
    {"tenant_id": 2316645070, "id_min": 232084236640, "id_max": 269279780065, "row_count": 8137},
)

CLEANUP_SLICES: Tuple[Dict[str, int], ...] = (
    {
        "tenant_id": 129325844705,
        "id_min": 2185675980,
        "id_max": 269198351859,
        "date_delete_min": 1673422479,
        "date_delete_max": 1676291116,
        "row_count": 4324,
    },
    {
        "tenant_id": 266105194887,
        "id_min": 3231369225,
        "id_max": 269258745779,
        "date_delete_min": 1756979463,
        "date_delete_max": 1757195605,
        "row_count": 1730,
    },
    {
        "tenant_id": 3022319763812,
        "id_min": 4717928749,
        "id_max": 269244710081,
        "date_delete_min": 1737256310,
        "date_delete_max": 1740136052,
        "row_count": 773,
    },
    {
        "tenant_id": 1686616431892,
        "id_min": 13425384401,
        "id_max": 268472578993,
        "date_delete_min": 1731208743,
        "date_delete_max": 1731647257,
        "row_count": 689,
    },
    {
        "tenant_id": 248755617873,
        "id_min": 60913754625,
        "id_max": 268680659523,
        "date_delete_min": 1689743300,
        "date_delete_max": 1731212187,
        "row_count": 632,
    },
    {
        "tenant_id": 1260474526836,
        "id_min": 2539052885,
        "id_max": 269101530948,
        "date_delete_min": 1693549020,
        "date_delete_max": 1693711797,
        "row_count": 607,
    },
    {
        "tenant_id": 373801950214,
        "id_min": 2263665376,
        "id_max": 267924245937,
        "date_delete_min": 1726646775,
        "date_delete_max": 1740630614,
        "row_count": 595,
    },
    {
        "tenant_id": 307149796768,
        "id_min": 4981578559,
        "id_max": 268156807937,
        "date_delete_min": 1671086032,
        "date_delete_max": 1730881741,
        "row_count": 362,
    },
    {
        "tenant_id": 130081984548,
        "id_min": 4409014011,
        "id_max": 268931279095,
        "date_delete_min": 1650606086,
        "date_delete_max": 1651496969,
        "row_count": 198,
    },
    {
        "tenant_id": 1464325207666,
        "id_min": 14304010913,
        "id_max": 264984288229,
        "date_delete_min": 1646981994,
        "date_delete_max": 1647001820,
        "row_count": 177,
    },
    {
        "tenant_id": 1446938121600,
        "id_min": 2147530545,
        "id_max": 2151644866,
        "date_delete_min": 1756722478,
        "date_delete_max": 1756722478,
        "row_count": 170,
    },
    {
        "tenant_id": 7231661608292,
        "id_min": 4150899886,
        "id_max": 264551336306,
        "date_delete_min": 1729909004,
        "date_delete_max": 1730601479,
        "row_count": 102,
    },
    {
        "tenant_id": 801333372738,
        "id_min": 5086655110,
        "id_max": 265046836994,
        "date_delete_min": 1752030094,
        "date_delete_max": 1752042711,
        "row_count": 101,
    },
    {
        "tenant_id": 1462595188720,
        "id_min": 15797178483,
        "id_max": 269037913585,
        "date_delete_min": 1742622941,
        "date_delete_max": 1742712759,
        "row_count": 83,
    },
    {
        "tenant_id": 469006140324,
        "id_min": 9365253858,
        "id_max": 263343867314,
        "date_delete_min": 1723009527,
        "date_delete_max": 1723212061,
        "row_count": 83,
    },
    {
        "tenant_id": 463855761557,
        "id_min": 2147484070,
        "id_max": 2151964563,
        "date_delete_min": 1756722478,
        "date_delete_max": 1756722478,
        "row_count": 80,
    },
    {
        "tenant_id": 75899570098,
        "id_min": 2652726438,
        "id_max": 228597890181,
        "date_delete_min": 1747611847,
        "date_delete_max": 1757671795,
        "row_count": 73,
    },
    {
        "tenant_id": 1438991927250,
        "id_min": 9102138530,
        "id_max": 266935101574,
        "date_delete_min": 1700199170,
        "date_delete_max": 1730509943,
        "row_count": 72,
    },
    {
        "tenant_id": 1629529079510,
        "id_min": 3923303357,
        "id_max": 268529181921,
        "date_delete_min": 1746126471,
        "date_delete_max": 1746158969,
        "row_count": 57,
    },
    {
        "tenant_id": 446716762119,
        "id_min": 6518238226,
        "id_max": 268230047461,
        "date_delete_min": 1642663135,
        "date_delete_max": 1642724117,
        "row_count": 52,
    },
)

SLICE_COUNT = min(len(RECENT_WINDOW_SLICES), len(SEQUENTIAL_SLICES), len(CLEANUP_SLICES))


@dataclass(frozen=True)
class WorkloadQuery:
    name: str
    sql_template: str
    param_key: str
    requires_result: bool = False
    weight: int = 1

    def render(self, params: Dict[str, Any]) -> str:
        """Fill the SQL template with the supplied parameters."""

        return self.sql_template.format(**params)


RECENT_WINDOW_QUERY = WorkloadQuery(
    name="recent_window_select",
    sql_template=(
        "SELECT "
        "id, tenant_id, user_id, date_create, token, pub_token, size, is_stored,"
        " original_name, stored_name, title, mimetype, parent_id, contents,"
        " contents_enc, contents_highlight, contents_highlight_enc, highlight_type,"
        " metadata, metadata_enc, external_url, is_deleted, date_delete,"
        " is_public, pub_shared, last_indexed, source, external_id,"
        " service_type_id, service_id, is_multitenant, original_tenant_id,"
        " tenants_shared_with, service_tenant_id, is_tombstoned, thumbnail_version,"
        " date_thumbnail_retrieved, external_ptr, md5, metadata_version,"
        " unencrypted_metadata, restriction_type, version, weight_string(date_create)"
        " FROM repro_db.files"
        " WHERE tenant_id = {tenant_id}"
        "   AND is_stored = 0"
        "   AND date_create > {date_threshold}"
        " ORDER BY files.date_create ASC"
        " LIMIT 10"
    ),
    param_key="recent_window",
    requires_result=True,
)

SEQUENTIAL_QUERY = WorkloadQuery(
    name="sequential_id_scan",
    sql_template=(
        "SELECT "
        "id, tenant_id, user_id, date_create, token, pub_token, size, is_stored,"
        " original_name, stored_name, title, mimetype, parent_id, contents,"
        " contents_enc, contents_highlight, contents_highlight_enc, highlight_type,"
        " metadata, metadata_enc, external_url, is_deleted, date_delete,"
        " is_public, pub_shared, last_indexed, source, external_id,"
        " service_type_id, service_id, is_multitenant, original_tenant_id,"
        " tenants_shared_with, service_tenant_id, is_tombstoned, thumbnail_version,"
        " date_thumbnail_retrieved, external_ptr, md5, metadata_version,"
        " unencrypted_metadata, restriction_type, version, weight_string(id)"
        " FROM repro_db.files IGNORE INDEX (`primary`)"
        " WHERE tenant_id = {tenant_id}"
        "   AND id > {id_floor_small}"
        " ORDER BY files.id ASC"
        " LIMIT 10"
    ),
    param_key="sequential_scan",
    requires_result=True,
    weight=2,
)

CLEANUP_QUERY = WorkloadQuery(
    name="cleanup_batch_prune",
    sql_template=(
        "SELECT "
        " id, tenant_id, user_id, date_create, token, pub_token, "
        "size, is_stored, original_name, stored_name, title, mimetype, parent_id,"
        " contents, contents_enc, contents_highlight, contents_highlight_enc,"
        " highlight_type, metadata, metadata_enc, external_url, is_deleted,"
        " date_delete, is_public, pub_shared, last_indexed, source, external_id,"
        " service_type_id, service_id, is_multitenant, original_tenant_id,"
        " tenants_shared_with, service_tenant_id, is_tombstoned, thumbnail_version,"
        " date_thumbnail_retrieved, external_ptr, md5, metadata_version,"
        " unencrypted_metadata, restriction_type, version, weight_string(id)"
        " FROM repro_db.files"
        " WHERE tenant_id = {tenant_id}"
        "   AND highlight_type NOT IN {highlight_types}"
        "   AND date_delete != 0"
        "   AND date_delete <= {date_cutoff}"
        "   AND id > {id_floor}"
        " ORDER BY files.id ASC"
        " LIMIT 1000"
    ),
    param_key="cleanup_batch",
    requires_result=True,
)


WORKLOADS: Sequence[WorkloadQuery] = (
    RECENT_WINDOW_QUERY,
    SEQUENTIAL_QUERY,
    CLEANUP_QUERY,
)

EXPANDED_WORKLOADS: Tuple[WorkloadQuery, ...] = tuple(
    workload
    for workload in WORKLOADS
    for _ in range(max(1, workload.weight))
)

WORKLOAD_NAMES: Tuple[str, ...] = tuple(sorted({workload.name for workload in EXPANDED_WORKLOADS}))



@dataclass
class StackCollector:
    container: str
    os_pid: int
    duration: float
    output_dir: Path
    enabled: bool = True

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if not self.enabled:
            return
        try:
            subprocess.run(
                ["docker", "exec", self.container, "perf", "--version"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            logging.error(
                "perf not available inside container %s; disabling stack capture",
                self.container,
            )
            self.enabled = False

    def profile(self, label: str, iteration: int, func, *, repeat: int = 1):
        if not self.enabled:
            return func(), None

        perf_data = f"/tmp/perf-{self.os_pid}-{iteration}-r{max(repeat,1)}.data"
        perf_duration = max(self.duration, 0.05)
        cmd = [
            "docker",
            "exec",
            self.container,
            "perf",
            "record",
            "--call-graph",
            "dwarf,16384",
            "-e",
            "cpu-clock:u",
            "-F",
            "999",
            "-q",
            "-o",
            perf_data,
            "-p",
            str(self.os_pid),
            "--",
            "sleep",
            str(perf_duration),
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        caught_exc = None
        result = None
        try:
            result = func()
        except Exception as exc:  # pragma: no cover - propagate after cleanup
            caught_exc = exc
        finally:
            _, stderr = proc.communicate()

        if caught_exc:
            raise caught_exc

        if proc.returncode != 0:
            logging.warning(
                "perf record failed for %s (iteration %d, rc=%s): %s",
                label,
                iteration,
                proc.returncode,
                (stderr or b"").decode().strip() or "<no stderr>",
            )
            return result, None

        try:
            trace_text = subprocess.check_output(
                ["docker", "exec", self.container, "perf", "script", "--header", "--demangle", "-i", perf_data],
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            logging.warning(
                "perf script failed for %s (iteration %d): %s",
                label,
                iteration,
                exc,
            )
            trace_text = exc.output if hasattr(exc, "output") and exc.output else ""
        finally:
            subprocess.run(["docker", "exec", self.container, "rm", "-f", perf_data], check=False)

        output_file = self.output_dir / f"{label}_iter{iteration:06d}_r{max(repeat,1)}.perf.txt"
        output_file.write_text(trace_text)
        return result, output_file


# Helper utilities




def get_thread_os_id(cursor: Any) -> Optional[int]:
    cursor.execute("SELECT CONNECTION_ID()")
    conn_row = cursor.fetchone()
    if conn_row is None:
        return None
    if isinstance(conn_row, dict):
        conn_id = next(iter(conn_row.values()))
    else:
        conn_id = conn_row[0]

    cursor.execute("SELECT THREAD_OS_ID FROM performance_schema.threads WHERE PROCESSLIST_ID = %s", (conn_id,))
    thread_row = cursor.fetchone()
    if not thread_row:
        return None
    if isinstance(thread_row, dict):
        value = thread_row.get("THREAD_OS_ID")
    else:
        value = thread_row[0]
    return int(value) if value is not None else None


def fetch_dataset_totals(cursor: Any) -> Dict[str, int]:
    """Fetch lightweight repro_db.files stats without scanning the table."""

    totals = {
        "min_id": 0,
        "max_id": 0,
        "min_tenant": 0,
        "max_tenant": 0,
        "total_rows": 0,
    }

    # table_rows is an estimate but good enough for a health check, and it avoids COUNT(*) on InnoDB.
    cursor.execute(
        """
        SELECT TABLE_ROWS
          FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        ("repro_db", "files"),
    )
    row = cursor.fetchone()
    if row:
        if isinstance(row, dict):
            value = row.get("TABLE_ROWS")
        else:
            value = row[0]
        totals["total_rows"] = int(value or 0)

    cursor.execute("SELECT id FROM repro_db.files ORDER BY id ASC LIMIT 1")
    row = cursor.fetchone()
    if row:
        totals["min_id"] = int(row["id"] if isinstance(row, dict) else row[0])

    cursor.execute("SELECT id FROM repro_db.files ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        totals["max_id"] = int(row["id"] if isinstance(row, dict) else row[0])

    cursor.execute("SELECT tenant_id FROM repro_db.files ORDER BY tenant_id ASC LIMIT 1")
    row = cursor.fetchone()
    if row:
        totals["min_tenant"] = int(row["tenant_id"] if isinstance(row, dict) else row[0])

    cursor.execute("SELECT tenant_id FROM repro_db.files ORDER BY tenant_id DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        totals["max_tenant"] = int(row["tenant_id"] if isinstance(row, dict) else row[0])

    return totals


def build_precomputed_fixtures() -> Dict[str, List[Dict[str, int]]]:
    """Return the curated slice definitions gathered from repro-local-ps8036."""

    return {
        "recent_window": [dict(slice_def) for slice_def in RECENT_WINDOW_SLICES[:SLICE_COUNT]],
        "sequential_scan": [dict(slice_def) for slice_def in SEQUENTIAL_SLICES[:SLICE_COUNT]],
        "cleanup_batch": [dict(slice_def) for slice_def in CLEANUP_SLICES[:SLICE_COUNT]],
    }


@dataclass(frozen=True)
class DatasetContext:
    totals: Dict[str, int]
    fixtures: Dict[str, List[Dict[str, int]]]
    slice_count: int


def build_query_params(
    param_key: str,
    dataset: DatasetContext,
    slice_index: int,
    *,
    rng: Optional[random.Random] = None,
) -> Dict[str, Any]:
    rng_obj = rng or random

    slices = dataset.fixtures.get(param_key)
    if not slices:
        raise ValueError(f"Unknown param key {param_key}")

    fixture = slices[slice_index % len(slices)]

    if param_key == "recent_window":
        base = max(fixture["date_min"], RECENT_WINDOW_DATE_THRESHOLD)
        span = max(0, fixture["date_max"] - base)
        if span > 0:
            date_threshold = base + rng_obj.randint(0, span)
        else:
            date_threshold = base
        return {
            "tenant_id": fixture["tenant_id"],
            "date_threshold": date_threshold,
        }

    if param_key == "sequential_scan":
        id_min = fixture["id_min"]
        id_max = fixture["id_max"]
        span = max(0, id_max - id_min)
        if span > 20:
            id_floor = id_min + rng_obj.randint(0, span - 10)
        else:
            id_floor = id_min
        return {
            "tenant_id": fixture["tenant_id"],
            "id_floor_small": max(id_min, id_floor),
        }

    if param_key == "cleanup_batch":
        id_min = fixture["id_min"]
        id_max = fixture["id_max"]
        span = max(0, id_max - id_min)
        if span > 500:
            id_floor = id_max - rng_obj.randint(50, min(span, 500))
        else:
            id_floor = id_min

        delete_min = fixture.get("date_delete_min", 1)
        delete_max = fixture.get("date_delete_max", CLEANUP_DATE_CUTOFF_MAX)
        if delete_min > delete_max:
            delete_min, delete_max = delete_max, delete_min
        date_cutoff = rng_obj.randint(delete_min, max(delete_min, delete_max))

        return {
            "tenant_id": fixture["tenant_id"],
            "highlight_types": HIGHLIGHT_FILTER,
            "date_cutoff": date_cutoff,
            "id_floor": id_floor,
        }

    raise ValueError(f"Unhandled param key {param_key}")


def run_query(
    conn: Any,
    cursor: Any,
    query: WorkloadQuery,
    sql: str,
    iteration: int,
    *,
    explain: bool,
    show_trace: bool,
    stack_collector: Optional[StackCollector],
    stack_repeat: int = 1,
    stack_label: Optional[str] = None,
) -> Tuple[int, float, Optional[Path]]:
    def execute_single_sql():
        start_ts = time.time()
        if explain:
            cursor.execute(f"EXPLAIN {sql}")
            cursor.fetchall()
        cursor.execute(sql)

        if query.requires_result:
            rows_out = len(cursor.fetchall())
        else:
            rows_out = cursor.rowcount if cursor.rowcount != -1 else 0
            cursor.fetchall()

        elapsed_time = time.time() - start_ts

        trace_snippet = None
        if show_trace:
            if USING_MYSQL_CONNECTOR:
                trace_cursor = conn.cursor(dictionary=True)  # type: ignore[union-attr]
            else:
                trace_cursor = conn.cursor()
            try:
                trace_cursor.execute("SELECT TRACE FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE")
                trace_row = trace_cursor.fetchone()
                if trace_row:
                    if isinstance(trace_row, dict):
                        trace_snippet = trace_row.get("TRACE", "")[:512]
                    else:
                        trace_snippet = trace_row[0][:512]
            finally:
                trace_cursor.close()
        return rows_out, elapsed_time, trace_snippet

    repeat_count = max(stack_repeat, 1)
    label = stack_label or query.name

    if stack_collector and stack_collector.enabled:
        def execute_sql_bundle():
            total_elapsed = 0.0
            last_rows: int = 0
            last_trace: Optional[str] = None
            for _ in range(repeat_count):
                rows_out, elapsed_time, trace_snippet = execute_single_sql()
                last_rows = rows_out
                total_elapsed += elapsed_time
                if trace_snippet:
                    last_trace = trace_snippet
            return last_rows, total_elapsed, last_trace

        (rows, elapsed, trace_snippet), stack_path = stack_collector.profile(
            label,
            iteration,
            execute_sql_bundle,
            repeat=repeat_count,
        )
    else:
        total_elapsed = 0.0
        last_rows: int = 0
        last_trace: Optional[str] = None
        for _ in range(repeat_count):
            rows_out, elapsed_time, trace_snippet = execute_single_sql()
            last_rows = rows_out
            total_elapsed += elapsed_time
            if trace_snippet:
                last_trace = trace_snippet
        rows, elapsed, trace_snippet = last_rows, total_elapsed, last_trace
        stack_path = None

    if trace_snippet is not None:
        logging.debug("TRACE %s: %s...", query.name, trace_snippet.replace("\n", ' ')[:200])

    return rows, elapsed, stack_path


def run_worker(
    worker_id: int,
    iterations: int,
    args: argparse.Namespace,
    dataset: DatasetContext,
    stack_output_dir: Path,
    stop_event: threading.Event,
    error_sink: List[Tuple[int, Exception]],
    error_lock: threading.Lock,
    coverage_counts: Dict[str, int],
    coverage_lock: threading.Lock,
    deadline: Optional[float],
) -> None:
    rng = random.Random(time.time() + worker_id)
    conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
    try:
        if USING_MYSQL_CONNECTOR:
            cursor = conn.cursor(dictionary=True)  # type: ignore[union-attr]
        else:
            cursor = conn.cursor()
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {PER_QUERY_TIMEOUT_MS}")

        stack_collector: Optional[StackCollector] = None
        if args.collect_stack:
            thread_os_id = get_thread_os_id(cursor)
            if thread_os_id is None:
                logging.warning("Worker %02d unable to determine THREAD_OS_ID; stacks disabled", worker_id)
            else:
                stack_collector = StackCollector(
                    container=args.sandbox_container,
                    os_pid=thread_os_id,
                    duration=args.stack_duration,
                    output_dir=stack_output_dir / f"worker{worker_id:02d}",
                )
                if not stack_collector.enabled:
                    stack_collector = None

        local_counts: Counter[str] = Counter()
        counts_recorded = False

        def record_counts() -> None:
            nonlocal counts_recorded
            if counts_recorded:
                return
            if local_counts:
                with coverage_lock:
                    for name, value in local_counts.items():
                        coverage_counts[name] = coverage_counts.get(name, 0) + value
            counts_recorded = True

        label_prefix = f"W{worker_id:02d}"
        max_iterations = iterations if iterations > 0 else None
        iteration = 0
        num_slices = max(1, dataset.slice_count)
        start_offset = (worker_id - 1) % num_slices
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break
            if max_iterations is not None and iteration >= max_iterations:
                break
            iteration += 1
            slice_index = (iteration - 1 + start_offset) % num_slices
            for workload in EXPANDED_WORKLOADS:
                query_params = build_query_params(
                    workload.param_key,
                    dataset,
                    slice_index,
                    rng=rng,
                )
                logging.debug(
                    "%s iteration %s %s params %s",
                    label_prefix,
                    iteration,
                    workload.name,
                    query_params,
                )
                sql = workload.render(query_params)
                try:
                    rows, elapsed, stack_path = run_query(
                        conn,
                        cursor,
                        workload,
                        sql,
                        iteration,
                        explain=args.explain,
                        show_trace=args.show_trace,
                        stack_collector=stack_collector,
                        stack_repeat=args.stack_repeat,
                        stack_label=f"{workload.name}_{label_prefix}",
                    )
                    if stack_path:
                        logging.info(
                            "[%s][%03d] %s rows=%s elapsed=%.3fs repeat=%d stack=%s",
                            label_prefix,
                            iteration,
                            workload.name,
                            rows,
                            elapsed,
                            args.stack_repeat,
                            stack_path,
                        )
                    else:
                        logging.info(
                            "[%s][%03d] %s rows=%s elapsed=%.3fs repeat=%d",
                            label_prefix,
                            iteration,
                            workload.name,
                            rows,
                            elapsed,
                            args.stack_repeat,
                        )
                    local_counts[workload.name] += 1
                except Exception as exc:  # pragma: no cover - diagnostic path
                    with error_lock:
                        error_sink.append((worker_id, exc))
                    stop_event.set()
                    logging.exception("Worker %02d query failed", worker_id)
                    record_counts()
                    return
            if args.sleep:
                time.sleep(args.sleep)
    except Exception as exc:  # pragma: no cover
        with error_lock:
            error_sink.append((worker_id, exc))
        stop_event.set()
        logging.exception("Worker %02d aborted", worker_id)
        record_counts()
    finally:
        try:
            conn.close()
        except Exception:
            pass
        record_counts()


# CLI wiring


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("WORKLOAD_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("WORKLOAD_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("WORKLOAD_USER", "root"))
    parser.add_argument(
        "--password",
        default=os.getenv("WORKLOAD_PASSWORD", ""),
        help="MySQL password (defaults to WORKLOAD_PASSWORD env variable)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of workload cycles per worker (0 means no limit)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=float(os.getenv("WORKLOAD_DURATION", "0")),
        help="Minimum seconds to run; keeps looping until this time elapses (default: 0).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between iterations (default: 0)",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Prepends EXPLAIN to each workload query before execution",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--log-file",
        default=os.getenv("WORKLOAD_LOG_FILE"),
        help="Optional path to a log file; default is logs/run_<timestamp>_w<workers>_i<iterations>.log",
    )
    parser.add_argument(
        "--show-source-info",
        action="store_true",
        help="Print source tree paths used by this harness.",
    )
    parser.add_argument(
        "--show-trace",
        action="store_true",
        help="Fetch optimizer trace JSON after each query (use with caution).",
    )
    parser.add_argument(
        "--collect-stack",
        action="store_true",
        help="Capture perf stack traces for each query (requires perf inside the sandbox).",
    )
    parser.add_argument(
        "--stack-duration",
        type=float,
        default=0.2,
        help="Seconds perf should sample when collecting stacks (default: 0.2).",
    )
    parser.add_argument(
        "--sandbox-container",
        default=os.getenv("WORKLOAD_SANDBOX", "workload-local-ps8036"),
        help="Docker container name running the sandbox (for perf exec).",
    )
    parser.add_argument(
        "--stack-output-dir",
        default=str(Path(__file__).resolve().parent / "stack_dumps"),
        help="Directory to store perf script outputs (default: stack_dumps).",
    )
    parser.add_argument(
        "--stack-repeat",
        type=int,
        default=1,
        help="Execute each query this many times while perf samples (default: 1).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent worker threads to run (default: 1).",
    )
    return parser.parse_args(argv)


def configure_logging(args: argparse.Namespace) -> Optional[Path]:
    """Set up logging to stdout and, if requested, a file."""

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    log_path: Optional[Path]
    if args.log_file:
        log_path = Path(args.log_file).expanduser()
    else:
        logs_dir = Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        duration_part = f"_d{int(args.duration)}" if args.duration else ""
        log_path = logs_dir / f"run_{timestamp}_w{args.workers}_i{args.iterations}{duration_part}.log"

    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(log_path))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )

    return log_path


# Main entry point


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    log_path = configure_logging(args)

    if log_path:
        logging.info("Logging to %s", log_path)

    if args.iterations < 0:
        logging.error("iterations must be >= 0 (got %s)", args.iterations)
        return 1
    if args.duration < 0:
        logging.error("duration must be >= 0 (got %s)", args.duration)
        return 1

    if args.duration:
        logging.info("Target duration %.1fs (~%.2fh)", args.duration, args.duration / 3600)
    if args.iterations == 0:
        logging.info("Iterations set to unlimited; duration/stop event will control runtime")

    if args.show_source_info:
        logging.info("Source tree: %s", SOURCE_INFO['percona_server_root'])

    if args.workers < 1:
        logging.error("workers must be >= 1 (got %s)", args.workers)
        return 1
    if args.stack_repeat < 1:
        logging.error("stack-repeat must be >= 1 (got %s)", args.stack_repeat)
        return 1

    stack_output_dir = Path(args.stack_output_dir)
    stack_output_dir.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Preparing dataset stats (host=%s port=%s user=%s)",
        args.host,
        args.port,
        args.user,
    )

    stats_conn = open_connection(
        host=args.host, port=args.port, user=args.user, password=args.password
    )
    stats_cursor = None
    dataset: Optional[DatasetContext] = None
    try:
        if USING_MYSQL_CONNECTOR:
            stats_cursor = stats_conn.cursor(dictionary=True)  # type: ignore[union-attr]
        else:
            stats_cursor = stats_conn.cursor()
        stats_cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {PER_QUERY_TIMEOUT_MS}")

        totals = fetch_dataset_totals(stats_cursor)
        fixtures = build_precomputed_fixtures()
        if SLICE_COUNT == 0:
            raise RuntimeError("No workload slices available; please refresh the slice catalog.")
        dataset = DatasetContext(totals=totals, fixtures=fixtures, slice_count=SLICE_COUNT)
        logging.info(
            "Loaded %d workload slice set(s); per-query timeout %d ms",
            dataset.slice_count,
            PER_QUERY_TIMEOUT_MS,
        )
    except Exception as exc:
        logging.error("Failed to prepare dataset stats: %s", exc)
        return 1
    finally:
        if stats_cursor is not None:
            try:
                stats_cursor.close()
            except Exception:
                pass
        try:
            stats_conn.close()
        except Exception:
            pass

    if dataset is None:
        logging.error("Dataset preparation failed; aborting")
        return 1

    logging.info(
        "Dataset stats: rows=%s id[%s,%s] tenant[%s,%s]",
        dataset.totals["total_rows"],
        dataset.totals["min_id"],
        dataset.totals["max_id"],
        dataset.totals["min_tenant"],
        dataset.totals["max_tenant"],
    )
    iteration_label = "unlimited" if args.iterations == 0 else str(args.iterations)
    logging.info(
        "Launching %d worker(s) × %s iterations (stack_repeat=%d)",
        args.workers,
        iteration_label,
        args.stack_repeat,
    )

    stop_event = threading.Event()
    error_sink: List[Tuple[int, Exception]] = []
    error_lock = threading.Lock()
    coverage_lock = threading.Lock()
    coverage_counts: Dict[str, int] = {name: 0 for name in WORKLOAD_NAMES}
    threads: List[threading.Thread] = []
    start_time = time.time()

    deadline = time.time() + args.duration if args.duration else None

    for worker_id in range(1, args.workers + 1):
        thread = threading.Thread(
            target=run_worker,
            args=(
                worker_id,
                args.iterations,
                args,
                dataset,
                stack_output_dir,
                stop_event,
                error_sink,
                error_lock,
                coverage_counts,
                coverage_lock,
                deadline,
            ),
            name=f"worker-{worker_id:02d}",
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if error_sink:
        worker_id, exc = error_sink[0]
        logging.error("Worker %02d encountered an error: %s", worker_id, exc)
        return 1

    if stop_event.is_set():
        logging.warning("Termination requested; workload ended early")
        return 1

    elapsed = time.time() - start_time
    if deadline:
        logging.info(
            "Elapsed %.1fs against target %.1fs", elapsed, args.duration
        )
    else:
        logging.info("Elapsed %.1fs", elapsed)

    logging.info(
        "Workers finished normally (%d worker(s), stack_repeat=%d, iteration goal=%s)",
        args.workers,
        args.stack_repeat,
        iteration_label,
    )

    logging.info("Workload coverage summary:")
    for name in WORKLOAD_NAMES:
        logging.info("  %s: %d", name, coverage_counts.get(name, 0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
