#!/usr/bin/env python3
"""Replay the repro_db.files workload.

"""
from __future__ import annotations

import argparse
import hashlib
import json
import string
import logging
import os
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

VENDOR_DIR = Path(__file__).resolve().parent / "vendor"
if VENDOR_DIR.exists():
    sys.path.insert(0, str(VENDOR_DIR))

try:  # Python 3.11+
    import tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for 3.10
    try:
        import tomli as tomllib  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
        tomllib = None  # type: ignore

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

MODE_CHOICES: Tuple[str, ...] = ("dual", "writer", "reader")

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
    """Open a MySQL connection using whichever driver is installed."""

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
PER_QUERY_TIMEOUT_MS = 60_000
RECENT_WINDOW_SECONDS = 7 * 24 * 3600
RECENT_TARGET_ROWS = 500
SEQUENTIAL_TARGET_ROWS = 45_000
CLEANUP_TARGET_ROWS = 5_000
CLEANUP_DATE_WINDOW = 7 * 24 * 3600

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

CATALOG_SEQUENTIAL_ENTRIES: Tuple[Dict[str, int], ...] = (
    {"key": "catalog_seq_tenant_75899570098", "tenant_id": 75899570098, "id_floor": 9_605_126_712},
    {"key": "catalog_seq_tenant_2981626498", "tenant_id": 2_981_626_498, "id_floor": 92_256_644_502},
    {"key": "catalog_seq_tenant_2151557247", "tenant_id": 2_151_557_247, "id_floor": 4_586_750_407},
    {"key": "catalog_seq_tenant_147557470051", "tenant_id": 147_557_470_051, "id_floor": 2_305_882_893},
    {"key": "catalog_seq_tenant_10736132949", "tenant_id": 10_736_132_949, "id_floor": 18_964_520_038},
)

CATALOG_CLEANUP_ENTRIES: Tuple[Dict[str, int], ...] = (
    {
        "key": "catalog_cleanup_tenant_129325844705",
        "tenant_id": 129_325_844_705,
        "date_cutoff": 1_675_740_108,
        "id_floor": 2_185_675_980,
    },
    {
        "key": "catalog_cleanup_tenant_266105194887",
        "tenant_id": 266_105_194_887,
        "date_cutoff": 1_756_925_112,
        "id_floor": 3_231_369_225,
    },
    {
        "key": "catalog_cleanup_tenant_3022319763812",
        "tenant_id": 3_022_319_763_812,
        "date_cutoff": 1_739_573_710,
        "id_floor": 4_717_928_749,
    },
    {
        "key": "catalog_cleanup_tenant_463855761557_base",
        "tenant_id": 463_855_761_557,
        "date_cutoff": 1_750_933_684,
        "id_floor": 7_345_917_986_898,
    },
    {
        "key": "catalog_cleanup_tenant_463855761557_shift",
        "tenant_id": 463_855_761_557,
        "date_cutoff": 1_756_722_478,
        "id_floor": 7_409_689_955_523,
    },
)

CATALOG_BACKFILL_ENTRIES: Tuple[Dict[str, int], ...] = (
    {
        "key": "catalog_backfill_id",
        "tenant_id": 1_446_938_121_600,
        "id_floor": 1_173_300_827_680,
    },
    {
        "key": "catalog_backfill_date",
        "tenant_id": 1_446_938_121_600,
        "id_floor": 1_173_300_827_680,
    },
)

CATALOG_SEQUENTIAL_PARAMS = {entry["key"]: entry for entry in CATALOG_SEQUENTIAL_ENTRIES}
CATALOG_CLEANUP_PARAMS = {entry["key"]: entry for entry in CATALOG_CLEANUP_ENTRIES}
CATALOG_BACKFILL_PARAMS = {entry["key"]: entry for entry in CATALOG_BACKFILL_ENTRIES}

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
        " FROM repro_db.files FORCE INDEX (`primary`)"
        " WHERE tenant_id = {tenant_id}"
        "   AND is_stored = 0"
        "   AND date_create > {date_threshold}"
        " ORDER BY files.date_create ASC"
        " LIMIT 10"
    ),
    param_key="recent_window",
    requires_result=True,
    weight=0,
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
        " FROM repro_db.files FORCE INDEX (`primary`)"
        " WHERE id > {id_floor_small}"
        " ORDER BY files.id ASC"
        " LIMIT 10"
    ),
    param_key="sequential_scan",
    requires_result=True,
    weight=1,
)

PRIMARY_ID_QUERY = WorkloadQuery(
    name="primary_id_scan",
    sql_template=(
        "SELECT id, tenant_id, date_create "
        "FROM repro_db.files FORCE INDEX (`primary`) "
        "WHERE id <= {id_floor_small} "
        "ORDER BY id DESC "
        "LIMIT 10"
    ),
    param_key="sequential_scan",
    requires_result=True,
    weight=20,
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
        " FROM repro_db.files FORCE INDEX (`primary`)"
        " WHERE tenant_id = {tenant_id}"
        "   AND highlight_type NOT IN {highlight_types}"
        "   AND date_delete != 0"
        "   AND date_delete <= {date_cutoff}"
        "   AND id > {id_floor}"
        " ORDER BY files.id ASC"
        " LIMIT 250"
    ),
    param_key="cleanup_batch",
    requires_result=True,
    weight=0,
)

BACKFILL_ID_QUERY = WorkloadQuery(
    name="backfill_id_scan",
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
        " FROM repro_db.files FORCE INDEX (`primary`)"
        " WHERE tenant_id = {tenant_id}"
        "   AND id > {id_floor_small}"
        " ORDER BY files.id ASC"
        " LIMIT 10"
    ),
    param_key="catalog_backfill_id",
    requires_result=True,
    weight=0,
)

BACKFILL_DATE_QUERY = WorkloadQuery(
    name="backfill_date_scan",
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
        "   AND id > {id_floor_small}"
        " ORDER BY files.date_create ASC"
        " LIMIT 10"
    ),
    param_key="catalog_backfill_date",
    requires_result=True,
    weight=0,
)

CATALOG_SEQUENTIAL_WORKLOADS: Tuple[WorkloadQuery, ...] = tuple(
    WorkloadQuery(
        name=f"catalog_seq_tenant_{entry['tenant_id']}",
        sql_template=SEQUENTIAL_QUERY.sql_template,
        param_key=entry["key"],
        requires_result=True,
        weight=0,
    )
    for entry in CATALOG_SEQUENTIAL_ENTRIES
)

CATALOG_CLEANUP_WORKLOADS: Tuple[WorkloadQuery, ...] = tuple(
    WorkloadQuery(
        name=f"catalog_cleanup_tenant_{entry['tenant_id']}",
        sql_template=CLEANUP_QUERY.sql_template,
        param_key=entry["key"],
        requires_result=True,
        weight=0,
    )
    for entry in CATALOG_CLEANUP_ENTRIES
)

WORKLOADS: Sequence[WorkloadQuery] = (
    RECENT_WINDOW_QUERY,
    SEQUENTIAL_QUERY,
    PRIMARY_ID_QUERY,
    CLEANUP_QUERY,
    *CATALOG_SEQUENTIAL_WORKLOADS,
    *CATALOG_CLEANUP_WORKLOADS,
    BACKFILL_ID_QUERY,
    BACKFILL_DATE_QUERY,
)

EXPANDED_WORKLOADS: Tuple[WorkloadQuery, ...] = tuple(
    workload
    for workload in WORKLOADS
    for _ in range(max(0, workload.weight))
)

if not EXPANDED_WORKLOADS:
    raise RuntimeError("No reader workloads enabled after weighting; check configuration.")

WORKLOAD_NAMES: Tuple[str, ...] = tuple(sorted({workload.name for workload in EXPANDED_WORKLOADS}))
STATE_FILE = Path(__file__).resolve().parent / "state" / "master_high_watermark.json"



# Harness configuration dataclasses --------------------------------------------


@dataclass
class ReaderOverrides:
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    workers: Optional[int] = None
    iterations: Optional[int] = None
    duration: Optional[float] = None
    sleep: Optional[float] = None
    explain: Optional[bool] = None
    log_level: Optional[str] = None
    log_file: Optional[str] = None
    show_source_info: Optional[bool] = None
    show_trace: Optional[bool] = None


@dataclass
class MasterOverrides:
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None


@dataclass
class MonitorSettings:
    enabled: bool = True
    dest_root: Path = Path("/data/percona_monitor")
    retention_days: int = 1
    run_seconds: int = 86400


@dataclass
class MonitorProcess:
    process: subprocess.Popen
    stop_flag: Path

    def stop(self, timeout: float = 15.0) -> None:
        if not self.process:
            return
        if not self.stop_flag.exists():
            try:
                self.stop_flag.touch()
            except Exception:
                logging.debug("Unable to create stop flag %s", self.stop_flag, exc_info=True)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.poll() is not None:
                break
            time.sleep(0.5)
        else:
            logging.info("Monitor still running; sending terminate")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("Monitor unresponsive; killing")
                self.process.kill()
        try:
            self.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        if self.stop_flag.exists():
            try:
                self.stop_flag.unlink()
            except Exception:
                logging.debug("Unable to remove stop flag %s", self.stop_flag, exc_info=True)


@dataclass
class CrashQuerySettings:
    key: str
    name: str
    sql: str
    threads: int = 1
    sleep: float = 0.0
    enabled: bool = True


# Writer workload configuration -------------------------------------------------


@dataclass
class InsertSettings:
    enabled: bool = False
    threads: int = 0
    batch_size: int = 1
    sleep: float = 0.0
    tenant_id: Optional[int] = None
    tenant_ids: List[int] = field(default_factory=list)
    user_id: int = 0
    blob_min_bytes: int = 16_384
    blob_max_bytes: int = 49_152
    payload_mode: str = "random"
    payload_path: Optional[str] = None
    source_tag: str = "HARNESS_V3"
    metadata_prefix: str = "harness_v3"
    highlight_type: str = "harness_v3"


@dataclass
class UpdateSettings:
    enabled: bool = False
    threads: int = 0
    batch_size: int = 1
    sleep: float = 0.0
    columns: List[str] = field(
        default_factory=lambda: ["contents", "contents_highlight", "metadata"]
    )
    blob_min_bytes: int = 16_384
    blob_max_bytes: int = 49_152
    payload_mode: str = "random"
    payload_path: Optional[str] = None
    reuse_fraction: float = 0.0
    tenant_ids: List[int] = field(default_factory=list)
    highlight_type: str = "harness_v3"


@dataclass
class RecycleSettings:
    enabled: bool = False
    threads: int = 0
    batch_size: int = 1
    sleep: float = 0.0
    tenant_ids: List[int] = field(default_factory=list)
    blob_min_bytes: int = 16_384
    blob_max_bytes: int = 262_144
    metadata_bytes: int = 48_000
    tenants_shared_with_bytes: int = 32_000
    payload_mode: str = "mixed"
    payload_path: Optional[str] = None
    metadata_prefix: str = "harness_recycle"
    source_tag: str = "HARNESS_RECYCLE"
    highlight_type: str = "harness_recycle"


@dataclass
class DeleteSettings:
    enabled: bool = False
    threads: int = 0
    batch_size: int = 1
    sleep: float = 0.0
    mode: str = "soft"
    target: str = "inserted"
    tenant_ids: List[int] = field(default_factory=list)


@dataclass
class WriterConfig:
    path: Path
    insert: InsertSettings
    update: UpdateSettings
    recycle: RecycleSettings
    delete: DeleteSettings


def load_high_watermark(path: Path) -> Optional[int]:
    """Read the local JSON state file and return the stored MAX(id) if present."""

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logging.warning("High-watermark file %s not found; falling back to live MAX(id).", path)
        return None
    except json.JSONDecodeError as exc:
        logging.warning("Unable to parse %s (%s); falling back to live MAX(id).", path, exc)
        return None

    raw_value = data.get("max_id")
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logging.warning("High-watermark file %s contains invalid max_id=%r; falling back to live MAX(id).", path, raw_value)
        return None


class IdAllocator:
    """Thread-safe monotonic ID allocator for writer inserts."""

    def __init__(self, start: int) -> None:
        self._lock = threading.Lock()
        self._next = max(start, 1)

    def next_ids(self, count: int) -> List[int]:
        if count <= 0:
            return []
        with self._lock:
            ids = [self._next + i for i in range(count)]
            self._next += count
        return ids


class InsertedIdStore:
    """Tracks IDs created by the harness so updates/deletes can target them."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: List[int] = []

    def add(self, ids: Iterable[int]) -> None:
        with self._lock:
            self._ids.extend(int(value) for value in ids)

    def sample(self, rng: random.Random, count: int) -> List[int]:
        with self._lock:
            if not self._ids or count <= 0:
                return []
            pick = min(count, len(self._ids))
            return rng.sample(self._ids, pick)

    def pop(self, rng: random.Random, count: int) -> List[int]:
        with self._lock:
            if not self._ids or count <= 0:
                return []
            pick = min(count, len(self._ids))
            chosen = rng.sample(self._ids, pick)
            removal = set(chosen)
            self._ids = [value for value in self._ids if value not in removal]
        return chosen

    def __len__(self) -> int:  # pragma: no cover - debug helper
        with self._lock:
            return len(self._ids)


class WriterStats:
    """Aggregates writer outcomes for the summary footer."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Counter[Tuple[str, str]] = Counter()

    def record(self, op: str, outcome: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[(op, outcome)] += amount

    def snapshot(self) -> Dict[Tuple[str, str], int]:
        with self._lock:
            return dict(self._counts)


class PayloadFactory:
    """Generates blobs with different compressibility characteristics."""

    _PATTERN_CHARS = string.ascii_letters + string.digits

    def __init__(self, mode: str, payload_path: Optional[str]) -> None:
        self.mode = (mode or "random").lower()
        self.payload_path = Path(payload_path).expanduser() if payload_path else None
        self._file_bytes: Optional[bytes] = None
        self._lock = threading.Lock()

    def _load_file_bytes(self) -> Optional[bytes]:
        if not self.payload_path:
            return None
        with self._lock:
            if self._file_bytes is None:
                try:
                    self._file_bytes = self.payload_path.read_bytes()
                except FileNotFoundError:
                    logging.warning(
                        "Payload file %s not found; falling back to random bytes",
                        self.payload_path,
                    )
                    self._file_bytes = b""
        return self._file_bytes

    def _build_pattern(self, rng: random.Random, length: int) -> bytes:
        chunk_len = min(64, max(8, length // 8 or 8))
        chunk = ''.join(rng.choice(self._PATTERN_CHARS) for _ in range(chunk_len)).encode('ascii')
        repeats = (length // len(chunk)) + 1
        return (chunk * repeats)[:length]

    def generate(
        self,
        rng: random.Random,
        min_bytes: int,
        max_bytes: int,
        *,
        prefer_pattern: bool = False,
    ) -> bytes:
        hi = max(min_bytes, max_bytes)
        length = rng.randint(min_bytes, hi) if hi > 0 else min_bytes
        mode = self.mode
        if mode == "pattern":
            return self._build_pattern(rng, length)
        if mode == "file":
            data = self._load_file_bytes()
            if data:
                repeats = (length // len(data)) + 1
                return (data * repeats)[:length]
            return os.urandom(length)
        if mode == "mixed":
            use_pattern = prefer_pattern or rng.random() < 0.5
            if use_pattern:
                return self._build_pattern(rng, length)
            return os.urandom(length)
        # Default: random bytes, optionally bias towards pattern for reuse.
        if prefer_pattern:
            return self._build_pattern(rng, length)
        return os.urandom(length)


def random_ascii_token(rng: random.Random, length: int = 24) -> str:
    """Return a short lowercase+digit token for metadata fields."""
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(rng.choice(alphabet) for _ in range(length))


def build_metadata(prefix: str, op: str, row_id: int, tenant_id: int, blob_len: int) -> str:
    """Assemble the JSON metadata blob describing a harness write."""
    entry = {
        "harness": prefix,
        "op": op,
        "row_id": row_id,
        "tenant_id": tenant_id,
        "blob_bytes": blob_len,
        "ts": int(time.time()),
    }
    return json.dumps(entry, separators=(",", ":"))


TENANT_RANGE_CACHE: Dict[int, Tuple[int, int]] = {}
TENANT_RANGE_LOCK = threading.Lock()

HOTSPOT_DATE_WINDOW_SECONDS = 30 * 24 * 3600  # bias secondary index inserts into historical slots


def _extract_min_max(row: Any) -> Optional[Tuple[int, int]]:
    """Return (min_id, max_id) from a cursor row, guarding against NULLs or junk."""
    if not row:
        return None
    if isinstance(row, dict):
        values = list(row.values())
    else:
        values = list(row)
    if len(values) < 2:
        return None
    try:
        min_id = int(values[0]) if values[0] is not None else None
        max_id = int(values[1]) if values[1] is not None else None
    except (TypeError, ValueError):
        return None
    if min_id is None or max_id is None:
        return None
    return min_id, max_id


def _get_tenant_range(cursor: Any, tenant_id: int, *, refresh: bool = False) -> Optional[Tuple[int, int]]:
    """Fetch and cache the live id range for the requested tenant."""
    if not refresh:
        with TENANT_RANGE_LOCK:
            cached = TENANT_RANGE_CACHE.get(tenant_id)
        if cached:
            return cached

    try:
        cursor.execute(
            "SELECT MIN(id), MAX(id) FROM repro_db.files WHERE tenant_id = %s AND is_deleted = 0",
            (tenant_id,),
        )
        row = cursor.fetchone()
    except Exception:
        return None

    extracted = _extract_min_max(row)
    if not extracted:
        return None

    with TENANT_RANGE_LOCK:
        TENANT_RANGE_CACHE[tenant_id] = extracted
    return extracted


def _fetch_ids_from_offset(
    cursor: Any,
    tenant_id: int,
    lower_bound: int,
    limit: int,
) -> List[int]:
    """Pull a window of ids >= lower_bound for a tenant, ordered ascending."""
    if limit <= 0:
        return []

    sql = (
        "SELECT id FROM repro_db.files "
        "WHERE tenant_id = %s AND is_deleted = 0 AND id >= %s "
        "ORDER BY id ASC LIMIT %s"
    )
    try:
        cursor.execute(sql, (tenant_id, lower_bound, limit))
        rows = cursor.fetchall() or []
    except Exception as exc:  # pragma: no cover - depends on server schema
        logging.debug("Unable to fetch ids using offset for tenant %s: %s", tenant_id, exc)
        return []
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return [int(row["id"]) for row in rows if row.get("id") is not None]
    return [int(row[0]) for row in rows if row and row[0] is not None]


def fetch_existing_ids(
    cursor: Any,
    tenant_ids: List[int],
    limit: int,
    rng: random.Random,
) -> List[int]:
    """Sample existing ids for the supplied tenants, biasing toward fresh ranges."""
    if limit <= 0 or not tenant_ids:
        return []
    results: List[int] = []
    seen: set[int] = set()
    attempts = 0

    while len(results) < limit and attempts < max(3, limit * 2):
        tenant_id = int(rng.choice(tenant_ids))
        tenant_range = _get_tenant_range(cursor, tenant_id, refresh=False)
        if not tenant_range:
            attempts += 1
            continue
        min_id, max_id = tenant_range
        if min_id >= max_id:
            attempts += 1
            continue

        start_id = rng.randint(min_id, max_id)
        needed = limit - len(results)
        batch = _fetch_ids_from_offset(cursor, tenant_id, start_id, needed)

        if not batch:
            # Retry from the beginning of the range in case we landed past the tail,
            # optionally refreshing cached bounds.
            batch = _fetch_ids_from_offset(cursor, tenant_id, min_id, needed)
            if not batch:
                refreshed = _get_tenant_range(cursor, tenant_id, refresh=True)
                if refreshed and refreshed != tenant_range:
                    min_id, max_id = refreshed
                    start_id = rng.randint(min_id, max_id)
                    batch = _fetch_ids_from_offset(cursor, tenant_id, start_id, needed)

        for row_id in batch:
            if row_id not in seen:
                results.append(row_id)
                seen.add(row_id)
            if len(results) >= limit:
                break

        attempts += 1

    return results[:limit]


def _coerce_tenant_ids(value: Any) -> List[int]:
    """Normalize tenant id config values into a list of ints."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result
    try:
        return [int(value)]
    except (TypeError, ValueError):
        return []


def load_harness_config(
    config_path: Path,
) -> Tuple[
    ReaderOverrides,
    MasterOverrides,
    WriterConfig,
    MonitorSettings,
    List[CrashQuerySettings],
]:
    """Parse config/_v3.toml and return connection overrides plus writer/monitor/crash settings."""
    reader = ReaderOverrides()
    master = MasterOverrides()
    insert = InsertSettings()
    update = UpdateSettings()
    recycle = RecycleSettings()
    delete = DeleteSettings()
    monitor_settings = MonitorSettings()
    crash_queries: List[CrashQuerySettings] = []

    if tomllib is None:
        logging.warning(
            "tomllib/tomli not available; writer workloads disabled and reader overrides ignored",
        )
        writer_cfg = WriterConfig(config_path, insert, update, recycle, delete)
        return reader, master, writer_cfg, monitor_settings, crash_queries

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config {config_path} not found; _v3 harness expects all runtime knobs in TOML."
        )

    try:
        raw = tomllib.loads(config_path.read_text())
    except Exception as exc:  # pragma: no cover - config parse errors
        logging.error("Failed to parse %s: %s", config_path, exc)
        writer_cfg = WriterConfig(config_path, insert, update, recycle, delete)
        return reader, master, writer_cfg, monitor_settings, crash_queries

    master_section = {}
    if isinstance(raw, dict):
        read_section = raw.get("read", {})
        if isinstance(read_section, dict):
            reader.host = read_section.get("host")
            reader.port = read_section.get("port")
            reader.user = read_section.get("user")
            reader.password = read_section.get("password")
            reader.workers = read_section.get("workers")
            reader.iterations = read_section.get("iterations")
            reader.duration = read_section.get("duration")
            reader.sleep = read_section.get("sleep")
            reader.explain = read_section.get("explain")
            reader.log_level = read_section.get("log_level")
            reader.log_file = read_section.get("log_file")
            reader.show_source_info = read_section.get("show_source_info")
            reader.show_trace = read_section.get("show_trace")
        master_section = raw.get("master", {})

    master.host = reader.host
    master.port = reader.port
    master.user = reader.user
    master.password = reader.password
    if isinstance(master_section, dict):
        if master_section.get("host") is not None:
            master.host = master_section.get("host")
        if master_section.get("port") is not None:
            master.port = master_section.get("port")
        if master_section.get("user") is not None:
            master.user = master_section.get("user")
        if master_section.get("password") is not None:
            master.password = master_section.get("password")

    defaults = raw.get("defaults", {}) if isinstance(raw, dict) else {}
    crash_section = raw.get("crash_queries", {}) if isinstance(raw, dict) else {}
    monitor_section = raw.get("monitor", {}) if isinstance(raw, dict) else {}
    default_tenant_ids = _coerce_tenant_ids(defaults.get("tenant_ids"))
    default_user_id = int(defaults.get("user_id", 0) or 0)
    default_source_tag = str(defaults.get("source_tag", "HARNESS_V3"))
    default_metadata_prefix = str(defaults.get("metadata_prefix", "harness_v3"))
    default_highlight_type = str(defaults.get("highlight_type", default_metadata_prefix))
    default_payload_mode = str(defaults.get("payload_mode", "random"))
    default_payload_file = defaults.get("payload_file") or None
    default_blob_min = int(defaults.get("blob_min_bytes", 16_384) or 16_384)
    default_blob_max = int(defaults.get("blob_max_bytes", 49_152) or 49_152)
    default_reuse = float(defaults.get("reuse_fraction", 0.0) or 0.0)
    default_delete_mode = str(defaults.get("delete_mode", "soft"))
    default_delete_target = str(defaults.get("delete_target", "inserted"))
    default_sleep = float(defaults.get("writer_sleep", 0.0) or 0.0)

    insert_section = raw.get("insert", {}) if isinstance(raw, dict) else {}
    update_section = raw.get("update", {}) if isinstance(raw, dict) else {}
    delete_section = raw.get("delete", {}) if isinstance(raw, dict) else {}
    recycle_section = raw.get("recycle", {}) if isinstance(raw, dict) else {}

    # Insert settings
    insert.enabled = bool(insert_section.get("enabled", False))
    insert.threads = max(0, int(insert_section.get("threads", insert.threads)))
    insert.batch_size = max(1, int(insert_section.get("batch_size", insert.batch_size)))
    insert.sleep = float(insert_section.get("sleep", default_sleep))
    insert.tenant_id = insert_section.get("tenant_id")
    if insert.tenant_id is not None:
        try:
            insert.tenant_id = int(insert.tenant_id)
        except (TypeError, ValueError):
            insert.tenant_id = None
    insert.tenant_ids = _coerce_tenant_ids(insert_section.get("tenant_ids")) or list(default_tenant_ids)
    if insert.tenant_id is not None and insert.tenant_id not in insert.tenant_ids:
        insert.tenant_ids.append(insert.tenant_id)
    insert.user_id = int(insert_section.get("user_id", default_user_id) or default_user_id)
    insert.blob_min_bytes = max(1, int(insert_section.get("blob_min_bytes", default_blob_min) or default_blob_min))
    insert.blob_max_bytes = max(insert.blob_min_bytes, int(insert_section.get("blob_max_bytes", default_blob_max) or default_blob_max))
    insert.payload_mode = str(insert_section.get("payload_mode", default_payload_mode))
    payload_file_value = insert_section.get("payload_file", default_payload_file) or None
    insert.payload_path = payload_file_value
    insert.source_tag = str(insert_section.get("source_tag", default_source_tag))
    insert.metadata_prefix = str(insert_section.get("metadata_prefix", default_metadata_prefix))
    insert.highlight_type = str(insert_section.get("highlight_type", default_highlight_type))

    # Update settings
    update.enabled = bool(update_section.get("enabled", False))
    update.threads = max(0, int(update_section.get("threads", update.threads)))
    update.batch_size = max(1, int(update_section.get("batch_size", update.batch_size)))
    update.sleep = float(update_section.get("sleep", default_sleep))
    columns_value = update_section.get("columns")
    if columns_value:
        update.columns = [str(col) for col in columns_value]
    update.blob_min_bytes = max(1, int(update_section.get("blob_min_bytes", default_blob_min) or default_blob_min))
    update.blob_max_bytes = max(update.blob_min_bytes, int(update_section.get("blob_max_bytes", default_blob_max) or default_blob_max))
    update.payload_mode = str(update_section.get("payload_mode", default_payload_mode))
    update.payload_path = update_section.get("payload_file", default_payload_file) or None
    update.reuse_fraction = max(0.0, min(1.0, float(update_section.get("reuse_fraction", default_reuse) or default_reuse)))
    update.tenant_ids = _coerce_tenant_ids(update_section.get("tenant_ids")) or list(default_tenant_ids)
    update.highlight_type = str(update_section.get("highlight_type", default_highlight_type))

    # Delete settings
    delete.enabled = bool(delete_section.get("enabled", False))
    delete.threads = max(0, int(delete_section.get("threads", delete.threads)))
    delete.batch_size = max(1, int(delete_section.get("batch_size", delete.batch_size)))
    delete.sleep = float(delete_section.get("sleep", default_sleep))
    delete.mode = str(delete_section.get("mode", default_delete_mode)).lower()
    if delete.mode not in {"soft", "hard"}:
        delete.mode = "soft"
    delete.target = str(delete_section.get("target", default_delete_target)).lower()
    if delete.target not in {"inserted", "any"}:
        delete.target = "inserted"
    delete.tenant_ids = _coerce_tenant_ids(delete_section.get("tenant_ids")) or list(default_tenant_ids)

    # Recycle settings
    recycle.enabled = bool(recycle_section.get("enabled", False))
    recycle.threads = max(0, int(recycle_section.get("threads", recycle.threads)))
    recycle.batch_size = max(1, int(recycle_section.get("batch_size", recycle.batch_size)))
    recycle.sleep = float(recycle_section.get("sleep", default_sleep))
    recycle.tenant_ids = _coerce_tenant_ids(recycle_section.get("tenant_ids")) or list(default_tenant_ids)
    recycle.blob_min_bytes = max(1, int(recycle_section.get("blob_min_bytes", recycle.blob_min_bytes) or recycle.blob_min_bytes))
    recycle.blob_max_bytes = max(recycle.blob_min_bytes, int(recycle_section.get("blob_max_bytes", recycle.blob_max_bytes) or recycle.blob_max_bytes))
    recycle.metadata_bytes = max(1, int(recycle_section.get("metadata_bytes", recycle.metadata_bytes) or recycle.metadata_bytes))
    recycle.metadata_bytes = min(recycle.metadata_bytes, 60_000)
    recycle.tenants_shared_with_bytes = max(1, int(recycle_section.get("tenants_shared_with_bytes", recycle.tenants_shared_with_bytes) or recycle.tenants_shared_with_bytes))
    recycle.tenants_shared_with_bytes = min(recycle.tenants_shared_with_bytes, 60_000)
    recycle.payload_mode = str(recycle_section.get("payload_mode", default_payload_mode))
    recycle.payload_path = recycle_section.get("payload_file", default_payload_file) or None
    recycle.metadata_prefix = str(recycle_section.get("metadata_prefix", recycle.metadata_prefix))
    recycle.source_tag = str(recycle_section.get("source_tag", default_source_tag))
    recycle.highlight_type = str(recycle_section.get("highlight_type", default_highlight_type))

    logging.info(
        "Writer config %s loaded (insert=%d update=%d recycle=%d delete=%d threads)",
        config_path,
        insert.threads if insert.enabled else 0,
        update.threads if update.enabled else 0,
        recycle.threads if recycle.enabled else 0,
        delete.threads if delete.enabled else 0,
    )

    monitor_settings.enabled = bool(monitor_section.get("enabled", True))
    monitor_settings.dest_root = Path(
        monitor_section.get("dest_root", monitor_settings.dest_root)
    ).expanduser()
    monitor_settings.retention_days = int(
        monitor_section.get("retention_days", monitor_settings.retention_days) or 1
    )
    monitor_settings.run_seconds = int(
        monitor_section.get("run_seconds", monitor_settings.run_seconds) or 86400
    )

    if isinstance(crash_section, dict):
        for key, entry in crash_section.items():
            if not isinstance(entry, dict):
                logging.warning("Crash query %s ignored because value is not a table", key)
                continue
            sql_value = entry.get("sql")
            if not isinstance(sql_value, str) or not sql_value.strip():
                logging.warning("Crash query %s ignored because sql string is missing/empty", key)
                continue
            enabled = bool(entry.get("enabled", True))
            threads = entry.get("threads", 1)
            try:
                threads_int = max(0, int(threads))
            except (TypeError, ValueError):
                logging.warning("Crash query %s has invalid threads=%r; defaulting to 1", key, threads)
                threads_int = 1
            sleep_value = entry.get("sleep", 0.0)
            try:
                sleep_float = float(sleep_value or 0.0)
            except (TypeError, ValueError):
                logging.warning("Crash query %s has invalid sleep=%r; defaulting to 0.0", key, sleep_value)
                sleep_float = 0.0
            name = str(entry.get("name", key))
            crash_queries.append(
                CrashQuerySettings(
                    key=str(key),
                    name=name,
                    sql=sql_value.strip(),
                    threads=threads_int,
                    sleep=max(0.0, sleep_float),
                    enabled=enabled,
                )
            )

    writer_cfg = WriterConfig(config_path, insert, update, recycle, delete)

    return reader, master, writer_cfg, monitor_settings, crash_queries


def start_monitor_process(
    monitor_settings: MonitorSettings,
    config_path: Path,
    run_label: Optional[str] = None,
) -> Optional[MonitorProcess]:
    """Launch percona_monitor.py if enabled and return the controller handle."""
    if not monitor_settings.enabled:
        logging.info("Monitor disabled via config; skipping monitor process.")
        return None

    monitor_script = Path(__file__).resolve().parent / "percona_monitor.py"
    if not monitor_script.exists():
        logging.warning("percona_monitor.py not found; monitor disabled.")
        return None

    stop_flag = Path(tempfile.gettempdir()) / f"percona_monitor_stop_{os.getpid()}_{int(time.time())}"
    try:
        if stop_flag.exists():
            stop_flag.unlink()
    except OSError:
        logging.debug("Could not remove existing stop flag %s", stop_flag, exc_info=True)

    safe_run_label: Optional[str] = None
    if run_label:
        safe_run_label = re.sub(r"[^A-Za-z0-9_.-]", "_", run_label).strip("._-")
        if not safe_run_label:
            safe_run_label = None

    cmd = [
        sys.executable,
        str(monitor_script),
        "--config",
        str(config_path),
        "--stop-flag",
        str(stop_flag),
    ]
    if monitor_settings.run_seconds:
        cmd.extend(["--max-runtime", str(monitor_settings.run_seconds)])
    if safe_run_label:
        cmd.extend(["--run-id", safe_run_label])

    logging.info("Starting monitor process: %s", " ".join(cmd))
    try:
        process = subprocess.Popen(cmd)
    except Exception as exc:
        logging.error("Failed to start monitor process: %s", exc, exc_info=True)
        return None

    logging.info("Monitor process started (PID %s)", process.pid)
    return MonitorProcess(process=process, stop_flag=stop_flag)


def apply_reader_overrides(args: argparse.Namespace, overrides: ReaderOverrides) -> None:
    """Copy any values supplied in the `[read]` section onto the parsed arguments."""
    for field in (
        "host",
        "port",
        "user",
        "password",
        "workers",
        "iterations",
        "duration",
        "sleep",
        "explain",
        "log_level",
        "log_file",
        "show_source_info",
        "show_trace",
    ):
        value = getattr(overrides, field)
        if value is not None:
            setattr(args, field, value)


def apply_master_overrides(args: argparse.Namespace, overrides: MasterOverrides) -> None:
    """Override reader credentials with master-specific values when running writers."""
    for field in ("host", "port", "user", "password"):
        value = getattr(overrides, field, None)
        if value is not None:
            setattr(args, field, value)


def apply_reader_defaults(args: argparse.Namespace) -> None:
    """Validate required reader options and coerce numeric fields to the right types."""
    required_fields = (
        "host",
        "port",
        "user",
        "workers",
        "iterations",
        "duration",
        "sleep",
        "log_level",
        "explain",
        "show_source_info",
        "show_trace",
    )
    missing = [field for field in required_fields if getattr(args, field, None) is None]
    if missing:
        logging.error(
            "Missing required settings in config/_v3.toml: %s",
            ", ".join(sorted(missing)),
        )
        raise SystemExit(2)

    if getattr(args, "password", None) is None:
        args.password = ""

    try:
        args.port = int(args.port)
    except (TypeError, ValueError):
        logging.error("Invalid port value in config/_v3.toml: %s", args.port)
        raise SystemExit(2)

    try:
        args.workers = int(args.workers)
        args.iterations = int(args.iterations)
    except (TypeError, ValueError):
        logging.error(
            "Invalid worker/iteration values in config/_v3.toml: workers=%s iterations=%s",
            args.workers,
            args.iterations,
        )
        raise SystemExit(2)

    try:
        args.duration = float(args.duration)
        args.sleep = float(args.sleep)
    except (TypeError, ValueError):
        logging.error(
            "Invalid duration/sleep values in config/_v3.toml: duration=%s sleep=%s",
            args.duration,
            args.sleep,
        )
        raise SystemExit(2)

    args.explain = bool(args.explain)
    args.show_source_info = bool(args.show_source_info)
    args.show_trace = bool(args.show_trace)

    if isinstance(args.log_level, str):
        args.log_level = args.log_level.strip().upper()
    else:
        logging.error("Invalid log_level type in config/_v3.toml: %s", args.log_level)
        raise SystemExit(2)
    if args.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        logging.error("Unsupported log_level in config/_v3.toml: %s", args.log_level)
        raise SystemExit(2)

    log_file = getattr(args, "log_file", None)
    if isinstance(log_file, str):
        args.log_file = log_file.strip()
        if args.log_file == "":
            args.log_file = None
    elif log_file is None:
        args.log_file = None
    else:
        logging.error("Invalid log_file type in config/_v3.toml: %s", log_file)
        raise SystemExit(2)


# Writer worker implementations -------------------------------------------------


INSERT_SQL = (
    "INSERT INTO repro_db.files "
    "(id, tenant_id, user_id, date_create, size, contents, contents_highlight, metadata, "
    "tenants_shared_with, source, title, stored_name, original_name, pub_token, token, md5, "
    "metadata_version, version, highlight_type) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)


def run_insert_writer(
    worker_id: int,
    settings: InsertSettings,
    args: argparse.Namespace,
    id_allocator: IdAllocator,
    inserted_store: InsertedIdStore,
    payload_factory: PayloadFactory,
    tenant_hotspots: Dict[int, TenantHotspot],
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    stats: WriterStats,
    deadline: Optional[float],
) -> None:
    """Producer thread that generates new rows mimicking application insert patterns."""
    label = f"insert-{worker_id:02d}"
    rng = random.Random(time.time() + worker_id * 7919)
    tenant_hotspot_map = tenant_hotspots or {}

    if settings.tenant_id is None and not settings.tenant_ids:
        logging.warning("%s has no tenant IDs configured; skipping", label)
        return

    try:
        conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
        cursor = conn.cursor(dictionary=True) if USING_MYSQL_CONNECTOR else conn.cursor()
    except Exception as exc:  # pragma: no cover - connection failure is fatal
        logging.error("%s unable to open MySQL connection: %s", label, exc)
        with error_lock:
            error_sink.append(("insert", worker_id, exc))
        stop_event.set()
        return

    try:
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break

            ids = id_allocator.next_ids(settings.batch_size)
            if not ids:
                time.sleep(max(settings.sleep, 0.05))
                continue

            rows: List[Tuple[Any, ...]] = []
            applied_ids: List[int] = []
            blob_sizes: List[int] = []
            for row_id in ids:
                tenant_id = _pick_tenant_id_for_insert(settings, rng, tenant_hotspot_map)
                if not tenant_id:
                    logging.debug("%s skipping insert because no tenant_id is available", label)
                    continue

                created_ts = _pick_created_ts(tenant_id, rng, tenant_hotspot_map)
                blob = payload_factory.generate(
                    rng,
                    settings.blob_min_bytes,
                    settings.blob_max_bytes,
                )
                blob_len = len(blob)
                if blob_len == 0:
                    blob = b"0"
                    blob_len = 1
                blob_sizes.append(blob_len)
                highlight_len = min(blob_len, max(1024, max(1, blob_len // 4)))
                highlight = blob[:highlight_len]
                metadata = build_metadata(
                    settings.metadata_prefix,
                    "insert",
                    row_id,
                    tenant_id,
                    blob_len,
                )
                md5_hex = hashlib.md5(blob).hexdigest()
                title = f"{settings.metadata_prefix}_{row_id}"
                stored_name = f"{row_id}.bin"
                token = random_ascii_token(rng, 32)
                pub_token = random_ascii_token(rng, 32)
                rows.append(
                    (
                        row_id,
                        tenant_id,
                        settings.user_id,
                        created_ts,
                        blob_len,
                        blob,
                        highlight,
                        metadata,
                        "[]",
                        settings.source_tag,
                        title,
                        stored_name,
                        stored_name,
                        pub_token,
                        token,
                        md5_hex,
                        1,
                        1,
                        settings.highlight_type,
                    )
                )
                applied_ids.append(row_id)

            if not rows:
                time.sleep(max(settings.sleep, 0.05))
                continue

            try:
                cursor.executemany(INSERT_SQL, rows)
                inserted_store.add(applied_ids)
                stats.record("insert", "success", len(applied_ids))
                if blob_sizes:
                    logging.info("%s inserted blob_bytes=%s", label, ",".join(str(size) for size in blob_sizes))
                logging.debug("%s inserted %d row(s)", label, len(applied_ids))
            except Exception as exc:  # pragma: no cover - relies on server responses
                logging.exception("%s failed to insert batch: %s", label, exc)
                stats.record("insert", "error", 1)
                with error_lock:
                    error_sink.append(("insert", worker_id, exc))
                stop_event.set()
                return

            if settings.sleep:
                time.sleep(settings.sleep)

    finally:
        try:
            cursor.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass
        try:
            conn.close()
        except Exception:
            pass


def run_recycle_writer(
    worker_id: int,
    settings: RecycleSettings,
    args: argparse.Namespace,
    inserted_store: InsertedIdStore,
    payload_factory: PayloadFactory,
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    stats: WriterStats,
    deadline: Optional[float],
) -> None:
    """Rewrite existing rows in place to stress updates and blob churn."""
    label = f"recycle-{worker_id:02d}"
    rng = random.Random(time.time() + worker_id * 6761)

    if not settings.tenant_ids:
        logging.warning("%s has no tenant IDs configured; skipping", label)
        return

    try:
        conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
        if hasattr(conn, "autocommit"):
            try:
                conn.autocommit = False  # type: ignore[attr-defined]
            except Exception:
                pass
        cursor = conn.cursor(dictionary=True) if USING_MYSQL_CONNECTOR else conn.cursor()
    except Exception as exc:
        logging.error("%s unable to open MySQL connection: %s", label, exc)
        with error_lock:
            error_sink.append(("recycle", worker_id, exc))
        stop_event.set()
        return

    target_metadata = max(1, min(settings.metadata_bytes, 60_000))
    target_tenants = max(1, min(settings.tenants_shared_with_bytes, 60_000))

    try:
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break

            ids = fetch_existing_ids(cursor, settings.tenant_ids, settings.batch_size, rng)
            if not ids:
                time.sleep(max(settings.sleep, 0.05))
                continue

            for row_id in ids:
                if stop_event.is_set():
                    break

                try:
                    conn.begin()
                except Exception:
                    pass

                row: Optional[Dict[str, Any]]
                try:
                    cursor.execute(
                        "SELECT id, tenant_id, user_id, date_create, size, contents, contents_highlight, metadata, tenants_shared_with, "
                        "source, title, stored_name, original_name, pub_token, token, md5, metadata_version, version, highlight_type "
                        "FROM repro_db.files WHERE id=%s FOR UPDATE",
                        (row_id,),
                    )
                    row = cursor.fetchone()
                except Exception as exc:
                    logging.debug("%s unable to fetch id=%s: %s", label, row_id, exc)
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue

                if not row:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue

                original_params = (
                    row["id"],
                    row["tenant_id"],
                    row["user_id"],
                    row["date_create"],
                    row.get("size", 0),
                    row.get("contents", b""),
                    row.get("contents_highlight", b""),
                    row.get("metadata", "") or "",
                    row.get("tenants_shared_with", "") or "[]",
                    row.get("source") or settings.source_tag,
                    row.get("title") or f"{settings.metadata_prefix}_{row_id}",
                    row.get("stored_name") or f"{row_id}.bin",
                    row.get("original_name") or f"{row_id}.bin",
                    row.get("pub_token") or random_ascii_token(rng, 32),
                    row.get("token") or random_ascii_token(rng, 32),
                    row.get("md5") or hashlib.md5(row.get("contents", b"") or b"").hexdigest(),
                    int(row.get("metadata_version", 0) or 0),
                    int(row.get("version", 0) or 0),
                    row.get("highlight_type") or settings.highlight_type,
                )

                try:
                    new_blob = payload_factory.generate(
                        rng,
                        settings.blob_min_bytes,
                        settings.blob_max_bytes,
                    )
                    if not new_blob:
                        new_blob = os.urandom(settings.blob_min_bytes or 1)
                    blob_len = len(new_blob)
                    highlight_len = min(blob_len, max(2048, blob_len // 4))
                    new_highlight = new_blob[:highlight_len]

                    metadata = build_metadata(
                        settings.metadata_prefix,
                        "recycle",
                        row_id,
                        int(row["tenant_id"]),
                        blob_len,
                    )
                    metadata = _build_large_text(metadata, target_metadata, rng)
                    tenants_shared_with = _build_large_tenant_list(target_tenants, rng)

                    source_tag = row.get("source") or settings.source_tag
                    title = row.get("title") or f"{settings.metadata_prefix}_{row_id}"
                    original_name = row.get("original_name") or f"{row_id}.bin"
                    stored_name = row.get("stored_name") or f"{row_id}.bin"
                    pub_token = random_ascii_token(rng, 48)
                    token = random_ascii_token(rng, 48)
                    md5_hex = hashlib.md5(new_blob).hexdigest()
                    metadata_version = int(row.get("metadata_version", 0) or 0) + 1
                    version = int(row.get("version", 0) or 0) + 1
                    highlight_type = row.get("highlight_type") or settings.highlight_type

                    cursor.execute("DELETE FROM repro_db.files WHERE id=%s", (row_id,))
                    cursor.execute(
                        INSERT_SQL,
                        (
                            row_id,
                            row["tenant_id"],
                            row["user_id"],
                            row["date_create"],
                            blob_len,
                            new_blob,
                            new_highlight,
                            metadata,
                            tenants_shared_with,
                            source_tag,
                            title[:255],
                            stored_name[:255],
                            original_name[:255],
                            pub_token,
                            token,
                            md5_hex,
                            metadata_version,
                            version,
                            highlight_type,
                        ),
                    )
                    conn.commit()
                    inserted_store.add([row_id])
                    stats.record("recycle", "success", 1)
                    logging.info(
                        "%s recycled id=%s blob_bytes=%d metadata_bytes=%d tenants_bytes=%d",
                        label,
                        row_id,
                        blob_len,
                        len(metadata.encode("utf-8")),
                        len(tenants_shared_with.encode("utf-8")),
                    )
                except Exception as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    logging.exception("%s failed recycling id=%s: %s", label, row_id, exc)
                    try:
                        conn.begin()
                        cursor.execute(INSERT_SQL, original_params)
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logging.exception("%s unable to restore original row id=%s", label, row_id)
                    stats.record("recycle", "error", 1)
                    with error_lock:
                        error_sink.append(("recycle", worker_id, exc))
                    stop_event.set()
                    return

            if settings.sleep:
                time.sleep(settings.sleep)

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def build_update_statement(columns: List[str]) -> Tuple[str, List[str]]:
    """Construct the UPDATE statement and value order based on enabled columns."""
    column_set = {col.lower() for col in columns}
    assignments: List[str] = []
    param_order: List[str] = []

    if "contents" in column_set:
        assignments.append("contents=%s")
        param_order.append("contents")
        assignments.append("size=%s")
        param_order.append("size")
        assignments.append("md5=%s")
        param_order.append("md5")

    if "contents_highlight" in column_set or "contents" in column_set:
        assignments.append("contents_highlight=%s")
        param_order.append("contents_highlight")

    if "metadata" in column_set:
        assignments.append("metadata=%s")
        param_order.append("metadata")

    if "metadata_enc" in column_set:
        assignments.append("metadata_enc=%s")
        param_order.append("metadata_enc")

    if "contents_enc" in column_set:
        assignments.append("contents_enc=%s")
        param_order.append("contents_enc")

    if "contents_highlight_enc" in column_set:
        assignments.append("contents_highlight_enc=%s")
        param_order.append("contents_highlight_enc")

    if "external_ptr" in column_set:
        assignments.append("external_ptr=%s")
        param_order.append("external_ptr")

    if "unencrypted_metadata" in column_set:
        assignments.append("unencrypted_metadata=%s")
        param_order.append("unencrypted_metadata")

    assignments.append("metadata_version = metadata_version + 1")
    assignments.append("version = version + 1")
    assignments.append("highlight_type=%s")
    param_order.append("highlight_type")

    sql = "UPDATE repro_db.files SET " + ", ".join(assignments) + " WHERE id=%s"
    return sql, param_order


def run_update_writer(
    worker_id: int,
    settings: UpdateSettings,
    args: argparse.Namespace,
    inserted_store: InsertedIdStore,
    payload_factory: PayloadFactory,
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    stats: WriterStats,
    deadline: Optional[float],
) -> None:
    """Issue UPDATEs over recently inserted ids to mimic application edits."""
    label = f"update-{worker_id:02d}"
    rng = random.Random(time.time() + worker_id * 6701)
    column_set = {col.lower() for col in settings.columns}

    try:
        conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
        cursor = conn.cursor(dictionary=True) if USING_MYSQL_CONNECTOR else conn.cursor()
    except Exception as exc:
        logging.error("%s unable to open MySQL connection: %s", label, exc)
        stats.record("update", "error", 1)
        with error_lock:
            error_sink.append(("update", worker_id, exc))
        stop_event.set()
        return

    update_sql, param_order = build_update_statement(settings.columns)

    try:
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break

            ids = inserted_store.sample(rng, settings.batch_size)
            if len(ids) < settings.batch_size:
                ids.extend(
                    fetch_existing_ids(
                        cursor,
                        settings.tenant_ids,
                        settings.batch_size - len(ids),
                        rng,
                    )
                )

            if not ids:
                time.sleep(max(settings.sleep, 0.05))
                continue

            affected = 0
            update_blob_sizes: List[int] = []
            for row_id in ids:
                prefer_pattern = rng.random() < settings.reuse_fraction
                blob = payload_factory.generate(
                    rng,
                    settings.blob_min_bytes,
                    settings.blob_max_bytes,
                    prefer_pattern=prefer_pattern,
                )
                if not blob:
                    blob = b"1"
                blob_len = len(blob)
                update_blob_sizes.append(blob_len)
                highlight_len = min(blob_len, max(1024, max(1, blob_len // 4)))
                highlight = blob[:highlight_len]
                metadata = build_metadata(
                    settings.highlight_type,
                    "update",
                    row_id,
                    settings.tenant_ids[0] if settings.tenant_ids else 0,
                    blob_len,
                )
                md5_hex = hashlib.md5(blob).hexdigest()
                param_values = {
                    "contents": blob,
                    "size": blob_len,
                    "md5": md5_hex,
                    "contents_highlight": highlight,
                    "metadata": metadata,
                    "highlight_type": settings.highlight_type,
                }

                if "metadata_enc" in column_set:
                    param_values["metadata_enc"] = payload_factory.generate(
                        rng, max(512, settings.blob_min_bytes // 4), max(4096, settings.blob_max_bytes // 2)
                    )
                if "contents_enc" in column_set:
                    param_values["contents_enc"] = payload_factory.generate(
                        rng, settings.blob_min_bytes, settings.blob_max_bytes
                    )
                if "contents_highlight_enc" in column_set:
                    enc_blob = param_values.get("contents_enc")
                    if enc_blob is None:
                        enc_blob = payload_factory.generate(
                            rng, settings.blob_min_bytes, settings.blob_max_bytes
                        )
                    param_values["contents_highlight_enc"] = enc_blob[:highlight_len]
                if "external_ptr" in column_set:
                    param_values["external_ptr"] = payload_factory.generate(
                        rng, 512, max(2048, settings.blob_min_bytes)
                    )
                if "unencrypted_metadata" in column_set:
                    param_values["unencrypted_metadata"] = build_metadata(
                        settings.highlight_type,
                        "update_plain",
                        row_id,
                        settings.tenant_ids[0] if settings.tenant_ids else 0,
                        blob_len,
                    )

                values = [param_values[name] for name in param_order]
                values.append(int(row_id))
                try:
                    cursor.execute(update_sql, tuple(values))
                    affected += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 1
                except Exception as exc:  # pragma: no cover
                    logging.exception("%s failed updating id=%s", label, row_id)
                    stats.record("update", "error", 1)
                    with error_lock:
                        error_sink.append(("update", worker_id, exc))
                    stop_event.set()
                    return

            if affected:
                stats.record("update", "success", affected)
                if update_blob_sizes:
                    logging.info(
                        "%s updated blob_bytes=%s",
                        label,
                        ",".join(str(size) for size in update_blob_sizes),
                    )
                logging.debug("%s updated %d row(s)", label, affected)

            if settings.sleep:
                time.sleep(settings.sleep)

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def run_delete_writer(
    worker_id: int,
    settings: DeleteSettings,
    args: argparse.Namespace,
    inserted_store: InsertedIdStore,
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    stats: WriterStats,
    deadline: Optional[float],
) -> None:
    """Soft or hard delete ids the harness previously inserted."""
    label = f"delete-{worker_id:02d}"
    rng = random.Random(time.time() + worker_id * 5881)

    try:
        conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
        cursor = conn.cursor(dictionary=True) if USING_MYSQL_CONNECTOR else conn.cursor()
    except Exception as exc:
        logging.error("%s unable to open MySQL connection: %s", label, exc)
        stats.record("delete", "error", 1)
        with error_lock:
            error_sink.append(("delete", worker_id, exc))
        stop_event.set()
        return

    try:
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break

            ids = inserted_store.pop(rng, settings.batch_size)
            if settings.target == "any" and len(ids) < settings.batch_size:
                ids.extend(
                    fetch_existing_ids(
                        cursor,
                        settings.tenant_ids,
                        settings.batch_size - len(ids),
                        rng,
                    )
                )

            if not ids:
                time.sleep(max(settings.sleep, 0.1))
                continue

            placeholders = ", ".join(["%s"] * len(ids))
            try:
                if settings.mode == "hard":
                    cursor.execute(
                        f"DELETE FROM repro_db.files WHERE id IN ({placeholders})",
                        tuple(int(x) for x in ids),
                    )
                else:
                    ts = int(time.time())
                    cursor.execute(
                        f"UPDATE repro_db.files SET is_deleted=1, date_delete=%s WHERE id IN ({placeholders})",
                        (ts, *[int(x) for x in ids]),
                    )
                stats.record("delete", "success", len(ids))
                logging.debug("%s %s %d row(s)", label, settings.mode, len(ids))
            except Exception as exc:  # pragma: no cover
                logging.exception("%s failed deleting ids=%s", label, ids)
                stats.record("delete", "error", 1)
                with error_lock:
                    error_sink.append(("delete", worker_id, exc))
                stop_event.set()
                return

            if settings.sleep:
                time.sleep(settings.sleep)

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass



def _extract_error_code(exc: Exception) -> Optional[int]:
    """Best-effort helper to pull a numeric error code off a DB exception."""
    if hasattr(exc, "errno"):
        code = getattr(exc, "errno")
        if isinstance(code, int):
            return code
    if hasattr(exc, "args") and exc.args:
        first = exc.args[0]
        if isinstance(first, int):
            return first
    return None


def _is_query_timeout_error(exc: Exception) -> bool:
    """Return True when the exception corresponds to MAX_EXECUTION_TIME timeout."""
    return _extract_error_code(exc) == 3024


def fetch_dataset_totals(cursor: Any) -> Dict[str, int]:
    """Return quick aggregate stats for repro_db.files without scanning the table."""

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
    """Return the frozen workload slices captured from production crash bundles."""

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


@dataclass(frozen=True)
class TenantHotspot:
    tenant_id: int
    date_min: Optional[int]
    date_max: Optional[int]
    id_min: Optional[int]
    id_max: Optional[int]


def build_tenant_hotspots(
    dataset: DatasetContext,
    preferred_tenant_ids: Sequence[int],
) -> Dict[int, TenantHotspot]:
    """Describe per-tenant hotspots so insert threads choose realistic ids and dates."""
    """Construct per-tenant hotspots so inserts can target historical index ranges."""

    sequential = {
        int(entry["tenant_id"]): entry for entry in dataset.fixtures.get("sequential_scan", [])
    }
    recent = {
        int(entry["tenant_id"]): entry for entry in dataset.fixtures.get("recent_window", [])
    }
    tenants: List[int]
    if preferred_tenant_ids:
        tenants = list(dict.fromkeys(int(tid) for tid in preferred_tenant_ids))
    else:
        tenants = sorted(set(sequential) | set(recent))

    hotspots: Dict[int, TenantHotspot] = {}
    min_id_default = dataset.totals.get("min_id", 0)
    max_id_default = dataset.totals.get("max_id", 0)

    for tenant_id in tenants:
        seq_entry = sequential.get(tenant_id)
        recent_entry = recent.get(tenant_id)
        id_min = int(seq_entry["id_min"]) if seq_entry and "id_min" in seq_entry else min_id_default
        id_max = int(seq_entry["id_max"]) if seq_entry and "id_max" in seq_entry else max_id_default
        date_min = (
            int(recent_entry["date_min"]) if recent_entry and "date_min" in recent_entry else None
        )
        date_max = (
            int(recent_entry["date_max"]) if recent_entry and "date_max" in recent_entry else None
        )
        hotspots[tenant_id] = TenantHotspot(
            tenant_id=tenant_id,
            date_min=date_min,
            date_max=date_max,
            id_min=id_min,
            id_max=id_max,
        )

    return hotspots


def _pick_tenant_id_for_insert(
    settings: InsertSettings,
    rng: random.Random,
    tenant_hotspots: Dict[int, TenantHotspot],
) -> int:
    """Pick the tenant id used for a fresh insert, honoring overrides and hotspots."""
    if settings.tenant_id is not None:
        return int(settings.tenant_id)

    hotspot_tenant_ids = list(tenant_hotspots.keys())
    if settings.tenant_ids:
        pool = [int(tid) for tid in settings.tenant_ids if not hotspot_tenant_ids or tid in tenant_hotspots]
        if pool:
            return int(rng.choice(pool))
    if hotspot_tenant_ids:
        return int(rng.choice(hotspot_tenant_ids))
    if settings.tenant_ids:
        return int(rng.choice(settings.tenant_ids))
    return 0


def _pick_created_ts(tenant_id: int, rng: random.Random, tenant_hotspots: Dict[int, TenantHotspot]) -> int:
    """Choose a created timestamp for an insert, biased by the tenant's hotspot window."""
    hotspot = tenant_hotspots.get(int(tenant_id))
    if hotspot and hotspot.date_min and hotspot.date_max and hotspot.date_max > hotspot.date_min:
        high = hotspot.date_max
        span = hotspot.date_max - hotspot.date_min
        window = min(span, HOTSPOT_DATE_WINDOW_SECONDS)
        low = max(hotspot.date_min, high - window)
        if low < high:
            return rng.randint(low, high)
    now = int(time.time())
    jitter = rng.randint(0, HOTSPOT_DATE_WINDOW_SECONDS)
    return max(0, now - jitter)


def _build_large_text(base: str, target_bytes: int, rng: random.Random) -> str:
    """Pad a base string until it reaches the requested size in bytes."""
    if target_bytes <= 0:
        return base
    encoded = base.encode("utf-8")
    if len(encoded) >= target_bytes:
        return base
    pad_len = target_bytes - len(encoded)
    chunk = ''.join(rng.choice(string.ascii_letters + string.digits) for _ in range(32))
    repeats = (pad_len // len(chunk)) + 2
    filler = (chunk * repeats)[:pad_len]
    return base + filler


def _build_large_tenant_list(target_bytes: int, rng: random.Random) -> str:
    """Synthesize a JSON array of tenant ids roughly target_bytes long."""
    if target_bytes <= 0:
        return "[]"
    entries: List[str] = []
    while True:
        entry = f"\"tenant-{rng.randint(1, 9_999_999_999)}\""
        entries.append(entry)
        candidate = "[" + ",".join(entries) + "]"
        if len(candidate.encode("utf-8")) >= target_bytes:
            return candidate


def build_query_params(
    param_key: str,
    dataset: DatasetContext,
    slice_index: int,
    *,
    rng: Optional[random.Random] = None,
) -> Dict[str, Any]:
    """Translate a workload slice entry into concrete SQL parameters."""
    rng_obj = rng or random

    if param_key in CATALOG_SEQUENTIAL_PARAMS:
        entry = CATALOG_SEQUENTIAL_PARAMS[param_key]
        return {
            "tenant_id": entry["tenant_id"],
            "id_floor_small": entry["id_floor"],
        }

    if param_key in CATALOG_CLEANUP_PARAMS:
        entry = CATALOG_CLEANUP_PARAMS[param_key]
        return {
            "tenant_id": entry["tenant_id"],
            "highlight_types": HIGHLIGHT_FILTER,
            "date_cutoff": entry["date_cutoff"],
            "id_floor": entry["id_floor"],
        }

    if param_key in CATALOG_BACKFILL_PARAMS:
        entry = CATALOG_BACKFILL_PARAMS[param_key]
        return {
            "tenant_id": entry["tenant_id"],
            "id_floor_small": entry["id_floor"],
        }

    slices = dataset.fixtures.get(param_key)
    if not slices:
        raise ValueError(f"Unknown param key {param_key}")

    fixture = slices[slice_index % len(slices)]

    if param_key == "recent_window":
        latest = fixture["date_max"]
        earliest = max(fixture["date_min"], RECENT_WINDOW_DATE_THRESHOLD)
        row_count = max(1, fixture.get("row_count", 1))
        date_span = max(0, latest - earliest)
        avg_gap = max(1, date_span // row_count) if date_span else 1
        candidate_windows = [RECENT_WINDOW_SECONDS, max(1, avg_gap * RECENT_TARGET_ROWS)]
        if date_span:
            candidate_windows.append(date_span)
        window = max(1, min(candidate_windows))
        window_floor = max(earliest, latest - window)
        if latest <= window_floor:
            date_threshold = latest
        else:
            date_threshold = rng_obj.randint(window_floor, latest)
        return {
            "tenant_id": fixture["tenant_id"],
            "date_threshold": date_threshold,
        }

    if param_key == "sequential_scan":
        id_min = fixture["id_min"]
        id_max = fixture["id_max"]
        row_count = max(1, fixture.get("row_count", 1))
        span = max(0, id_max - id_min)
        avg_gap = max(1, span // max(1, row_count - 1)) if span else 1
        target_rows = min(row_count, SEQUENTIAL_TARGET_ROWS)
        max_offset_rows = max(0, row_count - target_rows)
        if max_offset_rows:
            tail_guard = max(1, target_rows // 4)
            safe_limit = max(0, max_offset_rows - tail_guard)
            offset_rows = rng_obj.randint(0, safe_limit or max_offset_rows)
        else:
            offset_rows = 0
        id_floor_val = id_min + offset_rows * avg_gap
        if id_floor_val >= id_max:
            id_floor_val = max(id_min, id_max - avg_gap)
        return {
            "tenant_id": fixture["tenant_id"],
            "id_floor_small": id_floor_val,
        }

    if param_key == "cleanup_batch":
        id_min = fixture["id_min"]
        id_max = fixture["id_max"]
        row_count = max(1, fixture.get("row_count", 1))
        span = max(0, id_max - id_min)
        avg_gap = max(1, span // max(1, row_count - 1)) if span else 1
        target_rows = min(row_count, CLEANUP_TARGET_ROWS)
        max_offset_rows = max(0, row_count - target_rows)
        if max_offset_rows:
            tail_guard = max(1, target_rows // 4)
            safe_limit = max(0, max_offset_rows - tail_guard)
            offset_rows = rng_obj.randint(0, safe_limit or max_offset_rows)
        else:
            offset_rows = 0
        id_floor_val = id_min + offset_rows * avg_gap
        if id_floor_val >= id_max:
            id_floor_val = max(id_min, id_max - avg_gap)

        delete_min = fixture.get("date_delete_min", 1)
        delete_max = min(fixture.get("date_delete_max", CLEANUP_DATE_CUTOFF_MAX), CLEANUP_DATE_CUTOFF_MAX)
        if delete_max < delete_min:
            delete_min, delete_max = delete_max, delete_min
        date_span = max(0, delete_max - delete_min)
        date_avg = max(1, date_span // row_count) if date_span else 1
        candidate_date_windows = [CLEANUP_DATE_WINDOW, max(1, date_avg * CLEANUP_TARGET_ROWS)]
        if date_span:
            candidate_date_windows.append(date_span)
        date_window = max(1, min(candidate_date_windows))
        window_floor = max(delete_min, delete_max - date_window)
        if delete_max <= window_floor:
            date_cutoff = delete_max
        else:
            date_cutoff = rng_obj.randint(window_floor, delete_max)

        return {
            "tenant_id": fixture["tenant_id"],
            "highlight_types": HIGHLIGHT_FILTER,
            "date_cutoff": date_cutoff,
            "id_floor": id_floor_val,
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
) -> Tuple[int, float]:
    """Execute one workload query and return (rows fetched, elapsed seconds)."""
    def execute_single_sql():
        start_ts = time.time()
        trace_snippet = None
        try:
            if explain:
                cursor.execute(f"EXPLAIN {sql}")
                cursor.fetchall()
            cursor.execute(sql)

            if query.requires_result:
                rows_out = len(cursor.fetchall())
            else:
                rows_out = cursor.rowcount if cursor.rowcount != -1 else 0
                cursor.fetchall()
        except Exception as exc:
            if _is_query_timeout_error(exc):
                elapsed_time = time.time() - start_ts
                logging.warning(
                    "Query %s exceeded %d ms and was cancelled; continuing",
                    query.name,
                    PER_QUERY_TIMEOUT_MS,
                )
                return 0, elapsed_time, None
            raise

        elapsed_time = time.time() - start_ts

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

    rows, elapsed, trace_snippet = execute_single_sql()

    if trace_snippet is not None:
        logging.debug("TRACE %s: %s...", query.name, trace_snippet.replace("\n", ' ')[:200])

    return rows, elapsed


def run_worker(
    worker_id: int,
    iterations: int,
    args: argparse.Namespace,
    dataset: DatasetContext,
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    coverage_counts: Dict[str, int],
    coverage_lock: threading.Lock,
    deadline: Optional[float],
) -> None:
    """Main reader thread loop pulling slices and tracking coverage stats."""
    rng = random.Random(time.time() + worker_id)
    conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
    try:
        if USING_MYSQL_CONNECTOR:
            cursor = conn.cursor(dictionary=True)  # type: ignore[union-attr]
        else:
            cursor = conn.cursor()
        cursor.execute("SET optimizer_trace='enabled=on', optimizer_trace_max_mem_size=262144")
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {PER_QUERY_TIMEOUT_MS}")

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
        stack_collector = None
        logging.info(
            "%s starting (target_iterations=%s, stacks=%s)",
            label_prefix,
            "unlimited" if max_iterations is None else max_iterations,
            "on" if stack_collector else "off",
        )
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
                    rows, elapsed = run_query(
                        conn,
                        cursor,
                        workload,
                        sql,
                        iteration,
                        explain=args.explain,
                        show_trace=args.show_trace,
                    )
                    logging.info(
                        "[%s][%03d] %s rows=%s elapsed=%.3fs",
                        label_prefix,
                        iteration,
                        workload.name,
                        rows,
                        elapsed,
                    )
                    local_counts[workload.name] += 1
                except Exception as exc:  # pragma: no cover - diagnostic path
                    with error_lock:
                        error_sink.append(("reader", worker_id, exc))
                    stop_event.set()
                    logging.exception("Worker %02d query failed", worker_id)
                    record_counts()
                    return
            if args.sleep:
                time.sleep(args.sleep)
    except Exception as exc:  # pragma: no cover
        with error_lock:
            error_sink.append(("reader", worker_id, exc))
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


def run_crash_query(
    global_id: int,
    per_query_idx: int,
    query_settings: CrashQuerySettings,
    args: argparse.Namespace,
    stop_event: threading.Event,
    error_sink: List[Tuple[str, int, Exception]],
    error_lock: threading.Lock,
    deadline: Optional[float],
) -> None:
    """Continuously execute a crash-specific SELECT, logging each batch of rows returned."""
    label = f"crash-{query_settings.name}-{per_query_idx:02d}"
    try:
        conn = open_connection(host=args.host, port=args.port, user=args.user, password=args.password)
        cursor = conn.cursor(dictionary=True) if USING_MYSQL_CONNECTOR else conn.cursor()
        try:
            cursor.execute("USE repro_db")
        except Exception:
            pass
    except Exception as exc:
        logging.error("%s unable to open MySQL connection: %s", label, exc)
        with error_lock:
            error_sink.append(("crash", global_id, exc))
        stop_event.set()
        return

    iteration = 0
    try:
        while not stop_event.is_set():
            if deadline and time.time() >= deadline:
                break
            iteration += 1
            try:
                cursor.execute(query_settings.sql)
                rows = cursor.fetchall()
                logging.info("[%s][%03d] crash query returned %d row(s)", label, iteration, len(rows))
            except Exception as exc:
                logging.exception("%s encountered an error executing crash query", label)
                with error_lock:
                    error_sink.append(("crash", global_id, exc))
                stop_event.set()
                return
            if query_settings.sleep:
                time.sleep(query_settings.sleep)
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Build the CLI parser for the harness entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "config" / "_v3.toml"),
        help="Path to TOML config controlling writer workloads (default: config/_v3.toml)",
    )
    parser.add_argument(
        "--mode",
        choices=MODE_CHOICES,
        default="dual",
        help=(
            "Run mode: 'dual' (default) runs readers and writers together, "
            "'writer' runs only the DML workloads, and 'reader' replays the SELECT workload only."
        ),
    )
    return parser.parse_args(argv)


def configure_logging(args: argparse.Namespace) -> Optional[Path]:
    """Set up logging to stdout and, when requested, tee the stream to a log file."""

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
        mode_value = getattr(args, "mode", "dual")
        mode_part = f"_m{mode_value}"
        log_path = logs_dir / f"run_{timestamp}_w{args.workers}_i{args.iterations}{duration_part}{mode_part}.log"

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


def _run_harness(
    args: argparse.Namespace,
    writer_config: WriterConfig,
    crash_queries: List[CrashQuerySettings],
    config_path: Path,
    log_path: Optional[Path],
) -> int:
    """Execute reader/writer workloads and any crash-specific queries until duration expires."""
    if log_path:
        logging.info("Logging to %s", log_path)

    mode = getattr(args, "mode", "dual")
    reader_enabled = mode in {"dual", "reader"}
    writer_enabled = mode in {"dual", "writer"}
    logging.info(
        "Run mode: %s (readers=%s, writers=%s)",
        mode,
        "enabled" if reader_enabled else "disabled",
        "enabled" if writer_enabled else "disabled",
    )

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

    if reader_enabled and args.workers < 1:
        logging.error("workers must be >= 1 (got %s)", args.workers)
        return 1

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
    if reader_enabled:
        iteration_label = "unlimited" if args.iterations == 0 else str(args.iterations)
        logging.info(
            "Launching %d reader worker(s) × %s iterations",
            args.workers,
            iteration_label,
        )
    else:
        logging.info("Reader workload disabled for this run.")

    writer_stats = WriterStats()
    inserted_store = InsertedIdStore()
    next_id_seed = int(dataset.totals.get("max_id", 0) or 0) + 1
    if writer_enabled:
        state_max = load_high_watermark(STATE_FILE)
        if state_max is not None:
            candidate_seed = max(int(state_max), 0) + 1
            if candidate_seed > next_id_seed:
                logging.info(
                    "Seeding insert IDs from high-watermark file %s (start id=%s)",
                    STATE_FILE,
                    candidate_seed,
                )
                next_id_seed = candidate_seed
            else:
                logging.info(
                    "High-watermark file %s reported %s (<= live max); retaining live seed %s",
                    STATE_FILE,
                    state_max,
                    next_id_seed,
                )
    id_allocator = IdAllocator(next_id_seed)
    if writer_enabled:
        logging.info("Insert ID allocator initial seed: %s", next_id_seed)

    if writer_enabled:
        logging.info(
            "Writer profile: insert=%s update=%s recycle=%s delete=%s (config=%s)",
            f"{writer_config.insert.threads} thread(s)" if writer_config.insert.enabled and writer_config.insert.threads else "disabled",
            f"{writer_config.update.threads} thread(s)" if writer_config.update.enabled and writer_config.update.threads else "disabled",
            f"{writer_config.recycle.threads} thread(s)" if writer_config.recycle.enabled and writer_config.recycle.threads else "disabled",
            f"{writer_config.delete.threads} thread(s)" if writer_config.delete.enabled and writer_config.delete.threads else "disabled",
            config_path,
        )
    else:
        logging.info("Writer workloads disabled for this run (config=%s)", config_path)

    stop_event = threading.Event()
    error_sink: List[Tuple[str, int, Exception]] = []
    error_lock = threading.Lock()
    coverage_lock = threading.Lock()
    coverage_counts: Dict[str, int] = {name: 0 for name in WORKLOAD_NAMES}
    reader_threads: List[threading.Thread] = []
    crash_threads: List[threading.Thread] = []
    writer_threads: List[threading.Thread] = []
    start_time = time.time()
    forced_stop = False

    deadline = time.time() + args.duration if args.duration else None

    if reader_enabled:
        crash_thread_counter = 0
        for worker_id in range(1, args.workers + 1):
            thread = threading.Thread(
                target=run_worker,
                args=(
                    worker_id,
                    args.iterations,
                    args,
                    dataset,
                    stop_event,
                    error_sink,
                    error_lock,
                    coverage_counts,
                    coverage_lock,
                    deadline,
                ),
                name=f"worker-{worker_id:02d}",
            )
            reader_threads.append(thread)
            thread.start()

        for query in crash_queries:
            if not query.enabled or query.threads <= 0:
                continue
            sanitized_name = query.name.replace(" ", "_")
            logging.info(
                "Starting crash query workload %s with %d thread(s)",
                query.name,
                query.threads,
            )
            for local_idx in range(1, query.threads + 1):
                crash_thread_counter += 1
                thread = threading.Thread(
                    target=run_crash_query,
                    args=(
                        crash_thread_counter,
                        local_idx,
                        query,
                        args,
                        stop_event,
                        error_sink,
                        error_lock,
                        deadline,
                    ),
                    name=f"crash-{sanitized_name}-{local_idx:02d}",
                )
                crash_threads.append(thread)
                thread.start()

    if writer_enabled and writer_config.insert.enabled and writer_config.insert.threads > 0:
        insert_factory = PayloadFactory(
            writer_config.insert.payload_mode,
            writer_config.insert.payload_path,
        )
        tenant_hotspots = build_tenant_hotspots(dataset, writer_config.insert.tenant_ids)
        for writer_id in range(1, writer_config.insert.threads + 1):
            thread = threading.Thread(
                target=run_insert_writer,
                args=(
                    writer_id,
                    writer_config.insert,
                    args,
                    id_allocator,
                    inserted_store,
                    insert_factory,
                    tenant_hotspots,
                    stop_event,
                    error_sink,
                    error_lock,
                    writer_stats,
                    deadline,
                ),
                name=f"writer-insert-{writer_id:02d}",
            )
            writer_threads.append(thread)
            thread.start()

    if writer_enabled and writer_config.update.enabled and writer_config.update.threads > 0:
        update_factory = PayloadFactory(
            writer_config.update.payload_mode,
            writer_config.update.payload_path,
        )
        for writer_id in range(1, writer_config.update.threads + 1):
            thread = threading.Thread(
                target=run_update_writer,
                args=(
                    writer_id,
                    writer_config.update,
                    args,
                    inserted_store,
                    update_factory,
                    stop_event,
                    error_sink,
                    error_lock,
                    writer_stats,
                    deadline,
                ),
                name=f"writer-update-{writer_id:02d}",
            )
            writer_threads.append(thread)
            thread.start()

    if writer_enabled and writer_config.recycle.enabled and writer_config.recycle.threads > 0:
        recycle_factory = PayloadFactory(
            writer_config.recycle.payload_mode,
            writer_config.recycle.payload_path,
        )
        for writer_id in range(1, writer_config.recycle.threads + 1):
            thread = threading.Thread(
                target=run_recycle_writer,
                args=(
                    writer_id,
                    writer_config.recycle,
                    args,
                    inserted_store,
                    recycle_factory,
                    stop_event,
                    error_sink,
                    error_lock,
                    writer_stats,
                    deadline,
                ),
                name=f"writer-recycle-{writer_id:02d}",
            )
            writer_threads.append(thread)
            thread.start()

    if writer_enabled and writer_config.delete.enabled and writer_config.delete.threads > 0:
        for writer_id in range(1, writer_config.delete.threads + 1):
            thread = threading.Thread(
                target=run_delete_writer,
                args=(
                    writer_id,
                    writer_config.delete,
                    args,
                    inserted_store,
                    stop_event,
                    error_sink,
                    error_lock,
                    writer_stats,
                    deadline,
                ),
                name=f"writer-delete-{writer_id:02d}",
            )
            writer_threads.append(thread)
            thread.start()

    for thread in reader_threads:
        thread.join()

    if reader_enabled and not deadline:
        stop_event.set()
        forced_stop = True

    for thread in crash_threads:
        thread.join()

    for thread in writer_threads:
        thread.join()

    if error_sink:
        worker_type, worker_id, exc = error_sink[0]
        logging.error("%s worker %02d encountered an error: %s", worker_type, worker_id, exc)
        return 1

    if stop_event.is_set() and not forced_stop and not error_sink:
        logging.warning("Termination requested; workload ended early")
        return 1

    elapsed = time.time() - start_time
    if deadline:
        logging.info("Elapsed %.1fs against target %.1fs", elapsed, args.duration)
    else:
        logging.info("Elapsed %.1fs", elapsed)

    if reader_enabled:
        logging.info(
            "Reader workload finished normally (%d worker(s))",
            args.workers,
        )
        logging.info("Workload coverage summary:")
        for name in WORKLOAD_NAMES:
            logging.info("  %s: %d", name, coverage_counts.get(name, 0))
    else:
        logging.info("Reader workload skipped; no coverage summary generated.")

    writer_snapshot = writer_stats.snapshot()
    if writer_enabled and writer_snapshot:
        logging.info("Writer coverage summary:")
        grouped: Dict[str, Counter[str]] = {}
        for (op, outcome), value in writer_snapshot.items():
            grouped.setdefault(op, Counter())[outcome] = value
        for op in sorted(grouped):
            success = grouped[op].get("success", 0)
            error_total = grouped[op].get("error", 0)
            logging.info("  %s: success=%d error=%d", op, success, error_total)
    elif not writer_enabled:
        logging.info("Writer workload skipped; no DML statistics collected.")

    if log_path:
        logging.info("Detailed log saved at %s", log_path)
    return 0



def main(argv: Optional[Sequence[str]] = None) -> int:
    """Top-level harness entry point used by crash_reproduction and direct CLI runs."""
    args = parse_args(argv)
    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        config_path = (Path(__file__).resolve().parent / config_path).resolve()

    try:
        (
            reader_overrides,
            master_overrides,
            writer_config,
            monitor_settings,
            crash_query_settings,
        ) = load_harness_config(config_path)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    apply_reader_overrides(args, reader_overrides)
    if args.mode == "writer":
        apply_master_overrides(args, master_overrides)
    apply_reader_defaults(args)
    log_path = configure_logging(args)

    monitor_process: Optional["MonitorProcess"] = None
    try:
        run_label: Optional[str] = None
        if log_path:
            run_label = log_path.stem
        if monitor_settings.enabled:
            monitor_process = start_monitor_process(monitor_settings, config_path, run_label)
        else:
            logging.info("Monitor disabled via config; skipping metrics collection.")
        return _run_harness(args, writer_config, crash_query_settings, config_path, log_path)
    finally:
        if monitor_process:
            monitor_process.stop()


if __name__ == "__main__":
    sys.exit(main())
