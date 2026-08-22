#!/usr/bin/env python3
import argparse
import sys
import datetime as dt
import os
import pathlib
import subprocess
import threading
import time
import re
import platform
import json
import shutil
import zipfile
import socket
from shlex import quote
import shlex
import csv
import copy
import shutil
import uuid

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_SCENARIOS = {
    "default_ibuf_3": REPO_ROOT / "configs" / "default_ibuf_3.json",
}

DEFAULTS = dict(
    host="127.0.0.1",
    port=34512,
    user="sbtest",
    password="sbtest",
    database="sbtest",
    tables=8,
    table_size=10000000,
    threads=32,
    duration=28800,
    report_interval=10,
    status_interval=10,
    metrics_interval=10,
    out_dir="/nvme/matias.sanchez/sandboxes/stats/results",
)

def _sanitize_name(name: str) -> str:
    name = name.strip()
    has_bad = re.search(r"[^A-Za-z0-9._\- ]", name)
    name = re.sub(r"[^A-Za-z0-9._\- ]", "", name)
    if has_bad:
        name = name.replace(" ", "")
    else:
        name = name.replace(" ", "_")
    if not name:
        name = dt.datetime.now().strftime("test_%Y%m%d_%H%M%S")
    return name

def timestamp():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def which(cmd):
    try:
        subprocess.run(["which", cmd], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False

def run_cmd(cmd, **popen_kwargs):
    return subprocess.run(cmd, **popen_kwargs)

def mysql_exec(host, port, user, password, sql, out_path=None, socket_path=None):
    cmd = ["mysql"]
    if socket_path:
        cmd.append(f"-S{socket_path}")
    else:
        cmd.extend([f"-h{host}", f"-P{port}"])
    cmd.extend([f"-u{user}", f"-p{password}", "-e", sql])
    if out_path:
        with open(out_path, "a") as fh:
            return run_cmd(cmd, stdout=fh, stderr=fh)
    return run_cmd(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def mysql_query_text(host, port, user, password, sql, socket_path=None) -> str:
    cmd = ["mysql"]
    if socket_path:
        cmd.append(f"-S{socket_path}")
    else:
        cmd.extend([f"-h{host}", f"-P{port}"])
    cmd.extend([f"-u{user}", f"-p{password}", "-N", "-e", sql])
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as e:
        return f"<mysql_query_error>\n{e.output}"


def _admin_user(args):
    """Return (user, password) preferring admin creds when available."""
    au = getattr(args, 'admin_user', None)
    ap = getattr(args, 'admin_password', None)
    if au and ap:
        return au, ap
    return args.user, args.password


def _mysql_exec_admin(args, sql: str, out_path=None):
    """Execute SQL using admin creds if available; append to out_path if provided."""
    user, pw = _admin_user(args)
    return mysql_exec(
        args.host,
        args.port,
        user,
        pw,
        sql,
        out_path=out_path,
        socket_path=getattr(args, 'socket', None),
    )


def _mysql_query_text_admin(args, sql: str) -> str:
    user, pw = _admin_user(args)
    return mysql_query_text(
        args.host,
        args.port,
        user,
        pw,
        sql,
        socket_path=getattr(args, 'socket', None),
    )
def _datadir_from_mysql(args):
    try:
        txt = mysql_query_text(args.host, args.port, args.user, args.password,
                               "SHOW VARIABLES LIKE 'datadir'",
                               socket_path=getattr(args, 'socket', None))
        m = re.search(r"\t(.+)$", str(txt).strip().splitlines()[-1])
        return m.group(1).strip() if m else None
    except Exception:
        return None


def _free_gb(path: str) -> float:
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024**3)
    except Exception:
        return -1.0


def ensure_min_free_space(args) -> None:
    min_gb = getattr(args, 'min_free_gb', None)
    if not min_gb:
        return
    datadir = _datadir_from_mysql(args) or "/"
    free = _free_gb(datadir)
    if free < 0:
        return
    if free < float(min_gb):
        msg = f"[{timestamp()}] ERROR: free space {free:.1f} GiB at {datadir} is below --min-free-gb={min_gb}"
        print(msg)
        try:
            with args.log_status.open("a") as fh:
                fh.write(msg + "\n")
        except Exception:
            pass
        raise SystemExit(3)


def _fmt_gib(bytes_val):
    try:
        return f"{(float(bytes_val) / (1024**3)):.2f} GiB"
    except Exception:
        return str(bytes_val)


def log_space_undo_snapshot(args, note: str = "snapshot") -> None:
    """Print a concise space + UNDO snapshot to stdout (captured in launch.log)."""
    datadir = _datadir_from_mysql(args) or "/"
    free = _free_gb(datadir)
    try:
        info = _read_undo_info(args)
    except Exception:
        info = []
    total = sum([row.get('size') or 0 for row in info])
    summary = ", ".join([f"{row.get('name')}={_fmt_gib(row.get('size'))}/{row.get('state')}" for row in info[:6]])
    print(f"[{timestamp()}] Space/UNDO {note}: datadir={datadir} free={free:.1f} GiB; undo_total={_fmt_gib(total)}; {summary}")


def extract_ibuf_section(status_text: str) -> str:
    try:
        m = re.search(
            r"INSERT BUFFER AND ADAPTIVE HASH INDEX\n[-]+\n(.*?)(?:\n\n[A-Z ].*\n[-]+|\n\nEND OF INNODB MONITOR OUTPUT)",
            status_text,
            re.S,
        )
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return status_text


def parse_ibuf_section(section: str):
    out = {
        'size': None,
        'seg_size': None,
        'free_list_len': None,
        'merges_insert': 0,
        'merges_delete_mark': 0,
        'merges_delete': 0,
        'discarded_insert': 0,
        'discarded_delete_mark': 0,
        'discarded_delete': 0,
    }
    m = re.search(r"Ibuf:\s*size\s*(\d+),\s*free list len\s*(\d+),\s*seg size\s*(\d+)", section)
    if m:
        out['size'] = int(m.group(1))
        out['free_list_len'] = int(m.group(2))
        out['seg_size'] = int(m.group(3))
    m = re.search(r"merged operations:\s*\n?\s*insert\s*(\d+),\s*delete mark\s*(\d+),\s*delete\s*(\d+)", section)
    if m:
        out['merges_insert'] = int(m.group(1))
        out['merges_delete_mark'] = int(m.group(2))
        out['merges_delete'] = int(m.group(3))
    m = re.search(r"discarded operations:\s*\n?\s*insert\s*(\d+),\s*delete mark\s*(\d+),\s*delete\s*(\d+)", section)
    if m:
        out['discarded_insert'] = int(m.group(1))
        out['discarded_delete_mark'] = int(m.group(2))
        out['discarded_delete'] = int(m.group(3))
    return out

def _cmd_output(cmd: list) -> str:
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except Exception as e:
        out = f"<error running {cmd}: {e}>"
    return out

def _write_and_print_header(args, sysbench_cmd_preview: str):
    info = {
        "timestamp": timestamp(),
        "host": socket.gethostname(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "paths": {
            "cwd": str(os.getcwd()),
            "PATH": os.environ.get("PATH", ""),
            "sysbench_path": shutil.which("sysbench"),
            "mysql_path": shutil.which("mysql"),
        },
        "binaries": {
            "sysbench_version": _cmd_output(["sysbench", "--version"]).strip(),
            "mysql_client_version": _cmd_output(["mysql", "--version"]).strip(),
        },
        "effective_args": {
            "host": args.host,
            "port": args.port,
            "socket": getattr(args, 'socket', None),
            "user": args.user,
            "database": args.database,
            "tables": args.tables,
            "table_size": args.table_size,
            "threads": args.threads,
            "duration": args.duration,
            "report_interval": args.report_interval,
            "status_interval": args.status_interval,
            "out_dir": args.out_dir,
            "skip_prepare": getattr(args, "skip_prepare", False),
            "reset_db": getattr(args, "reset_db", False),
            "sysbench_script": getattr(args, "sysbench_script", "oltp_write_only"),
            "sb_extra": getattr(args, "sb_extra", ""),
            "metrics_interval": getattr(args, "metrics_interval", args.status_interval),
            "phase": getattr(args, "phase", "run"),
            "create_extra_indexes": getattr(args, "create_extra_indexes", False),
            "target_gb": getattr(args, "target_gb", None),
            "est_bytes_per_row": getattr(args, "est_bytes_per_row", None),
            "ibuf_mode": getattr(args, "ibuf_mode", None),
            "restart_before_run": getattr(args, "restart_before_run", True),
            "my_cnf": getattr(args, "my_cnf", None),
        },
        "logs": {
            "prepare": str(args.log_prepare),
            "run": str(args.log_run),
            "status": str(args.log_status),
            "manifest": str(args.log_manifest),
            "variables_snapshot": str(args.log_vars),
            "innodb_metrics": str(args.log_ibuf_metrics),
            "innodb_status": str(args.log_innodb_status),
            "innodb_metrics_csv": str(args.csv_ibuf_metrics),
            "innodb_status_csv": str(args.csv_innodb_status),
        },
        "sysbench_command_preview": sysbench_cmd_preview,
    }

    if getattr(args, 'target_gb', None) is not None:
        info.setdefault('notes', {})['table_size_source'] = 'computed_from_target_gb'
    print(f"[{timestamp()}] === Test Environment & Effective Parameters ===")
    for k, v in info["effective_args"].items():
        print(f"  {k}: {v}")
    print(f"  sysbench: {info['binaries']['sysbench_version']}")
    print(f"  mysql:    {info['binaries']['mysql_client_version']}")
    print(f"  logs:     {info['logs']}")

    with args.log_manifest.open("w") as mf:
        json.dump(info, mf, indent=2, sort_keys=True)


def write_schema_validation(args, note: str = "validation"):
    ts = timestamp().replace(" ", "_").replace(":", "-")
    path = args.out_dir if isinstance(args.out_dir, str) else str(args.out_dir)
    log_path = pathlib.Path(path) / f"schema_{note}_{ts}.txt"
    try:
        with log_path.open("w") as fh:
            fh.write(f"# {timestamp()}  Schema validation ({note}) for DB `{args.database}`\n\n")
            # SHOW CREATE for sbtest1 (structure + row_format/key_block_size)
            try:
                txt = mysql_query_text(args.host, args.port, args.user, args.password,
                                       f"SHOW CREATE TABLE `{args.database}`.sbtest1\\G",
                                       socket_path=getattr(args, 'socket', None))
                fh.write("-- SHOW CREATE TABLE sbtest1 --\n")
                fh.write(txt)
                fh.write("\n\n")
            except Exception as e:
                fh.write(f"<error SHOW CREATE: {e}>\n\n")

            # Storage and approximate row counts from information_schema
            try:
                q = (
                    "SELECT table_name, engine, row_format, table_rows, avg_row_length, "
                    "data_length, index_length, (data_length+index_length) AS total_bytes "
                    f"FROM information_schema.tables WHERE table_schema='{args.database}' "
                    "ORDER BY table_name"
                )
                txt = mysql_query_text(args.host, args.port, args.user, args.password, q,
                                       socket_path=getattr(args, 'socket', None))
                fh.write("-- information_schema.tables summary --\n")
                fh.write(txt)
                fh.write("\n\n")
                q2 = (
                    "SELECT SUM(table_rows) AS approx_rows, "
                    "SUM(data_length) AS data_bytes, SUM(index_length) AS index_bytes, "
                    "SUM(data_length+index_length) AS total_bytes "
                    f"FROM information_schema.tables WHERE table_schema='{args.database}'"
                )
                txt2 = mysql_query_text(args.host, args.port, args.user, args.password, q2,
                                        socket_path=getattr(args, 'socket', None))
                fh.write("-- information_schema.tables totals --\n")
                fh.write(txt2)
                fh.write("\n\n")
            except Exception as e:
                fh.write(f"<error information_schema: {e}>\n\n")

            # Datadir path and du -sh for the database directory if possible
            try:
                datadir = mysql_query_text(args.host, args.port, args.user, args.password,
                                           "SHOW VARIABLES LIKE 'datadir'",
                                           socket_path=getattr(args, 'socket', None))
                fh.write("-- datadir --\n")
                fh.write(datadir)
                fh.write("\n")
                m = re.search(r"\t(.+)$", datadir.strip().splitlines()[-1])
                if m:
                    ddir = m.group(1).strip()
                    dbdir = pathlib.PurePosixPath(ddir) / args.database
                    # Use du to estimate size of the schema folder
                    try:
                        out = subprocess.check_output(["du", "-sh", str(dbdir)], text=True, stderr=subprocess.STDOUT)
                        fh.write(f"-- du -sh {dbdir} --\n{out}\n")
                    except Exception as e2:
                        fh.write(f"<error du -sh: {e2}>\n")
            except Exception as e:
                fh.write(f"<error datadir: {e}>\n")
    except Exception:
        pass

def status_sampler(stop_evt, args):
    hdr_written = False
    while not stop_evt.is_set():
        start = time.time()
        line = f"# {timestamp()}  SHOW GLOBAL STATUS\n"
        try:
            with args.log_status.open("a") as fh:
                fh.write(line)
            mysql_exec(
                args.host,
                args.port,
                args.user,
                args.password,
                "SHOW GLOBAL STATUS",
                out_path=str(args.log_status),
                socket_path=getattr(args, 'socket', None),
            )
            with args.log_status.open("a") as fh:
                fh.write("\n")
        except Exception as e:
            with args.log_status.open("a") as fh:
                fh.write(f"# {timestamp()}  ERROR collecting status: {e}\n\n")
        elapsed = time.time() - start
        to_sleep = max(0.0, args.status_interval - elapsed)
        stop_evt.wait(to_sleep)


def _popen_to_file(cmd, logfile: pathlib.Path):
    """Spawn a subprocess piping combined stdout/stderr to logfile asynchronously."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def _pump():
        with logfile.open("a") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()

    t = threading.Thread(target=_pump, daemon=True)
    t.start()
    return proc

def build_sysbench_base(args):
    base = [
        "sysbench",
        f"{args.sysbench_script}",
        "--db-driver=mysql",
        f"--mysql-host={args.host}",
        f"--mysql-port={args.port}",
        f"--mysql-user={args.user}",
        f"--mysql-password={args.password}",
        f"--mysql-db={args.database}",
        f"--tables={args.tables}",
    ]
    sock = getattr(args, 'socket', None)
    if sock:
        base.append(f"--mysql-socket={sock}")
    return base

def ensure_outdir_and_logs(args):
    pathlib.Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    args.log_prepare  = pathlib.Path(args.out_dir) / f"sysbench_prepare_{stamp}.log"
    args.log_run      = pathlib.Path(args.out_dir) / f"sysbench_run_{stamp}.log"
    args.log_status   = pathlib.Path(args.out_dir) / f"global_status_{stamp}.log"
    args.log_manifest = pathlib.Path(args.out_dir) / f"manifest_{stamp}.json"
    args.log_vars     = pathlib.Path(args.out_dir) / f"mysql_variables_{stamp}.log"
    args.log_ibuf_metrics = pathlib.Path(args.out_dir) / f"innodb_metrics_{stamp}.log"
    args.log_innodb_status = pathlib.Path(args.out_dir) / f"innodb_status_{stamp}.log"
    args.csv_ibuf_metrics  = pathlib.Path(args.out_dir) / f"innodb_metrics_{stamp}.csv"
    args.csv_innodb_status = pathlib.Path(args.out_dir) / f"innodb_status_{stamp}.csv"


def _build_preview_command(args) -> str:
    """Create a human-readable sysbench command preview string for manifest/logs."""
    return " ".join([
        "sysbench",
        args.sysbench_script,
        "--db-driver=mysql",
        f"--mysql-host={args.host}",
        f"--mysql-port={args.port}",
        f"--mysql-user={args.user}",
        f"--mysql-db={args.database}",
        f"--tables={args.tables}",
        f"--table-size={args.table_size}",
        f"--threads={args.threads}",
        f"--time={args.duration}",
        f"--report-interval={args.report_interval}",
        (args.sb_extra or "").strip(),
    ])

def sysbench_prepare(args):
    cmd = build_sysbench_base(args) + [
        f"--table-size={args.table_size}",
        "--threads=8",
        f"--report-interval={args.report_interval}",
    ] + (shlex.split(getattr(args, "sb_extra", "")) if getattr(args, "sb_extra", "") else []) + [
        "--create_secondary=on",
        "--mysql-storage-engine=InnoDB",
        "prepare",
    ]
    proc = _popen_to_file(cmd, args.log_prepare)
    proc.wait()
    return subprocess.CompletedProcess(cmd, returncode=proc.returncode)

def sysbench_run(args):
    cmd = build_sysbench_base(args) + [
        f"--threads={args.threads}",
        f"--time={args.duration}",
        f"--report-interval={args.report_interval}",
    ] + (shlex.split(getattr(args, "sb_extra", "")) if getattr(args, "sb_extra", "") else []) + [
        "run",
    ]
    return _popen_to_file(cmd, args.log_run)

def maybe_create_db(args):
    """
    Ensure the target database exists and that the workload user can access it.
    If --admin-user/--admin-password are provided, use them to create the DB and
    grant privileges to the workload user on that DB.
    """
    # Create DB (prefer admin credentials if provided)
    creator_user = getattr(args, "admin_user", None) or args.user
    creator_pass = getattr(args, "admin_password", None) or args.password
    sql = f"CREATE DATABASE IF NOT EXISTS `{args.database}`"
    rc = mysql_exec(args.host, args.port, creator_user, creator_pass, sql, out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None)).returncode
    # If admin credentials were used, also grant privileges to workload user
    if getattr(args, "admin_user", None):
        grant_sqls = [
            f"GRANT ALL PRIVILEGES ON `{args.database}`.* TO '{args.user}'@'127.0.0.1'",
            f"GRANT ALL PRIVILEGES ON `{args.database}`.* TO '{args.user}'@'localhost'",
        ]
        for gsql in grant_sqls:
            mysql_exec(args.host, args.port, args.admin_user, args.admin_password, gsql, out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
    # Verify workload user can access the DB
    verify = mysql_exec(
        args.host,
        args.port,
        args.user,
        args.password,
        f"USE `{args.database}`; SELECT 1;",
        out_path=str(args.log_prepare),
        socket_path=getattr(args, 'socket', None),
    ).returncode
    return rc, verify


def _ensure_prereqs():
    """Ensure required binaries are present, raising SystemExit on failure."""
    if not which("sysbench"):
        raise SystemExit("ERROR: 'sysbench' not found in PATH.")
    if not which("mysql"):
        raise SystemExit("ERROR: 'mysql' client not found in PATH.")


def _start_samplers(stop_evt: threading.Event, args):
    """Start background sampler threads; return list of threads."""
    threads = []
    t_stat = threading.Thread(target=status_sampler, args=(stop_evt, args), daemon=True)
    t_stat.start()
    threads.append(t_stat)
    if not args.disable_ibuf_sampler:
        t_ibuf = threading.Thread(target=ibuf_sampler, args=(stop_evt, args), daemon=True)
        t_ibuf.start()
        threads.append(t_ibuf)
    return threads


def _stop_samplers(stop_evt: threading.Event, threads: list[threading.Thread]):
    """Signal stop and join sampler threads with small timeout."""
    stop_evt.set()
    for t in threads:
        try:
            t.join(timeout=5)
        except Exception:
            pass


def _read_undo_info(args) -> list[dict]:
    """Query INFORMATION_SCHEMA for UNDO tablespaces and return list of dicts.
    Columns: NAME, FILE_SIZE, STATE
    """
    sql = (
        "SELECT NAME, FILE_SIZE, STATE FROM INFORMATION_SCHEMA.INNODB_TABLESPACES "
        "WHERE SPACE_TYPE='Undo' ORDER BY NAME"
    )
    txt = _mysql_query_text_admin(args, sql)
    out = []
    for line in str(txt).strip().splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 3:
            continue
        name, size_str, state = parts
        try:
            size = int(size_str)
        except ValueError:
            size = None
        out.append({"name": name.strip(), "size": size, "state": state.strip().lower()})
    return out


def _is_reserved_undo(name: str) -> bool:
    return name.startswith("innodb_undo_")


def _create_new_undo_tablespaces(args, count: int, log_path: pathlib.Path) -> list[str]:
    """Create `count` new undo tablespaces named undo_<uuid>_<n>. Returns names created."""
    created = []
    for i in range(1, count + 1):
        uniq = uuid.uuid4().hex[:8]
        name = f"undo_{uniq}_{i}"
        sql = f"CREATE UNDO TABLESPACE {name} ADD DATAFILE '{name}.ibu'"
        _mysql_exec_admin(args, sql, out_path=str(log_path))
        created.append(name)
    return created


def _set_inactive_undo(args, name: str, log_path: pathlib.Path) -> None:
    sql = f"ALTER UNDO TABLESPACE {name} SET INACTIVE"
    _mysql_exec_admin(args, sql, out_path=str(log_path))


def _drop_undo_if_empty_and_allowed(args, name: str, log_path: pathlib.Path, allow_drop: bool) -> bool:
    if not allow_drop or _is_reserved_undo(name):
        return False
    # Verify empty state before attempting drop
    info = _read_undo_info(args)
    state = None
    for row in info:
        if row.get("name") == name:
            state = row.get("state")
            break
    if state != "empty":
        return False
    sql = f"DROP UNDO TABLESPACE {name}"
    rc = _mysql_exec_admin(args, sql, out_path=str(log_path)).returncode
    return rc == 0


def reclaim_undo_after_run(args) -> None:
    """Reclaim UNDO space automatically after each RUN.
    Strategy: always create N new UNDO spaces, set others INACTIVE (no DROP by default).
    Reserved 'innodb_undo_*' are never dropped; may become empty.
    """
    if not getattr(args, 'auto_undo_reclaim', True):
        return
    # If admin creds are not present, just return silently
    au, ap = getattr(args, 'admin_user', None), getattr(args, 'admin_password', None)
    if not (au and ap):
        return
    log_path = getattr(args, 'log_status', None) or getattr(args, 'log_prepare', None)
    if isinstance(log_path, pathlib.Path):
        lp = log_path
    else:
        lp = pathlib.Path(args.out_dir) / "undo_reclaim.log"
    try:
        with lp.open("a") as fh:
            fh.write(f"# {timestamp()}  UNDO reclaim start\n")
    except Exception:
        pass

    try:
        before = _read_undo_info(args)
        # Create new UNDO tablespaces
        keep = set(_create_new_undo_tablespaces(args, int(getattr(args, 'undo_rotate_count', 2)), lp))
        # Attempt to set all other UNDO spaces inactive, keeping at least two active ones
        for row in before:
            name = row.get("name")
            if not name or name in keep:
                continue
            try:
                _set_inactive_undo(args, name, lp)
            except Exception:
                # Best effort; continue
                pass
        # Optionally drop any non-reserved that are empty now
        if bool(getattr(args, 'undo_allow_drop', False)):
            current = _read_undo_info(args)
            for row in current:
                name = row.get("name")
                state = row.get("state")
                if not name or _is_reserved_undo(name):
                    continue
                if state == "empty":
                    try:
                        _drop_undo_if_empty_and_allowed(args, name, lp, True)
                    except Exception:
                        pass
        after = _read_undo_info(args)
        try:
            with lp.open("a") as fh:
                fh.write(f"# {timestamp()}  UNDO after: {after}\n\n")
        except Exception:
            pass
        # Console summary to launch.log
        total_before = sum([(r.get('size') or 0) for r in before])
        total_after = sum([(r.get('size') or 0) for r in after])
        print(f"[{timestamp()}] UNDO reclaim: total_before={_fmt_gib(total_before)} total_after={_fmt_gib(total_after)}")
    except Exception as e:
        try:
            with lp.open("a") as fh:
                fh.write(f"# {timestamp()}  UNDO reclaim error: {e}\n\n")
        except Exception:
            pass


def _graph_script_path() -> pathlib.Path:
    """Resolve path to graph_comparison.py next to this script if present."""
    here = pathlib.Path(__file__).resolve().parent
    p = here / "graph_comparison.py"
    return p


def _siblings_for_ibuf_group(out_dir: pathlib.Path):
    """Given an output dir of a run (…/results/<test_name>), return (base, sibling_dirs)
    when the test_name follows '<base>_ibuf_<mode>' pattern. Otherwise (None, [])."""
    test_name = out_dir.name
    if "_ibuf_" not in test_name:
        return None, []
    base = test_name.split("_ibuf_", 1)[0]
    modes = ["all", "inserts", "none"]
    siblings = [out_dir.parent / f"{base}_ibuf_{m}" for m in modes]
    return base, siblings


def _all_siblings_ready(siblings: list[pathlib.Path]) -> bool:
    for d in siblings:
        if not d.is_dir():
            return False
        if not any(d.glob("global_status_*.log")):
            return False
    return True


def _zip_folder(src_dir: pathlib.Path, zip_path: pathlib.Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(zip_path), "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for r, _, files in os.walk(src_dir):
            for fn in files:
                fp = pathlib.Path(r) / fn
                arc = fp.relative_to(src_dir)
                zf.write(str(fp), str(arc))


def generate_graphs_if_complete(args) -> None:
    """If this run belongs to an ibuf group and all 3 flavors exist, generate graphs.
    Graphs are saved under results_total/<base>/ and zipped to results_total/<base>_graphs.zip.
    """
    out_dir = pathlib.Path(args.out_dir)
    base, siblings = _siblings_for_ibuf_group(out_dir)
    if not base:
        return
    if not _all_siblings_ready(siblings):
        return
    graph_py = _graph_script_path()
    if not graph_py.exists():
        print(f"[{timestamp()}] NOTE: graph script not found at {graph_py}; skipping graphs.")
        return
    prefix = out_dir.parent.parent / "results_total" / base
    prefix.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable or "python3",
        str(graph_py),
        "--folders",
    ] + [str(d) for d in siblings] + [
        "--prefix", str(prefix),
        "--status-interval", str(args.status_interval),
        "--bucket-seconds", str(args.status_interval),
    ]
    try:
        print(f"[{timestamp()}] Generating graphs for ibuf group '{base}' → {prefix}")
        rc = run_cmd(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        # Append graph tool output to the run log for convenience
        with open(out_dir / "graphs_generation.log", "a") as gh:
            gh.write(rc.stdout or "")
        if rc.returncode == 0:
            zip_path = prefix.parent / f"{base}_graphs.zip"
            _zip_folder(prefix, zip_path)
            print(f"[{timestamp()}] Graphs ready: {prefix} (zip: {zip_path})")
        else:
            print(f"[{timestamp()}] WARNING: graph tool returned rc={rc.returncode}")
    except Exception as e:
        print(f"[{timestamp()}] WARNING: graphs generation failed: {e}")

def estimate_bytes_per_row(extra_indexes_count: int) -> int:
    base = 200
    per_sec = 38
    bpr = int(base + per_sec * extra_indexes_count)
    return int(bpr * 1.2)


def apply_target_gb(args):
    if getattr(args, 'target_gb', None) is None:
        return
    extra_idx = 1
    if getattr(args, 'create_extra_indexes', False):
        extra_idx += 3
    args.est_bytes_per_row = estimate_bytes_per_row(extra_idx)
    target_bytes = int(float(args.target_gb) * (1024**3))
    total_rows = max(1, target_bytes // max(1, args.est_bytes_per_row))
    per_table = max(1, total_rows // args.tables)
    args.table_size = per_table

def _ensure_csv_header(path: pathlib.Path, header: list[str]):
    exists = path.exists()
    with path.open('a', newline='') as fh:
        w = csv.writer(fh)
        if not exists:
            w.writerow(header)


def ibuf_sampler(stop_evt, args):
    _ensure_csv_header(args.csv_ibuf_metrics, ['ts','name','count'])
    _ensure_csv_header(args.csv_innodb_status, ['ts','size','seg_size','free_list_len','merges_insert','merges_delete_mark','merges_delete','discarded_insert','discarded_delete_mark','discarded_delete'])
    while not stop_evt.is_set():
        start = time.time()
        ts = timestamp()
        try:
            txt = mysql_query_text(
                args.host, args.port, args.user, args.password,
                "SELECT NAME, COUNT FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME LIKE 'ibuf_%' ORDER BY NAME"
            )
            with args.log_ibuf_metrics.open("a") as fh:
                fh.write(f"# {ts}  INNODB_METRICS ibuf_*\n")
                fh.write(txt if txt.endswith("\n") else txt+"\n")
                fh.write("\n")
            with args.csv_ibuf_metrics.open('a', newline='') as fhc:
                w = csv.writer(fhc)
                for line in txt.strip().splitlines():
                    parts = line.split('\t')
                    if len(parts) == 2 and parts[1].strip().isdigit():
                        w.writerow([ts, parts[0], parts[1]])
        except Exception as e:
            with args.log_ibuf_metrics.open("a") as fh:
                fh.write(f"# {ts}  ERROR collecting INNODB_METRICS: {e}\n\n")
        try:
            full = mysql_query_text(
                args.host, args.port, args.user, args.password,
                "SHOW ENGINE INNODB STATUS"
            )
            section = extract_ibuf_section(full)
            with args.log_innodb_status.open("a") as fh:
                fh.write(f"# {ts}  SHOW ENGINE INNODB STATUS (INSERT BUFFER section)\n")
                fh.write(section + ("\n" if not section.endswith("\n") else ""))
                fh.write("\n\n")
            parsed = parse_ibuf_section(section)
            with args.csv_innodb_status.open('a', newline='') as fhc:
                w = csv.writer(fhc)
                w.writerow([
                    ts,
                    parsed.get('size'), parsed.get('seg_size'), parsed.get('free_list_len'),
                    parsed.get('merges_insert'), parsed.get('merges_delete_mark'), parsed.get('merges_delete'),
                    parsed.get('discarded_insert'), parsed.get('discarded_delete_mark'), parsed.get('discarded_delete'),
                ])
        except Exception as e:
            with args.log_innodb_status.open("a") as fh:
                fh.write(f"# {ts}  ERROR collecting SHOW ENGINE INNODB STATUS: {e}\n\n")
        elapsed = time.time() - start
        to_sleep = max(0.0, args.metrics_interval - elapsed)
        stop_evt.wait(to_sleep)


def update_my_cnf(params: dict, my_cnf_path: str):
    path = pathlib.Path(my_cnf_path)
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        lines = []
    out_lines = []
    in_mysqld = False
    handled = set()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('['):
            if in_mysqld:
                for k, v in params.items():
                    if k not in handled:
                        out_lines.append(f"{k}={v}")
                        handled.add(k)
            in_mysqld = stripped.lower() == '[mysqld]'
            out_lines.append(line)
            continue
        if in_mysqld:
            key = stripped.split('=')[0].strip() if '=' in stripped else None
            if key in params:
                out_lines.append(f"{key}={params[key]}")
                handled.add(key)
            else:
                out_lines.append(line)
        else:
            out_lines.append(line)
    if in_mysqld:
        for k, v in params.items():
            if k not in handled:
                out_lines.append(f"{k}={v}")
                handled.add(k)
    elif params:
        out_lines.append('[mysqld]')
        for k, v in params.items():
            out_lines.append(f"{k}={v}")
    path.write_text("\n".join(out_lines) + "\n")


def restart_mysql():
    candidates = [
        ["systemctl", "restart", "mysqld"],
        ["systemctl", "restart", "mysql"],
        ["brew", "services", "restart", "mysql"],
        ["brew", "services", "restart", "mysql@8.0"],
        ["mysql.server", "restart"],
    ]
    last_rc = None
    for cmd in candidates:
        try:
            rc = run_cmd(cmd)
            last_rc = rc.returncode
            if rc.returncode == 0:
                return
        except Exception:
            continue
    raise SystemExit(f"mysql restart failed using known methods (last rc={last_rc}). Please restart the service manually.")


def restart_mysql_best(args=None):
    """Restart MySQL using sandbox script if available; otherwise fallback."""
    # Try sandbox-style restart based on my.cnf path
    if args is not None:
        my_cnf = getattr(args, 'my_cnf', None)
        if my_cnf:
            cnf_path = pathlib.Path(my_cnf).resolve()
            restart_sh = cnf_path.parent / 'restart'
            if restart_sh.exists() and os.access(restart_sh, os.X_OK):
                rc = run_cmd([str(restart_sh)])
                if rc.returncode == 0:
                    return
    # Fallback to generic restart methods
    return restart_mysql()


def parse_sysbench_log(path: pathlib.Path):
    tps = None
    p95 = None
    try:
        with path.open() as fh:
            for line in fh:
                m = re.search(r"transactions:\s*\d+\s*\(([^ ]+) per sec\.\)", line)
                if m:
                    try:
                        tps = float(m.group(1))
                    except ValueError:
                        pass
                m = re.search(r"95th percentile:\s*([0-9.]+)", line)
                if m:
                    try:
                        p95 = float(m.group(1))
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return tps, p95


def compute_ibuf_rates(csv_path: pathlib.Path, duration: int):
    metrics = [
        'ibuf_merges_insert',
        'ibuf_merges_delete_mark',
        'ibuf_merges_delete',
        'ibuf_discarded_insert',
        'ibuf_discarded_delete_mark',
        'ibuf_discarded_delete',
    ]
    data = {m: [] for m in metrics}
    try:
        with csv_path.open() as fh:
            reader = csv.reader(fh)
            next(reader, None)
            for row in reader:
                if len(row) != 3:
                    continue
                _, name, cnt = row
                if name in data and cnt.isdigit():
                    data[name].append(int(cnt))
    except FileNotFoundError:
        pass
    rates = {}
    for m, values in data.items():
        if len(values) >= 2:
            rates[f"{m}_per_s"] = (values[-1] - values[0]) / max(1, duration)
        else:
            rates[f"{m}_per_s"] = 0.0
    return rates


def load_scenarios(path: pathlib.Path):
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        raise SystemExit(f"Scenario file not found: {path}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in scenario file {path}: {e}")

    if not isinstance(data, list):
        raise SystemExit("Scenario file must contain a JSON array")

    for idx, scen in enumerate(data, 1):
        if not isinstance(scen, dict):
            raise SystemExit(f"Scenario #{idx} is not an object")
        name = scen.get("name")
        if not isinstance(name, str):
            raise SystemExit(f"Scenario #{idx} missing 'name' string")
        params = scen.get("params")
        if not isinstance(params, dict):
            raise SystemExit(f"Scenario '{name}' missing 'params' object")
        sb = scen.get("sysbench")
        if not isinstance(sb, dict):
            raise SystemExit(f"Scenario '{name}' missing 'sysbench' object")
        for key in ("threads", "duration"):
            if not isinstance(sb.get(key), int):
                raise SystemExit(f"Scenario '{name}' sysbench.{key} must be an integer")
        for opt in ("script", "sb_extra"):
            if opt in sb and not isinstance(sb[opt], str):
                raise SystemExit(f"Scenario '{name}' sysbench.{opt} must be a string")

    return data


def run_single(args):
    if args.test_name:
        test_name = _sanitize_name(args.test_name)
    else:
        print(f"[{timestamp()}] Ready to start. A per-test folder will be created under base: {args.out_dir}")
        raw = input("Enter a descriptive test folder name (e.g., 'cb_all_comp_8idx_32thr'): ").strip()
        test_name = _sanitize_name(raw)
    args.out_dir = str(pathlib.Path(args.out_dir) / test_name)

    apply_target_gb(args)
    _ensure_prereqs()

    ensure_outdir_and_logs(args)

    try:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  SHOW GLOBAL VARIABLES snapshot\n")
        mysql_exec(
            args.host,
            args.port,
            args.user,
            args.password,
            "SHOW GLOBAL VARIABLES",
            out_path=str(args.log_vars),
            socket_path=getattr(args, 'socket', None),
        )
        with args.log_vars.open("a") as cf:
            cf.write("\n")
    except Exception as e:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  ERROR collecting SHOW GLOBAL VARIABLES: {e}\n\n")

    try:
        mysql_exec(args.host, args.port, args.user, args.password,
                   "SET GLOBAL innodb_monitor_enable='all'",
                   out_path=str(args.log_vars), socket_path=getattr(args, 'socket', None))
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  Enabled innodb_monitor_enable=all\n\n")
    except Exception as e:
        with args.log_vars.open("a") as cf:
            cf.write(f"# {timestamp()}  ERROR enabling innodb_monitor_enable=all: {e}\n\n")

    preview = _build_preview_command(args)
    _write_and_print_header(args, preview)

    print(f"[{timestamp()}] Using test folder: {args.out_dir}")
    print(f"[{timestamp()}] Logs:")
    print(f"  prepare -> {args.log_prepare}")
    print(f"  run     -> {args.log_run}")
    print(f"  status  -> {args.log_status}")
    print(f"  ibuf metrics -> {args.log_ibuf_metrics}")
    print(f"  innodb status -> {args.log_innodb_status}")
    print(f"  metrics CSV -> {args.csv_ibuf_metrics}")
    print(f"  status  CSV -> {args.csv_innodb_status}")
    if args.target_gb is not None:
        print(f"  NOTE: --target-gb set to {args.target_gb}. table-size computed as {args.table_size} rows/table across {args.tables} tables.")

    if args.phase in ("init","both"):
        if args.reset_db:
            mysql_exec(args.host, args.port, args.user, args.password,
                       f"DROP DATABASE IF EXISTS `{args.database}`",
                       out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
            mysql_exec(args.host, args.port, args.user, args.password,
                       f"CREATE DATABASE `{args.database}`",
                       out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
        else:
            create_rc, verify_rc = maybe_create_db(args)
            if verify_rc != 0:
                print(f"[{timestamp()}] ERROR: cannot access database '{args.database}' as user '{args.user}'. See {args.log_prepare}")
                raise SystemExit(2)
        print(f"[{timestamp()}] Running sysbench PREPARE (table-size={args.table_size})…")
        rc = sysbench_prepare(args).returncode
        if rc != 0:
            print(f"[{timestamp()}] PREPARE returned non-zero ({rc}). Check {args.log_prepare}")
            if args.phase == "init":
                raise SystemExit(rc)
        if args.create_extra_indexes:
            print(f"[{timestamp()}] Adding extra secondary indexes on sbtest tables…")
            for i in range(1, args.tables + 1):
                sql = (
                    f"ALTER TABLE `{args.database}`.sbtest{i} "
                    f"ADD INDEX idx_c (c(20)), "
                    f"ADD INDEX idx_pad (pad(20)), "
                    f"ADD INDEX idx_kid (k,id)"
                )
                mysql_exec(args.host, args.port, args.user, args.password, sql, out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
        # Optionally remove partitioning if present (custom sysbench variant creates partitions)
        if getattr(args, 'remove_partitioning', False):
            print(f"[{timestamp()}] Removing partitioning from sbtest tables (if any)…")
            for i in range(1, args.tables + 1):
                # Only run when the table is partitioned to avoid errors
                check_sql = (
                    f"SELECT COUNT(*) FROM information_schema.partitions "
                    f"WHERE table_schema='{args.database}' AND table_name='sbtest{i}' AND partition_name IS NOT NULL"
                )
                txt = mysql_query_text(args.host, args.port, args.user, args.password, check_sql, socket_path=getattr(args, 'socket', None))
                try:
                    cnt = int(str(txt).strip().splitlines()[-1]) if str(txt).strip() else 0
                except Exception:
                    cnt = 0
                if cnt > 0:
                    sql = f"ALTER TABLE `{args.database}`.sbtest{i} REMOVE PARTITIONING"
                    mysql_exec(args.host, args.port, args.user, args.password, sql, out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
                else:
                    # still log the action for traceability
                    with args.log_prepare.open("a") as fh:
                        fh.write(f"# {timestamp()}  sbtest{i} not partitioned; skip REMOVE PARTITIONING\n")
        # Optionally alter row format after indexes are ensured
        if args.row_format:
            rf = args.row_format.upper()
            kb = args.key_block_size
            kb_clause = f" KEY_BLOCK_SIZE={int(kb)}" if (kb is not None and rf == "COMPRESSED") else ""
            print(f"[{timestamp()}] Altering sbtest tables to ROW_FORMAT={rf}{kb_clause}…")
            for i in range(1, args.tables + 1):
                sql = f"ALTER TABLE `{args.database}`.sbtest{i} ROW_FORMAT={rf}{kb_clause}"
                mysql_exec(args.host, args.port, args.user, args.password, sql, out_path=str(args.log_prepare), socket_path=getattr(args, 'socket', None))
        # Always write a schema validation snapshot after PREPARE
        try:
            write_schema_validation(args, note="post_prepare")
        except Exception:
            pass

        if args.phase == "init":
            print(f"[{timestamp()}] Initialization completed. Data prepared in DB '{args.database}'.")
            print(f"[{timestamp()}] Done. Logs in: {args.out_dir}")
            return

    # Always restart before RUN (default behavior), and optionally adjust ibuf mode
    # If ibuf_mode is provided, update my.cnf first to avoid a double restart
    do_restart = getattr(args, 'restart_before_run', True)
    ibuf_mode = getattr(args, 'ibuf_mode', None)
    if do_restart:
        try:
            if ibuf_mode:
                update_my_cnf({'innodb_change_buffering': ibuf_mode}, args.my_cnf)
            restart_mysql_best(args)
        except Exception as e:
            print(f"[{timestamp()}] WARNING: restart_before_run failed: {e}")

    print(f"[{timestamp()}] Starting sysbench RUN for {args.duration}s with {args.threads} threads…")
    proc = sysbench_run(args)

    stop_evt = threading.Event()
    samplers = _start_samplers(stop_evt, args)
    rc = proc.wait()
    _stop_samplers(stop_evt, samplers)
    # UNDO reclaim hook (always after each RUN when enabled)
    try:
        reclaim_undo_after_run(args)
    except Exception as _:
        pass
    print(f"[{timestamp()}] sysbench finished with exit code {rc}")
    print(f"[{timestamp()}] Done. Logs in: {args.out_dir}")
    # Attempt to generate graphs comparing ibuf modes if all siblings are present
    try:
        generate_graphs_if_complete(args)
    except Exception as _:
        pass


def run_scenarios(args):
    base_dir = pathlib.Path(args.out_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    scen_path = DEFAULT_SCENARIOS.get(args.scenario_file, pathlib.Path(args.scenario_file))
    scenarios = load_scenarios(scen_path)
    summary = []
    for idx, scen in enumerate(scenarios):
        scen_name = _sanitize_name(scen.get('name', 'scenario'))
        params = scen.get('params', {})
        sb = scen.get('sysbench', {})
        # Apply MySQL config params at scenario boundary and restart once here
        if isinstance(params, dict) and params:
            update_my_cnf(params, args.my_cnf)
        restart_mysql_best(args)
        scen_args = copy.deepcopy(args)
        scen_args.test_name = scen_name
        scen_args.out_dir = str(base_dir)
        scen_args.threads = sb.get('threads', args.threads)
        scen_args.duration = sb.get('duration', args.duration)
        if 'script' in sb:
            scen_args.sysbench_script = sb['script']
        if 'sb_extra' in sb:
            scen_args.sb_extra = sb['sb_extra']
        # Scenario-level phase (init, run, both)
        if isinstance(scen.get('phase', None), str):
            scen_args.phase = scen['phase']
        # Scenario-level flags
        if isinstance(scen.get('skip_prepare', None), bool):
            scen_args.skip_prepare = scen['skip_prepare']
        if isinstance(scen.get('reset_db', None), bool):
            scen_args.reset_db = scen['reset_db']
        # Map IBUF mode from params so run_single also updates my.cnf prior to RUN
        if isinstance(params, dict) and 'innodb_change_buffering' in params:
            scen_args.ibuf_mode = params['innodb_change_buffering']
        # Shape overrides per-scenario (optional)
        shape = scen.get('shape', {})
        if isinstance(shape, dict):
            # Reset key_block_size by default to avoid carrying COMPRESSED settings
            # into shapes that do not require compression (e.g., DYNAMIC).
            if hasattr(scen_args, 'key_block_size'):
                scen_args.key_block_size = None
            if 'database' in shape:
                scen_args.database = shape['database']
            if 'tables' in shape:
                scen_args.tables = int(shape['tables'])
            if 'table_size' in shape:
                scen_args.table_size = int(shape['table_size'])
            if 'row_format' in shape and shape['row_format']:
                scen_args.row_format = str(shape['row_format']).upper()
                # If not COMPRESSED, ensure no KEY_BLOCK_SIZE is applied.
                if scen_args.row_format != 'COMPRESSED':
                    scen_args.key_block_size = None
                    # If dynamic row format desired, use dynamic sysbench wrapper if present
                    dyn_wrapper = "/nvme/matias.sanchez/sandboxes/stats/sysbench/oltp_write_only_dyn.lua"
                    try:
                        # Only override if current script is the std write_only
                        if scen_args.sysbench_script and 'oltp_write_only_std.lua' in scen_args.sysbench_script:
                            scen_args.sysbench_script = dyn_wrapper
                    except Exception:
                        pass
            if 'key_block_size' in shape and shape['key_block_size'] is not None:
                scen_args.key_block_size = int(shape['key_block_size'])
            if 'create_extra_indexes' in shape:
                scen_args.create_extra_indexes = bool(shape['create_extra_indexes'])
            if 'remove_partitioning' in shape:
                scen_args.remove_partitioning = bool(shape['remove_partitioning'])
        # Ensure we also restart right before RUN inside run_single
        setattr(scen_args, 'restart_before_run', True)
        # Space/UNDO snapshot before RUN phase
        try:
            log_space_undo_snapshot(scen_args, note=f"before {scen_name}")
        except Exception:
            pass
        # Guard free space before RUN phase
        try:
            if getattr(scen_args, 'phase', 'run') == 'run':
                ensure_min_free_space(scen_args)
        except SystemExit as e:
            raise
        run_single(scen_args)
        # Space/UNDO snapshot after RUN phase
        try:
            log_space_undo_snapshot(scen_args, note=f"after {scen_name}")
        except Exception:
            pass
        # Auto-drop DB at shape boundary (when next scenario switches database)
        try:
            if getattr(args, 'auto_drop_shape_db', False):
                cur_db = scen_args.database
                next_db = None
                if idx + 1 < len(scenarios):
                    nxt = scenarios[idx + 1]
                    shp = nxt.get('shape', {}) if isinstance(nxt.get('shape', {}), dict) else {}
                    next_db = shp.get('database') if isinstance(shp, dict) else None
                if next_db and next_db != cur_db:
                    # Drop current database before moving to next shape
                    print(f"[{timestamp()}] Auto-drop DB '{cur_db}' before switching to '{next_db}'")
                    _mysql_exec_admin(args, f"DROP DATABASE IF EXISTS `{cur_db}`", out_path=str(scen_args.log_prepare))
        except Exception:
            pass
        tps, p95 = parse_sysbench_log(scen_args.log_run)
        rates = compute_ibuf_rates(scen_args.csv_ibuf_metrics, scen_args.duration)
        row = {
            'scenario': scen_name,
            'threads': scen_args.threads,
            'duration': scen_args.duration,
            'tps_avg': tps,
            'p95_ms': p95,
        }
        row.update(rates)
        summary.append(row)
    if summary:
        summary_path = base_dir / 'summary.csv'
        with summary_path.open('w', newline='') as fh:
            fieldnames = list(summary[0].keys())
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in summary:
                writer.writerow(row)
        print(f"[{timestamp()}] Summary written to {summary_path}")



def main():
    parser = argparse.ArgumentParser(
        description="Run sysbench and collect SHOW GLOBAL STATUS in parallel.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    g_conn = parser.add_argument_group("Connection options")
    g_shape = parser.add_argument_group("Data shape (prepare phase)")
    g_exec = parser.add_argument_group("Benchmark execution")
    g_mon = parser.add_argument_group("Monitoring")
    g_flow = parser.add_argument_group("Workflow control")
    g_workload = parser.add_argument_group("Workload shaping")
    g_space = parser.add_argument_group("Space & cleanup")
    g_undo = parser.add_argument_group("UNDO space management")
    g_out = parser.add_argument_group("Logging & output")
    g_conn.add_argument("--host", default=DEFAULTS["host"], help="MySQL host")
    g_conn.add_argument("--port", type=int, default=DEFAULTS["port"], help="MySQL port")
    g_conn.add_argument("--user", default=DEFAULTS["user"], help="MySQL user")
    g_conn.add_argument("--password", default=DEFAULTS["password"], help="MySQL password")
    g_conn.add_argument("--socket", default=None, help="MySQL Unix socket path (optional)")
    g_conn.add_argument("--admin-user", default=None, help="Admin user for DB create/grants (optional)")
    g_conn.add_argument("--admin-password", default=None, help="Admin password (optional)")
    g_conn.add_argument("--database", default=DEFAULTS["database"], help="Database name")
    g_shape.add_argument("--tables", type=int, default=DEFAULTS["tables"], help="Number of sbtest tables")
    # Optional post-PREPARE schema adjustments
    g_shape.add_argument("--row-format", type=str.upper, choices=["DYNAMIC", "COMPRESSED", "COMPACT"], default=None,
                         help="Set ROW_FORMAT on sbtest tables after PREPARE via ALTER TABLE (optional)")
    g_shape.add_argument("--key-block-size", type=int, default=None,
                         help="KEY_BLOCK_SIZE for COMPRESSED row format; applied with --row-format if provided")
    g_shape.add_argument("--remove-partitioning", action="store_true",
                         help="After PREPARE, run ALTER TABLE ... REMOVE PARTITIONING on sbtest tables (if partitioned)")
    mex = g_shape.add_mutually_exclusive_group()
    mex.add_argument("--table-size", type=int, default=DEFAULTS["table_size"], help="Rows per table during prepare. Mutually exclusive with --target-gb.")
    mex.add_argument("--target-gb", type=float, default=None, help="Quick estimator: target total dataset size in GiB; computes rows/table from expected bytes/row. Mutually exclusive with --table-size.")
    g_exec.add_argument("--threads", type=int, default=DEFAULTS["threads"], help="Sysbench worker threads during RUN phase")
    g_exec.add_argument("--duration", type=int, default=DEFAULTS["duration"], help="Run time in seconds")
    g_exec.add_argument("--report-interval", type=int, default=DEFAULTS["report_interval"], help="Seconds between sysbench reports")
    g_mon.add_argument("--status-interval", type=int, default=DEFAULTS["status_interval"], help="Seconds between SHOW GLOBAL STATUS samples")
    g_mon.add_argument("--metrics-interval", type=int, default=DEFAULTS.get("metrics_interval", 10), help="Seconds between INNODB_METRICS and SHOW ENGINE INNODB STATUS samples")
    g_mon.add_argument("--disable-ibuf-sampler", action="store_true", help="Disable change buffer (ibuf) samplers")
    g_flow.add_argument("--phase", choices=["init","run","both"], default="run", help="init: data load only; run: benchmark only; both: prepare then run")
    g_flow.add_argument("--skip-prepare", action="store_true", help="Skip the initial sysbench prepare (data load)")
    g_flow.add_argument("--reset-db", action="store_true", help="Drop and re-create the database before prepare (requires privileges)")
    g_flow.add_argument("--ibuf-mode", choices=["all","inserts","none"], default=None, help="Set innodb_change_buffering before RUN (server is restarted before RUN)")
    parser.set_defaults(restart_before_run=True)
    g_workload.add_argument("--sysbench-script", default="oltp_write_only", help="Sysbench script (e.g., oltp_write_only, oltp_update_index)")
    g_workload.add_argument("--sb-extra", default="", help="Extra sysbench script options appended to both PREPARE and RUN")
    g_workload.add_argument("--create-extra-indexes", action="store_true", help="After PREPARE, add 3 non-unique secondary indexes per sbtestN: idx_c(c(20)), idx_pad(pad(20)), idx_kid(k,id)")
    g_out.add_argument("--out-dir", default=DEFAULTS["out_dir"], help="Base directory for logs; a per-test subfolder is created")
    g_out.add_argument("--test-name", default=None, help="Name for the per-test subfolder (skips interactive prompt)")
    g_out.add_argument("--scenario-file", default=None, help="JSON file describing scenarios to run sequentially or name of a built-in set (e.g., 'default_ibuf_3')")
    g_out.add_argument("--my-cnf", default="/etc/my.cnf", help="Path to my.cnf for scenario param updates")
    # UNDO management flags (defaults chosen to always rotate and never drop)
    parser.add_argument("--no-auto-undo-reclaim", dest="auto_undo_reclaim", action="store_false", help="Disable automatic UNDO reclaim after each RUN")
    g_undo.add_argument("--undo-rotate-count", type=int, default=2, help="Number of new UNDO tablespaces to create when reclaiming")
    g_undo.add_argument("--undo-allow-drop", action="store_true", help="Allow dropping non-reserved UNDO tablespaces once STATE=empty")
    # Space and cleanup behavior
    g_space.add_argument("--min-free-gb", type=float, default=None, help="Abort RUN if free GiB at datadir is below this threshold")
    g_space.add_argument("--auto-drop-shape-db", action="store_true", help="Drop the shape database when the scenario switches to a different database")
    args = parser.parse_args()
    if args.scenario_file:
        run_scenarios(args)
    else:
        run_single(args)

if __name__ == "__main__":
    main()
