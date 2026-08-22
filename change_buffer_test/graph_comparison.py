#!/usr/bin/env python3
"""Plot InnoDB status and change buffer metrics."""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import MaxNLocator
sns.set_theme(style="whitegrid")
plt.rcParams.update({
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
})
import numpy as np

# --- Manifest helpers & clamping ---
import json
from pathlib import Path

# --- Bucketing helpers (aggregate per fixed seconds) ---
from collections import defaultdict

def _bucketize_series(t_seconds: List[float], values: List[int], bucket_seconds: float) -> Tuple[List[float], List[int]]:
    """Aggregate values into fixed-size buckets in seconds (sum per bucket).

    This is used for generic counters where downstream logic decides how to
    normalize the bucket sums (e.g., divide by wall-time).
    """
    if not t_seconds or not values or bucket_seconds <= 0:
        return t_seconds, values
    agg = defaultdict(int)
    for t, v in zip(t_seconds, values):
        agg[int(t // bucket_seconds)] += int(v)
    buckets = sorted(agg)
    return [b * bucket_seconds for b in buckets], [agg[b] for b in buckets]


def _bucketize_rates_from_deltas(
    t_seconds: List[float],
    deltas: List[int],
    bucket_seconds: float,
) -> Tuple[List[float], List[float]]:
    """Compute per-second rates by distributing deltas over real elapsed time.

    - Uses actual dt between consecutive snapshots instead of assuming a fixed
      status interval. This avoids undercounting when samples are missing or
      when timestamps jitter.
    - Each (delta, dt) pair is attributed to the bucket of the END timestamp.
    """
    n = min(len(t_seconds), len(deltas))
    if n < 2 or bucket_seconds <= 0:
        return ([], [])
    sum_v: Dict[int, float] = defaultdict(float)
    sum_dt: Dict[int, float] = defaultdict(float)
    # Start from index 1 because deltas[0] corresponds to no interval
    for i in range(1, n):
        dt = max(1e-9, float(t_seconds[i] - t_seconds[i - 1]))
        b = int(t_seconds[i] // bucket_seconds)
        sum_v[b] += float(deltas[i])
        sum_dt[b] += dt
    buckets = sorted(sum_v.keys())
    t_out = [b * bucket_seconds for b in buckets]
    rates = [sum_v[b] / max(1e-9, sum_dt[b]) for b in buckets]
    return t_out, rates

def _align_two_series(t1: List[float], v1: List[int], t2: List[float], v2: List[int]) -> Tuple[List[float], List[int], List[int]]:
    i1 = {t: v for t, v in zip(t1, v1)}
    i2 = {t: v for t, v in zip(t2, v2)}
    common = sorted(set(i1) & set(i2))
    return common, [i1[t] for t in common], [i2[t] for t in common]

import math
def _mask_isolated_zeros(vals: List[float]) -> List[float]:
    if not vals:
        return vals
    out = vals[:]
    for i, v in enumerate(out):
        if v == 0:
            left = out[i-1] if i > 0 else None
            right = out[i+1] if i+1 < len(out) else None
            if (left and left > 0) or (right and right > 0):
                out[i] = math.nan
    return out

def _read_manifest_duration(folder: str) -> Optional[int]:
    try:
        manifests = sorted(Path(folder).glob("manifest_*.json"), key=lambda p: p.stat().st_mtime)
        if not manifests:
            return None
        data = json.loads(manifests[-1].read_text())
        dur = (data.get("effective_args", {}) or {}).get("duration")
        if isinstance(dur, int):
            return dur
        if isinstance(data.get("duration"), int):
            return data["duration"]
    except Exception:
        return None
    return None

def _clamp_run_series_to_duration(run: "RunSeries", seconds: Optional[int]) -> "RunSeries":
    if not seconds or not run.t_rel:
        return run
    last = len(run.t_rel) - 1
    while last >= 0 and run.t_rel[last] > float(seconds):
        last -= 1
    if last < 0:
        last = 0
    run.t_rel = run.t_rel[: last + 1]
    for m, vals in list(run.series.items()):
        run.series[m] = vals[: last + 1]
    return run

# --- Helper functions for parsing numeric ranges with suffixes ---
def _parse_num_with_suffix(s: str) -> float:
    s = s.strip().lower()
    mult = 1.0
    if s.endswith("k"):
        mult = 1_000.0
        s = s[:-1]
    elif s.endswith("m"):
        mult = 1_000_000.0
        s = s[:-1]
    return float(s) * mult

def _parse_fixed_range(spec: str) -> Tuple[float, float]:
    parts = spec.split(":")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("Expected format MIN:MAX (e.g., 20k:25k)")
    lo = _parse_num_with_suffix(parts[0])
    hi = _parse_num_with_suffix(parts[1])
    if hi <= lo:
        raise argparse.ArgumentTypeError("MAX must be > MIN")
    return (lo, hi)

def compute_deltas(series: Dict[str, List[int]]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    for m, vals in series.items():
        if not vals:
            out[m] = []
            continue
        deltas = [0]
        for i in range(1, len(vals)):
            d = vals[i] - vals[i-1]
            if d < 0:
                d = 0
            deltas.append(d)
        out[m] = deltas
    return out

# Default metrics to compare (case-sensitive keys as in SHOW GLOBAL STATUS)
DEFAULT_METRICS = [
    "Innodb_rows_inserted",
    "Innodb_rows_updated",
    "Innodb_rows_deleted",
]

STATUS_FILE_PATTERN = "global_status_*.log"
IBUF_METRICS_PATTERN = "innodb_metrics_*.csv"
IBUF_STATUS_PATTERN = "innodb_status_*.csv"

# Additional high‑level metrics for QPS/TPS dashboard
QPS_TPS_METRICS = ["Queries", "Com_commit", "Com_rollback"]

# Change buffer metric groups (defaults)
IBUF_MERGE_METRICS = [
    "ibuf_merges_insert",
    "ibuf_merges_delete_mark",
    "ibuf_merges_delete",
]
IBUF_DISCARD_METRICS = [
    "ibuf_discard_insert",
    "ibuf_discard_delete_mark",
    "ibuf_discard_delete",
]
IBUF_SIZE_FIELDS = ["size", "free_list_len"]


@dataclass
class Snapshot:
    t_abs: Optional[datetime]          # absolute timestamp if known
    values: Dict[str, int]             # cumulative counter values


@dataclass
class RunSeries:
    name: str
    # time series aligned to relative seconds (t0 = 0 for each run)
    t_rel: List[float]
    series: Dict[str, List[int]]       # metric -> list of values
    source_file: str


# Regex helpers
TS_RE = re.compile(
    r"(?P<dt>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})"
)
KV_RE = re.compile(r"^(\w[\w_]+)\s+(\d+)$")


def parse_status_timeseries(path: str, metrics: List[str], status_interval: int) -> List[Snapshot]:
    snapshots: List[Snapshot] = []
    cur_values: Dict[str, int] = {}
    cur_ts: Optional[datetime] = None
    saw_any_kv = False

    def flush_snapshot():
        nonlocal cur_values, cur_ts, saw_any_kv
        if not cur_values and not saw_any_kv:
            return
        snapshots.append(Snapshot(t_abs=cur_ts, values=cur_values))
        cur_values = {}
        cur_ts = None
        saw_any_kv = False

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                flush_snapshot()
                continue
            m_ts = TS_RE.search(line)
            if m_ts:
                flush_snapshot()
                try:
                    cur_ts = datetime.strptime(m_ts.group("dt"), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        cur_ts = datetime.strptime(m_ts.group("dt"), "%Y-%m-%dT%H:%M:%S")
                    except ValueError:
                        cur_ts = None
                continue
            m_kv = KV_RE.match(line)
            if m_kv:
                key, val_s = m_kv.group(1), m_kv.group(2)
                if key in metrics:
                    cur_values[key] = int(val_s)
                saw_any_kv = True

    flush_snapshot()
    if not snapshots and (cur_values or saw_any_kv):
        snapshots.append(Snapshot(t_abs=None, values=cur_values))
    return snapshots


def normalize_run_series(name: str, snapshots: List[Snapshot], metrics: List[str], status_interval: int, source_file: str) -> RunSeries:
    t_rel: List[float] = []
    series: Dict[str, List[int]] = {m: [] for m in metrics}
    have_ts = any(s.t_abs is not None for s in snapshots)

    if have_ts:
        first_idx = next(i for i, s in enumerate(snapshots) if s.t_abs is not None)
        t0 = snapshots[first_idx].t_abs
        for s in snapshots:
            cur_t = (s.t_abs - t0).total_seconds() if s.t_abs else (t_rel[-1] + status_interval) if t_rel else 0.0
            if t_rel and cur_t <= t_rel[-1]:
                cur_t = t_rel[-1] + status_interval
            t_rel.append(cur_t)
    else:
        t_rel = [i * float(status_interval) for i in range(len(snapshots))]

    last_vals: Dict[str, int] = {m: 0 for m in metrics}
    for s in snapshots:
        for m in metrics:
            if m in s.values:
                last_vals[m] = s.values[m]
            if series[m]:
                last_vals[m] = max(last_vals[m], series[m][-1])
            series[m].append(last_vals[m])

    if t_rel:
        t0 = t_rel[0]
        t_rel = [t - t0 for t in t_rel]

    return RunSeries(name=name, t_rel=t_rel, series=series, source_file=source_file)


def find_status_file(folder: str) -> str:
    candidates = sorted(
        glob.glob(os.path.join(folder, STATUS_FILE_PATTERN)), key=os.path.getmtime
    )
    if not candidates:
        raise FileNotFoundError(
            f"No '{STATUS_FILE_PATTERN}' file found in {folder!r}."
        )
    return candidates[-1]


def load_runs(folders: List[str], metrics: List[str], status_interval: int) -> List[RunSeries]:
    runs: List[RunSeries] = []
    for folder in folders:
        status_path = find_status_file(folder)
        snapshots = parse_status_timeseries(status_path, metrics, status_interval)
        if not snapshots:
            raise RuntimeError(f"No parsable snapshots in {status_path}")
        name = os.path.basename(os.path.abspath(folder))
        r = normalize_run_series(name, snapshots, metrics, status_interval, source_file=status_path)
        # If a manifest with duration exists, clamp to it so X-axis reflects the run length
        dur = _read_manifest_duration(folder)
        r = _clamp_run_series_to_duration(r, dur)
        runs.append(r)
    return runs


def find_ibuf_metrics_file(folder: str) -> str:
    candidates = sorted(
        glob.glob(os.path.join(folder, IBUF_METRICS_PATTERN)), key=os.path.getmtime
    )
    if not candidates:
        raise FileNotFoundError(
            f"No '{IBUF_METRICS_PATTERN}' file found in {folder!r}."
        )
    return candidates[-1]


def parse_ibuf_metrics_csv(path: str, metrics: List[str]) -> Tuple[List[float], Dict[str, List[int]]]:
    records: Dict[datetime, Dict[str, int]] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            name = row.get("name")
            val_s = row.get("count")
            if name is None or val_s is None:
                continue
            try:
                val = int(val_s)
            except ValueError:
                continue
            records.setdefault(ts, {})[name] = val
    if not records:
        return ([], {m: [] for m in metrics})
    ts_sorted = sorted(records.keys())
    t0 = ts_sorted[0]
    t_rel = [(t - t0).total_seconds() for t in ts_sorted]
    series: Dict[str, List[int]] = {m: [] for m in metrics}
    last_vals: Dict[str, int] = {m: 0 for m in metrics}
    for t in ts_sorted:
        vals = records[t]
        for m in metrics:
            if m in vals:
                last_vals[m] = vals[m]
            series[m].append(last_vals[m])
    return t_rel, series


def load_ibuf_metric_runs(folders: List[str], metrics: List[str]) -> List[RunSeries]:
    runs: List[RunSeries] = []
    for folder in folders:
        path = find_ibuf_metrics_file(folder)
        t_rel, series = parse_ibuf_metrics_csv(path, metrics)
        name = os.path.basename(os.path.abspath(folder))
        r = RunSeries(name=name, t_rel=t_rel, series=series, source_file=path)
        dur = _read_manifest_duration(folder)
        r = _clamp_run_series_to_duration(r, dur)
        runs.append(r)
    return runs


def find_ibuf_status_file(folder: str) -> str:
    candidates = sorted(
        glob.glob(os.path.join(folder, IBUF_STATUS_PATTERN)), key=os.path.getmtime
    )
    if not candidates:
        raise FileNotFoundError(
            f"No '{IBUF_STATUS_PATTERN}' file found in {folder!r}."
        )
    return candidates[-1]


def parse_ibuf_status_csv(path: str, fields: List[str]) -> Tuple[List[float], Dict[str, List[int]]]:
    rows: List[Tuple[datetime, Dict[str, int]]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                ts = datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            vals: Dict[str, int] = {}
            for fld in fields:
                v = row.get(fld)
                if v is not None and v != "":
                    try:
                        vals[fld] = int(v)
                    except ValueError:
                        pass
            rows.append((ts, vals))
    if not rows:
        return ([], {f: [] for f in fields})
    rows.sort(key=lambda x: x[0])
    t0 = rows[0][0]
    t_rel = [(ts - t0).total_seconds() for ts, _ in rows]
    series: Dict[str, List[int]] = {f: [] for f in fields}
    last_vals: Dict[str, int] = {f: 0 for f in fields}
    for _, vals in rows:
        for f in fields:
            if f in vals:
                last_vals[f] = vals[f]
            series[f].append(last_vals[f])
    return t_rel, series


def load_ibuf_status_runs(folders: List[str], fields: List[str]) -> List[RunSeries]:
    runs: List[RunSeries] = []
    for folder in folders:
        path = find_ibuf_status_file(folder)
        t_rel, series = parse_ibuf_status_csv(path, fields)
        name = os.path.basename(os.path.abspath(folder))
        r = RunSeries(name=name, t_rel=t_rel, series=series, source_file=path)
        dur = _read_manifest_duration(folder)
        r = _clamp_run_series_to_duration(r, dur)
        runs.append(r)
    return runs


def plot_timeseries(
    runs: List[RunSeries],
    metrics: List[str],
    out_path: str,
    warmup_skip: int,
    warmup_seconds: float,
    bucket_seconds: float,
    show_raw: bool = False,
) -> None:
    """Plot per-interval deltas for basic InnoDB row counters."""
    ncols = 1
    nrows = 3
    fig, axes = plt.subplots(nrows, ncols, figsize=(11, 8.5), sharex=True)
    # Consistent colors & linestyles per run name
    palette = plt.get_cmap("tab10")
    run_names = [r.name for r in runs]
    color_map = {name: palette(i % 10) for i, name in enumerate(sorted(set(run_names)))}
    linestyles = ["-", "--", ":", "-."]
    style_map = {name: linestyles[i % len(linestyles)] for i, name in enumerate(sorted(set(run_names)))}
    if nrows == 1:
        axes = [axes]

    titles = {
        "Innodb_rows_inserted": "Rows Inserted",
        "Innodb_rows_updated": "Rows Updated",
        "Innodb_rows_deleted": "Rows Deleted",
    }

    prepared: Dict[str, Dict[str, Tuple[List[float], List[float]]]] = {}

    for r in runs:
        # Precompute deltas once per run
        series_delta = compute_deltas(r.series)

        # Determine the first index at or after warmup_seconds
        idx_by_time = 0
        while idx_by_time < len(r.t_rel) and r.t_rel[idx_by_time] < warmup_seconds:
            idx_by_time += 1
        start = max(warmup_skip, idx_by_time)

        for m in metrics:
            t_vals_sec = r.t_rel[start:]
            y_vals = series_delta[m][start:]
            if not t_vals_sec or not y_vals:
                continue

            t_vals_sec, y_vals = _bucketize_series(t_vals_sec, y_vals, bucket_seconds)
            if not t_vals_sec or not y_vals:
                continue

            if len(t_vals_sec) >= 2:
                dt = max(1.0, t_vals_sec[1] - t_vals_sec[0])
                window = max(1, int(round(60.0 / dt)))  # ≈ one minute smoothing
            else:
                window = 1

            t_vals_hr = [t / 3600.0 for t in t_vals_sec]

            if window > 1 and len(y_vals) > window:
                kernel = np.ones(window) / float(window)
                y_smooth = np.convolve(y_vals, kernel, mode="valid").tolist()
                t_smooth = t_vals_hr[window - 1:]
            else:
                y_smooth = y_vals
                t_smooth = t_vals_hr
            y_smooth = _mask_isolated_zeros([float(v) for v in y_smooth])

            prepared.setdefault(m, {})[r.name] = (t_smooth, y_smooth)

    y_ranges: Dict[str, Tuple[float, float]] = {}
    for m in metrics:
        all_y: List[float] = []
        for _, (_, y_smooth) in prepared.get(m, {}).items():
            # filter zeros/negatives (counter resets) to avoid dragging the axis down
            all_y.extend([v for v in y_smooth if v > 0])
        if all_y:
            lo_q = float(np.percentile(all_y, 1))
            hi_q = float(np.percentile(all_y, 99))
            max_y = float(max(all_y))
            yhi_base = max(hi_q, max_y)
            span = max(1.0, yhi_base - max(0.0, lo_q))
            pad = max(1.0, 0.05 * span)
            y_ranges[m] = (0.0, yhi_base + pad)
        else:
            y_ranges[m] = (0.0, 1.0)

    # --- Plot each metric with time shown in HOURS ---
    for idx, m in enumerate(metrics):
        ax = axes[idx]
        ax.set_facecolor("white")

        for r in runs:
            if r.name not in prepared.get(m, {}):
                continue
            t_smooth, y_smooth = prepared[m][r.name]

            # Optional raw line (can look “filled” for dense runs)
            if show_raw:
                # Plot a faint unsmoothed line for context
                _, raw_y = prepared[m][r.name]  # we don't keep raw separately here
            ax.plot(
                t_smooth, y_smooth, linewidth=2.0, alpha=0.95,
                color=color_map[r.name], linestyle=style_map[r.name],
                label=r.name, zorder=3
            )

        # Apply robust Y‑axis range and formatting
        _, yhi = y_ranges[m]
        ax.set_ylim(0, yhi)
        ax.set_autoscale_on(False)
        ax.set_ylabel(titles.get(m, m))
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=6))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))

        ax.legend(loc="upper right")

    axes[-1].set_xlabel("Time since start (hours)")
    fig.suptitle("InnoDB per-interval deltas over time (relative to each run start)")
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def write_timeseries_csv(
    runs: List[RunSeries],
    metrics: List[str],
    csv_path: str,
    warmup_skip: int,
    warmup_seconds: float,
    bucket_seconds: float,
) -> None:
    """Write bucketed per-interval deltas to CSV."""
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "metric", "t_seconds", "value"])
        for r in runs:
            series_delta = compute_deltas(r.series)
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(min(warmup_skip, n), idx_by_time)

            for m in metrics:
                t_vals_sec = r.t_rel[start:]
                y_vals = series_delta.get(m, [])[start:]
                if not t_vals_sec or not y_vals:
                    continue
                t_vals_sec, y_vals = _bucketize_series(t_vals_sec, y_vals, bucket_seconds)
                y_vals = _mask_isolated_zeros([float(v) for v in y_vals])
                for t_sec, val in zip(t_vals_sec, y_vals):
                    if math.isnan(val):
                        continue
                    w.writerow([r.name, m, f"{t_sec:.3f}", int(val)])


def plot_qps_tps(
    runs: List[RunSeries],
    out_path: str,
    warmup_skip: int,
    warmup_seconds: float,
    bucket_seconds: float,
) -> None:
    metrics = {
        "Queries": "Queries per second",
        "Transactions": "Transactions per second",
    }
    nrows = len(metrics)
    # Use a constant figure size so every generated plot shares the same
    # dimensions, avoiding discrepancies such as the smaller per-interval
    # graph.
    fig, axes = plt.subplots(nrows, 1, figsize=(11, 8.5), sharex=True)
    if nrows == 1:
        axes = [axes]
    palette = plt.get_cmap("tab10")
    run_names = [r.name for r in runs]
    color_map = {name: palette(i % 10) for i, name in enumerate(sorted(set(run_names)))}
    linestyles = ["-", "--", ":", "-."]
    style_map = {name: linestyles[i % len(linestyles)] for i, name in enumerate(sorted(set(run_names)))}

    prepared: Dict[str, Dict[str, Tuple[List[float], List[float]]]] = {m: {} for m in metrics}
    debug_info: Dict[str, object] = {
        "bucket_seconds": bucket_seconds,
        "warmup_seconds": warmup_seconds,
        "warmup_skip": warmup_skip,
        "runs": [r.name for r in runs],
        "per_run": {},
        "y_axis": {},
    }
    # Collect per-run stats for Queries in the first 120s after warmup
    qps_stats: Dict[str, Tuple[float, float, float]] = {}
    for r in runs:
        deltas = compute_deltas(r.series)
        n = len(r.t_rel)
        idx_by_time = 0
        while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
            idx_by_time += 1
        start = max(warmup_skip, idx_by_time)
        t_vals_sec = r.t_rel[start:]
        qps = deltas.get("Queries", [])[start:]
        tps_commit = deltas.get("Com_commit", [])[start:]
        tps_rollback = deltas.get("Com_rollback", [])[start:]
        tps = [c + r for c, r in zip(tps_commit, tps_rollback)]
        t_rel = t_vals_sec[:]
        # Use real elapsed time for per-second rates
        t_q, q_b = _bucketize_rates_from_deltas(t_rel, qps, bucket_seconds)
        t_t, t_b = _bucketize_rates_from_deltas(t_rel, tps, bucket_seconds)
        t_common, q_b, t_b = _align_two_series(t_q, q_b, t_t, t_b)
        if not t_common:
            continue
        # Compute stats for first 120s after warmup
        try:
            q_subset = [v for t, v in zip(t_common, q_b) if t <= 120.0 and v > 0]
            if q_subset:
                qps_stats[r.name] = (
                    float(min(q_subset)),
                    float(np.mean(q_subset)),
                    float(max(q_subset)),
                )
        except Exception:
            pass
        t_hr = [t / 3600.0 for t in t_common]
        # Disable smoothing for QPS/TPS when using small buckets (e.g., 10s)
        # to avoid attenuating peaks and causing misleading scales.
        window = 1
        if window > 1 and len(q_b) > window:
            kernel = np.ones(window) / float(window)
            qps_s = np.convolve(q_b, kernel, mode="valid").tolist()
            tps_s = np.convolve(t_b, kernel, mode="valid").tolist()
            t_smooth = t_hr[window - 1:]
        else:
            qps_s = q_b
            tps_s = t_b
            t_smooth = t_hr
        # Do not mask zeros to NaN for QPS/TPS; instead we'll drop non-positive
        # samples at plot time to avoid breaking the line into invisible singletons.
        # qps_s and tps_s remain raw per-bucket rates here.
        if t_smooth:
            prepared["Queries"][r.name] = (t_smooth, qps_s)
            prepared["Transactions"][r.name] = (t_smooth, tps_s)

        # Collect per-run debug sample points (first 6 QPS values after warmup)
        try:
            debug_info["per_run"][r.name] = {
                "points": len(q_b),
                "window": window,
                "first_qps_samples": [round(float(x), 2) for x in q_b[:6]],
                "max_qps": round(float(max(q_b)) if q_b else 0.0, 2),
            }
        except Exception:
            pass

    for ax, m in zip(axes, metrics.keys()):
        ax.set_facecolor("white")
        all_y = []
        for _, (_, y) in prepared[m].items():
            all_y.extend([v for v in y if v > 0])
        if all_y:
            lo_q = float(np.percentile(all_y, 1))
            hi_q = float(np.percentile(all_y, 99))
            # Ensure we include true maxima so peaks are visible on the graph
            max_y = float(max(all_y))
            yhi_base = max(hi_q, max_y)
            span = max(1.0, yhi_base - max(0.0, lo_q))
            pad = max(1.0, 0.05 * span)
            ylo, yhi = 0.0, yhi_base + pad
            # Debug capture for y-axis
            debug_info["y_axis"][m] = {
                "lo_q": round(lo_q, 3),
                "hi_q": round(hi_q, 3),
                "max_y": round(max_y, 3),
                "yhi": round(yhi, 3),
            }
        else:
            ylo, yhi = 0.0, 1.0
            debug_info["y_axis"][m] = {"yhi": 1.0, "note": "no data"}
        for name, (t_hr, y_vals) in prepared[m].items():
            # Filter out non-positive and NaN samples so lines connect across gaps
            t_plot: List[float] = []
            y_plot: List[float] = []
            for tt, yy in zip(t_hr, y_vals):
                try:
                    yv = float(yy)
                except Exception:
                    continue
                if not (yv > 0 and not math.isnan(yv) and math.isfinite(yv)):
                    continue
                t_plot.append(tt)
                y_plot.append(yv)
            if t_plot:
                ax.plot(
                    t_plot, y_plot,
                    linewidth=2.0, alpha=0.95,
                    color=color_map[name], linestyle=style_map[name], label=name,
                )
        ax.set_ylim(0, yhi)
        ax.set_ylabel(metrics[m])
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.legend(loc="upper right")

    axes[-1].set_xlabel("Time since start (hours)")
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.suptitle("QPS/TPS over time")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    # Emit debug sidecar JSON next to the PNG
    try:
        import json as _json
        dbg_path = out_path.replace(".png", ".debug.json")
        with open(dbg_path, "w", encoding="utf-8") as fh:
            _json.dump(debug_info, fh, indent=2)
        # Also print a one-line summary to stdout
        q_axis = debug_info.get("y_axis", {}).get("Queries", {})
        print(f"[qps_tps] bucket={bucket_seconds}s y_max={q_axis.get('yhi')} max_qps_per_run={ {k:v.get('max_qps') for k,v in debug_info.get('per_run',{}).items()} }")
    except Exception as e:
        print(f"[qps_tps] debug emit failed: {e}")


def write_qps_csv(
    runs: List[RunSeries],
    csv_path: str,
    warmup_skip: int,
    warmup_seconds: float,
    bucket_seconds: float,
) -> None:
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "metric", "t_seconds", "value"])
        deltas = {r.name: compute_deltas(r.series) for r in runs}
        for r in runs:
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(warmup_skip, idx_by_time)
            # Build per-interval deltas
            d = deltas.get(r.name, {})
            q_list = d.get("Queries", [])
            c_list = d.get("Com_commit", [])
            rb_list = d.get("Com_rollback", [])
            tps_list = [c + rb for c, rb in zip(c_list, rb_list)]
            t_rel = r.t_rel[start:]
            q_vals = q_list[start:]
            tps_vals = tps_list[start:]

            # Bucketize using real elapsed-time based rates
            t_q, q_b = _bucketize_rates_from_deltas(t_rel, q_vals, bucket_seconds)
            t_t, t_b = _bucketize_rates_from_deltas(t_rel, tps_vals, bucket_seconds)
            # Align on common timestamps
            t_common, q_b, t_b = _align_two_series(t_q, q_b, t_t, t_b)

            for t_sec, qv, tv in zip(t_common, q_b, t_b):
                w.writerow([r.name, "Queries", f"{t_sec:.3f}", int(round(qv))])
                w.writerow([r.name, "Transactions", f"{t_sec:.3f}", int(round(tv))])


def plot_ibuf_series(
    runs_metrics: List[RunSeries],
    metric_names: List[str],
    runs_status: List[RunSeries],
    status_fields: List[str],
    out_path: str,
    warmup_skip: int,
    warmup_seconds: float,
) -> None:
    total = len(metric_names) + len(status_fields)
    if total == 0:
        return
    # Keep figure size consistent with other plots so that all exported
    # images share identical dimensions.
    fig, axes = plt.subplots(total, 1, figsize=(11, 8.5), sharex=False)
    if total == 1:
        axes = [axes]
    palette = plt.get_cmap("tab10")
    run_names = {r.name for r in runs_metrics + runs_status}
    color_map = {name: palette(i % 10) for i, name in enumerate(sorted(run_names))}
    linestyles = ["-", "--", ":", "-."]
    style_map = {name: linestyles[i % len(linestyles)] for i, name in enumerate(sorted(run_names))}
    titles_metric = {
        "ibuf_merges_insert": "Ibuf Merges (Insert)",
        "ibuf_merges_delete": "Ibuf Merges (Delete)",
        "ibuf_merges_delete_mark": "Ibuf Merges (Delete Mark)",
        "ibuf_discard_insert": "Ibuf Discards (Insert)",
        "ibuf_discard_delete": "Ibuf Discards (Delete)",
        "ibuf_discard_delete_mark": "Ibuf Discards (Delete Mark)",
    }
    titles_status = {
        "size": "Ibuf Size (pages)",
        "seg_size": "Segment Size",
        "free_list_len": "Free List Length",
    }

    idx = 0
    for m in metric_names:
        ax = axes[idx]
        ax.set_facecolor("white")
        prepared: Dict[str, Tuple[List[float], List[float]]] = {}
        for r in runs_metrics:
            series_delta = compute_deltas(r.series)
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(warmup_skip, idx_by_time)
            t_vals_sec = r.t_rel[start:]
            y_vals = series_delta[m][start:] if m in series_delta else []
            if not t_vals_sec or not y_vals:
                continue
            if len(t_vals_sec) >= 2:
                dt = max(1.0, t_vals_sec[1] - t_vals_sec[0])
                window = max(1, int(round(60.0 / dt)))
            else:
                window = 1
            t_hr = [t / 3600.0 for t in t_vals_sec]
            if window > 1 and len(y_vals) > window:
                kernel = np.ones(window) / float(window)
                y_smooth = np.convolve(y_vals, kernel, mode="valid").tolist()
                t_smooth = t_hr[window - 1:]
            else:
                y_smooth = y_vals
                t_smooth = t_hr
            prepared[r.name] = (t_smooth, y_smooth)
        all_y = []
        for _, (_, y) in prepared.items():
            all_y.extend([v for v in y if v > 0])
        if all_y:
            lo_q = float(np.percentile(all_y, 1))
            hi_q = float(np.percentile(all_y, 99))
            if hi_q <= lo_q:
                hi_q = lo_q + 1.0
            span = hi_q - lo_q
            pad = max(1.0, 0.05 * span)
            ylo, yhi = max(0.0, lo_q - pad), hi_q + pad
        else:
            ylo, yhi = 0.0, 1.0
        for name, (t_smooth, y_smooth) in prepared.items():
            ax.plot(t_smooth, y_smooth, linewidth=2.0, alpha=0.95, color=color_map[name], linestyle=style_map[name], label=name)
        ax.set_ylim(ylo, yhi)
        ax.set_ylabel(titles_metric.get(m, m))
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.legend(loc="upper right")
        idx += 1

    for field in status_fields:
        ax = axes[idx]
        ax.set_facecolor("white")
        for r in runs_status:
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(warmup_skip, idx_by_time)
            t_vals_sec = r.t_rel[start:]
            y_vals = r.series.get(field, [])[start:]
            if not t_vals_sec or not y_vals:
                continue
            t_hr = [t / 3600.0 for t in t_vals_sec]
            if len(t_vals_sec) >= 2:
                dt = max(1.0, t_vals_sec[1] - t_vals_sec[0])
                window = max(1, int(round(60.0 / dt)))
            else:
                window = 1
            if window > 1 and len(y_vals) > window:
                kernel = np.ones(window) / float(window)
                y_smooth = np.convolve(y_vals, kernel, mode="valid").tolist()
                t_smooth = t_hr[window - 1:]
            else:
                y_smooth = y_vals
                t_smooth = t_hr
            ax.plot(t_smooth, y_smooth, linewidth=2.0, alpha=0.95, color=color_map[r.name], linestyle=style_map[r.name], label=r.name)
        ax.set_ylabel(titles_status.get(field, field))
        ax.grid(True, linestyle="--", alpha=0.3)
        ax.legend(loc="upper right")
        idx += 1

    axes[-1].set_xlabel("Time since start (hours)")
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.suptitle("Change Buffer metrics over time")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def write_ibuf_csv(
    runs_metrics: List[RunSeries],
    metric_names: List[str],
    runs_status: List[RunSeries],
    status_fields: List[str],
    csv_path: str,
    warmup_skip: int,
    warmup_seconds: float,
) -> None:
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run", "metric", "t_seconds", "value"])

        # metrics (deltas)
        deltas = {r.name: compute_deltas(r.series) for r in runs_metrics}
        for r in runs_metrics:
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(warmup_skip, idx_by_time)
            last_t = None
            for i in range(start, n):
                if last_t is not None and r.t_rel[i] <= last_t:
                    continue
                for m in metric_names:
                    w.writerow([r.name, m, f"{r.t_rel[i]:.3f}", deltas[r.name][m][i]])
                last_t = r.t_rel[i]

        # status (absolute)
        for r in runs_status:
            n = len(r.t_rel)
            idx_by_time = 0
            while idx_by_time < n and r.t_rel[idx_by_time] < warmup_seconds:
                idx_by_time += 1
            start = max(warmup_skip, idx_by_time)
            last_t = None
            for i in range(start, n):
                if last_t is not None and r.t_rel[i] <= last_t:
                    continue
                for f_field in status_fields:
                    w.writerow([r.name, f_field, f"{r.t_rel[i]:.3f}", r.series.get(f_field, [])[i]])
                last_t = r.t_rel[i]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot time-series of InnoDB cumulative counters (aligned to relative time).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--folders",
        nargs="+",
        required=True,
        help="Two or more folders; each must contain a global_status_*.log",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=DEFAULT_METRICS,
        help="Status variables to track (exact names).",
    )
    parser.add_argument(
        "--status-interval",
        type=int,
        default=10,
        help="Seconds between SHOW GLOBAL STATUS snapshots if timestamps are absent.",
    )
    parser.add_argument(
        "--warmup-skip",
        type=int,
        default=5,
        help="Number of initial samples to skip as warmup (removes the first jump/spike).",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=60.0,
        help="Drop the first N seconds from each run (removes initial warmup ramp).",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help=(
            "Output directory for all generated files (e.g., results_total/test1). "
            "The directory will be created if needed and overrides the individual "
            "--out/--csv/etc. options."
        ),
    )
    parser.add_argument(
        "--out",
        default="timeseries.png",
        help="Path of the output image (PNG).",
    )
    parser.add_argument(
        "--csv",
        default="timeseries.csv",
        help="CSV path for the time-series (long form).",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Overlay the unsmoothed raw line (can appear ‘filled’ due to very dense points)."
    )
    parser.add_argument(
        "--qps-out",
        default="qps_tps.png",
        help="Output PNG for QPS/TPS metrics.",
    )
    parser.add_argument(
        "--qps-csv",
        default="qps_tps.csv",
        help="CSV path for QPS/TPS metrics.",
    )
    parser.add_argument(
        "--ibuf-merges-out",
        default="ibuf_merges.png",
        help="Output PNG for change buffer merge metrics.",
    )
    parser.add_argument(
        "--ibuf-merges-csv",
        default="ibuf_merges.csv",
        help="CSV path for change buffer merge metrics.",
    )
    parser.add_argument(
        "--ibuf-discards-out",
        default="ibuf_discards.png",
        help="Output PNG for change buffer discard metrics.",
    )
    parser.add_argument(
        "--ibuf-discards-csv",
        default="ibuf_discards.csv",
        help="CSV path for change buffer discard metrics.",
    )
    parser.add_argument(
        "--ibuf-size-out",
        default="ibuf_size.png",
        help="Output PNG for change buffer size metrics.",
    )
    parser.add_argument(
        "--ibuf-size-csv",
        default="ibuf_size.csv",
        help="CSV path for change buffer size metrics.",
    )
    parser.add_argument(
        "--bucket-seconds",
        type=float,
        default=60.0,
        help=(
            "Aggregate deltas into this many seconds per point "
            "(reduces noise/zeros) for both time-series and QPS/TPS graphs."
        ),
    )
    args = parser.parse_args()

    if args.prefix:
        base = args.prefix
        os.makedirs(base, exist_ok=True)
        args.out = os.path.join(base, "timeseries.png")
        args.csv = os.path.join(base, "timeseries.csv")
        args.qps_out = os.path.join(base, "qps_tps.png")
        args.qps_csv = os.path.join(base, "qps_tps.csv")
        args.ibuf_merges_out = os.path.join(base, "ibuf_merges.png")
        args.ibuf_merges_csv = os.path.join(base, "ibuf_merges.csv")
        args.ibuf_discards_out = os.path.join(base, "ibuf_discards.png")
        args.ibuf_discards_csv = os.path.join(base, "ibuf_discards.csv")
        args.ibuf_size_out = os.path.join(base, "ibuf_size.png")
        args.ibuf_size_csv = os.path.join(base, "ibuf_size.csv")

    if len(args.folders) < 2:
        raise SystemExit("Please provide at least two folders to compare.")

    all_metrics = list(dict.fromkeys(args.metrics + QPS_TPS_METRICS))
    runs = load_runs(args.folders, all_metrics, args.status_interval)
    ibuf_metric_runs = load_ibuf_metric_runs(
        args.folders, list(dict.fromkeys(IBUF_MERGE_METRICS + IBUF_DISCARD_METRICS))
    )
    ibuf_status_runs = load_ibuf_status_runs(args.folders, IBUF_SIZE_FIELDS)

    # Summarize deltas per run to stdout
    print("== Summary (first -> last deltas) ==")
    for r in runs:
        print(f"Run: {r.name}  (source: {r.source_file})")
        series_delta = compute_deltas(r.series)
        n = len(r.t_rel)
        idx_by_time = 0
        while idx_by_time < n and r.t_rel[idx_by_time] < args.warmup_seconds:
            idx_by_time += 1
        start = max(min(args.warmup_skip, n), idx_by_time)
        for m in args.metrics:
            total = sum(series_delta.get(m, [])[start:])
            print(f"  {m:>22}: {total:>12,}  (from t&gt;={args.warmup_seconds:.0f}s)")
        q_total = sum(series_delta.get("Queries", [])[start:])
        t_total = 0
        cc = series_delta.get("Com_commit", [])
        cr = series_delta.get("Com_rollback", [])
        for i in range(start, min(len(cc), len(cr))):
            t_total += cc[i] + cr[i]
        print(f"  {'Queries':>22}: {q_total:>12,}  (from t&gt;={args.warmup_seconds:.0f}s)")
        print(f"  {'Transactions':>22}: {t_total:>12,}  (from t&gt;={args.warmup_seconds:.0f}s)")
        print()

    plot_timeseries(
        runs, args.metrics, args.out,
        args.warmup_skip, args.warmup_seconds, args.bucket_seconds,
        show_raw=args.show_raw,
    )
    write_timeseries_csv(
        runs, args.metrics, args.csv,
        args.warmup_skip, args.warmup_seconds, args.bucket_seconds,
    )
    plot_qps_tps(runs, args.qps_out, args.warmup_skip, args.warmup_seconds, args.bucket_seconds)
    write_qps_csv(runs, args.qps_csv, args.warmup_skip, args.warmup_seconds, args.bucket_seconds)

    print(f"Saved time-series graph to: {os.path.abspath(args.out)}")
    print(f"Saved time-series CSV   to: {os.path.abspath(args.csv)}")
    print(f"Saved QPS/TPS graph to: {os.path.abspath(args.qps_out)}")
    print(f"Saved QPS/TPS CSV   to: {os.path.abspath(args.qps_csv)}")

    # Change buffer summaries
    print("== Change Buffer Summary ==")
    for r in ibuf_metric_runs:
        print(f"Run: {r.name}  (source: {r.source_file})")
        series_delta = compute_deltas(r.series)
        n = len(r.t_rel)
        idx_by_time = 0
        while idx_by_time < n and r.t_rel[idx_by_time] < args.warmup_seconds:
            idx_by_time += 1
        start = max(min(args.warmup_skip, n), idx_by_time)
        for m in IBUF_MERGE_METRICS + IBUF_DISCARD_METRICS:
            if m in series_delta:
                total = sum(series_delta[m][start:])
                print(f"  {m:>22}: {total:>12,}  (from t>={args.warmup_seconds:.0f}s)")
        print()

    plot_ibuf_series(
        ibuf_metric_runs,
        IBUF_MERGE_METRICS,
        [],
        [],
        args.ibuf_merges_out,
        args.warmup_skip,
        args.warmup_seconds,
    )
    write_ibuf_csv(
        ibuf_metric_runs,
        IBUF_MERGE_METRICS,
        [],
        [],
        args.ibuf_merges_csv,
        args.warmup_skip,
        args.warmup_seconds,
    )
    plot_ibuf_series(
        ibuf_metric_runs,
        IBUF_DISCARD_METRICS,
        [],
        [],
        args.ibuf_discards_out,
        args.warmup_skip,
        args.warmup_seconds,
    )
    write_ibuf_csv(
        ibuf_metric_runs,
        IBUF_DISCARD_METRICS,
        [],
        [],
        args.ibuf_discards_csv,
        args.warmup_skip,
        args.warmup_seconds,
    )
    plot_ibuf_series(
        [],
        [],
        ibuf_status_runs,
        IBUF_SIZE_FIELDS,
        args.ibuf_size_out,
        args.warmup_skip,
        args.warmup_seconds,
    )
    write_ibuf_csv(
        [],
        [],
        ibuf_status_runs,
        IBUF_SIZE_FIELDS,
        args.ibuf_size_csv,
        args.warmup_skip,
        args.warmup_seconds,
    )

    print(f"Saved change-buffer merge graph to: {os.path.abspath(args.ibuf_merges_out)}")
    print(f"Saved change-buffer merge CSV   to: {os.path.abspath(args.ibuf_merges_csv)}")
    print(f"Saved change-buffer discard graph to: {os.path.abspath(args.ibuf_discards_out)}")
    print(f"Saved change-buffer discard CSV   to: {os.path.abspath(args.ibuf_discards_csv)}")
    print(f"Saved change-buffer size graph to: {os.path.abspath(args.ibuf_size_out)}")
    print(f"Saved change-buffer size CSV   to: {os.path.abspath(args.ibuf_size_csv)}")


if __name__ == "__main__":
    main()
