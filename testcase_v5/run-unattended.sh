#!/usr/bin/env bash
#
# run-unattended.sh — launch-and-leave reproduction soak for the
# mysqld SIGSEGV. Supervises tc-repro, watches the target mysqld for a signal-11
# crash / new core dump, captures the result, and self-reports to files so no
# live terminal session is needed. Modeled on the the reproduction
# campaign rig.
#
# This script manages a HOST session only. It never changes MySQL settings and
# never changes system-wide core configuration silently — it CHECKS them and
# prints what to fix. The only thing it sets is `ulimit -c unlimited` for its
# own child process (process-scoped, safe).
#
# ─────────────────────────────────────────────────────────────────────────────
# READ FIRST — core dumps contain live data:
#   If mysqld crashes with innodb_buffer_pool_in_core_file=ON (recommended, so
#   the core carries the buffer pool that every prior application core LACKED), the
#   resulting core file contains real rows from your `files` table. Treat any
#   captured core as production data and handle/transfer it under your data-
#   sharing agreement.
# ─────────────────────────────────────────────────────────────────────────────
#
# Usage:
#   TC_CONFIG=./tc_config.search_mix.json \
#   MYSQL_ERROR_LOG=/var/log/mysql/error.log \
#   CORE_DIR=/var/lib/mysql/cores \
#   ./run-unattended.sh
#
# Environment (all overridable):
#   TC_BIN           path to tc-repro binary        (default: ./bin/tc-repro-linux-amd64,
#                                                   falls back to ./build/tc-repro)
#   TC_CONFIG        harness config               (default: ./tc_config.search_mix.json)
#   TC_VERB          init | run | start           (default: run — seed first with `init`)
#   MYSQL_ERROR_LOG  mysqld error log to watch    (REQUIRED for crash detection)
#   CORE_DIR         directory where cores land    (REQUIRED for crash detection)
#   WORKDIR          runner state dir             (default: ./run-state)
#   POLL_SECONDS     crash-watch interval         (default: 15)
#   BACKSTOP_GRACE   kill tc-repro this many seconds past its own deadline (default: 300)
#
# The harness's own duration is safety.max_runtime_seconds in TC_CONFIG. Set it
# to your desired soak length (our internal best-shot runs used 72h = 259200).
set -euo pipefail

TC_BIN="${TC_BIN:-}"
if [[ -z "${TC_BIN}" ]]; then
  if [[ -x ./bin/tc-repro-linux-amd64 ]]; then TC_BIN=./bin/tc-repro-linux-amd64
  elif [[ -x ./build/tc-repro ]]; then TC_BIN=./build/tc-repro
  else echo "FATAL: no tc-repro binary found; set TC_BIN or run 'make build-linux'." >&2; exit 2; fi
fi
TC_CONFIG="${TC_CONFIG:-./tc_config.search_mix.json}"
TC_VERB="${TC_VERB:-run}"
WORKDIR="${WORKDIR:-./run-state}"
POLL_SECONDS="${POLL_SECONDS:-15}"
BACKSTOP_GRACE="${BACKSTOP_GRACE:-300}"
MYSQL_ERROR_LOG="${MYSQL_ERROR_LOG:-}"
CORE_DIR="${CORE_DIR:-}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "$(ts) $*" | tee -a "${WORKDIR}/runner.log"; }

[[ -x "${TC_BIN}" ]] || { echo "FATAL: TC_BIN not executable: ${TC_BIN}" >&2; exit 2; }
[[ -f "${TC_CONFIG}" ]] || { echo "FATAL: TC_CONFIG not found: ${TC_CONFIG}" >&2; exit 2; }
mkdir -p "${WORKDIR}"
rm -f "${WORKDIR}/CRASH_DETECTED" "${WORKDIR}/STATUS"

log "=== runner start ==="
log "bin=${TC_BIN} config=${TC_CONFIG} verb=${TC_VERB} workdir=${WORKDIR}"

# ── Pre-flight: report (do not change) crash-capture readiness ───────────────
log "--- crash-capture readiness (this rig changes none of these for you) ---"
log "kernel.core_pattern = $(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo '?')"
log "process core limit   = $(ulimit -c)  (this runner sets 'unlimited' for tc-repro's child)"
if [[ -z "${MYSQL_ERROR_LOG}" || -z "${CORE_DIR}" ]]; then
  log "WARN: MYSQL_ERROR_LOG and/or CORE_DIR unset — crash DETECTION is disabled."
  log "WARN: the soak will still run, but a crash will not be auto-captured/reported."
  log "WARN: to enable detection, set both to your mysqld's error log and core directory."
fi
[[ -n "${CORE_DIR}" && ! -d "${CORE_DIR}" ]] && log "WARN: CORE_DIR '${CORE_DIR}' does not exist yet."
log "REMINDER: on the REPLICA, set innodb_buffer_pool_in_core_file=ON so a crash"
log "          core carries the buffer pool (the artifact prior application cores lacked)."
log "REMINDER: a captured core contains live table data — handle under your DSA."

# Baseline core snapshot so we only react to NEW cores.
baseline_cores=""
if [[ -n "${CORE_DIR}" && -d "${CORE_DIR}" ]]; then
  baseline_cores="$(ls -1 "${CORE_DIR}" 2>/dev/null | sort || true)"
fi
err_start_size=0
[[ -n "${MYSQL_ERROR_LOG}" && -f "${MYSQL_ERROR_LOG}" ]] && err_start_size="$(wc -c < "${MYSQL_ERROR_LOG}")"

# ── Launch tc-repro (its own max_runtime_seconds bounds the soak) ───────────────
( ulimit -c unlimited; exec "${TC_BIN}" --config "${TC_CONFIG}" "${TC_VERB}" ) \
  >> "${WORKDIR}/tc-repro.out" 2>&1 &
TC_PID=$!
log "tc-repro launched pid=${TC_PID}; output -> ${WORKDIR}/tc-repro.out"

start_epoch=$(date -u +%s)

crash_capture() {
  local reason="$1" detail="$2"
  {
    echo "crash_detected_at=$(ts)"
    echo "reason=${reason}"
    echo "detail=${detail}"
    [[ -n "${CORE_DIR}" ]] && echo "new_cores=$(comm -13 <(echo "${baseline_cores}") <(ls -1 "${CORE_DIR}" 2>/dev/null | sort) | tr '\n' ' ')"
  } > "${WORKDIR}/CRASH_DETECTED"
  log "!!! CRASH DETECTED (${reason}) — see ${WORKDIR}/CRASH_DETECTED"
  log "Next: analyze the core with GDB against the matching mysqld build; verify the"
  log "signature (signal 11, pthread_self, btr_cur_search_to_nth_level, CR2≈RIP-flipped-bit)."
}

# ── Supervise: poll for a crash; exit when tc-repro exits ──────────────────────
while kill -0 "${TC_PID}" 2>/dev/null; do
  elapsed=$(( $(date -u +%s) - start_epoch ))
  cores_now="-"; [[ -n "${CORE_DIR}" && -d "${CORE_DIR}" ]] && cores_now="$(ls -1 "${CORE_DIR}" 2>/dev/null | wc -l | tr -d ' ')"
  echo "$(ts) elapsed=${elapsed}s tc_pid=${TC_PID} alive=yes cores=${cores_now}" > "${WORKDIR}/STATUS"

  # (a) new core file?
  if [[ -n "${CORE_DIR}" && -d "${CORE_DIR}" ]]; then
    new="$(comm -13 <(echo "${baseline_cores}") <(ls -1 "${CORE_DIR}" 2>/dev/null | sort) || true)"
    if [[ -n "${new}" ]]; then crash_capture "new_core" "${new}"; break; fi
  fi
  # (b) signal 11 in the error log since we started?
  if [[ -n "${MYSQL_ERROR_LOG}" && -f "${MYSQL_ERROR_LOG}" ]]; then
    cur_size="$(wc -c < "${MYSQL_ERROR_LOG}")"
    if (( cur_size > err_start_size )); then
      if tail -c "+$((err_start_size + 1))" "${MYSQL_ERROR_LOG}" | grep -qiE 'got signal 11|signal 11|SIGSEGV'; then
        crash_capture "error_log_signal11" "${MYSQL_ERROR_LOG}"; break
      fi
    fi
  fi
  sleep "${POLL_SECONDS}"
done

# If tc-repro is still alive here, the loop broke on a crash — stop it.
if kill -0 "${TC_PID}" 2>/dev/null; then
  log "stopping tc-repro (pid=${TC_PID}) after crash detection"
  kill "${TC_PID}" 2>/dev/null || true
  sleep 5; kill -9 "${TC_PID}" 2>/dev/null || true
fi
wait "${TC_PID}" 2>/dev/null || true

if [[ -f "${WORKDIR}/CRASH_DETECTED" ]]; then
  log "=== runner end: CRASH captured. Preserve the core + ${WORKDIR}/ artifacts. ==="
  exit 42
fi
log "=== runner end: tc-repro exited, NO crash detected. ==="
log "Per the investigation: a null run is a MEASURED HAZARD CEILING + an armed"
log "capture rig — NOT evidence the fleet is unaffected. See README.md §Honest framing."
exit 0
