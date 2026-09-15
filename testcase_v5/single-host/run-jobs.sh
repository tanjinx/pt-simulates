#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${ROOT}/write-job.sh" &
write_pid=$!
"${ROOT}/read-job.sh" &
read_pid=$!

stop_jobs() {
  kill "${write_pid}" "${read_pid}" 2>/dev/null || true
}
trap stop_jobs INT TERM

status=0
wait "${write_pid}" || status=$?
wait "${read_pid}" || status=$?
exit "${status}"
