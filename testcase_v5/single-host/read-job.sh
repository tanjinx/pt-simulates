#!/usr/bin/env bash
set -euo pipefail

: "${MYSQL_DATABASE:?set MYSQL_DATABASE}"
: "${MYSQL_TABLE:?set MYSQL_TABLE}"
: "${MYSQL_TENANT_ID:?set MYSQL_TENANT_ID}"
: "${MYSQL_ID_MIN:?set MYSQL_ID_MIN}"

MYSQL_BIN="${MYSQL_BIN:-mysql}"
READ_WORKERS="${READ_WORKERS:-1}"
READ_BATCH_SIZE="${READ_BATCH_SIZE:-10}"
JOB_ITERATIONS="${JOB_ITERATIONS:-1}"

[[ "${MYSQL_DATABASE}" =~ ^[A-Za-z0-9_]+$ ]]
[[ "${MYSQL_TABLE}" =~ ^[A-Za-z0-9_]+$ ]]
[[ "${MYSQL_TENANT_ID}" =~ ^[0-9]+$ ]]
[[ "${MYSQL_ID_MIN}" =~ ^[0-9]+$ ]]
[[ "${READ_WORKERS}" =~ ^[1-9][0-9]*$ ]]
[[ "${READ_BATCH_SIZE}" =~ ^[1-9][0-9]*$ ]]
[[ "${JOB_ITERATIONS}" =~ ^[1-9][0-9]*$ ]]

mysql_args=(--batch --skip-column-names "${MYSQL_DATABASE}")
[[ -n "${MYSQL_DEFAULTS_FILE:-}" ]] && mysql_args=("--defaults-extra-file=${MYSQL_DEFAULTS_FILE}" "${mysql_args[@]}")
[[ -n "${MYSQL_SOCKET:-}" ]] && mysql_args+=("--socket=${MYSQL_SOCKET}")

read_worker() {
  local iteration
  for ((iteration = 1; iteration <= JOB_ITERATIONS; iteration++)); do
    "${MYSQL_BIN}" "${mysql_args[@]}" >/dev/null <<SQL
SET SESSION transaction_isolation = 'REPEATABLE-READ';
SELECT *
FROM \`${MYSQL_DATABASE}\`.\`${MYSQL_TABLE}\` IGNORE INDEX (\`PRIMARY\`)
WHERE tenant_id = ${MYSQL_TENANT_ID}
  AND id > ${MYSQL_ID_MIN}
ORDER BY id ASC
LIMIT ${READ_BATCH_SIZE};
SQL
  done
}

pids=()
for ((worker = 1; worker <= READ_WORKERS; worker++)); do
  read_worker &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "${pid}"
done
