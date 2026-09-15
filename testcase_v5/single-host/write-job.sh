#!/usr/bin/env bash
set -euo pipefail

: "${MYSQL_DATABASE:?set MYSQL_DATABASE}"
: "${MYSQL_TABLE:?set MYSQL_TABLE}"
: "${MYSQL_TENANT_ID:?set MYSQL_TENANT_ID}"
: "${MYSQL_ID_MIN:?set MYSQL_ID_MIN}"
: "${MYSQL_ID_MAX:?set MYSQL_ID_MAX}"

MYSQL_BIN="${MYSQL_BIN:-mysql}"
WRITE_WORKERS="${WRITE_WORKERS:-1}"
JOB_ITERATIONS="${JOB_ITERATIONS:-1}"

[[ "${MYSQL_DATABASE}" =~ ^[A-Za-z0-9_]+$ ]]
[[ "${MYSQL_TABLE}" =~ ^[A-Za-z0-9_]+$ ]]
[[ "${MYSQL_TENANT_ID}" =~ ^[0-9]+$ ]]
[[ "${MYSQL_ID_MIN}" =~ ^[0-9]+$ ]]
[[ "${MYSQL_ID_MAX}" =~ ^[0-9]+$ ]]
[[ "${WRITE_WORKERS}" =~ ^[1-9][0-9]*$ ]]
[[ "${JOB_ITERATIONS}" =~ ^[1-9][0-9]*$ ]]
(( MYSQL_ID_MIN <= MYSQL_ID_MAX ))

mysql_args=(--batch --skip-column-names "${MYSQL_DATABASE}")
[[ -n "${MYSQL_DEFAULTS_FILE:-}" ]] && mysql_args=("--defaults-extra-file=${MYSQL_DEFAULTS_FILE}" "${mysql_args[@]}")
[[ -n "${MYSQL_SOCKET:-}" ]] && mysql_args+=("--socket=${MYSQL_SOCKET}")

write_worker() {
  local worker="$1"
  local iteration output
  for ((iteration = 1; iteration <= JOB_ITERATIONS; iteration++)); do
    output="$("${MYSQL_BIN}" "${mysql_args[@]}" <<SQL
SET SESSION sql_log_bin = 0;
SELECT @@session.sql_log_bin;
UPDATE \`${MYSQL_DATABASE}\`.\`${MYSQL_TABLE}\`
SET contents_enc = UNHEX(SHA2(CONCAT(id, ':contents:', ${worker}, ':', ${iteration}), 256)),
    contents_highlight_enc = UNHEX(SHA2(CONCAT(id, ':highlight:', ${worker}, ':', ${iteration}), 256)),
    metadata_enc = UNHEX(SHA2(CONCAT(id, ':metadata:', ${worker}, ':', ${iteration}), 256)),
    version = version + 1
WHERE tenant_id = ${MYSQL_TENANT_ID}
  AND id BETWEEN ${MYSQL_ID_MIN} AND ${MYSQL_ID_MAX}
  AND @@session.sql_log_bin = 0;
SQL
)"
    [[ "${output%%$'\n'*}" == "0" ]] || {
      echo "write worker ${worker}: sql_log_bin is not disabled" >&2
      return 1
    }
  done
}

pids=()
for ((worker = 1; worker <= WRITE_WORKERS; worker++)); do
  write_worker "${worker}" &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "${pid}"
done
