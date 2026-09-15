#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

cat > "${TMP}/mysql" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
input="$(cat)"
{
  printf '%s\n' '---'
  printf '%s\n' "${input}"
} >> "${MYSQL_CAPTURE}"
if [[ "${input}" == *"SELECT @@session.sql_log_bin"* ]]; then
  printf '0\n'
fi
EOF
chmod +x "${TMP}/mysql"

export PATH="${TMP}:${PATH}"
export MYSQL_CAPTURE="${TMP}/capture"
export MYSQL_DATABASE="repro_db"
export MYSQL_TABLE="files"
export MYSQL_TENANT_ID="42"
export MYSQL_ID_MIN="100"
export MYSQL_ID_MAX="199"
export JOB_ITERATIONS="1"
export READ_WORKERS="1"
export WRITE_WORKERS="1"

"${ROOT}/run-jobs.sh"

captured="$(cat "${MYSQL_CAPTURE}")"

[[ "${captured}" == *"SET SESSION sql_log_bin = 0"* ]]
[[ "${captured}" == *"SELECT @@session.sql_log_bin"* ]]
[[ "${captured}" == *"UPDATE \`repro_db\`.\`files\`"* ]]
[[ "${captured}" == *"tenant_id = 42"* ]]
[[ "${captured}" == *"id BETWEEN 100 AND 199"* ]]
[[ "${captured}" == *"@@session.sql_log_bin = 0"* ]]
[[ "${captured}" == *"IGNORE INDEX (\`PRIMARY\`)"* ]]

echo "single-host job tests passed"
