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
  printf 'args:'
  printf ' %s' "$@"
  printf '\n'
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
export MYSQL_HOST="127.0.0.1"
export JOB_ITERATIONS="1"
export READ_WORKERS="1"
export WRITE_WORKERS="1"

"${ROOT}/run-jobs.sh"

captured="$(cat "${MYSQL_CAPTURE}")"

grep -Fq "SET SESSION sql_log_bin = 0" <<<"${captured}"
grep -Fq -- "--host=127.0.0.1" <<<"${captured}"
grep -Fq "SELECT @@session.sql_log_bin" <<<"${captured}"
grep -Fq 'UPDATE `repro_db`.`files`' <<<"${captured}"
grep -Fq "tenant_id = 42" <<<"${captured}"
grep -Fq "id BETWEEN 100 AND 199" <<<"${captured}"
grep -Fq "@@session.sql_log_bin = 0" <<<"${captured}"
grep -Fq 'IGNORE INDEX (`PRIMARY`)' <<<"${captured}"

echo "single-host job tests passed"
