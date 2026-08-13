#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: ROUTE10_MYSQL_DEFAULTS_FILE=/path/to/client.cnf $0 export|restore SNAPSHOT.sql.gz" >&2
  exit 2
}

action="${1:-}"
snapshot="${2:-}"
defaults_file="${ROUTE10_MYSQL_DEFAULTS_FILE:-}"
database="${ROUTE10_MYSQL_DATABASE:-farm_route10}"
[[ -n "$action" && -n "$snapshot" && -n "$defaults_file" ]] || usage
[[ "$database" == "farm_route10" ]] || { echo "[ERROR] database must be exactly farm_route10" >&2; exit 2; }
[[ -r "$defaults_file" ]] || { echo "[ERROR] MySQL defaults file is not readable" >&2; exit 2; }
command -v mysql >/dev/null || { echo "[ERROR] mysql client not found" >&2; exit 2; }
command -v gzip >/dev/null || { echo "[ERROR] gzip not found" >&2; exit 2; }

mysql_args=("--defaults-extra-file=$defaults_file" --batch --skip-column-names "$database")
case "$action" in
  export)
    command -v mysqldump >/dev/null || { echo "[ERROR] mysqldump not found" >&2; exit 2; }
    [[ ! -e "$snapshot" && ! -e "$snapshot.sha256" ]] || { echo "[ERROR] snapshot output already exists" >&2; exit 2; }
    min_rows="${ROUTE10_LEDGER_MIN_ROWS:-20000000}"
    max_rows="${ROUTE10_LEDGER_MAX_ROWS:-21000000}"
    ledger_rows="$(mysql "${mysql_args[@]}" --execute 'SELECT COUNT(*) FROM economy_transactions')"
    synthetic_violations="$(mysql "${mysql_args[@]}" --execute "SELECT (SELECT COUNT(*) FROM auth_identities WHERE provider <> 'guest' OR provider_subject NOT LIKE 'route10-%') + (SELECT COUNT(*) FROM accounts WHERE profile_json IS NOT NULL OR (display_name <> '' AND display_name NOT REGEXP '^[0-9]+$')) + (SELECT COUNT(*) FROM consumer_failed_events) + (SELECT COUNT(*) FROM outbox_events WHERE status = 'DEAD')")"
    (( ledger_rows >= min_rows && ledger_rows <= max_rows )) || { echo "[ERROR] economy_transactions rows=$ledger_rows outside [$min_rows,$max_rows]" >&2; exit 2; }
    [[ "$synthetic_violations" == "0" ]] || { echo "[ERROR] snapshot source is not a clean synthetic route10 dataset" >&2; exit 2; }
    mysqldump "--defaults-extra-file=$defaults_file" --single-transaction --hex-blob --routines --triggers --events --set-gtid-purged=OFF --databases "$database" | gzip -9 >"$snapshot"
    (cd "$(dirname "$snapshot")" && sha256sum -- "$(basename "$snapshot")" >"$(basename "$snapshot").sha256")
    echo "[OK] synthetic Route 10 snapshot exported: rows=$ledger_rows file=$snapshot"
    ;;
  restore)
    [[ "${ROUTE10_RESTORE_CONFIRM:-}" == "DROP_AND_RESTORE_farm_route10" ]] || {
      echo "[ERROR] restore is destructive; set ROUTE10_RESTORE_CONFIRM=DROP_AND_RESTORE_farm_route10" >&2
      exit 2
    }
    [[ -r "$snapshot" && -r "$snapshot.sha256" ]] || { echo "[ERROR] snapshot or checksum missing" >&2; exit 2; }
    (cd "$(dirname "$snapshot")" && sha256sum --check "$(basename "$snapshot").sha256")
    mysql "--defaults-extra-file=$defaults_file" --execute "DROP DATABASE IF EXISTS farm_route10; CREATE DATABASE farm_route10 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    gzip -dc -- "$snapshot" | mysql "--defaults-extra-file=$defaults_file"
    restored_rows="$(mysql "${mysql_args[@]}" --execute 'SELECT COUNT(*) FROM economy_transactions')"
    echo "[OK] Route 10 snapshot restored: rows=$restored_rows database=$database"
    ;;
  *) usage ;;
esac
