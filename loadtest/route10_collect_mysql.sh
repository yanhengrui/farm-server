#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: ROUTE10_MYSQL_DEFAULTS_FILE=/path/to/client.cnf $0 OUTPUT_DIR [DATABASE]" >&2
  exit 2
}

output_dir="${1:-}"
database="${2:-farm_route10}"
defaults_file="${ROUTE10_MYSQL_DEFAULTS_FILE:-}"
[[ -n "$output_dir" && -n "$defaults_file" ]] || usage
[[ -r "$defaults_file" ]] || { echo "[ERROR] MySQL defaults file is not readable" >&2; exit 2; }
command -v mysql >/dev/null || { echo "[ERROR] mysql client not found" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "[ERROR] output directory already exists: $output_dir" >&2; exit 2; }

mysql_args=("--defaults-extra-file=$defaults_file" --batch --raw "--database=$database")
mysql_scalar() {
  mysql "${mysql_args[@]}" --skip-column-names --execute "$1" | tr -d '\r'
}

performance_schema="$(mysql_scalar "SELECT @@performance_schema;")"
[[ "$performance_schema" == "1" ]] || { echo "[ERROR] MySQL Performance Schema must be enabled" >&2; exit 1; }
enabled_consumers="$(mysql_scalar "SELECT COUNT(*) FROM performance_schema.setup_consumers WHERE NAME IN ('global_instrumentation','thread_instrumentation','statements_digest','events_statements_current','events_transactions_current') AND ENABLED = 'YES';")"
[[ "$enabled_consumers" == "5" ]] || { echo "[ERROR] required Performance Schema consumers are not enabled" >&2; exit 1; }

mkdir -p "$output_dir"

collect() {
  local name="$1"
  local query="$2"
  mysql "${mysql_args[@]}" --execute "$query" >"$output_dir/$name.tsv"
}

# Permission and feature probes fail the script before a future A/B silently
# produces partial evidence. No query below reads business row values.
collect server_identity "SELECT VERSION() AS mysql_version, @@version_comment AS version_comment, @@performance_schema AS performance_schema, DATABASE() AS schema_name, CURRENT_USER() AS current_user;"
collect performance_schema_setup "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN ('global_instrumentation','thread_instrumentation','statements_digest','events_statements_current','events_transactions_current') ORDER BY NAME;"
collect durability_variables "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME IN ('innodb_flush_log_at_trx_commit','sync_binlog','innodb_buffer_pool_size','innodb_redo_log_capacity','innodb_log_file_size','innodb_log_buffer_size','max_connections','binlog_group_commit_sync_delay','binlog_group_commit_sync_no_delay_count') ORDER BY VARIABLE_NAME;"
collect statement_digest "SELECT SCHEMA_NAME, DIGEST, LEFT(DIGEST_TEXT,512) AS digest_text, COUNT_STAR, ROUND(SUM_TIMER_WAIT/1000000000000,6) AS total_seconds, ROUND(AVG_TIMER_WAIT/1000000000,3) AS avg_ms, SUM_ROWS_EXAMINED, SUM_ROWS_AFFECTED, SUM_ERRORS FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME = DATABASE() AND DIGEST IS NOT NULL ORDER BY SUM_TIMER_WAIT DESC LIMIT 200;"
collect transaction_summary "SELECT EVENT_NAME, COUNT_STAR, ROUND(SUM_TIMER_WAIT/1000000000000,6) AS total_seconds, ROUND(AVG_TIMER_WAIT/1000000000,3) AS avg_ms, COUNT_READ_WRITE, COUNT_READ_ONLY FROM performance_schema.events_transactions_summary_global_by_event_name ORDER BY SUM_TIMER_WAIT DESC;"
collect file_waits "SELECT EVENT_NAME, COUNT_STAR, ROUND(SUM_TIMER_WAIT/1000000000000,6) AS total_seconds, SUM_NUMBER_OF_BYTES_READ, SUM_NUMBER_OF_BYTES_WRITE FROM performance_schema.file_summary_by_event_name WHERE EVENT_NAME LIKE 'wait/io/file/innodb/%' OR EVENT_NAME LIKE 'wait/io/file/sql/binlog%' ORDER BY SUM_TIMER_WAIT DESC;"
collect global_status "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Threads_connected','Threads_running','Innodb_os_log_written','Innodb_log_waits','Innodb_data_fsyncs','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_reads','Innodb_buffer_pool_pages_dirty','Innodb_rows_inserted','Innodb_rows_updated','Innodb_row_lock_waits','Innodb_row_lock_time','Com_commit','Com_rollback') ORDER BY VARIABLE_NAME;"
collect innodb_metrics "SELECT NAME, COUNT, TYPE, STATUS FROM information_schema.innodb_metrics WHERE NAME IN ('trx_rseg_history_len','buffer_pool_pages_dirty','buffer_pool_read_requests','buffer_pool_reads','log_lsn_current','log_lsn_last_checkpoint','log_waits','os_log_bytes_written','os_log_fsyncs') ORDER BY NAME;"
collect table_sizes "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, DATA_FREE FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() ORDER BY DATA_LENGTH + INDEX_LENGTH DESC;"
collect index_io "SELECT OBJECT_NAME, INDEX_NAME, COUNT_READ, COUNT_WRITE, COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE FROM performance_schema.table_io_waits_summary_by_index_usage WHERE OBJECT_SCHEMA = DATABASE() ORDER BY COUNT_WRITE DESC, OBJECT_NAME, INDEX_NAME;"

assert_columns() {
  local file="$1"
  shift
  [[ -s "$file" ]] || { echo "[ERROR] empty evidence file: $file" >&2; exit 1; }
  local column
  for column in "$@"; do
    head -n 1 "$file" | tr '\t' '\n' | grep -Fxq "$column" || {
      echo "[ERROR] missing column '$column' in $file" >&2
      exit 1
    }
  done
}

assert_columns "$output_dir/server_identity.tsv" mysql_version performance_schema schema_name
assert_columns "$output_dir/performance_schema_setup.tsv" NAME ENABLED
assert_columns "$output_dir/durability_variables.tsv" VARIABLE_NAME VARIABLE_VALUE
assert_columns "$output_dir/statement_digest.tsv" DIGEST COUNT_STAR total_seconds avg_ms
assert_columns "$output_dir/transaction_summary.tsv" EVENT_NAME COUNT_STAR total_seconds
assert_columns "$output_dir/file_waits.tsv" EVENT_NAME COUNT_STAR total_seconds
assert_columns "$output_dir/global_status.tsv" VARIABLE_NAME VARIABLE_VALUE
assert_columns "$output_dir/innodb_metrics.tsv" NAME COUNT TYPE STATUS
assert_columns "$output_dir/table_sizes.tsv" TABLE_NAME TABLE_ROWS DATA_LENGTH INDEX_LENGTH
assert_columns "$output_dir/index_io.tsv" OBJECT_NAME INDEX_NAME COUNT_READ COUNT_WRITE

{
  echo "collected_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "database=$database"
  echo "collector=route10_collect_mysql_v3"
} >"$output_dir/collector-meta.txt"
(cd "$output_dir" && sha256sum -- *.tsv collector-meta.txt >SHA256SUMS)
echo "[OK] Route 10 read-only MySQL evidence: $output_dir"
