#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-}"
snapshot="${2:-}"
sanitized_config="${3:-}"
host_evidence_dir="${4:-}"
mysql_evidence_dir="${5:-}"
image_digest="${ROUTE10_IMAGE_DIGEST:-}"
[[ -n "$output_dir" && -r "$snapshot" && -r "$snapshot.sha256" && -r "$sanitized_config" && -d "$host_evidence_dir" && -d "$mysql_evidence_dir" ]] || {
  echo "usage: ROUTE10_IMAGE_DIGEST=sha256:... $0 OUTPUT_DIR SNAPSHOT.sql.gz SANITIZED_CONFIG HOST_EVIDENCE_DIR MYSQL_EVIDENCE_DIR" >&2
  exit 2
}
[[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "[ERROR] ROUTE10_IMAGE_DIGEST must be an immutable sha256 digest" >&2; exit 2; }
[[ "$(basename "$sanitized_config")" == *sanitized* ]] || { echo "[ERROR] config filename must explicitly contain 'sanitized'" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "[ERROR] output directory already exists" >&2; exit 2; }
(cd "$host_evidence_dir" && sha256sum --check SHA256SUMS)
(cd "$mysql_evidence_dir" && sha256sum --check SHA256SUMS)
[[ -s "$mysql_evidence_dir/durability_variables.tsv" && -s "$mysql_evidence_dir/server_identity.tsv" ]] || {
  echo "[ERROR] MySQL evidence is incomplete" >&2
  exit 2
}
grep -Fxq "collector=route10_collect_mysql_v3" "$mysql_evidence_dir/collector-meta.txt" || {
  echo "[ERROR] MySQL evidence must be produced by route10_collect_mysql_v3" >&2
  exit 2
}
grep -Fxq "database=farm_route10" "$mysql_evidence_dir/collector-meta.txt" || {
  echo "[ERROR] MySQL evidence must target the dedicated farm_route10 database" >&2
  exit 2
}
(cd "$(dirname "$snapshot")" && sha256sum --check "$(basename "$snapshot").sha256")

commit="$(git rev-parse HEAD)"
dirty=false
if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then dirty=true; fi
[[ "$dirty" == false ]] || { echo "[ERROR] baseline must be frozen from a clean worktree, including untracked files" >&2; exit 2; }

mysql_variable() {
  local name="$1"
  awk -F '\t' -v wanted="$name" '$1 == wanted { print $2 }' "$mysql_evidence_dir/durability_variables.tsv"
}
flush_log_at_commit="$(mysql_variable innodb_flush_log_at_trx_commit)"
sync_binlog="$(mysql_variable sync_binlog)"
buffer_pool_size="$(mysql_variable innodb_buffer_pool_size)"
redo_log_capacity="$(mysql_variable innodb_redo_log_capacity)"
log_file_size="$(mysql_variable innodb_log_file_size)"
max_connections="$(mysql_variable max_connections)"
group_commit_delay="$(mysql_variable binlog_group_commit_sync_delay)"
group_commit_count="$(mysql_variable binlog_group_commit_sync_no_delay_count)"
[[ "$flush_log_at_commit" == "1" && "$sync_binlog" == "1" ]] || {
  echo "[ERROR] collected MySQL durability must have innodb_flush_log_at_trx_commit=1 and sync_binlog=1" >&2
  exit 2
}
[[ -n "$buffer_pool_size" && -n "$max_connections" && ( -n "$redo_log_capacity" || -n "$log_file_size" ) ]] || {
  echo "[ERROR] collected MySQL evidence lacks required capacity variables" >&2
  exit 2
}

snapshot_sha="$(sha256sum -- "$snapshot" | awk '{print $1}')"
config_sha="$(sha256sum -- "$sanitized_config" | awk '{print $1}')"
host_sha="$(cd "$host_evidence_dir" && find . -maxdepth 1 -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')"
mysql_sha="$(cd "$mysql_evidence_dir" && find . -maxdepth 1 -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')"
mkdir -p "$output_dir"

{
  echo "format=route10-baseline-v2"
  echo "frozen_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$commit"
  echo "git_dirty=$dirty"
  echo "image_digest=$image_digest"
  echo "snapshot_sha256=$snapshot_sha"
  echo "sanitized_config_sha256=$config_sha"
  echo "host_evidence_sha256=$host_sha"
  echo "mysql_evidence_sha256=$mysql_sha"
  echo "topology_source=sanitized_config_sha256:$config_sha"
  echo "mysql_innodb_flush_log_at_trx_commit=$flush_log_at_commit"
  echo "mysql_sync_binlog=$sync_binlog"
  echo "mysql_binlog_group_commit_sync_delay=${group_commit_delay:-not_set}"
  echo "mysql_binlog_group_commit_sync_no_delay_count=${group_commit_count:-not_set}"
  echo "mysql_innodb_buffer_pool_size=$buffer_pool_size"
  echo "mysql_innodb_redo_log_capacity=${redo_log_capacity:-not_exposed}"
  echo "mysql_innodb_log_file_size=${log_file_size:-not_exposed}"
  echo "mysql_max_connections=$max_connections"
  echo "workload_source=sanitized_config_sha256:$config_sha"
  echo "acceptance_plan_source=ADR-023"
} >"$output_dir/baseline-manifest.txt"
sha256sum -- migrations/mysql/*.sql >"$output_dir/migration-SHA256SUMS"
cp "$snapshot.sha256" "$output_dir/snapshot-SHA256SUM"
cp "$sanitized_config" "$output_dir/config-sanitized"
cp "$mysql_evidence_dir/server_identity.tsv" "$output_dir/mysql-server-identity.tsv"
cp "$mysql_evidence_dir/durability_variables.tsv" "$output_dir/mysql-variables.tsv"
(cd "$output_dir" && sha256sum -- baseline-manifest.txt migration-SHA256SUMS snapshot-SHA256SUM config-sanitized mysql-server-identity.tsv mysql-variables.tsv >SHA256SUMS)
echo "[OK] immutable Route 10 baseline manifest: $output_dir"
