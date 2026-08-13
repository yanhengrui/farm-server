#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-}"
interval="${ROUTE10_HOST_SAMPLE_INTERVAL:-1}"
[[ -n "$output_dir" ]] || { echo "usage: $0 OUTPUT_DIR" >&2; exit 2; }
[[ "$interval" =~ ^[0-9]+$ && "$interval" -gt 0 ]] || { echo "[ERROR] sample interval must be a positive integer" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "[ERROR] output directory already exists: $output_dir" >&2; exit 2; }
mkdir -p "$output_dir"

uname -a >"$output_dir/uname.txt"
cp /proc/cpuinfo "$output_dir/cpuinfo.txt"
cp /proc/meminfo "$output_dir/meminfo-before.txt"
cp /proc/stat "$output_dir/proc-stat-before.txt"
cp /proc/diskstats "$output_dir/diskstats-before.txt"
if command -v lscpu >/dev/null; then lscpu >"$output_dir/lscpu.txt"; fi
if command -v lsblk >/dev/null; then lsblk -b -o NAME,TYPE,SIZE,ROTA,RO,MOUNTPOINTS >"$output_dir/lsblk.txt"; fi
if command -v iostat >/dev/null; then iostat -xz "$interval" 2 >"$output_dir/iostat.txt"; fi

for source in cpu.stat memory.current memory.events io.stat; do
  if [[ -r "/sys/fs/cgroup/$source" ]]; then
    cp "/sys/fs/cgroup/$source" "$output_dir/cgroup-${source//./-}-before.txt"
  fi
done
sleep "$interval"
cp /proc/meminfo "$output_dir/meminfo-after.txt"
cp /proc/stat "$output_dir/proc-stat-after.txt"
cp /proc/diskstats "$output_dir/diskstats-after.txt"
for source in cpu.stat memory.current memory.events io.stat; do
  if [[ -r "/sys/fs/cgroup/$source" ]]; then
    cp "/sys/fs/cgroup/$source" "$output_dir/cgroup-${source//./-}-after.txt"
  fi
done
{
  echo "collected_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "sample_interval_seconds=$interval"
  echo "collector=route10_collect_host_v1"
} >"$output_dir/collector-meta.txt"
(cd "$output_dir" && sha256sum -- *.txt >SHA256SUMS)
echo "[OK] Route 10 host evidence: $output_dir"
