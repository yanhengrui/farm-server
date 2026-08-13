#!/usr/bin/env bash
set -euo pipefail

# Guarded Route 9.5 outage harness. Start the public-protocol stateful driver in
# another process before invoking this script. Only the exact route95 broker is
# ever stopped or started.

kafka_container="${ROUTE95_KAFKA_CONTAINER:-route95-kafka}"
mysql_container="${ROUTE95_MYSQL_CONTAINER:-route95-mysql}"
outage_seconds="${ROUTE95_KAFKA_OUTAGE_SECONDS:-60}"
converge_timeout="${ROUTE95_KAFKA_CONVERGE_TIMEOUT:-10m}"
group_prefix="${ROUTE95_KAFKA_GROUP_PREFIX:-route95-workersvr-}"

case "$kafka_container" in route95-*) ;; *) echo '[ERROR] Kafka container must start with route95-' >&2; exit 2;; esac
case "$mysql_container" in route95-*) ;; *) echo '[ERROR] MySQL container must start with route95-' >&2; exit 2;; esac

docker inspect "$kafka_container" "$mysql_container" >/dev/null
docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$kafka_container" | grep -q 'route95-net'
docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$mysql_container" | grep -q 'route95-net'

outbox_counts() {
  docker exec "$mysql_container" mysql -N -uroot farm_route95 -e \
    "SELECT SUM(status='PENDING'),SUM(status='PUBLISHING'),SUM(status='DEAD'),COALESCE(TIMESTAMPDIFF(SECOND,MIN(CASE WHEN status IN ('PENDING','PUBLISHING') THEN created_at END),UTC_TIMESTAMP()),0) FROM outbox_events;"
}

consumer_lag() {
  docker exec "$kafka_container" /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server 127.0.0.1:9092 --describe --all-groups 2>/dev/null |
    awk -v prefix="$group_prefix" '$1 ~ "^" prefix && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum + 0}'
}

read -r pending_before publishing_before dead_before oldest_before < <(outbox_counts)
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"

docker stop --timeout 30 "$kafka_container" >/dev/null
sleep "$outage_seconds"
read -r peak_pending peak_publishing dead_during peak_oldest < <(outbox_counts)

docker start "$kafka_container" >/dev/null
health_deadline=$(( $(date +%s) + 180 ))
until [ "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' "$kafka_container")" = healthy ]; do
  if [ "$(date +%s)" -ge "$health_deadline" ]; then
    echo '[ERROR] route95 Kafka did not become healthy' >&2
    exit 1
  fi
  sleep 2
done

timeout_seconds="$(awk -v raw="$converge_timeout" 'BEGIN { if (raw ~ /m$/) {sub(/m$/, "", raw); print raw*60} else if (raw ~ /s$/) {sub(/s$/, "", raw); print raw} else print raw }')"
deadline=$(( $(date +%s) + timeout_seconds ))
final_lag=-1
while :; do
  read -r pending_after publishing_after dead_after oldest_after < <(outbox_counts)
  final_lag="$(consumer_lag)"
  if [ "$pending_after" = 0 ] && [ "$publishing_after" = 0 ] && [ "$final_lag" = 0 ]; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo '[ERROR] route95 Outbox/consumer lag did not converge' >&2
    exit 1
  fi
  sleep 2
done

ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
recovery_seconds=$(( $(date +%s) - started_epoch - outage_seconds ))
printf '{"status":"CONVERGED","outage_started_at":"%s","recovered_at":"%s","outage_seconds":%s,"peak_pending":%s,"peak_publishing":%s,"peak_oldest_seconds":%s,"recovery_seconds":%s,"dead_before":%s,"dead_after":%s,"final_consumer_lag":%s}\n' \
  "$started_at" "$ended_at" "$outage_seconds" "$peak_pending" "$peak_publishing" "$peak_oldest" "$recovery_seconds" "$dead_before" "$dead_after" "$final_lag"
