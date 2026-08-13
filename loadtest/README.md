# Route 9 load evidence

This directory contains the reproducible client for measuring the existing HTTP/JSON path before Route 9.2 changes the Actor runtime.

Run a smoke baseline:

```bash
go run ./loadtest/http_baseline.go \
  -url http://127.0.0.1:8080/api/v1/ping \
  -concurrency 32 -duration 30s
```

For JSON endpoints, `{{SEQ}}` in `-body` is replaced with a process-wide unique sequence. For a farm command baseline, first obtain test accounts and current snapshot versions, then use `{{SEQ}}` inside `cmd_id`; a realistic state-changing workload must also advance `base_version`, so use the scenario driver added with the relevant E2E environment rather than replaying one static body. Save stdout as `baseline-<commit>-<date>.json` together with:

- Git commit and dirty-state marker;
- host CPU/RAM and four service instance counts;
- MySQL/Redis/Kafka versions and pool settings;
- command mix and connection count;
- `/metrics` snapshots before and after the run;
- correctness checks: no duplicate asset settlement, no farm-version regression, and error totals grouped by reason.

The tool exits non-zero when any request fails. Route 9.2 adds `route9.2-report.json`: a scheduler microbenchmark and the same local HTTP loopback smoke scenario after admission/rate limiting was added. Neither local result exercises the database-backed player command path.

A capacity claim requires a dedicated environment run for 30–60 minutes; local loopback results are only smoke/regression evidence.

Route 9.5 adds `route9.5-report.json` and three explicit gates:

```bash
make route9-acceptance
make route9-soak ROUTE95_SOAK_DURATION=30m ROUTE95_SOAK_RPS=20000
ROUTE95_MYSQL_DSN='...' make route9-mysql
ROUTE95_MYSQL_DSN='...' ROUTE95_REDIS_ADDR='...' \
  ROUTE95_KAFKA_BROKERS='...' make route9-external
```

`route9-acceptance` is the reproducible local E2E/fault/race gate. The soak
isolates scheduler bounds, while `route9-mysql` must target a disposable,
fully migrated MySQL database and verifies ACK-loss idempotency plus persistent
route fencing. The Route 9 external baseline required the real
MySQL/Redis/Kafka environment; passing the local gate alone was never capacity
evidence.

`route9-external` adds real Redis Pub/Sub and Kafka publish/consume round trips
to the MySQL gate. The Kafka test uses the isolated `route95-e2e` topic and
creates it when the test cluster has topic auto-creation disabled. For the
public protocol path, start two gatesvr instances against the same test
middleware and run:

```bash
go run ./loadtest/route95_e2e.go \
  -url http://127.0.0.1:28080 \
  -viewer-url http://127.0.0.1:28082 \
  -device route95-e2e-fixed-device-v1
```

The driver verifies cross-gateway EVENT delivery, complete plot patch fields,
Snapshot convergence, and durable ACK-loss replay with the same `cmd_id`. It
prints no access token or middleware credential.

Route 9 is now closed as a correctness/stability/capacity-baseline phase: its
reports locate the first bottleneck but are not the final business-capacity
claim. Route 10 reuses these tools for one fixed host and one authoritative
MySQL shard; Route 11 owns independent-load-host and cross-machine cluster
capacity. Historical report status remains unchanged so a failed or partial
run is never rewritten into a pass after the fact.

## Route 9.5 stateful capacity driver

`route95_stateful.go` drives state-changing commands through the public
gatesvr HTTP/WebSocket protocol. It keeps an independent `farm_id`,
`base_version` and `client_seq` per virtual farm, uses a new UUIDv7 `cmd_id`
for normal commands, and only reuses a command ID in the explicit ACK-loss
case. Queries, rejected commands and retries are excluded from accepted
throughput.

```bash
make route9-stateful ROUTE95_GATEWAY_URLS=http://127.0.0.1:28080 \
  ROUTE95_USERS=5000 ROUTE95_TARGET_RPS=650 \
  ROUTE95_MIN_ACCEPTED_RATIO=1.0 \
  ROUTE95_STATEFUL_DURATION=30m ROUTE95_DEVICE_PREFIX=route95-example
```

The JSON output separates `attempted`, `accepted`, `ack_ok`, `rejected`,
`failed` and `retried`, includes error classes and per-minute samples, and can
round-robin across comma-separated gatesvr URLs. `-hot-viewers 1,2,4,8,20`
adds representative shared-farm WebSocket viewers.

A full-duration run is `COMPLETE` only when it has no failed/rejected commands,
no load-generator queue overflow or farm-version errors, and accepted RPS meets
`target-rps * min-accepted-ratio` (1.0 by default). A canceled measurement is
`INTERRUPTED`; an unmet criterion is `FAILED`. Both statuses are still written
to JSON and then return a non-zero process exit code for CI.

For repeatable data, first run the driver with `-setup-only`, then apply
`route95_fixtures.sql` only to a database named exactly `farm_route95`. The SQL
requires a `route95-...-%` device prefix and refuses any other database. These
fixtures fund users and prepare plots before the measurement; all measured
mutations still use authoritative public business commands.

`make route9-scale` uses the same driver for one matrix point. Instance
orchestration remains an environment concern because every service needs a
unique `INSTANCE_ID`, advertised addresses, the matching
`GAMESVR_EXPECTED_INSTANCES`, and a fixed total MySQL connection budget.

## Route 10 single-node performance workflow

Route 10 first implements instrumentation, audits query/index/event
dependencies, and finishes deterministic schema/query simplifications without
rerunning remote capacity tests. Preserve the pre-optimization commit/image and
prepare one sanitized large-table snapshot plus checksum. After the design and
implementation converge, restore that same snapshot and run one paired 2–5
minute before/after A/B with transaction-stage, database-pool, Performance
Schema, redo/fsync and host-I/O evidence. Run a full staircase plus one
30-minute soak only if the candidate merits final acceptance. Multi-host
1/2/4/8 scaling, database shards, Kafka/Redis clusters and N+1 tests belong to
Route 11.

Validate the Route 10 instrumentation and evidence tools without sending load:

```bash
make route10-observability-check
ROUTE10_MYSQL_DEFAULTS_FILE=/secure/client.cnf \
  ./loadtest/route10_collect_mysql.sh evidence/mysql farm_route10
./loadtest/route10_collect_host.sh evidence/host
```

The MySQL collector is read-only and exports Performance Schema statement,
transaction and file-wait summaries plus InnoDB redo/checkpoint/buffer-pool
counters. It requires Performance Schema and the statement/transaction consumers,
and validates the expected TSV columns. Both collectors refuse an existing
output directory. The host collector records CPU, memory, cgroup throttling and block-I/O snapshots.
Neither tool starts a load driver. Credentials stay in a MySQL
`--defaults-extra-file` and are never copied into evidence.

The frozen large-table dataset must be synthetic: database name exactly
`farm_route10`, guest subjects prefixed `route10-`, no profile JSON, permanent
consumer failure or DEAD outbox rows, and 20–21 million economy ledger rows by
default. Export and restore are guarded:

```bash
ROUTE10_MYSQL_DEFAULTS_FILE=/secure/client.cnf \
  ./loadtest/route10_snapshot.sh export /evidence/route10-before.sql.gz

# Destructive and accepted only for the dedicated farm_route10 database.
ROUTE10_MYSQL_DEFAULTS_FILE=/secure/client.cnf \
ROUTE10_RESTORE_CONFIRM=DROP_AND_RESTORE_farm_route10 \
  ./loadtest/route10_snapshot.sh restore /evidence/route10-before.sql.gz
```

After recording a sanitized config and host evidence, freeze the pre-change
commit/image/schema/config/snapshot tuple with `route10_freeze_baseline.sh`.
The image must be supplied by immutable `sha256:` digest. Future before/after
runs are comparable only when their manifest fields and snapshot checksum
match.

```bash
ROUTE10_IMAGE_DIGEST=sha256:... \
  ./loadtest/route10_freeze_baseline.sh /evidence/baseline \
  /evidence/route10-before.sql.gz config-sanitized \
  /evidence/host /evidence/mysql
```

Freezing requires a completely clean worktree, including untracked files, and
verified host/MySQL collector checksums. Durability and capacity values in the
manifest are read from the MySQL evidence; topology/workload and acceptance are
recorded by their sanitized-config and ADR sources rather than asserted as
unverified facts.

`make route9-kafka-outage` invokes the guarded outage helper. It refuses a
Kafka container whose name does not begin with `route95-`; review its report
directory and use it only with a dedicated broker and dedicated consumer-group
prefix. A same-host load generator must have explicit CPU/memory limits and
the resulting numbers must be described as single-host resource-pool capacity.
