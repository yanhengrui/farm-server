# Route 9.1 correctness evidence

Date: 2026-07-31

Implemented and verified in this workspace:

- W3C `traceparent` parsing, generation and HTTP client propagation;
- Prometheus registry and stable low-cardinality HTTP route labels;
- event `trace_id`, `correlation_id`, `causation_id` fields remain distinct from `event_id` and `cmd_id`;
- DB/Redis pool, Actor request, WSS connection/slow-consumer, Outbox backlog/publish/failure and Kafka consume/lag metrics;
- all Go tests pass;
- race tests pass for Actor, WebSocket and farm infrastructure;
- build, vet and diff checks pass.

Local HTTP smoke result is stored in `baseline-report.json`. It is deliberately not a player-command capacity claim. A dedicated environment with MySQL, Redis and Kafka is still required for the Route 9/9.5 30–60 minute correctness and 10k–50k commands/s acceptance gate.

Route 9.2 additionally verifies bounded per-farm FIFO, fixed maximum worker concurrency, unrelated-farm progress on the same scheduler shard, accepted-command behavior after caller cancellation, saturation rejection, and retry metadata propagation. Local measurements are stored in `route9.2-report.json`; the database-backed acceptance gate remains open under Route 9.5.
