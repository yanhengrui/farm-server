// Package observability provides the Route 9.1 HTTP trace and Prometheus baseline.
package observability

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const TraceparentHeader = "traceparent"

type traceContext struct {
	TraceID string
	SpanID  string
	Flags   string
}

type contextKey struct{}

// TraceID returns the current W3C trace ID, if one has been established.
func TraceID(ctx context.Context) string {
	v, _ := ctx.Value(contextKey{}).(traceContext)
	return v.TraceID
}

// Traceparent returns a W3C traceparent value for propagation to another transport.
func Traceparent(ctx context.Context) string {
	tc, _ := ctx.Value(contextKey{}).(traceContext)
	if tc.TraceID == "" {
		tc = newTrace("")
	}
	return formatTraceparent(tc)
}

// ContextFromTraceparent restores an incoming W3C trace context, or starts a new trace.
func ContextFromTraceparent(ctx context.Context, value string) context.Context {
	tc, ok := parseTraceparent(strings.ToLower(value))
	if !ok {
		tc = newTrace("")
	} else {
		tc = newTrace(tc.TraceID)
	}
	return withTrace(ctx, tc)
}

// ContextWithTraceID restores event causality for asynchronous consumers.
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	if len(traceID) != 32 || !isLowerHex(traceID) || allZero(traceID) {
		return ctx
	}
	return withTrace(ctx, newTrace(traceID))
}

func withTrace(ctx context.Context, tc traceContext) context.Context {
	return context.WithValue(ctx, contextKey{}, tc)
}

func newTrace(traceID string) traceContext {
	if traceID == "" {
		traceID = randomHex(16)
	}
	return traceContext{TraceID: traceID, SpanID: randomHex(8), Flags: "01"}
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("generate trace id: %v", err))
	}
	const digits = "0123456789abcdef"
	out := make([]byte, n*2)
	for i, v := range b {
		out[i*2], out[i*2+1] = digits[v>>4], digits[v&15]
	}
	return string(out)
}

func parseTraceparent(v string) (traceContext, bool) {
	p := strings.Split(v, "-")
	if len(p) != 4 || p[0] != "00" || len(p[1]) != 32 || len(p[2]) != 16 || len(p[3]) != 2 {
		return traceContext{}, false
	}
	if !isLowerHex(p[1]) || !isLowerHex(p[2]) || !isLowerHex(p[3]) || allZero(p[1]) || allZero(p[2]) {
		return traceContext{}, false
	}
	return traceContext{TraceID: p[1], SpanID: p[2], Flags: p[3]}, true
}

func isLowerHex(v string) bool {
	for _, c := range v {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allZero(v string) bool { return strings.Trim(v, "0") == "" }

func formatTraceparent(tc traceContext) string {
	return "00-" + tc.TraceID + "-" + tc.SpanID + "-" + tc.Flags
}

// Metrics owns one registry per process. Labels deliberately exclude entity IDs.
type Metrics struct {
	registry *prometheus.Registry
	log      *slog.Logger

	httpRequests           *prometheus.CounterVec
	httpDuration           *prometheus.HistogramVec
	httpInflight           *prometheus.GaugeVec
	grpcRequests           *prometheus.CounterVec
	grpcDuration           *prometheus.HistogramVec
	grpcInflight           *prometheus.GaugeVec
	actorRequests          *prometheus.CounterVec
	actorDuration          prometheus.Histogram
	actorInflight          prometheus.Gauge
	actorStates            prometheus.Gauge
	actorQueued            prometheus.Gauge
	actorReady             prometheus.Gauge
	actorWorkers           prometheus.Gauge
	actorQueueWait         prometheus.Histogram
	actorRejected          *prometheus.CounterVec
	wsActive               prometheus.Gauge
	wsSlowConsumer         prometheus.Counter
	mailboxPublishFailures prometheus.Counter
	mailboxQueueDrops      prometheus.Counter
	mailboxOutOfOrderDrops prometheus.Counter
	outboxPublished        prometheus.Counter
	outboxFailures         *prometheus.CounterVec
	petScanDuration        *prometheus.HistogramVec
	petSubmissions         *prometheus.CounterVec
	retentionRows          *prometheus.CounterVec
	retentionFails         *prometheus.CounterVec
	kafkaConsumed          *prometheus.CounterVec
	kafkaLag               *prometheus.GaugeVec
	redisCommands          *prometheus.CounterVec
	redisDuration          *prometheus.HistogramVec
	farmPubSub             *prometheus.CounterVec
	farmVersionGaps        prometheus.Counter
	economyStage           *prometheus.HistogramVec
	economyActive          *prometheus.GaugeVec
	dbPoolAcquire          *prometheus.HistogramVec
	internalErrors         *prometheus.CounterVec
	admissionQueued        *prometheus.GaugeVec
	admissionActive        *prometheus.GaugeVec
	admissionWait          *prometheus.HistogramVec
	admissionReject        *prometheus.CounterVec
	readCacheRequests      *prometheus.CounterVec
	readCacheDuration      *prometheus.HistogramVec
}

func New(service string, log *slog.Logger) *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{registry: r, log: log,
		httpRequests:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_http_requests_total", Help: "HTTP requests by stable route."}, []string{"service", "direction", "method", "route", "status"}),
		httpDuration:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_http_request_duration_seconds", Help: "HTTP request duration.", Buckets: prometheus.DefBuckets}, []string{"service", "direction", "method", "route"}),
		httpInflight:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_http_inflight", Help: "In-flight HTTP requests."}, []string{"service", "direction", "route"}),
		grpcRequests:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_grpc_requests_total", Help: "gRPC requests by stable method and status."}, []string{"service", "direction", "method", "code"}),
		grpcDuration:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_grpc_request_duration_seconds", Help: "gRPC request duration.", Buckets: prometheus.DefBuckets}, []string{"service", "direction", "method"}),
		grpcInflight:           prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_grpc_inflight", Help: "In-flight gRPC requests."}, []string{"service", "direction", "method"}),
		actorRequests:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_actor_requests_total", Help: "Current Actor runtime submissions observed at farmsvc."}, []string{"result"}),
		actorDuration:          prometheus.NewHistogram(prometheus.HistogramOpts{Name: "farm_actor_request_duration_seconds", Help: "Current Actor runtime request duration.", Buckets: prometheus.DefBuckets}),
		actorInflight:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_actor_inflight", Help: "Current Actor runtime requests in flight."}),
		actorStates:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_actor_active_states", Help: "Active lightweight actor states."}),
		actorQueued:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_actor_fifo_queued", Help: "Commands queued in per-farm FIFOs."}),
		actorReady:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_actor_ready_depth", Help: "Commands in the global ready queue."}),
		actorWorkers:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_actor_workers_busy", Help: "Busy fixed workers."}),
		actorQueueWait:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "farm_actor_queue_wait_seconds", Help: "Actor command queue wait.", Buckets: prometheus.DefBuckets}),
		actorRejected:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_actor_rejected_total", Help: "Actor submissions rejected by reason."}, []string{"reason"}),
		wsActive:               prometheus.NewGauge(prometheus.GaugeOpts{Name: "farm_ws_active_connections", Help: "Active WebSocket connections."}),
		wsSlowConsumer:         prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_ws_slow_consumer_total", Help: "WebSocket writes rejected by a full queue."}),
		mailboxPublishFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_mailbox_publish_failures_total", Help: "Mailbox badge notifications that failed before Redis publish completed."}),
		mailboxQueueDrops:      prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_mailbox_ws_queue_dropped_total", Help: "Mailbox badge notifications dropped because a WebSocket write queue was full."}),
		mailboxOutOfOrderDrops: prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_mailbox_out_of_order_dropped_total", Help: "Duplicate or out-of-order mailbox badge versions rejected by gatesvr."}),
		outboxPublished:        prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_outbox_published_total", Help: "Outbox events published."}),
		outboxFailures:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_outbox_failures_total", Help: "Outbox scan or publish failures."}, []string{"stage"}),
		petScanDuration:        prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_pet_scan_duration_seconds", Help: "Pet scheduler scan duration by physical shard.", Buckets: prometheus.DefBuckets}, []string{"shard"}),
		petSubmissions:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_pet_command_submissions_total", Help: "Pet auto-harvest command submission outcomes by physical shard."}, []string{"shard", "result"}),
		retentionRows:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_retention_deleted_total", Help: "Rows deleted by the retention reaper per table."}, []string{"table"}),
		retentionFails:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_retention_failures_total", Help: "Retention reaper failures per table."}, []string{"table"}),
		kafkaConsumed:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_kafka_consumed_total", Help: "Kafka consume outcomes."}, []string{"group", "result"}),
		kafkaLag:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_kafka_consumer_lag", Help: "Latest observed Kafka partition lag."}, []string{"group", "partition"}),
		redisCommands:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_redis_commands_total", Help: "Redis command outcomes."}, []string{"command", "result"}),
		redisDuration:          prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_redis_command_duration_seconds", Help: "Redis command duration.", Buckets: prometheus.DefBuckets}, []string{"command"}),
		farmPubSub:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_realtime_pubsub_total", Help: "Farm realtime Pub/Sub outcomes."}, []string{"direction", "result"}),
		farmVersionGaps:        prometheus.NewCounter(prometheus.CounterOpts{Name: "farm_realtime_version_gap_total", Help: "Realtime farm version gaps requiring Snapshot recovery."}),
		economyStage:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_economy_transaction_stage_duration_seconds", Help: "Authoritative economy transaction duration by bounded operation, stage and result.", Buckets: prometheus.DefBuckets}, []string{"operation", "stage", "result"}),
		economyActive:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_economy_transactions_active", Help: "Active authoritative economy transactions."}, []string{"operation"}),
		dbPoolAcquire:          prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_db_pool_acquire_duration_seconds", Help: "Time spent acquiring a database/sql connection from the pool.", Buckets: prometheus.DefBuckets}, []string{"database", "operation", "result"}),
		internalErrors:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_internal_errors_total", Help: "Internal failures classified by bounded source, operation, stage and technical code."}, []string{"source", "operation", "stage", "code"}),
		admissionQueued:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_admission_queued", Help: "Requests currently waiting at an admission gate."}, []string{"reason"}),
		admissionActive:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "farm_admission_inflight", Help: "Requests holding an admission gate slot."}, []string{"reason"}),
		admissionWait:          prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_admission_wait_duration_seconds", Help: "Admission queue wait by stable source and result.", Buckets: prometheus.DefBuckets}, []string{"reason", "result"}),
		admissionReject:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_admission_rejected_total", Help: "Admission rejections by stable capacity source."}, []string{"reason"}),
		readCacheRequests:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "farm_read_cache_requests_total", Help: "Read-through cache outcomes by bounded resource."}, []string{"resource", "result"}),
		readCacheDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "farm_read_cache_operation_duration_seconds", Help: "Read-through cache operation duration.", Buckets: prometheus.DefBuckets}, []string{"resource", "result"}),
	}
	r.MustRegister(m.httpRequests, m.httpDuration, m.httpInflight, m.grpcRequests, m.grpcDuration, m.grpcInflight, m.actorRequests, m.actorDuration, m.actorInflight, m.actorStates, m.actorQueued, m.actorReady, m.actorWorkers, m.actorQueueWait, m.actorRejected, m.wsActive, m.wsSlowConsumer, m.mailboxPublishFailures, m.mailboxQueueDrops, m.mailboxOutOfOrderDrops, m.outboxPublished, m.outboxFailures, m.petScanDuration, m.petSubmissions, m.retentionRows, m.retentionFails, m.kafkaConsumed, m.kafkaLag, m.redisCommands, m.redisDuration, m.farmPubSub, m.farmVersionGaps, m.economyStage, m.economyActive, m.dbPoolAcquire, m.internalErrors, m.admissionQueued, m.admissionActive, m.admissionWait, m.admissionReject, m.readCacheRequests, m.readCacheDuration)
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	_ = service
	return m
}

func (m *Metrics) GRPCServerInterceptor(service string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		m.grpcInflight.WithLabelValues(service, "server", info.FullMethod).Inc()
		defer m.grpcInflight.WithLabelValues(service, "server", info.FullMethod).Dec()
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		m.grpcRequests.WithLabelValues(service, "server", info.FullMethod, code).Inc()
		if code == "Internal" {
			m.InternalError("rpc", info.FullMethod, "server", "grpc_internal")
		}
		m.grpcDuration.WithLabelValues(service, "server", info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

func (m *Metrics) GRPCClientInterceptor(service string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m.grpcInflight.WithLabelValues(service, "client", method).Inc()
		defer m.grpcInflight.WithLabelValues(service, "client", method).Dec()
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		code := status.Code(err).String()
		m.grpcRequests.WithLabelValues(service, "client", method, code).Inc()
		if code == "Internal" {
			m.InternalError("rpc", method, "client", "grpc_internal")
		}
		m.grpcDuration.WithLabelValues(service, "client", method).Observe(time.Since(start).Seconds())
		return err
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RegisterDB exports database/sql pool saturation without wrapping queries.
func (m *Metrics) RegisterDB(name string, db *sql.DB) {
	fields := map[string]func(sql.DBStats) float64{
		"open":                  func(s sql.DBStats) float64 { return float64(s.OpenConnections) },
		"in_use":                func(s sql.DBStats) float64 { return float64(s.InUse) },
		"idle":                  func(s sql.DBStats) float64 { return float64(s.Idle) },
		"wait_count":            func(s sql.DBStats) float64 { return float64(s.WaitCount) },
		"wait_duration_seconds": func(s sql.DBStats) float64 { return s.WaitDuration.Seconds() },
	}
	for field, read := range fields {
		field, read := field, read
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_db_pool_" + field, Help: "database/sql pool " + field, ConstLabels: prometheus.Labels{"database": name}}, func() float64 { return read(db.Stats()) }))
	}
}

func (m *Metrics) RegisterOutbox(db *sql.DB) {
	m.RegisterOutboxShard("primary", db)
}

// RegisterOutboxShard exports one bounded-cardinality series per physical
// shard. It must be called once for every configured outbox database.
func (m *Metrics) RegisterOutboxShard(shard string, db *sql.DB) {
	labels := prometheus.Labels{"shard": shard}
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_outbox_pending", Help: "Pending or publishing outbox rows.", ConstLabels: labels}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var n float64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_events WHERE status IN ('PENDING','PUBLISHING')`).Scan(&n); err != nil {
			return -1
		}
		return n
	}))
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_outbox_oldest_pending_age_seconds", Help: "Age of the oldest unpublished outbox row.", ConstLabels: labels}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var age sql.NullFloat64
		if err := db.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND, MIN(created_at), UTC_TIMESTAMP(6)) / 1000000 FROM outbox_events WHERE status IN ('PENDING','PUBLISHING')`).Scan(&age); err != nil || !age.Valid {
			return 0
		}
		return age.Float64
	}))
}

// RegisterPetScannerShard exports the due queue size and the age of its oldest
// item. A negative backlog means the diagnostic query failed; age is zero when
// no work is due.
func (m *Metrics) RegisterPetScannerShard(shard string, db *sql.DB) {
	labels := prometheus.Labels{"shard": shard}
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_pet_due_backlog", Help: "Due pet auto-harvest schedules awaiting a scanner claim.", ConstLabels: labels}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var n float64
		const q = `SELECT COUNT(*) FROM farm_snapshots AS fs JOIN player_pets AS pp ON pp.user_id = fs.owner_user_id AND pp.status = 'ACTIVE' AND pp.auto_harvest_enabled = 1 WHERE fs.next_pet_action_at IS NOT NULL AND fs.next_pet_action_at <= UTC_TIMESTAMP(6) AND (fs.pet_scan_lease_until IS NULL OR fs.pet_scan_lease_until <= UTC_TIMESTAMP(6))`
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return -1
		}
		return n
	}))
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_pet_oldest_due_age_seconds", Help: "Age of the oldest unclaimed due pet auto-harvest schedule.", ConstLabels: labels}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var age sql.NullFloat64
		const q = `SELECT TIMESTAMPDIFF(MICROSECOND, MIN(fs.next_pet_action_at), UTC_TIMESTAMP(6)) / 1000000 FROM farm_snapshots AS fs JOIN player_pets AS pp ON pp.user_id = fs.owner_user_id AND pp.status = 'ACTIVE' AND pp.auto_harvest_enabled = 1 WHERE fs.next_pet_action_at IS NOT NULL AND fs.next_pet_action_at <= UTC_TIMESTAMP(6) AND (fs.pet_scan_lease_until IS NULL OR fs.pet_scan_lease_until <= UTC_TIMESTAMP(6))`
		if err := db.QueryRowContext(ctx, q).Scan(&age); err != nil || !age.Valid {
			return 0
		}
		return age.Float64
	}))
}

func (m *Metrics) RegisterRedis(name string, c *redis.Client) {
	c.AddHook(redisHook{m: m})
	fields := map[string]func(*redis.PoolStats) float64{
		"total_connections":   func(s *redis.PoolStats) float64 { return float64(s.TotalConns) },
		"idle_connections":    func(s *redis.PoolStats) float64 { return float64(s.IdleConns) },
		"wait_timeouts_total": func(s *redis.PoolStats) float64 { return float64(s.Timeouts) },
	}
	for field, read := range fields {
		field, read := field, read
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_redis_pool_" + field, Help: "Redis pool " + field, ConstLabels: prometheus.Labels{"redis": name}}, func() float64 { return read(c.PoolStats()) }))
	}
}

func (m *Metrics) WSConnected() func()            { m.wsActive.Inc(); return m.wsActive.Dec }
func (m *Metrics) ActorStateDelta(n int)          { m.actorStates.Add(float64(n)) }
func (m *Metrics) ActorQueueDelta(n int)          { m.actorQueued.Add(float64(n)) }
func (m *Metrics) ActorWorkerDelta(n int)         { m.actorWorkers.Add(float64(n)) }
func (m *Metrics) ActorReadyDelta(n int)          { m.actorReady.Add(float64(n)) }
func (m *Metrics) ActorQueueWait(d time.Duration) { m.actorQueueWait.Observe(d.Seconds()) }
func (m *Metrics) ActorRejected(reason string)    { m.actorRejected.WithLabelValues(reason).Inc() }
func (m *Metrics) WSSlowConsumer()                { m.wsSlowConsumer.Inc() }
func (m *Metrics) MailboxPublishFailure()         { m.mailboxPublishFailures.Inc() }
func (m *Metrics) MailboxQueueDropped()           { m.mailboxQueueDrops.Inc() }
func (m *Metrics) MailboxOutOfOrderDropped()      { m.mailboxOutOfOrderDrops.Inc() }
func (m *Metrics) FarmPubSub(direction, result string) {
	m.farmPubSub.WithLabelValues(direction, result).Inc()
}
func (m *Metrics) FarmVersionGap()            { m.farmVersionGaps.Inc() }
func (m *Metrics) OutboxPublished(n int)      { m.outboxPublished.Add(float64(n)) }
func (m *Metrics) OutboxFailure(stage string) { m.outboxFailures.WithLabelValues(stage).Inc() }
func (m *Metrics) PetScanDuration(shard string, d time.Duration) {
	m.petScanDuration.WithLabelValues(shard).Observe(d.Seconds())
}
func (m *Metrics) PetSubmission(shard, result string) {
	m.petSubmissions.WithLabelValues(shard, result).Inc()
}

func (m *Metrics) RetentionDeleted(table string, n int) {
	m.retentionRows.WithLabelValues(table).Add(float64(n))
}
func (m *Metrics) RetentionFailure(table string) { m.retentionFails.WithLabelValues(table).Inc() }
func (m *Metrics) KafkaConsumed(group, result string) {
	m.kafkaConsumed.WithLabelValues(group, result).Inc()
}
func (m *Metrics) KafkaLag(group string, partition int, lag int64) {
	if lag < 0 {
		lag = 0
	}
	m.kafkaLag.WithLabelValues(group, strconv.Itoa(partition)).Set(float64(lag))
}

func (m *Metrics) EconomyStage(operation, stage, result string, duration time.Duration) {
	m.economyStage.WithLabelValues(operation, stage, result).Observe(duration.Seconds())
}

func (m *Metrics) EconomyTransactionDelta(operation string, delta int) {
	m.economyActive.WithLabelValues(operation).Add(float64(delta))
}

func (m *Metrics) DBPoolAcquire(operation, result string, duration time.Duration) {
	m.dbPoolAcquire.WithLabelValues("primary", operation, result).Observe(duration.Seconds())
}

func (m *Metrics) InternalError(source, operation, stage, code string) {
	m.internalErrors.WithLabelValues(source, operation, stage, code).Inc()
}

func (m *Metrics) AdmissionQueueDelta(reason string, delta int) {
	m.admissionQueued.WithLabelValues(reason).Add(float64(delta))
}

func (m *Metrics) AdmissionInflightDelta(reason string, delta int) {
	m.admissionActive.WithLabelValues(reason).Add(float64(delta))
}

func (m *Metrics) AdmissionWait(reason, result string, duration time.Duration) {
	m.admissionWait.WithLabelValues(reason, result).Observe(duration.Seconds())
}

func (m *Metrics) AdmissionRejected(reason string) {
	m.admissionReject.WithLabelValues(reason).Inc()
}

func (m *Metrics) ReadCache(resource, result string, duration time.Duration) {
	m.readCacheRequests.WithLabelValues(resource, result).Inc()
	m.readCacheDuration.WithLabelValues(resource, result).Observe(duration.Seconds())
}

// Middleware establishes a server span, emits access logs and records stable-route metrics.
func (m *Metrics) Middleware(service string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parent, ok := parseTraceparent(strings.ToLower(r.Header.Get(TraceparentHeader)))
		traceID := ""
		if ok {
			traceID = parent.TraceID
		}
		tc := newTrace(traceID)
		ctx := withTrace(r.Context(), tc)
		route := routeLabel(r.URL.Path)
		m.httpInflight.WithLabelValues(service, "server", route).Inc()
		defer m.httpInflight.WithLabelValues(service, "server", route).Dec()
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		w.Header().Set(TraceparentHeader, formatTraceparent(tc))
		w.Header().Set("X-Trace-ID", tc.TraceID)
		if route == "/farm/submit" {
			m.actorInflight.Inc()
			defer m.actorInflight.Dec()
		}
		next.ServeHTTP(rw, r.WithContext(ctx))
		d := time.Since(start).Seconds()
		status := strconv.Itoa(rw.status)
		m.httpRequests.WithLabelValues(service, "server", r.Method, route, status).Inc()
		m.httpDuration.WithLabelValues(service, "server", r.Method, route).Observe(d)
		if route == "/farm/submit" {
			result := "ok"
			if rw.status >= 400 {
				result = "error"
			}
			m.actorRequests.WithLabelValues(result).Inc()
			m.actorDuration.Observe(d)
		}
		attrs := []any{slog.String("trace_id", tc.TraceID), slog.String("cmd_id", commandID(r)), slog.String("method", r.Method), slog.String("route", route), slog.Int("status", rw.status), slog.Duration("duration", time.Since(start))}
		if rw.status >= 400 {
			m.log.WarnContext(ctx, "http request failed", attrs...)
		} else {
			// Successful high-volume requests stay at debug; counters/histograms are the aggregate signal.
			m.log.DebugContext(ctx, "http request", attrs...)
		}
	})
}

func commandID(r *http.Request) string {
	if v := r.Header.Get("Idempotency-Key"); v != "" {
		return v
	}
	return r.Header.Get("X-Cmd-ID")
}

// Transport creates a child span and injects traceparent into internal HTTP RPCs.
func (m *Metrics) Transport(service string, next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		parent, _ := req.Context().Value(contextKey{}).(traceContext)
		child := newTrace(parent.TraceID)
		req = req.Clone(withTrace(req.Context(), child))
		req.Header.Set(TraceparentHeader, formatTraceparent(child))
		route := routeLabel(req.URL.Path)
		m.httpInflight.WithLabelValues(service, "client", route).Inc()
		start := time.Now()
		resp, err := next.RoundTrip(req)
		m.httpInflight.WithLabelValues(service, "client", route).Dec()
		status := "error"
		if resp != nil {
			status = strconv.Itoa(resp.StatusCode)
		}
		m.httpRequests.WithLabelValues(service, "client", req.Method, route, status).Inc()
		m.httpDuration.WithLabelValues(service, "client", req.Method, route).Observe(time.Since(start).Seconds())
		return resp, err
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var knownRoutes = map[string]struct{}{
	"/ws": {}, "/farm/submit": {}, "/api/v1/ping": {},
	"/api/v1/auth/guest-login": {}, "/api/v1/auth/register": {}, "/api/v1/auth/login": {},
	"/api/v1/auth/refresh": {}, "/api/v1/auth/logout": {},
	"/api/v1/farm/snapshot": {}, "/api/v1/farm/sell": {}, "/api/v1/shop/purchase": {},
	"/api/v1/social/invite": {}, "/api/v1/social/invite/accept": {}, "/api/v1/social/friends": {},
	"/api/v1/mail/list": {}, "/api/v1/mail/claim": {}, "/api/v1/mail/read": {},
	"/api/v1/task/list": {}, "/api/v1/task/claim": {}, "/api/v1/pet/buy": {}, "/api/v1/pet/status": {},
	"/rpc/farm/commit": {}, "/rpc/farm/load-snapshot": {}, "/rpc/farm/get-snapshot": {},
	"/rpc/farm/advance-route-epoch": {},
	"/rpc/account/guest-login":      {}, "/rpc/account/register": {}, "/rpc/account/password-login": {},
	"/rpc/account/refresh": {}, "/rpc/account/logout": {}, "/rpc/account/authenticate": {},
	"/rpc/social/create-invite": {}, "/rpc/social/accept-invite": {}, "/rpc/social/are-friends": {}, "/rpc/social/list-friends": {},
	"/rpc/mail/send": {}, "/rpc/mail/list": {}, "/rpc/mail/claim": {}, "/rpc/mail/mark-read": {},
	"/rpc/task/incr-progress": {}, "/rpc/task/list": {}, "/rpc/task/claim": {},
	"/rpc/pet/buy": {}, "/rpc/pet/has": {},
}

func routeLabel(path string) string {
	if _, ok := knownRoutes[path]; ok {
		return path
	}
	return "unknown"
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijacking unsupported")
	}
	return h.Hijack()
}
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type redisHook struct{ m *Metrics }

func (h redisHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h redisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		result := "ok"
		if err != nil && err != redis.Nil {
			result = "error"
		}
		name := strings.ToLower(cmd.Name())
		h.m.redisCommands.WithLabelValues(name, result).Inc()
		h.m.redisDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
		return err
	}
}
func (h redisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		result := "ok"
		if err != nil && err != redis.Nil {
			result = "error"
		}
		h.m.redisCommands.WithLabelValues("pipeline", result).Add(float64(len(cmds)))
		h.m.redisDuration.WithLabelValues("pipeline").Observe(time.Since(start).Seconds())
		return err
	}
}
