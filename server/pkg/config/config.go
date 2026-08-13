// Package config 集中加载并在启动时 fail-fast 校验环境变量。
// 变量清单与语义见 05-配置与环境变量.md；此处仅覆盖骨架所需的最小子集。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Common 是四个部署单元共享的基础配置。
type Common struct {
	AppEnv                   string       // local/devcloud/test/stress
	ServiceName              string       // gatesvr/farmsvr/gamesvr/workersvr
	InstanceID               string       // Pod/容器内唯一
	HTTPAddr                 string       // 对外/诊断 HTTP 监听
	GRPCAddr                 string       // 内部 RPC 监听（条件必填）
	DiagAddr                 string       // /live /ready /metrics
	LogLevel                 string       // debug/info/warn/error
	MySQLDSN                 string       // 权威库 DSN；gatesvr/farmsvr 可为空
	MySQLShards              []MySQLShard // 有名字的物理分片；未配置时由 MySQLDSN 兼容为 primary
	GlobalIDNodeID           int          // 多分片时必须为 0..255，防止多 gamesvr 生成冲突 ID
	RedisAddrs               string       // 至少一个 host:port
	RedisPassword            string       // 仅 ACL 启用时填写
	EtcdEndpoints            string       // 路线 9.4 服务发现与虚拟路由桶控制面
	EtcdUsername             string
	EtcdPassword             string
	EtcdPrefix               string
	AdvertiseHTTPAddr        string // 注册到 etcd 的可访问地址（含 http://）
	AdvertiseGRPCAddr        string // 注册到 etcd 的原生 gRPC 地址
	RouteBucketCount         int
	RouteLeaseTTL            time.Duration
	RouteReconcileEvery      time.Duration
	PubSubUnsubscribeWait    time.Duration
	EventBusDriver           string // memory/kafka
	KafkaBrokers             string // driver=kafka 时至少一个 broker（host:port 逗号分隔）
	KafkaTopic               string
	KafkaConsumerGroupPrefix string
	KafkaConsumerMaxTries    int
	KafkaConsumerRetryMin    time.Duration
	KafkaConsumerRetryMax    time.Duration
	OutboxBatchSize          int
	OutboxPollInterval       time.Duration
	OutboxDrainInterval      time.Duration
	OutboxLockTTL            time.Duration
	OutboxMaxRetry           int
	PetScanInterval          time.Duration
	PetScanBatchSize         int
	PetScanWorkers           int
	PetScanRatePerSecond     int
	PetScanLeaseTTL          time.Duration
	PetScanIdleMaxBackoff    time.Duration
	RetentionInterval        time.Duration
	RetentionBatchSize       int
	RetentionMaxPerRun       int
	RetentionOutbox          time.Duration
	RetentionCmdReceipts     time.Duration
	RetentionConsumedEvents  time.Duration
	GameSvrAddr              string // farmsvr 调用 gamesvr 的 HTTP 地址，例如 "http://gamesvr:9090"
	FarmSvrAddr              string // gatesvr 调用 farmsvr 的 HTTP 地址，例如 "http://farmsvr:8080"
	NativeGRPCAddr           string
	GameSvrGRPCAddr          string
	FarmSvrGRPCAddr          string
	RPCTransport             string
	RPCDefaultTimeout        time.Duration
	RPCKeepaliveTime         time.Duration
	RPCKeepaliveTimeout      time.Duration
	RPCMaxMessageBytes       int
	RPCMaxStreams            int
	TokenSecret              string // HMAC 令牌签名密钥；必须来自安全随机字节
	ShutdownTimeout          time.Duration
	ActorShards              int
	ActorWorkers             int
	ActorFarmQueue           int
	ActorReadyQueue          int
	ActorIngress             int
	ActorMaxActive           int
	ActorEnqueueWait         time.Duration
	ActorExecutionTimeout    time.Duration
	ActorIdleTTL             time.Duration
	GameMaxInflight          int
	MySQLTotalConnections    int
	GameExpectedInstances    int
	AdmissionWait            time.Duration
	AdmissionRetryAfter      time.Duration
	GatewayUserRPS           int
	GatewayUserBurst         int
	GatewayIPRPS             int
	GatewayIPBurst           int
	GatewayMaxInflight       int
	GatewayReadCacheTTL      time.Duration
	GatewayReadCacheStaleTTL time.Duration
	GatewayReadCacheTimeout  time.Duration
}

// MySQLShard binds a stable logical shard name to one independent MySQL DSN.
// The DSN is never safe to place in metrics, logs, or reports.
type MySQLShard struct {
	Name string
	DSN  string
}

var validServices = map[string]bool{
	"gatesvr": true, "farmsvr": true, "gamesvr": true, "workersvr": true,
}
var validEnvs = map[string]bool{
	"local": true, "devcloud": true, "test": true, "stress": true,
}

// Load 从环境变量装配 Common，并执行跨字段 fail-fast 校验。
// requireMySQL 表示该服务是否强制要求 MySQL_DSN（gatesvr/farmsvr 可为 false）。
// Deprecated: 建议使用 LoadForService，它根据 SERVICE_NAME 自动决定依赖要求。
func Load(requireMySQL bool) (*Common, error) {
	etcdPassword, err := readSecretFileEnv("ETCD_PASSWORD_FILE")
	if err != nil {
		return nil, err
	}
	shards, err := parseMySQLShards(getenv("MYSQL_SHARD_DSNS", ""))
	if err != nil {
		return nil, err
	}
	for i := range shards {
		shards[i].DSN = withMySQLUTCSession(shards[i].DSN)
	}
	c := &Common{
		AppEnv:                   getenv("APP_ENV", ""),
		ServiceName:              getenv("SERVICE_NAME", ""),
		InstanceID:               getInstanceID(),
		HTTPAddr:                 getenv("HTTP_LISTEN_ADDR", ":8080"),
		GRPCAddr:                 getenv("GRPC_LISTEN_ADDR", ":9090"),
		DiagAddr:                 getenv("DIAG_LISTEN_ADDR", ":9091"),
		LogLevel:                 getenv("LOG_LEVEL", "info"),
		MySQLDSN:                 withMySQLUTCSession(getenv("MYSQL_DSN", "")),
		MySQLShards:              shards,
		GlobalIDNodeID:           getintNonNegative("GLOBAL_ID_NODE_ID", -1),
		RedisAddrs:               getenv("REDIS_ADDRS", ""),
		RedisPassword:            getenv("REDIS_PASSWORD", ""),
		EtcdEndpoints:            getenv("ETCD_ENDPOINTS", ""),
		EtcdUsername:             getenv("ETCD_USERNAME", ""),
		EtcdPassword:             etcdPassword,
		EtcdPrefix:               getenv("ETCD_PREFIX", "/farm-server/v1"),
		AdvertiseHTTPAddr:        getenv("ADVERTISE_HTTP_ADDR", ""),
		AdvertiseGRPCAddr:        getenv("ADVERTISE_GRPC_ADDR", ""),
		RouteBucketCount:         getint("ROUTE_BUCKET_COUNT", 1024),
		RouteLeaseTTL:            getdur("ROUTE_LEASE_TTL", 15*time.Second),
		RouteReconcileEvery:      getdur("ROUTE_RECONCILE_INTERVAL", time.Second),
		PubSubUnsubscribeWait:    getdur("PUBSUB_UNSUBSCRIBE_DELAY", 5*time.Second),
		EventBusDriver:           getenv("EVENT_BUS_DRIVER", "memory"),
		KafkaBrokers:             getenv("KAFKA_BROKERS", ""),
		KafkaTopic:               getenv("KAFKA_TOPIC", "farm-events"),
		KafkaConsumerGroupPrefix: getenv("KAFKA_CONSUMER_GROUP_PREFIX", "workersvr"),
		KafkaConsumerMaxTries:    getint("KAFKA_CONSUMER_MAX_TRIES", 5),
		KafkaConsumerRetryMin:    getdur("KAFKA_CONSUMER_RETRY_MIN", 250*time.Millisecond),
		KafkaConsumerRetryMax:    getdur("KAFKA_CONSUMER_RETRY_MAX", 4*time.Second),
		OutboxBatchSize:          getint("OUTBOX_RELAY_BATCH_SIZE", 50),
		OutboxPollInterval:       getdur("OUTBOX_RELAY_POLL_INTERVAL", time.Second),
		OutboxDrainInterval:      getdur("OUTBOX_RELAY_DRAIN_INTERVAL", 10*time.Millisecond),
		OutboxLockTTL:            getdur("OUTBOX_RELAY_LOCK_TTL", 30*time.Second),
		OutboxMaxRetry:           getint("OUTBOX_RELAY_MAX_RETRY", 5),
		PetScanInterval:          getdur("PET_SCAN_INTERVAL", time.Second),
		PetScanBatchSize:         getint("PET_SCAN_BATCH_SIZE", 50),
		PetScanWorkers:           getint("PET_SCAN_WORKERS", 4),
		PetScanRatePerSecond:     getint("PET_SCAN_RATE_PER_SECOND", 50),
		PetScanLeaseTTL:          getdur("PET_SCAN_LEASE_TTL", 15*time.Second),
		PetScanIdleMaxBackoff:    getdur("PET_SCAN_IDLE_MAX_BACKOFF", 5*time.Second),
		// 保留期清理（路线 10.2）。设为 0s 可单独关闭某张表的清理。
		// DEAD outbox 与 economy_transactions 不在清理范围内。
		RetentionInterval:       getdur("RETENTION_SCAN_INTERVAL", 5*time.Minute),
		RetentionBatchSize:      getint("RETENTION_BATCH_SIZE", 500),
		RetentionMaxPerRun:      getint("RETENTION_MAX_ROWS_PER_RUN", 20000),
		RetentionOutbox:         getdur("RETENTION_OUTBOX_PUBLISHED", 48*time.Hour),
		RetentionCmdReceipts:    getdur("RETENTION_CMD_RECEIPTS", 72*time.Hour),
		RetentionConsumedEvents: getdur("RETENTION_CONSUMED_EVENTS", 7*24*time.Hour),
		GameSvrAddr:             getenv("GAMESVR_ADDR", "http://localhost:9090"),
		FarmSvrAddr:             getenv("FARMSVR_ADDR", "http://localhost:8081"),
		NativeGRPCAddr:          getenv("NATIVE_GRPC_LISTEN_ADDR", ":9092"),
		GameSvrGRPCAddr:         getenv("GAMESVR_GRPC_ADDR", "localhost:9092"),
		FarmSvrGRPCAddr:         getenv("FARMSVR_GRPC_ADDR", "localhost:9093"),
		RPCTransport:            getenv("RPC_TRANSPORT", "http"),
		RPCDefaultTimeout:       getdur("RPC_DEFAULT_TIMEOUT", 3*time.Second),
		RPCKeepaliveTime:        getdur("RPC_KEEPALIVE_TIME", 30*time.Second),
		RPCKeepaliveTimeout:     getdur("RPC_KEEPALIVE_TIMEOUT", 10*time.Second),
		RPCMaxMessageBytes:      getint("RPC_MAX_MESSAGE_BYTES", 1<<20),
		RPCMaxStreams:           getint("RPC_MAX_CONCURRENT_STREAMS", 128),
		TokenSecret:             getenv("TOKEN_SECRET", "dev-secret-change-in-production"),
		ShutdownTimeout:         getdur("GRACEFUL_SHUTDOWN_TIMEOUT", 30*time.Second),
		ActorShards:             getint("REALTIME_SCHEDULER_SHARD_COUNT", 64),
		ActorWorkers:            getint("REALTIME_WORKER_COUNT", 64),
		ActorFarmQueue:          getint("REALTIME_FARM_FIFO_CAPACITY", 8),
		ActorReadyQueue:         getint("REALTIME_READY_QUEUE_CAPACITY", 128),
		ActorIngress:            getint("REALTIME_INGRESS_CAPACITY", 256),
		ActorMaxActive:          getint("REALTIME_ACTOR_MAX_ACTIVE", 100000),
		ActorEnqueueWait:        getdur("REALTIME_QUEUE_WAIT_TIMEOUT", 5*time.Millisecond),
		ActorExecutionTimeout:   getdur("REALTIME_COMMAND_TIMEOUT", 3*time.Second),
		ActorIdleTTL:            getdur("REALTIME_ACTOR_IDLE_EVICT_AFTER", 5*time.Minute),
		GameMaxInflight:         getint("CORE_GLOBAL_MAX_INFLIGHT", 20),
		MySQLTotalConnections:   getint("MYSQL_TOTAL_CONNECTION_BUDGET", 200),
		GameExpectedInstances:   getint("GAMESVR_EXPECTED_INSTANCES", 8),
		AdmissionWait:           getdur("CORE_ADMISSION_WAIT_TIMEOUT", 5*time.Millisecond),
		AdmissionRetryAfter:     getdur("OVERLOAD_RETRY_AFTER", 50*time.Millisecond),
		GatewayUserRPS:          getint("GATEWAY_HTTP_RPS_PER_USER", 10),
		GatewayUserBurst:        getint("GATEWAY_HTTP_BURST_PER_USER", 20),
		GatewayIPRPS:            getint("GATEWAY_HTTP_RPS_PER_IP", 50),
		GatewayIPBurst:          getint("GATEWAY_HTTP_BURST_PER_IP", 100),
		// Zero preserves existing production behavior. A deployment can opt in
		// to a process-wide bound for non-WebSocket public HTTP requests.
		GatewayMaxInflight:       getint("GATEWAY_GLOBAL_MAX_INFLIGHT", 0),
		GatewayReadCacheTTL:      getdur("GATEWAY_READ_CACHE_TTL", 2*time.Second),
		GatewayReadCacheStaleTTL: getdur("GATEWAY_READ_CACHE_STALE_TTL", 30*time.Second),
		GatewayReadCacheTimeout:  getdur("GATEWAY_READ_CACHE_TIMEOUT", 25*time.Millisecond),
	}
	if os.Getenv("NATIVE_GRPC_LISTEN_ADDR") == "" && c.ServiceName == "farmsvr" {
		c.NativeGRPCAddr = ":9093"
	}
	if len(c.MySQLShards) == 0 && c.MySQLDSN != "" {
		c.MySQLShards = []MySQLShard{{Name: "primary", DSN: c.MySQLDSN}}
	}

	if !validEnvs[c.AppEnv] {
		return nil, fmt.Errorf("APP_ENV 非法或为空: %q（仅 local/devcloud/test/stress）", c.AppEnv)
	}
	if !validServices[c.ServiceName] {
		return nil, fmt.Errorf("SERVICE_NAME 非法或为空: %q（仅 gatesvr/farmsvr/gamesvr/workersvr）", c.ServiceName)
	}
	if c.AppEnv != "local" && c.InstanceID == "local-1" {
		return nil, fmt.Errorf("%s 环境必须设置非默认 INSTANCE_ID", c.AppEnv)
	}
	if requireMySQL && len(c.MySQLShards) == 0 {
		return nil, fmt.Errorf("%s 需要 MYSQL_DSN 或 MYSQL_SHARD_DSNS，但均为空", c.ServiceName)
	}
	if len(c.MySQLShards) > 1 && (c.GlobalIDNodeID < 0 || c.GlobalIDNodeID > 255) {
		return nil, fmt.Errorf("多分片部署必须设置 0..255 的 GLOBAL_ID_NODE_ID")
	}
	if c.EventBusDriver != "memory" && c.EventBusDriver != "kafka" {
		return nil, fmt.Errorf("EVENT_BUS_DRIVER 非法: %q（仅 memory/kafka）", c.EventBusDriver)
	}
	if c.ServiceName == "workersvr" && len(c.MySQLShards) > 1 && c.EventBusDriver == "memory" {
		return nil, fmt.Errorf("workersvr 多分片部署必须使用 Kafka 事件驱动")
	}
	if (c.EtcdUsername == "") != (c.EtcdPassword == "") {
		return nil, fmt.Errorf("ETCD_USERNAME and ETCD_PASSWORD_FILE must be configured together")
	}
	if c.KafkaConsumerMaxTries < 1 {
		return nil, fmt.Errorf("KAFKA_CONSUMER_MAX_TRIES 必须至少为 1")
	}
	if c.KafkaConsumerRetryMin <= 0 || c.KafkaConsumerRetryMax < c.KafkaConsumerRetryMin {
		return nil, fmt.Errorf("kafka consumer retry 必须满足 0 < KAFKA_CONSUMER_RETRY_MIN <= KAFKA_CONSUMER_RETRY_MAX")
	}
	if c.OutboxPollInterval <= 0 || c.OutboxDrainInterval <= 0 || c.OutboxLockTTL <= 0 {
		return nil, fmt.Errorf("outbox relay interval 和 lock TTL 必须大于 0")
	}
	if c.PetScanInterval <= 0 || c.PetScanBatchSize <= 0 || c.PetScanWorkers <= 0 || c.PetScanRatePerSecond <= 0 || c.PetScanLeaseTTL <= 0 {
		return nil, fmt.Errorf("PET_SCAN_INTERVAL、PET_SCAN_BATCH_SIZE、PET_SCAN_WORKERS、PET_SCAN_RATE_PER_SECOND 和 PET_SCAN_LEASE_TTL 必须大于 0")
	}
	if c.PetScanIdleMaxBackoff < c.PetScanInterval {
		return nil, fmt.Errorf("PET_SCAN_IDLE_MAX_BACKOFF 必须不小于 PET_SCAN_INTERVAL")
	}
	petQueueDelay := time.Duration((c.PetScanBatchSize-1+c.PetScanRatePerSecond-1)/c.PetScanRatePerSecond) * time.Second
	minimumPetLeaseTTL := petQueueDelay + c.RPCDefaultTimeout + time.Second
	if c.PetScanLeaseTTL < minimumPetLeaseTTL {
		return nil, fmt.Errorf("PET_SCAN_LEASE_TTL 必须至少为 %s，以覆盖批内限流排队、RPC 和安全余量", minimumPetLeaseTTL)
	}
	if (c.RetentionOutbox > 0 || c.RetentionCmdReceipts > 0 || c.RetentionConsumedEvents > 0) && c.RetentionInterval <= 0 {
		return nil, fmt.Errorf("启用任一 retention window 时 RETENTION_SCAN_INTERVAL 必须大于 0")
	}
	if c.RPCTransport != "http" && c.RPCTransport != "grpc" && c.RPCTransport != "grpc_fallback" {
		return nil, fmt.Errorf("RPC_TRANSPORT 非法: %q（仅 http/grpc/grpc_fallback）", c.RPCTransport)
	}
	if c.RouteLeaseTTL < 5*time.Second {
		return nil, fmt.Errorf("ROUTE_LEASE_TTL 必须至少为 5s")
	}
	if c.EventBusDriver == "memory" && (c.AppEnv == "devcloud" || c.AppEnv == "stress") {
		return nil, fmt.Errorf("memory 事件驱动不得用于 %s 环境", c.AppEnv)
	}
	return c, nil
}

// withMySQLUTCSession makes the SQL session timezone agree with the
// application's UTC persistence contract. DATETIME has no timezone metadata;
// setting this connection variable prevents NOW()/CURRENT_TIMESTAMP and any
// SQL-side date arithmetic from silently switching to the server's local zone.
// The driver applies system-variable DSN parameters to every pooled connection.
func withMySQLUTCSession(dsn string) string {
	if dsn == "" || strings.Contains(strings.ToLower(dsn), "time_zone=") {
		return dsn
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "time_zone=%27%2B00%3A00%27"
}

// parseMySQLShards parses MYSQL_SHARD_DSNS as shard-name=go-mysql-dsn pairs,
// separated by commas.  It is intentionally strict: a duplicate or unnamed
// shard would make user routing non-deterministic across application instances.
func parseMySQLShards(raw string) ([]MySQLShard, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	seen := make(map[string]struct{})
	parts := strings.Split(raw, ",")
	shards := make([]MySQLShard, 0, len(parts))
	for _, part := range parts {
		name, dsn, ok := strings.Cut(strings.TrimSpace(part), "=")
		name, dsn = strings.TrimSpace(name), strings.TrimSpace(dsn)
		if !ok || name == "" || dsn == "" {
			return nil, fmt.Errorf("MYSQL_SHARD_DSNS 必须为 shard=dsn，收到 %q", part)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("MYSQL_SHARD_DSNS 包含重复分片 %q", name)
		}
		seen[name] = struct{}{}
		shards = append(shards, MySQLShard{Name: name, DSN: dsn})
	}
	return shards, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getInstanceID() string {
	if value := strings.TrimSpace(os.Getenv("INSTANCE_ID")); value != "" {
		return value
	}
	return "local-1"
}

func readSecretFileEnv(k string) (string, error) {
	path := strings.TrimSpace(os.Getenv(k))
	if path == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", k, err)
	}
	secret := strings.TrimSpace(string(raw))
	if secret == "" {
		return "", fmt.Errorf("%s points to an empty file", k)
	}
	return secret, nil
}

func getdur(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	return def
}

func getint(k string, def int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil || v <= 0 {
		return def
	}
	return v
}

// getintNonNegative distinguishes an explicit zero from an unset value.
// GLOBAL_ID_NODE_ID encodes zero as a valid node number in a multi-shard ID.
func getintNonNegative(k string, def int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil || v < 0 {
		return def
	}
	return v
}

// LoadForService 按服务名决定必填依赖并 fail-fast 校验。
// 依赖规则（见 04-路线1.4.2）：
//
//	gamesvr / workersvr → MySQL 必填
//	gatesvr / farmsvr   → Redis 必填
//	workersvr           → Kafka 在 driver=kafka 时必填
func LoadForService() (*Common, error) {
	name := os.Getenv("SERVICE_NAME")
	requireMySQL := name == "gamesvr" || name == "workersvr"
	c, err := Load(requireMySQL)
	if err != nil {
		return nil, err
	}
	// Redis：gatesvr 和 farmsvr 在非 local 环境必须配置。
	if (name == "gatesvr" || name == "farmsvr") && c.RedisAddrs == "" && c.AppEnv != "local" {
		return nil, fmt.Errorf("%s 需要 REDIS_ADDRS，但为空", name)
	}
	if (c.AppEnv == "devcloud" || c.AppEnv == "stress") && c.EtcdEndpoints == "" {
		return nil, fmt.Errorf("%s 在 %s 环境需要 ETCD_ENDPOINTS", name, c.AppEnv)
	}
	if c.EtcdEndpoints != "" && (name == "farmsvr" || name == "gamesvr") && (c.AdvertiseHTTPAddr == "" || c.AdvertiseGRPCAddr == "") {
		return nil, fmt.Errorf("%s 启用 etcd 时需要 ADVERTISE_HTTP_ADDR 和 ADVERTISE_GRPC_ADDR", name)
	}
	// Kafka：workersvr 且 driver=kafka 时必须配置。
	if name == "workersvr" && c.EventBusDriver == "kafka" && c.KafkaBrokers == "" {
		return nil, fmt.Errorf("workersvr Kafka 驱动需要 KAFKA_BROKERS，但为空")
	}
	return c, nil
}
