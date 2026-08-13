package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEtcdCredentialsFromPasswordFile(t *testing.T) {
	baseEnv(t, "gatesvr")
	passwordPath := filepath.Join(t.TempDir(), "etcd-password")
	if err := os.WriteFile(passwordPath, []byte("strong-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETCD_USERNAME", "farm-app")
	t.Setenv("ETCD_PASSWORD_FILE", passwordPath)
	cfg, err := Load(false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EtcdUsername != "farm-app" || cfg.EtcdPassword != "strong-password" {
		t.Fatalf("unexpected etcd credentials: username=%q password_length=%d", cfg.EtcdUsername, len(cfg.EtcdPassword))
	}
}

func TestEtcdCredentialsMustBeConfiguredTogether(t *testing.T) {
	baseEnv(t, "gatesvr")
	t.Setenv("ETCD_USERNAME", "farm-app")
	if _, err := Load(false); err == nil {
		t.Fatal("expected missing ETCD_PASSWORD_FILE error")
	}
}

func baseEnv(t *testing.T, service string) {
	t.Helper()
	t.Setenv("APP_ENV", "local")
	t.Setenv("SERVICE_NAME", service)
	t.Setenv("INSTANCE_ID", "test-1")
	t.Setenv("EVENT_BUS_DRIVER", "memory")
	t.Setenv("NATIVE_GRPC_LISTEN_ADDR", "")
}

func TestFarmSvrNativeGRPCDefault(t *testing.T) {
	baseEnv(t, "farmsvr")
	cfg, err := Load(false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NativeGRPCAddr != ":9093" || cfg.RPCTransport != "http" {
		t.Fatalf("native=%q transport=%q", cfg.NativeGRPCAddr, cfg.RPCTransport)
	}
}

func TestRejectsInvalidRPCTransport(t *testing.T) {
	baseEnv(t, "gatesvr")
	t.Setenv("RPC_TRANSPORT", "automatic")
	if _, err := Load(false); err == nil {
		t.Fatal("expected invalid RPC_TRANSPORT error")
	}
}

func TestMultiInstanceRequiresEtcdAndAdvertisedEndpoints(t *testing.T) {
	baseEnv(t, "farmsvr")
	t.Setenv("APP_ENV", "stress")
	t.Setenv("REDIS_ADDRS", "redis:6379")
	t.Setenv("EVENT_BUS_DRIVER", "kafka")
	if _, err := LoadForService(); err == nil {
		t.Fatal("stress must require ETCD_ENDPOINTS")
	}
	t.Setenv("ETCD_ENDPOINTS", "etcd:2379")
	if _, err := LoadForService(); err == nil {
		t.Fatal("registered farmsvr must advertise endpoints")
	}
	t.Setenv("ADVERTISE_HTTP_ADDR", "http://farm-a:8080")
	t.Setenv("ADVERTISE_GRPC_ADDR", "farm-a:9093")
	if _, err := LoadForService(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiShardWorkerRejectsMemoryEventBus(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "")
	t.Setenv("MYSQL_SHARD_DSNS", "shard-0=user:pass@tcp(mysql-a:3306)/farm,shard-1=user:pass@tcp(mysql-b:3306)/farm")
	t.Setenv("GLOBAL_ID_NODE_ID", "1")
	t.Setenv("EVENT_BUS_DRIVER", "memory")
	if _, err := LoadForService(); err == nil {
		t.Fatal("multi-shard workersvr unexpectedly accepted memory event bus")
	}
}

func TestConnectionBudgetDefaults(t *testing.T) {
	baseEnv(t, "gamesvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQLTotalConnections != 200 || cfg.GameExpectedInstances != 8 {
		t.Fatalf("budget=%d instances=%d", cfg.MySQLTotalConnections, cfg.GameExpectedInstances)
	}
}

func TestMySQLDSNUsesUTCSession(t *testing.T) {
	baseEnv(t, "gamesvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm?parseTime=true&loc=UTC")
	t.Setenv("MYSQL_SHARD_DSNS", "shard-0=user:pass@tcp(mysql-a:3306)/farm?parseTime=true&loc=UTC,shard-1=user:pass@tcp(mysql-b:3306)/farm?parseTime=true&loc=UTC")
	t.Setenv("GLOBAL_ID_NODE_ID", "1")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	const utcParam = "time_zone=%27%2B00%3A00%27"
	if !strings.Contains(cfg.MySQLDSN, utcParam) {
		t.Fatalf("primary DSN lacks UTC session parameter: %q", cfg.MySQLDSN)
	}
	for _, shard := range cfg.MySQLShards {
		if !strings.Contains(shard.DSN, utcParam) {
			t.Fatalf("shard %q DSN lacks UTC session parameter: %q", shard.Name, shard.DSN)
		}
	}
}

func TestGatewayMaxInflightDefaultAndOverride(t *testing.T) {
	baseEnv(t, "gatesvr")
	cfg, err := Load(false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GatewayMaxInflight != 0 {
		t.Fatalf("default max inflight=%d, want 0", cfg.GatewayMaxInflight)
	}
	t.Setenv("GATEWAY_GLOBAL_MAX_INFLIGHT", "1024")
	cfg, err = Load(false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GatewayMaxInflight != 1024 {
		t.Fatalf("override max inflight=%d, want 1024", cfg.GatewayMaxInflight)
	}
}

func TestKafkaConsumerFailurePolicyDefaults(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaConsumerMaxTries != 5 || cfg.KafkaConsumerRetryMin != 250*time.Millisecond || cfg.KafkaConsumerRetryMax != 4*time.Second {
		t.Fatalf("Kafka consumer failure policy: tries=%d min=%s max=%s",
			cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax)
	}
	if cfg.KafkaTopic != "farm-events" || cfg.KafkaConsumerGroupPrefix != "workersvr" {
		t.Fatalf("Kafka isolation defaults: topic=%q prefix=%q", cfg.KafkaTopic, cfg.KafkaConsumerGroupPrefix)
	}
	if cfg.OutboxBatchSize != 50 || cfg.OutboxPollInterval != time.Second || cfg.OutboxDrainInterval != 10*time.Millisecond || cfg.OutboxLockTTL != 30*time.Second || cfg.OutboxMaxRetry != 5 {
		t.Fatalf("outbox defaults: batch=%d poll=%s drain=%s lock=%s retry=%d",
			cfg.OutboxBatchSize, cfg.OutboxPollInterval, cfg.OutboxDrainInterval, cfg.OutboxLockTTL, cfg.OutboxMaxRetry)
	}
}

func TestPetSchedulerDefaultsAndValidation(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PetScanInterval != time.Second || cfg.PetScanBatchSize != 50 || cfg.PetScanWorkers != 4 || cfg.PetScanRatePerSecond != 50 || cfg.PetScanLeaseTTL != 15*time.Second || cfg.PetScanIdleMaxBackoff != 5*time.Second {
		t.Fatalf("unexpected pet scheduler defaults: %+v", cfg)
	}
	t.Setenv("PET_SCAN_IDLE_MAX_BACKOFF", "500ms")
	if _, err := LoadForService(); err == nil {
		t.Fatal("expected idle backoff below scan interval to be rejected")
	}
}

func TestPetSchedulerRejectsLeaseShorterThanQueuedWork(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	t.Setenv("PET_SCAN_BATCH_SIZE", "50")
	t.Setenv("PET_SCAN_RATE_PER_SECOND", "1")
	t.Setenv("PET_SCAN_LEASE_TTL", "1s")
	if _, err := LoadForService(); err == nil {
		t.Fatal("expected lease TTL shorter than queued work to be rejected")
	}
}

func TestRetentionRequiresPositiveScanIntervalWhenEnabled(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	t.Setenv("RETENTION_SCAN_INTERVAL", "0s")
	if _, err := LoadForService(); err == nil {
		t.Fatal("expected enabled retention with zero scan interval to be rejected")
	}
	t.Setenv("RETENTION_OUTBOX_PUBLISHED", "0s")
	t.Setenv("RETENTION_CMD_RECEIPTS", "0s")
	t.Setenv("RETENTION_CONSUMED_EVENTS", "0s")
	if _, err := LoadForService(); err != nil {
		t.Fatalf("all retention windows disabled should allow zero interval: %v", err)
	}
}

func TestMultiShardAllowsGlobalIDNodeZero(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "")
	t.Setenv("MYSQL_SHARD_DSNS", "shard-0=user:pass@tcp(mysql-a:3306)/farm,shard-1=user:pass@tcp(mysql-b:3306)/farm")
	t.Setenv("GLOBAL_ID_NODE_ID", "0")
	t.Setenv("EVENT_BUS_DRIVER", "kafka")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GlobalIDNodeID != 0 {
		t.Fatalf("node id=%d, want 0", cfg.GlobalIDNodeID)
	}
}

func TestNonLocalEnvironmentRejectsDefaultInstanceID(t *testing.T) {
	baseEnv(t, "gatesvr")
	t.Setenv("APP_ENV", "test")
	t.Setenv("EVENT_BUS_DRIVER", "kafka")
	t.Setenv("INSTANCE_ID", "")
	if _, err := Load(false); err == nil {
		t.Fatal("expected default INSTANCE_ID to be rejected outside local")
	}
}

func TestKafkaAndRelayIsolationOverrides(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	t.Setenv("KAFKA_TOPIC", "route95-events")
	t.Setenv("KAFKA_CONSUMER_GROUP_PREFIX", "route95-workersvr")
	t.Setenv("OUTBOX_RELAY_BATCH_SIZE", "1000")
	t.Setenv("OUTBOX_RELAY_POLL_INTERVAL", "100ms")
	t.Setenv("OUTBOX_RELAY_DRAIN_INTERVAL", "2ms")
	t.Setenv("OUTBOX_RELAY_LOCK_TTL", "45s")
	t.Setenv("OUTBOX_RELAY_MAX_RETRY", "9")
	cfg, err := LoadForService()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaTopic != "route95-events" || cfg.KafkaConsumerGroupPrefix != "route95-workersvr" || cfg.OutboxBatchSize != 1000 || cfg.OutboxPollInterval != 100*time.Millisecond || cfg.OutboxDrainInterval != 2*time.Millisecond || cfg.OutboxLockTTL != 45*time.Second || cfg.OutboxMaxRetry != 9 {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}

func TestRejectsInvalidKafkaConsumerFailurePolicy(t *testing.T) {
	baseEnv(t, "workersvr")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/farm")
	t.Setenv("KAFKA_CONSUMER_MAX_TRIES", "2")
	t.Setenv("KAFKA_CONSUMER_RETRY_MIN", "2s")
	t.Setenv("KAFKA_CONSUMER_RETRY_MAX", "1s")
	if _, err := LoadForService(); err == nil {
		t.Fatal("expected invalid Kafka consumer retry range error")
	}
}
