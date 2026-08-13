// Package probe 提供真实依赖探针函数，供各服务注册到 app.ReadinessCheck。
// Route 1 阶段：仅 TCP 层连通探测（确认端口可达）。
// Route 2 阶段：gamesvr 引入 MySQL 驱动后可升级为 SELECT 1 探测。
package probe

import (
	"context"
	"database/sql"
	"fmt"
	"net"
)

// TCP 返回一个 ReadinessCheck：尝试与目标地址建立 TCP 连接。
// addr 格式为 "host:port"。
func TCP(name, addr string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if addr == "" {
			return fmt.Errorf("%s: address empty", name)
		}
		d := net.Dialer{}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("%s tcp dial %s: %w", name, addr, err)
		}
		conn.Close()
		return nil
	}
}

// MySQL 返回一个复用业务连接池的 ReadinessCheck。
func MySQL(db *sql.DB) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if db == nil {
			return fmt.Errorf("mysql: db is nil")
		}
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("mysql ping: %w", err)
		}
		return nil
	}
}

// Redis 返回一个 ReadinessCheck：TCP 连通探测 Redis 端口。
// addr 为单个 "host:port"。
func Redis(addr string) func(ctx context.Context) error {
	return TCP("redis", firstBroker(addr))
}

// Etcd probes the first endpoint; the client itself maintains the full list.
func Etcd(endpoints string) func(ctx context.Context) error {
	return TCP("etcd", firstBroker(endpoints))
}

// Kafka 返回一个 ReadinessCheck：TCP 连通探测第一个 Kafka broker 端口。
// brokers 为逗号分隔的 "host:port" 列表。
func Kafka(brokers string) func(ctx context.Context) error {
	addr := firstBroker(brokers)
	return TCP("kafka", addr)
}

// extractMySQLAddr 从 DSN 中提取 host:port。
// 支持 "user:pass@tcp(host:port)/db" 格式。
func extractMySQLAddr(dsn string) string {
	// 查找 @tcp( 结构。
	const prefix = "@tcp("
	i := 0
	for i < len(dsn)-len(prefix) {
		if dsn[i:i+len(prefix)] == prefix {
			rest := dsn[i+len(prefix):]
			j := 0
			for j < len(rest) && rest[j] != ')' {
				j++
			}
			return rest[:j]
		}
		i++
	}
	return dsn // 无法解析时原样返回
}

// firstBroker 返回逗号分隔列表中的第一个 broker 地址。
func firstBroker(brokers string) string {
	for i := 0; i < len(brokers); i++ {
		if brokers[i] == ',' {
			return brokers[:i]
		}
	}
	return brokers
}
