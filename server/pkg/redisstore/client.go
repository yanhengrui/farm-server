// Package redisstore 封装 Redis 连接与各类 gatesvr 状态存储。
// 单机模式：REDIS_ADDRS 为单个 host:port；集群模式待扩展。
package redisstore

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// New 构建 Redis 客户端。addrs 为逗号分隔的 host:port 列表（本期只取第一个）。
func New(addrs, password string) *redis.Client {
	addr := strings.SplitN(strings.TrimSpace(addrs), ",", 2)[0]
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
}

// Ping 检测 Redis 连通性，用于就绪探针。
func Ping(ctx context.Context, rdb *redis.Client) error {
	return rdb.Ping(ctx).Err()
}
