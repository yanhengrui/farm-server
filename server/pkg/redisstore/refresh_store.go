package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const refreshTokenTTL = 7 * 24 * time.Hour

// RefreshStore 存储 sessionID、refresh token hash 与 userID 的绑定，TTL 自动过期。
// 替代 MySQL sessions 表中的 refresh_token_hash 列，避免 30M DAU 下行数无上限增长。
type RefreshStore struct {
	rdb *redis.Client
}

// NewRefreshStore 构造 RefreshStore。
func NewRefreshStore(rdb *redis.Client) *RefreshStore {
	return &RefreshStore{rdb: rdb}
}

func refreshKey(tokenHash string) string {
	return fmt.Sprintf("refresh:%s", tokenHash)
}

func authSessionKey(sessionID string) string { return "auth:session:" + sessionID }

// IsActive verifies that a signed access token still belongs to the active
// Redis session that created it. It is used by gatesvr for immediate logout
// invalidation without adding a MySQL read to every authenticated request.
func (s *RefreshStore) IsActive(ctx context.Context, sessionID string, userID int64) (bool, error) {
	values, err := s.rdb.HMGet(ctx, authSessionKey(sessionID), "user_id", "status").Result()
	if err != nil {
		return false, fmt.Errorf("load auth session: %w", err)
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return false, nil
	}
	storedUserID, err := strconv.ParseInt(fmt.Sprint(values[0]), 10, 64)
	if err != nil {
		return false, nil
	}
	return storedUserID == userID && fmt.Sprint(values[1]) == "ACTIVE", nil
}

// Create 原子创建绑定 sessionID 的七天刷新会话。
func (s *RefreshStore) Create(ctx context.Context, sessionID, tokenHash string, userID int64) error {
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, authSessionKey(sessionID), "user_id", userID, "refresh_hash", tokenHash, "status", "ACTIVE")
	pipe.Expire(ctx, authSessionKey(sessionID), refreshTokenTTL)
	// 反向索引继续保存纯数字 userID，使滚动更新期间的旧 gamesvr 仍可读取。
	pipe.Set(ctx, refreshKey(tokenHash), userID, refreshTokenTTL)
	_, err := pipe.Exec(ctx)
	return err
}

var rotateRefreshScript = redis.NewScript(`
local session_key = KEYS[1]
local old_refresh_key = KEYS[2]
local new_refresh_key = KEYS[3]
local expected_session = ARGV[1]
local old_hash = ARGV[2]
local new_hash = ARGV[3]
local ttl = tonumber(ARGV[4])
if redis.call('HGET', session_key, 'status') ~= 'ACTIVE' then return 0 end
if redis.call('HGET', session_key, 'refresh_hash') ~= old_hash then return 0 end
local user_id = redis.call('HGET', session_key, 'user_id')
if not user_id then return 0 end
if redis.call('GET', old_refresh_key) ~= user_id then return 0 end
redis.call('DEL', old_refresh_key)
redis.call('HSET', session_key, 'refresh_hash', new_hash)
redis.call('EXPIRE', session_key, ttl)
redis.call('SET', new_refresh_key, user_id, 'EX', ttl)
return user_id
`)

// Rotate 校验 sessionID 与旧 refresh token 的绑定关系，并原子轮换 token。
func (s *RefreshStore) Rotate(ctx context.Context, sessionID, oldHash, newHash string) (int64, error) {
	userID, err := rotateRefreshScript.Run(ctx, s.rdb,
		[]string{authSessionKey(sessionID), refreshKey(oldHash), refreshKey(newHash)},
		sessionID, oldHash, newHash, int64(refreshTokenTTL/time.Second)).Int64()
	if err != nil {
		return 0, fmt.Errorf("rotate refresh session: %w", err)
	}
	return userID, nil
}

var migrateLegacyRefreshScript = redis.NewScript(`
local session_key = KEYS[1]
local old_refresh_key = KEYS[2]
local new_refresh_key = KEYS[3]
local old_hash = ARGV[1]
local new_hash = ARGV[2]
local ttl = tonumber(ARGV[3])
if redis.call('EXISTS', session_key) == 1 then return 0 end
local user_id = redis.call('GET', old_refresh_key)
if not user_id or not tonumber(user_id) then return 0 end
redis.call('DEL', old_refresh_key)
redis.call('HSET', session_key, 'user_id', user_id, 'refresh_hash', new_hash, 'status', 'ACTIVE')
redis.call('EXPIRE', session_key, ttl)
redis.call('SET', new_refresh_key, user_id, 'EX', ttl)
return user_id
`)

// MigrateLegacy 原子接管旧 gamesvr 创建的 tokenHash → userID 会话。
func (s *RefreshStore) MigrateLegacy(ctx context.Context, sessionID, oldHash, newHash string) (int64, error) {
	userID, err := migrateLegacyRefreshScript.Run(ctx, s.rdb,
		[]string{authSessionKey(sessionID), refreshKey(oldHash), refreshKey(newHash)},
		oldHash, newHash, int64(refreshTokenTTL/time.Second)).Int64()
	if err != nil {
		return 0, fmt.Errorf("migrate legacy refresh session: %w", err)
	}
	return userID, nil
}

var deleteRefreshScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'refresh_hash') ~= ARGV[1] then return 0 end
local user_id = redis.call('HGET', KEYS[1], 'user_id')
if not user_id or redis.call('GET', KEYS[2]) ~= user_id then return 0 end
redis.call('DEL', KEYS[1], KEYS[2])
local conn = redis.call('GET', 'ws:conn:' .. user_id)
if conn then
  local sep = string.find(conn, ':', 1, true)
  if sep then
    local instance_id = string.sub(conn, 1, sep - 1)
    redis.call('PUBLISH', 'ws:kick:instance:' .. instance_id, user_id .. '|logout:' .. ARGV[2])
  end
end
return 1
`)

// DeleteSession 仅在 refresh token 与 sessionID 匹配时撤销会话。
func (s *RefreshStore) DeleteSession(ctx context.Context, sessionID, tokenHash string) (bool, error) {
	deleted, err := deleteRefreshScript.Run(ctx, s.rdb,
		[]string{authSessionKey(sessionID), refreshKey(tokenHash)}, tokenHash, sessionID).Int()
	if err != nil {
		return false, fmt.Errorf("delete refresh session: %w", err)
	}
	return deleted == 1, nil
}
