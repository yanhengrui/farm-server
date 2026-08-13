package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const sessionTTL = 24 * time.Hour

// Session 是 gatesvr 存储的用户会话快照（无状态实例恢复用）。
type Session struct {
	UserID        int64
	FarmID        int64
	LastSeq       int64 // 上次确认的 server_seq
	DeviceID      string
	OwnerInstance string
	OwnerEpoch    int64
	Status        string
}

// SessionStore 负责 Redis Hash 形式的会话存取。
type SessionStore struct {
	rdb *redis.Client
}

// NewSessionStore 构造 SessionStore。
func NewSessionStore(rdb *redis.Client) *SessionStore {
	return &SessionStore{rdb: rdb}
}

func sessKey(sessionID string) string { return "sess:" + sessionID }

// Save 写入或覆盖一条会话（TTL 重置为 24h）。
func (s *SessionStore) Save(ctx context.Context, sessionID string, sess Session) error {
	key := sessKey(sessionID)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"user_id", sess.UserID,
		"farm_id", sess.FarmID,
		"last_seq", sess.LastSeq,
		"device_id", sess.DeviceID,
		"owner_instance", sess.OwnerInstance,
		"owner_epoch", sess.OwnerEpoch,
		"status", sess.Status,
	)
	pipe.Expire(ctx, key, sessionTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Load 读取会话；键不存在返回 redis.Nil 错误。
func (s *SessionStore) Load(ctx context.Context, sessionID string) (Session, error) {
	m, err := s.rdb.HGetAll(ctx, sessKey(sessionID)).Result()
	if err != nil {
		return Session{}, fmt.Errorf("load session %s: %w", sessionID, err)
	}
	if len(m) == 0 {
		return Session{}, fmt.Errorf("load session %s: %w", sessionID, redis.Nil)
	}
	sess := Session{DeviceID: m["device_id"], OwnerInstance: m["owner_instance"], Status: m["status"]}
	sess.UserID, _ = strconv.ParseInt(m["user_id"], 10, 64)
	sess.FarmID, _ = strconv.ParseInt(m["farm_id"], 10, 64)
	sess.LastSeq, _ = strconv.ParseInt(m["last_seq"], 10, 64)
	sess.OwnerEpoch, _ = strconv.ParseInt(m["owner_epoch"], 10, 64)
	return sess, nil
}

var claimSessionScript = redis.NewScript(`
local epoch = redis.call("HINCRBY", KEYS[1], "owner_epoch", 1)
redis.call("HSET", KEYS[1],
  "user_id", ARGV[1], "farm_id", ARGV[2], "owner_instance", ARGV[3],
  "status", "ACTIVE")
redis.call("HSETNX", KEYS[1], "last_seq", 0)
redis.call("EXPIRE", KEYS[1], ARGV[4])
return epoch
`)

// Claim atomically transfers a session to this gatesvr and returns a monotonic
// owner epoch. It is connection metadata only; reconnect still fetches Snapshot.
func (s *SessionStore) Claim(ctx context.Context, sessionID string, userID, farmID int64, instanceID string) (int64, error) {
	value, err := claimSessionScript.Run(ctx, s.rdb, []string{sessKey(sessionID)}, userID, farmID, instanceID, int64(sessionTTL/time.Second)).Int64()
	if err != nil {
		return 0, fmt.Errorf("claim session %s: %w", sessionID, err)
	}
	return value, nil
}

var handoffScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "owner_instance") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "owner_epoch") ~= ARGV[2] then
  return 0
end
redis.call("HSET", KEYS[1], "status", "HANDOFF_PENDING")
redis.call("SET", KEYS[2], ARGV[3], "EX", ARGV[4])
return 1
`)

func resumeKey(ticket string) string { return "ws:resume:" + ticket }

// BeginHandoff marks only the current owner epoch pending and creates a short
// lived one-time resume ticket. The ticket carries no farm state or events.
func (s *SessionStore) BeginHandoff(ctx context.Context, sessionID, instanceID string, ownerEpoch int64, ticket string, ttl time.Duration) error {
	result, err := handoffScript.Run(ctx, s.rdb, []string{sessKey(sessionID), resumeKey(ticket)}, instanceID, ownerEpoch, sessionID, int64(ttl/time.Second)).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return fmt.Errorf("session %s is no longer owned by epoch %d", sessionID, ownerEpoch)
	}
	return nil
}

var consumeResumeScript = redis.NewScript(`
local session_id = redis.call("GET", KEYS[1])
if not session_id or session_id ~= ARGV[1] then return 0 end
redis.call("DEL", KEYS[1])
return 1
`)

func (s *SessionStore) ConsumeResume(ctx context.Context, ticket, sessionID string) error {
	result, err := consumeResumeScript.Run(ctx, s.rdb, []string{resumeKey(ticket)}, sessionID).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return fmt.Errorf("resume ticket invalid or expired")
	}
	return nil
}

// UpdateLastSeq 原子更新会话的 last_seq 字段并刷新 TTL。
func (s *SessionStore) UpdateLastSeq(ctx context.Context, sessionID string, seq int64) error {
	key := sessKey(sessionID)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, "last_seq", seq)
	pipe.Expire(ctx, key, sessionTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// Delete 删除会话（强制下线时调用）。
func (s *SessionStore) Delete(ctx context.Context, sessionID string) error {
	return s.rdb.Del(ctx, sessKey(sessionID)).Err()
}
