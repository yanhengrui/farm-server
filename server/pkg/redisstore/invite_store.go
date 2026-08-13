package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const inviteTTL = 30 * time.Minute

// ErrInviteNotFound 表示邀请码不存在或已过期。
var ErrInviteNotFound = errors.New("invite not found or expired")

// InviteStore 管理邀请码的 Redis 存储。
//
// 数据结构：
//
//	invite:{code}      = inviter_user_id 字符串  TTL=30min（主键）
//	invite:user:{uid}  = code 字符串             TTL=30min（复用反查）
//
// 邀请码在有效期内可被多人使用（不会因消费而失效），TTL 到期后 Redis 自动清除。
type InviteStore struct {
	rdb *redis.Client
}

// NewInviteStore 构造 InviteStore。
func NewInviteStore(rdb *redis.Client) *InviteStore {
	return &InviteStore{rdb: rdb}
}

func inviteCodeKey(code string) string  { return "invite:" + code }
func inviteUserKey(userID int64) string { return fmt.Sprintf("invite:user:%d", userID) }

// GetOrCreate 复用或生成邀请码：
// - 若 inviter 已有未过期的 code，直接返回旧码
// - 否则用传入的 newCode 写入两条 key，返回 newCode
// newCode 由调用方生成（如 id.NewV7()），保持无状态。
func (s *InviteStore) GetOrCreate(ctx context.Context, inviterUserID int64, newCode string) (string, error) {
	userKey := inviteUserKey(inviterUserID)

	// 尝试复用已有码（可能刚过期，GET 返回空则重建）。
	existing, err := s.rdb.Get(ctx, userKey).Result()
	if err == nil && existing != "" {
		// 检查主键是否仍有效（防止 userKey 还在但 codeKey 已过期的窗口）。
		ttl, err2 := s.rdb.TTL(ctx, inviteCodeKey(existing)).Result()
		if err2 == nil && ttl > 0 {
			return existing, nil
		}
	}

	// 写入两条 key，使用 Pipeline 保证原子性（不用 MULTI/EXEC，允许极小窗口）。
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, inviteCodeKey(newCode), fmt.Sprintf("%d", inviterUserID), inviteTTL)
	pipe.Set(ctx, userKey, newCode, inviteTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("invite_store set: %w", err)
	}
	return newCode, nil
}

// GetInviter 根据邀请码查询邀请人 user_id。
// 码不存在或已过期返回 ErrInviteNotFound。
func (s *InviteStore) GetInviter(ctx context.Context, code string) (int64, error) {
	val, err := s.rdb.Get(ctx, inviteCodeKey(code)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrInviteNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("invite_store get: %w", err)
	}
	var inviterID int64
	if _, err := fmt.Sscanf(val, "%d", &inviterID); err != nil {
		return 0, fmt.Errorf("invite_store parse: %w", err)
	}
	return inviterID, nil
}
