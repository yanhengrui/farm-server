// Package infrastructure 提供 social 领域的 MySQL 实现。
// 邀请码使用 Redis 存储（InviteStore），好友关系持久化到 MySQL（friendships）。
package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
	socialdomain "github.com/photon/farm-server/server/internal/social/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/mysqlretry"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/redisstore"
)

// MySQLSocialService 实现 social/domain.SocialService。
// 邀请码走 InviteStore（Redis）；好友关系走 MySQL（friendships）。
type MySQLSocialService struct {
	db     *sql.DB
	invite *redisstore.InviteStore
}

// NewMySQLSocialService 构造 MySQLSocialService。invite 不可为 nil。
func NewMySQLSocialService(db *sql.DB, invite *redisstore.InviteStore) *MySQLSocialService {
	return &MySQLSocialService{db: db, invite: invite}
}

// CreateInvite 复用或生成 Redis 邀请码（有效期 30min，有效期内可被多人使用）。
func (s *MySQLSocialService) CreateInvite(ctx context.Context, inviterUserID int64) (string, error) {
	return s.invite.GetOrCreate(ctx, inviterUserID, id.NewV7())
}

// AcceptInvite 验证 Redis 邀请码，在 MySQL 事务内建立双向好友关系。
// 已是好友时幂等返回 nil；Redis 码过期时返回 SocialInviteExpired。
func (s *MySQLSocialService) AcceptInvite(ctx context.Context, inviteCode string, accepterUserID int64) error {
	// 1. 从 Redis 获取邀请人（码不存在/过期立即返回）。
	inviterID, err := s.invite.GetInviter(ctx, inviteCode)
	if errors.Is(err, redisstore.ErrInviteNotFound) {
		return errcode.New(errcode.SocialInviteExpired, "invite not found or expired")
	}
	if err != nil {
		return fmt.Errorf("get_inviter: %w", err)
	}

	// 2. 不允许自邀。
	if inviterID == accepterUserID {
		return errcode.New(errcode.SocialSelfInvite, "cannot accept your own invite")
	}

	// 3. 规范化好友对（小 ID 放 user_id_a），并以自然唯一键执行
	// 有限重试；并发插入的 1062 会在下一次查询中变成幂等成功。
	a, b := inviterID, accepterUserID
	if a > b {
		a, b = b, a
	}
	return mysqlretry.Do(ctx, func(err error) bool {
		return mysqlretry.IsTransient(err) || mysqlretry.IsDuplicateKey(err)
	}, func() error {
		return s.acceptInviteOnce(ctx, inviterID, accepterUserID, a, b)
	})
}

func (s *MySQLSocialService) acceptInviteOnce(ctx context.Context, inviterID, accepterUserID, a, b int64) error {
	// MySQL 事务：幂等建立好友关系 + 写 outbox。
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin_tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// 检查是否已经是好友（幂等）。
	const qCheck = `SELECT 1 FROM friendships WHERE user_id_a = ? AND user_id_b = ? LIMIT 1`
	var dummy int
	err = tx.QueryRowContext(ctx, qCheck, uint64(a), uint64(b)).Scan(&dummy)
	if err == nil {
		return tx.Commit() // 已是好友，幂等成功。
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check_friendship: %w", err)
	}

	// 插入好友关系。
	now := time.Now().UTC()
	const qInsert = `INSERT INTO friendships (user_id_a, user_id_b, created_at) VALUES (?,?,?)`
	if _, err := tx.ExecContext(ctx, qInsert, uint64(a), uint64(b), now); err != nil {
		return fmt.Errorf("insert_friendship: %w", err)
	}

	// 写入 outbox 事件，workersvr 消费后向双方发邮件通知。
	if err := insertFriendOutbox(ctx, tx, inviterID, accepterUserID, a, b, now); err != nil {
		return fmt.Errorf("insert_friend_outbox: %w", err)
	}

	return tx.Commit()
}

// AreFriends 检查两个用户是否已存在好友关系。
func (s *MySQLSocialService) AreFriends(ctx context.Context, userIDA, userIDB int64) (bool, error) {
	a, b := userIDA, userIDB
	if a > b {
		a, b = b, a
	}
	const q = `SELECT 1 FROM friendships WHERE user_id_a = ? AND user_id_b = ? LIMIT 1`
	var dummy int
	err := s.db.QueryRowContext(ctx, q, uint64(a), uint64(b)).Scan(&dummy)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("are_friends: %w", err)
	}
	return true, nil
}

// ListFriends 返回 userID 的所有好友；friendships 规范化存储，需同时查两侧。
func (s *MySQLSocialService) ListFriends(ctx context.Context, userID int64) ([]socialdomain.FriendInfo, error) {
	const q = `
		SELECT related.friend_id,
		       COALESCE(NULLIF(TRIM(a.display_name), ''), CAST(related.friend_id AS CHAR)) AS display_name
		FROM (
			SELECT f.friendship_id,
			       CASE WHEN f.user_id_a = ? THEN f.user_id_b ELSE f.user_id_a END AS friend_id
			FROM friendships AS f
			WHERE f.user_id_a = ? OR f.user_id_b = ?
		) AS related
		LEFT JOIN accounts AS a ON a.user_id = related.friend_id
		ORDER BY related.friendship_id
	`
	rows, err := s.db.QueryContext(ctx, q, uint64(userID), uint64(userID), uint64(userID))
	if err != nil {
		return nil, fmt.Errorf("list_friends: %w", err)
	}
	defer rows.Close()

	var friends []socialdomain.FriendInfo
	for rows.Next() {
		var uid uint64
		var displayName string
		if err := rows.Scan(&uid, &displayName); err != nil {
			return nil, fmt.Errorf("list_friends scan: %w", err)
		}
		if strings.TrimSpace(displayName) == "" {
			displayName = strconv.FormatUint(uid, 10)
		}
		friends = append(friends, socialdomain.FriendInfo{UserID: int64(uid), DisplayName: displayName})
	}
	return friends, rows.Err()
}

// insertFriendOutbox 在 AcceptInvite 事务内写入 social.friend_accepted.v1 事件。
// 同时查询双方 display_name，查询失败时降级为 ID 字符串。
func insertFriendOutbox(ctx context.Context, tx *sql.Tx, inviterID, accepterID, uidA, uidB int64, now time.Time) error {
	return insertFriendAcceptedOutbox(ctx, tx, inviterID, accepterID, uidA, uidB,
		loadDisplayName(ctx, tx, inviterID),
		loadDisplayName(ctx, tx, accepterID),
		now,
	)
}

// insertFriendAcceptedOutbox writes the existing public friendship-completed
// event. Cross-shard Saga confirmation supplies names captured before the
// request; same-shard acceptance loads them inside its local transaction.
func insertFriendAcceptedOutbox(ctx context.Context, tx *sql.Tx, inviterID, accepterID, uidA, uidB int64, inviterDisplayName, accepterDisplayName string, now time.Time) error {
	eventID := id.NewV7()
	pairID := strconv.FormatInt(uidA, 10) + ":" + strconv.FormatInt(uidB, 10)
	inviterDisplayName = normalizeFriendDisplayName(inviterDisplayName, inviterID)
	accepterDisplayName = normalizeFriendDisplayName(accepterDisplayName, accepterID)

	payload := socialevents.FriendAcceptedPayload{
		UserIDA:             strconv.FormatInt(uidA, 10),
		UserIDB:             strconv.FormatInt(uidB, 10),
		InviterID:           strconv.FormatInt(inviterID, 10),
		AccepterID:          strconv.FormatInt(accepterID, 10),
		InviterDisplayName:  inviterDisplayName,
		AccepterDisplayName: accepterDisplayName,
		AcceptedAt:          now,
	}
	envelope := socialevents.EventEnvelope{
		EventID:       eventID,
		EventType:     socialevents.EventTypeFriendAccepted,
		AggregateType: "social",
		AggregateID:   pairID,
		SchemaVersion: 1,
		OccurredAt:    now,
		TraceID:       observability.TraceID(ctx),
		CorrelationID: eventID,
		Payload:       payload,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal_friend_outbox: %w", err)
	}

	const q = `
		INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, schema_version, payload, status, available_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)`
	_, err = tx.ExecContext(ctx, q,
		eventID, "social", uint64(uidA), pairID,
		string(socialevents.EventTypeFriendAccepted),
		"1.0", raw, now, now, now,
	)
	return err
}

// loadDisplayName 查询单个用户昵称；查询失败时以 ID 字符串降级。
func loadDisplayName(ctx context.Context, tx *sql.Tx, userID int64) string {
	var name string
	err := tx.QueryRowContext(ctx,
		`SELECT display_name FROM accounts WHERE user_id = ? LIMIT 1`,
		uint64(userID),
	).Scan(&name)
	if err != nil || strings.TrimSpace(name) == "" {
		return strconv.FormatInt(userID, 10)
	}
	return name
}
