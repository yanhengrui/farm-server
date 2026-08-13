package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	socialdomain "github.com/photon/farm-server/server/internal/social/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedSocialService keeps same-shard behavior unchanged and uses the
// FriendEdgeSaga for cross-shard accept operations.
type ShardedSocialService struct {
	router   *shard.Router
	services map[string]*MySQLSocialService
	sagas    map[string]*FriendEdgeSaga
	invite   *redisstore.InviteStore
}

func NewShardedSocialService(router *shard.Router, services map[string]*MySQLSocialService, sagas map[string]*FriendEdgeSaga, invite *redisstore.InviteStore) (*ShardedSocialService, error) {
	if router == nil || invite == nil {
		return nil, fmt.Errorf("router and invite store are required")
	}
	for _, name := range router.Shards() {
		if services[name] == nil || sagas[name] == nil {
			return nil, fmt.Errorf("missing social shard backend %q", name)
		}
	}
	return &ShardedSocialService{router: router, services: services, sagas: sagas, invite: invite}, nil
}

func (s *ShardedSocialService) CreateInvite(ctx context.Context, inviterUserID int64) (string, error) {
	return s.invite.GetOrCreate(ctx, inviterUserID, NewSourceEventID())
}

func (s *ShardedSocialService) AcceptInvite(ctx context.Context, code string, accepterUserID int64) error {
	inviterID, err := s.invite.GetInviter(ctx, code)
	if errors.Is(err, redisstore.ErrInviteNotFound) {
		return errcode.New(errcode.SocialInviteExpired, "invite not found or expired")
	}
	if err != nil {
		return err
	}
	if inviterID == accepterUserID {
		return errcode.New(errcode.SocialSelfInvite, "cannot accept your own invite")
	}
	inviterShard, err := s.router.ShardForUserID(inviterID)
	if err != nil {
		return err
	}
	accepterShard, err := s.router.ShardForUserID(accepterUserID)
	if err != nil {
		return err
	}
	if inviterShard == accepterShard {
		return s.services[accepterShard].AcceptInvite(ctx, code, accepterUserID)
	}
	return s.sagas[accepterShard].BeginWithDisplayNames(ctx, accepterUserID, inviterID, NewSourceEventID(),
		s.displayNameOrID(ctx, accepterUserID),
		s.displayNameOrID(ctx, inviterID),
	)
}

func (s *ShardedSocialService) AreFriends(ctx context.Context, a, b int64) (bool, error) {
	left, err := s.router.ShardForUserID(a)
	if err != nil {
		return false, err
	}
	right, err := s.router.ShardForUserID(b)
	if err != nil {
		return false, err
	}
	if left == right {
		return s.services[left].AreFriends(ctx, a, b)
	}
	var state string
	err = s.sagas[left].db.QueryRowContext(ctx, "SELECT state FROM friendship_edges WHERE user_id=? AND friend_user_id=?", uint64(a), uint64(b)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return state == "ACTIVE", err
}

func (s *ShardedSocialService) ListFriends(ctx context.Context, userID int64) ([]socialdomain.FriendInfo, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return nil, err
	}
	// Same-shard friendships continue to use the legacy normalized table. The
	// directed edge table adds only cross-shard relations after their ACK.
	friends, err := s.services[name].ListFriends(ctx, userID)
	if err != nil {
		return nil, err
	}
	seen := make(map[int64]struct{}, len(friends))
	for _, friend := range friends {
		seen[friend.UserID] = struct{}{}
	}
	rows, err := s.sagas[name].db.QueryContext(ctx,
		"SELECT friend_user_id FROM friendship_edges WHERE user_id=? AND state='ACTIVE' ORDER BY updated_at, friend_user_id",
		uint64(userID),
	)
	if err != nil {
		return nil, fmt.Errorf("list cross-shard friend edges: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var friendID uint64
		if err := rows.Scan(&friendID); err != nil {
			return nil, fmt.Errorf("scan cross-shard friend edge: %w", err)
		}
		id := int64(friendID)
		if _, exists := seen[id]; exists {
			continue
		}
		info, err := s.lookupFriendInfo(ctx, id)
		if err != nil {
			return nil, err
		}
		seen[id] = struct{}{}
		friends = append(friends, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cross-shard friend edges: %w", err)
	}
	return friends, nil
}

func (s *ShardedSocialService) lookupFriendInfo(ctx context.Context, userID int64) (socialdomain.FriendInfo, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return socialdomain.FriendInfo{}, err
	}
	var displayName string
	err = s.services[name].db.QueryRowContext(ctx,
		"SELECT display_name FROM accounts WHERE user_id=? LIMIT 1", uint64(userID),
	).Scan(&displayName)
	if errors.Is(err, sql.ErrNoRows) {
		return friendInfo(userID), nil
	}
	if err != nil {
		return socialdomain.FriendInfo{}, fmt.Errorf("load cross-shard friend display name: %w", err)
	}
	if strings.TrimSpace(displayName) == "" {
		return friendInfo(userID), nil
	}
	return socialdomain.FriendInfo{UserID: userID, DisplayName: displayName}, nil
}

func (s *ShardedSocialService) displayNameOrID(ctx context.Context, userID int64) string {
	fallback := strconv.FormatInt(userID, 10)
	info, err := s.lookupFriendInfo(ctx, userID)
	if err != nil {
		return fallback
	}
	if name := strings.TrimSpace(info.DisplayName); name != "" {
		return name
	}
	return fallback
}

var _ socialdomain.SocialService = (*ShardedSocialService)(nil)

func friendInfo(id int64) socialdomain.FriendInfo {
	return socialdomain.FriendInfo{UserID: id, DisplayName: strconv.FormatInt(id, 10)}
}
