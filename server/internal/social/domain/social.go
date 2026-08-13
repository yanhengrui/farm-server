// Package domain 定义好友系统的核心接口与值对象。
// 不依赖 HTTP/gRPC/MySQL/Redis；见依赖方向约束。
package domain

import "context"

// FriendInfo 是好友列表中的单个好友信息。
type FriendInfo struct {
	UserID      int64
	DisplayName string
}

// SocialService 是好友系统的领域服务接口，由 gamesvr 实现。
type SocialService interface {
	// CreateInvite 为 inviterUserID 生成一个新邀请码（有效期 7 天，单次使用）。
	CreateInvite(ctx context.Context, inviterUserID int64) (inviteCode string, err error)
	// AcceptInvite 消费邀请码，在 inviterUserID 和 accepterUserID 之间建立好友关系。
	// 幂等：已是好友时返回 nil（不报错）。
	AcceptInvite(ctx context.Context, inviteCode string, accepterUserID int64) error
	// AreFriends 检查两个用户是否已是好友关系。
	AreFriends(ctx context.Context, userIDA, userIDB int64) (bool, error)
	// ListFriends 返回 userID 的所有好友列表。
	ListFriends(ctx context.Context, userID int64) ([]FriendInfo, error)
}
