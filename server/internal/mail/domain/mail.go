// Package domain 定义邮件系统的核心实体与服务接口。
// 不依赖 HTTP/gRPC/MySQL/Redis；见依赖方向约束。
package domain

import (
	"context"
	"time"
)

// MailType 邮件类型。
type MailType string

const (
	MailTypeFriendAccepted MailType = "FRIEND_ACCEPTED" // 好友建立通知
	MailTypeStealNotify    MailType = "STEAL_NOTIFY"    // 被偷菜通知
	MailTypeSystem         MailType = "SYSTEM"          // 系统邮件
)

// MailStatus 邮件阅读状态。
type MailStatus string

const (
	MailStatusUnread  MailStatus = "UNREAD"
	MailStatusRead    MailStatus = "READ"
	MailStatusDeleted MailStatus = "DELETED"
)

// Attachment 邮件附件（作物/种子/金币）。
type Attachment struct {
	AttachmentID int64
	MailID       int64
	ItemType     string // "CROP" | "SEED" | "COIN"
	ItemID       int64  // COIN 时为 0
	Quantity     int64
	ClaimedAt    *time.Time
}

// Mail 站内信实体。
type Mail struct {
	MailID      int64
	UserID      int64
	SenderID    *int64 // nil 表示系统邮件
	MailType    MailType
	Title       string
	Content     string
	Status      MailStatus
	ReadAt      *time.Time
	ExpiresAt   *time.Time
	CreatedAt   time.Time
	Attachments []Attachment
}

// SendMailReq 发送邮件请求。
type SendMailReq struct {
	UserID      int64
	SenderID    *int64 // nil = 系统邮件
	MailType    MailType
	Title       string
	Content     string
	ExpiresAt   *time.Time
	Attachments []Attachment // 无附件时为空
}

// MailboxSummary is the authoritative lightweight state used by the client
// mail badge. Mail contents remain available only through ListMails.
type MailboxSummary struct {
	UserID      int64
	UnreadCount int64
	Version     int64
}

// MailboxNotifier publishes a best-effort invalidation after MySQL commits.
// Consumers must reconcile with GetSummary after reconnect or message loss.
type MailboxNotifier interface {
	NotifyMailboxChanged(ctx context.Context, summary MailboxSummary) error
}

// MailService 是邮件系统的领域服务接口，由 gamesvr 实现。
type MailService interface {
	// SendMail 发送一封站内信（含附件）给目标用户。
	SendMail(ctx context.Context, req SendMailReq) (mailID int64, err error)
	// ListMails 返回用户未删除邮件（按 created_at 降序），limit 最多 50 封。
	ListMails(ctx context.Context, userID int64, limit int) ([]Mail, error)
	// GetSummary returns only unread count and mailbox version for badges.
	GetSummary(ctx context.Context, userID int64) (MailboxSummary, error)
	// ClaimAttachment 领取附件：将物品写入用户库存/钱包，标记 claimed_at。
	// 幂等：已领取时返回 MailAlreadyClaimed 错误。
	ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error
	// MarkRead 将邮件标记为已读（静默，不报错如果已读）。
	MarkRead(ctx context.Context, mailID int64, userID int64) error
}
