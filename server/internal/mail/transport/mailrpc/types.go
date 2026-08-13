// Package mailrpc 提供 gatesvr → gamesvr 的邮件服务 HTTP/JSON RPC 传输层。
package mailrpc

import "time"

// SendMailReq 是 /rpc/mail/send 请求体。
type SendMailReq struct {
	UserID      int64           `json:"user_id"`
	SenderID    *int64          `json:"sender_id,omitempty"`
	MailType    string          `json:"mail_type"`
	Title       string          `json:"title"`
	Content     string          `json:"content"`
	ExpiresAt   *time.Time      `json:"expires_at,omitempty"`
	Attachments []AttachmentDTO `json:"attachments,omitempty"`
}

// SendMailResp 是 /rpc/mail/send 响应体。
type SendMailResp struct {
	MailID int64   `json:"mail_id,omitempty"`
	Err    *RPCErr `json:"error,omitempty"`
}

// ListMailsReq 是 /rpc/mail/list 请求体。
type ListMailsReq struct {
	UserID int64 `json:"user_id"`
	Limit  int   `json:"limit,omitempty"`
}

// MailDTO 是邮件列表项。
type MailDTO struct {
	MailID      int64           `json:"mail_id"`
	SenderID    *int64          `json:"sender_id,omitempty"`
	MailType    string          `json:"mail_type"`
	Title       string          `json:"title"`
	Content     string          `json:"content"`
	Status      string          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	Attachments []AttachmentDTO `json:"attachments,omitempty"`
}

// AttachmentDTO 是附件 DTO。
type AttachmentDTO struct {
	AttachmentID int64      `json:"attachment_id,omitempty"`
	ItemType     string     `json:"item_type"`
	ItemID       int64      `json:"item_id"`
	Quantity     int64      `json:"quantity"`
	ClaimedAt    *time.Time `json:"claimed_at,omitempty"`
}

// ListMailsResp 是 /rpc/mail/list 响应体。
type ListMailsResp struct {
	Mails []MailDTO `json:"mails"`
	Err   *RPCErr   `json:"error,omitempty"`
}

type GetSummaryReq struct {
	UserID int64 `json:"user_id"`
}

type MailboxSummaryDTO struct {
	UnreadCount int64 `json:"unread_count"`
	Version     int64 `json:"mailbox_version"`
}

type GetSummaryResp struct {
	Summary MailboxSummaryDTO `json:"summary"`
	Err     *RPCErr           `json:"error,omitempty"`
}

// ClaimAttachmentReq 是 /rpc/mail/claim 请求体。
type ClaimAttachmentReq struct {
	AttachmentID int64 `json:"attachment_id"`
	UserID       int64 `json:"user_id"`
}

// ClaimAttachmentResp 是 /rpc/mail/claim 响应体。
type ClaimAttachmentResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// MarkReadReq 是 /rpc/mail/mark-read 请求体。
type MarkReadReq struct {
	MailID int64 `json:"mail_id"`
	UserID int64 `json:"user_id"`
}

// MarkReadResp 是 /rpc/mail/mark-read 响应体。
type MarkReadResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误；Code 对应 errcode.Code。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
