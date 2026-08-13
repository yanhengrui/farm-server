// Package mailrpc — Client 侧，由 gatesvr 和 workersvr 使用。
// 通过 HTTP/JSON 调用 gamesvr 的邮件服务端点。
package mailrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client 通过 HTTP/JSON 调用 gamesvr 邮件服务。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.MailServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.MailServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。baseURL 是 gamesvr 内部地址，例如 "http://gamesvr:9090"。
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// SendMail 调用 gamesvr 发送站内信。
func (c *Client) SendMail(ctx context.Context, req maildomain.SendMailReq) (int64, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		in := &rpcv1.SendMailRequest{UserId: req.UserID, SenderId: req.SenderID, MailType: string(req.MailType), Title: req.Title, Content: req.Content, ExpiresAt: timeProto(req.ExpiresAt)}
		for _, a := range req.Attachments {
			in.Attachments = append(in.Attachments, &rpcv1.MailAttachment{ItemType: a.ItemType, ItemId: a.ItemID, Quantity: a.Quantity})
		}
		out, err := c.grpc.SendMail(ctx, in)
		if err == nil {
			return out.MailId, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return 0, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(SendMailReq{
		UserID:    req.UserID,
		SenderID:  req.SenderID,
		MailType:  string(req.MailType),
		Title:     req.Title,
		Content:   req.Content,
		ExpiresAt: req.ExpiresAt,
		Attachments: func() []AttachmentDTO {
			atts := make([]AttachmentDTO, len(req.Attachments))
			for i, a := range req.Attachments {
				atts[i] = AttachmentDTO{ItemType: a.ItemType, ItemID: a.ItemID, Quantity: a.Quantity}
			}
			return atts
		}(),
	})
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/mail/send", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(r)
	if err != nil {
		return 0, fmt.Errorf("http_do send_mail: %w", err)
	}
	defer resp.Body.Close()

	var out SendMailResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode_send_mail_resp: %w", err)
	}
	if out.Err != nil {
		return 0, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.MailID, nil
}

// ListMails 调用 gamesvr 获取用户邮件列表。
func (c *Client) ListMails(ctx context.Context, userID int64, limit int) ([]MailDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.ListMails(ctx, &rpcv1.ListMailsRequest{UserId: userID, Limit: int32(limit)})
		if err == nil {
			return mailDTOsFromProto(out.Mails), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(ListMailsReq{UserID: userID, Limit: limit})
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/mail/list", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(r)
	if err != nil {
		return nil, fmt.Errorf("http_do list_mails: %w", err)
	}
	defer resp.Body.Close()

	var out ListMailsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_list_mails_resp: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.Mails, nil
}

func (c *Client) GetSummary(ctx context.Context, userID int64) (MailboxSummaryDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.GetSummary(ctx, &rpcv1.GetMailboxSummaryRequest{UserId: userID})
		if err == nil {
			return MailboxSummaryDTO{UnreadCount: out.UnreadCount, Version: out.MailboxVersion}, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return MailboxSummaryDTO{}, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(GetSummaryReq{UserID: userID})
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/mail/summary", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return MailboxSummaryDTO{}, fmt.Errorf("http_do mailbox_summary: %w", err)
	}
	defer resp.Body.Close()
	var out GetSummaryResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return MailboxSummaryDTO{}, fmt.Errorf("decode_mailbox_summary_resp: %w", err)
	}
	if out.Err != nil {
		return MailboxSummaryDTO{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.Summary, nil
}

// ClaimAttachment 调用 gamesvr 领取附件。
func (c *Client) ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.ClaimAttachment(ctx, &rpcv1.ClaimAttachmentRequest{AttachmentId: attachmentID, UserId: userID})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(ClaimAttachmentReq{AttachmentID: attachmentID, UserID: userID})
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/mail/claim", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(r)
	if err != nil {
		return fmt.Errorf("http_do claim_attachment: %w", err)
	}
	defer resp.Body.Close()

	var out ClaimAttachmentResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_claim_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// MarkRead 调用 gamesvr 将邮件标记为已读。
func (c *Client) MarkRead(ctx context.Context, mailID int64, userID int64) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.MarkRead(ctx, &rpcv1.MarkReadRequest{MailId: mailID, UserId: userID})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(MarkReadReq{MailID: mailID, UserID: userID})
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/mail/mark-read", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(r)
	if err != nil {
		return fmt.Errorf("http_do mark_read: %w", err)
	}
	defer resp.Body.Close()

	var out MarkReadResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_mark_read_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

func mailDTOsFromProto(items []*rpcv1.Mail) []MailDTO {
	out := make([]MailDTO, 0, len(items))
	for _, item := range items {
		dto := MailDTO{MailID: item.MailId, SenderID: item.SenderId, MailType: item.MailType, Title: item.Title, Content: item.Content, Status: item.Status, CreatedAt: protoTime(item.CreatedAt)}
		for _, a := range item.Attachments {
			dto.Attachments = append(dto.Attachments, AttachmentDTO{AttachmentID: a.AttachmentId, ItemType: a.ItemType, ItemID: a.ItemId, Quantity: a.Quantity, ClaimedAt: protoTimePtr(a.ClaimedAt)})
		}
		out = append(out, dto)
	}
	return out
}

func timeProto(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func protoTime(t *timestamppb.Timestamp) time.Time {
	if t == nil || !t.IsValid() {
		return time.Time{}
	}
	return t.AsTime()
}

func protoTimePtr(t *timestamppb.Timestamp) *time.Time {
	if t == nil || !t.IsValid() {
		return nil
	}
	v := t.AsTime()
	return &v
}
