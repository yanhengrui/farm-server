package mailrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// stubMailSvc 是 MailSvc 的内存 stub，不依赖 DB。
type stubMailSvc struct {
	mails       map[int64]maildomain.Mail
	attachments map[int64]maildomain.Attachment
	nextMailID  int64
	nextAttID   int64
}

func newStubMailSvc() *stubMailSvc {
	return &stubMailSvc{
		mails:       make(map[int64]maildomain.Mail),
		attachments: make(map[int64]maildomain.Attachment),
		nextMailID:  1,
		nextAttID:   1,
	}
}

func (s *stubMailSvc) SendMail(_ context.Context, req maildomain.SendMailReq) (int64, error) {
	if req.UserID == 0 || req.Title == "" {
		return 0, errcode.New(errcode.CommonInvalidArgument, "invalid req")
	}
	id := s.nextMailID
	s.nextMailID++
	m := maildomain.Mail{
		MailID:   id,
		UserID:   req.UserID,
		SenderID: req.SenderID,
		MailType: req.MailType,
		Title:    req.Title,
		Content:  req.Content,
		Status:   maildomain.MailStatusUnread,
	}
	for _, a := range req.Attachments {
		attID := s.nextAttID
		s.nextAttID++
		att := maildomain.Attachment{
			AttachmentID: attID,
			MailID:       id,
			ItemType:     a.ItemType,
			ItemID:       a.ItemID,
			Quantity:     a.Quantity,
		}
		s.attachments[attID] = att
		m.Attachments = append(m.Attachments, att)
	}
	s.mails[id] = m
	return id, nil
}

func (s *stubMailSvc) ListMails(_ context.Context, userID int64, _ int) ([]maildomain.Mail, error) {
	var result []maildomain.Mail
	for _, m := range s.mails {
		if m.UserID == userID && m.Status != maildomain.MailStatusDeleted {
			result = append(result, m)
		}
	}
	return result, nil
}

func (s *stubMailSvc) GetSummary(_ context.Context, userID int64) (maildomain.MailboxSummary, error) {
	var unread int64
	for _, mail := range s.mails {
		if mail.UserID == userID && mail.Status == maildomain.MailStatusUnread {
			unread++
		}
	}
	return maildomain.MailboxSummary{UserID: userID, UnreadCount: unread, Version: int64(len(s.mails))}, nil
}

func (s *stubMailSvc) ClaimAttachment(_ context.Context, attachmentID int64, userID int64) error {
	att, ok := s.attachments[attachmentID]
	if !ok {
		return errcode.New(errcode.MailNotFound, "not found")
	}
	m, ok := s.mails[att.MailID]
	if !ok || m.UserID != userID {
		return errcode.New(errcode.MailNotFound, "not yours")
	}
	if att.ClaimedAt != nil {
		return errcode.New(errcode.MailAlreadyClaimed, "already claimed")
	}
	// 标记已领取（stub 内直接修改）。
	now := s.mails[att.MailID].CreatedAt
	att.ClaimedAt = &now
	s.attachments[attachmentID] = att
	return nil
}

func (s *stubMailSvc) MarkRead(_ context.Context, mailID int64, userID int64) error {
	m, ok := s.mails[mailID]
	if !ok || m.UserID != userID {
		return nil // 静默
	}
	m.Status = maildomain.MailStatusRead
	s.mails[mailID] = m
	return nil
}

func newTestPair(t *testing.T) (*Client, *stubMailSvc) {
	t.Helper()
	svc := newStubMailSvc()
	srv := NewServer(svc)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL), svc
}

// TestMailRPC_SendAndList 验证发送邮件后能查询到。
func TestMailRPC_SendAndList(t *testing.T) {
	c, _ := newTestPair(t)

	senderID := int64(99)
	mailID, err := c.SendMail(t.Context(), maildomain.SendMailReq{
		UserID:   100,
		SenderID: &senderID,
		MailType: maildomain.MailTypeFriendAccepted,
		Title:    "好友建立",
		Content:  "你们已成为好友",
	})
	if err != nil {
		t.Fatalf("send mail: %v", err)
	}
	if mailID == 0 {
		t.Error("expected non-zero mailID")
	}

	mails, err := c.ListMails(t.Context(), 100, 10)
	if err != nil {
		t.Fatalf("list mails: %v", err)
	}
	if len(mails) != 1 {
		t.Fatalf("expected 1 mail, got %d", len(mails))
	}
	if mails[0].Title != "好友建立" {
		t.Errorf("unexpected title: %q", mails[0].Title)
	}
}

// TestMailRPC_Claim_AlreadyClaimed 验证重复领取返回 MAIL_ALREADY_CLAIMED。
func TestMailRPC_Claim_AlreadyClaimed(t *testing.T) {
	c, _ := newTestPair(t)

	// 发一封带附件的邮件。
	_, err := c.SendMail(t.Context(), maildomain.SendMailReq{
		UserID:   200,
		MailType: maildomain.MailTypeStealNotify,
		Title:    "被偷了",
		Attachments: []maildomain.Attachment{
			{ItemType: "CROP", ItemID: 1, Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("send mail: %v", err)
	}

	// 领取附件（attachment_id=1）。
	if err := c.ClaimAttachment(t.Context(), 1, 200); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// 重复领取应返回 MAIL_ALREADY_CLAIMED。
	err = c.ClaimAttachment(t.Context(), 1, 200)
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.MailAlreadyClaimed {
		t.Errorf("expected MAIL_ALREADY_CLAIMED, got %v", err)
	}
}

// TestMailRPC_Claim_NotFound 验证领取不存在附件返回 MAIL_NOT_FOUND。
func TestMailRPC_Claim_NotFound(t *testing.T) {
	c, _ := newTestPair(t)
	err := c.ClaimAttachment(t.Context(), 999, 100)
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.MailNotFound {
		t.Errorf("expected MAIL_NOT_FOUND, got %v", err)
	}
}

// TestMailRPC_ListMails_Empty 验证无邮件时返回空列表。
func TestMailRPC_ListMails_Empty(t *testing.T) {
	c, _ := newTestPair(t)
	mails, err := c.ListMails(t.Context(), 999, 10)
	if err != nil {
		t.Fatalf("list mails: %v", err)
	}
	if len(mails) != 0 {
		t.Errorf("expected empty, got %d", len(mails))
	}
}

func TestMailRPC_GetSummary(t *testing.T) {
	c, _ := newTestPair(t)
	if _, err := c.SendMail(t.Context(), maildomain.SendMailReq{UserID: 300, MailType: maildomain.MailTypeSystem, Title: "notice"}); err != nil {
		t.Fatal(err)
	}
	summary, err := c.GetSummary(t.Context(), 300)
	if err != nil {
		t.Fatal(err)
	}
	if summary.UnreadCount != 1 || summary.Version != 1 {
		t.Fatalf("summary=%+v", summary)
	}
}
