package infrastructure

import (
	"context"
	"fmt"

	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedMailService routes both a recipient's mail rows and attachment asset
// claims to that recipient's single aggregate shard.
type ShardedMailService struct {
	router   *shard.Router
	services map[string]*MySQLMailService
}

func NewShardedMailService(router *shard.Router, services map[string]*MySQLMailService) (*ShardedMailService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLMailService, len(services))
	for _, name := range router.Shards() {
		if services[name] == nil {
			return nil, fmt.Errorf("missing mail service for shard %q", name)
		}
		copyServices[name] = services[name]
	}
	return &ShardedMailService{router: router, services: copyServices}, nil
}

func (s *ShardedMailService) service(userID int64) (*MySQLMailService, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return nil, err
	}
	return s.services[name], nil
}
func (s *ShardedMailService) SendMail(ctx context.Context, req maildomain.SendMailReq) (int64, error) {
	service, err := s.service(req.UserID)
	if err != nil {
		return 0, err
	}
	return service.SendMail(ctx, req)
}
func (s *ShardedMailService) ListMails(ctx context.Context, userID int64, limit int) ([]maildomain.Mail, error) {
	service, err := s.service(userID)
	if err != nil {
		return nil, err
	}
	return service.ListMails(ctx, userID, limit)
}
func (s *ShardedMailService) GetSummary(ctx context.Context, userID int64) (maildomain.MailboxSummary, error) {
	service, err := s.service(userID)
	if err != nil {
		return maildomain.MailboxSummary{}, err
	}
	return service.GetSummary(ctx, userID)
}
func (s *ShardedMailService) ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error {
	service, err := s.service(userID)
	if err != nil {
		return err
	}
	return service.ClaimAttachment(ctx, attachmentID, userID)
}
func (s *ShardedMailService) MarkRead(ctx context.Context, mailID int64, userID int64) error {
	service, err := s.service(userID)
	if err != nil {
		return err
	}
	return service.MarkRead(ctx, mailID, userID)
}

var _ maildomain.MailService = (*ShardedMailService)(nil)
