package mailrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type grpcServer struct {
	rpcv1.UnimplementedMailServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterMailServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) SendMail(ctx context.Context, req *rpcv1.SendMailRequest) (*rpcv1.SendMailResponse, error) {
	if req.UserId == 0 || req.Title == "" {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id and title required"))
	}
	domainReq := maildomain.SendMailReq{UserID: req.UserId, SenderID: req.SenderId, MailType: maildomain.MailType(req.MailType), Title: req.Title, Content: req.Content, ExpiresAt: protoTimePtr(req.ExpiresAt)}
	for _, a := range req.Attachments {
		domainReq.Attachments = append(domainReq.Attachments, maildomain.Attachment{ItemType: a.ItemType, ItemID: a.ItemId, Quantity: a.Quantity})
	}
	id, err := g.server.svc.SendMail(ctx, domainReq)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.SendMailResponse{MailId: id}, nil
}

func (g *grpcServer) ListMails(ctx context.Context, req *rpcv1.ListMailsRequest) (*rpcv1.ListMailsResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	items, err := g.server.svc.ListMails(ctx, req.UserId, int(req.Limit))
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	out := &rpcv1.ListMailsResponse{}
	for _, m := range items {
		item := &rpcv1.Mail{MailId: m.MailID, SenderId: m.SenderID, MailType: string(m.MailType), Title: m.Title, Content: m.Content, Status: string(m.Status), CreatedAt: timestamppb.New(m.CreatedAt)}
		for _, a := range m.Attachments {
			item.Attachments = append(item.Attachments, &rpcv1.MailAttachment{AttachmentId: a.AttachmentID, ItemType: a.ItemType, ItemId: a.ItemID, Quantity: a.Quantity, ClaimedAt: timeProto(a.ClaimedAt)})
		}
		out.Mails = append(out.Mails, item)
	}
	return out, nil
}

func (g *grpcServer) GetSummary(ctx context.Context, req *rpcv1.GetMailboxSummaryRequest) (*rpcv1.GetMailboxSummaryResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	summary, err := g.server.svc.GetSummary(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.GetMailboxSummaryResponse{UnreadCount: summary.UnreadCount, MailboxVersion: summary.Version}, nil
}

func (g *grpcServer) ClaimAttachment(ctx context.Context, req *rpcv1.ClaimAttachmentRequest) (*rpcv1.ClaimAttachmentResponse, error) {
	if req.AttachmentId == 0 || req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "attachment_id and user_id required"))
	}
	if err := g.server.svc.ClaimAttachment(ctx, req.AttachmentId, req.UserId); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.ClaimAttachmentResponse{}, nil
}

func (g *grpcServer) MarkRead(ctx context.Context, req *rpcv1.MarkReadRequest) (*rpcv1.MarkReadResponse, error) {
	if req.MailId == 0 || req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "mail_id and user_id required"))
	}
	if err := g.server.svc.MarkRead(ctx, req.MailId, req.UserId); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.MarkReadResponse{}, nil
}
