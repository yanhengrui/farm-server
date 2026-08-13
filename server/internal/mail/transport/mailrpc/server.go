// Package mailrpc — Server 侧，由 gamesvr 使用。
// 将 MySQLMailService 暴露为 HTTP/JSON，供 gatesvr 和 workersvr 调用。
package mailrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// MailSvc 是 Server 持有的邮件服务接口。
type MailSvc interface {
	SendMail(ctx context.Context, req maildomain.SendMailReq) (int64, error)
	ListMails(ctx context.Context, userID int64, limit int) ([]maildomain.Mail, error)
	GetSummary(ctx context.Context, userID int64) (maildomain.MailboxSummary, error)
	ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error
	MarkRead(ctx context.Context, mailID int64, userID int64) error
}

// Server 将邮件 RPC 暴露为 HTTP/JSON；注册在 gamesvr 的内部端口（GRPC_LISTEN_ADDR）。
type Server struct {
	svc MailSvc
}

// NewServer 构造 Server。
func NewServer(svc MailSvc) *Server { return &Server{svc: svc} }

// RegisterRoutes 注册邮件 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/mail/send", s.handleSend)
	mux.HandleFunc("/rpc/mail/list", s.handleList)
	mux.HandleFunc("/rpc/mail/summary", s.handleSummary)
	mux.HandleFunc("/rpc/mail/claim", s.handleClaim)
	mux.HandleFunc("/rpc/mail/mark-read", s.handleMarkRead)
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GetSummaryReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, GetSummaryResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "user_id required"}})
		return
	}
	summary, err := s.svc.GetSummary(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GetSummaryResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, GetSummaryResp{Summary: MailboxSummaryDTO{UnreadCount: summary.UnreadCount, Version: summary.Version}})
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SendMailReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 || req.Title == "" {
		writeResp(w, http.StatusBadRequest, SendMailResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id and title required",
		}})
		return
	}

	domainReq := maildomain.SendMailReq{
		UserID:    req.UserID,
		SenderID:  req.SenderID,
		MailType:  maildomain.MailType(req.MailType),
		Title:     req.Title,
		Content:   req.Content,
		ExpiresAt: req.ExpiresAt,
	}
	for _, a := range req.Attachments {
		domainReq.Attachments = append(domainReq.Attachments, maildomain.Attachment{
			ItemType: a.ItemType,
			ItemID:   a.ItemID,
			Quantity: a.Quantity,
		})
	}

	mailID, err := s.svc.SendMail(r.Context(), domainReq)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), SendMailResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, SendMailResp{MailID: mailID})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ListMailsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, ListMailsResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id required",
		}})
		return
	}

	mails, err := s.svc.ListMails(r.Context(), req.UserID, req.Limit)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ListMailsResp{Err: rpcErr})
		return
	}
	dtos := make([]MailDTO, len(mails))
	for i, m := range mails {
		dtos[i] = mailToDTO(m)
	}
	writeResp(w, http.StatusOK, ListMailsResp{Mails: dtos})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ClaimAttachmentReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AttachmentID == 0 || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, ClaimAttachmentResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "attachment_id and user_id required",
		}})
		return
	}

	if err := s.svc.ClaimAttachment(r.Context(), req.AttachmentID, req.UserID); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ClaimAttachmentResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, ClaimAttachmentResp{})
}

func (s *Server) handleMarkRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req MarkReadReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MailID == 0 || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, MarkReadResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "mail_id and user_id required",
		}})
		return
	}

	if err := s.svc.MarkRead(r.Context(), req.MailID, req.UserID); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), MarkReadResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, MarkReadResp{})
}

// mailToDTO 将领域实体转换为传输 DTO。
func mailToDTO(m maildomain.Mail) MailDTO {
	dto := MailDTO{
		MailID:    m.MailID,
		SenderID:  m.SenderID,
		MailType:  string(m.MailType),
		Title:     m.Title,
		Content:   m.Content,
		Status:    string(m.Status),
		CreatedAt: m.CreatedAt,
	}
	for _, a := range m.Attachments {
		dto.Attachments = append(dto.Attachments, AttachmentDTO{
			AttachmentID: a.AttachmentID,
			ItemType:     a.ItemType,
			ItemID:       a.ItemID,
			Quantity:     a.Quantity,
			ClaimedAt:    a.ClaimedAt,
		})
	}
	return dto
}

func toRPCErr(err error) *RPCErr {
	var e *errcode.Error
	if errors.As(err, &e) {
		return &RPCErr{Code: string(e.Code), Message: e.Message, Reason: e.Reason, RetryAfterMs: errcode.RetryAfter(err).Milliseconds()}
	}
	return &RPCErr{Code: string(errcode.Internal), Message: err.Error()}
}

func writeResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
