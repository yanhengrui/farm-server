// Package http — 邮件系统 HTTP 处理器。
// GET  /api/v1/mail/list：获取当前用户邮件列表（需登录）。
// POST /api/v1/mail/claim：领取附件物品（需登录）。
// POST /api/v1/mail/read：标记邮件已读（需登录）。
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// MailClient 是 gatesvr 调用 gamesvr 邮件服务的最小接口。
type MailClient interface {
	ListMails(ctx context.Context, userID int64, limit int) ([]mailrpc.MailDTO, error)
	GetSummary(ctx context.Context, userID int64) (mailrpc.MailboxSummaryDTO, error)
	ClaimAttachment(ctx context.Context, attachmentID int64, userID int64) error
	MarkRead(ctx context.Context, mailID int64, userID int64) error
}

// MailHandler 处理邮件相关 HTTP 请求。
type MailHandler struct {
	tokenSecret []byte
	mail        MailClient
	readCache   *ReadCache
}

// NewMailHandler 构造 MailHandler。
func NewMailHandler(tokenSecret []byte, mail MailClient) *MailHandler {
	return &MailHandler{tokenSecret: tokenSecret, mail: mail}
}

func (h *MailHandler) WithReadCache(cache *ReadCache) *MailHandler {
	h.readCache = cache
	return h
}

// RegisterRoutes 注册 /api/v1/mail/* 端点。
func (h *MailHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/mail/list", h.handleList)
	mux.HandleFunc("/api/v1/mail/summary", h.handleSummary)
	mux.HandleFunc("/api/v1/mail/claim", h.handleClaim)
	mux.HandleFunc("/api/v1/mail/read", h.handleRead)
}

func (h *MailHandler) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	summary, err := h.mail.GetSummary(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

// mailListResp 邮件列表响应体。
type mailListResp struct {
	Mails []mailrpc.MailDTO `json:"mails"`
}

func (h *MailHandler) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	mails, err := h.mail.ListMails(r.Context(), userID, limit)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if mails == nil {
		mails = []mailrpc.MailDTO{}
	}
	writeJSON(w, http.StatusOK, mailListResp{Mails: mails})
}

// claimReq 领取附件请求体。
type claimReq struct {
	AttachmentID int64 `json:"attachment_id"`
}

func (h *MailHandler) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	var req claimReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AttachmentID == 0 {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "attachment_id required")
		return
	}

	if err := h.mail.ClaimAttachment(r.Context(), req.AttachmentID, userID); err != nil {
		writeErrFromError(w, err)
		return
	}
	if h.readCache != nil {
		h.readCache.Invalidate(r.Context(), 0, userID)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// readReq 标记已读请求体。
type readReq struct {
	MailID int64 `json:"mail_id"`
}

func (h *MailHandler) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	var req readReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MailID == 0 {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "mail_id required")
		return
	}

	if err := h.mail.MarkRead(r.Context(), req.MailID, userID); err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *MailHandler) authUser(w http.ResponseWriter, r *http.Request) (int64, bool) {
	token := bearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "token required")
		return 0, false
	}
	userID, err := session.Parse(token, h.tokenSecret)
	if err != nil {
		writeErrFromError(w, err)
		return 0, false
	}
	return userID, true
}
