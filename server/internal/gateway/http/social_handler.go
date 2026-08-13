// Package http — 好友系统 HTTP 处理器。
// POST /api/v1/social/invite：生成分享邀请码（需登录）。
// POST /api/v1/social/invite/accept：消费邀请码，自动加好友（需登录）。
// GET  /api/v1/social/friends：获取当前用户好友列表（需登录）。
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/photon/farm-server/server/internal/social/transport/socialrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// SocialClient 是 gatesvr 调用 gamesvr 好友服务的最小接口。
type SocialClient interface {
	CreateInvite(ctx context.Context, inviterUserID int64) (string, error)
	AcceptInvite(ctx context.Context, inviteCode string, accepterUserID int64) error
	ListFriends(ctx context.Context, userID int64) ([]socialrpc.FriendDTO, error)
}

// SocialHandler 处理好友系统相关 HTTP 请求。
type SocialHandler struct {
	tokenSecret []byte
	social      SocialClient
}

// NewSocialHandler 构造 SocialHandler。
func NewSocialHandler(tokenSecret []byte, social SocialClient) *SocialHandler {
	return &SocialHandler{tokenSecret: tokenSecret, social: social}
}

// RegisterRoutes 注册 /api/v1/social/* 端点。
func (h *SocialHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/social/invite", h.handleCreateInvite)
	mux.HandleFunc("/api/v1/social/invite/accept", h.handleAcceptInvite)
	mux.HandleFunc("/api/v1/social/friends", h.handleListFriends)
}

// createInviteResp 生成邀请码响应体。
type createInviteResp struct {
	InviteCode string `json:"invite_code"`
	InvitePath string `json:"invite_path"`
}

func (h *SocialHandler) handleCreateInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	code, err := h.social.CreateInvite(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, createInviteResp{
		InviteCode: code,
		InvitePath: "/invite?code=" + url.QueryEscape(code),
	})
}

// acceptInviteReq 接受邀请请求体。
type acceptInviteReq struct {
	InviteCode string `json:"invite_code"`
}

func (h *SocialHandler) handleAcceptInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	var req acceptInviteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.InviteCode == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "invite_code required")
		return
	}

	if err := h.social.AcceptInvite(r.Context(), req.InviteCode, userID); err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type friendView struct {
	UserID      int64  `json:"user_id,string"`
	DisplayName string `json:"display_name"`
}

// friendsResp 好友列表响应体。
type friendsResp struct {
	Friends []friendView `json:"friends"`
}

func (h *SocialHandler) handleListFriends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}

	friends, err := h.social.ListFriends(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	views := make([]friendView, len(friends))
	for i, friend := range friends {
		views[i] = friendView{
			UserID: friend.UserID, DisplayName: friend.DisplayName,
		}
	}
	writeJSON(w, http.StatusOK, friendsResp{Friends: views})
}

func (h *SocialHandler) authUser(w http.ResponseWriter, r *http.Request) (int64, bool) {
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
