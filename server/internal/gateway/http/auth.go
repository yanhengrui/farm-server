// Package http 提供 gatesvr 的 HTTP 入口处理器。
// 所有端点挂载在 /api/v1/ 前缀下。
// 认证路由通过 accountrpc.Client 调用 gamesvr AccountService（完整 MySQL 路径）。
package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/photon/farm-server/server/internal/account/transport/accountrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// AccountClient 是 gatesvr 调用 gamesvr 账号服务的最小接口。
type AccountClient interface {
	GuestLogin(ctx context.Context, deviceID, displayName string) (accountrpc.GuestLoginResult, error)
	Register(ctx context.Context, username, password, displayName string) (accountrpc.GuestLoginResult, error)
	PasswordLogin(ctx context.Context, username, password string) (accountrpc.GuestLoginResult, error)
	RefreshSession(ctx context.Context, sessionID, refreshToken string) (accountrpc.RefreshResult, error)
	Logout(ctx context.Context, sessionID, refreshToken string) error
}

// authUser authenticates a public API request and returns only the token user.
func authUser(secret []byte, w http.ResponseWriter, r *http.Request) (int64, bool) {
	token := bearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "token required")
		return 0, false
	}
	userID, err := session.Parse(token, secret)
	if err != nil {
		writeErrFromError(w, err)
		return 0, false
	}
	return userID, true
}

// AuthHandler 处理认证类 HTTP 请求。
type AuthHandler struct {
	acct AccountClient
}

// NewAuthHandler 构造 AuthHandler。
func NewAuthHandler(acct AccountClient) *AuthHandler {
	return &AuthHandler{acct: acct}
}

// RegisterRoutes 注册 /api/v1/auth/* 端点。
func (h *AuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/auth/guest-login", h.handleGuestLogin)
	mux.HandleFunc("/api/v1/auth/register", h.handleRegister)
	mux.HandleFunc("/api/v1/auth/login", h.handlePasswordLogin)
	mux.HandleFunc("/api/v1/auth/refresh", h.handleRefresh)
	mux.HandleFunc("/api/v1/auth/logout", h.handleLogout)
}

// GuestLoginRequest 游客登录请求体。
type GuestLoginRequest struct {
	DeviceID    string `json:"device_id"`
	DisplayName string `json:"display_name"`
}

// GuestLoginResponse 游客登录响应体。
type GuestLoginResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	SessionID    string `json:"session_id"`
	UserID       int64  `json:"user_id,string"`
	FarmID       int64  `json:"farm_id,string"`
	ExpiresIn    int    `json:"expires_in"`
	DisplayName  string `json:"display_name"`
}

type RegisterRequest struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

type PasswordLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func authResponse(result accountrpc.GuestLoginResult) GuestLoginResponse {
	return GuestLoginResponse{AccessToken: result.AccessToken, RefreshToken: result.RefreshToken,
		SessionID: result.SessionID, UserID: result.UserID, FarmID: result.FarmID,
		ExpiresIn: 1800, DisplayName: result.DisplayName}
}

func (h *AuthHandler) handleGuestLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GuestLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.DeviceID) == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "device_id required")
		return
	}

	result, err := h.acct.GuestLogin(r.Context(), req.DeviceID, req.DisplayName)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, authResponse(result))
}

func (h *AuthHandler) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RegisterRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, err.Error())
		return
	}
	result, err := h.acct.Register(r.Context(), req.Username, req.Password, req.DisplayName)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, authResponse(result))
}

func (h *AuthHandler) handlePasswordLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PasswordLoginRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, err.Error())
		return
	}
	result, err := h.acct.PasswordLogin(r.Context(), req.Username, req.Password)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, authResponse(result))
}

func decodeJSONBody(r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid request body")
	}
	return nil
}

// RefreshRequest 刷新令牌请求体。
type RefreshRequest struct {
	SessionID    string `json:"session_id"`
	RefreshToken string `json:"refresh_token"`
}

// RefreshResponse 刷新令牌响应体。
type RefreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

func (h *AuthHandler) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "decode error")
		return
	}
	if req.SessionID == "" || req.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "session_id and refresh_token required")
		return
	}

	result, err := h.acct.RefreshSession(r.Context(), req.SessionID, req.RefreshToken)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, RefreshResponse{
		AccessToken: result.AccessToken, RefreshToken: result.RefreshToken,
		ExpiresIn: 1800,
	})
}

func (h *AuthHandler) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RefreshRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, err.Error())
		return
	}
	if err := h.acct.Logout(r.Context(), req.SessionID, req.RefreshToken); err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code errcode.Code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": string(code), "message": msg})
}

func writeErrFromError(w http.ResponseWriter, err error) {
	if retry := errcode.RetryAfter(err); retry > 0 {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("Retry-After-Ms", strconv.FormatInt(retry.Milliseconds(), 10))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		w.Header().Set("X-Capacity-Reason", errcode.CapacityReasonDownstreamTimeout)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": string(errcode.ResourceExhausted), "message": "downstream request timed out", "reason": errcode.CapacityReasonDownstreamTimeout})
		return
	}
	var e *errcode.Error
	if errors.As(err, &e) {
		if e.Reason != "" {
			w.Header().Set("X-Capacity-Reason", e.Reason)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(errcode.HTTPStatus(e.Code))
			_ = json.NewEncoder(w).Encode(map[string]string{"code": string(e.Code), "message": e.Message, "reason": e.Reason})
			return
		}
		writeError(w, errcode.HTTPStatus(e.Code), e.Code, e.Message)
		return
	}
	writeError(w, http.StatusInternalServerError, errcode.Internal, err.Error())
}
