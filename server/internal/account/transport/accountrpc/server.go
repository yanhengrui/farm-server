// Package accountrpc — Server 侧，由 gamesvr 使用。
// 将 MySQLAccountService 暴露为 HTTP/JSON，供 gatesvr 通过 Client 调用。
package accountrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/photon/farm-server/server/internal/account/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

const accessTokenTTL = 30 * time.Minute

// AccountSvc 是 gamesvr 实现的账号服务，由 Server 持有。
type AccountSvc interface {
	GuestLogin(ctx context.Context, deviceID, displayName string) (infrastructure.GuestLoginResult, error)
	Register(ctx context.Context, username, password, displayName string) (infrastructure.GuestLoginResult, error)
	PasswordLogin(ctx context.Context, username, password string) (infrastructure.GuestLoginResult, error)
	RefreshSession(ctx context.Context, sessionID, refreshToken string) (infrastructure.RefreshSessionResult, error)
	Logout(ctx context.Context, sessionID, refreshToken string) error
	Authenticate(accessToken string) (infrastructure.AuthenticateResult, error)
}

// Server 将账号 RPC 暴露为 HTTP/JSON；注册在 gamesvr 的内部端口（GRPC_LISTEN_ADDR）。
type Server struct {
	svc AccountSvc
}

// NewServer 构造 Server。
func NewServer(svc AccountSvc) *Server { return &Server{svc: svc} }

// RegisterRoutes 注册账号 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/account/guest-login", s.handleGuestLogin)
	mux.HandleFunc("/rpc/account/register", s.handleRegister)
	mux.HandleFunc("/rpc/account/password-login", s.handlePasswordLogin)
	mux.HandleFunc("/rpc/account/refresh", s.handleRefresh)
	mux.HandleFunc("/rpc/account/logout", s.handleLogout)
	mux.HandleFunc("/rpc/account/authenticate", s.handleAuthenticate)
}

func loginResponse(result infrastructure.GuestLoginResult) GuestLoginResp {
	return GuestLoginResp{UserID: result.Account.UserID, FarmID: result.Account.FarmID,
		AccessToken: result.Session.AccessToken, RefreshToken: result.Session.RefreshToken,
		SessionID: result.Session.SessionID, ExpiresIn: int(accessTokenTTL.Seconds()), DisplayName: result.Account.DisplayName}
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, GuestLoginResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "invalid request"}})
		return
	}
	result, err := s.svc.Register(r.Context(), req.Username, req.Password, req.DisplayName)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GuestLoginResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, loginResponse(result))
}

func (s *Server) handlePasswordLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PasswordLoginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, GuestLoginResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "invalid request"}})
		return
	}
	result, err := s.svc.PasswordLogin(r.Context(), req.Username, req.Password)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GuestLoginResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, loginResponse(result))
}

func (s *Server) handleGuestLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GuestLoginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DeviceID == "" {
		writeResp(w, http.StatusBadRequest, GuestLoginResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "device_id required",
		}})
		return
	}

	result, err := s.svc.GuestLogin(r.Context(), req.DeviceID, req.DisplayName)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GuestLoginResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, GuestLoginResp{
		UserID:       result.Account.UserID,
		FarmID:       result.Account.FarmID,
		AccessToken:  result.Session.AccessToken,
		RefreshToken: result.Session.RefreshToken,
		SessionID:    result.Session.SessionID,
		ExpiresIn:    int(accessTokenTTL.Seconds()),
		DisplayName:  result.Account.DisplayName,
	})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RefreshReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, RefreshResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error(),
		}})
		return
	}

	result, err := s.svc.RefreshSession(r.Context(), req.SessionID, req.RefreshToken)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), RefreshResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, RefreshResp{
		AccessToken: result.AccessToken, RefreshToken: result.RefreshToken,
		ExpiresIn: int(accessTokenTTL.Seconds()),
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req LogoutReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, LogoutResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "invalid request"}})
		return
	}
	if err := s.svc.Logout(r.Context(), req.SessionID, req.RefreshToken); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), LogoutResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, LogoutResp{OK: true})
}

func (s *Server) handleAuthenticate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AuthenticateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, AuthenticateResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error(),
		}})
		return
	}

	result, err := s.svc.Authenticate(req.AccessToken)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), AuthenticateResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, AuthenticateResp{UserID: result.UserID, FarmID: result.FarmID})
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
