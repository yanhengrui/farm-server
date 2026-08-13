// Package socialrpc — Server 侧，由 gamesvr 使用。
// 将 MySQLSocialService 暴露为 HTTP/JSON，供 gatesvr 通过 Client 调用。
package socialrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	socialdomain "github.com/photon/farm-server/server/internal/social/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// SocialSvc 是 gamesvr 实现的好友服务接口，由 Server 持有。
type SocialSvc interface {
	CreateInvite(ctx context.Context, inviterUserID int64) (string, error)
	AcceptInvite(ctx context.Context, inviteCode string, accepterUserID int64) error
	AreFriends(ctx context.Context, userIDA, userIDB int64) (bool, error)
	ListFriends(ctx context.Context, userID int64) ([]socialdomain.FriendInfo, error)
}

// Server 将好友 RPC 暴露为 HTTP/JSON；注册在 gamesvr 的内部端口（GRPC_LISTEN_ADDR）。
type Server struct {
	svc SocialSvc
}

// NewServer 构造 Server。
func NewServer(svc SocialSvc) *Server { return &Server{svc: svc} }

// RegisterRoutes 注册好友 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/social/create-invite", s.handleCreateInvite)
	mux.HandleFunc("/rpc/social/accept-invite", s.handleAcceptInvite)
	mux.HandleFunc("/rpc/social/are-friends", s.handleAreFriends)
	mux.HandleFunc("/rpc/social/list-friends", s.handleListFriends)
}

func (s *Server) handleCreateInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreateInviteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.InviterUserID == 0 {
		writeResp(w, http.StatusBadRequest, CreateInviteResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "inviter_user_id required",
		}})
		return
	}

	code, err := s.svc.CreateInvite(r.Context(), req.InviterUserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), CreateInviteResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, CreateInviteResp{InviteCode: code})
}

func (s *Server) handleAcceptInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AcceptInviteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.InviteCode == "" || req.AccepterUserID == 0 {
		writeResp(w, http.StatusBadRequest, AcceptInviteResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "invite_code and accepter_user_id required",
		}})
		return
	}

	if err := s.svc.AcceptInvite(r.Context(), req.InviteCode, req.AccepterUserID); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), AcceptInviteResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, AcceptInviteResp{})
}

func (s *Server) handleAreFriends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AreFriendsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserIDA == 0 || req.UserIDB == 0 {
		writeResp(w, http.StatusBadRequest, AreFriendsResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id_a and user_id_b required",
		}})
		return
	}

	friends, err := s.svc.AreFriends(r.Context(), req.UserIDA, req.UserIDB)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), AreFriendsResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, AreFriendsResp{Friends: friends})
}

func (s *Server) handleListFriends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ListFriendsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, ListFriendsResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id required",
		}})
		return
	}

	list, err := s.svc.ListFriends(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ListFriendsResp{Err: rpcErr})
		return
	}
	dtos := make([]FriendDTO, len(list))
	for i, f := range list {
		dtos[i] = newFriendDTO(f.UserID, f.DisplayName)
	}
	writeResp(w, http.StatusOK, ListFriendsResp{Friends: dtos})
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
