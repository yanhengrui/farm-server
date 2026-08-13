// Package petrpc — Server 侧，由 gamesvr 使用。
package petrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	petdomain "github.com/photon/farm-server/server/internal/pet/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// PetSvc 是 Server 持有的宠物服务接口。
type PetSvc interface {
	BuyPet(ctx context.Context, userID int64) error
	HasPet(ctx context.Context, userID int64) (bool, error)
	GetStatus(ctx context.Context, userID int64) (petdomain.PlayerStatus, error)
	SetAutoHarvest(ctx context.Context, userID int64, enabled bool) error
}

// Server 将宠物 RPC 暴露为 HTTP/JSON。
type Server struct {
	svc PetSvc
}

// NewServer 构造 Server。
func NewServer(svc PetSvc) *Server { return &Server{svc: svc} }

// RegisterRoutes 注册宠物 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/pet/buy", s.handleBuy)
	mux.HandleFunc("/rpc/pet/has", s.handleHas)
	mux.HandleFunc("/rpc/pet/set-auto-harvest", s.handleSetAutoHarvest)
}

func (s *Server) handleBuy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BuyPetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, BuyPetResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id required",
		}})
		return
	}
	if err := s.svc.BuyPet(r.Context(), req.UserID); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), BuyPetResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, BuyPetResp{})
}

func (s *Server) handleHas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req HasPetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, HasPetResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id required",
		}})
		return
	}
	status, err := s.svc.GetStatus(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), HasPetResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, HasPetResp{HasPet: status.HasPet, AutoHarvestEnabled: status.AutoHarvestEnabled})
}

func (s *Server) handleSetAutoHarvest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SetAutoHarvestReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID <= 0 {
		writeResp(w, http.StatusBadRequest, SetAutoHarvestResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "user_id required"}})
		return
	}
	if err := s.svc.SetAutoHarvest(r.Context(), req.UserID, req.Enabled); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), SetAutoHarvestResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, SetAutoHarvestResp{})
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
