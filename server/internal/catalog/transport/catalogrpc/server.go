package catalogrpc

import (
	"encoding/json"
	"errors"
	"net/http"

	catalogdomain "github.com/photon/farm-server/server/internal/catalog/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

type Server struct{ svc catalogdomain.Service }

func NewServer(svc catalogdomain.Service) *Server { return &Server{svc: svc} }

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/catalog/list", s.handleList)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ListReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID <= 0 {
		writeResp(w, http.StatusBadRequest, ListResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "user_id required"}})
		return
	}
	items, err := s.svc.ListCatalogUnlocks(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ListResp{Err: rpcErr})
		return
	}
	out := make([]UnlockDTO, 0, len(items))
	for _, item := range items {
		out = append(out, UnlockDTO{CatalogKey: item.CatalogKey, UnlockedAt: item.UnlockedAt})
	}
	writeResp(w, http.StatusOK, ListResp{Unlocks: out})
}

func toRPCErr(err error) *RPCErr {
	var e *errcode.Error
	if errors.As(err, &e) {
		return &RPCErr{Code: string(e.Code), Message: e.Message, Reason: e.Reason, RetryAfterMs: errcode.RetryAfter(err).Milliseconds()}
	}
	return &RPCErr{Code: string(errcode.Internal), Message: err.Error()}
}

func writeResp(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
