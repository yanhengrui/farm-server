package assetrpc

import (
	"encoding/json"
	"errors"
	"net/http"

	economydomain "github.com/photon/farm-server/server/internal/economy/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

type Server struct{ svc economydomain.AssetService }

func NewServer(svc economydomain.AssetService) *Server { return &Server{svc: svc} }

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/player/assets", s.handleGet)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID <= 0 {
		writeResp(w, http.StatusBadRequest, GetResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "user_id required"}})
		return
	}
	assets, err := s.svc.GetPlayerAssets(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GetResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, GetResp{Assets: assetsDTO(assets)})
}

func assetsDTO(assets economydomain.Assets) *AssetsDTO {
	out := &AssetsDTO{CoinBalance: assets.CoinBalance, Inventory: make([]InventoryItemDTO, 0, len(assets.Inventory))}
	for _, item := range assets.Inventory {
		out.Inventory = append(out.Inventory, InventoryItemDTO{ItemType: item.ItemType, ItemID: item.ItemID, Quantity: item.Quantity})
	}
	return out
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
