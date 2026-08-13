// Package http — 宠物系统 HTTP 处理器。
// POST /api/v1/pet/buy：购买宠物（需登录）。
// GET  /api/v1/pet/status：查询是否有宠物（需登录）。
package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/photon/farm-server/server/internal/pet/transport/petrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// PetClient 是 gatesvr 调用 gamesvr 宠物服务的最小接口。
type PetClient interface {
	BuyPet(ctx context.Context, userID int64) error
	GetStatus(ctx context.Context, userID int64) (petrpc.StatusDTO, error)
	SetAutoHarvest(ctx context.Context, userID int64, enabled bool) error
}

// PetHandler 处理宠物相关 HTTP 请求。
type PetHandler struct {
	tokenSecret []byte
	pet         PetClient
	readCache   *ReadCache
}

// NewPetHandler 构造 PetHandler。
func NewPetHandler(tokenSecret []byte, pet PetClient) *PetHandler {
	return &PetHandler{tokenSecret: tokenSecret, pet: pet}
}

func (h *PetHandler) WithReadCache(cache *ReadCache) *PetHandler {
	h.readCache = cache
	return h
}

// RegisterRoutes 注册 /api/v1/pet/* 端点。
func (h *PetHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/pet/buy", h.handleBuy)
	mux.HandleFunc("/api/v1/pet/status", h.handleStatus)
	mux.HandleFunc("/api/v1/pet/auto-harvest", h.handleAutoHarvest)
}

func (h *PetHandler) handleBuy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	if err := h.pet.BuyPet(r.Context(), userID); err != nil {
		writeErrFromError(w, err)
		return
	}
	if h.readCache != nil {
		h.readCache.Invalidate(r.Context(), 0, userID)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type petStatusResp struct {
	HasPet             bool `json:"has_pet"`
	AutoHarvestEnabled bool `json:"auto_harvest_enabled"`
}

func (h *PetHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	status, err := h.pet.GetStatus(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, petStatusResp{HasPet: status.HasPet, AutoHarvestEnabled: status.AutoHarvestEnabled})
}

type setAutoHarvestReq struct {
	Enabled *bool `json:"enabled"`
}

func (h *PetHandler) handleAutoHarvest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	var req setAutoHarvestReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Enabled == nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "enabled required")
		return
	}
	if err := h.pet.SetAutoHarvest(r.Context(), userID, *req.Enabled); err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"auto_harvest_enabled": *req.Enabled})
}

func (h *PetHandler) authUser(w http.ResponseWriter, r *http.Request) (int64, bool) {
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
