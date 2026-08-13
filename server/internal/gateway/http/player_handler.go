package http

import (
	"context"
	"net/http"

	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
)

type PlayerAssetClient interface {
	GetPlayerAssets(ctx context.Context, userID int64) (*assetrpc.AssetsDTO, error)
}

type PlayerHandler struct {
	tokenSecret []byte
	assets      PlayerAssetClient
}

func NewPlayerHandler(tokenSecret []byte, assets PlayerAssetClient) *PlayerHandler {
	return &PlayerHandler{tokenSecret: tokenSecret, assets: assets}
}

func (h *PlayerHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/player/assets", h.handleAssets)
}

func (h *PlayerHandler) handleAssets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := authUser(h.tokenSecret, w, r)
	if !ok {
		return
	}
	assets, err := h.assets.GetPlayerAssets(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, assets)
}
