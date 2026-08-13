package http

import (
	"context"
	"net/http"

	"github.com/photon/farm-server/server/internal/catalog/transport/catalogrpc"
)

type CatalogClient interface {
	ListCatalogUnlocks(ctx context.Context, userID int64) ([]catalogrpc.UnlockDTO, error)
}

type CatalogHandler struct {
	tokenSecret []byte
	catalog     CatalogClient
}

func NewCatalogHandler(tokenSecret []byte, catalog CatalogClient) *CatalogHandler {
	return &CatalogHandler{tokenSecret: tokenSecret, catalog: catalog}
}

func (h *CatalogHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/catalog/list", h.handleList)
}

func (h *CatalogHandler) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := authUser(h.tokenSecret, w, r)
	if !ok {
		return
	}
	items, err := h.catalog.ListCatalogUnlocks(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if items == nil {
		items = []catalogrpc.UnlockDTO{}
	}
	writeJSON(w, http.StatusOK, map[string][]catalogrpc.UnlockDTO{"unlocks": items})
}
