// Package http — 农场相关 HTTP 处理器。
// GET /api/v1/farm/snapshot：从 token 解析 user_id，支持 ?farm_id= 查看好友农场（未传默认自己的）。
package http

import (
	"context"
	"net/http"
	"strconv"

	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// FarmSnapshotClient 是 gatesvr 调用 gamesvr 获取快照的最小接口。
type FarmSnapshotClient interface {
	GetSnapshot(ctx context.Context, farmID int64) (*farmrpc.FarmSnapshotDTO, error)
}

// FarmHandler 处理农场相关 HTTP 请求。
type FarmHandler struct {
	tokenSecret []byte
	farmClient  FarmSnapshotClient
}

// NewFarmHandler 构造 FarmHandler。
func NewFarmHandler(tokenSecret []byte, farmClient FarmSnapshotClient) *FarmHandler {
	return &FarmHandler{tokenSecret: tokenSecret, farmClient: farmClient}
}

// RegisterRoutes 注册 /api/v1/farm/* 端点。
func (h *FarmHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/farm/snapshot", h.handleGetSnapshot)
}

func (h *FarmHandler) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从 Authorization: Bearer <token> 或 ?token= 解析 user_id。
	token := bearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "token required")
		return
	}
	userID, err := session.Parse(token, h.tokenSecret)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	// ?farm_id= 可选，用于查看好友农场；未传则默认自己的农场。
	farmID := userID
	if fid := r.URL.Query().Get("farm_id"); fid != "" {
		if v, err := strconv.ParseInt(fid, 10, 64); err == nil && v > 0 {
			farmID = v
		}
	}

	snap, err := h.farmClient.GetSnapshot(r.Context(), farmID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// bearerToken 从 Authorization 头提取 Bearer token。
func bearerToken(r *http.Request) string {
	hdr := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(hdr) > len(prefix) && hdr[:len(prefix)] == prefix {
		return hdr[len(prefix):]
	}
	return ""
}
