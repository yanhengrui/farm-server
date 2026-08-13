// Package http — 商店与出售 HTTP 处理器。
// POST /api/v1/shop/purchase：购买种子（直接调用 gamesvr，绕过 farmsvr Actor）。
// POST /api/v1/farm/sell：出售作物。
// 两个端点不修改农场地块，无需 farmsvr 串行化；直通 gamesvr CommitFarmCommand。
// 可选的 EconomyCoalescer 把短窗口内同一用户的同类命令合并为单条 qty=N 命令，
// 减少事务数与 fsync 次数。
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/session"
)

// EconomicCommitClient 是 gatesvr 直接调用 gamesvr 经济命令的最小接口。
// *farmrpc.Client 满足此接口（已实现 application.Committer）。
type EconomicCommitClient interface {
	CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error)
}

// ShopHandler 处理购买与出售类 HTTP 请求。
// coalescer 可选；非 nil 时启用短窗口 qty 合并，减少事务数与 fsync 次数。
type ShopHandler struct {
	tokenSecret []byte
	ecoClient   EconomicCommitClient
	coalescer   *EconomyCoalescer
	readCache   *ReadCache
}

// NewShopHandler 构造 ShopHandler（不启用聚合）。
func NewShopHandler(tokenSecret []byte, ecoClient EconomicCommitClient) *ShopHandler {
	return &ShopHandler{tokenSecret: tokenSecret, ecoClient: ecoClient}
}

// WithReadCache invalidates derived read views only after the authoritative
// economy transaction commits successfully.
func (h *ShopHandler) WithReadCache(cache *ReadCache) *ShopHandler {
	h.readCache = cache
	return h
}

// NewShopHandlerWithCoalescer 构造启用聚合窗口的 ShopHandler。
// window 推荐 50ms；传 0 退化为无聚合。
func NewShopHandlerWithCoalescer(tokenSecret []byte, ecoClient EconomicCommitClient, window time.Duration) *ShopHandler {
	h := &ShopHandler{tokenSecret: tokenSecret, ecoClient: ecoClient}
	if window > 0 {
		h.coalescer = NewEconomyCoalescer(ecoClient, window)
	}
	return h
}

// RegisterRoutes 注册 /api/v1/shop/* 和 /api/v1/farm/sell 端点。
func (h *ShopHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/shop/purchase", h.handlePurchase)
	mux.HandleFunc("/api/v1/farm/sell", h.handleSell)
}

// purchaseReq 购买种子请求体。
type purchaseReq struct {
	CropID   string `json:"crop_id"`  // 种子对应的作物 ID（如 "1" 或 "WHEAT"）
	Quantity int64  `json:"quantity"` // 购买数量（≥1）
}

// sellReq 出售作物请求体。
type sellReq struct {
	CropID   string `json:"crop_id"`  // 待出售作物 ID
	Quantity int64  `json:"quantity"` // 出售数量（≥1）
}

// economicResp 购买/出售统一响应体。
// coin_balance 为执行后的权威金币余额，客户端应以此值覆盖本地缓存。
type economicResp struct {
	EventID     string `json:"event_id"`
	CoinBalance int64  `json:"coin_balance"`
}

func (h *ShopHandler) handlePurchase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	cmdID, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}

	var req purchaseReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "请求体解析失败")
		return
	}
	if req.CropID == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "crop_id 不能为空")
		return
	}
	if req.Quantity <= 0 {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "quantity 必须 ≥ 1")
		return
	}
	// 上限校验与领域常量一致；权威事务内还会再校验一次，此处只为让客户端拿到 400
	// 而不是把一个必然失败的命令送到数据库。
	if req.Quantity > domain.MaxEconomyQuantity {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument,
			fmt.Sprintf("quantity 不能超过 %d", domain.MaxEconomyQuantity))
		return
	}

	res, err := h.commitEconomy(r.Context(), userID, domain.CmdPurchaseSeed, req.CropID, cmdID, req.Quantity)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if h.readCache != nil {
		h.readCache.Invalidate(r.Context(), userID, userID)
	}
	writeJSON(w, http.StatusOK, economicResp{EventID: res.EventID, CoinBalance: res.CoinBalance})
}

func (h *ShopHandler) handleSell(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	cmdID, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}

	var req sellReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "请求体解析失败")
		return
	}
	if req.CropID == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "crop_id 不能为空")
		return
	}
	if req.Quantity <= 0 {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "quantity 必须 ≥ 1")
		return
	}
	// 上限校验与领域常量一致；权威事务内还会再校验一次，此处只为让客户端拿到 400
	// 而不是把一个必然失败的命令送到数据库。
	if req.Quantity > domain.MaxEconomyQuantity {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument,
			fmt.Sprintf("quantity 不能超过 %d", domain.MaxEconomyQuantity))
		return
	}

	res, err := h.commitEconomy(r.Context(), userID, domain.CmdSellCrop, req.CropID, cmdID, req.Quantity)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if h.readCache != nil {
		h.readCache.Invalidate(r.Context(), userID, userID)
	}
	writeJSON(w, http.StatusOK, economicResp{EventID: res.EventID, CoinBalance: res.CoinBalance})
}

// commitEconomy 路由到聚合器（若已启用）或直接调用 gamesvr。
func (h *ShopHandler) commitEconomy(ctx context.Context, userID int64, cmdType domain.CommandType, cropID, idemKey string, qty int64) (application.CommitResult, error) {
	if h.coalescer != nil {
		return h.coalescer.Submit(ctx, userID, cmdType, cropID, idemKey, qty)
	}
	return h.ecoClient.CommitFarmCommand(ctx, application.CommitRequest{
		Command: domain.Command{
			CmdID:     idemKey,
			FarmID:    userID,
			ActorUser: userID,
			Type:      cmdType,
			CropID:    cropID,
			Quantity:  qty,
		},
	})
}

func requireIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key, ok := id.NormalizeV7(r.Header.Get("Idempotency-Key"))
	if !ok {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidMetadata, "valid UUIDv7 Idempotency-Key required")
		return "", false
	}
	return key, true
}

// authUser 从请求中解析 Bearer token，返回 userID；失败时写错误响应并返回 false。
func (h *ShopHandler) authUser(w http.ResponseWriter, r *http.Request) (int64, bool) {
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
