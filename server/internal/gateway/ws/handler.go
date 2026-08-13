// Package ws 实现 gatesvr 的 WebSocket 接入层。
//
// 并发模型（每连接三 goroutine）：
//
//	reader goroutine（serveConn）
//	writer goroutine（startWriter）+ ping ticker
//	kick goroutine（ConnStore.ListenKick）
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	wscontract "github.com/photon/farm-server/server/contracts/ws"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/realtime"
	"github.com/photon/farm-server/server/pkg/admission"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/session"
)

// 断线重连语义（路线 7.5）：ReplayCache 已砍除。
// 重连后客户端直接 GET /api/v1/farm/snapshot 全量拉取，不走增量补发。
// ConnStore（全局单连接踢旧）保留不变。

const (
	pingInterval = 20 * time.Second
	idleTimeout  = 60 * time.Second
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// HandlerDeps 包含 Redis 可选依赖（通过 WithRedis 注入）。
type HandlerDeps struct {
	SessionStore *redisstore.SessionStore
	AuthSessions *redisstore.RefreshStore
	ConnStore    *redisstore.ConnStore
	InstanceID   string
}

type FarmCommandClient interface {
	SubmitCommand(ctx context.Context, cmd domain.Command) (application.CommitResult, error)
}

// Handler 管理 WebSocket 连接升级与请求分发。
type Handler struct {
	ctx         context.Context
	tokenSecret []byte
	farmClient  FarmCommandClient
	hub         *Hub
	log         *slog.Logger

	sessionStore   *redisstore.SessionStore
	authSessions   *redisstore.RefreshStore
	connStore      *redisstore.ConnStore
	metrics        *observability.Metrics
	commandLimiter *admission.Limiter
	retryAfter     time.Duration
	farmEvents     *realtime.Subscriber
	committedHook  func(context.Context, domain.Command)
	instanceID     string
	connsMu        sync.RWMutex
	conns          map[*connState]struct{}
}

func (h *Handler) WithFarmEvents(events *realtime.Subscriber) error {
	if events == nil {
		return nil
	}
	if err := events.Start(h.ctx, h.broadcastRemoteEvent); err != nil {
		return err
	}
	h.farmEvents = events
	h.hub.WithSubscriptionHooks(events.Subscribe, events.Unsubscribe)
	return nil
}

// WithMetrics enables connection and slow-consumer metrics.
func (h *Handler) WithMetrics(metrics *observability.Metrics) *Handler { h.metrics = metrics; return h }

func (h *Handler) WithCommandLimiter(l *admission.Limiter, retryAfter time.Duration) *Handler {
	h.commandLimiter = l
	h.retryAfter = retryAfter
	return h
}

// WithCommittedHook installs best-effort post-commit maintenance such as
// invalidating derived read caches. It never changes command success.
func (h *Handler) WithCommittedHook(hook func(context.Context, domain.Command)) *Handler {
	h.committedHook = hook
	return h
}

// NewHandler 构造 Handler，签名保持不变以兼容测试。
func NewHandler(ctx context.Context, secret []byte, farmClient FarmCommandClient, log *slog.Logger) *Handler {
	return &Handler{
		ctx:         ctx,
		tokenSecret: secret,
		farmClient:  farmClient,
		hub:         NewHub(),
		log:         log,
		conns:       make(map[*connState]struct{}),
	}
}

// WithRedis 注入 Redis 依赖并返回同一 Handler（支持链式调用）。
func (h *Handler) WithRedis(deps HandlerDeps) *Handler {
	h.sessionStore = deps.SessionStore
	h.authSessions = deps.AuthSessions
	h.connStore = deps.ConnStore
	h.instanceID = deps.InstanceID
	return h
}

// RegisterRoutes 注册 WSS 升级端点。
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ws", h.handleUpgrade)
}

// PushMailboxChanged sends a best-effort badge invalidation to the local
// connection for userID. Offline users reconcile through the HTTP summary.
func (h *Handler) PushMailboxChanged(userID, unreadCount, version int64) {
	h.connsMu.RLock()
	targets := make([]*connState, 0, 1)
	for connection := range h.conns {
		if connection.userID == userID {
			targets = append(targets, connection)
		}
	}
	h.connsMu.RUnlock()
	for _, connection := range targets {
		connection.mailboxMu.Lock()
		if version <= connection.mailboxVersion {
			if h.metrics != nil {
				h.metrics.MailboxOutOfOrderDropped()
			}
			connection.mailboxMu.Unlock()
			continue
		}
		seq := connection.serverSeq.Add(1)
		raw, err := json.Marshal(wscontract.Frame{
			Meta: wscontract.Meta{Type: wscontract.FrameTypeEvent, Service: "mail", Method: "MailboxChanged", ServerSeq: seq},
			Body: wscontract.MailboxChangedBody{UnreadCount: unreadCount, Version: version},
		})
		if err != nil {
			connection.mailboxMu.Unlock()
			continue
		}
		select {
		case connection.writeCh <- raw:
			connection.mailboxVersion = version
		case <-connection.done:
		default:
			if h.metrics != nil {
				h.metrics.MailboxQueueDropped()
				h.metrics.WSSlowConsumer()
			}
		}
		connection.mailboxMu.Unlock()
	}
}

// connState 持有单个 WebSocket 连接的会话状态。
type connState struct {
	conn           *websocket.Conn
	userID         int64
	sessionID      string
	connectionID   string
	ownerEpoch     int64
	lastClientSeq  int64
	serverSeq      atomic.Int64
	farmID         atomic.Int64
	farmVersion    atomic.Int64
	mailboxMu      sync.Mutex
	mailboxVersion int64
	// writeCh 由 writer goroutine 独占消费；reader goroutine 和 broadcastEvent 写入。
	writeCh chan []byte
	// done 在连接退出时关闭（先 Unsubscribe 后 close(done)）。
	done chan struct{}
}

func newConnState(conn *websocket.Conn, userID int64, sessionID string) *connState {
	return &connState{
		conn:         conn,
		userID:       userID,
		sessionID:    sessionID,
		connectionID: id.NewV7(),
		writeCh:      make(chan []byte, writeChanCap),
		done:         make(chan struct{}),
	}
}

// legacySessionID 为滚动更新期间的 v1 token 提取兼容会话 ID。
func legacySessionID(token string) string {
	if idx := strings.LastIndexByte(token, '.'); idx >= 0 {
		return token[idx+1:]
	}
	return token
}

func (h *Handler) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Error(w, "token required", http.StatusUnauthorized)
		return
	}
	claims, err := session.ParseClaims(token, h.tokenSecret)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	userID := claims.UserID
	sessionID := claims.SessionID
	if sessionID == "" {
		sessionID = legacySessionID(token)
	} else if h.authSessions != nil {
		active, validateErr := h.authSessions.IsActive(r.Context(), sessionID, userID)
		if validateErr != nil {
			http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
			return
		}
		if !active {
			http.Error(w, "session logged out or expired", http.StatusUnauthorized)
			return
		}
	}
	var lastSeq, ownerEpoch int64
	if h.sessionStore != nil {
		if ticket := r.URL.Query().Get("resume_ticket"); ticket != "" {
			if err := h.sessionStore.ConsumeResume(r.Context(), ticket, sessionID); err != nil {
				http.Error(w, "invalid resume ticket", http.StatusUnauthorized)
				return
			}
		}
		if sess, loadErr := h.sessionStore.Load(r.Context(), sessionID); loadErr == nil {
			lastSeq = sess.LastSeq
		}
		ownerEpoch, err = h.sessionStore.Claim(r.Context(), sessionID, userID, userID, h.instanceID)
		if err != nil {
			http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
			return
		}
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", slog.String("error", err.Error()))
		return
	}

	c := newConnState(conn, userID, sessionID)
	c.serverSeq.Store(lastSeq)
	c.ownerEpoch = ownerEpoch

	// 兼容既有客户端：连接时先观察自己的农场，游标未知为 0。客户端安装
	// Snapshot 后应发送 SUBSCRIBE_FARM，以明确当前观看农场和版本游标。
	_ = h.hub.Subscribe(userID, 0, c)
	h.connsMu.Lock()
	h.conns[c] = struct{}{}
	h.connsMu.Unlock()

	go h.serveConn(h.ctx, c)
}

// serveConn 是单连接的 reader goroutine，随 ctx 或连接断开退出。
func (h *Handler) serveConn(ctx context.Context, c *connState) {
	if h.metrics != nil {
		defer h.metrics.WSConnected()()
	}
	writerDone := make(chan struct{})
	go h.startWriter(c, writerDone)

	// kick goroutine：必须在 Register 之前订阅，避免漏接踢人消息。
	var kickStopped <-chan struct{}
	if h.connStore != nil {
		kickStopped = h.connStore.ListenKick(ctx, c.userID, c.connectionID, c.done, func() {
			h.sendClose(c, wscontract.WSCloseKicked, "kicked by new connection")
		})
		if err := h.connStore.Register(ctx, c.userID, c.connectionID); err != nil {
			h.log.Warn("connstore register failed", slog.Int64("user_id", c.userID), slog.String("err", err.Error()))
		}
	}

	defer func() {
		h.connsMu.Lock()
		delete(h.conns, c)
		h.connsMu.Unlock()
		h.hub.Unsubscribe(c)
		close(c.done)
		<-writerDone
		if h.connStore != nil {
			_ = h.connStore.Unregister(ctx, c.userID, c.connectionID)
			if kickStopped != nil {
				<-kickStopped
			}
		}
		c.conn.Close()
		h.log.Info("ws disconnected", slog.Int64("user_id", c.userID))
	}()

	// 空闲超时：60s 无数据则断开。
	_ = c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	c.conn.SetPongHandler(func(_ string) error {
		return c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	})

	h.log.Info("ws connected", slog.Int64("user_id", c.userID))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				h.log.Info("ws unexpected close", slog.Int64("user_id", c.userID),
					slog.String("err", err.Error()))
			}
			return
		}

		if err := h.handleFrame(ctx, c, raw); err != nil {
			h.log.Warn("frame error", slog.Int64("user_id", c.userID), slog.String("err", err.Error()))
		}
	}
}

// BeginHandoff marks active sessions HANDOFF_PENDING and sends one-time resume
// tickets before shutdown. Resuming never replays commands or EVENT frames;
// clients still replace their local state from the HTTP Snapshot endpoint.
func (h *Handler) BeginHandoff(ctx context.Context) {
	if h.sessionStore == nil {
		return
	}
	h.connsMu.RLock()
	connections := make([]*connState, 0, len(h.conns))
	for connection := range h.conns {
		connections = append(connections, connection)
	}
	h.connsMu.RUnlock()
	for _, connection := range connections {
		ticket := id.NewV7()
		if err := h.sessionStore.BeginHandoff(ctx, connection.sessionID, h.instanceID, connection.ownerEpoch, ticket, 30*time.Second); err != nil {
			continue
		}
		seq := connection.serverSeq.Add(1)
		frame := wscontract.Frame{
			Meta: wscontract.Meta{Type: wscontract.FrameTypeHandoff, ServerSeq: seq},
			Body: wscontract.HandoffBody{ResumeTicket: ticket, RetryAfterMs: 100, Reason: "SERVER_DRAINING"},
		}
		if raw, err := json.Marshal(frame); err == nil {
			if connection.push(raw) {
				_ = h.sessionStore.UpdateLastSeq(ctx, connection.sessionID, seq)
			}
		}
		time.AfterFunc(100*time.Millisecond, func() { h.sendClose(connection, websocket.CloseGoingAway, "server handoff") })
	}
}

// startWriter 序列化写操作，带 ping ticker（20s）。
func (h *Handler) startWriter(c *connState, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case raw, ok := <-c.writeCh:
			if !ok {
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
				return
			}
		case <-ticker.C:
			deadline := time.Now().Add(idleTimeout)
			if err := c.conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				return
			}
			if h.connStore != nil {
				_ = h.connStore.Refresh(context.Background(), c.userID, c.connectionID)
			}
		case <-c.done:
			// drain remaining messages before exiting
			for {
				select {
				case raw := <-c.writeCh:
					_ = c.conn.WriteMessage(websocket.TextMessage, raw)
				default:
					return
				}
			}
		}
	}
}

// push 将已序列化的 JSON 帧非阻塞地放入写队列。
func (c *connState) push(raw []byte) bool {
	select {
	case c.writeCh <- raw:
		return true
	case <-c.done:
		return false
	default:
		return false // 写缓冲满，丢弃
	}
}

// ── Frame handling ─────────────────────────────────────────────────────────────

type rawFrame struct {
	Meta wscontract.Meta `json:"meta"`
	Body json.RawMessage `json:"body"`
}

func (h *Handler) handleFrame(ctx context.Context, c *connState, raw []byte) error {
	var f rawFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return h.sendError(c, 0, "", errcode.CommonInvalidMetadata, "invalid frame json")
	}
	switch f.Meta.Type {
	case wscontract.FrameTypeCommand:
		return h.handleCommand(ctx, c, f)
	case wscontract.FrameTypeSubscribeFarm:
		return h.handleSubscribeFarm(ctx, c, f)
	default:
		return h.sendError(c, f.Meta.ClientSeq, f.Meta.CmdID, errcode.CommonInvalidMetadata,
			fmt.Sprintf("unsupported frame type: %s", f.Meta.Type))
	}
}

func (h *Handler) handleSubscribeFarm(ctx context.Context, c *connState, f rawFrame) error {
	farmID, err := strconv.ParseInt(f.Meta.FarmID, 10, 64)
	if err != nil || farmID <= 0 {
		return h.sendError(c, f.Meta.ClientSeq, f.Meta.CmdID, errcode.CommonInvalidArgument, "farm_id must be a positive integer")
	}
	var body wscontract.SubscribeFarmBody
	if err := json.Unmarshal(f.Body, &body); err != nil || body.SnapshotVersion == "" {
		return h.sendError(c, f.Meta.ClientSeq, f.Meta.CmdID, errcode.CommonInvalidArgument, "snapshot_version is required")
	}
	version, err := strconv.ParseInt(body.SnapshotVersion, 10, 64)
	if err != nil || version < 0 {
		return h.sendError(c, f.Meta.ClientSeq, f.Meta.CmdID, errcode.CommonInvalidArgument, "snapshot_version must be a non-negative integer")
	}
	if err := h.hub.Subscribe(farmID, version, c); err != nil {
		return h.sendError(c, f.Meta.ClientSeq, f.Meta.CmdID, errcode.ResourceExhausted, "farm viewer limit reached")
	}
	return h.sendSubscribeACK(ctx, c, f.Meta.ClientSeq, f.Meta.CmdID, version)
}

func (h *Handler) handleCommand(ctx context.Context, c *connState, f rawFrame) error {
	meta := f.Meta
	clientSeq := meta.ClientSeq
	if h.commandLimiter != nil && !h.commandLimiter.Allow(admission.UserKey(c.userID)) {
		return h.sendErrorWithRetry(c, clientSeq, meta.CmdID, errcode.CommonRateLimited, "command rate limited", h.retryAfter)
	}

	if clientSeq != 0 && clientSeq <= c.lastClientSeq {
		return h.sendError(c, clientSeq, meta.CmdID, errcode.CommonInvalidMetadata,
			fmt.Sprintf("client_seq stale: got %d, last %d", clientSeq, c.lastClientSeq))
	}

	cmd, err := h.parseCommand(c, meta, f.Body)
	if err != nil {
		return h.sendError(c, clientSeq, meta.CmdID, errcode.CommonInvalidArgument, err.Error())
	}

	result, err := h.farmClient.SubmitCommand(ctx, cmd)
	if err != nil {
		var ec *errcode.Error
		if errors.As(err, &ec) {
			return h.sendErrorWithRetry(c, clientSeq, meta.CmdID, ec.Code, ec.Message, errcode.RetryAfter(err))
		}
		return h.sendError(c, clientSeq, meta.CmdID, errcode.Internal, err.Error())
	}
	if h.committedHook != nil {
		h.committedHook(ctx, cmd)
	}

	if clientSeq != 0 {
		c.lastClientSeq = clientSeq
	}

	// 命令目标与当前观看农场是两个独立概念。只有显式 SUBSCRIBE_FARM
	// 才切换 Hub 订阅；当前页面收到 ACK 后可推进自己的版本游标。
	if c.farmID.Load() == cmd.FarmID {
		advanceFarmVersion(c, result.NewVersion)
	}

	// 回 ACK 给操作者。
	if err := h.sendACK(ctx, c, clientSeq, meta.CmdID, result); err != nil {
		return err
	}

	// 向农场其他观察者广播 EVENT（幂等重放不广播）。
	if !result.Replayed && h.farmEvents == nil {
		h.broadcastEvent(ctx, cmd.FarmID, cmd.Type, result, c)
	}

	return nil
}

func (h *Handler) broadcastRemoteEvent(event realtime.Message) {
	for _, viewer := range h.hub.GetViewers(event.FarmID, nil) {
		if viewer.farmID.Load() != event.FarmID {
			continue
		}
		last := viewer.farmVersion.Load()
		if last > 0 && event.FarmVersion <= last {
			continue
		}
		if last > 0 && event.FarmVersion > last+1 {
			if h.metrics != nil {
				h.metrics.FarmVersionGap()
			}
			// The current patch is still authoritative. Deliver it immediately so
			// viewers do not keep showing stale land while the client refreshes the
			// missing versions in the background.
		}
		patch := firstPlotPatch(event.Patch, time.Now().UTC())
		seq := viewer.serverSeq.Add(1)
		frame := wscontract.Frame{Meta: wscontract.Meta{Type: wscontract.FrameTypeEvent, ServerSeq: seq}, Body: wscontract.FarmEventBody{
			EventID: event.EventID, Version: strconv.FormatInt(event.FarmVersion, 10), Patch: patch, ActorUserID: strconv.FormatInt(event.ActorUserID, 10), CommandType: string(event.CommandType),
		}}
		raw, err := json.Marshal(frame)
		if err != nil {
			continue
		}
		if viewer.push(raw) {
			viewer.farmVersion.Store(event.FarmVersion)
		} else if h.metrics != nil {
			h.metrics.WSSlowConsumer()
		}
	}
}

func formatWSTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

func firstPlotPatch(patch domain.Patch, now time.Time) wscontract.PlotPatch {
	if len(patch.Plots) == 0 {
		return wscontract.PlotPatch{}
	}
	p := patch.Plots[0]
	out := wscontract.PlotPatch{
		PlotID:         p.PlotID,
		State:          string(p.Status),
		CropID:         p.CropID,
		PlantedAt:      formatWSTime(p.PlantedAt),
		MatureAt:       formatWSTime(p.MatureAt),
		RemainingYield: p.RemainingYield,
	}
	if p.Status != domain.PlotEmpty && p.Status != "" {
		out.GrowthStage = string(p.GrowthStage(now))
	}
	return out
}

func advanceFarmVersion(c *connState, version int64) {
	for current := c.farmVersion.Load(); version > current; current = c.farmVersion.Load() {
		if c.farmVersion.CompareAndSwap(current, version) {
			return
		}
	}
}

// parseCommand 根据 meta.Method 解析帧体为 domain.Command。
// farm_id 优先取 meta.FarmID，缺省为操作者自己的农场（P0）。
func (h *Handler) parseCommand(c *connState, meta wscontract.Meta, body json.RawMessage) (domain.Command, error) {
	cmdID, ok := id.NormalizeV7(meta.CmdID)
	if !ok {
		return domain.Command{}, fmt.Errorf("cmd_id must be a UUIDv7")
	}
	farmID := c.userID // default: own farm
	if meta.FarmID != "" {
		if id, err := strconv.ParseInt(meta.FarmID, 10, 64); err == nil {
			farmID = id
		}
	}
	if meta.BaseVersion == "" {
		return domain.Command{}, fmt.Errorf("base_version is required")
	}
	baseVersion, err := strconv.ParseInt(meta.BaseVersion, 10, 64)
	if err != nil || baseVersion < 0 {
		return domain.Command{}, fmt.Errorf("base_version must be a non-negative integer")
	}

	base := domain.Command{
		CmdID:       cmdID,
		FarmID:      farmID,
		ActorUser:   c.userID,
		BaseVersion: baseVersion,
	}

	switch meta.Method {
	case "farm.Plant":
		var b wscontract.PlantBody
		if err := json.Unmarshal(body, &b); err != nil {
			return domain.Command{}, fmt.Errorf("parse plant body: %w", err)
		}
		base.Type = domain.CmdPlant
		base.PlotID = b.PlotID
		base.CropID = b.SeedItemID
	case "farm.Harvest":
		var b wscontract.HarvestBody
		if err := json.Unmarshal(body, &b); err != nil {
			return domain.Command{}, fmt.Errorf("parse harvest body: %w", err)
		}
		base.Type = domain.CmdHarvest
		base.PlotID = b.PlotID
	case "farm.Water":
		var b wscontract.WaterBody
		if err := json.Unmarshal(body, &b); err != nil {
			return domain.Command{}, fmt.Errorf("parse water body: %w", err)
		}
		base.Type = domain.CmdWater
		base.PlotID = b.PlotID
	case "farm.HelpWater":
		var b wscontract.WaterBody
		if err := json.Unmarshal(body, &b); err != nil {
			return domain.Command{}, fmt.Errorf("parse help-water body: %w", err)
		}
		base.Type = domain.CmdHelpWater
		base.PlotID = b.PlotID
	case "farm.StealCrop":
		var b wscontract.HarvestBody
		if err := json.Unmarshal(body, &b); err != nil {
			return domain.Command{}, fmt.Errorf("parse steal-crop body: %w", err)
		}
		base.Type = domain.CmdStealCrop
		base.PlotID = b.PlotID
	default:
		return domain.Command{}, fmt.Errorf("unknown method: %s", meta.Method)
	}
	return base, nil
}

// sendACK 组装 ACK 帧并放入写队列，同时推送重放缓存并更新 session last_seq。
func (h *Handler) sendACK(ctx context.Context, c *connState, clientSeq int64, cmdID string, result application.CommitResult) error {
	seq := c.serverSeq.Add(1)
	patch := firstPlotPatch(result.Patch, time.Now().UTC())
	frame := wscontract.Frame{
		Meta: wscontract.Meta{
			Type:      wscontract.FrameTypeACK,
			ClientSeq: clientSeq,
			ServerSeq: seq,
			CmdID:     cmdID,
		},
		Body: wscontract.ACKBody{
			Result:     "OK",
			NewVersion: strconv.FormatInt(result.NewVersion, 10),
			Patch:      patch,
			Replayed:   result.Replayed,
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal ack: %w", err)
	}
	if !c.push(raw) && h.metrics != nil {
		h.metrics.WSSlowConsumer()
	}

	if h.sessionStore != nil {
		_ = h.sessionStore.UpdateLastSeq(ctx, c.sessionID, seq)
	}
	return nil
}

func (h *Handler) sendSubscribeACK(ctx context.Context, c *connState, clientSeq int64, cmdID string, version int64) error {
	seq := c.serverSeq.Add(1)
	frame := wscontract.Frame{
		Meta: wscontract.Meta{Type: wscontract.FrameTypeACK, ClientSeq: clientSeq, ServerSeq: seq, CmdID: cmdID},
		Body: wscontract.ACKBody{Result: "OK", NewVersion: strconv.FormatInt(version, 10)},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal subscribe ack: %w", err)
	}
	if !c.push(raw) && h.metrics != nil {
		h.metrics.WSSlowConsumer()
	}
	if h.sessionStore != nil {
		_ = h.sessionStore.UpdateLastSeq(ctx, c.sessionID, seq)
	}
	return nil
}

// sendError 组装错误 ACK 帧并放入写队列。
func (h *Handler) sendError(c *connState, clientSeq int64, cmdID string, code errcode.Code, msg string) error {
	return h.sendErrorWithRetry(c, clientSeq, cmdID, code, msg, 0)
}

func (h *Handler) sendErrorWithRetry(c *connState, clientSeq int64, cmdID string, code errcode.Code, msg string, retryAfter time.Duration) error {
	seq := c.serverSeq.Add(1)
	frame := wscontract.Frame{
		Meta: wscontract.Meta{
			Type:      wscontract.FrameTypeACK,
			ClientSeq: clientSeq,
			ServerSeq: seq,
			CmdID:     cmdID,
		},
		Body: wscontract.ACKBody{Result: string(code), RetryAfterMs: retryAfter.Milliseconds()},
	}
	if raw, err := json.Marshal(frame); err == nil {
		if !c.push(raw) && h.metrics != nil {
			h.metrics.WSSlowConsumer()
		}
	}
	return fmt.Errorf("ws error: %s: %s", code, msg)
}

// sendClose 发送 WS 关闭帧并关闭底层连接。
func (h *Handler) sendClose(c *connState, code int, reason string) {
	msg := websocket.FormatCloseMessage(code, reason)
	deadline := time.Now().Add(5 * time.Second)
	_ = c.conn.WriteControl(websocket.CloseMessage, msg, deadline)
	c.conn.Close()
}

// broadcastEvent 向农场其他观察者推送 EVENT 帧（每个接收者独立 server_seq + 重放缓存）。
func (h *Handler) broadcastEvent(ctx context.Context, farmID int64, commandType domain.CommandType, result application.CommitResult, except *connState) {
	patch := firstPlotPatch(result.Patch, time.Now().UTC())

	for _, viewer := range h.hub.GetViewers(farmID, except) {
		seq := viewer.serverSeq.Add(1)
		frame := wscontract.Frame{
			Meta: wscontract.Meta{
				Type:      wscontract.FrameTypeEvent,
				ServerSeq: seq,
			},
			Body: wscontract.FarmEventBody{
				EventID:     result.EventID,
				Version:     strconv.FormatInt(result.NewVersion, 10),
				Patch:       patch,
				ActorUserID: strconv.FormatInt(except.userID, 10),
				CommandType: string(commandType),
			},
		}
		raw, err := json.Marshal(frame)
		if err != nil {
			h.log.Error("marshal event frame", slog.String("err", err.Error()))
			continue
		}
		select {
		case viewer.writeCh <- raw:
		case <-viewer.done:
		default:
		}
	}
}
