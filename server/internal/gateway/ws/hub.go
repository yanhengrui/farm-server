// Package ws — Hub 管理农场观察者注册表，支持多人实时事件广播。
//
// 设计约束：
//   - 同一农场可有多个在线观察者（主人 + 访客）
//   - 每个 WebSocket 连接同时只订阅一个农场（最近操作的农场）
//   - 广播非阻塞：writeCh 满时直接丢弃，不阻塞广播方
//   - 安全性：Unsubscribe 在 close(done) 之前调用，确保不会向已关闭连接广播
//   - 访客上限：每农场同时最多 maxGuestViewers 个访客（农场主不计入上限）
package ws

import "sync"

const (
	writeChanCap    = 64 // 每连接写缓冲区容量
	maxGuestViewers = 5  // 每农场访客上限（主人不计）
)

// ErrFarmFull 表示农场访客已达上限，新访客无法进入。
var ErrFarmFull = errFarmFull{}

type errFarmFull struct{}

func (errFarmFull) Error() string { return "farm viewer limit reached" }

// Hub 维护 farm_id → 在线连接集合，用于 EVENT 广播。
type Hub struct {
	mu       sync.RWMutex
	viewers  map[int64]map[*connState]struct{} // farmID → connections
	connFarm map[*connState]int64              // reverse: conn → farmID
	onFirst  func(int64)
	onLast   func(int64)
}

func (h *Hub) WithSubscriptionHooks(onFirst, onLast func(int64)) {
	h.onFirst, h.onLast = onFirst, onLast
}

// NewHub 构造 Hub。
func NewHub() *Hub {
	return &Hub{
		viewers:  make(map[int64]map[*connState]struct{}),
		connFarm: make(map[*connState]int64),
	}
}

// Subscribe 将连接 c 注册为农场 farmID 的观察者，并把 version 作为该连接
// 已安装的 Snapshot 游标。
// 若连接已订阅其他农场，自动解订旧农场。
// 访客（c.userID != farmID）超过 maxGuestViewers 时返回 ErrFarmFull，不加入。
func (h *Hub) Subscribe(farmID, version int64, c *connState) error {
	h.mu.Lock()
	var first, last int64

	// 已订阅同一农场时只重置 Snapshot 游标。
	if cur, ok := h.connFarm[c]; ok && cur == farmID {
		c.farmID.Store(farmID)
		c.farmVersion.Store(version)
		h.mu.Unlock()
		return nil
	}

	// 先检查目标容量，失败时必须保留旧农场订阅。
	isOwner := c.userID == farmID
	if !isOwner {
		guestCount := 0
		for conn := range h.viewers[farmID] {
			if conn.userID != farmID {
				guestCount++
			}
		}
		if guestCount >= maxGuestViewers {
			h.mu.Unlock()
			return ErrFarmFull
		}
	}

	// 目标可进入后再解订旧农场。
	if old, ok := h.connFarm[c]; ok {
		delete(h.viewers[old], c)
		if len(h.viewers[old]) == 0 {
			delete(h.viewers, old)
			last = old
		}
		delete(h.connFarm, c)
	}

	if h.viewers[farmID] == nil {
		h.viewers[farmID] = make(map[*connState]struct{})
		first = farmID
	}
	// 在连接进入 viewers 前安装 farm/version，使并发广播不会看到半切换状态。
	c.farmID.Store(farmID)
	c.farmVersion.Store(version)
	h.viewers[farmID][c] = struct{}{}
	h.connFarm[c] = farmID
	h.mu.Unlock()
	if last != 0 && h.onLast != nil {
		h.onLast(last)
	}
	if first != 0 && h.onFirst != nil {
		h.onFirst(first)
	}
	return nil
}

// Unsubscribe 将连接 c 从其当前订阅的农场中移除。
// 必须在 close(c.done) 之前调用，以保证广播不会向已关闭的 done channel 发送。
func (h *Hub) Unsubscribe(c *connState) {
	h.mu.Lock()
	var last int64
	if farmID, ok := h.connFarm[c]; ok {
		delete(h.viewers[farmID], c)
		if len(h.viewers[farmID]) == 0 {
			delete(h.viewers, farmID)
			last = farmID
		}
		delete(h.connFarm, c)
	}
	h.mu.Unlock()
	if last != 0 && h.onLast != nil {
		h.onLast(last)
	}
}

// Broadcast 向 farmID 的所有在线观察者（除 except 外）非阻塞地推送帧。
// writeCh 满时丢弃该次推送，不阻塞调用方。
func (h *Hub) Broadcast(farmID int64, frame []byte, except *connState) {
	h.mu.RLock()
	targets := make([]*connState, 0, len(h.viewers[farmID]))
	for c := range h.viewers[farmID] {
		if c != except {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range targets {
		select {
		case c.writeCh <- frame:
		case <-c.done:
			// 连接正在关闭，忽略
		default:
			// 写缓冲满，丢弃此次广播（非关键事件）
		}
	}
}

// GetViewers 返回 farmID 当前所有观察者（除 except 外）的快照切片。
// 供调用方对每个接收者生成带独立 server_seq 的帧。
func (h *Hub) GetViewers(farmID int64, except *connState) []*connState {
	h.mu.RLock()
	out := make([]*connState, 0, len(h.viewers[farmID]))
	for c := range h.viewers[farmID] {
		if c != except {
			out = append(out, c)
		}
	}
	h.mu.RUnlock()
	return out
}

// FarmViewerCount 返回当前订阅 farmID 的连接数（主要用于测试和指标）。
func (h *Hub) FarmViewerCount(farmID int64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.viewers[farmID])
}
