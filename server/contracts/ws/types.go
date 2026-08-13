// Package ws 定义 WebSocket 帧的 Go 类型契约。
// 对应接口规范 03-HTTP-WebSocket-gRPC接口契约.md §4。
// 生产使用 Protobuf 二进制编码；H5 调试允许 JSON 等价表示。
package ws

// FrameType WS 帧类型。
type FrameType string

const (
	FrameTypeCommand  FrameType = "COMMAND"
	FrameTypeACK      FrameType = "ACK"
	FrameTypeEvent    FrameType = "EVENT"
	FrameTypeResume   FrameType = "RESUME"
	FrameTypeSnapshot FrameType = "SNAPSHOT"
	FrameTypeHandoff  FrameType = "HANDOFF"
	// FrameTypeSubscribeFarm switches the single watched farm after the client
	// has atomically installed that farm's HTTP Snapshot.
	FrameTypeSubscribeFarm FrameType = "SUBSCRIBE_FARM"
)

// Meta 是每个 WS 帧的公共头。
type Meta struct {
	Type            FrameType `json:"type"`
	Service         string    `json:"service,omitempty"`
	Method          string    `json:"method,omitempty"`
	ClientSeq       int64     `json:"client_seq,omitempty"`
	ServerSeq       int64     `json:"server_seq,omitempty"`
	AckServerSeq    int64     `json:"ack_server_seq,omitempty"`
	TimeoutMs       int32     `json:"timeout_ms,omitempty"`
	CmdID           string    `json:"cmd_id,omitempty"`
	FarmID          string    `json:"farm_id,omitempty"`
	BaseVersion     string    `json:"base_version,omitempty"` // 字符串避免 JS 精度丢失
	ProtocolVersion string    `json:"protocol_version,omitempty"`
}

// Frame 是完整的 WS 帧（meta + body）。
type Frame struct {
	Meta Meta        `json:"meta"`
	Body interface{} `json:"body"`
}

// ── 客户端命令 body ────────────────────────────────────────────────────────────

// PlantBody Plant 命令 body。
type PlantBody struct {
	PlotID     int32  `json:"plot_id"`
	SeedItemID string `json:"seed_item_id"`
}

// HarvestBody Harvest 命令 body。
type HarvestBody struct {
	PlotID int32 `json:"plot_id"`
	// 禁止客户端传成熟结果或产量；服务端以当前 UTC 时间和宠物配置计算。
}

// WaterBody Water 命令 body。
type WaterBody struct {
	PlotID int32 `json:"plot_id"`
}

// ResumeBody Resume 命令 body（重连时携带）。
type ResumeBody struct {
	LastSeq      int64  `json:"last_seq"`
	LocalVersion string `json:"local_version"`
}

// SubscribeFarmBody establishes the EVENT cursor for the watched farm. The
// version must be the version of the Snapshot already installed by the client.
type SubscribeFarmBody struct {
	SnapshotVersion string `json:"snapshot_version"`
}

// ── 服务端响应 body ───────────────────────────────────────────────────────────

// PlotPatch 是一个地块的增量变化，随 ACK/EVENT 下发。
type PlotPatch struct {
	PlotID         int32  `json:"plot_id"`
	State          string `json:"state,omitempty"` // "EMPTY" | "PLANTED"
	CropID         string `json:"crop_id,omitempty"`
	PlantedAt      string `json:"planted_at,omitempty"` // RFC 3339 毫秒 UTC
	MatureAt       string `json:"mature_at,omitempty"`  // RFC 3339 毫秒 UTC
	RemainingYield int64  `json:"remaining_yield"`
	// GrowthStage 是服务端惰性推导的客户端三态，随 ACK/EVENT 下发。
	// 取值：SEEDLING | SEMI_MATURE | MATURE。
	GrowthStage string `json:"growth_stage,omitempty"`
}

// ACKBody ACK body。
type ACKBody struct {
	Result       string    `json:"result"` // "OK" | 错误码
	NewVersion   string    `json:"new_version,omitempty"`
	Patch        PlotPatch `json:"patch,omitempty"`
	Replayed     bool      `json:"replayed,omitempty"` // true 表示幂等重放
	RetryAfterMs int64     `json:"retry_after_ms,omitempty"`
}

// FarmEventBody FarmPatched EVENT body。
type FarmEventBody struct {
	EventID     string    `json:"event_id"`
	Version     string    `json:"version"`
	Patch       PlotPatch `json:"patch"`
	ActorUserID string    `json:"actor_user_id"`
	CommandType string    `json:"command_type,omitempty"`
}

// MemberEventBody MemberJoined/MemberLeft EVENT body。
type MemberEventBody struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
}

// MailboxChangedBody contains only lightweight badge state. Mail titles,
// content and attachments remain HTTP pull data. Receivers MUST apply an
// event only when mailbox_version is greater than the last applied version.
type MailboxChangedBody struct {
	UnreadCount int64 `json:"unread_count"`
	Version     int64 `json:"mailbox_version"`
}

// SnapshotBody SNAPSHOT 帧 body，客户端需原子替换本地农场状态。
type SnapshotBody struct {
	FarmID    string      `json:"farm_id"`
	Version   string      `json:"version"`
	Plots     []PlotPatch `json:"plots"`
	ServerSeq int64       `json:"server_seq"` // 快照对应的 server_seq，后续 EVENT 从此+1 开始
}

// ErrorBody 错误帧 body（ACK result != OK 时携带）。
type ErrorBody struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable"`
	Details   map[string]string `json:"details,omitempty"`
}

// HandoffBody HANDOFF 帧 body，服务端通知客户端重连到新实例。
type HandoffBody struct {
	ResumeTicket string `json:"resume_ticket"`
	RetryAfterMs int32  `json:"retry_after_ms"`
	Reason       string `json:"reason"` // "SERVER_DRAINING" 等
}

// WS 帧层关闭码（应用保留区间 4000-4999，RFC 6455 §7.4.2）。
// 对应设计文档 §10.1 错误码表（文档编号 1001-1007 映射为 4001-4007）。
const (
	WSCloseAuthFailed    = 4001 // 鉴权失败，需重新登录
	WSCloseTokenExpired  = 4002 // token 过期/无效，刷新后重连
	WSCloseParamInvalid  = 4003 // 参数非法（farm_id/last_seq）
	WSCloseCursorExpired = 4004 // 游标过期，走 HTTP 全量同步
	WSCloseKicked        = 4005 // 被新连接踢旧
	WSCloseRateLimited   = 4006 // 限流触发
	WSCloseInternal      = 4007 // 服务内部错误
)
