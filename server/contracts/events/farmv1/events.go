// Package farmv1 定义农场领域事件，用于 gamesvr → Kafka（通过 outbox_events）。
// 对应 events/farm/v1；workersvr Relay 读取 outbox_events.payload_json 反序列化为此类型。
package farmv1

import "time"

// EventType 农场事件类型。
type EventType string

const (
	EventTypeFarmPlanted   EventType = "farm.planted.v1"
	EventTypeFarmHarvested EventType = "farm.harvested.v1"
	EventTypeFarmWatered   EventType = "farm.watered.v1"
	// EventTypeFarmStolen 偷菜事件：好友从农场主地块偷取作物，收益归 ActorUser。
	// 下游任务/邮件通知可据此区分自收和偷取行为。
	EventTypeFarmStolen EventType = "farm.stolen.v1"
)

// EventEnvelope 是写入 outbox_events.payload_json 的统一信封。
type EventEnvelope struct {
	EventID       string    `json:"event_id"` // UUIDv7 hex
	EventType     EventType `json:"event_type"`
	AggregateType string    `json:"aggregate_type"` // "farm"
	AggregateID   string    `json:"aggregate_id"`   // farm_id 字符串
	SchemaVersion int       `json:"schema_version"` // 当前 1
	OccurredAt    time.Time `json:"occurred_at"`    // UTC
	TraceID       string    `json:"trace_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	Payload       any       `json:"payload"`
}

// FarmPlantedPayload 播种事件负载。
type FarmPlantedPayload struct {
	FarmID      string    `json:"farm_id"`
	OwnerUserID string    `json:"owner_user_id"`
	ActorUserID string    `json:"actor_user_id"`
	FarmVersion int64     `json:"farm_version"`
	PlotID      int32     `json:"plot_id"`
	CropID      string    `json:"crop_id"`
	CropCycle   int32     `json:"crop_cycle"`
	PlantedAt   time.Time `json:"planted_at"`
	MatureAt    time.Time `json:"mature_at"`
	CmdID       string    `json:"cmd_id"`
}

// FarmHarvestedPayload 收获事件负载。
type FarmHarvestedPayload struct {
	FarmID      string    `json:"farm_id"`
	OwnerUserID string    `json:"owner_user_id"`
	ActorUserID string    `json:"actor_user_id"`
	FarmVersion int64     `json:"farm_version"`
	PlotID      int32     `json:"plot_id"`
	CropID      string    `json:"crop_id"`
	CropCycle   int32     `json:"crop_cycle"`
	Yield       int64     `json:"yield"` // 本次收获数量（服务端计算）
	Mode        string    `json:"mode"`  // "MANUAL" | "PET_AUTO"
	HarvestedAt time.Time `json:"harvested_at"`
	CmdID       string    `json:"cmd_id"`
}

// FarmWateredPayload 浇水事件负载。
type FarmWateredPayload struct {
	FarmID      string    `json:"farm_id"`
	OwnerUserID string    `json:"owner_user_id"`
	ActorUserID string    `json:"actor_user_id"`
	FarmVersion int64     `json:"farm_version"`
	PlotID      int32     `json:"plot_id"`
	WateredAt   time.Time `json:"watered_at"`
	CmdID       string    `json:"cmd_id"`
}

// FarmStolenPayload 偷菜事件负载。
// ActorUserID 是实施偷菜的好友（收益归属），OwnerUserID 是农场主（被偷方）。
// StolenAmount = floor(HarvestYield × 20%)，即实际被偷数量（非地块总产量）。
type FarmStolenPayload struct {
	FarmID           string    `json:"farm_id"`
	OwnerUserID      string    `json:"owner_user_id"`      // 农场主（被偷方）
	ActorUserID      string    `json:"actor_user_id"`      // 实施偷菜的好友
	OwnerDisplayName string    `json:"owner_display_name"` // 农场主昵称（查不到时为 ID 字符串）
	ActorDisplayName string    `json:"actor_display_name"` // 偷菜者昵称（查不到时为 ID 字符串）
	FarmVersion      int64     `json:"farm_version"`
	PlotID           int32     `json:"plot_id"`
	CropID           string    `json:"crop_id"`
	StolenAmount     int64     `json:"stolen_amount"` // 被偷数量（原 yield，现为实际偷取量）
	RemainingYield   int64     `json:"remaining_yield"`
	StolenAt         time.Time `json:"stolen_at"`
	CmdID            string    `json:"cmd_id"`
}
