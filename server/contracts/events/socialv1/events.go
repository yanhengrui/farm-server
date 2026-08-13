// Package socialv1 定义社交领域事件，用于 gamesvr → Kafka（通过 outbox_events）。
// 对应 events/social/v1；workersvr Relay 读取 outbox_events.payload_json 反序列化为此类型。
package socialv1

import "time"

// EventType 社交事件类型。
type EventType string

const (
	// EventTypeFriendAccepted 好友建立事件：inviter 和 accepter 双方建立好友关系。
	EventTypeFriendAccepted EventType = "social.friend_accepted.v1"
	// EventTypeFriendEdgeRequested asks the remote user's shard to create its
	// half of a friendship edge. The source event ID is the Saga idempotency key.
	EventTypeFriendEdgeRequested EventType = "social.friend_edge_requested.v1"
	// EventTypeFriendEdgeApplied confirms that the remote shard durably created
	// its edge. The source shard uses it to move PENDING to ACTIVE.
	EventTypeFriendEdgeApplied EventType = "social.friend_edge_applied.v1"
)

// EventEnvelope 是写入 outbox_events.payload_json 的统一信封。
type EventEnvelope struct {
	EventID       string    `json:"event_id"`
	EventType     EventType `json:"event_type"`
	AggregateType string    `json:"aggregate_type"` // "social"
	AggregateID   string    `json:"aggregate_id"`   // "uid_a:uid_b"（小 ID 在前）
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	TraceID       string    `json:"trace_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	Payload       any       `json:"payload"`
}

// FriendEdgeRequestedPayload describes one cross-shard friendship edge. The
// target_user_id owns the database transaction that applies this event.
type FriendEdgeRequestedPayload struct {
	SourceEventID     string    `json:"source_event_id"`
	SourceUserID      string    `json:"source_user_id"`
	TargetUserID      string    `json:"target_user_id"`
	SourceDisplayName string    `json:"source_display_name,omitempty"`
	TargetDisplayName string    `json:"target_display_name,omitempty"`
	RequestedAt       time.Time `json:"requested_at"`
}

// FriendEdgeAppliedPayload is emitted from the target shard in the same local
// transaction that creates the remote edge. SourceEventID remains the Saga
// idempotency key; EventID on the envelope is a distinct Kafka event ID.
type FriendEdgeAppliedPayload struct {
	SourceEventID     string    `json:"source_event_id"`
	SourceUserID      string    `json:"source_user_id"`
	TargetUserID      string    `json:"target_user_id"`
	SourceDisplayName string    `json:"source_display_name,omitempty"`
	TargetDisplayName string    `json:"target_display_name,omitempty"`
	AppliedAt         time.Time `json:"applied_at"`
}

// FriendAcceptedPayload 好友建立事件负载。
type FriendAcceptedPayload struct {
	UserIDA             string    `json:"user_id_a"` // 小 ID（inviter 或 accepter 中较小者）
	UserIDB             string    `json:"user_id_b"` // 大 ID
	InviterID           string    `json:"inviter_id"`
	AccepterID          string    `json:"accepter_id"`
	InviterDisplayName  string    `json:"inviter_display_name"`  // 邀请者昵称
	AccepterDisplayName string    `json:"accepter_display_name"` // 接受者昵称
	AcceptedAt          time.Time `json:"accepted_at"`
}
