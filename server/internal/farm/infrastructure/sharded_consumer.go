package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/photon/farm-server/server/pkg/shard"
)

// NewShardedConsumerDedupResolver keeps consumer idempotency beside the user
// aggregate affected by an event. Unknown/system events intentionally use the
// supplied fallback instead of guessing a shard.
func NewShardedConsumerDedupResolver(router *shard.Router, dedups map[string]*ConsumerDedup, fallback *ConsumerDedup) (ConsumerDedupResolver, error) {
	if router == nil || fallback == nil {
		return nil, fmt.Errorf("router and fallback consumer dedup are required")
	}
	for _, name := range router.Shards() {
		if dedups[name] == nil {
			return nil, fmt.Errorf("missing consumer dedup for shard %q", name)
		}
	}
	return func(_ context.Context, raw []byte) (*ConsumerDedup, error) {
		var envelope struct {
			EventType string          `json:"event_type"`
			Payload   json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("unmarshal event routing envelope: %w", err)
		}
		var payload struct {
			OwnerUserID  string `json:"owner_user_id"`
			SourceUserID string `json:"source_user_id"`
			TargetUserID string `json:"target_user_id"`
			UserIDA      string `json:"user_id_a"`
		}
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return nil, fmt.Errorf("unmarshal event routing payload: %w", err)
		}
		userID := payload.OwnerUserID
		switch envelope.EventType {
		case "social.friend_edge_requested.v1":
			userID = payload.TargetUserID
		case "social.friend_edge_applied.v1":
			userID = payload.SourceUserID
		case "social.friend_accepted.v1":
			userID = payload.UserIDA
		}
		if userID == "" {
			return fallback, nil
		}
		id, err := strconv.ParseInt(userID, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid user_id %q for event %q", userID, envelope.EventType)
		}
		name, err := router.ShardForUserID(id)
		if err != nil {
			return nil, err
		}
		return dedups[name], nil
	}, nil
}
