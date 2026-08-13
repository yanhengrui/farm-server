package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
	"github.com/photon/farm-server/server/pkg/shard"
)

// FriendEdgeProjector consumes the two event legs of a cross-shard friendship
// Saga. It is deliberately a single Kafka consumer group with access to all
// shard pools; routing selects exactly one local transaction per event.
type FriendEdgeProjector struct {
	router *shard.Router
	sagas  map[string]*FriendEdgeSaga
}

func NewFriendEdgeProjector(router *shard.Router, sagas map[string]*FriendEdgeSaga) (*FriendEdgeProjector, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	for _, name := range router.Shards() {
		if sagas[name] == nil {
			return nil, fmt.Errorf("missing friend edge saga for shard %q", name)
		}
	}
	return &FriendEdgeProjector{router: router, sagas: sagas}, nil
}

func (p *FriendEdgeProjector) Name() string { return "friend-edge-projector" }

func (p *FriendEdgeProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	switch string(env.EventType) {
	case string(socialevents.EventTypeFriendEdgeRequested):
		var payload socialevents.FriendEdgeRequestedPayload
		if err := decodeFriendEdgePayload(env.Payload, &payload); err != nil {
			return err
		}
		sourceID, targetID, err := friendEdgeIDs(payload.SourceUserID, payload.TargetUserID)
		if err != nil {
			return err
		}
		if payload.SourceEventID == "" {
			return fmt.Errorf("friend edge request source_event_id is required")
		}
		name, err := p.router.ShardForUserID(targetID)
		if err != nil {
			return err
		}
		return p.sagas[name].ApplyRemoteWithDisplayNames(ctx, sourceID, targetID, payload.SourceEventID, payload.SourceDisplayName, payload.TargetDisplayName)
	case string(socialevents.EventTypeFriendEdgeApplied):
		var payload socialevents.FriendEdgeAppliedPayload
		if err := decodeFriendEdgePayload(env.Payload, &payload); err != nil {
			return err
		}
		sourceID, targetID, err := friendEdgeIDs(payload.SourceUserID, payload.TargetUserID)
		if err != nil {
			return err
		}
		if payload.SourceEventID == "" {
			return fmt.Errorf("friend edge acknowledgement source_event_id is required")
		}
		name, err := p.router.ShardForUserID(sourceID)
		if err != nil {
			return err
		}
		return p.sagas[name].ConfirmSourceWithDisplayNames(ctx, sourceID, targetID, payload.SourceEventID, payload.SourceDisplayName, payload.TargetDisplayName)
	default:
		return nil
	}
}

func decodeFriendEdgePayload(raw any, dst any) error {
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal friend edge payload: %w", err)
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("unmarshal friend edge payload: %w", err)
	}
	return nil
}

func friendEdgeIDs(source, target string) (int64, int64, error) {
	sourceID, err := strconv.ParseInt(source, 10, 64)
	if err != nil || sourceID <= 0 {
		return 0, 0, fmt.Errorf("invalid source_user_id %q", source)
	}
	targetID, err := strconv.ParseInt(target, 10, 64)
	if err != nil || targetID <= 0 || targetID == sourceID {
		return 0, 0, fmt.Errorf("invalid target_user_id %q", target)
	}
	return sourceID, targetID, nil
}
