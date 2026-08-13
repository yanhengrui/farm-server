package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/shard"
)

// CrossShardStealProjector is the destination half of the minimal asset Saga.
// The owner shard atomically saves the farm mutation and farm.stolen outbox;
// this projector atomically records an actor-shard ledger idempotency key and
// credits the stolen crop. It never uses a cross-database transaction.
type CrossShardStealProjector struct {
	router *shard.Router
	pools  map[string]*sql.DB
}

func NewCrossShardStealProjector(router *shard.Router, pools map[string]*sql.DB) (*CrossShardStealProjector, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyPools := make(map[string]*sql.DB, len(pools))
	for _, name := range router.Shards() {
		if pools[name] == nil {
			return nil, fmt.Errorf("missing asset shard pool %q", name)
		}
		copyPools[name] = pools[name]
	}
	return &CrossShardStealProjector{router: router, pools: copyPools}, nil
}

func (p *CrossShardStealProjector) Name() string { return "cross-shard-steal-projector" }

func (p *CrossShardStealProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	if env.EventType != farmevents.EventTypeFarmStolen {
		return nil
	}
	eventID, ok := id.NormalizeV7(env.EventID)
	if !ok {
		return fmt.Errorf("invalid farm.stolen event_id")
	}
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("marshal stolen payload: %w", err)
	}
	var payload farmevents.FarmStolenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("unmarshal stolen payload: %w", err)
	}
	ownerID, err := strconv.ParseInt(payload.OwnerUserID, 10, 64)
	if err != nil || ownerID <= 0 {
		return fmt.Errorf("invalid stolen owner_user_id %q", payload.OwnerUserID)
	}
	actorID, err := strconv.ParseInt(payload.ActorUserID, 10, 64)
	if err != nil || actorID <= 0 || actorID == ownerID {
		return fmt.Errorf("invalid stolen actor_user_id %q", payload.ActorUserID)
	}
	if payload.StolenAmount <= 0 || payload.CropID == "" {
		return fmt.Errorf("invalid stolen asset payload")
	}
	ownerShard, err := p.router.ShardForUserID(ownerID)
	if err != nil {
		return err
	}
	actorShard, err := p.router.ShardForUserID(actorID)
	if err != nil {
		return err
	}
	if ownerShard == actorShard {
		return nil // same-shard path already credited in the authority transaction
	}
	return creditCrossShardSteal(ctx, p.pools[actorShard], actorID, payload.CropID, payload.StolenAmount, eventID)
}

func creditCrossShardSteal(ctx context.Context, db *sql.DB, actorID int64, cropID string, quantity int64, eventID string) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin cross-shard steal credit: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO economy_transactions
		(user_id, biz_type, biz_id, currency_type, balance_before, balance_delta, balance_after, item_changes_json, source_type, status, created_at)
		VALUES (?, 'CROSS_SHARD_STEAL', UNHEX(?), 'CROP', 0, 0, 0, JSON_OBJECT('crop_id', ?, 'quantity', ?), 'FARM_STOLEN', 'COMMITTED', ?)`,
		uint64(actorID), eventID, cropID, quantity, now,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return nil // ledger row and inventory credit were committed together
		}
		return fmt.Errorf("insert cross-shard steal ledger: %w", err)
	}
	if err := addInventoryItem(ctx, tx, actorID, "CROP", cropIDToItemID(cropID), quantity, now); err != nil {
		return fmt.Errorf("credit cross-shard stolen inventory: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cross-shard steal credit: %w", err)
	}
	return nil
}

// NewCrossShardStealDedupResolver routes the asset projector's consumer
// receipt to the actor shard (rather than the farm owner shard used by the
// ordinary farm projectors).
func NewCrossShardStealDedupResolver(router *shard.Router, dedups map[string]*ConsumerDedup, fallback *ConsumerDedup) (ConsumerDedupResolver, error) {
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
			return nil, err
		}
		if envelope.EventType != string(farmevents.EventTypeFarmStolen) {
			return fallback, nil
		}
		var payload struct {
			ActorUserID string `json:"actor_user_id"`
		}
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return nil, err
		}
		actorID, err := strconv.ParseInt(payload.ActorUserID, 10, 64)
		if err != nil || actorID <= 0 {
			return nil, fmt.Errorf("invalid farm.stolen actor_user_id %q", payload.ActorUserID)
		}
		name, err := router.ShardForUserID(actorID)
		if err != nil {
			return nil, err
		}
		return dedups[name], nil
	}, nil
}
