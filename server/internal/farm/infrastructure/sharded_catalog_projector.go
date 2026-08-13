package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedCatalogProjector routes a harvested owner's catalog projection to the
// owner shard instead of retaining the workersvr primary DB shortcut.
type ShardedCatalogProjector struct {
	router *shard.Router
	pools  map[string]*sql.DB
	log    *slog.Logger
}

func NewShardedCatalogProjector(router *shard.Router, pools map[string]*sql.DB, log *slog.Logger) (*ShardedCatalogProjector, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyPools := make(map[string]*sql.DB, len(pools))
	for _, name := range router.Shards() {
		if pools[name] == nil {
			return nil, fmt.Errorf("missing catalog pool for shard %q", name)
		}
		copyPools[name] = pools[name]
	}
	return &ShardedCatalogProjector{router: router, pools: copyPools, log: log}, nil
}

func (p *ShardedCatalogProjector) Name() string { return "catalog-projector" }

func (p *ShardedCatalogProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	if env.EventType != farmevents.EventTypeFarmHarvested {
		return nil
	}
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("sharded catalog marshal payload: %w", err)
	}
	var payload farmevents.FarmHarvestedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("sharded catalog unmarshal payload: %w", err)
	}
	ownerID, err := strconv.ParseInt(payload.OwnerUserID, 10, 64)
	if err != nil || ownerID <= 0 || payload.CropID == "" {
		return fmt.Errorf("invalid harvested catalog payload")
	}
	catalogKey, ok := catalogKeyForCrop(payload.CropID)
	if !ok {
		if p.log != nil {
			p.log.Warn("sharded catalog: unknown crop", slog.String("crop_id", payload.CropID), slog.String("event_id", env.EventID))
		}
		return nil
	}
	name, err := p.router.ShardForUserID(ownerID)
	if err != nil {
		return err
	}
	if _, err = p.pools[name].ExecContext(ctx,
		`INSERT IGNORE INTO catalog_unlocks (user_id, catalog_key, unlocked_at) VALUES (?,?,?)`,
		uint64(ownerID), catalogKey, time.Now().UTC()); err != nil {
		return fmt.Errorf("sharded catalog insert: %w", err)
	}
	if p.log != nil {
		p.log.Info("catalog unlocked", slog.Int64("user_id", ownerID), slog.String("catalog_key", catalogKey))
	}
	return nil
}

var _ EventProjector = (*ShardedCatalogProjector)(nil)
