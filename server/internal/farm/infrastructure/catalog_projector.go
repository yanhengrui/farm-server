// Package infrastructure 提供图鉴投影器 CatalogProjector。
// CatalogProjector 消费 farm.harvested.v1 事件，按 crop_id 解锁玩家图鉴。
// catalog_key = "crop_" + cropID（如 "crop_wheat"）。
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
)

// CatalogProjector 消费收获事件，向 catalog_unlocks 写入图鉴解锁记录。
type CatalogProjector struct {
	db  *sql.DB
	log *slog.Logger
}

// NewCatalogProjector 构造 CatalogProjector。
func NewCatalogProjector(db *sql.DB, log *slog.Logger) *CatalogProjector {
	return &CatalogProjector{db: db, log: log}
}

// Name 返回消费者去重名称。
func (p *CatalogProjector) Name() string { return "catalog-projector" }

// Handle 处理农场事件；只处理 farm.harvested.v1，其余事件静默忽略。
func (p *CatalogProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	if env.EventType != farmevents.EventTypeFarmHarvested {
		return nil
	}

	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("catalog_projector marshal: %w", err)
	}
	var payload farmevents.FarmHarvestedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("catalog_projector unmarshal: %w", err)
	}
	if payload.OwnerUserID == "" || payload.CropID == "" {
		p.log.Warn("catalog_projector: missing owner or crop",
			slog.String("event_id", env.EventID),
		)
		return nil
	}

	ownerID, err := strconv.ParseInt(payload.OwnerUserID, 10, 64)
	if err != nil {
		return fmt.Errorf("catalog_projector parse owner: %w", err)
	}

	catalogKey, ok := catalogKeyForCrop(payload.CropID)
	if !ok {
		p.log.Warn("catalog_projector: unknown crop", slog.String("crop_id", payload.CropID), slog.String("event_id", env.EventID))
		return nil
	}
	now := time.Now().UTC()
	if _, err := p.db.ExecContext(ctx,
		`INSERT IGNORE INTO catalog_unlocks (user_id, catalog_key, unlocked_at) VALUES (?,?,?)`,
		uint64(ownerID), catalogKey, now,
	); err != nil {
		return fmt.Errorf("catalog_projector insert: %w", err)
	}

	p.log.Info("catalog unlocked",
		slog.Int64("user_id", ownerID),
		slog.String("catalog_key", catalogKey),
	)
	return nil
}
