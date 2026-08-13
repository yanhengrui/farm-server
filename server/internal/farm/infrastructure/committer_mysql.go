// Package infrastructure 提供 farm 相关端口的实现。
// MySQLCommitter 是 gamesvr 的权威持久化实现，在单笔 MySQL 事务内按统一锁顺序写入
// farm_snapshots、inventory/wallet、economy_transactions、cmd_receipts、outbox_events。
package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/mysqlretry"
	"github.com/photon/farm-server/server/pkg/observability"
)

// MySQLCommitter 在单笔 MySQL 事务内完成农场命令的权威持久化提交。
// 全局锁顺序：farm → claim → wallet → inventory（不涉及的节点跳过）；
// 没有自然业务唯一键的资产命令才写短期 cmd_receipts，经济命令使用
// economy_transactions 唯一键。
type MySQLCommitter struct {
	db                     *sql.DB
	clk                    clock.Clock
	observer               EconomyTransactionObserver
	useFriendshipEdges     bool
	deferRemoteStealCredit func(ownerUserID, actorUserID int64) bool
}

// EconomyTransactionObserver keeps the authoritative hot path independent of
// a concrete metrics backend. All string values passed to it come from bounded
// vocabularies in this file; entity IDs are never labels.
type EconomyTransactionObserver interface {
	EconomyStage(operation, stage, result string, duration time.Duration)
	EconomyTransactionDelta(operation string, delta int)
	DBPoolAcquire(operation, result string, duration time.Duration)
	InternalError(source, operation, stage, code string)
}

// NewMySQLCommitter 构造 MySQL Committer。db 由调用方构造，Committer 不关闭它。
func NewMySQLCommitter(db *sql.DB, clk clock.Clock) *MySQLCommitter {
	return &MySQLCommitter{db: db, clk: clk}
}

func (c *MySQLCommitter) WithObserver(observer EconomyTransactionObserver) *MySQLCommitter {
	c.observer = observer
	return c
}

// WithFriendshipEdges enables the directed local friendship edge model used
// only by the multi-shard candidate. Legacy single-DSN deployments continue
// reading the existing friendships table until their migration is complete.
func (c *MySQLCommitter) WithFriendshipEdges() *MySQLCommitter {
	c.useFriendshipEdges = true
	return c
}

// WithDeferredRemoteStealCredit keeps the farm mutation on its owner shard
// and defers a remote thief's inventory credit to the farm.stolen Outbox
// consumer. Returning false preserves the original same-shard transaction.
func (c *MySQLCommitter) WithDeferredRemoteStealCredit(decider func(ownerUserID, actorUserID int64) bool) *MySQLCommitter {
	c.deferRemoteStealCredit = decider
	return c
}

var _ application.Committer = (*MySQLCommitter)(nil)
var _ application.SnapshotLoader = (*MySQLCommitter)(nil)
var _ application.RouteFencer = (*MySQLCommitter)(nil)

// LoadSnapshot 从 farm_snapshots 读取快照；行不存在时返回版本 0 的空快照。
func (c *MySQLCommitter) LoadSnapshot(ctx context.Context, farmID int64) (domain.Snapshot, error) {
	const q = `SELECT owner_user_id, version, snapshot, route_epoch FROM farm_snapshots WHERE farm_id = ?`
	row := c.db.QueryRowContext(ctx, q, uint64(farmID))

	var ownerID int64
	var version int64
	var raw []byte
	var routeEpoch int64
	if err := row.Scan(&ownerID, &version, &raw, &routeEpoch); err == sql.ErrNoRows {
		return emptySnapshot(farmID), nil
	} else if err != nil {
		return domain.Snapshot{}, fmt.Errorf("load_snapshot scan farm_id=%d: %w", farmID, err)
	}
	snap, err := unmarshalSnapshot(farmID, ownerID, version, raw)
	snap.RouteEpoch = routeEpoch
	return snap, err
}

// AdvanceRouteEpoch is the durable ownership cutover. It locks the farm row,
// advances only monotonically, and commits before the new farmsvr activates
// the actor. A stale owner can therefore never move the fence backwards.
func (c *MySQLCommitter) AdvanceRouteEpoch(ctx context.Context, farmID, epoch int64) error {
	if epoch <= 0 {
		return errcode.New(errcode.CommonInvalidArgument, "route epoch must be positive")
	}
	return mysqlretry.Do(ctx, mysqlretry.IsTransient, func() error {
		return c.advanceRouteEpochOnce(ctx, farmID, epoch)
	})
}

func (c *MySQLCommitter) advanceRouteEpochOnce(ctx context.Context, farmID, epoch int64) error {
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin route fence: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	var current int64
	if err = tx.QueryRowContext(ctx, `SELECT route_epoch FROM farm_snapshots WHERE farm_id = ? FOR UPDATE`, uint64(farmID)).Scan(&current); err == sql.ErrNoRows {
		return errcode.New(errcode.FarmNotFound, "farm not found while advancing route epoch")
	} else if err != nil {
		return fmt.Errorf("lock route epoch farm_id=%d: %w", farmID, err)
	}
	if epoch < current {
		return errcode.New(errcode.RoutingEpochStale, "route epoch is older than persistent fence")
	}
	if epoch > current {
		if _, err = tx.ExecContext(ctx, `UPDATE farm_snapshots SET route_epoch = ?, updated_at = ? WHERE farm_id = ?`, uint64(epoch), c.clk.NowUTC(), uint64(farmID)); err != nil {
			return fmt.Errorf("advance route epoch farm_id=%d: %w", farmID, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit route fence farm_id=%d: %w", farmID, err)
	}
	return nil
}

// CommitFarmCommand 在单笔 MySQL 事务内应用命令并落持久化幂等凭据。
// 经济类命令（PurchaseSeed/SellCrop）走独立分支，不加载农场快照。
// 农场类命令（Plant/Water/Harvest）在快照行锁保护下同时写入库存变化。
func (c *MySQLCommitter) CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	economic := req.Command.Type == domain.CmdPurchaseSeed || req.Command.Type == domain.CmdSellCrop
	durableKey := economic || requiresCommandReceipt(req.Command.Type)
	return mysqlretry.Value(ctx, func(err error) bool {
		return mysqlretry.IsTransient(err) || durableKey && mysqlretry.IsDuplicateKey(err)
	}, func() (application.CommitResult, error) {
		return c.commitFarmCommandOnce(ctx, req)
	})
}

func (c *MySQLCommitter) commitFarmCommandOnce(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	cmd := req.Command
	now := c.clk.NowUTC()

	// 经济类命令：不需要农场快照锁。
	if cmd.Type == domain.CmdPurchaseSeed || cmd.Type == domain.CmdSellCrop {
		return c.commitEconomicTx(ctx, cmd, now)
	}

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("begin_tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	// Only asset-producing commands without a natural business key use the
	// bounded cmd_receipts fallback. State-only commands rely on base_version.
	if requiresCommandReceipt(cmd.Type) {
		if res, replayed, err := checkReceipt(ctx, tx, cmd.ActorUser, cmd.CmdID); err != nil {
			return application.CommitResult{}, err
		} else if replayed {
			return res, nil
		}
	}

	// 2. 加载快照并加行锁（Farm Actor 保证串行，此为事务内防御性二次锁）。
	// next_pet_action_at 随快照行一并读出，CmdPetAutoHarvest 直接在已锁行上校验，
	// 不再发起第二次 SELECT 同一行。
	snap, nextPetAt, err := loadSnapshotForUpdate(ctx, tx, cmd.FarmID)
	if err != nil {
		return application.CommitResult{}, err
	}
	if cmd.RouteEpoch != snap.RouteEpoch {
		return application.CommitResult{}, errcode.New(errcode.RoutingFenced, "route epoch does not own farm")
	}
	if cmd.Type == domain.CmdPetAutoHarvest {
		if cmd.ActorUser != snap.OwnerID {
			return application.CommitResult{}, errcode.New(errcode.SocialNotFarmOwner, "pet harvest actor must own farm")
		}
		if cmd.PetScheduledAt.IsZero() {
			return application.CommitResult{}, errcode.New(errcode.CommonInvalidArgument, "pet_scheduled_at is required")
		}
		if !nextPetAt.Valid || !sameMillis(nextPetAt.Time, cmd.PetScheduledAt) {
			return application.CommitResult{}, errcode.New(errcode.FarmVersionConflict, "pet schedule is stale")
		}
		if now.Before(nextPetAt.Time) {
			return application.CommitResult{}, errcode.New(errcode.FarmVersionConflict, "pet schedule is not due")
		}
	}

	// 3. Every external farm mutation uses optimistic concurrency, including
	// version zero for a new farm. The scanner-only command is internal and is
	// instead fenced by its deterministic schedule key and the farm row lock.
	if cmd.Type != domain.CmdPetAutoHarvest && cmd.BaseVersion != snap.Version {
		return application.CommitResult{}, errcode.New(errcode.FarmVersionConflict, "base_version stale")
	}

	// 3.5. 好友鉴权：HelpWater/StealCrop 需要访客与农场主是好友。
	if cmd.Type == domain.CmdHelpWater || cmd.Type == domain.CmdStealCrop {
		if cmd.ActorUser == snap.OwnerID {
			return application.CommitResult{}, errcode.New(errcode.SocialNotFarmOwner, "cannot help-water your own farm")
		}
		if err := c.checkFriendship(ctx, tx, cmd.ActorUser, snap.OwnerID); err != nil {
			return application.CommitResult{}, err
		}
	}
	// The scanner filter is only an optimization. Recheck the persisted switch
	// under the same transaction and after the farm row lock so a completed
	// disable request fences every later auto-harvest command.
	if cmd.Type == domain.CmdPetAutoHarvest {
		if err := checkPetAutoHarvestEnabled(ctx, tx, snap.OwnerID); err != nil {
			return application.CommitResult{}, err
		}
	}

	// 4. 应用领域命令，捕获变化结果。
	ar, err := applyCmd(snap, cmd, now)
	if err != nil {
		return application.CommitResult{}, err
	}
	snap.Version++

	// 5. 经济类副作用（在快照行锁保护下写入，防止与同农场命令并发）。
	if err := c.applyEconomicSideEffects(ctx, tx, cmd, ar, snap.OwnerID, now); err != nil {
		return application.CommitResult{}, err
	}

	// 6. 持久化快照。
	if err := upsertSnapshot(ctx, tx, snap, now); err != nil {
		return application.CommitResult{}, err
	}

	// 7. Write a fallback receipt only for commands selected by the policy.
	eventID := id.NewV7()
	if requiresCommandReceipt(cmd.Type) {
		if err := insertReceipt(ctx, tx, cmd.ActorUser, cmd.CmdID, snap.Version, now); err != nil {
			return application.CommitResult{}, err
		}
	}

	// 8. 写入 outbox 事件。宠物收获同样是权威收获事实，任务和图鉴依赖
	// farm.harvested.v1 推进；幂等 receipt 与 outbox 仍在本事务内提交。
	if err := insertOutbox(ctx, tx, eventID, ar.eventType, snap, cmd, ar, now); err != nil {
		return application.CommitResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return application.CommitResult{}, fmt.Errorf("commit_tx farm_id=%d: %w", cmd.FarmID, err)
	}

	return application.CommitResult{
		NewVersion: snap.Version,
		Patch: domain.Patch{
			FarmID:    snap.FarmID,
			Version:   snap.Version,
			Plots:     []domain.Plot{ar.changedPlot},
			ActorUser: cmd.ActorUser,
		},
		EventID: eventID,
	}, nil
}

// ── apply ─────────────────────────────────────────────────────────────────────

// applyResult 是 applyCmd 的输出，携带变更地块和事件类型。
type applyResult struct {
	changedPlot     domain.Plot
	eventType       farmevents.EventType
	harvestedCropID string // HARVEST 时记录清除前的 crop_id
	harvestedYield  int64
	harvestMode     string
	stolenAmount    int64
	remainingYield  int64
}

// applyCmd 在内存中对 snap 应用命令并返回结果；snap.Plots 就地修改。
func applyCmd(snap *domain.Snapshot, cmd domain.Command, now time.Time) (applyResult, error) {
	plot := snap.Plots[cmd.PlotID]
	plot.PlotID = cmd.PlotID

	switch cmd.Type {
	case domain.CmdPlant:
		if plot.Status != "" && plot.Status != domain.PlotEmpty {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "plot not empty")
		}
		// 从作物配置读取成熟时长；未知作物使用兜底时长。
		duration := defaultGrowthDuration
		if cfg, err := getCropConfig(cmd.CropID); err == nil {
			duration = cfg.GrowthDuration
		}
		plot.CropID = cmd.CropID
		plot.Status = domain.PlotGrowing
		plot.PlantedAt = now
		plot.MatureAt = now.Add(duration)
		plot.WateredCount = 0
		plot.RemainingYield = harvestYieldForCrop(cmd.CropID)
		snap.Plots[cmd.PlotID] = plot
		return applyResult{changedPlot: plot, eventType: farmevents.EventTypeFarmPlanted}, nil

	case domain.CmdHarvest:
		if plot.EffectiveStatus(now) != domain.PlotMature {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "plot not mature")
		}
		harvestedCropID := plot.CropID
		harvestedYield := plot.RemainingYield
		plot = domain.Plot{PlotID: cmd.PlotID, Status: domain.PlotEmpty}
		snap.Plots[cmd.PlotID] = plot
		return applyResult{
			changedPlot:     plot,
			eventType:       farmevents.EventTypeFarmHarvested,
			harvestedCropID: harvestedCropID,
			harvestedYield:  harvestedYield,
			harvestMode:     "MANUAL",
		}, nil

	case domain.CmdWater:
		// WATER：幼苗期仅第 1 次、半成熟期仅第 2 次浇水生效；重复浇水静默跳过。
		eff := plot.EffectiveStatus(now)
		if eff == domain.PlotEmpty || eff == "" {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "cannot water empty plot")
		}
		plot = waterByGrowthStage(plot, now)
		snap.Plots[cmd.PlotID] = plot
		return applyResult{changedPlot: plot, eventType: farmevents.EventTypeFarmWatered}, nil

	case domain.CmdHelpWater:
		// 好友代浇水，逻辑等同于 Water（调用方已完成好友鉴权）。
		eff := plot.EffectiveStatus(now)
		if eff == domain.PlotEmpty || eff == "" {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "cannot water empty plot")
		}
		plot = waterByGrowthStage(plot, now)
		snap.Plots[cmd.PlotID] = plot
		return applyResult{changedPlot: plot, eventType: farmevents.EventTypeFarmWatered}, nil

	case domain.CmdStealCrop:
		// Stealing preserves the mature crop and only deducts the actor's share.
		// It must leave at least one unit so a friend can never complete harvest
		// on the owner's behalf or emit a zero-yield steal event.
		if plot.EffectiveStatus(now) != domain.PlotMature {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "plot not mature")
		}
		harvestedCropID := plot.CropID
		remainingYield := plot.RemainingYield
		stealAmount := harvestYieldForCrop(plot.CropID) / 5
		if stealAmount <= 0 || remainingYield <= stealAmount {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "no stealable yield remains")
		}
		plot.RemainingYield = remainingYield - stealAmount
		snap.Plots[cmd.PlotID] = plot
		return applyResult{
			changedPlot:     plot,
			eventType:       farmevents.EventTypeFarmStolen,
			harvestedCropID: harvestedCropID,
			stolenAmount:    stealAmount,
			remainingYield:  plot.RemainingYield,
		}, nil

	case domain.CmdPetAutoHarvest:
		// 扫描器只负责找候选；权威事务重新确认它仍是 PlotID 最小的成熟地块。
		if plot.EffectiveStatus(now) != domain.PlotMature {
			return applyResult{}, errcode.New(errcode.FarmPlotState, "plot not mature for pet harvest")
		}
		if smallest, ok := smallestMaturePlotID(snap.Plots, now); !ok || smallest != cmd.PlotID {
			return applyResult{}, errcode.New(errcode.FarmVersionConflict, "pet harvest candidate is stale")
		}
		harvestedCropID := plot.CropID
		harvestedYield := plot.RemainingYield
		plot = domain.Plot{PlotID: cmd.PlotID, Status: domain.PlotEmpty}
		snap.Plots[cmd.PlotID] = plot
		return applyResult{
			changedPlot:     plot,
			eventType:       farmevents.EventTypeFarmHarvested,
			harvestedCropID: harvestedCropID,
			harvestedYield:  harvestedYield,
			harvestMode:     "PET_AUTO",
		}, nil

	default:
		return applyResult{}, errcode.New(errcode.CommonInvalidArgument, "unknown command type: "+string(cmd.Type))
	}
}

func smallestMaturePlotID(plots map[int32]domain.Plot, now time.Time) (int32, bool) {
	var smallest int32
	found := false
	for plotID, plot := range plots {
		if plot.EffectiveStatus(now) != domain.PlotMature {
			continue
		}
		if !found || plotID < smallest {
			smallest = plotID
			found = true
		}
	}
	return smallest, found
}

// waterByGrowthStage applies at most one watering in each eligible growth
// stage. WateredCount also records stage progress: 0 is the seedling watering,
// and 1 is the semi-mature watering. Missing the seedling watering cannot be
// made up twice in the semi-mature stage.
func waterByGrowthStage(plot domain.Plot, now time.Time) domain.Plot {
	switch plot.GrowthStage(now) {
	case domain.CropGrowthStageSeedling:
		if plot.WateredCount != 0 {
			return plot
		}
	case domain.CropGrowthStageSemiMature:
		if plot.WateredCount != 1 {
			return plot
		}
	default:
		return plot
	}

	plot.MatureAt = plot.MatureAt.Add(-time.Minute)
	plot.WateredCount++
	return plot
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

// checkReceipt 查 cmd_receipts 幂等键；找到返回 (result, true, nil)，未找到返回 (zero, false, nil)。
func checkReceipt(ctx context.Context, tx *sql.Tx, userID int64, cmdID string) (application.CommitResult, bool, error) {
	const q = `SELECT result_version FROM cmd_receipts WHERE user_id = ? AND cmd_id = ? LIMIT 1`
	var ver sql.NullInt64
	err := tx.QueryRowContext(ctx, q, uint64(userID), cmdID).Scan(&ver)
	if err == sql.ErrNoRows {
		return application.CommitResult{}, false, nil
	}
	if err != nil {
		return application.CommitResult{}, false, fmt.Errorf("check_receipt user_id=%d cmd_id=%s: %w", userID, cmdID, err)
	}
	return application.CommitResult{NewVersion: ver.Int64, Replayed: true}, true, nil
}

func requiresCommandReceipt(commandType domain.CommandType) bool {
	switch commandType {
	case domain.CmdPlant, domain.CmdHarvest, domain.CmdStealCrop, domain.CmdPetAutoHarvest:
		return true
	default:
		return false
	}
}

func checkPetAutoHarvestEnabled(ctx context.Context, tx *sql.Tx, userID int64) error {
	var enabled bool
	err := tx.QueryRowContext(ctx, `
		SELECT auto_harvest_enabled
		FROM player_pets
		WHERE user_id = ? AND status = 'ACTIVE'
		FOR UPDATE`, uint64(userID)).Scan(&enabled)
	if err == sql.ErrNoRows {
		return errcode.New(errcode.PetNotOwned, "active pet not owned")
	}
	if err != nil {
		return fmt.Errorf("check_pet_auto_harvest user_id=%d: %w", userID, err)
	}
	if !enabled {
		return errcode.New(errcode.PetAutoDisabled, "pet auto harvest disabled")
	}
	return nil
}

func sameMillis(a, b time.Time) bool {
	return a.UTC().Truncate(time.Millisecond).Equal(b.UTC().Truncate(time.Millisecond))
}

// loadSnapshotForUpdate 加行锁读取快照；行不存在时返回空快照（版本 0）。
// 同时读出 next_pet_action_at，供 CmdPetAutoHarvest 在已锁行上直接校验，
// 避免随后再发起一次无锁 SELECT 同一行。
func loadSnapshotForUpdate(ctx context.Context, tx *sql.Tx, farmID int64) (*domain.Snapshot, sql.NullTime, error) {
	const q = `SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots WHERE farm_id = ? FOR UPDATE`
	var ownerID, version, routeEpoch int64
	var raw []byte
	var nextPetAt sql.NullTime
	err := tx.QueryRowContext(ctx, q, uint64(farmID)).Scan(&ownerID, &version, &raw, &routeEpoch, &nextPetAt)
	if err == sql.ErrNoRows {
		s := emptySnapshot(farmID)
		return &s, sql.NullTime{}, nil
	}
	if err != nil {
		return nil, sql.NullTime{}, fmt.Errorf("load_snapshot_for_update farm_id=%d: %w", farmID, err)
	}
	snap, err := unmarshalSnapshot(farmID, ownerID, version, raw)
	if err != nil {
		return nil, sql.NullTime{}, fmt.Errorf("unmarshal_snapshot farm_id=%d: %w", farmID, err)
	}
	snap.RouteEpoch = routeEpoch
	return &snap, nextPetAt, nil
}

// upsertSnapshot 用 INSERT ... ON DUPLICATE KEY UPDATE 安全写入快照。
func upsertSnapshot(ctx context.Context, tx *sql.Tx, snap *domain.Snapshot, now time.Time) error {
	raw, err := marshalSnapshot(snap)
	if err != nil {
		return fmt.Errorf("marshal_snapshot farm_id=%d: %w", snap.FarmID, err)
	}
	const q = `
		INSERT INTO farm_snapshots (farm_id, owner_user_id, version, snapshot, route_epoch, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE version = VALUES(version), snapshot = VALUES(snapshot), route_epoch = VALUES(route_epoch), updated_at = VALUES(updated_at)
	`
	_, err = tx.ExecContext(ctx, q,
		uint64(snap.FarmID), uint64(snap.OwnerID),
		uint64(snap.Version), raw, uint64(snap.RouteEpoch),
		now, now,
	)
	return err
}

// insertReceipt 写入幂等回执；重复 cmd_id 会触发唯一键冲突（由 Actor 串行保证不发生）。
func insertReceipt(ctx context.Context, tx *sql.Tx, userID int64, cmdID string, version int64, now time.Time) error {
	const q = `INSERT INTO cmd_receipts (user_id, cmd_id, result_code, result_version, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := tx.ExecContext(ctx, q, uint64(userID), cmdID, "OK", uint64(version), now)
	return err
}

// insertOutbox 将领域事件写入 outbox_events，与业务事务同库同事务提交。
// 对于 EventTypeFarmStolen，会查询 accounts.display_name 填入 payload；查询失败时降级为 ID 字符串。
func insertOutbox(ctx context.Context, tx *sql.Tx, eventID string, eventType farmevents.EventType,
	snap *domain.Snapshot, cmd domain.Command, ar applyResult, now time.Time,
) error {
	// StealCrop 需要 display_name，提前查好（事务内单次查询，< 1ms）。
	var displayNames map[int64]string
	if eventType == farmevents.EventTypeFarmStolen {
		displayNames = loadDisplayNames(ctx, tx, []int64{snap.OwnerID, cmd.ActorUser})
	}

	envelope := farmevents.EventEnvelope{
		EventID:       eventID,
		EventType:     eventType,
		AggregateType: "farm",
		AggregateID:   strconv.FormatInt(snap.FarmID, 10),
		SchemaVersion: 1,
		OccurredAt:    now,
		TraceID:       observability.TraceID(ctx),
		CorrelationID: cmd.CmdID,
		CausationID:   cmd.CmdID,
		Payload:       buildPayload(eventType, snap, cmd, ar, now, displayNames),
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal_outbox farm_id=%d: %w", snap.FarmID, err)
	}
	const q = `
		INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, schema_version, payload, status, available_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
	`
	_, err = tx.ExecContext(ctx, q,
		eventID,
		"farm",
		uint64(snap.FarmID),
		strconv.FormatInt(snap.FarmID, 10),
		string(eventType),
		"1.0",
		payload,
		now, now, now,
	)
	return err
}

// buildPayload 按事件类型构造 outbox 事件负载。
// displayNames 仅 EventTypeFarmStolen 时传入（map[userID→displayName]），其余传 nil。
func buildPayload(eventType farmevents.EventType, snap *domain.Snapshot, cmd domain.Command, ar applyResult, now time.Time, displayNames map[int64]string) any {
	farmStr := strconv.FormatInt(snap.FarmID, 10)
	ownerStr := strconv.FormatInt(snap.OwnerID, 10)
	actorStr := strconv.FormatInt(cmd.ActorUser, 10)
	plot := ar.changedPlot

	switch eventType {
	case farmevents.EventTypeFarmPlanted:
		return farmevents.FarmPlantedPayload{
			FarmID:      farmStr,
			OwnerUserID: ownerStr,
			ActorUserID: actorStr,
			FarmVersion: snap.Version,
			PlotID:      plot.PlotID,
			CropID:      plot.CropID,
			PlantedAt:   plot.PlantedAt,
			MatureAt:    plot.MatureAt,
			CmdID:       cmd.CmdID,
		}
	case farmevents.EventTypeFarmHarvested:
		return farmevents.FarmHarvestedPayload{
			FarmID:      farmStr,
			OwnerUserID: ownerStr,
			ActorUserID: actorStr,
			FarmVersion: snap.Version,
			PlotID:      plot.PlotID,
			CropID:      ar.harvestedCropID,
			Yield:       ar.harvestedYield,
			Mode:        ar.harvestMode,
			HarvestedAt: now,
			CmdID:       cmd.CmdID,
		}
	case farmevents.EventTypeFarmWatered:
		return farmevents.FarmWateredPayload{
			FarmID:      farmStr,
			OwnerUserID: ownerStr,
			ActorUserID: actorStr,
			FarmVersion: snap.Version,
			PlotID:      plot.PlotID,
			WateredAt:   now,
			CmdID:       cmd.CmdID,
		}
	case farmevents.EventTypeFarmStolen:
		return farmevents.FarmStolenPayload{
			FarmID:           farmStr,
			OwnerUserID:      ownerStr,
			ActorUserID:      actorStr,
			OwnerDisplayName: displayName(displayNames, snap.OwnerID),
			ActorDisplayName: displayName(displayNames, cmd.ActorUser),
			FarmVersion:      snap.Version,
			PlotID:           plot.PlotID,
			CropID:           ar.harvestedCropID,
			StolenAmount:     ar.stolenAmount,
			RemainingYield:   ar.remainingYield,
			StolenAt:         now,
			CmdID:            cmd.CmdID,
		}
	default:
		return nil
	}
}

// loadDisplayNames 批量查询用户昵称；查询失败时以 ID 字符串降级，不影响事务。
func loadDisplayNames(ctx context.Context, tx *sql.Tx, userIDs []int64) map[int64]string {
	result := make(map[int64]string, len(userIDs))
	for _, uid := range userIDs {
		var name string
		err := tx.QueryRowContext(ctx,
			`SELECT display_name FROM accounts WHERE user_id = ? LIMIT 1`,
			uint64(uid),
		).Scan(&name)
		if err != nil || strings.TrimSpace(name) == "" {
			name = strconv.FormatInt(uid, 10)
		}
		result[uid] = name
	}
	return result
}

// displayName 从 map 取昵称；map 为 nil 或 key 不存在时返回 ID 字符串。
func displayName(names map[int64]string, userID int64) string {
	if names != nil {
		if n, ok := names[userID]; ok {
			return n
		}
	}
	return strconv.FormatInt(userID, 10)
}

// ── 经济类命令事务 ────────────────────────────────────────────────────────────

// commitEconomicTx 处理 PurchaseSeed / SellCrop，不加载农场快照。
// 加锁顺序：wallet → inventory（固定顺序防死锁）。
func (c *MySQLCommitter) commitEconomicTx(ctx context.Context, cmd domain.Command, now time.Time) (result application.CommitResult, retErr error) {
	operation := economyOperation(cmd.Type)
	totalStarted := time.Now()
	defer func() {
		c.observeEconomyStage(operation, "total", economyResult(retErr, result.Replayed), time.Since(totalStarted))
	}()

	// 事务外快速参数校验：非法 quantity 不进入连接池，避免浪费连接和事务开销。
	// 事务内仍有最终校验（在幂等查询之后），两层互为兜底。
	qty := cmd.Quantity
	if qty < 1 {
		return application.CommitResult{}, errcode.New(errcode.CommonInvalidArgument, "quantity 必须 ≥ 1")
	}
	if qty > domain.MaxEconomyQuantity {
		return application.CommitResult{}, errcode.New(errcode.CommonInvalidArgument,
			fmt.Sprintf("quantity 超过单条命令上限 %d", domain.MaxEconomyQuantity))
	}

	poolStarted := time.Now()
	conn, err := c.db.Conn(ctx)
	c.observeDBPoolAcquire(operation, metricResult(err), time.Since(poolStarted))
	if err != nil {
		c.observeEconomyError(operation, "db_pool_wait", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return application.CommitResult{}, errcode.NewReason(errcode.ResourceExhausted, "database pool wait exceeded request deadline", errcode.CapacityReasonDBPoolWait)
		}
		return application.CommitResult{}, fmt.Errorf("acquire_db_conn economic: %w", err)
	}
	defer conn.Close()
	beginStarted := time.Now()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	c.observeEconomyStage(operation, "begin", metricResult(err), time.Since(beginStarted))
	if err != nil {
		c.observeEconomyError(operation, "begin", err)
		return application.CommitResult{}, fmt.Errorf("begin_tx economic: %w", err)
	}
	c.observeEconomyTransactionDelta(operation, 1)
	defer c.observeEconomyTransactionDelta(operation, -1)
	defer tx.Rollback() //nolint:errcheck

	bizType := economyBizType(cmd.Type)
	stageStarted := time.Now()
	res, replayed, err := checkEconomyTransaction(ctx, tx, cmd.ActorUser, bizType, cmd.CmdID)
	idempotencyResult := metricResult(err)
	if replayed {
		idempotencyResult = "replayed"
	}
	c.observeEconomyStage(operation, "idempotency_query", idempotencyResult, time.Since(stageStarted))
	if err != nil {
		c.observeEconomyError(operation, "idempotency_query", err)
		return application.CommitResult{}, err
	} else if replayed {
		return res, nil
	}

	cfg, err := getCropConfig(cmd.CropID)
	if err != nil {
		return application.CommitResult{}, err
	}
	// qty 已在事务外做过快速校验；事务内再次确认，两层互为兜底。
	if qty < 1 {
		return application.CommitResult{}, errcode.New(errcode.CommonInvalidArgument, "quantity 必须 ≥ 1")
	}
	if qty > domain.MaxEconomyQuantity {
		return application.CommitResult{}, errcode.New(errcode.CommonInvalidArgument,
			fmt.Sprintf("quantity 超过单条命令上限 %d", domain.MaxEconomyQuantity))
	}
	itemID := cropIDToItemID(cmd.CropID)

	var resultBalance int64 // 事务完成后的最新金币余额

	switch cmd.Type {
	case domain.CmdPurchaseSeed:
		price := cfg.SeedPrice * qty
		// 加载并锁定钱包行。
		stageStarted = time.Now()
		balance, err := loadWalletForUpdate(ctx, tx, cmd.ActorUser)
		c.observeEconomyStage(operation, "wallet_lock", metricResult(err), time.Since(stageStarted))
		if err != nil {
			c.observeEconomyError(operation, "wallet_lock", err)
			return application.CommitResult{}, err
		}
		if balance < price {
			return application.CommitResult{}, errcode.New(errcode.EconomyInsufficient,
				fmt.Sprintf("金币不足：需要 %d，剩余 %d", price, balance))
		}
		newBalance := balance - price
		resultBalance = newBalance
		stageStarted = time.Now()
		if err := setWalletBalance(ctx, tx, cmd.ActorUser, newBalance, now); err != nil {
			c.observeEconomyStage(operation, "wallet_update", "error", time.Since(stageStarted))
			c.observeEconomyError(operation, "wallet_update", err)
			return application.CommitResult{}, err
		}
		c.observeEconomyStage(operation, "wallet_update", "ok", time.Since(stageStarted))
		stageStarted = time.Now()
		if err := addInventoryItem(ctx, tx, cmd.ActorUser, "SEED", itemID, qty, now); err != nil {
			c.observeEconomyStage(operation, "inventory_write", "error", time.Since(stageStarted))
			c.observeEconomyError(operation, "inventory_write", err)
			return application.CommitResult{}, err
		}
		c.observeEconomyStage(operation, "inventory_write", "ok", time.Since(stageStarted))
		stageStarted = time.Now()
		if err := insertEconomyTransaction(ctx, tx, cmd.ActorUser, bizType, cmd.CmdID,
			"COIN", balance, -price, newBalance, "PURCHASE", now); err != nil {
			c.observeEconomyStage(operation, "ledger_insert", "error", time.Since(stageStarted))
			c.observeEconomyError(operation, "ledger_insert", err)
			return application.CommitResult{}, err
		}
		c.observeEconomyStage(operation, "ledger_insert", "ok", time.Since(stageStarted))

	case domain.CmdSellCrop:
		gain := cfg.SellPrice * qty
		// Global order: wallet before inventory. The transaction rolls back the
		// wallet lock without mutation when inventory is insufficient.
		stageStarted = time.Now()
		balance, err := loadWalletForUpdate(ctx, tx, cmd.ActorUser)
		c.observeEconomyStage(operation, "wallet_lock", metricResult(err), time.Since(stageStarted))
		if err != nil {
			c.observeEconomyError(operation, "wallet_lock", err)
			return application.CommitResult{}, err
		}
		stageStarted = time.Now()
		ok, err := deductInventoryItem(ctx, tx, cmd.ActorUser, "CROP", itemID, qty, now)
		c.observeEconomyStage(operation, "inventory_write", metricResult(err), time.Since(stageStarted))
		if err != nil {
			c.observeEconomyError(operation, "inventory_write", err)
			return application.CommitResult{}, err
		}
		if !ok {
			return application.CommitResult{}, errcode.New(errcode.EconomyInsufficient,
				fmt.Sprintf("作物库存不足：需要 %d", qty))
		}
		newBalance := balance + gain
		resultBalance = newBalance
		stageStarted = time.Now()
		if err := setWalletBalance(ctx, tx, cmd.ActorUser, newBalance, now); err != nil {
			c.observeEconomyStage(operation, "wallet_update", "error", time.Since(stageStarted))
			c.observeEconomyError(operation, "wallet_update", err)
			return application.CommitResult{}, err
		}
		c.observeEconomyStage(operation, "wallet_update", "ok", time.Since(stageStarted))
		stageStarted = time.Now()
		if err := insertEconomyTransaction(ctx, tx, cmd.ActorUser, bizType, cmd.CmdID,
			"COIN", balance, gain, newBalance, "SELL", now); err != nil {
			c.observeEconomyStage(operation, "ledger_insert", "error", time.Since(stageStarted))
			c.observeEconomyError(operation, "ledger_insert", err)
			return application.CommitResult{}, err
		}
		c.observeEconomyStage(operation, "ledger_insert", "ok", time.Since(stageStarted))
	}

	eventID := id.NewV7()
	stageStarted = time.Now()
	if err := tx.Commit(); err != nil {
		c.observeEconomyStage(operation, "commit", "error", time.Since(stageStarted))
		c.observeEconomyError(operation, "commit", err)
		return application.CommitResult{}, fmt.Errorf("commit_tx economic: %w", err)
	}
	c.observeEconomyStage(operation, "commit", "ok", time.Since(stageStarted))
	return application.CommitResult{EventID: eventID, CoinBalance: resultBalance}, nil
}

func economyOperation(commandType domain.CommandType) string {
	if commandType == domain.CmdPurchaseSeed {
		return "purchase"
	}
	return "sell"
}

func metricResult(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func economyResult(err error, replayed bool) string {
	if replayed {
		return "replayed"
	}
	if err == nil {
		return "ok"
	}
	var ec *errcode.Error
	if errors.As(err, &ec) {
		switch ec.Code {
		case errcode.EconomyInsufficient, errcode.EconomyItemNotFound, errcode.FarmCropNotFound, errcode.CommonInvalidArgument:
			return "business_error"
		}
	}
	return "error"
}

func internalErrorCode(err error) string {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062, 1205, 1213:
			return "mysql_" + strconv.Itoa(int(mysqlErr.Number))
		default:
			return "mysql_other"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	var ec *errcode.Error
	if errors.As(err, &ec) {
		if ec.Code == errcode.Internal {
			return "application_internal"
		}
		return "business_error"
	}
	return "other"
}

func (c *MySQLCommitter) observeEconomyStage(operation, stage, result string, d time.Duration) {
	if c.observer != nil {
		c.observer.EconomyStage(operation, stage, result, d)
	}
}

func (c *MySQLCommitter) observeEconomyTransactionDelta(operation string, delta int) {
	if c.observer != nil {
		c.observer.EconomyTransactionDelta(operation, delta)
	}
}

func (c *MySQLCommitter) observeDBPoolAcquire(operation, result string, d time.Duration) {
	if c.observer != nil {
		c.observer.DBPoolAcquire(operation, result, d)
	}
}

func (c *MySQLCommitter) observeEconomyError(operation, stage string, err error) {
	if c.observer == nil || err == nil {
		return
	}
	code := internalErrorCode(err)
	if code == "business_error" {
		return
	}
	c.observer.InternalError("mysql", operation, stage, code)
}

func economyBizType(commandType domain.CommandType) string {
	if commandType == domain.CmdPurchaseSeed {
		return "SHOP_PURCHASE"
	}
	return "FARM_SELL"
}

func checkEconomyTransaction(ctx context.Context, tx *sql.Tx, userID int64, bizType, bizID string) (application.CommitResult, bool, error) {
	var balanceAfter int64
	err := tx.QueryRowContext(ctx, `
		SELECT balance_after
		FROM economy_transactions
		WHERE user_id = ? AND biz_type = ? AND biz_id = UNHEX(?) AND status = 'COMMITTED'
		LIMIT 1`, uint64(userID), bizType, bizID).Scan(&balanceAfter)
	if err == sql.ErrNoRows {
		return application.CommitResult{}, false, nil
	}
	if err != nil {
		return application.CommitResult{}, false, fmt.Errorf("check_economy_transaction user_id=%d biz_type=%s: %w", userID, bizType, err)
	}
	return application.CommitResult{CoinBalance: balanceAfter, Replayed: true}, true, nil
}

// applyEconomicSideEffects 在农场快照行锁保护下写入库存变化。
// crop_id 未知时静默跳过（兼容存量测试数据）。
func (c *MySQLCommitter) applyEconomicSideEffects(
	ctx context.Context, tx *sql.Tx,
	cmd domain.Command, ar applyResult, ownerID int64, now time.Time,
) error {
	switch cmd.Type {
	case domain.CmdPlant:
		// 播种消耗 1 颗种子；作物未知时跳过（兼容存量数据）。
		if _, err := getCropConfig(cmd.CropID); err != nil {
			return nil
		}
		ok, err := deductInventoryItem(ctx, tx, cmd.ActorUser, "SEED", cropIDToItemID(cmd.CropID), 1, now)
		if err != nil {
			return err
		}
		if !ok {
			return errcode.New(errcode.EconomyInsufficient, "种子库存不足")
		}
	case domain.CmdHarvest:
		// Owner receives only the yield left after successful steals.
		if ar.harvestedYield > 0 {
			if err := addInventoryItem(ctx, tx, ownerID, "CROP",
				cropIDToItemID(ar.harvestedCropID), ar.harvestedYield, now); err != nil {
				return err
			}
		}
	case domain.CmdPetAutoHarvest:
		// Pet auto-harvest follows the same remaining-yield rule as manual harvest.
		if ar.harvestedYield > 0 {
			if err := addInventoryItem(ctx, tx, ownerID, "CROP",
				cropIDToItemID(ar.harvestedCropID), ar.harvestedYield, now); err != nil {
				return err
			}
		}
		// 只推进本命令校验过的调度槽；即使未来锁顺序调整，CAS 仍防止过期命令覆盖新计划。
		result, err := tx.ExecContext(ctx,
			`UPDATE farm_snapshots SET next_pet_action_at = ?, pet_scan_lease_owner = NULL, pet_scan_lease_until = NULL WHERE farm_id = ? AND next_pet_action_at = ?`,
			now.Add(30*time.Second), uint64(cmd.FarmID), cmd.PetScheduledAt,
		)
		if err != nil {
			return fmt.Errorf("update_next_pet_action: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("update_next_pet_action rows: %w", err)
		}
		if affected != 1 {
			return errcode.New(errcode.FarmVersionConflict, "pet schedule changed during harvest")
		}
	case domain.CmdStealCrop:
		// Only the thief is credited now; the owner receives remaining_yield on
		// a later manual or pet harvest. An actor on another physical shard is
		// credited by CrossShardStealProjector from this transaction's Outbox.
		if c.deferRemoteStealCredit != nil && c.deferRemoteStealCredit(ownerID, cmd.ActorUser) {
			return nil
		}
		if err := addInventoryItem(ctx, tx, cmd.ActorUser, "CROP",
			cropIDToItemID(ar.harvestedCropID), ar.stolenAmount, now); err != nil {
			return err
		}
	}
	return nil
}

// ── 经济 SQL helpers ──────────────────────────────────────────────────────────

// loadWalletForUpdate 加行锁读取金币余额。
func loadWalletForUpdate(ctx context.Context, tx *sql.Tx, userID int64) (int64, error) {
	var balance int64
	err := tx.QueryRowContext(ctx,
		`SELECT coin_balance FROM wallets WHERE user_id = ? FOR UPDATE`,
		uint64(userID),
	).Scan(&balance)
	if err == sql.ErrNoRows {
		return 0, errcode.New(errcode.Internal, fmt.Sprintf("wallet not found user_id=%d", userID))
	}
	return balance, err
}

// setWalletBalance 直接设置金币余额（调用方已完成余额校验）。
func setWalletBalance(ctx context.Context, tx *sql.Tx, userID, newBalance int64, now time.Time) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE wallets SET coin_balance = ?, updated_at = ? WHERE user_id = ?`,
		newBalance, now, uint64(userID),
	)
	return err
}

// addInventoryItem 幂等增加库存（INSERT ON DUPLICATE KEY UPDATE）。
func addInventoryItem(ctx context.Context, tx *sql.Tx, userID int64, itemType string, itemID, delta int64, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inventory_items (user_id, item_type, item_id, quantity, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity), updated_at = VALUES(updated_at)`,
		uint64(userID), itemType, uint64(itemID), delta, now, now,
	)
	return err
}

// deductInventoryItem 扣减库存；库存不足时返回 (false, nil)。
func deductInventoryItem(ctx context.Context, tx *sql.Tx, userID int64, itemType string, itemID, delta int64, now time.Time) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE inventory_items
		SET quantity = quantity - ?, updated_at = ?
		WHERE user_id = ? AND item_type = ? AND item_id = ? AND quantity >= ?`,
		delta, now, uint64(userID), itemType, uint64(itemID), delta,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// insertEconomyTransaction 写入一条经济流水记录（幂等键 user_id+biz_type+biz_id）。
func insertEconomyTransaction(ctx context.Context, tx *sql.Tx,
	userID int64, bizType, bizID, currencyType string,
	balanceBefore, delta, balanceAfter int64,
	sourceType string, now time.Time,
) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO economy_transactions
		(user_id, biz_type, biz_id, currency_type, balance_before, balance_delta, balance_after,
		 source_type, status, created_at)
		VALUES (?, ?, UNHEX(?), ?, ?, ?, ?, ?, 'COMMITTED', ?)`,
		uint64(userID), bizType, bizID, currencyType,
		balanceBefore, delta, balanceAfter,
		sourceType, now,
	)
	return err
}

// ── snapshot JSON ─────────────────────────────────────────────────────────────

// snapshotJSON 是 farm_snapshots.snapshot 列的稳定 JSON 格式。
// farm_id、owner_user_id、version 存于独立列，不重复写入 JSON。
type snapshotJSON struct {
	Plots map[string]plotJSON `json:"plots"`
}

type plotJSON struct {
	CropID         string    `json:"crop_id,omitempty"`
	Status         string    `json:"status"`
	PlantedAt      time.Time `json:"planted_at,omitempty"`
	MatureAt       time.Time `json:"mature_at,omitempty"`
	WateredCount   int       `json:"watered_count,omitempty"`
	RemainingYield *int64    `json:"remaining_yield,omitempty"`
}

func marshalSnapshot(snap *domain.Snapshot) ([]byte, error) {
	sj := snapshotJSON{Plots: make(map[string]plotJSON, len(snap.Plots))}
	for plotID, p := range snap.Plots {
		remainingYield := p.RemainingYield
		sj.Plots[strconv.Itoa(int(plotID))] = plotJSON{
			CropID:         p.CropID,
			Status:         string(p.Status),
			PlantedAt:      p.PlantedAt,
			MatureAt:       p.MatureAt,
			WateredCount:   p.WateredCount,
			RemainingYield: &remainingYield,
		}
	}
	return json.Marshal(sj)
}

func unmarshalSnapshot(farmID, ownerID, version int64, raw []byte) (domain.Snapshot, error) {
	var sj snapshotJSON
	if err := json.Unmarshal(raw, &sj); err != nil {
		return domain.Snapshot{}, fmt.Errorf("json_unmarshal: %w", err)
	}
	plots := make(map[int32]domain.Plot, len(sj.Plots))
	for k, p := range sj.Plots {
		pid, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return domain.Snapshot{}, fmt.Errorf("invalid plot_id key %q: %w", k, err)
		}
		remainingYield := int64(0)
		if p.RemainingYield != nil {
			remainingYield = *p.RemainingYield
		} else if domain.PlotStatus(p.Status) != domain.PlotEmpty && p.Status != "" {
			// Backward compatibility for snapshots written before remaining_yield.
			remainingYield = harvestYieldForCrop(p.CropID)
		}
		plots[int32(pid)] = domain.Plot{
			PlotID:         int32(pid),
			CropID:         p.CropID,
			Status:         domain.PlotStatus(p.Status),
			PlantedAt:      p.PlantedAt,
			MatureAt:       p.MatureAt,
			WateredCount:   p.WateredCount,
			RemainingYield: remainingYield,
		}
	}
	return domain.Snapshot{FarmID: farmID, OwnerID: ownerID, Version: version, Plots: plots}, nil
}

func emptySnapshot(farmID int64) domain.Snapshot {
	return domain.Snapshot{FarmID: farmID, OwnerID: farmID, Version: 0, Plots: make(map[int32]domain.Plot)}
}

// checkFriendship 在事务内校验两用户是否为好友；不是好友时返回 SocialNotFriend 错误。
func (c *MySQLCommitter) checkFriendship(ctx context.Context, tx *sql.Tx, userIDA, userIDB int64) error {
	if c.useFriendshipEdges {
		var dummy int
		err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM friendship_edges WHERE user_id = ? AND friend_user_id = ? AND state = 'ACTIVE' LIMIT 1`,
			uint64(userIDB), uint64(userIDA),
		).Scan(&dummy)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// Existing users may still have their same-shard relationship only in
		// the normalized legacy table. Keep that data authoritative during the
		// transition so enabling sharding does not revoke an established friend.
	}
	a, b := userIDA, userIDB
	if a > b {
		a, b = b, a
	}
	var dummy int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM friendships WHERE user_id_a = ? AND user_id_b = ? LIMIT 1`,
		uint64(a), uint64(b),
	).Scan(&dummy)
	if err == sql.ErrNoRows {
		return errcode.New(errcode.SocialNotFriend, "not friends")
	}
	return err
}
