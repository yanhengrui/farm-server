package infrastructure

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// ── applyCmd unit tests ───────────────────────────────────────────────────────

func TestApplyCmd_Plant_Success(t *testing.T) {
	snap := emptySnapshot(1)
	cmd := domain.Command{CmdID: "c1", FarmID: 1, ActorUser: 10, Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	ar, err := applyCmd(&snap, cmd, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ar.changedPlot.Status != domain.PlotGrowing {
		t.Errorf("expected GROWING, got %q", ar.changedPlot.Status)
	}
	if ar.changedPlot.CropID != "WHEAT" {
		t.Errorf("expected WHEAT, got %q", ar.changedPlot.CropID)
	}
	if ar.changedPlot.RemainingYield != 5 {
		t.Errorf("expected remaining_yield=5, got %d", ar.changedPlot.RemainingYield)
	}
	if snap.Plots[0].Status != domain.PlotGrowing {
		t.Error("snapshot not updated in place")
	}
}

func TestApplyCmd_Plant_NonEmptyPlot(t *testing.T) {
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{PlotID: 0, Status: domain.PlotGrowing, CropID: "WHEAT"}
	cmd := domain.Command{Type: domain.CmdPlant, PlotID: 0, CropID: "CORN"}

	_, err := applyCmd(&snap, cmd, time.Now())
	assertErrCode(t, err, errcode.FarmPlotState)
}

func TestApplyCmd_Harvest_Success(t *testing.T) {
	// Plant first, then advance past mature_at.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{
		PlotID:         0,
		CropID:         "WHEAT",
		Status:         domain.PlotGrowing,
		PlantedAt:      now.Add(-20 * time.Minute),
		MatureAt:       now.Add(-1 * time.Minute), // already mature
		RemainingYield: 4,
	}

	cmd := domain.Command{CmdID: "c2", FarmID: 1, ActorUser: 10, Type: domain.CmdHarvest, PlotID: 0}
	ar, err := applyCmd(&snap, cmd, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ar.changedPlot.Status != domain.PlotEmpty {
		t.Errorf("expected EMPTY after harvest, got %q", ar.changedPlot.Status)
	}
	if ar.harvestedCropID != "WHEAT" {
		t.Errorf("expected harvestedCropID=WHEAT, got %q", ar.harvestedCropID)
	}
	if ar.harvestedYield != 4 {
		t.Errorf("expected harvestedYield=4, got %d", ar.harvestedYield)
	}
}

func TestApplyCmd_Harvest_NotMature(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{
		PlotID:    0,
		CropID:    "WHEAT",
		Status:    domain.PlotGrowing,
		PlantedAt: now.Add(-1 * time.Minute),
		MatureAt:  now.Add(9 * time.Minute), // not mature yet
	}
	cmd := domain.Command{Type: domain.CmdHarvest, PlotID: 0}
	_, err := applyCmd(&snap, cmd, now)
	assertErrCode(t, err, errcode.FarmPlotState)
}

func TestApplyCmd_Water_GrowthStageLimits(t *testing.T) {
	for _, commandType := range []domain.CommandType{domain.CmdWater, domain.CmdHelpWater} {
		t.Run(string(commandType), func(t *testing.T) {
			plantedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			snap := emptySnapshot(1)
			snap.Plots[0] = domain.Plot{
				PlotID:    0,
				Status:    domain.PlotGrowing,
				PlantedAt: plantedAt,
				MatureAt:  plantedAt.Add(10 * time.Minute),
			}
			cmd := domain.Command{Type: commandType, PlotID: 0}

			first, err := applyCmd(&snap, cmd, plantedAt)
			if err != nil {
				t.Fatalf("first seedling watering: %v", err)
			}
			if first.changedPlot.WateredCount != 1 || !first.changedPlot.MatureAt.Equal(plantedAt.Add(9*time.Minute)) {
				t.Fatalf("first seedling watering changed plot=%+v", first.changedPlot)
			}

			secondSeedling, err := applyCmd(&snap, cmd, plantedAt)
			if err != nil {
				t.Fatalf("repeated seedling watering: %v", err)
			}
			if secondSeedling.changedPlot.WateredCount != 1 || !secondSeedling.changedPlot.MatureAt.Equal(plantedAt.Add(9*time.Minute)) {
				t.Fatalf("repeated seedling watering must be skipped, plot=%+v", secondSeedling.changedPlot)
			}

			semiMatureAt := plantedAt.Add(5 * time.Minute)
			second, err := applyCmd(&snap, cmd, semiMatureAt)
			if err != nil {
				t.Fatalf("semi-mature watering: %v", err)
			}
			if second.changedPlot.WateredCount != 2 || !second.changedPlot.MatureAt.Equal(plantedAt.Add(8*time.Minute)) {
				t.Fatalf("semi-mature watering changed plot=%+v", second.changedPlot)
			}

			repeatedSemiMature, err := applyCmd(&snap, cmd, semiMatureAt)
			if err != nil {
				t.Fatalf("repeated semi-mature watering: %v", err)
			}
			if repeatedSemiMature.changedPlot.WateredCount != 2 || !repeatedSemiMature.changedPlot.MatureAt.Equal(plantedAt.Add(8*time.Minute)) {
				t.Fatalf("repeated semi-mature watering must be skipped, plot=%+v", repeatedSemiMature.changedPlot)
			}

			mature, err := applyCmd(&snap, cmd, plantedAt.Add(8*time.Minute))
			if err != nil {
				t.Fatalf("mature watering: %v", err)
			}
			if mature.changedPlot.WateredCount != 2 || !mature.changedPlot.MatureAt.Equal(plantedAt.Add(8*time.Minute)) {
				t.Fatalf("mature watering must be skipped, plot=%+v", mature.changedPlot)
			}
		})
	}
}

func TestApplyCmd_Water_CannotMakeUpSeedlingWatering(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{
		PlotID:    0,
		Status:    domain.PlotGrowing,
		PlantedAt: now.Add(-6 * time.Minute),
		MatureAt:  now.Add(4 * time.Minute),
	}

	ar, err := applyCmd(&snap, domain.Command{Type: domain.CmdWater, PlotID: 0}, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ar.changedPlot.WateredCount != 0 || !ar.changedPlot.MatureAt.Equal(now.Add(4*time.Minute)) {
		t.Fatalf("semi-mature stage must not accept the missing first watering, plot=%+v", ar.changedPlot)
	}
}

func TestApplyCmd_Water_EmptyPlot(t *testing.T) {
	snap := emptySnapshot(1)
	cmd := domain.Command{Type: domain.CmdWater, PlotID: 0}
	_, err := applyCmd(&snap, cmd, time.Now())
	assertErrCode(t, err, errcode.FarmPlotState)
}

func TestApplyCmd_UnknownType(t *testing.T) {
	snap := emptySnapshot(1)
	cmd := domain.Command{Type: "UNKNOWN", PlotID: 0}
	_, err := applyCmd(&snap, cmd, time.Now())
	assertErrCode(t, err, errcode.CommonInvalidArgument)
}

// ── snapshot JSON round-trip ──────────────────────────────────────────────────

func TestSnapshotJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	orig := domain.Snapshot{
		FarmID:  42,
		OwnerID: 99,
		Version: 7,
		Plots: map[int32]domain.Plot{
			0: {PlotID: 0, Status: domain.PlotEmpty},
			1: {PlotID: 1, CropID: "WHEAT", Status: domain.PlotGrowing, PlantedAt: now, MatureAt: now.Add(10 * time.Minute), RemainingYield: 4},
		},
	}

	raw, err := marshalSnapshot(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := unmarshalSnapshot(orig.FarmID, orig.OwnerID, orig.Version, raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.FarmID != orig.FarmID || got.Version != orig.Version {
		t.Errorf("header mismatch: got %+v", got)
	}
	if len(got.Plots) != 2 {
		t.Errorf("expected 2 plots, got %d", len(got.Plots))
	}
	p1 := got.Plots[1]
	if p1.CropID != "WHEAT" || p1.Status != domain.PlotGrowing || p1.RemainingYield != 4 {
		t.Errorf("plot 1 mismatch: %+v", p1)
	}
}

func TestSnapshotJSON_LegacyPlotRestoresConfiguredYield(t *testing.T) {
	raw := []byte(`{"plots":{"1":{"crop_id":"WHEAT","status":"GROWING","planted_at":"2026-01-01T00:00:00Z","mature_at":"2026-01-01T00:10:00Z"},"2":{"status":"EMPTY"}}}`)
	snap, err := unmarshalSnapshot(1, 1, 3, raw)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Plots[1].RemainingYield != 5 || snap.Plots[2].RemainingYield != 0 {
		t.Fatalf("legacy migration mismatch: growing=%d empty=%d", snap.Plots[1].RemainingYield, snap.Plots[2].RemainingYield)
	}
}

func TestSnapshotJSON_ExplicitZeroIsNotTreatedAsLegacy(t *testing.T) {
	raw := []byte(`{"plots":{"1":{"crop_id":"WHEAT","status":"GROWING","remaining_yield":0}}}`)
	snap, err := unmarshalSnapshot(1, 1, 3, raw)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Plots[1].RemainingYield != 0 {
		t.Fatalf("remaining_yield=%d", snap.Plots[1].RemainingYield)
	}
}

func TestSnapshotJSON_EmptyPlots(t *testing.T) {
	snap := emptySnapshot(1)
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// should produce valid JSON
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
}

// ── MySQLCommitter mock DB tests ──────────────────────────────────────────────

func newTestCommitter(t *testing.T) (*MySQLCommitter, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
	return NewMySQLCommitter(db, clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))), mock
}

func TestCheckFriendshipEdgesActive(t *testing.T) {
	c, mock := newTestCommitter(t)
	c.WithFriendshipEdges()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM friendship_edges`).
		WithArgs(uint64(22), uint64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectRollback()

	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck
	if err := c.checkFriendship(context.Background(), tx, 11, 22); err != nil {
		t.Fatalf("checkFriendship: %v", err)
	}
}

func TestCheckFriendshipEdgesFallsBackToLegacyFriendship(t *testing.T) {
	c, mock := newTestCommitter(t)
	c.WithFriendshipEdges()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM friendship_edges`).
		WithArgs(uint64(22), uint64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectQuery(`SELECT 1 FROM friendships`).
		WithArgs(uint64(11), uint64(22)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectRollback()

	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck
	if err := c.checkFriendship(context.Background(), tx, 11, 22); err != nil {
		t.Fatalf("checkFriendship: %v", err)
	}
}

func TestCheckFriendshipEdgesRejectsWhenBothStoresMiss(t *testing.T) {
	c, mock := newTestCommitter(t)
	c.WithFriendshipEdges()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM friendship_edges`).
		WithArgs(uint64(22), uint64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectQuery(`SELECT 1 FROM friendships`).
		WithArgs(uint64(11), uint64(22)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectRollback()

	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck
	err = c.checkFriendship(context.Background(), tx, 11, 22)
	assertErrCode(t, err, errcode.SocialNotFriend)
}

type economyObserverCapture struct {
	stages   map[string]int
	active   int
	pool     int
	internal map[string]int
}

func newEconomyObserverCapture() *economyObserverCapture {
	return &economyObserverCapture{stages: make(map[string]int), internal: make(map[string]int)}
}

func (o *economyObserverCapture) EconomyStage(operation, stage, result string, _ time.Duration) {
	o.stages[operation+"/"+stage+"/"+result]++
}
func (o *economyObserverCapture) EconomyTransactionDelta(_ string, delta int) { o.active += delta }
func (o *economyObserverCapture) DBPoolAcquire(_, _ string, _ time.Duration)  { o.pool++ }
func (o *economyObserverCapture) InternalError(source, operation, stage, code string) {
	o.internal[source+"/"+operation+"/"+stage+"/"+code]++
}

func newGrowingSnapshotJSON(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	snap := domain.Snapshot{
		FarmID:  1,
		OwnerID: 1,
		Version: 3,
		Plots: map[int32]domain.Plot{
			0: {
				PlotID:         0,
				CropID:         "WHEAT",
				Status:         domain.PlotGrowing,
				PlantedAt:      now.Add(-20 * time.Minute),
				MatureAt:       now.Add(-1 * time.Minute), // already mature at test time
				RemainingYield: 5,
			},
		},
	}
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatalf("prepare snapshot JSON: %v", err)
	}
	return raw
}

func TestLoadSnapshot_NotFound(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch"}))

	snap, err := c.LoadSnapshot(context.Background(), 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.FarmID != 99 || snap.Version != 0 || len(snap.Plots) != 0 {
		t.Errorf("expected empty snapshot, got %+v", snap)
	}
}

func TestLoadSnapshot_Found(t *testing.T) {
	c, mock := newTestCommitter(t)

	raw, _ := marshalSnapshot(&domain.Snapshot{FarmID: 1, OwnerID: 1, Version: 5, Plots: map[int32]domain.Plot{
		0: {PlotID: 0, Status: domain.PlotEmpty},
	}})
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch"}).AddRow(int64(1), int64(5), raw, int64(0)))

	snap, err := c.LoadSnapshot(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.Version != 5 {
		t.Errorf("expected version 5, got %d", snap.Version)
	}
}

func TestCommitFarmCommand_Plant_Success(t *testing.T) {
	c, mock := newTestCommitter(t)

	emptyRows := sqlmock.NewRows([]string{"result_version"})
	emptySnapRows := sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"})

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(emptyRows)
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(emptySnapRows)
	// 播种消耗 1 颗种子（WHEAT crop_id 已在 cropConfigs 中）。
	mock.ExpectExec(`UPDATE inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO cmd_receipts`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "cmd-1", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT", BaseVersion: 0,
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.NewVersion != 1 {
		t.Errorf("expected new version 1, got %d", res.NewVersion)
	}
	if res.Replayed {
		t.Error("should not be replayed")
	}
	if res.EventID == "" {
		t.Error("EventID should be non-empty")
	}
}

func TestCommitFarmCommand_Plant_NoSeed(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}))
	// 库存不足：UPDATE 影响 0 行。
	mock.ExpectExec(`UPDATE inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "cmd-noseed", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}}
	_, err := c.CommitFarmCommand(context.Background(), req)
	assertErrCode(t, err, errcode.EconomyInsufficient)
}

func TestCommitFarmCommand_Harvest_Success(t *testing.T) {
	c, mock := newTestCommitter(t)

	raw := newGrowingSnapshotJSON(t) // version=3, plot 0 = mature WHEAT

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
			AddRow(int64(1), int64(3), raw, int64(0), nil))
	// 收获将作物加入库存。
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO cmd_receipts`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "harvest-1", FarmID: 1, ActorUser: 10,
		Type: domain.CmdHarvest, PlotID: 0, BaseVersion: 3,
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.NewVersion != 4 {
		t.Errorf("expected version 4 after harvest, got %d", res.NewVersion)
	}
	if len(res.Patch.Plots) != 1 || res.Patch.Plots[0].Status != domain.PlotEmpty {
		t.Errorf("patch should show EMPTY plot, got %+v", res.Patch.Plots)
	}
}

func TestCommitFarmCommand_PetAutoHarvest_ValidatesScheduleAndWritesHarvestEvent(t *testing.T) {
	c, mock := newTestCommitter(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(-time.Second)
	snap := domain.Snapshot{
		FarmID: 1, OwnerID: 1, Version: 3,
		Plots: map[int32]domain.Plot{
			0: {PlotID: 0, CropID: "WHEAT", Status: domain.PlotGrowing, PlantedAt: now.Add(-20 * time.Minute), MatureAt: now.Add(-time.Minute), RemainingYield: 4},
		},
	}
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	// next_pet_action_at 现在随快照行一并返回，不再有第二次 SELECT。
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).AddRow(int64(1), int64(3), raw, int64(0), scheduledAt))
	mock.ExpectQuery(`SELECT auto_harvest_enabled[\s\S]+FROM player_pets`).
		WithArgs(uint64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"auto_harvest_enabled"}).AddRow(true))
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WithArgs(uint64(1), "CROP", sqlmock.AnyArg(), int64(4), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE farm_snapshots SET next_pet_action_at`).
		WithArgs(sqlmock.AnyArg(), uint64(1), scheduledAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO cmd_receipts`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Pet auto-harvest must emit farm.harvested.v1 for task/catalog projectors.
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), string(farmevents.EventTypeFarmHarvested), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	res, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{
		CmdID: "pet-harvest", FarmID: 1, ActorUser: 1, Type: domain.CmdPetAutoHarvest, PlotID: 0, PetScheduledAt: scheduledAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.NewVersion != 4 || res.EventID == "" || len(res.Patch.Plots) != 1 || res.Patch.Plots[0].Status != domain.PlotEmpty {
		t.Fatalf("result=%+v", res)
	}
}

// checkPetAutoHarvestSchedule は CommitFarmCommand の事務フローへ統合されたため、
// 以下のテストでその代わりとなる統合レベルの検証を行う。
// next_pet_action_at は loadSnapshotForUpdate と同一の FOR UPDATE クエリで返されるため、
// 独立した SELECT を発行しない（スタールな日程と未来の日程の両方を拒否することを確認）。
func TestCommitFarmCommand_PetAutoHarvest_RejectsStaleAndFutureSchedule(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := domain.Snapshot{FarmID: 1, OwnerID: 1, Version: 3, Plots: map[int32]domain.Plot{}}
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		persisted time.Time
		scheduled time.Time
	}{
		// スケジュールが古い：コマンドの PetScheduledAt が保存値と一致しない
		{"stale", now.Add(-time.Second), now.Add(-2 * time.Second)},
		// スケジュールが未来：保存値がまだ期限切れでない
		{"future", now.Add(time.Second), now.Add(time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mock := newTestCommitter(t)
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
				WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
			mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
				WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
					AddRow(int64(1), int64(3), raw, int64(0), tc.persisted))
			mock.ExpectRollback()

			_, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{
				CmdID: "pet-" + tc.name, FarmID: 1, ActorUser: 1,
				Type: domain.CmdPetAutoHarvest, PetScheduledAt: tc.scheduled,
			}})
			assertErrCode(t, err, errcode.FarmVersionConflict)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("mock expectations: %v", err)
			}
		})
	}
}

func TestBuildPayload_PetHarvestUsesPetAutoModeAndRemainingYield(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	payload, ok := buildPayload(farmevents.EventTypeFarmHarvested,
		&domain.Snapshot{FarmID: 1, OwnerID: 1, Version: 4},
		domain.Command{CmdID: "pet", ActorUser: 1},
		applyResult{changedPlot: domain.Plot{PlotID: 0}, harvestedCropID: "WHEAT", harvestedYield: 4, harvestMode: "PET_AUTO"},
		now, nil,
	).(farmevents.FarmHarvestedPayload)
	if !ok || payload.Mode != "PET_AUTO" || payload.Yield != 4 {
		t.Fatalf("payload=%+v ok=%v", payload, ok)
	}
}

func TestCommitFarmCommand_PurchaseSeed_Success(t *testing.T) {
	c, mock := newTestCommitter(t)
	observer := newEconomyObserverCapture()
	c.WithObserver(observer)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_after"}))
	// 钱包余额 500 金币。
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(int64(500)))
	// 扣金币（10 * 3 = 30）。
	mock.ExpectExec(`UPDATE wallets`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 增加种子库存。
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 写经济流水。
	mock.ExpectExec(`INSERT INTO economy_transactions`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "buy-seed-1", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 3,
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EventID == "" {
		t.Error("EventID should be non-empty")
	}
	for _, stage := range []string{"begin", "idempotency_query", "wallet_lock", "wallet_update", "inventory_write", "ledger_insert", "commit", "total"} {
		if observer.stages["purchase/"+stage+"/ok"] != 1 {
			t.Errorf("stage %s metrics=%v", stage, observer.stages)
		}
	}
	if observer.active != 0 || observer.pool != 1 || len(observer.internal) != 0 {
		t.Fatalf("observer active=%d pool=%d internal=%v", observer.active, observer.pool, observer.internal)
	}
}

// 数量上限是权威事务内不可绕过的关口：没有它，`单价 × Quantity` 会在 int64
// 上溢出为负数，使余额检查失效。网关也会校验，但 gRPC 直调会跳过网关。
// TestCommitFarmCommand_PurchaseSeed_RejectsInvalidQuantity 验证两端边界：
//   - 下限：quantity < 1（0、-1、缺省）不能被静默改为 1，出售时否则会意外扣库存
//   - 上限：quantity > MaxEconomyQuantity 防止乘法溢出绕过余额检查
func TestCommitFarmCommand_PurchaseSeed_RejectsInvalidQuantity(t *testing.T) {
	for _, tc := range []struct {
		name string
		qty  int64
	}{
		// 下限：< 1 必须拒绝
		{"zero", 0},
		{"negative_one", -1},
		{"min_int64", math.MinInt64},
		// 上限：> MaxEconomyQuantity 必须拒绝
		{"just_over_limit", domain.MaxEconomyQuantity + 1},
		{"overflows_int64_multiplication", math.MaxInt64 / 2},
		{"max_int64", math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mock := newTestCommitter(t)
			// 校验在事务外发生，不应接触连接池、BeginTx 或任何 SQL。
			req := application.CommitRequest{Command: domain.Command{
				CmdID: "buy-seed-invalid-qty", FarmID: 1, ActorUser: 10,
				Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: tc.qty,
			}}
			_, err := c.CommitFarmCommand(context.Background(), req)
			if err == nil {
				t.Fatalf("quantity=%d 必须被拒绝", tc.qty)
			}
			var coded *errcode.Error
			if !errors.As(err, &coded) || coded.Code != errcode.CommonInvalidArgument {
				t.Fatalf("期望 CommonInvalidArgument，实际 err=%v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("不应有任何数据库操作: %v", err)
			}
		})
	}
}

// 上限本身必须放行，避免把合法的连点合并请求误拒。
func TestCommitFarmCommand_PurchaseSeed_AllowsQuantityAtLimit(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_after"}))
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(int64(1_000_000_000)))
	mock.ExpectExec(`UPDATE wallets`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO economy_transactions`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "buy-seed-at-limit", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: domain.MaxEconomyQuantity,
	}}
	if _, err := c.CommitFarmCommand(context.Background(), req); err != nil {
		t.Fatalf("上限值必须放行，实际 err=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestCommitEconomicTxClassifiesMySQLErrorByStage(t *testing.T) {
	c, mock := newTestCommitter(t)
	observer := newEconomyObserverCapture()
	c.WithObserver(observer)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WillReturnError(&mysql.MySQLError{Number: 1213, Message: "deadlock"})
	mock.ExpectRollback()

	_, err := c.commitEconomicTx(context.Background(), domain.Command{
		CmdID: "deadlock", ActorUser: 10, Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 1,
	}, time.Now())
	if err == nil {
		t.Fatal("expected MySQL error")
	}
	if observer.internal["mysql/purchase/idempotency_query/mysql_1213"] != 1 {
		t.Fatalf("internal=%v", observer.internal)
	}
	if observer.active != 0 || observer.stages["purchase/total/error"] != 1 {
		t.Fatalf("active=%d stages=%v", observer.active, observer.stages)
	}
}

func TestCommitEconomicTxClassifiesDBPoolDeadline(t *testing.T) {
	c, _ := newTestCommitter(t)
	observer := newEconomyObserverCapture()
	c.WithObserver(observer)
	c.db.SetMaxOpenConns(1)
	held, err := c.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = c.commitEconomicTx(ctx, domain.Command{Type: domain.CmdPurchaseSeed, Quantity: 1}, time.Now())
	assertErrCode(t, err, errcode.ResourceExhausted)
	if errcode.Reason(err) != errcode.CapacityReasonDBPoolWait {
		t.Fatalf("reason=%q", errcode.Reason(err))
	}
	if observer.internal["mysql/purchase/db_pool_wait/context_deadline"] != 1 {
		t.Fatalf("internal=%v", observer.internal)
	}
	if observer.stages["purchase/begin/error"] != 0 {
		t.Fatalf("pool wait must not be included in begin stage: stages=%v", observer.stages)
	}
}

func TestCommitFarmCommand_PurchaseSeed_InsufficientFunds(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_after"}))
	// 只有 5 金币，买不起一颗种子（单价 10）。
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(int64(5)))
	mock.ExpectRollback()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "buy-fail", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 1,
	}}
	_, err := c.CommitFarmCommand(context.Background(), req)
	assertErrCode(t, err, errcode.EconomyInsufficient)
}

func TestCommitFarmCommand_SellCrop_Success(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_after"}))
	// Global lock order is wallet then inventory.
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(int64(100)))
	// 扣库存（2 个作物）。
	mock.ExpectExec(`UPDATE inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 加金币（20 * 2 = 40）。
	mock.ExpectExec(`UPDATE wallets`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 写经济流水。
	mock.ExpectExec(`INSERT INTO economy_transactions`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "sell-1", FarmID: 1, ActorUser: 10,
		Type: domain.CmdSellCrop, CropID: "WHEAT", Quantity: 2,
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EventID == "" {
		t.Error("EventID should be non-empty")
	}
}

func TestCommitFarmCommand_SellCrop_NoInventory(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"balance_after"}))
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(int64(100)))
	// 库存不足：UPDATE 影响 0 行。
	mock.ExpectExec(`UPDATE inventory_items`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "sell-fail", FarmID: 1, ActorUser: 10,
		Type: domain.CmdSellCrop, CropID: "WHEAT", Quantity: 5,
	}}
	_, err := c.CommitFarmCommand(context.Background(), req)
	assertErrCode(t, err, errcode.EconomyInsufficient)
}

func TestCommitFarmCommand_Replayed(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}).AddRow(int64(5)))
	mock.ExpectRollback() // deferred Rollback on un-committed tx

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "dup-cmd", FarmID: 1, ActorUser: 10,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Replayed {
		t.Error("expected Replayed=true")
	}
	if res.NewVersion != 5 {
		t.Errorf("expected cached version 5, got %d", res.NewVersion)
	}
}

func TestCommitFarmCommand_StealReplaySkipsYieldAndInventoryMutation(t *testing.T) {
	c, mock := newTestCommitter(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(uint64(200), "same-steal").
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}).AddRow(int64(9)))
	mock.ExpectRollback()

	res, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{
		CmdID: "same-steal", FarmID: 1, ActorUser: 200, Type: domain.CmdStealCrop, PlotID: 0, BaseVersion: 8,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Replayed || res.NewVersion != 9 {
		t.Fatalf("result=%+v", res)
	}
	// No snapshot lock or inventory write is expected after the receipt hit.
}

func TestCommitFarmCommand_RetriesWholeTransactionAfterDeadlock(t *testing.T) {
	c, mock := newTestCommitter(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WillReturnError(&mysql.MySQLError{Number: 1213, Message: "deadlock"})
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}).AddRow(int64(5)))
	mock.ExpectRollback()

	res, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{
		CmdID: "retry-deadlock", FarmID: 1, ActorUser: 10, Type: domain.CmdPlant,
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Replayed || res.NewVersion != 5 {
		t.Fatalf("result=%+v", res)
	}
}

func TestCommitFarmCommand_VersionConflict(t *testing.T) {
	c, mock := newTestCommitter(t)

	// Snapshot in DB has version=3; command sends BaseVersion=2 (stale).
	raw := newGrowingSnapshotJSON(t) // version=3, plot 0 = mature WHEAT

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
			AddRow(int64(1), int64(3), raw, int64(0), nil))
	mock.ExpectRollback()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "cmd-stale", FarmID: 1, ActorUser: 10,
		Type: domain.CmdHarvest, PlotID: 0, BaseVersion: 2, // stale
	}}
	_, err := c.CommitFarmCommand(context.Background(), req)
	assertErrCode(t, err, errcode.FarmVersionConflict)
}

func TestCommitFarmCommand_ExternalVersionZeroDoesNotBypassConflict(t *testing.T) {
	c, mock := newTestCommitter(t)
	raw := newGrowingSnapshotJSON(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
			AddRow(int64(1), int64(3), raw, int64(0), nil))
	mock.ExpectRollback()

	_, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{
		CmdID: "zero-is-a-version", FarmID: 1, ActorUser: 1, Type: domain.CmdWater, BaseVersion: 0,
	}})
	assertErrCode(t, err, errcode.FarmVersionConflict)
}

// TestCommitFarmCommand_EconomicCmd_Replayed 验证 PurchaseSeed/SellCrop 的幂等重放。
// 相同 cmd_id 第二次提交时，commitEconomicTx 应直接返回经济流水结果，不重复扣钱/加钱。
func TestCommitFarmCommand_EconomicCmd_Replayed(t *testing.T) {
	for _, cmdType := range []domain.CommandType{domain.CmdPurchaseSeed, domain.CmdSellCrop} {
		t.Run(string(cmdType), func(t *testing.T) {
			c, mock := newTestCommitter(t)

			// Natural business key hits economy_transactions: no wallet/inventory writes.
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT balance_after[\s\S]+FROM economy_transactions`).
				WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"balance_after"}).AddRow(int64(123)))
			mock.ExpectRollback()

			req := application.CommitRequest{Command: domain.Command{
				CmdID: "dup-eco-cmd", FarmID: 1, ActorUser: 10,
				Type: cmdType, CropID: "WHEAT", Quantity: 1,
			}}
			res, err := c.CommitFarmCommand(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.Replayed {
				t.Error("expected Replayed=true for duplicate economic command")
			}
			if res.CoinBalance != 123 {
				t.Fatalf("coin_balance=%d", res.CoinBalance)
			}
		})
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func assertErrCode(t *testing.T, err error, want errcode.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", want)
	}
	e, ok := err.(*errcode.Error)
	if !ok {
		t.Fatalf("expected *errcode.Error, got %T: %v", err, err)
	}
	if e.Code != want {
		t.Errorf("expected code %s, got %s", want, e.Code)
	}
}

// ── HelpWater / StealCrop applyCmd 单测 ─────────────────────────────────────

// TestApplyCmd_HelpWater_Success 验证好友代浇水等同于普通 Water。
func TestApplyCmd_HelpWater_Success(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.OwnerID = 99 // 农场主与 ActorUser 不同
	snap.Plots[0] = domain.Plot{
		PlotID:    0,
		CropID:    "WHEAT",
		Status:    domain.PlotGrowing,
		PlantedAt: now.Add(-1 * time.Minute),
		MatureAt:  now.Add(10 * time.Minute),
	}
	cmd := domain.Command{CmdID: "hw1", FarmID: 1, ActorUser: 200, Type: domain.CmdHelpWater, PlotID: 0}

	ar, err := applyCmd(&snap, cmd, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 地块状态不变，仅产生 Watered 事件。
	if ar.changedPlot.Status != domain.PlotGrowing {
		t.Errorf("expected GROWING, got %q", ar.changedPlot.Status)
	}
}

// TestApplyCmd_HelpWater_EmptyPlot 验证代浇空地块返回 FarmPlotState 错误。
func TestApplyCmd_HelpWater_EmptyPlot(t *testing.T) {
	snap := emptySnapshot(1)
	cmd := domain.Command{Type: domain.CmdHelpWater, PlotID: 0}

	_, err := applyCmd(&snap, cmd, time.Now())
	assertErrCode(t, err, errcode.FarmPlotState)
}

// TestApplyCmd_StealCrop_Success 验证好友偷菜只扣偷取份额并保留成熟作物。
func TestApplyCmd_StealCrop_Success(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.OwnerID = 99
	snap.Plots[0] = domain.Plot{
		PlotID:         0,
		CropID:         "WHEAT",
		Status:         domain.PlotGrowing,
		PlantedAt:      now.Add(-20 * time.Minute),
		MatureAt:       now.Add(-1 * time.Minute), // 已成熟
		RemainingYield: 5,
	}
	original := snap.Plots[0]
	cmd := domain.Command{CmdID: "sc1", FarmID: 1, ActorUser: 200, Type: domain.CmdStealCrop, PlotID: 0}

	ar, err := applyCmd(&snap, cmd, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ar.changedPlot.Status != domain.PlotGrowing || ar.changedPlot.EffectiveStatus(now) != domain.PlotMature {
		t.Errorf("expected mature planted plot after steal, got %+v", ar.changedPlot)
	}
	if ar.harvestedCropID != "WHEAT" || ar.stolenAmount != 1 || ar.remainingYield != 4 {
		t.Errorf("unexpected steal result: %+v", ar)
	}
	if ar.changedPlot.CropID != original.CropID || !ar.changedPlot.PlantedAt.Equal(original.PlantedAt) || !ar.changedPlot.MatureAt.Equal(original.MatureAt) {
		t.Errorf("steal changed crop lifecycle: before=%+v after=%+v", original, ar.changedPlot)
	}
}

func TestApplyCmd_StealThenOwnerHarvestsRemainingYield(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{PlotID: 0, CropID: "WHEAT", Status: domain.PlotGrowing, PlantedAt: now.Add(-20 * time.Minute), MatureAt: now.Add(-time.Minute), RemainingYield: 5}

	if _, err := applyCmd(&snap, domain.Command{Type: domain.CmdStealCrop, PlotID: 0}, now); err != nil {
		t.Fatal(err)
	}
	harvest, err := applyCmd(&snap, domain.Command{Type: domain.CmdHarvest, PlotID: 0}, now)
	if err != nil {
		t.Fatal(err)
	}
	if harvest.harvestedYield != 4 || harvest.changedPlot.Status != domain.PlotEmpty || harvest.changedPlot.RemainingYield != 0 {
		t.Fatalf("harvest=%+v", harvest)
	}
}

func TestApplyCmd_StealCannotExhaustOwnerYield(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{PlotID: 0, CropID: "WHEAT", Status: domain.PlotGrowing, MatureAt: now.Add(-time.Minute), RemainingYield: 1}

	_, err := applyCmd(&snap, domain.Command{Type: domain.CmdStealCrop, PlotID: 0}, now)
	assertErrCode(t, err, errcode.FarmPlotState)
	if snap.Plots[0].RemainingYield != 1 || snap.Plots[0].Status != domain.PlotGrowing {
		t.Fatalf("failed steal changed plot: %+v", snap.Plots[0])
	}
}

func TestBuildPayload_StolenIncludesAmounts(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := domain.Snapshot{FarmID: 11, OwnerID: 22, Version: 7}
	cmd := domain.Command{CmdID: "steal-payload", ActorUser: 33}
	ar := applyResult{changedPlot: domain.Plot{PlotID: 4}, harvestedCropID: "WHEAT", stolenAmount: 1, remainingYield: 4}

	payload, ok := buildPayload(farmevents.EventTypeFarmStolen, &snap, cmd, ar, now, nil).(farmevents.FarmStolenPayload)
	if !ok {
		t.Fatal("unexpected payload type")
	}
	if payload.StolenAmount != 1 || payload.RemainingYield != 4 || payload.CropID != "WHEAT" {
		t.Fatalf("payload=%+v", payload)
	}
}

// TestApplyCmd_StealCrop_NotMature 验证偷未成熟地块返回 FarmPlotState 错误。
func TestApplyCmd_StealCrop_NotMature(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := emptySnapshot(1)
	snap.Plots[0] = domain.Plot{
		PlotID:    0,
		CropID:    "WHEAT",
		Status:    domain.PlotGrowing,
		PlantedAt: now.Add(-1 * time.Minute),
		MatureAt:  now.Add(10 * time.Minute), // 尚未成熟
	}
	cmd := domain.Command{Type: domain.CmdStealCrop, PlotID: 0}

	_, err := applyCmd(&snap, cmd, now)
	assertErrCode(t, err, errcode.FarmPlotState)
}

// TestCommitFarmCommand_HelpWater_NotFriend 验证非好友代浇水被 checkFriendship 拦截。
// gamesvr 事务内 checkFriendship 查不到好友行时返回 SocialNotFriend。
func TestCommitFarmCommand_HelpWater_NotFriend(t *testing.T) {
	c, mock := newTestCommitter(t)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 农场主 ownerID=99，ActorUser=200（不是好友）
	snap := emptySnapshot(1)
	snap.OwnerID = 99
	snap.Plots[0] = domain.Plot{
		PlotID:    0,
		CropID:    "WHEAT",
		Status:    domain.PlotGrowing,
		PlantedAt: now.Add(-1 * time.Minute),
		MatureAt:  now.Add(10 * time.Minute),
	}
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
			AddRow(int64(99), int64(1), raw, int64(0), nil))
	// checkFriendship：未找到好友行。
	mock.ExpectQuery(`SELECT 1 FROM friendships`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"1"})) // 空结果 → sql.ErrNoRows
	mock.ExpectRollback()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "hw-nf", FarmID: 1, ActorUser: 200, Type: domain.CmdHelpWater, PlotID: 0, BaseVersion: 1,
	}}
	_, err = c.CommitFarmCommand(context.Background(), req)
	assertErrCode(t, err, errcode.SocialNotFriend)
}

// TestCommitFarmCommand_StealCrop_InventoryCredit 验证偷菜只给偷菜者入库。
// WHEAT yield=5：stealAmount=floor(5×20%)=1，owner 此时不入库。
func TestCommitFarmCommand_StealCrop_InventoryCredit(t *testing.T) {
	c, mock := newTestCommitter(t)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 农场主 ownerID=99，ActorUser=200（偷菜者）
	snap := emptySnapshot(1)
	snap.OwnerID = 99
	snap.Plots[0] = domain.Plot{
		PlotID:         0,
		CropID:         "WHEAT",
		Status:         domain.PlotGrowing,
		PlantedAt:      now.Add(-20 * time.Minute),
		MatureAt:       now.Add(-1 * time.Minute), // 已成熟
		RemainingYield: 5,
	}
	raw, err := marshalSnapshot(&snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	actorUser := int64(200)
	ownerUser := int64(99)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).
			AddRow(ownerUser, int64(1), raw, int64(0), nil))
	// checkFriendship：找到好友行（返回 1 行）。
	mock.ExpectQuery(`SELECT 1 FROM friendships`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	// addInventoryItem：仅偷取方拿 1 个；若代码给农场主入库会打乱后续 SQL 期望。
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WithArgs(uint64(actorUser), "CROP", sqlmock.AnyArg(), int64(1), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// upsertSnapshot
	mock.ExpectExec(`INSERT INTO farm_snapshots`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// insertReceipt
	mock.ExpectExec(`INSERT INTO cmd_receipts`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// insertOutbox
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := application.CommitRequest{Command: domain.Command{
		CmdID: "sc-credit", FarmID: 1, ActorUser: actorUser, Type: domain.CmdStealCrop, PlotID: 0, BaseVersion: 1,
	}}
	res, err := c.CommitFarmCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.NewVersion != 2 {
		t.Errorf("expected version 2, got %d", res.NewVersion)
	}
	if len(res.Patch.Plots) != 1 || res.Patch.Plots[0].Status != domain.PlotGrowing || res.Patch.Plots[0].RemainingYield != 4 {
		t.Fatalf("unexpected steal patch: %+v", res.Patch)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sql expectations not met: %v", err)
	}
}

func TestAdvanceRouteEpoch_SuccessAndStale(t *testing.T) {
	t.Run("advance", func(t *testing.T) {
		c, mock := newTestCommitter(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT route_epoch FROM farm_snapshots`).WithArgs(uint64(7)).
			WillReturnRows(sqlmock.NewRows([]string{"route_epoch"}).AddRow(int64(8)))
		mock.ExpectExec(`UPDATE farm_snapshots SET route_epoch`).
			WithArgs(uint64(9), sqlmock.AnyArg(), uint64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if err := c.AdvanceRouteEpoch(context.Background(), 7, 9); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("stale", func(t *testing.T) {
		c, mock := newTestCommitter(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT route_epoch FROM farm_snapshots`).WithArgs(uint64(7)).
			WillReturnRows(sqlmock.NewRows([]string{"route_epoch"}).AddRow(int64(9)))
		mock.ExpectRollback()
		err := c.AdvanceRouteEpoch(context.Background(), 7, 8)
		var coded *errcode.Error
		if !errors.As(err, &coded) || coded.Code != errcode.RoutingEpochStale {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestCommitFarmCommand_RejectsStaleRouteEpoch(t *testing.T) {
	c, mock := newTestCommitter(t)
	snapshot := domain.Snapshot{FarmID: 1, OwnerID: 1, Version: 3, Plots: map[int32]domain.Plot{}}
	raw, marshalErr := marshalSnapshot(&snapshot)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT result_version FROM cmd_receipts`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"result_version"}))
	mock.ExpectQuery(`SELECT owner_user_id, version, snapshot, route_epoch, next_pet_action_at FROM farm_snapshots`).WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"owner_user_id", "version", "snapshot", "route_epoch", "next_pet_action_at"}).AddRow(int64(1), int64(3), raw, int64(9), nil))
	mock.ExpectRollback()
	_, err := c.CommitFarmCommand(context.Background(), application.CommitRequest{Command: domain.Command{CmdID: "old-owner", FarmID: 1, ActorUser: 1, Type: domain.CmdPlant, CropID: "WHEAT", RouteEpoch: 8}})
	var coded *errcode.Error
	if !errors.As(err, &coded) || coded.Code != errcode.RoutingFenced {
		t.Fatalf("err=%v", err)
	}
}

// TestGetCropConfig_SafeMultiplyGuard 验证单价超出安全乘法边界时 getCropConfig 返回 Internal 错误。
// 当前价格是硬编码的小值；此测试防止未来从数据库读取价格时引入溢出漏洞。
func TestGetCropConfig_SafeMultiplyGuard(t *testing.T) {
	original := cropConfigs["WHEAT"]
	// 写入一个单价超过安全边界的临时配置，跑完立即恢复。
	unsafePrice := math.MaxInt64/domain.MaxEconomyQuantity + 1
	cropConfigs["WHEAT"] = CropConfig{
		SeedPrice: unsafePrice, GrowthDuration: original.GrowthDuration,
		HarvestYield: original.HarvestYield, SellPrice: original.SellPrice,
	}
	defer func() { cropConfigs["WHEAT"] = original }()

	_, err := getCropConfig("WHEAT")
	if err == nil {
		t.Fatal("单价超出安全乘法上限时必须返回错误")
	}
	var coded *errcode.Error
	if !errors.As(err, &coded) || coded.Code != errcode.Internal {
		t.Fatalf("期望 Internal 错误，实际 err=%v", err)
	}
}
