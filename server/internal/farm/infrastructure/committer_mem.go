// Package infrastructure 提供 farm 相关端口的实现。
// MemCommitter 是骨架用内存版 Committer，演示"单事务提交 + cmd_receipt 幂等"语义；
// 正式实现应在 gamesvr 内用一笔 MySQL 事务按统一锁顺序写入
// farm_snapshots、inventory/wallet、economy_transactions、cmd_receipts、outbox_events。
package infrastructure

import (
	"context"
	"sync"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
)

// MemCommitter 用内存模拟权威提交，仅供骨架联调与单测。
type MemCommitter struct {
	clk clock.Clock

	mu        sync.Mutex
	snapshots map[int64]*domain.Snapshot          // farm_id -> 快照
	receipts  map[string]application.CommitResult // 幂等键 -> 已提交结果
}

// NewMemCommitter 构造内存 Committer。
func NewMemCommitter(clk clock.Clock) *MemCommitter {
	return &MemCommitter{
		clk:       clk,
		snapshots: make(map[int64]*domain.Snapshot),
		receipts:  make(map[string]application.CommitResult),
	}
}

var _ application.Committer = (*MemCommitter)(nil)
var _ application.SnapshotLoader = (*MemCommitter)(nil)
var _ application.RouteFencer = (*MemCommitter)(nil)

func (m *MemCommitter) AdvanceRouteEpoch(_ context.Context, farmID, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.ensureLocked(farmID)
	if epoch < s.RouteEpoch {
		return errcode.New(errcode.RoutingEpochStale, "route epoch stale")
	}
	s.RouteEpoch = epoch
	return nil
}

// LoadSnapshot 返回农场快照，不存在则初始化空快照。
func (m *MemCommitter) LoadSnapshot(_ context.Context, farmID int64) (domain.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.ensureLocked(farmID)
	return *s, nil
}

// CommitFarmCommand 在"单事务"内应用命令并落幂等回执。
// 这里用一把互斥锁模拟事务边界；正式实现替换为 MySQL 事务。
func (m *MemCommitter) CommitFarmCommand(_ context.Context, req application.CommitRequest) (application.CommitResult, error) {
	cmd := req.Command
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1) 幂等：命中已有回执直接返回，不重复结算（ADR-018）。
	if r, ok := m.receipts[receiptKey(cmd)]; ok {
		r.Replayed = true
		return r, nil
	}

	s := m.ensureLocked(cmd.FarmID)
	if cmd.RouteEpoch != s.RouteEpoch {
		return application.CommitResult{}, errcode.New(errcode.RoutingFenced, "route epoch does not own farm")
	}

	// 2) 事务内重新校验版本（不信任 Actor 预校验为最终依据）。
	if cmd.Type != domain.CmdPetAutoHarvest && cmd.BaseVersion != s.Version {
		return application.CommitResult{}, errcode.New(errcode.FarmVersionConflict, "base_version stale")
	}

	// 3) 应用领域变化（骨架仅演示 PLANT/HARVEST）。
	now := m.clk.NowUTC()
	plot := s.Plots[cmd.PlotID]
	plot.PlotID = cmd.PlotID
	switch cmd.Type {
	case domain.CmdPlant:
		if plot.Status != "" && plot.Status != domain.PlotEmpty {
			return application.CommitResult{}, errcode.New(errcode.FarmPlotState, "plot not empty")
		}
		plot.CropID = cmd.CropID
		plot.Status = domain.PlotGrowing
		plot.PlantedAt = now
		plot.MatureAt = now.Add(60) // 骨架占位：真实成熟时长由作物配置决定
		plot.RemainingYield = harvestYieldForCrop(cmd.CropID)
	case domain.CmdHarvest:
		if plot.EffectiveStatus(now) != domain.PlotMature {
			return application.CommitResult{}, errcode.New(errcode.FarmPlotState, "plot not mature")
		}
		plot = domain.Plot{PlotID: cmd.PlotID, Status: domain.PlotEmpty}
	}
	s.Plots[cmd.PlotID] = plot
	s.Version++

	// 4) 生成结果与 outbox 事件 ID，并落幂等回执。
	res := application.CommitResult{
		NewVersion: s.Version,
		Patch: domain.Patch{
			FarmID:    s.FarmID,
			Version:   s.Version,
			Plots:     []domain.Plot{plot},
			ActorUser: cmd.ActorUser,
		},
		EventID: id.NewV7(),
	}
	m.receipts[receiptKey(cmd)] = res
	return res, nil
}

func (m *MemCommitter) ensureLocked(farmID int64) *domain.Snapshot {
	s, ok := m.snapshots[farmID]
	if !ok {
		s = &domain.Snapshot{FarmID: farmID, OwnerID: farmID, Version: 0, Plots: make(map[int32]domain.Plot)}
		m.snapshots[farmID] = s
	}
	return s
}

// receiptKey 按 user_id + cmd_id 定义幂等范围（见 ADR-018）。
func receiptKey(c domain.Command) string {
	return c.CmdID // 骨架简化；正式实现应为 fmt.Sprintf("%d:%s", c.ActorUser, c.CmdID)
}
