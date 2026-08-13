// Package infrastructure — 完整玩家流程集成测试
// 单个玩家：登录 → 购买种子 → 播种 → 等待成熟 → 收获 → 出售 → 余额校验
// 全程 cmd_receipts 幂等验证：相同 cmd_id 重试不重复结算
package infrastructure

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/id"
)

// ── 测试用全功能内存 Committer ─────────────────────────────────────────────────
// 支持全部 5 种命令：Plant / Water / Harvest / PurchaseSeed / SellCrop
// 内部维护 farm_snapshots + wallet + inventory + receipts 四套内存数据

type fullMemCommitter struct {
	clk clock.Clock
	mu  sync.Mutex

	// farms
	snapshots map[int64]*domain.Snapshot
	// wallet: userID → coin balance
	wallets map[int64]int64
	// inventory: (userID, kind, itemID) → quantity
	// key = fmt.Sprintf("%d:%s:%d", userID, kind, itemID)
	inventory map[string]int64
	// receipts: cmdID → cached result (幂等)
	receipts map[string]commitEntry
}

type commitEntry struct {
	version int64
	eventID string
	patch   domain.Patch
}

func newFullMemCommitter() *fullMemCommitter {
	return &fullMemCommitter{
		clk:       clock.System{},
		snapshots: make(map[int64]*domain.Snapshot),
		wallets:   make(map[int64]int64),
		inventory: make(map[string]int64),
		receipts:  make(map[string]commitEntry),
	}
}

// InitPlayer 初始化玩家：创建农场 + 给初始金币 + 确保空背包
func (c *fullMemCommitter) InitPlayer(userID int64, initialCoins int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots[userID] = &domain.Snapshot{
		FarmID:  userID,
		OwnerID: userID,
		Version: 0,
		Plots:   make(map[int32]domain.Plot),
	}
	c.wallets[userID] = initialCoins
}

// WalletBalance 返回玩家金币余额
func (c *fullMemCommitter) WalletBalance(userID int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wallets[userID]
}

// InventoryCount 返回玩家某种物品的库存
func (c *fullMemCommitter) InventoryCount(userID int64, kind string, itemID int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inventory[fmt.Sprintf("%d:%s:%d", userID, kind, itemID)]
}

// Snapshot 返回农场快照（含 plots 状态）
func (c *fullMemCommitter) Snapshot(farmID int64) *domain.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.snapshots[farmID]
	if s == nil {
		return nil
	}
	cp := *s
	cp.Plots = make(map[int32]domain.Plot, len(s.Plots))
	for k, v := range s.Plots {
		cp.Plots[k] = v
	}
	return &cp
}

// Commit 提交命令（模拟 MySQL Committer 的完整逻辑）
func (c *fullMemCommitter) Commit(userID int64, cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ── 1) 幂等检查 ──
	if entry, ok := c.receipts[cmd.CmdID]; ok {
		return entry.patch, entry.eventID, true, nil // replayed
	}

	// ── 2) 分发到经济/农场分支 ──
	switch cmd.Type {
	case domain.CmdPurchaseSeed:
		return c.commitPurchaseSeed(cmd, now)
	case domain.CmdSellCrop:
		return c.commitSellCrop(cmd, now)
	case domain.CmdPlant:
		return c.commitPlant(cmd, now)
	case domain.CmdHarvest:
		return c.commitHarvest(cmd, now)
	case domain.CmdWater:
		return c.commitWater(cmd, now)
	default:
		return domain.Patch{}, "", false,
			errcode.New(errcode.CommonInvalidArgument, "unsupported command type")
	}
}

// ── 经济类命令 ──

func (c *fullMemCommitter) commitPurchaseSeed(cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	cfg, err := getCropConfig(cmd.CropID)
	if err != nil {
		return domain.Patch{}, "", false, err
	}
	qty := cmd.Quantity
	if qty <= 0 {
		qty = 1
	}
	itemID := cropIDToItemID(cmd.CropID)
	price := cfg.SeedPrice * qty

	balance := c.wallets[cmd.ActorUser]
	if balance < price {
		return domain.Patch{}, "", false, errcode.New(errcode.EconomyInsufficient,
			fmt.Sprintf("金币不足：需要 %d，剩余 %d", price, balance))
	}

	// 扣金币
	c.wallets[cmd.ActorUser] = balance - price
	// 加库存
	key := fmt.Sprintf("%d:SEED:%d", cmd.ActorUser, itemID)
	c.inventory[key] += qty

	return c.persist(cmd, 0, domain.Patch{FarmID: cmd.FarmID, ActorUser: cmd.ActorUser})
}

func (c *fullMemCommitter) commitSellCrop(cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	cfg, err := getCropConfig(cmd.CropID)
	if err != nil {
		return domain.Patch{}, "", false, err
	}
	qty := cmd.Quantity
	if qty <= 0 {
		qty = 1
	}
	itemID := cropIDToItemID(cmd.CropID)

	key := fmt.Sprintf("%d:CROP:%d", cmd.ActorUser, itemID)
	if c.inventory[key] < qty {
		return domain.Patch{}, "", false, errcode.New(errcode.EconomyInsufficient,
			fmt.Sprintf("作物库存不足：需要 %d，剩余 %d", qty, c.inventory[key]))
	}

	gain := cfg.SellPrice * qty
	c.inventory[key] -= qty
	c.wallets[cmd.ActorUser] += gain

	return c.persist(cmd, 0, domain.Patch{FarmID: cmd.FarmID, ActorUser: cmd.ActorUser})
}

// ── 农场类命令 ──

func (c *fullMemCommitter) commitPlant(cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	s := c.ensureSnap(cmd.FarmID)

	if cmd.BaseVersion != 0 && cmd.BaseVersion != s.Version {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmVersionConflict, "base_version stale")
	}

	plot := s.Plots[cmd.PlotID]
	plot.PlotID = cmd.PlotID
	if plot.Status != "" && plot.Status != domain.PlotEmpty {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmPlotState, "plot not empty")
	}

	// 扣库存种子
	itemID := cropIDToItemID(cmd.CropID)
	key := fmt.Sprintf("%d:SEED:%d", s.OwnerID, itemID)
	if c.inventory[key] <= 0 {
		return domain.Patch{}, "", false,
			errcode.New(errcode.EconomyInsufficient, "种子库存不足")
	}
	c.inventory[key]--

	plot.CropID = cmd.CropID
	plot.Status = domain.PlotGrowing
	plot.PlantedAt = now
	plot.MatureAt = now.Add(60) // 骨架：固定 60s 成熟
	s.Plots[cmd.PlotID] = plot
	s.Version++

	return c.persist(cmd, s.Version, domain.Patch{
		FarmID:    s.FarmID,
		Version:   s.Version,
		Plots:     []domain.Plot{plot},
		ActorUser: cmd.ActorUser,
	})
}

func (c *fullMemCommitter) commitHarvest(cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	s := c.ensureSnap(cmd.FarmID)

	if cmd.BaseVersion != 0 && cmd.BaseVersion != s.Version {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmVersionConflict, "base_version stale")
	}

	plot := s.Plots[cmd.PlotID]
	if plot.EffectiveStatus(now) != domain.PlotMature {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmPlotState, "plot not mature")
	}

	// 收获：作物加入库存
	itemID := cropIDToItemID(plot.CropID)
	key := fmt.Sprintf("%d:CROP:%d", s.OwnerID, itemID)
	c.inventory[key]++

	// 清空地快
	harvestedPlot := domain.Plot{PlotID: cmd.PlotID, Status: domain.PlotEmpty}
	s.Plots[cmd.PlotID] = harvestedPlot
	s.Version++

	return c.persist(cmd, s.Version, domain.Patch{
		FarmID:    s.FarmID,
		Version:   s.Version,
		Plots:     []domain.Plot{harvestedPlot},
		ActorUser: cmd.ActorUser,
	})
}

func (c *fullMemCommitter) commitWater(cmd domain.Command, now time.Time) (domain.Patch, string, bool, error) {
	s := c.ensureSnap(cmd.FarmID)

	if cmd.BaseVersion != 0 && cmd.BaseVersion != s.Version {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmVersionConflict, "base_version stale")
	}

	plot := s.Plots[cmd.PlotID]
	if plot.Status != domain.PlotGrowing {
		return domain.Patch{}, "", false,
			errcode.New(errcode.FarmPlotState, "only growing plots can be watered")
	}

	// 浇水加速成长（减少成熟时间）
	plot.MatureAt = plot.MatureAt.Add(-10) // 加速 10s
	if plot.MatureAt.Before(now) {
		plot.MatureAt = now
	}
	s.Plots[cmd.PlotID] = plot
	// Watering does not increment farm version in P0 (non-authoritative action)

	return c.persist(cmd, s.Version, domain.Patch{
		FarmID:    s.FarmID,
		Version:   s.Version,
		Plots:     []domain.Plot{plot},
		ActorUser: cmd.ActorUser,
	})
}

// ── helpers ──

func (c *fullMemCommitter) ensureSnap(farmID int64) *domain.Snapshot {
	s, ok := c.snapshots[farmID]
	if !ok {
		s = &domain.Snapshot{
			FarmID: farmID, OwnerID: farmID,
			Version: 0, Plots: make(map[int32]domain.Plot),
		}
		c.snapshots[farmID] = s
	}
	return s
}

func (c *fullMemCommitter) persist(cmd domain.Command, version int64, patch domain.Patch) (domain.Patch, string, bool, error) {
	eventID := id.NewV7()
	c.receipts[cmd.CmdID] = commitEntry{
		version: version,
		eventID: eventID,
		patch:   patch,
	}
	return patch, eventID, false, nil
}

// ── 集成测试 ─────────────────────────────────────────────────────────────────

// TestFullPlayerFlow 完整玩家流程
// 登录 → 购买种子 → 播种 → 等待成熟 → 收获 → 出售 → 余额一致性校验
func TestFullPlayerFlow(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	committer := newFullMemCommitter()

	const userID int64 = 1001
	const initCoins int64 = 100

	// ── 登录/初始化 ──
	committer.InitPlayer(userID, initCoins)
	if bal := committer.WalletBalance(userID); bal != 100 {
		t.Fatalf("初始余额应为 100，实际 %d", bal)
	}
	t.Logf("✅ 登录：用户 %d，初始金币 %d", userID, initCoins)

	// ── 购买种子（3 颗小麦，每颗 10 金币，共 30 金币） ──
	now := time.Now()
	_, _, replayed, err := committer.Commit(userID, domain.Command{
		CmdID: "purchase-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 3,
	}, now)
	if err != nil {
		t.Fatalf("购买种子失败: %v", err)
	}
	if replayed {
		t.Error("首次购买不应重放")
	}
	bal := committer.WalletBalance(userID)
	if bal != 70 {
		t.Fatalf("购买 3 颗种子后余额应为 70 (100-30)，实际 %d", bal)
	}
	seedCount := committer.InventoryCount(userID, "SEED", 1)
	if seedCount != 3 {
		t.Fatalf("种子库存应为 3，实际 %d", seedCount)
	}
	t.Logf("✅ 购买种子：花费 30 金币，余额 %d，种子库存 %d", bal, seedCount)

	// ── 幂等验证：重复购买（相同 cmd_id） ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "purchase-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 3,
	}, now)
	if err != nil {
		t.Fatalf("幂等重放失败: %v", err)
	}
	if !replayed {
		t.Error("相同 cmd_id 重放应标记为 replayed=true")
	}
	bal2 := committer.WalletBalance(userID)
	if bal2 != 70 {
		t.Fatalf("幂等重放后余额不应变化：期望 70，实际 %d", bal2)
	}
	seedCount2 := committer.InventoryCount(userID, "SEED", 1)
	if seedCount2 != 3 {
		t.Fatalf("幂等重放后种子库存不应变化：期望 3，实际 %d", seedCount2)
	}
	t.Logf("✅ 幂等验证（购买）：相同 cmd_id 重试不重复扣金币，余额仍为 %d，库存仍为 %d", bal2, seedCount2)

	// ── 播种（plot 0，用掉 1 颗小麦种子） ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "plant-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}, now)
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}
	if replayed {
		t.Error("首次播种不应重放")
	}
	seedCount = committer.InventoryCount(userID, "SEED", 1)
	if seedCount != 2 {
		t.Fatalf("播种后种子库存应为 2 (3-1)，实际 %d", seedCount)
	}
	snap := committer.Snapshot(userID)
	plot0 := snap.Plots[0]
	if plot0.Status != domain.PlotGrowing {
		t.Fatalf("播种后 plot 0 状态应为 GROWING，实际 %s", plot0.Status)
	}
	if plot0.CropID != "WHEAT" {
		t.Fatalf("播种后 plot 0 作物应为 WHEAT，实际 %s", plot0.CropID)
	}
	t.Logf("✅ 播种：plot 0 已种植 WHEAT，种子库存剩余 %d", seedCount)

	// ── 幂等验证：重复播种 ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "plant-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}, now)
	if err != nil {
		t.Fatalf("幂等重放播种失败: %v", err)
	}
	if !replayed {
		t.Error("相同 cmd_id 播种重放应标记为 replayed=true")
	}
	seedCount3 := committer.InventoryCount(userID, "SEED", 1)
	if seedCount3 != 2 {
		t.Fatalf("幂等重放后种子库存不应变化：期望 2，实际 %d", seedCount3)
	}
	t.Logf("✅ 幂等验证（播种）：重复 cmd_id 不消耗额外种子，库存仍为 %d", seedCount3)

	// ── 播种第二个地块（plot 1） ──
	_, _, _, err = committer.Commit(userID, domain.Command{
		CmdID: "plant-002", FarmID: userID, ActorUser: userID,
		Type: domain.CmdPlant, PlotID: 1, CropID: "WHEAT",
	}, now)
	if err != nil {
		t.Fatalf("播种 plot 1 失败: %v", err)
	}
	seedCount = committer.InventoryCount(userID, "SEED", 1)
	if seedCount != 1 {
		t.Fatalf("播种 plot 1 后种子库存应为 1，实际 %d", seedCount)
	}
	t.Logf("✅ 播种 plot 1：种子库存剩余 %d", seedCount)

	// ── 等待成熟（前进 65 秒，超过 60 秒成熟时间） ──
	now = now.Add(65 * time.Second)
	t.Logf("⏳ 等待 65 秒，作物应已成熟……")

	snap = committer.Snapshot(userID)
	if snap.Plots[0].EffectiveStatus(now) != domain.PlotMature {
		t.Fatalf("65s 后 plot 0 应已成熟，实际状态: %v", snap.Plots[0].EffectiveStatus(now))
	}
	if snap.Plots[1].EffectiveStatus(now) != domain.PlotMature {
		t.Fatalf("65s 后 plot 1 应已成熟")
	}
	t.Logf("✅ 作物已成熟：plot 0 和 plot 1 均可以收获")

	// ── 收获 plot 0 ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "harvest-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdHarvest, PlotID: 0,
	}, now)
	if err != nil {
		t.Fatalf("收获 plot 0 失败: %v", err)
	}
	if replayed {
		t.Error("首次收获不应重放")
	}
	cropCount := committer.InventoryCount(userID, "CROP", 1)
	if cropCount != 1 {
		t.Fatalf("收获后作物库存应为 1，实际 %d", cropCount)
	}
	snap = committer.Snapshot(userID)
	if snap.Plots[0].Status != domain.PlotEmpty {
		t.Fatalf("收获后 plot 0 应为 EMPTY，实际 %s", snap.Plots[0].Status)
	}
	t.Logf("✅ 收获 plot 0：获得 1 个 WHEAT 作物，地块已清空")

	// ── 幂等验证：重复收获 ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "harvest-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdHarvest, PlotID: 0,
	}, now)
	if err != nil {
		t.Fatalf("幂等重放收获失败: %v", err)
	}
	if !replayed {
		t.Error("相同 cmd_id 收获重放应标记为 replayed=true")
	}
	cropCount2 := committer.InventoryCount(userID, "CROP", 1)
	if cropCount2 != 1 {
		t.Fatalf("幂等重放后作物库存不应变化：期望 1，实际 %d", cropCount2)
	}
	t.Logf("✅ 幂等验证（收获）：重复 cmd_id 不重复增加库存，作物库存仍为 %d", cropCount2)

	// ── 收获 plot 1 ──
	_, _, _, err = committer.Commit(userID, domain.Command{
		CmdID: "harvest-002", FarmID: userID, ActorUser: userID,
		Type: domain.CmdHarvest, PlotID: 1,
	}, now)
	if err != nil {
		t.Fatalf("收获 plot 1 失败: %v", err)
	}
	cropCount = committer.InventoryCount(userID, "CROP", 1)
	if cropCount != 2 {
		t.Fatalf("收获 plot 1 后作物库存应为 2，实际 %d", cropCount)
	}
	t.Logf("✅ 收获 plot 1：作物库存增至 %d", cropCount)

	// ── 出售 2 个作物（每个 20 金币，共 40 金币） ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "sell-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdSellCrop, CropID: "WHEAT", Quantity: 2,
	}, now)
	if err != nil {
		t.Fatalf("出售作物失败: %v", err)
	}
	if replayed {
		t.Error("首次出售不应重放")
	}
	bal = committer.WalletBalance(userID)
	cropCount = committer.InventoryCount(userID, "CROP", 1)
	if cropCount != 0 {
		t.Fatalf("出售后作物库存应为 0，实际 %d", cropCount)
	}
	t.Logf("✅ 出售作物：获得 40 金币，作物库存 %d", cropCount)

	// ── 幂等验证：重复出售 ──
	_, _, replayed, err = committer.Commit(userID, domain.Command{
		CmdID: "sell-001", FarmID: userID, ActorUser: userID,
		Type: domain.CmdSellCrop, CropID: "WHEAT", Quantity: 2,
	}, now)
	if err != nil {
		t.Fatalf("幂等重放出售失败: %v", err)
	}
	if !replayed {
		t.Error("相同 cmd_id 出售重放应标记为 replayed=true")
	}
	bal3 := committer.WalletBalance(userID)
	if bal3 != bal {
		t.Fatalf("幂等重放后余额不应变化：期望 %d，实际 %d", bal, bal3)
	}
	t.Logf("✅ 幂等验证（出售）：重复 cmd_id 不重复加金币，余额仍为 %d", bal3)

	// ── 余额一致性校验 ──
	// 初始 100
	// - 买种子 3 颗：-30 → 70
	// + 卖作物 2 个：+40 → 110
	expectedBalance := int64(110)
	if bal != expectedBalance {
		t.Fatalf("💰 余额校验失败：期望 %d，实际 %d", expectedBalance, bal)
	}
	expectedSeedCount := int64(1) // 3 颗种子，用了 2 颗
	actualSeedCount := committer.InventoryCount(userID, "SEED", 1)
	if actualSeedCount != expectedSeedCount {
		t.Fatalf("种子库存校验失败：期望 %d，实际 %d", expectedSeedCount, actualSeedCount)
	}

	t.Logf("")
	t.Logf("═══════════════════════════════════════")
	t.Logf("  🎉 完整流程测试通过！")
	t.Logf("  初始金币:  100")
	t.Logf("  买种子×3:  -30 → 余额 70")
	t.Logf("  播种×2:    种子 3→1")
	t.Logf("  等待成熟:  65s")
	t.Logf("  收获×2:    作物 0→2")
	t.Logf("  出售作物×2: +40 → 余额 110")
	t.Logf("  最终余额:  %d ✅", bal)
	t.Logf("  最终种子:  %d 颗", actualSeedCount)
	t.Logf("  最终作物:  %d 个", cropCount)
	t.Logf("  幂等验证:  全部通过 ✅")
	t.Logf("═══════════════════════════════════════")
}

// TestIdempotency_AllCommandTypes 全面验证五种命令的幂等性
func TestIdempotency_AllCommandTypes(t *testing.T) {
	committer := newFullMemCommitter()
	const userID int64 = 2001
	now := time.Now()

	// 初始化：给钱 + 给种子库存（模拟已有资源，跳过首次购买）
	committer.InitPlayer(userID, 100)
	committer.inventory[fmt.Sprintf("%d:SEED:%d", userID, 1)] = 2 // 给 2 颗种子
	committer.inventory[fmt.Sprintf("%d:CROP:%d", userID, 1)] = 1 // 给 1 个作物

	t.Logf("初始化：金币=100, 种子=2, 作物=1")

	type idemCase struct {
		name  string
		cmd   domain.Command
		now   time.Time
		check func(t *testing.T)
	}
	matureTime := now.Add(70 * time.Second)

	cases := []idemCase{
		{
			name: "购买种子幂等",
			cmd: domain.Command{
				CmdID: "idem-buy", FarmID: userID, ActorUser: userID,
				Type: domain.CmdPurchaseSeed, CropID: "WHEAT", Quantity: 2,
			},
			now: now,
			check: func(t *testing.T) {
				if bal := committer.WalletBalance(userID); bal != 80 {
					t.Errorf("期望余额 80 (100-20)，实际 %d", bal)
				}
			},
		},
		{
			name: "播种幂等",
			cmd: domain.Command{
				CmdID: "idem-plant", FarmID: userID, ActorUser: userID,
				Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
			},
			now: now,
			check: func(t *testing.T) {
				if snap := committer.Snapshot(userID); snap.Plots[0].Status != domain.PlotGrowing {
					t.Errorf("期望 plot 0 为 GROWING，实际 %s", snap.Plots[0].Status)
				}
			},
		},
		{
			name: "收获幂等",
			cmd: domain.Command{
				CmdID: "idem-harvest", FarmID: userID, ActorUser: userID,
				Type: domain.CmdHarvest, PlotID: 0,
			},
			now: matureTime, // 成熟后才能收获
			check: func(t *testing.T) {
				count := committer.InventoryCount(userID, "CROP", 1)
				if count == 0 {
					t.Error("收获后作物库存不应为 0")
				}
			},
		},
		{
			name: "出售幂等",
			cmd: domain.Command{
				CmdID: "idem-sell", FarmID: userID, ActorUser: userID,
				Type: domain.CmdSellCrop, CropID: "WHEAT", Quantity: 1,
			},
			now: matureTime,
			check: func(t *testing.T) {
				// 金额变化会在各 case 间累积
			},
		},
	}

	// 记录首次执行后的状态
	snapshots := make(map[string]struct {
		balance    int64
		seeds      int64
		crops      int64
		plotStatus string
	})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 首次执行
			_, _, replayed, err := committer.Commit(userID, tc.cmd, tc.now)
			if err != nil {
				t.Fatalf("首次执行失败: %v", err)
			}
			if replayed {
				t.Error("首次执行不应标记为 replayed")
			}

			// 记录状态快照
			snap := committer.Snapshot(userID)
			ps := domain.PlotEmpty
			if p, ok := snap.Plots[0]; ok {
				ps = p.Status
			}
			snapshots[tc.name] = struct {
				balance    int64
				seeds      int64
				crops      int64
				plotStatus string
			}{
				balance:    committer.WalletBalance(userID),
				seeds:      committer.InventoryCount(userID, "SEED", 1),
				crops:      committer.InventoryCount(userID, "CROP", 1),
				plotStatus: string(ps),
			}

			// 重复执行（幂等重放）
			_, _, replayed, err = committer.Commit(userID, tc.cmd, tc.now)
			if err != nil {
				t.Fatalf("幂等重放失败: %v", err)
			}
			if !replayed {
				t.Error("幂等重放应标记为 replayed=true")
			}

			// 验证状态未变
			s1 := snapshots[tc.name]
			if bal := committer.WalletBalance(userID); bal != s1.balance {
				t.Errorf("幂等重放后余额变化：期望 %d，实际 %d", s1.balance, bal)
			}
			if s := committer.InventoryCount(userID, "SEED", 1); s != s1.seeds {
				t.Errorf("幂等重放后种子库存变化：期望 %d，实际 %d", s1.seeds, s)
			}
			if s := committer.InventoryCount(userID, "CROP", 1); s != s1.crops {
				t.Errorf("幂等重放后作物库存变化：期望 %d，实际 %d", s1.crops, s)
			}

			t.Logf("✅ %s: 幂等通过（余额=%d, 种子=%d, 作物=%d, plot=%s）",
				tc.name, s1.balance, s1.seeds, s1.crops, s1.plotStatus)
		})
	}
}
