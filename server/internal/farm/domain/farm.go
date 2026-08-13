// Package domain 定义农场聚合的核心实体、值对象与领域规则。
// 不依赖 HTTP/gRPC/WS/MySQL/Redis/Kafka；见依赖方向约束 06-4.1。
package domain

import "time"

// PlotStatus 地块状态（服务端内部）。
// 快照 JSON 中 GROWING 序列化为 "PLANTED"；MATURE 为惰性推导结果，不持久化。
type PlotStatus string

const (
	PlotEmpty   PlotStatus = "EMPTY"
	PlotGrowing PlotStatus = "GROWING" // JSON 序列化为 "PLANTED"
	PlotMature  PlotStatus = "MATURE"  // 惰性推导，不持久化
)

// CropGrowthStage 是客户端三态模型形态，对应三种视觉状态。
// 由 planted_at 和 mature_at 惰性推导，不落库，见 ADR-013。
type CropGrowthStage string

const (
	// CropGrowthStageSeedling 幼苗：播种后不足成长时长的 50%。
	CropGrowthStageSeedling CropGrowthStage = "SEEDLING"
	// CropGrowthStageSemiMature 半成熟：已过成长时长 50% 但尚未到达 mature_at。
	CropGrowthStageSemiMature CropGrowthStage = "SEMI_MATURE"
	// CropGrowthStageMature 成熟：server_now >= mature_at，可以收获。
	CropGrowthStageMature CropGrowthStage = "MATURE"
)

// Plot 是农场地块。作物成长只存 PlantedAt/MatureAt，访问时惰性计算，
// 禁止为每地块创建常驻定时器（见 ADR-013）。
// WateredCount 记录本次生长周期已浇水次数（最多 2 次有效，每阶段各 1 次）。
// RemainingYield 在播种时初始化为作物总产量。偷菜只扣减该值，并至少为农场主
// 保留 1 份；只有农场主手动收获或宠物自动收获才清空地块并将其归零。
type Plot struct {
	PlotID         int32
	CropID         string
	Status         PlotStatus
	PlantedAt      time.Time
	MatureAt       time.Time
	WateredCount   int // 已浇水次数，最多 2 次缩短 mature_at
	RemainingYield int64
}

// EffectiveStatus 用服务端权威时间惰性推导地块阶段。
func (p Plot) EffectiveStatus(nowUTC time.Time) PlotStatus {
	if p.Status == PlotGrowing && !p.MatureAt.IsZero() && !nowUTC.Before(p.MatureAt) {
		return PlotMature
	}
	return p.Status
}

// GrowthStage 推导客户端三态模型：幼苗 → 半成熟 → 成熟。
// 仅对 PlotGrowing/PlotMature 有意义；空地块返回 CropGrowthStageSeedling（调用方应先
// 检查 Status == PlotEmpty）。
// 阶段划分：
//
//	[0%, 50%) → SEEDLING
//	[50%, 100%) → SEMI_MATURE
//	[100%, ∞) → MATURE（server_now >= mature_at）
func (p Plot) GrowthStage(nowUTC time.Time) CropGrowthStage {
	if p.Status == PlotEmpty {
		return CropGrowthStageSeedling
	}
	if p.MatureAt.IsZero() || !nowUTC.Before(p.MatureAt) {
		return CropGrowthStageMature
	}
	total := p.MatureAt.Sub(p.PlantedAt)
	if total <= 0 {
		return CropGrowthStageMature
	}
	elapsed := nowUTC.Sub(p.PlantedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	// 过半成长时长进入半成熟。
	if elapsed*2 >= total {
		return CropGrowthStageSemiMature
	}
	return CropGrowthStageSeedling
}

// Snapshot 是农场版本化快照，一农场一聚合，按 farm_id 单写者串行修改。
type Snapshot struct {
	FarmID     int64
	OwnerID    int64
	Version    int64
	RouteEpoch int64
	Plots      map[int32]Plot
}

// Patch 是广播给同农场在线成员的增量变化，只含变化字段。
type Patch struct {
	FarmID    int64
	Version   int64
	Plots     []Plot
	ActorUser int64
}

// CommandType 农场命令类型。
type CommandType string

const (
	CmdPlant          CommandType = "PLANT"
	CmdWater          CommandType = "WATER"
	CmdHarvest        CommandType = "HARVEST"
	CmdPurchaseSeed   CommandType = "PURCHASE_SEED"    // 购买种子（扣金币 + 加库存）
	CmdSellCrop       CommandType = "SELL_CROP"        // 出售作物（扣库存 + 加金币）
	CmdHelpWater      CommandType = "HELP_WATER"       // 好友代浇水（ActorUser ≠ FarmID 所有者，需好友关系）
	CmdStealCrop      CommandType = "STEAL_CROP"       // 好友偷菜（好友农场成熟地块，收益归 ActorUser）
	CmdPetAutoHarvest CommandType = "PET_AUTO_HARVEST" // 宠物自动收割（workersvr 发起，按 PlotID 升序收最小成熟地）
)

// MaxEconomyQuantity 是单条经济命令允许的最大数量。
//
// 上限存在的原因是权威事务里 `单价 × Quantity` 用 int64 计算：没有上限时一个
// 极大 Quantity 会让乘法溢出为负数，从而绕过 `balance < price` 的余额检查。
// 该常量属于领域规则，网关与权威提交层都必须校验，不能只靠客户端不发大数。
//
// 取值 10000 远高于任何正常连点合并场景，同时让 单价×数量 距 int64 上限有极大余量。
const MaxEconomyQuantity int64 = 10000

// Command 是进入 Farm Actor 邮箱串行执行的领域命令。
// CmdID 负责业务幂等；BaseVersion 用于乐观版本校验。
// Quantity 供经济类命令（PurchaseSeed/SellCrop）携带数量；农场命令忽略该字段。
type Command struct {
	CmdID       string
	FarmID      int64
	ActorUser   int64
	Type        CommandType
	PlotID      int32
	CropID      string
	Quantity    int64
	BaseVersion int64
	RouteEpoch  int64
	// PetScheduledAt 仅供 CmdPetAutoHarvest 使用，是扫描器看到的持久化
	// next_pet_action_at。权威事务必须校验它仍匹配且已经到期。
	PetScheduledAt time.Time
}
