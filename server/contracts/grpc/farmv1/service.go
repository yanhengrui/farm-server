// Package farmv1 定义 FarmService 的 Go 接口契约。
// 对应 proto/farm/v1，farmsvr → gamesvr 内部 gRPC 调用。
// farmsvr 只做串行预校验和路由；gamesvr 在单 MySQL 事务中提交权威事实。
package farmv1

import (
	"context"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

// PlotSnapshot 是单个地块的快照，供客户端渲染三态模型。
type PlotSnapshot struct {
	PlotID         int32
	State          string                 // "EMPTY" | "PLANTED"（JSON 序列化值）
	CropID         string                 // State=="PLANTED" 时有值
	CropCycle      int32                  // 播种递增；用于收获幂等键
	PlantedAt      time.Time              // UTC
	MatureAt       time.Time              // UTC；服务端计算，客户端不传
	GrowthStage    domain.CropGrowthStage // 惰性推导的客户端三态
	WateredCount   int32
	RemainingYield int64
	PlotVersion    int64
}

// FarmSnapshot 是农场完整快照。
type FarmSnapshot struct {
	FarmID           int64
	OwnerUserID      int64
	OwnerDisplayName string
	Version          int64
	Plots            []PlotSnapshot
	UpdatedAt        time.Time
}

// GetSnapshotRequest 获取农场快照请求。
type GetSnapshotRequest struct {
	FarmID int64
}

// GetSnapshotResponse 获取农场快照响应。
type GetSnapshotResponse struct {
	Snapshot FarmSnapshot
}

// FarmService 是 gamesvr 实现、farmsvr 调用的农场服务接口。
// CommitFarmCommand 已在 application 包定义（application.Committer）；
// 此处仅补充 gamesvr 向 farmsvr 暴露的其他查询接口。
type FarmService interface {
	// GetSnapshot 返回农场当前快照，含基于服务器时间推导的地块三态。
	GetSnapshot(ctx context.Context, req GetSnapshotRequest) (GetSnapshotResponse, error)

	// CommitFarmCommand 见 application.Committer；此处声明以统一服务边界。
	application.Committer

	// LoadSnapshot 见 application.SnapshotLoader；farmsvr Actor 冷激活时使用。
	application.SnapshotLoader
}
