// Package farmrpc 提供 farmsvr → gamesvr 的 HTTP/JSON RPC 传输层契约。
// farmsvr 作为 RPC 客户端调用 gamesvr（Server），共享本包 types。
// 待 proto 环境具备后可替换为 gRPC 实现，接口契约（application.Committer/SnapshotLoader）不变。
package farmrpc

import (
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

// CommitReq 是 /rpc/farm/commit 请求体。
type CommitReq struct {
	Command domain.Command `json:"command"`
}

// CommitResp 是 /rpc/farm/commit 响应体。
type CommitResp struct {
	Result *application.CommitResult `json:"result,omitempty"`
	Err    *RPCErr                   `json:"error,omitempty"`
}

// LoadSnapshotReq 是 /rpc/farm/load-snapshot 请求体。
type LoadSnapshotReq struct {
	FarmID int64 `json:"farm_id"`
}

// LoadSnapshotResp 是 /rpc/farm/load-snapshot 响应体。
type LoadSnapshotResp struct {
	Snapshot *domain.Snapshot `json:"snapshot,omitempty"`
	Err      *RPCErr          `json:"error,omitempty"`
}

type AdvanceRouteEpochReq struct {
	FarmID     int64 `json:"farm_id"`
	RouteEpoch int64 `json:"route_epoch"`
}

type AdvanceRouteEpochResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// GetSnapshotReq 是 /rpc/farm/get-snapshot 请求体。
type GetSnapshotReq struct {
	FarmID int64 `json:"farm_id"`
}

// GetSnapshotResp 是 /rpc/farm/get-snapshot 响应体（含惰性推导的 GrowthStage）。
type GetSnapshotResp struct {
	Snapshot *FarmSnapshotDTO `json:"snapshot,omitempty"`
	Err      *RPCErr          `json:"error,omitempty"`
}

// FarmSnapshotDTO is the public farm state. Private player assets are exposed
// only through /api/v1/player/assets.
type FarmSnapshotDTO struct {
	FarmID           int64     `json:"farm_id,string"`
	OwnerUserID      int64     `json:"owner_user_id,string"`
	OwnerDisplayName string    `json:"owner_display_name"`
	Version          int64     `json:"version,string"`
	Plots            []PlotDTO `json:"plots"`
}

// PlotDTO 是单地块的客户端视图。
type PlotDTO struct {
	PlotID         int32  `json:"plot_id"`
	Status         string `json:"status"` // "EMPTY" | "GROWING"
	CropID         string `json:"crop_id,omitempty"`
	GrowthStage    string `json:"growth_stage,omitempty"` // "SEEDLING"|"SEMI_MATURE"|"MATURE"
	PlantedAt      string `json:"planted_at,omitempty"`   // RFC3339
	MatureAt       string `json:"mature_at,omitempty"`    // RFC3339
	RemainingYield int64  `json:"remaining_yield"`
}

// RPCErr 是跨服务稳定错误描述；Code 对应 errcode.Code。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}

const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(rfc3339Milli)
}
