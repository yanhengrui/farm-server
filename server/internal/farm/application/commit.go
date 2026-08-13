// Package application 编排农场命令用例，声明 farmsvr 依赖的端口接口。
// CommitFarmCommand 是 farmsvr -> gamesvr 的关键同步提交契约（见 ADR-017）：
// farmsvr 只做串行预校验，gamesvr 在单 MySQL 事务中提交全部权威事实。
package application

import (
	"context"

	"github.com/photon/farm-server/server/internal/farm/domain"
)

// CommitRequest 是 farmsvr 传给 gamesvr 的已预校验命令。
type CommitRequest struct {
	Command domain.Command
}

// CommitResult 是 gamesvr 单事务提交成功后的权威结果。
type CommitResult struct {
	NewVersion  int64
	Patch       domain.Patch
	EventID     string // outbox 事件 ID；用于事件去重/关联，不得代替 trace_id
	Replayed    bool   // true 表示命中持久化幂等凭据，未重复结算
	CoinBalance int64  // 经济类命令执行后的最新金币余额（0 表示未填写）
}

// Committer 由 gamesvr 实现：在一笔 MySQL 事务内按 farm → claim → wallet →
// inventory 的统一锁顺序写入权威表；不涉及的锁节点直接跳过。
// gamesvr 必须在事务内重新校验关键前置条件，不能仅信任 farmsvr 预校验。
type Committer interface {
	CommitFarmCommand(ctx context.Context, req CommitRequest) (CommitResult, error)
}

// SnapshotLoader 由 gamesvr 实现：farmsvr 冷激活 Actor 时加载已提交快照。
type SnapshotLoader interface {
	LoadSnapshot(ctx context.Context, farmID int64) (domain.Snapshot, error)
}

// RouteFencer persists a new farmsvr ownership epoch before a cold actor is
// allowed to submit commands. The gamesvr implementation performs this update
// under a MySQL row lock; stale epochs are never allowed to move it backwards.
type RouteFencer interface {
	AdvanceRouteEpoch(ctx context.Context, farmID, epoch int64) error
}
