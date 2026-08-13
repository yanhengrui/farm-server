// Package domain 定义任务/图鉴系统的核心实体与服务接口。
package domain

import (
	"context"
	"time"
)

// TaskStatus 任务状态。
type TaskStatus string

const (
	TaskStatusActive        TaskStatus = "ACTIVE"
	TaskStatusCompleted     TaskStatus = "COMPLETED"
	TaskStatusRewardClaimed TaskStatus = "REWARD_CLAIMED"
)

// TaskConfig 任务配置（硬编码，P0）。
type TaskConfig struct {
	TaskKey     string // 唯一标识
	Description string // 任务描述
	TargetEvent string // 触发进度的事件类型（farm.*）
	Target      int    // 完成所需进度
	CoinReward  int64  // 完成奖励金币
}

// TaskProgress 玩家任务进度。
type TaskProgress struct {
	TaskID    int64
	UserID    int64
	TaskKey   string
	Progress  int
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TaskService 是任务系统的领域服务接口，由 gamesvr 实现。
type TaskService interface {
	// IncrProgress 为 userID 的 taskKey 任务增加 delta 进度（仅 ACTIVE 状态有效）。
	// 若任务不存在则自动创建（进度从 0 开始）；达到目标后状态变 COMPLETED。
	// 幂等：已 COMPLETED/REWARD_CLAIMED 时静默跳过。
	IncrProgress(ctx context.Context, userID int64, taskKey string, delta int) error
	// ListTasks 返回用户所有任务进度（含所有状态）。
	ListTasks(ctx context.Context, userID int64) ([]TaskProgress, error)
	// ClaimReward 领取任务奖励：COMPLETED→REWARD_CLAIMED + 写钱包。
	// 幂等：已领取时返回 TaskAlreadyClaimed 错误。
	ClaimReward(ctx context.Context, userID int64, taskKey string) (coinReward int64, err error)
}
