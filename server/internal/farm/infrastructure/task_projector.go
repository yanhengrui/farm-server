// Package infrastructure 提供任务投影器 TaskProjector。
// TaskProjector 消费农场事件，通过 taskrpc.Client 推进玩家任务进度。
package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	taskinfra "github.com/photon/farm-server/server/internal/task/infrastructure"
	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
)

// TaskProjector 消费农场事件，推进对应任务的进度。
// 实现 farm/infrastructure.EventProjector 接口。
type TaskProjector struct {
	task *taskrpc.Client
	log  *slog.Logger
}

// NewTaskProjector 构造 TaskProjector。
func NewTaskProjector(task *taskrpc.Client, log *slog.Logger) *TaskProjector {
	return &TaskProjector{task: task, log: log}
}

// Name 返回消费者去重名称。
func (p *TaskProjector) Name() string { return "task-projector" }

// Handle 根据事件类型推进任务进度。
func (p *TaskProjector) Handle(ctx context.Context, env farmevents.EventEnvelope) error {
	// HelpWater 事件与普通 Water 共用 farm.watered.v1，通过 ActorUser != OwnerUser 区分。
	taskKeys := taskinfra.TaskKeysByEvent(string(env.EventType))
	if len(taskKeys) == 0 {
		// 检查是否是 HelpWater（farm.watered.v1 且 actor != owner）。
		if env.EventType == farmevents.EventTypeFarmWatered {
			taskKeys = p.helpWaterKeys(env)
		}
		if len(taskKeys) == 0 {
			return nil
		}
	}

	// 从 payload 提取 ActorUserID。
	actorID, err := extractActorUserID(env)
	if err != nil {
		p.log.Warn("task projector: cannot extract actor",
			slog.String("event_id", env.EventID),
			slog.String("error", err.Error()),
		)
		return nil // 降级，不影响事件处理
	}

	for _, key := range taskKeys {
		if err := p.task.IncrProgress(ctx, actorID, key, 1); err != nil {
			p.log.Error("task incr progress failed",
				slog.String("event_id", env.EventID),
				slog.String("task_key", key),
				slog.Int64("actor_id", actorID),
				slog.String("error", err.Error()),
			)
			return fmt.Errorf("incr progress %s: %w", key, err)
		}
	}
	return nil
}

// helpWaterKeys 判断浇水事件是否来自好友（actor != owner），返回 help_water 相关任务。
func (p *TaskProjector) helpWaterKeys(env farmevents.EventEnvelope) []string {
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return nil
	}
	var payload farmevents.FarmWateredPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	// 好友浇水：actor != owner
	if payload.ActorUserID != payload.OwnerUserID {
		return taskinfra.TaskKeysByEvent("farm.watered.v1_help")
	}
	return nil
}

// extractActorUserID 从事件 payload 提取操作者 user_id。
func extractActorUserID(env farmevents.EventEnvelope) (int64, error) {
	raw, err := json.Marshal(env.Payload)
	if err != nil {
		return 0, err
	}
	// 所有 farm 事件 payload 都有 actor_user_id 字段。
	var generic struct {
		ActorUserID string `json:"actor_user_id"`
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return 0, err
	}
	if generic.ActorUserID == "" {
		return 0, fmt.Errorf("actor_user_id empty")
	}
	return strconv.ParseInt(generic.ActorUserID, 10, 64)
}
