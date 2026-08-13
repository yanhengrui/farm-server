// Package infrastructure 提供 task 领域的 MySQL 实现。
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	taskdomain "github.com/photon/farm-server/server/internal/task/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/mysqlretry"
)

// taskConfigs 硬编码任务配置，P0 四个任务。
var taskConfigs = map[string]taskdomain.TaskConfig{
	"plant_10": {
		TaskKey:     "plant_10",
		Description: "累计播种 10 次",
		TargetEvent: "farm.planted.v1",
		Target:      10,
		CoinReward:  50,
	},
	"harvest_5": {
		TaskKey:     "harvest_5",
		Description: "累计收获 5 次",
		TargetEvent: "farm.harvested.v1",
		Target:      5,
		CoinReward:  80,
	},
	"steal_1": {
		TaskKey:     "steal_1",
		Description: "偷菜 1 次",
		TargetEvent: "farm.stolen.v1",
		Target:      1,
		CoinReward:  30,
	},
	"help_water_3": {
		TaskKey:     "help_water_3",
		Description: "帮好友浇水 3 次",
		TargetEvent: "farm.watered.v1_help", // HelpWater 专用虚拟事件 key
		Target:      3,
		CoinReward:  40,
	},
}

// GetTaskConfig 返回任务配置；未知 task_key 返回 false。
func GetTaskConfig(taskKey string) (taskdomain.TaskConfig, bool) {
	cfg, ok := taskConfigs[taskKey]
	return cfg, ok
}

// TaskKeysByEvent 返回与某个事件类型对应的所有 task_key 列表。
func TaskKeysByEvent(eventType string) []string {
	var keys []string
	for k, cfg := range taskConfigs {
		if cfg.TargetEvent == eventType {
			keys = append(keys, k)
		}
	}
	return keys
}

// MySQLTaskService 实现 task/domain.TaskService。
type MySQLTaskService struct {
	db *sql.DB
}

// NewMySQLTaskService 构造 MySQLTaskService。
func NewMySQLTaskService(db *sql.DB) *MySQLTaskService {
	return &MySQLTaskService{db: db}
}

// IncrProgress 为 userID 的 taskKey 任务增加 delta 进度（仅 ACTIVE 状态有效）。
func (s *MySQLTaskService) IncrProgress(ctx context.Context, userID int64, taskKey string, delta int) error {
	cfg, ok := taskConfigs[taskKey]
	if !ok {
		return nil // 未知 task_key 静默跳过
	}
	now := time.Now().UTC()

	// INSERT OR UPDATE：首次触发自动创建行，已 COMPLETED/REWARD_CLAIMED 时 WHERE 条件不匹配。
	const q = `
		INSERT INTO player_tasks (user_id, task_key, progress, status, created_at, updated_at)
		VALUES (?, ?, ?, 'ACTIVE', ?, ?)
		ON DUPLICATE KEY UPDATE
			progress  = IF(status = 'ACTIVE', LEAST(progress + ?, ?), progress),
			status    = IF(status = 'ACTIVE' AND progress >= ?, 'COMPLETED', status),
			updated_at = IF(status = 'ACTIVE', VALUES(updated_at), updated_at)
	`
	_, err := s.db.ExecContext(ctx, q,
		uint64(userID), taskKey, delta, now, now, // INSERT 初始值
		delta, cfg.Target, // ON DUPLICATE: LEAST(progress+delta, target)
		cfg.Target, // ON DUPLICATE: 状态翻转阈值
	)
	return err
}

// ListTasks 返回用户所有任务进度，同时补齐未开始的任务（进度=0，状态=ACTIVE）。
func (s *MySQLTaskService) ListTasks(ctx context.Context, userID int64) ([]taskdomain.TaskProgress, error) {
	const q = `
		SELECT task_id, task_key, progress, status, created_at, updated_at
		FROM player_tasks
		WHERE user_id = ?
		ORDER BY task_id
	`
	rows, err := s.db.QueryContext(ctx, q, uint64(userID))
	if err != nil {
		return nil, fmt.Errorf("list_tasks: %w", err)
	}
	defer rows.Close()

	found := make(map[string]taskdomain.TaskProgress)
	for rows.Next() {
		var tp taskdomain.TaskProgress
		tp.UserID = userID
		if err := rows.Scan(&tp.TaskID, &tp.TaskKey, &tp.Progress, &tp.Status, &tp.CreatedAt, &tp.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan_task: %w", err)
		}
		found[tp.TaskKey] = tp
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 补齐未开始的任务（前端可显示任务列表完整性）。
	result := make([]taskdomain.TaskProgress, 0, len(taskConfigs))
	now := time.Now().UTC()
	for key := range taskConfigs {
		if tp, ok := found[key]; ok {
			result = append(result, tp)
		} else {
			result = append(result, taskdomain.TaskProgress{
				UserID:    userID,
				TaskKey:   key,
				Progress:  0,
				Status:    taskdomain.TaskStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			})
		}
	}
	return result, nil
}

// ClaimReward 领取任务奖励：COMPLETED→REWARD_CLAIMED + 写钱包。
func (s *MySQLTaskService) ClaimReward(ctx context.Context, userID int64, taskKey string) (int64, error) {
	cfg, ok := taskConfigs[taskKey]
	if !ok {
		return 0, errcode.New(errcode.CommonInvalidArgument, "unknown task_key: "+taskKey)
	}
	return mysqlretry.Value(ctx, mysqlretry.IsTransient, func() (int64, error) {
		return s.claimRewardOnce(ctx, userID, taskKey, cfg)
	})
}

func (s *MySQLTaskService) claimRewardOnce(ctx context.Context, userID int64, taskKey string, cfg taskdomain.TaskConfig) (int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin_tx claim_reward: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()

	// 1. 原子翻转状态：COMPLETED → REWARD_CLAIMED；UPDATE 持有 claim 行锁。
	const qUpdate = `
		UPDATE player_tasks
		SET status = 'REWARD_CLAIMED', updated_at = ?
		WHERE user_id = ? AND task_key = ? AND status = 'COMPLETED'
	`
	res, err := tx.ExecContext(ctx, qUpdate, now, uint64(userID), taskKey)
	if err != nil {
		return 0, fmt.Errorf("claim_reward update: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		// 检查是否已领取
		var status string
		_ = tx.QueryRowContext(ctx,
			`SELECT status FROM player_tasks WHERE user_id=? AND task_key=? LIMIT 1`,
			uint64(userID), taskKey,
		).Scan(&status)
		if status == "REWARD_CLAIMED" {
			return 0, errcode.New(errcode.TaskAlreadyClaimed, "reward already claimed")
		}
		return 0, errcode.New(errcode.TaskNotCompleted, "task not completed")
	}

	// 2. 金币奖励入账。
	if cfg.CoinReward > 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE wallets SET coin_balance = coin_balance + ?, updated_at = ? WHERE user_id = ?`,
			cfg.CoinReward, now, uint64(userID),
		); err != nil {
			return 0, fmt.Errorf("claim_reward wallet: %w", err)
		}
	}

	return cfg.CoinReward, tx.Commit()
}
