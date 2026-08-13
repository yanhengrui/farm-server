// Package infrastructure 提供 RetentionReaper：按保留期分批清理已终结的异步链路记录。
//
// 清理范围（路线 10.2）：
//   - outbox_events 中 status='PUBLISHED' 的行（已投递事实，Kafka 已承接传播）
//   - cmd_receipts（短期响应幂等，保留期只覆盖协议重试窗口）
//   - consumed_events（消费者去重键，保留期覆盖 Kafka topic retention）
//
// 明确不清理：
//   - status='DEAD' 的 outbox 行。CONSTITUTION 要求 DEAD 先告警、审计并支持人工重放，
//     不得由自动清理删除。
//   - economy_transactions。它是权威幂等键与经济账本，02-数据模型 §10 规定不做业务
//     软删除，只能按审计策略归档到冷存储后分区清理（归路线 10.3）。
//
// 删除策略：每个表按主键升序小批量 DELETE，单次 tick 有总量上限，避免长事务与
// 大范围行锁影响权威写路径（02-数据模型 §10：清理任务必须按索引小批量执行并限速）。
package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/photon/farm-server/server/pkg/clock"
)

const (
	defaultReapBatchSize = 500
	defaultReapMaxPerRun = 20000
)

// RetentionPolicy 定义各表的保留期。零值表示跳过该表，便于按环境分别启用。
type RetentionPolicy struct {
	// PublishedOutbox 是 status='PUBLISHED' 行从 published_at 起的保留期。
	PublishedOutbox time.Duration
	// CmdReceipts 是命令响应幂等记录从 created_at 起的保留期。
	// 下界由客户端协议重试窗口决定，不得短于 ACK 丢失后允许重试的时间。
	CmdReceipts time.Duration
	// ConsumedEvents 是消费者去重键从 processed_at 起的保留期。
	// 下界由 Kafka topic retention 决定：只要消息还可能被重投，去重键就必须在。
	ConsumedEvents time.Duration
}

// Enabled 表示至少有一个表配置了保留期。
func (p RetentionPolicy) Enabled() bool {
	return p.PublishedOutbox > 0 || p.CmdReceipts > 0 || p.ConsumedEvents > 0
}

// RetentionReaperConfig 构造参数；零值使用默认值。
type RetentionReaperConfig struct {
	Policy RetentionPolicy
	// BatchSize 是单条 DELETE 的行数上限。
	BatchSize int
	// MaxPerRun 是单次 Reap 在单个表上删除的行数上限，防止积压时长时间占用连接。
	MaxPerRun int
}

// RetentionObserver 上报清理结果。table 为低基数标签（outbox_events/cmd_receipts/
// consumed_events），stage 用于区分失败位置。
type RetentionObserver interface {
	RetentionDeleted(table string, n int)
	RetentionFailure(table string)
}

// RetentionReaper 周期性删除已过保留期的终结态记录。
type RetentionReaper struct {
	db        *sql.DB
	clk       clock.Clock
	log       *slog.Logger
	policy    RetentionPolicy
	batchSize int
	maxPerRun int
	observer  RetentionObserver
}

func (r *RetentionReaper) WithObserver(o RetentionObserver) *RetentionReaper {
	r.observer = o
	return r
}

// NewRetentionReaper 创建一个 RetentionReaper。
func NewRetentionReaper(db *sql.DB, clk clock.Clock, log *slog.Logger, cfg RetentionReaperConfig) *RetentionReaper {
	r := &RetentionReaper{
		db:        db,
		clk:       clk,
		log:       log,
		policy:    cfg.Policy,
		batchSize: defaultReapBatchSize,
		maxPerRun: defaultReapMaxPerRun,
	}
	if cfg.BatchSize > 0 {
		r.batchSize = cfg.BatchSize
	}
	if cfg.MaxPerRun > 0 {
		r.maxPerRun = cfg.MaxPerRun
	}
	return r
}

// Reap 执行一轮清理，返回删除总行数。
// 单个表失败不阻塞其余表：记录 metric 与日志后继续，错误在最后聚合返回。
func (r *RetentionReaper) Reap(ctx context.Context) (int, error) {
	now := r.clk.NowUTC()
	total := 0
	var errs []error

	for _, spec := range r.specs() {
		if spec.retain <= 0 {
			continue
		}
		n, err := r.reapTable(ctx, spec.table, spec.query, now.Add(-spec.retain))
		total += n
		if err != nil {
			errs = append(errs, err)
		}
	}

	return total, errors.Join(errs...)
}

// tableSpec 描述一张表的清理方式。
type tableSpec struct {
	table  string
	query  string
	retain time.Duration
}

// specs 返回清理目标。每条 DELETE 按主键升序并带 LIMIT，配合外层循环分批推进。
func (r *RetentionReaper) specs() []tableSpec {
	return []tableSpec{
		{
			// 已发布的 outbox 行：复用 idx_outbox_dispatch(status, available_at, outbox_id)
			// 的 status 前缀定位，不为最热的写表新增索引。published_at 作为回表过滤条件。
			// DEAD 不在此范围内（status 显式限定 PUBLISHED）。
			table: "outbox_events",
			query: `
				DELETE FROM outbox_events
				WHERE status = 'PUBLISHED' AND published_at IS NOT NULL AND published_at < ?
				ORDER BY outbox_id ASC
				LIMIT ?`,
			retain: r.policy.PublishedOutbox,
		},
		{
			table: "cmd_receipts",
			query: `
				DELETE FROM cmd_receipts
				WHERE created_at < ?
				ORDER BY receipt_id ASC
				LIMIT ?`,
			retain: r.policy.CmdReceipts,
		},
		{
			table: "consumed_events",
			query: `
				DELETE FROM consumed_events
				WHERE processed_at < ?
				ORDER BY consumed_id ASC
				LIMIT ?`,
			retain: r.policy.ConsumedEvents,
		},
	}
}

// reapTable 按主键升序分批删除，直到无更多行、达到 maxPerRun 上限或 ctx 取消。
// 每批是独立自动提交事务，不持有跨批锁。
func (r *RetentionReaper) reapTable(ctx context.Context, table, query string, cutoff time.Time) (int, error) {
	deleted := 0
	for deleted < r.maxPerRun {
		if err := ctx.Err(); err != nil {
			return deleted, nil // shutdown：已删除的部分有效，下轮继续
		}
		limit := r.batchSize
		if remaining := r.maxPerRun - deleted; remaining < limit {
			limit = remaining
		}
		res, err := r.db.ExecContext(ctx, query, cutoff, limit)
		if err != nil {
			if r.observer != nil {
				r.observer.RetentionFailure(table)
			}
			return deleted, fmt.Errorf("retention reap %s: %w", table, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			if r.observer != nil {
				r.observer.RetentionFailure(table)
			}
			return deleted, fmt.Errorf("retention reap %s rows_affected: %w", table, err)
		}
		deleted += int(n)
		if int(n) < limit {
			break // 该表已无过期行
		}
	}
	if deleted > 0 {
		if r.observer != nil {
			r.observer.RetentionDeleted(table, deleted)
		}
		r.log.Info("retention reaped",
			slog.String("table", table),
			slog.Int("deleted", deleted),
			slog.Time("cutoff", cutoff),
		)
	}
	return deleted, nil
}
