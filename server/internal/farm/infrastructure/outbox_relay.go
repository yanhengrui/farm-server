// Package infrastructure 提供 OutboxRelay：扫描 outbox_events 表中 PENDING 行，
// 发布到消息总线，并标记为 PUBLISHED（失败则重试）。
//
// 锁策略：事务内 SELECT … FOR UPDATE SKIP LOCKED（MySQL 8.0+）原子性地认领一批事件；
// 崩溃 Worker 的 PUBLISHING 行在 locked_until 超时后被回收重试。
// 幂等由 event_id 唯一列保证。
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/id"
)

const (
	defaultTopic     = "farm-events"
	defaultBatchSize = 50
	defaultMaxRetry  = 5
	defaultLockTTL   = 30 * time.Second

	// 每次重试的回退间隔：retry_count * backoffBase
	backoffBase = 5 * time.Second
)

// outboxRow 存放从 PENDING/PUBLISHING 事件行中取出的字段。
type outboxRow struct {
	outboxID     uint64
	eventID      string
	partitionKey string
	eventType    string
	payload      []byte
	retryCount   int
}

// OutboxRelay 扫描 outbox_events 表并将 PENDING 事件发布到消息总线。
type OutboxRelay struct {
	db        *sql.DB
	pub       EventPublisher
	clk       clock.Clock
	log       *slog.Logger
	workerID  string
	batchSize int
	maxRetry  int
	lockTTL   time.Duration
	topic     string
	observer  OutboxObserver
}

type OutboxObserver interface {
	OutboxPublished(int)
	OutboxFailure(string)
}

// OutboxRelayConfig 构造参数；零值使用默认值。
type OutboxRelayConfig struct {
	BatchSize int
	MaxRetry  int
	LockTTL   time.Duration
	Topic     string
}

func (r *OutboxRelay) WithObserver(o OutboxObserver) *OutboxRelay { r.observer = o; return r }

// NewOutboxRelay 创建一个 OutboxRelay。
// workerID 应在每个进程/Pod 中唯一（如宿主机名或 UUIDv7）。
func NewOutboxRelay(db *sql.DB, pub EventPublisher, clk clock.Clock, log *slog.Logger, cfg OutboxRelayConfig) *OutboxRelay {
	r := &OutboxRelay{
		db:        db,
		pub:       pub,
		clk:       clk,
		log:       log,
		workerID:  id.NewV7(),
		batchSize: defaultBatchSize,
		maxRetry:  defaultMaxRetry,
		lockTTL:   defaultLockTTL,
		topic:     defaultTopic,
	}
	if cfg.BatchSize > 0 {
		r.batchSize = cfg.BatchSize
	}
	if cfg.MaxRetry > 0 {
		r.maxRetry = cfg.MaxRetry
	}
	if cfg.LockTTL > 0 {
		r.lockTTL = cfg.LockTTL
	}
	if cfg.Topic != "" {
		r.topic = cfg.Topic
	}
	return r
}

// ScanAndPublish 执行一次 Relay 循环：
//  1. 回收过期 PUBLISHING 行（来自崩溃的 Worker）。
//  2. 认领一批 PENDING 行 → 设为 PUBLISHING。
//  3. 逐条发布到消息总线。
//  4. 成功标记 PUBLISHED，失败则重试或进入 DEAD。
//
// 返回成功发布的事件数量。
func (r *OutboxRelay) ScanAndPublish(ctx context.Context) (int, error) {
	now := r.clk.NowUTC()

	if err := r.reclaimStale(ctx, now); err != nil {
		if r.observer != nil {
			r.observer.OutboxFailure("reclaim")
		}
		return 0, fmt.Errorf("outbox reclaim_stale: %w", err)
	}

	rows, err := r.claimBatch(ctx, now)
	if err != nil {
		if r.observer != nil {
			r.observer.OutboxFailure("claim")
		}
		return 0, fmt.Errorf("outbox claim_batch: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	if batchPublisher, ok := r.pub.(BatchEventPublisher); ok {
		return r.publishClaimedBatch(ctx, batchPublisher, rows, now)
	}

	published := 0
	for _, row := range rows {
		if err := r.publishOne(ctx, row, now); err != nil {
			if r.observer != nil {
				r.observer.OutboxFailure("publish")
			}
			r.log.Warn("outbox publish failed", "event_id", row.eventID, "error", err)
			if ackErr := r.markRetryOrDead(ctx, row, now, err.Error()); ackErr != nil {
				r.log.Error("outbox mark_retry_or_dead failed", "event_id", row.eventID, "error", ackErr)
			}
		} else {
			published++
			if r.observer != nil {
				r.observer.OutboxPublished(1)
			}
		}
	}
	return published, nil
}

func (r *OutboxRelay) publishClaimedBatch(ctx context.Context, publisher BatchEventPublisher, rows []outboxRow, now time.Time) (int, error) {
	messages := make([]MemMessage, len(rows))
	for i, row := range rows {
		messages[i] = MemMessage{Topic: r.topic, PartitionKey: row.partitionKey, Payload: row.payload}
	}
	results := publisher.PublishBatch(ctx, messages)
	if len(results) != len(rows) {
		return 0, fmt.Errorf("outbox publish_batch returned %d results for %d rows", len(results), len(rows))
	}

	succeeded := make([]outboxRow, 0, len(rows))
	for i, publishErr := range results {
		if publishErr == nil {
			succeeded = append(succeeded, rows[i])
			continue
		}
		if r.observer != nil {
			r.observer.OutboxFailure("publish")
		}
		r.log.Warn("outbox publish failed", "event_id", rows[i].eventID, "error", publishErr)
		if ackErr := r.markRetryOrDead(ctx, rows[i], now, publishErr.Error()); ackErr != nil {
			r.log.Error("outbox mark_retry_or_dead failed", "event_id", rows[i].eventID, "error", ackErr)
		}
	}
	if len(succeeded) == 0 {
		return 0, nil
	}
	if err := r.markPublishedBatch(ctx, succeeded, now); err != nil {
		if r.observer != nil {
			r.observer.OutboxFailure("mark_published")
		}
		return 0, fmt.Errorf("outbox mark_published_batch: %w", err)
	}
	if r.observer != nil {
		r.observer.OutboxPublished(len(succeeded))
	}
	return len(succeeded), nil
}

// ── 内部步骤 ────────────────────────────────────────────────────────────

// reclaimStale 将 locked_until 已过期的 PUBLISHING 行重置回 PENDING。
// 用于处理崩溃/卡住的 Worker，避免事件长期卡住。
func (r *OutboxRelay) reclaimStale(ctx context.Context, now time.Time) error {
	const q = `
		UPDATE outbox_events
		SET status = 'PENDING', locked_by = NULL, locked_until = NULL, updated_at = ?
		WHERE status = 'PUBLISHING' AND locked_until < ?`
	_, err := r.db.ExecContext(ctx, q, now, now)
	return err
}

// claimBatch 使用 SKIP LOCKED 选取最多 batchSize 条 PENDING 行，并标记为 PUBLISHING。
// 返回已认领的行用于发布。
func (r *OutboxRelay) claimBatch(ctx context.Context, now time.Time) ([]outboxRow, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin_tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	const sel = `
		SELECT outbox_id, event_id, partition_key, event_type, payload, retry_count
		FROM outbox_events
		WHERE status = 'PENDING' AND available_at <= ?
		ORDER BY outbox_id ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED`
	dbRows, err := tx.QueryContext(ctx, sel, now, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("select_pending: %w", err)
	}
	defer dbRows.Close()

	var batch []outboxRow
	var ids []uint64
	for dbRows.Next() {
		var row outboxRow
		if err := dbRows.Scan(&row.outboxID, &row.eventID, &row.partitionKey, &row.eventType, &row.payload, &row.retryCount); err != nil {
			return nil, fmt.Errorf("scan_row: %w", err)
		}
		batch = append(batch, row)
		ids = append(ids, row.outboxID)
	}
	if err := dbRows.Err(); err != nil {
		return nil, fmt.Errorf("rows_err: %w", err)
	}
	if len(ids) == 0 {
		_ = tx.Commit()
		return nil, nil
	}

	lockedUntil := now.Add(r.lockTTL)
	upd := fmt.Sprintf(`
		UPDATE outbox_events
		SET status = 'PUBLISHING', locked_by = ?, locked_until = ?, updated_at = ?
		WHERE outbox_id IN (%s)`, placeholders(len(ids)))
	args := make([]any, 0, 3+len(ids))
	args = append(args, r.workerID, lockedUntil, now)
	for _, oid := range ids {
		args = append(args, oid)
	}
	if _, err := tx.ExecContext(ctx, upd, args...); err != nil {
		return nil, fmt.Errorf("claim_update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return batch, nil
}

// publishOne 调用 EventPublisher，成功后标记该行为 PUBLISHED。
func (r *OutboxRelay) publishOne(ctx context.Context, row outboxRow, now time.Time) error {
	if err := r.pub.Publish(ctx, r.topic, row.partitionKey, row.payload); err != nil {
		return err
	}
	const upd = `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = ?, locked_by = NULL, locked_until = NULL, updated_at = ?
		WHERE outbox_id = ?`
	_, err := r.db.ExecContext(ctx, upd, now, now, row.outboxID)
	return err
}

func (r *OutboxRelay) markPublishedBatch(ctx context.Context, rows []outboxRow, now time.Time) error {
	ids := make([]any, 0, len(rows)+3)
	ids = append(ids, now, now, r.workerID)
	for _, row := range rows {
		ids = append(ids, row.outboxID)
	}
	query := fmt.Sprintf(`
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = ?, locked_by = NULL, locked_until = NULL, updated_at = ?
		WHERE status = 'PUBLISHING' AND locked_by = ? AND outbox_id IN (%s)`, placeholders(len(rows)))
	result, err := r.db.ExecContext(ctx, query, ids...)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != int64(len(rows)) {
		return fmt.Errorf("updated %d of %d claimed rows", updated, len(rows))
	}
	return nil
}

// markRetryOrDead 更新失败的行：递增 retry_count；超过上限则进入 DEAD，
// 否则通过 available_at 设置回退时间后重新排队 PENDING。
func (r *OutboxRelay) markRetryOrDead(ctx context.Context, row outboxRow, now time.Time, lastErr string) error {
	nextCount := row.retryCount + 1
	if nextCount > r.maxRetry {
		const dead = `
			UPDATE outbox_events
			SET status = 'DEAD', retry_count = ?, last_error = ?, locked_by = NULL, locked_until = NULL, updated_at = ?
			WHERE outbox_id = ?`
		_, err := r.db.ExecContext(ctx, dead, nextCount, lastErr, now, row.outboxID)
		return err
	}
	backoff := time.Duration(nextCount) * backoffBase
	availableAt := now.Add(backoff)
	const retry = `
		UPDATE outbox_events
		SET status = 'PENDING', retry_count = ?, last_error = ?, available_at = ?,
		    locked_by = NULL, locked_until = NULL, updated_at = ?
		WHERE outbox_id = ?`
	_, err := r.db.ExecContext(ctx, retry, nextCount, lastErr, availableAt, now, row.outboxID)
	return err
}

// placeholders 返回 n 个 "?" 占位符拼接字符串。
func placeholders(n int) string {
	if n == 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}
