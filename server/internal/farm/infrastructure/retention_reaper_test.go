package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// newReaperWithMock 创建一个由 sqlmock 支撑的 RetentionReaper。
func newReaperWithMock(t *testing.T, cfg RetentionReaperConfig) (*RetentionReaper, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewRetentionReaper(db, fixedClock{testNow}, testLog, cfg), mock
}

// countingObserver 记录每张表的删除行数与失败次数。
type countingObserver struct {
	deleted  map[string]int
	failures map[string]int
}

func newCountingObserver() *countingObserver {
	return &countingObserver{deleted: map[string]int{}, failures: map[string]int{}}
}

func (o *countingObserver) RetentionDeleted(table string, n int) { o.deleted[table] += n }
func (o *countingObserver) RetentionFailure(table string)        { o.failures[table]++ }

var _ RetentionObserver = (*countingObserver)(nil)

func TestRetentionPolicy_Enabled(t *testing.T) {
	if (RetentionPolicy{}).Enabled() {
		t.Fatal("零值 policy 应视为未启用")
	}
	if !(RetentionPolicy{CmdReceipts: time.Hour}).Enabled() {
		t.Fatal("单个表配置保留期即应启用")
	}
}

// 保留期为 0 的表必须完全跳过，不产生任何 SQL。
func TestReap_ZeroRetentionSkipsTable(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy: RetentionPolicy{CmdReceipts: 72 * time.Hour},
	})
	mock.ExpectExec("DELETE FROM cmd_receipts").
		WithArgs(testNow.Add(-72*time.Hour), defaultReapBatchSize).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := r.Reap(context.Background()); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	// outbox_events 与 consumed_events 未配置，不应有对应 Exec。
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("未配置保留期的表不应产生 SQL: %v", err)
	}
}

// 删除行数少于 batch 上限说明该表已清空，应立即停止而不是继续空转。
func TestReap_StopsWhenBatchNotFull(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy:    RetentionPolicy{CmdReceipts: time.Hour},
		BatchSize: 100,
	})
	mock.ExpectExec("DELETE FROM cmd_receipts").
		WithArgs(testNow.Add(-time.Hour), 100).
		WillReturnResult(sqlmock.NewResult(0, 30))

	n, err := r.Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 30 {
		t.Fatalf("删除行数 = %d，期望 30", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("批次未满时应停止: %v", err)
	}
}

// 批次满则继续下一批，直到某批未满。
func TestReap_ContinuesWhileBatchFull(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy:    RetentionPolicy{ConsumedEvents: time.Hour},
		BatchSize: 10,
	})
	cutoff := testNow.Add(-time.Hour)
	mock.ExpectExec("DELETE FROM consumed_events").WithArgs(cutoff, 10).
		WillReturnResult(sqlmock.NewResult(0, 10))
	mock.ExpectExec("DELETE FROM consumed_events").WithArgs(cutoff, 10).
		WillReturnResult(sqlmock.NewResult(0, 10))
	mock.ExpectExec("DELETE FROM consumed_events").WithArgs(cutoff, 10).
		WillReturnResult(sqlmock.NewResult(0, 4))

	n, err := r.Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 24 {
		t.Fatalf("删除行数 = %d，期望 24", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// maxPerRun 是单轮硬上限，防止积压时长时间占用连接。
func TestReap_RespectsMaxPerRun(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy:    RetentionPolicy{CmdReceipts: time.Hour},
		BatchSize: 10,
		MaxPerRun: 25,
	})
	cutoff := testNow.Add(-time.Hour)
	mock.ExpectExec("DELETE FROM cmd_receipts").WithArgs(cutoff, 10).
		WillReturnResult(sqlmock.NewResult(0, 10))
	mock.ExpectExec("DELETE FROM cmd_receipts").WithArgs(cutoff, 10).
		WillReturnResult(sqlmock.NewResult(0, 10))
	// 剩余额度只有 5，最后一批 LIMIT 必须收窄到 5。
	mock.ExpectExec("DELETE FROM cmd_receipts").WithArgs(cutoff, 5).
		WillReturnResult(sqlmock.NewResult(0, 5))

	n, err := r.Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 25 {
		t.Fatalf("删除行数 = %d，期望 25（受 maxPerRun 限制）", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// 单表失败不阻塞后续表，错误聚合返回。
func TestReap_TableFailureDoesNotBlockOthers(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy: RetentionPolicy{
			PublishedOutbox: time.Hour,
			ConsumedEvents:  time.Hour,
		},
	})
	obs := newCountingObserver()
	r.WithObserver(obs)

	mock.ExpectExec("DELETE FROM outbox_events").
		WillReturnError(errors.New("lock wait timeout"))
	mock.ExpectExec("DELETE FROM consumed_events").
		WillReturnResult(sqlmock.NewResult(0, 7))

	n, err := r.Reap(context.Background())
	if err == nil {
		t.Fatal("期望返回聚合错误")
	}
	if n != 7 {
		t.Fatalf("删除行数 = %d，期望 7（失败表之后仍继续）", n)
	}
	if obs.failures["outbox_events"] != 1 {
		t.Fatalf("outbox_events 失败计数 = %d，期望 1", obs.failures["outbox_events"])
	}
	if obs.deleted["consumed_events"] != 7 {
		t.Fatalf("consumed_events 删除计数 = %d，期望 7", obs.deleted["consumed_events"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// outbox 清理必须显式限定 status='PUBLISHED'，DEAD 行不得被删除。
func TestReap_OutboxNeverDeletesDead(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy: RetentionPolicy{PublishedOutbox: 48 * time.Hour},
	})
	// 正则同时断言 PUBLISHED 限定与 published_at 过滤存在。
	mock.ExpectExec(`DELETE FROM outbox_events\s+WHERE status = 'PUBLISHED' AND published_at IS NOT NULL AND published_at < \?`).
		WithArgs(testNow.Add(-48*time.Hour), defaultReapBatchSize).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := r.Reap(context.Background()); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("outbox 清理必须限定 PUBLISHED: %v", err)
	}
}

// economy_transactions 是权威幂等载体，任何 specs 都不得涉及它。
func TestReap_NeverTouchesEconomyTransactions(t *testing.T) {
	r, _ := newReaperWithMock(t, RetentionReaperConfig{
		Policy: RetentionPolicy{
			PublishedOutbox: time.Hour,
			CmdReceipts:     time.Hour,
			ConsumedEvents:  time.Hour,
		},
	})
	for _, spec := range r.specs() {
		if spec.table == "economy_transactions" {
			t.Fatal("economy_transactions 不得进入自动清理范围")
		}
		if contains(spec.query, "economy_transactions") {
			t.Fatalf("%s 的 SQL 不应引用 economy_transactions", spec.table)
		}
	}
}

// ctx 已取消时不发起任何 DELETE，且不返回错误（视为本轮跳过，下轮继续）。
func TestReap_StopsOnContextCancel(t *testing.T) {
	r, mock := newReaperWithMock(t, RetentionReaperConfig{
		Policy:    RetentionPolicy{CmdReceipts: time.Hour},
		BatchSize: 10,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 进入循环前即取消，断言不产生 SQL

	n, err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("取消应干净退出，不返回错误，得到: %v", err)
	}
	if n != 0 {
		t.Fatalf("删除行数 = %d，期望 0", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ctx 已取消时不应发起 DELETE: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
