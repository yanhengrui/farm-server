// Package http — Economy 命令短窗口 qty 聚合器。
//
// # 解决的问题
//
// 客户端连点（如快速购买 10 次 qty=1）会产生 10 次独立的 gamesvr gRPC 事务，
// 每次事务需要一次提交期 fsync。聚合器把同一用户在窗口内对同一
// (CmdType, CropID) 的多条请求合并成单条 qty=N 的命令，N 次 fsync 降为 1 次。
//
// # 幂等语义
//
// 每个等待者的原始 Idempotency-Key 在窗口内去重（同一 key 不重复计入 qty，
// 视为同一请求的重试，直接等待本窗口的结果）。聚合后生成新 cmd_id 发往 gamesvr；
// 若命令失败，所有等待者统一收到错误，可凭原 key 重试下一个窗口。
//
// # 边界
//
// - 窗口内累计 qty 超过 MaxEconomyQuantity 时触发提前发送，剩余请求进入新窗口。
// - 全为重复 key（窗口内同一用户发相同 Idempotency-Key）时，只发一次 qty=原始值的命令。
// - 不跨用户合并；不合并 Farm Actor 命令。
package http

import (
	"context"
	"sync"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/id"
)

type coalescerKey struct {
	userID  int64
	cmdType domain.CommandType
	cropID  string
}

type coalescerWaiter struct {
	ch chan<- coalescerOutcome
}

type coalescerOutcome struct {
	res application.CommitResult
	err error
}

type coalescerBucket struct {
	key      coalescerKey
	waiters  []coalescerWaiter
	seenKeys map[string]int64 // Idempotency-Key → 该 key 贡献的 qty（去重用）
	totalQty int64
	timer    *time.Timer
	once     sync.Once
	owner    *EconomyCoalescer
}

func (b *coalescerBucket) triggerFlush() {
	b.once.Do(func() {
		b.timer.Stop()
		go b.owner.flush(b) // 必须异步：caller 可能持有 c.mu，flush 内也会 Lock
	})
}

// EconomyCoalescer 为 Economy HTTP 端点提供短窗口 qty 聚合。
// 线程安全；零值不可用，必须通过 NewEconomyCoalescer 构造。
type EconomyCoalescer struct {
	client  EconomicCommitClient
	window  time.Duration
	mu      sync.Mutex
	buckets map[coalescerKey]*coalescerBucket
}

// NewEconomyCoalescer 构造聚合器。
// window 推荐 50ms；client 是下游 gamesvr 客户端。
func NewEconomyCoalescer(client EconomicCommitClient, window time.Duration) *EconomyCoalescer {
	return &EconomyCoalescer{
		client:  client,
		window:  window,
		buckets: make(map[coalescerKey]*coalescerBucket),
	}
}

// Submit 将一条 Economy 命令提交到聚合器，阻塞直到窗口触发并收到结果。
// ctx 取消只中断当前等待，不取消已经发出的聚合命令。
func (c *EconomyCoalescer) Submit(ctx context.Context, userID int64, cmdType domain.CommandType, cropID, idemKey string, qty int64) (application.CommitResult, error) {
	ch := make(chan coalescerOutcome, 1)
	w := coalescerWaiter{ch: ch}

	c.mu.Lock()
	k := coalescerKey{userID: userID, cmdType: cmdType, cropID: cropID}
	b := c.buckets[k]
	if b == nil {
		b = c.newBucket(k)
	}

	prevQty := b.seenKeys[idemKey]
	isNew := prevQty == 0
	if isNew {
		b.seenKeys[idemKey] = qty
	}
	addQty := qty
	if !isNew {
		addQty = 0
	}

	// qty 累加会超限时，把当前 bucket 提前触发，本次请求进入新 bucket。
	if addQty > 0 && b.totalQty+addQty > domain.MaxEconomyQuantity {
		b.triggerFlush()
		b = c.newBucket(k)
		b.seenKeys[idemKey] = qty
		addQty = qty
	}

	b.totalQty += addQty
	b.waiters = append(b.waiters, w)
	c.mu.Unlock()

	select {
	case out := <-ch:
		return out.res, out.err
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	}
}

func (c *EconomyCoalescer) newBucket(k coalescerKey) *coalescerBucket {
	b := &coalescerBucket{key: k, seenKeys: make(map[string]int64), owner: c}
	c.buckets[k] = b
	b.timer = time.AfterFunc(c.window, func() {
		b.once.Do(func() { c.flush(b) })
	})
	return b
}

// flush 在窗口到期（或提前触发）时执行，向 gamesvr 发出单条合并命令。
// 在 once.Do 内调用，保证每个 bucket 只触发一次。
func (c *EconomyCoalescer) flush(b *coalescerBucket) {
	c.mu.Lock()
	if c.buckets[b.key] == b {
		delete(c.buckets, b.key)
	}
	waiters := b.waiters
	totalQty := b.totalQty
	key := b.key
	c.mu.Unlock()

	if len(waiters) == 0 {
		return
	}

	var out coalescerOutcome
	if totalQty <= 0 {
		// 全是重复 key 的重试，直接报错让客户端重试（窗口内 qty 为 0 不应发命令）。
		out.err = context.DeadlineExceeded
	} else {
		out.res, out.err = c.client.CommitFarmCommand(context.Background(), application.CommitRequest{
			Command: domain.Command{
				CmdID:     id.NewV7(),
				FarmID:    key.userID,
				ActorUser: key.userID,
				Type:      key.cmdType,
				CropID:    key.cropID,
				Quantity:  totalQty,
			},
		})
	}
	for _, w := range waiters {
		w.ch <- out
	}
}
