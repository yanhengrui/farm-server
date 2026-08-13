package http

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

// coalescerClientCapture 记录 CommitFarmCommand 的调用次数与参数。
type coalescerClientCapture struct {
	mu       sync.Mutex
	calls    []application.CommitRequest
	response application.CommitResult
	err      error
}

func (c *coalescerClientCapture) CommitFarmCommand(_ context.Context, req application.CommitRequest) (application.CommitResult, error) {
	c.mu.Lock()
	c.calls = append(c.calls, req)
	c.mu.Unlock()
	return c.response, c.err
}

func (c *coalescerClientCapture) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *coalescerClientCapture) lastQty() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		return 0
	}
	return c.calls[len(c.calls)-1].Command.Quantity
}

// TestEconomyCoalescer_MergesQtyWithinWindow 验证窗口内多个不同 key 被合并为一次调用。
func TestEconomyCoalescer_MergesQtyWithinWindow(t *testing.T) {
	cap := &coalescerClientCapture{response: application.CommitResult{CoinBalance: 100}}
	c := NewEconomyCoalescer(cap, 80*time.Millisecond)

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			idemKey := "key-" + string(rune('a'+n))
			_, err := c.Submit(context.Background(), 1, domain.CmdPurchaseSeed, "WHEAT", idemKey, 2)
			if err != nil {
				t.Errorf("Submit failed: %v", err)
			}
		}(i)
	}
	// 让所有 goroutine 先入队再等窗口到期。
	time.Sleep(20 * time.Millisecond)
	wg.Wait()

	if n := cap.callCount(); n != 1 {
		t.Fatalf("期望 1 次 gamesvr 调用，实际 %d 次", n)
	}
	if qty := cap.lastQty(); qty != 10 {
		t.Fatalf("期望合并 qty=10，实际 %d", qty)
	}
}

// TestEconomyCoalescer_SameKeyNotDoubledQty 验证窗口内相同 Idempotency-Key 不重复计 qty。
func TestEconomyCoalescer_SameKeyNotDoubledQty(t *testing.T) {
	cap := &coalescerClientCapture{response: application.CommitResult{CoinBalance: 90}}
	c := NewEconomyCoalescer(cap, 80*time.Millisecond)

	results := make([]application.CommitResult, 3)
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 同一 Idempotency-Key 重试 3 次
			res, err := c.Submit(context.Background(), 2, domain.CmdPurchaseSeed, "WHEAT", "dup-key", 1)
			if err != nil {
				t.Errorf("Submit err: %v", err)
			}
			results[idx] = res
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	wg.Wait()

	// 只应发出一次命令，qty=1（不是 3）
	if n := cap.callCount(); n != 1 {
		t.Fatalf("重复 key 只应产生 1 次调用，实际 %d", n)
	}
	if qty := cap.lastQty(); qty != 1 {
		t.Fatalf("重复 key 合并 qty 应为 1，实际 %d", qty)
	}
	// 所有等待者都收到了相同的 CoinBalance
	for i, r := range results {
		if r.CoinBalance != 90 {
			t.Errorf("results[%d].CoinBalance=%d", i, r.CoinBalance)
		}
	}
}

// TestEconomyCoalescer_OverflowTriggersEarlyFlush 验证 qty 超限时提前触发当前窗口，剩余进入新窗口。
func TestEconomyCoalescer_OverflowTriggersEarlyFlush(t *testing.T) {
	cap := &coalescerClientCapture{response: application.CommitResult{CoinBalance: 50}}
	// 把 MaxEconomyQuantity 模拟为 5（通过发 qty=3+3 触发溢出）
	// 但常量不可修改，所以用较大窗口 + qty 刚好超过 MaxEconomyQuantity 来测试。
	// 实际测试：3 个请求各 qty=4000，总计 12000 > MaxEconomyQuantity(10000)，
	// 第三个应触发新窗口。
	c := NewEconomyCoalescer(cap, 200*time.Millisecond)

	var wg sync.WaitGroup
	var callCount atomic.Int64
	origCallCount := func() int64 { return callCount.Load() }
	_ = origCallCount

	done := make(chan struct{}, 3)
	for i, qty := range []int64{4000, 4000, 4000} {
		wg.Add(1)
		go func(n int, q int64) {
			defer wg.Done()
			defer func() { done <- struct{}{} }()
			k := "of-key-" + string(rune('a'+n))
			c.Submit(context.Background(), 3, domain.CmdPurchaseSeed, "WHEAT", k, q) //nolint:errcheck
		}(i, qty)
	}
	// 等所有结果
	for range 3 {
		<-done
	}

	if n := cap.callCount(); n < 2 {
		t.Fatalf("期望至少 2 次调用（溢出触发早期 flush），实际 %d", n)
	}
}

// TestEconomyCoalescer_CtxCancelDoesNotAffectOtherWaiters 验证单个等待者 ctx 取消不影响窗口内其他人。
func TestEconomyCoalescer_CtxCancelDoesNotAffectOtherWaiters(t *testing.T) {
	cap := &coalescerClientCapture{response: application.CommitResult{CoinBalance: 80}}
	c := NewEconomyCoalescer(cap, 100*time.Millisecond)

	normalDone := make(chan application.CommitResult, 1)
	cancelDone := make(chan error, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := c.Submit(context.Background(), 4, domain.CmdPurchaseSeed, "WHEAT", "normal-key", 1)
		if err != nil {
			t.Errorf("normal waiter err: %v", err)
		}
		normalDone <- res
	}()

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := c.Submit(ctx, 4, domain.CmdPurchaseSeed, "WHEAT", "cancel-key", 1)
		cancelDone <- err
	}()

	// 让两个 goroutine 都入队
	time.Sleep(15 * time.Millisecond)
	cancel()

	wg.Wait()

	// 取消者收到 ctx error
	if err := <-cancelDone; err == nil {
		t.Error("被取消的等待者应收到 context error")
	}
	// 正常等待者仍收到结果
	res := <-normalDone
	if res.CoinBalance != 80 {
		t.Errorf("normal waiter CoinBalance=%d", res.CoinBalance)
	}
	// gamesvr 只被调用一次（聚合窗口仍然触发）
	if n := cap.callCount(); n != 1 {
		t.Fatalf("期望 1 次 gamesvr 调用，实际 %d", n)
	}
}

// TestEconomyCoalescer_DifferentUsersDontMerge 不同用户不合并。
func TestEconomyCoalescer_DifferentUsersDontMerge(t *testing.T) {
	cap := &coalescerClientCapture{response: application.CommitResult{CoinBalance: 70}}
	c := NewEconomyCoalescer(cap, 80*time.Millisecond)

	var wg sync.WaitGroup
	for userID := int64(1); userID <= 3; userID++ {
		wg.Add(1)
		go func(uid int64) {
			defer wg.Done()
			k := "key-user-" + string(rune('0'+int(uid)))
			c.Submit(context.Background(), uid, domain.CmdPurchaseSeed, "WHEAT", k, 1) //nolint:errcheck
		}(userID)
	}
	time.Sleep(20 * time.Millisecond)
	wg.Wait()

	if n := cap.callCount(); n != 3 {
		t.Fatalf("不同用户应产生 3 次独立调用，实际 %d", n)
	}
}
