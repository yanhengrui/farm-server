package http

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
)

// --- snapshot stub ---

type snapClientStub struct {
	calls atomic.Int64
	delay time.Duration
	snap  *farmrpc.FarmSnapshotDTO
}

func (s *snapClientStub) GetSnapshot(_ context.Context, _ int64) (*farmrpc.FarmSnapshotDTO, error) {
	s.calls.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.snap, nil
}

// TestSingleflightSnapshot_ConcurrentSameFarm 同 farmID 并发读只发一次 RPC。
func TestSingleflightSnapshot_ConcurrentSameFarm(t *testing.T) {
	stub := &snapClientStub{snap: &farmrpc.FarmSnapshotDTO{FarmID: 1}, delay: 30 * time.Millisecond}
	client := NewSingleflightSnapshotClient(stub)

	const workers = 20
	results := make([]*farmrpc.FarmSnapshotDTO, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			snap, err := client.GetSnapshot(context.Background(), 1)
			if err != nil {
				t.Errorf("worker %d err: %v", n, err)
				return
			}
			results[n] = snap
		}(i)
	}
	// 让所有 goroutine 先到达 Do 再触发
	time.Sleep(5 * time.Millisecond)
	wg.Wait()

	if n := stub.calls.Load(); n > 3 {
		t.Errorf("期望显著合并（calls << %d），实际 %d", workers, n)
	}
	for i, r := range results {
		if r == nil || r.FarmID != 1 {
			t.Errorf("results[%d] 错误: %+v", i, r)
		}
	}
}

// TestSingleflightSnapshot_DifferentFarmsNotMerged 不同 farmID 各自独立发送。
func TestSingleflightSnapshot_DifferentFarmsNotMerged(t *testing.T) {
	stub := &snapClientStub{snap: &farmrpc.FarmSnapshotDTO{}}
	client := NewSingleflightSnapshotClient(stub)

	var wg sync.WaitGroup
	for farmID := int64(1); farmID <= 5; farmID++ {
		wg.Add(1)
		go func(fid int64) {
			defer wg.Done()
			client.GetSnapshot(context.Background(), fid) //nolint:errcheck
		}(farmID)
	}
	wg.Wait()

	if n := stub.calls.Load(); n != 5 {
		t.Fatalf("不同 farmID 应各发一次，实际 %d", n)
	}
}

// --- sfAssetClientStub（避免与 player_handler_test.go 的 assetClientStub 名冲突）---

type sfAssetClientStub struct {
	calls atomic.Int64
	delay time.Duration
}

func (s *sfAssetClientStub) GetPlayerAssets(_ context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	s.calls.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return &assetrpc.AssetsDTO{CoinBalance: userID * 100}, nil
}

// TestSingleflightAssets_ConcurrentSameUser 同 userID 并发读只发一次 RPC。
func TestSingleflightAssets_ConcurrentSameUser(t *testing.T) {
	stub := &sfAssetClientStub{delay: 30 * time.Millisecond}
	client := NewSingleflightAssetClient(stub)

	const workers = 15
	var wg sync.WaitGroup
	results := make([]*assetrpc.AssetsDTO, workers)
	for i := range workers {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			assets, err := client.GetPlayerAssets(context.Background(), 42)
			if err != nil {
				t.Errorf("worker %d err: %v", n, err)
				return
			}
			results[n] = assets
		}(i)
	}
	time.Sleep(5 * time.Millisecond)
	wg.Wait()

	if n := stub.calls.Load(); n > 3 {
		t.Errorf("期望显著合并（calls << %d），实际 %d", workers, n)
	}
	for i, r := range results {
		if r == nil || r.CoinBalance != 42*100 {
			t.Errorf("results[%d]=%+v", i, r)
		}
	}
}
