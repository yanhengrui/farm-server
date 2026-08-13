package http

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/redis/go-redis/v9"
)

type cacheSnapshotStub struct {
	calls   atomic.Int64
	version atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (s *cacheSnapshotStub) GetSnapshot(_ context.Context, farmID int64) (*farmrpc.FarmSnapshotDTO, error) {
	s.calls.Add(1)
	version := s.version.Load()
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return &farmrpc.FarmSnapshotDTO{FarmID: farmID, Version: version}, nil
}

type cacheAssetStub struct{ calls atomic.Int64 }

func (s *cacheAssetStub) GetPlayerAssets(_ context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	s.calls.Add(1)
	return &assetrpc.AssetsDTO{CoinBalance: userID * 10, Inventory: []assetrpc.InventoryItemDTO{}}, nil
}

type switchableAssetStub struct {
	calls atomic.Int64
	err   atomic.Bool
}

func (s *switchableAssetStub) GetPlayerAssets(_ context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	s.calls.Add(1)
	if s.err.Load() {
		return nil, errors.New("mysql unavailable")
	}
	return &assetrpc.AssetsDTO{CoinBalance: userID * 10}, nil
}

func newTestReadCache(t *testing.T) (*ReadCache, *redis.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewReadCache(rdb, "test:read", time.Minute, 5*time.Minute, time.Second, nil), rdb
}

func TestReadCacheHitsAndInvalidates(t *testing.T) {
	cache, _ := newTestReadCache(t)
	snapStub := &cacheSnapshotStub{}
	snapStub.version.Store(7)
	snapshots := NewCachedSnapshotClient(snapStub, cache)
	assetStub := &cacheAssetStub{}
	assets := NewCachedAssetClient(assetStub, cache)

	for range 2 {
		snapshot, err := snapshots.GetSnapshot(context.Background(), 42)
		if err != nil || snapshot.Version != 7 {
			t.Fatalf("snapshot = %+v, err = %v", snapshot, err)
		}
		if _, err = assets.GetPlayerAssets(context.Background(), 42); err != nil {
			t.Fatal(err)
		}
	}
	if snapStub.calls.Load() != 1 || assetStub.calls.Load() != 1 {
		t.Fatalf("cache did not absorb reads: snapshot=%d assets=%d", snapStub.calls.Load(), assetStub.calls.Load())
	}

	cache.Invalidate(context.Background(), 42, 42)
	snapStub.version.Store(8)
	snapshot, err := snapshots.GetSnapshot(context.Background(), 42)
	if err != nil || snapshot.Version != 8 || snapStub.calls.Load() != 2 {
		t.Fatalf("invalidation did not force reload: snapshot=%+v calls=%d err=%v", snapshot, snapStub.calls.Load(), err)
	}
}

func TestReadCacheDropsFillThatRacesWithInvalidation(t *testing.T) {
	cache, _ := newTestReadCache(t)
	stub := &cacheSnapshotStub{entered: make(chan struct{}), release: make(chan struct{})}
	stub.version.Store(1)
	client := NewCachedSnapshotClient(stub, cache)

	var first *farmrpc.FarmSnapshotDTO
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		first, firstErr = client.GetSnapshot(context.Background(), 9)
	}()
	<-stub.entered
	cache.Invalidate(context.Background(), 9, 9)
	close(stub.release)
	wg.Wait()
	if firstErr != nil || first.Version != 1 {
		t.Fatalf("first snapshot = %+v, err=%v", first, firstErr)
	}

	stub.entered = nil
	stub.version.Store(2)
	second, err := client.GetSnapshot(context.Background(), 9)
	if err != nil || second.Version != 2 || stub.calls.Load() != 2 {
		t.Fatalf("stale fill survived invalidation: snapshot=%+v calls=%d err=%v", second, stub.calls.Load(), err)
	}
}

func TestReadCacheFailsOpenWhenRedisUnavailable(t *testing.T) {
	cache, rdb := newTestReadCache(t)
	_ = rdb.Close()
	stub := &cacheAssetStub{}
	client := NewCachedAssetClient(stub, cache)
	assets, err := client.GetPlayerAssets(context.Background(), 3)
	if err != nil || assets.CoinBalance != 30 || stub.calls.Load() != 1 {
		t.Fatalf("fail-open read = %+v, calls=%d err=%v", assets, stub.calls.Load(), err)
	}
}

func TestReadCacheServesBoundedStaleDataOnDatabaseFailure(t *testing.T) {
	mini := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer rdb.Close()
	cache := NewReadCache(rdb, "test:stale", time.Millisecond, time.Minute, time.Second, nil)
	stub := &switchableAssetStub{}
	client := NewCachedAssetClient(stub, cache)

	first, err := client.GetPlayerAssets(context.Background(), 5)
	if err != nil || first.CoinBalance != 50 {
		t.Fatalf("warm cache: assets=%+v err=%v", first, err)
	}
	time.Sleep(3 * time.Millisecond)
	stub.err.Store(true)
	stale, err := client.GetPlayerAssets(context.Background(), 5)
	if err != nil || stale.CoinBalance != 50 || stub.calls.Load() != 2 {
		t.Fatalf("stale fallback: assets=%+v calls=%d err=%v", stale, stub.calls.Load(), err)
	}
}
