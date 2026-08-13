package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestConnStoreSharedKickChannelKicksOldConnection(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	oldStore := NewConnStore(rdb, "gate-a")
	if err := oldStore.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	kicked := make(chan struct{}, 1)
	stopped := oldStore.ListenKick(ctx, 42, "conn-old", done, func() { kicked <- struct{}{} })
	if err := oldStore.Register(ctx, 42, "conn-old"); err != nil {
		t.Fatal(err)
	}
	newStore := NewConnStore(rdb, "gate-b")
	if err := newStore.Register(ctx, 42, "conn-new"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-kicked:
	case <-time.After(time.Second):
		t.Fatal("old connection was not kicked")
	}
	close(done)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
}

func TestConnStoreOldConnectionCannotRefreshOrDeleteNewOwner(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := t.Context()
	oldStore, newStore := NewConnStore(rdb, "gate-a"), NewConnStore(rdb, "gate-b")
	if err := oldStore.Register(ctx, 7, "old"); err != nil {
		t.Fatal(err)
	}
	if err := newStore.Register(ctx, 7, "new"); err != nil {
		t.Fatal(err)
	}
	if err := oldStore.Refresh(ctx, 7, "old"); err != nil {
		t.Fatal(err)
	}
	if err := oldStore.Unregister(ctx, 7, "old"); err != nil {
		t.Fatal(err)
	}
	got, err := rdb.Get(ctx, connKey(7)).Result()
	if err != nil || got != "gate-b:new" {
		t.Fatalf("current owner=%q err=%v", got, err)
	}
}
