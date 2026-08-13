package redisstore

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRefreshStoreBindsRotatesAndRevokesSevenDaySession(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewRefreshStore(rdb)
	ctx := t.Context()

	if err := store.Create(ctx, "sid-1", "old-hash", 42); err != nil {
		t.Fatal(err)
	}
	if active, err := store.IsActive(ctx, "sid-1", 42); err != nil || !active {
		t.Fatalf("created session active=%v err=%v", active, err)
	}
	if active, err := store.IsActive(ctx, "sid-1", 7); err != nil || active {
		t.Fatalf("session accepted wrong user: active=%v err=%v", active, err)
	}
	ttl := server.TTL(authSessionKey("sid-1"))
	if ttl != 7*24*time.Hour {
		t.Fatalf("session ttl=%v", ttl)
	}
	if userID, err := store.Rotate(ctx, "wrong-sid", "old-hash", "new-hash"); err != nil || userID != 0 {
		t.Fatalf("mismatched session rotated: user=%d err=%v", userID, err)
	}
	userID, err := store.Rotate(ctx, "sid-1", "old-hash", "new-hash")
	if err != nil || userID != 42 {
		t.Fatalf("rotate user=%d err=%v", userID, err)
	}
	if userID, err := store.Rotate(ctx, "sid-1", "old-hash", "next-hash"); err != nil || userID != 0 {
		t.Fatalf("replayed refresh rotated: user=%d err=%v", userID, err)
	}
	if deleted, err := store.DeleteSession(ctx, "sid-1", "wrong-hash"); err != nil || deleted {
		t.Fatalf("mismatched logout deleted session: deleted=%v err=%v", deleted, err)
	}
	if deleted, err := store.DeleteSession(ctx, "sid-1", "new-hash"); err != nil || !deleted {
		t.Fatal(err)
	}
	if server.Exists(authSessionKey("sid-1")) || server.Exists(refreshKey("new-hash")) {
		t.Fatal("logout left refresh session keys behind")
	}
	if active, err := store.IsActive(ctx, "sid-1", 42); err != nil || active {
		t.Fatalf("deleted session active=%v err=%v", active, err)
	}
}

func TestRefreshStorePreservesGlobalIDAboveLuaSafeInteger(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewRefreshStore(rdb)
	ctx := t.Context()
	const userID int64 = 80581935558049792

	if err := store.Create(ctx, "sid-large", "old-large", userID); err != nil {
		t.Fatal(err)
	}
	rotated, err := store.Rotate(ctx, "sid-large", "old-large", "new-large")
	if err != nil || rotated != userID {
		t.Fatalf("rotate lost global ID precision: got=%d want=%d err=%v", rotated, userID, err)
	}
	if active, err := store.IsActive(ctx, "sid-large", userID); err != nil || !active {
		t.Fatalf("rotated large-ID session active=%v err=%v", active, err)
	}
}

func TestLogoutPublishesKickForExistingWebSocket(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := t.Context()
	refreshStore := NewRefreshStore(rdb)
	if err := refreshStore.Create(ctx, "sid-logout", "logout-hash", 42); err != nil {
		t.Fatal(err)
	}
	connStore := NewConnStore(rdb, "gate-1")
	if err := connStore.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	kicked := make(chan struct{})
	stopped := connStore.ListenKick(ctx, 42, "conn-1", done, func() { close(kicked) })
	if err := connStore.Register(ctx, 42, "conn-1"); err != nil {
		t.Fatal(err)
	}
	deleted, err := refreshStore.DeleteSession(ctx, "sid-logout", "logout-hash")
	if err != nil || !deleted {
		t.Fatalf("logout deleted=%v err=%v", deleted, err)
	}
	select {
	case <-kicked:
	case <-time.After(time.Second):
		t.Fatal("logout did not publish websocket kick")
	}
	close(done)
	<-stopped
}

func TestRefreshStoreMigratesLegacySessionDuringRollingUpdate(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewRefreshStore(rdb)
	ctx := t.Context()
	if err := rdb.Set(ctx, refreshKey("legacy-hash"), 77, 24*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	userID, err := store.MigrateLegacy(ctx, "legacy-sid", "legacy-hash", "rotated-hash")
	if err != nil || userID != 77 {
		t.Fatalf("migration user=%d err=%v", userID, err)
	}
	if server.Exists(refreshKey("legacy-hash")) || !server.Exists(authSessionKey("legacy-sid")) {
		t.Fatal("legacy keys were not migrated")
	}
	if got, _ := rdb.Get(ctx, refreshKey("rotated-hash")).Int64(); got != 77 {
		t.Fatalf("new reverse index=%d", got)
	}
}
