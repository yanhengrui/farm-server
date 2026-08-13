package redisstore

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSessionClaimEpochAndOneTimeHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewSessionStore(rdb)
	ctx := t.Context()
	epoch, err := store.Claim(ctx, "session-1", 7, 7, "gate-a")
	if err != nil || epoch != 1 {
		t.Fatalf("first claim epoch=%d err=%v", epoch, err)
	}
	session, err := store.Load(ctx, "session-1")
	if err != nil || session.OwnerInstance != "gate-a" || session.Status != "ACTIVE" {
		t.Fatalf("session=%+v err=%v", session, err)
	}
	if err := store.BeginHandoff(ctx, "session-1", "gate-a", epoch, "ticket-1", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeResume(ctx, "ticket-1", "session-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeResume(ctx, "ticket-1", "session-1"); err == nil {
		t.Fatal("resume ticket must be one-time")
	}
}

func TestOldSessionOwnerCannotIssueHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewSessionStore(rdb)
	ctx := t.Context()
	oldEpoch, _ := store.Claim(ctx, "session-2", 8, 8, "gate-a")
	newEpoch, _ := store.Claim(ctx, "session-2", 8, 8, "gate-b")
	if newEpoch <= oldEpoch {
		t.Fatalf("old=%d new=%d", oldEpoch, newEpoch)
	}
	if err := store.BeginHandoff(ctx, "session-2", "gate-a", oldEpoch, "stale-ticket", 30*time.Second); err == nil {
		t.Fatal("old owner issued handoff")
	}
}
