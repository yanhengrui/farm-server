package realtime

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/redis/go-redis/v9"
)

type recordingObserver struct{ publishFailures int }

func (o *recordingObserver) MailboxPublishFailure() { o.publishFailures++ }

func TestPublisherRoutesMailboxChangeToOwningGateway(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := t.Context()
	received := make(chan Message, 1)
	if err := NewSubscriber(rdb, "gate-2").Start(ctx, func(message Message) { received <- message }); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "ws:conn:42", "gate-2:conn-7", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher(rdb).NotifyMailboxChanged(ctx, maildomain.MailboxSummary{UserID: 42, UnreadCount: 3, Version: 8}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got.UserID != 42 || got.UnreadCount != 3 || got.Version != 8 {
			t.Fatalf("message=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("mailbox notification was not delivered")
	}
}

func TestPublisherIgnoresOfflineUser(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := NewPublisher(rdb).NotifyMailboxChanged(t.Context(), maildomain.MailboxSummary{UserID: 42, UnreadCount: 1, Version: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestPublisherCountsRedisFailure(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	observer := &recordingObserver{}
	publisher := NewPublisher(rdb).WithObserver(observer)
	server.Close()
	err := publisher.NotifyMailboxChanged(context.Background(), maildomain.MailboxSummary{UserID: 42, UnreadCount: 1, Version: 1})
	if err == nil {
		t.Fatal("expected Redis failure")
	}
	if observer.publishFailures != 1 {
		t.Fatalf("publish failures=%d", observer.publishFailures)
	}
	_ = rdb.Close()
}
