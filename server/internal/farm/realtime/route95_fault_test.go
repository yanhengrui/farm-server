package realtime

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/redis/go-redis/v9"
)

func waitRoute95Message(t *testing.T, ch <-chan Message, eventID string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case message := <-ch:
			if message.EventID == eventID {
				return
			}
		case <-deadline:
			t.Fatalf("event %s not received", eventID)
		}
	}
}

// TestRoute95ExternalRedisPubSubRoundTrip verifies the configured disposable
// Redis instance without creating keys: one gatesvr-style shared subscriber
// dynamically subscribes to a unique farm channel and receives a real publish.
func TestRoute95ExternalRedisPubSubRoundTrip(t *testing.T) {
	addr := os.Getenv("ROUTE95_REDIS_ADDR")
	if addr == "" {
		t.Skip("ROUTE95_REDIS_ADDR is not configured")
	}
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     os.Getenv("ROUTE95_REDIS_PASSWORD"),
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping external Redis: %v", err)
	}

	farmID := time.Now().UnixNano() & 0x3fffffffffffffff
	eventID := "route95-redis-roundtrip"
	received := make(chan Message, 4)
	subscriber := NewSubscriber(client, 10*time.Millisecond)
	if err := subscriber.Start(ctx, func(message Message) { received <- message }); err != nil {
		t.Fatalf("start external Redis subscriber: %v", err)
	}
	subscriber.Subscribe(farmID)
	publisher := NewPublisher(client)
	message := Message{FarmID: farmID, EventID: eventID, FarmVersion: 1}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := publisher.Publish(ctx, message); err != nil {
			t.Fatalf("publish external Redis message: %v", err)
		}
		select {
		case got := <-received:
			if got.EventID == eventID && got.FarmID == farmID {
				t.Logf("external Redis Pub/Sub round trip verified for farm=%d", farmID)
				return
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("external Redis subscriber did not receive the published event")
}

type route95Submitter struct{ commits int }

func (s *route95Submitter) Submit(context.Context, domain.Command) (application.CommitResult, error) {
	s.commits++
	return application.CommitResult{NewVersion: int64(s.commits), EventID: "committed"}, nil
}

// TestRoute95TwoGatewaysRecoverAfterRedisOutage verifies the route 9.5
// best-effort contract: two gatesvr subscribers receive the same patch, Redis
// loss never changes a successful authoritative commit, and shared Pub/Sub
// connections resubscribe after Redis returns.
func TestRoute95TwoGatewaysRecoverAfterRedisOutage(t *testing.T) {
	server := miniredis.NewMiniRedis()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	newClient := func() *redis.Client {
		client := redis.NewClient(&redis.Options{
			Addr:            server.Addr(),
			DialTimeout:     100 * time.Millisecond,
			ReadTimeout:     100 * time.Millisecond,
			WriteTimeout:    100 * time.Millisecond,
			MaxRetries:      1,
			MinRetryBackoff: 10 * time.Millisecond,
			MaxRetryBackoff: 20 * time.Millisecond,
		})
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	receivedA := make(chan Message, 8)
	receivedB := make(chan Message, 8)
	subscriberA := NewSubscriber(newClient(), 10*time.Millisecond)
	subscriberB := NewSubscriber(newClient(), 10*time.Millisecond)
	if err := subscriberA.Start(ctx, func(message Message) { receivedA <- message }); err != nil {
		t.Fatal(err)
	}
	if err := subscriberB.Start(ctx, func(message Message) { receivedB <- message }); err != nil {
		t.Fatal(err)
	}
	subscriberA.Subscribe(95)
	subscriberB.Subscribe(95)
	publisher := NewPublisher(newClient())

	publishUntil := func(message Message, timeout time.Duration) error {
		deadline := time.Now().Add(timeout)
		var lastErr error
		for time.Now().Before(deadline) {
			if err := publisher.Publish(ctx, message); err == nil {
				return nil
			} else {
				lastErr = err
			}
			time.Sleep(20 * time.Millisecond)
		}
		return lastErr
	}
	time.Sleep(20 * time.Millisecond)
	first := Message{FarmID: 95, EventID: "before-outage", FarmVersion: 1}
	if err := publishUntil(first, time.Second); err != nil {
		t.Fatalf("publish before outage: %v", err)
	}
	waitRoute95Message(t, receivedA, first.EventID, time.Second)
	waitRoute95Message(t, receivedB, first.EventID, time.Second)

	server.Close()
	failedCtx, failedCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	err := publisher.Publish(failedCtx, Message{FarmID: 95, EventID: "lost", FarmVersion: 2})
	failedCancel()
	if err == nil {
		t.Fatal("publish unexpectedly succeeded while Redis was down")
	}
	authoritative := &route95Submitter{}
	wrapped := NewPublishingSubmitter(authoritative, publisher, slog.Default())
	result, submitErr := wrapped.Submit(ctx, domain.Command{FarmID: 95})
	if submitErr != nil || result.NewVersion != 1 || authoritative.commits != 1 {
		t.Fatalf("Redis failure changed commit result: result=%+v commits=%d err=%v", result, authoritative.commits, submitErr)
	}

	if err := server.Restart(); err != nil {
		t.Fatalf("restart Redis: %v", err)
	}
	recovered := Message{FarmID: 95, EventID: "after-recovery", FarmVersion: 3}
	deadline := time.Now().Add(5 * time.Second)
	var recoveredA, recoveredB bool
	for time.Now().Before(deadline) && (!recoveredA || !recoveredB) {
		_ = publisher.Publish(ctx, recovered)
		for {
			select {
			case message := <-receivedA:
				recoveredA = recoveredA || message.EventID == recovered.EventID
			case message := <-receivedB:
				recoveredB = recoveredB || message.EventID == recovered.EventID
			default:
				goto drained
			}
		}
	drained:
		time.Sleep(25 * time.Millisecond)
	}
	if !recoveredA || !recoveredB {
		t.Fatalf("subscribers did not recover: gateA=%t gateB=%t", recoveredA, recoveredB)
	}
}
