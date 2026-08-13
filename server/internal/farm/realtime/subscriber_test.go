package realtime

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSubscriberUsesDynamicFarmSubscription(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	received := make(chan Message, 1)
	subscriber := NewSubscriber(rdb, 10*time.Millisecond)
	if err := subscriber.Start(ctx, func(message Message) { received <- message }); err != nil {
		t.Fatal(err)
	}
	subscriber.Subscribe(123)
	// Wait for the subscription acknowledgement; Pub/Sub remains best effort,
	// so production correctness never depends on this timing.
	time.Sleep(10 * time.Millisecond)
	if err := NewPublisher(rdb).Publish(ctx, Message{FarmID: 123, EventID: "event-1", FarmVersion: 8}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got.EventID != "event-1" || got.FarmVersion != 8 {
			t.Fatalf("message=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("farm event not received")
	}
}
