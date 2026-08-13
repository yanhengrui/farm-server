package infrastructure

import (
	"context"
	"testing"

	"github.com/photon/farm-server/server/pkg/shard"
)

func TestShardedConsumerDedupResolverRoutesByAggregateOwner(t *testing.T) {
	router, err := shard.NewRouter([]string{"shard-1", "shard-0"})
	if err != nil {
		t.Fatal(err)
	}
	primary := NewConsumerDedup(nil)
	shard0 := NewConsumerDedup(nil)
	shard1 := NewConsumerDedup(nil)
	resolver, err := NewShardedConsumerDedupResolver(router, map[string]*ConsumerDedup{
		"shard-0": shard0,
		"shard-1": shard1,
	}, primary)
	if err != nil {
		t.Fatal(err)
	}

	got, err := resolver(context.Background(), []byte(`{"event_type":"farm.harvested.v1","payload":{"owner_user_id":"3"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != shard1 { // 3 % 2 routes to lexically sorted shard-1.
		t.Fatal("owner event did not use owner shard dedup")
	}
}

func TestShardedConsumerDedupResolverRoutesFriendSagaLegs(t *testing.T) {
	router, err := shard.NewRouter([]string{"shard-0", "shard-1"})
	if err != nil {
		t.Fatal(err)
	}
	primary := NewConsumerDedup(nil)
	shard0 := NewConsumerDedup(nil)
	shard1 := NewConsumerDedup(nil)
	resolver, err := NewShardedConsumerDedupResolver(router, map[string]*ConsumerDedup{
		"shard-0": shard0,
		"shard-1": shard1,
	}, primary)
	if err != nil {
		t.Fatal(err)
	}

	request, err := resolver(context.Background(), []byte(`{"event_type":"social.friend_edge_requested.v1","payload":{"source_user_id":"3","target_user_id":"2"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if request != shard0 {
		t.Fatal("friend request did not use target shard dedup")
	}
	ack, err := resolver(context.Background(), []byte(`{"event_type":"social.friend_edge_applied.v1","payload":{"source_user_id":"3","target_user_id":"2"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if ack != shard1 {
		t.Fatal("friend acknowledgement did not use source shard dedup")
	}
}

func TestShardedConsumerDedupResolverUsesFallbackForSystemEvent(t *testing.T) {
	router, _ := shard.NewRouter([]string{"shard-0"})
	primary := NewConsumerDedup(nil)
	resolver, err := NewShardedConsumerDedupResolver(router, map[string]*ConsumerDedup{"shard-0": NewConsumerDedup(nil)}, primary)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver(context.Background(), []byte(`{"event_type":"system.tick.v1","payload":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != primary {
		t.Fatal("system event did not use fallback dedup")
	}
}
