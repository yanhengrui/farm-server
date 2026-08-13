package shard

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIDGeneratorIsUniqueAndCarriesShard(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g, err := NewIDGenerator(epoch, 3, 7)
	if err != nil {
		t.Fatal(err)
	}
	now := epoch.Add(time.Second)
	seen := map[int64]bool{}
	for i := 0; i < 256; i++ {
		id, err := g.Next(now)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = true
		shardID, err := ShardIDFromGlobalID(id)
		if err != nil || shardID != 3 {
			t.Fatalf("shard = %d, err=%v", shardID, err)
		}
	}
	if _, err := g.Next(now); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("sequence exhaustion err = %v", err)
	}
}

func TestRouterUsesGlobalIDShardPlacement(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g, err := NewIDGenerator(epoch, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := g.Next(epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter([]string{"shard-0", "shard-1"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ShardForUserID(id)
	if err != nil || got != "shard-1" {
		t.Fatalf("global ID route = %q, err=%v", got, err)
	}
}

func TestGlobalIDRoutesBackToEncodedShardForSupportedCounts(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for count := 1; count <= 64; count *= 2 {
		names := make([]string, count)
		for i := range names {
			names[i] = fmt.Sprintf("shard-%02d", i)
		}
		router, err := NewRouter(names)
		if err != nil {
			t.Fatalf("NewRouter(%d): %v", count, err)
		}
		for shardID := 0; shardID < count; shardID++ {
			generator, err := NewIDGenerator(epoch, uint8(shardID), 7)
			if err != nil {
				t.Fatalf("NewIDGenerator(count=%d shard=%d): %v", count, shardID, err)
			}
			generatedID, err := generator.Next(epoch.Add(time.Second))
			if err != nil {
				t.Fatalf("Next(count=%d shard=%d): %v", count, shardID, err)
			}
			got, err := router.ShardForUserID(generatedID)
			if err != nil || got != names[shardID] {
				t.Fatalf("count=%d encoded=%d route=%q err=%v, want %q", count, shardID, got, err, names[shardID])
			}
		}
	}
}
