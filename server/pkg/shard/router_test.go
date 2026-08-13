package shard

import "testing"

func TestRouterStableAndSharedAggregateRoute(t *testing.T) {
	r, err := NewRouter([]string{"shard-1", "shard-0"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := r.Shards()[0], "shard-0"; got != want {
		t.Fatalf("normalized shard order = %q, want %q", got, want)
	}
	for _, id := range []int64{1, 2, 3, 99, 1 << 20} {
		userShard, err := r.ShardForUserID(id)
		if err != nil {
			t.Fatalf("user route %d: %v", id, err)
		}
		farmShard, err := r.ShardForFarmID(id)
		if err != nil {
			t.Fatalf("farm route %d: %v", id, err)
		}
		if userShard != farmShard {
			t.Fatalf("aggregate split for %d: user=%s farm=%s", id, userShard, farmShard)
		}
	}
}

func TestRouterRejectsShardCountsThatBreakGlobalIDPlacement(t *testing.T) {
	for _, names := range [][]string{
		{"shard-0", "shard-1", "shard-2"},
		make([]string, 65),
	} {
		if _, err := NewRouter(names); err == nil {
			t.Fatalf("NewRouter with %d shards unexpectedly succeeded", len(names))
		}
	}
}

func TestRouterRejectsInvalidConfigurationAndKeys(t *testing.T) {
	for _, names := range [][]string{nil, {""}, {"shard-0", "shard-0"}} {
		if _, err := NewRouter(names); err == nil {
			t.Fatalf("NewRouter(%v) unexpectedly succeeded", names)
		}
	}
	r, err := NewRouter([]string{"shard-0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ShardForUserID(0); err == nil {
		t.Fatal("zero user ID unexpectedly routed")
	}
	first, err := r.ShardForKey("device-123")
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.ShardForKey("device-123")
	if err != nil || first != second {
		t.Fatalf("guest key route is not stable: %q/%q, err=%v", first, second, err)
	}
}
