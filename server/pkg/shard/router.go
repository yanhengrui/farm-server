// Package shard contains deterministic, low-cardinality routing primitives for
// the Farm Server's player aggregate sharding.  It intentionally has no SQL,
// RPC, or environment dependency so routing remains easy to test.
package shard

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// Router maps a stable player/farm key to one logical shard.  Shard names are
// configuration identifiers (for example shard-0 and shard-1), not DSNs.
type Router struct {
	shards []string
}

// NewRouter validates and freezes a deterministic shard order.
func NewRouter(shards []string) (*Router, error) {
	if len(shards) < 1 {
		return nil, fmt.Errorf("at least one shard is required")
	}
	// Global IDs encode the shard in six low bits while routing uses ID modulo
	// shard count.  The two agree only when the count is a power of two <= 64.
	if count := len(shards); count > maxShardID+1 || count&(count-1) != 0 {
		return nil, fmt.Errorf("shard count %d is unsupported; expected a power of two between 1 and %d", count, maxShardID+1)
	}
	seen := make(map[string]struct{}, len(shards))
	normalized := make([]string, 0, len(shards))
	for _, raw := range shards {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, fmt.Errorf("shard name must not be empty")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate shard %q", name)
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	// The configuration order must not silently change routing.  A lexical
	// order makes equivalent maps/lists produce the same route everywhere.
	sort.Strings(normalized)
	return &Router{shards: normalized}, nil
}

// Shards returns a copy suitable for metric registration and diagnostics.
func (r *Router) Shards() []string {
	return append([]string(nil), r.shards...)
}

// ShardForUserID routes a stable positive user ID.  farm_id is currently equal
// to user_id, so ShardForFarmID deliberately uses the same mapping.
func (r *Router) ShardForUserID(userID int64) (string, error) {
	if r == nil || len(r.shards) == 0 {
		return "", fmt.Errorf("router is not configured")
	}
	if userID <= 0 {
		return "", fmt.Errorf("user_id must be positive")
	}
	// Both legacy and global IDs route by the same stable rule.  New global IDs
	// intentionally encode their logical shard in low bits, so the initial
	// two-shard deployment has no old/new-ID threshold or directory lookup.
	return r.shards[int(uint64(userID)%uint64(len(r.shards)))], nil
}

func (r *Router) ShardForFarmID(farmID int64) (string, error) {
	return r.ShardForUserID(farmID)
}

// ShardForKey deterministically places a pre-account key such as a guest
// device ID.  This is used only before a global user ID exists.
func (r *Router) ShardForKey(key string) (string, error) {
	if r == nil || len(r.shards) == 0 {
		return "", fmt.Errorf("router is not configured")
	}
	if key == "" {
		return "", fmt.Errorf("route key must not be empty")
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return r.shards[int(h.Sum64()%uint64(len(r.shards)))], nil
}
