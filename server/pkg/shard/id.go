package shard

import (
	"fmt"
	"sync"
	"time"
)

// IDGenerator creates positive int64 IDs without a cross-shard auto-increment.
// Layout: milliseconds since epoch (41 bits), shard (6), node (8), sequence
// (8).  This supports 64 logical shards, 256 game-server instances and 256
// account creations per millisecond per instance for roughly 69 years.
type IDGenerator struct {
	mu       sync.Mutex
	epochMS  int64
	shard    uint8
	node     uint8
	lastMS   int64
	sequence uint8
}

const (
	sequenceBits = 8
	nodeBits     = 8
	shardBits    = 6
	maxShardID   = (1 << shardBits) - 1
)

func NewIDGenerator(epoch time.Time, shardID, nodeID uint8) (*IDGenerator, error) {
	if shardID > maxShardID {
		return nil, fmt.Errorf("shard ID %d exceeds %d", shardID, maxShardID)
	}
	return &IDGenerator{epochMS: epoch.UTC().UnixMilli(), shard: shardID, node: nodeID}, nil
}

// Next returns a globally unique ID for the supplied wall-clock time.  The
// caller should retry on ErrSequenceExhausted instead of spinning indefinitely.
func (g *IDGenerator) Next(now time.Time) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := now.UTC().UnixMilli() - g.epochMS
	if ms < 0 {
		return 0, fmt.Errorf("clock precedes configured ID epoch")
	}
	if ms == g.lastMS {
		if g.sequence == ^uint8(0) {
			return 0, ErrSequenceExhausted
		}
		g.sequence++
	} else {
		g.lastMS = ms
		g.sequence = 0
	}
	// Keep the shard bits at the low end.  For the initial two-shard rollout
	// this makes id %% 2 equal the encoded shard, so legacy and newly generated
	// IDs share exactly one routing rule and no threshold heuristic is needed.
	value := ((((uint64(ms)<<nodeBits)|uint64(g.node))<<sequenceBits)|uint64(g.sequence))<<shardBits | uint64(g.shard)
	if value > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("generated ID exceeds signed int64 range")
	}
	return int64(value), nil
}

// ErrSequenceExhausted is a bounded overload signal: a caller can retry after
// the next millisecond, rather than creating duplicate IDs or an unbounded loop.
var ErrSequenceExhausted = fmt.Errorf("global ID sequence exhausted for millisecond")

// ShardIDFromGlobalID extracts the encoded shard placement used for new users.
func ShardIDFromGlobalID(id int64) (uint8, error) {
	if id <= 0 {
		return 0, fmt.Errorf("global ID must be positive")
	}
	return uint8(uint64(id) & maxShardID), nil
}
