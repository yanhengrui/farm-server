package infrastructure

import (
	"context"
	"fmt"
	"sort"
)

// OutboxScanner is intentionally small so every physical shard can run the
// same bounded relay implementation without sharing a database transaction.
type OutboxScanner interface {
	ScanAndPublish(context.Context) (int, error)
}

// ShardedOutboxScanner drains every shard independently.  A failed shard is
// reported after the remaining shards have still been given one chance to
// drain, preventing a single database outage from starving healthy shards.
type ShardedOutboxScanner struct {
	names    []string
	scanners map[string]OutboxScanner
}

func NewShardedOutboxScanner(scanners map[string]OutboxScanner) (*ShardedOutboxScanner, error) {
	if len(scanners) == 0 {
		return nil, fmt.Errorf("at least one outbox scanner is required")
	}
	copyScanners := make(map[string]OutboxScanner, len(scanners))
	names := make([]string, 0, len(scanners))
	for name, scanner := range scanners {
		if name == "" || scanner == nil {
			return nil, fmt.Errorf("outbox shard name and scanner are required")
		}
		copyScanners[name] = scanner
		names = append(names, name)
	}
	sort.Strings(names)
	return &ShardedOutboxScanner{names: names, scanners: copyScanners}, nil
}

func (s *ShardedOutboxScanner) ScanAndPublish(ctx context.Context) (int, error) {
	total := 0
	var firstErr error
	for _, name := range s.names {
		n, err := s.scanners[name].ScanAndPublish(ctx)
		total += n
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shard %s: %w", name, err)
		}
	}
	return total, firstErr
}
