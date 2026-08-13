package infrastructure

import (
	"context"
	"fmt"
	"sort"
)

// ShardedRetentionReaper applies the same bounded terminal-record cleanup to
// every physical shard. A failed shard is reported after healthy shards have
// still received their cleanup pass.
type ShardedRetentionReaper struct{ reapers map[string]*RetentionReaper }

func NewShardedRetentionReaper(reapers map[string]*RetentionReaper) (*ShardedRetentionReaper, error) {
	if len(reapers) == 0 {
		return nil, fmt.Errorf("at least one retention reaper is required")
	}
	copyReapers := make(map[string]*RetentionReaper, len(reapers))
	for name, reaper := range reapers {
		if name == "" || reaper == nil {
			return nil, fmt.Errorf("invalid retention reaper shard")
		}
		copyReapers[name] = reaper
	}
	return &ShardedRetentionReaper{reapers: copyReapers}, nil
}

func (r *ShardedRetentionReaper) Reap(ctx context.Context) (int, error) {
	names := make([]string, 0, len(r.reapers))
	for name := range r.reapers {
		names = append(names, name)
	}
	sort.Strings(names)
	total := 0
	var firstErr error
	for _, name := range names {
		n, err := r.reapers[name].Reap(ctx)
		total += n
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("retention shard %s: %w", name, err)
		}
	}
	return total, firstErr
}
