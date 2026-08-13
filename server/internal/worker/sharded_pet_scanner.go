package worker

import (
	"context"
	"fmt"
	"sort"
)

// ShardedPetScanner scans every physical player shard. A failed shard is
// recorded but does not prevent healthy shards from receiving their scan.
type ShardedPetScanner struct{ scanners map[string]*PetScanner }

func NewShardedPetScanner(scanners map[string]*PetScanner) (*ShardedPetScanner, error) {
	if len(scanners) == 0 {
		return nil, fmt.Errorf("at least one pet scanner is required")
	}
	copyScanners := make(map[string]*PetScanner, len(scanners))
	for name, scanner := range scanners {
		if name == "" || scanner == nil {
			return nil, fmt.Errorf("invalid pet scanner shard")
		}
		copyScanners[name] = scanner
	}
	return &ShardedPetScanner{scanners: copyScanners}, nil
}

// Scan visits physical shards in a deterministic order. Each shard owns its
// own database-backed claims, so an unhealthy shard does not prevent the
// healthy shards from draining their due work.
func (s *ShardedPetScanner) Scan(ctx context.Context, batchSize int) (PetScanResult, error) {
	names := make([]string, 0, len(s.scanners))
	for name := range s.scanners {
		names = append(names, name)
	}
	sort.Strings(names)
	var total PetScanResult
	var firstErr error
	for _, name := range names {
		result, err := s.scanners[name].Scan(ctx, batchSize)
		total.Claimed += result.Claimed
		total.Harvested += result.Harvested
		total.Rescheduled += result.Rescheduled
		total.SubmitFailed += result.SubmitFailed
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pet scan shard %s: %w", name, err)
		}
	}
	return total, firstErr
}

func (s *ShardedPetScanner) ScanAndHarvest(ctx context.Context, batchSize int) (int, error) {
	result, err := s.Scan(ctx, batchSize)
	return result.Harvested, err
}
