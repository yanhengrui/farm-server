package infrastructure

import (
	"context"
	"fmt"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardCommitBackend keeps all operations for one farm aggregate on the same
// physical shard and transaction boundary.
type ShardCommitBackend struct {
	Committer application.Committer
	Loader    application.SnapshotLoader
	Fencer    application.RouteFencer
}

// ShardedCommitter delegates every farm operation by farm_id.  It prevents an
// actor's cold-load, route fence and commit from accidentally using different
// database pools.
type ShardedCommitter struct {
	router   *shard.Router
	backends map[string]ShardCommitBackend
}

func NewShardedCommitter(router *shard.Router, backends map[string]ShardCommitBackend) (*ShardedCommitter, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyBackends := make(map[string]ShardCommitBackend, len(backends))
	for _, name := range router.Shards() {
		backend, ok := backends[name]
		if !ok || backend.Committer == nil || backend.Loader == nil || backend.Fencer == nil {
			return nil, fmt.Errorf("incomplete commit backend for shard %q", name)
		}
		copyBackends[name] = backend
	}
	if len(copyBackends) != len(backends) {
		return nil, fmt.Errorf("commit backends contain an unknown shard")
	}
	return &ShardedCommitter{router: router, backends: copyBackends}, nil
}

func (s *ShardedCommitter) backend(farmID int64) (ShardCommitBackend, error) {
	name, err := s.router.ShardForFarmID(farmID)
	if err != nil {
		return ShardCommitBackend{}, err
	}
	return s.backends[name], nil
}

func (s *ShardedCommitter) CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	backend, err := s.backend(req.Command.FarmID)
	if err != nil {
		return application.CommitResult{}, err
	}
	return backend.Committer.CommitFarmCommand(ctx, req)
}

func (s *ShardedCommitter) LoadSnapshot(ctx context.Context, farmID int64) (domain.Snapshot, error) {
	backend, err := s.backend(farmID)
	if err != nil {
		return domain.Snapshot{}, err
	}
	return backend.Loader.LoadSnapshot(ctx, farmID)
}

func (s *ShardedCommitter) AdvanceRouteEpoch(ctx context.Context, farmID, epoch int64) error {
	backend, err := s.backend(farmID)
	if err != nil {
		return err
	}
	return backend.Fencer.AdvanceRouteEpoch(ctx, farmID, epoch)
}

var (
	_ application.Committer      = (*ShardedCommitter)(nil)
	_ application.SnapshotLoader = (*ShardedCommitter)(nil)
	_ application.RouteFencer    = (*ShardedCommitter)(nil)
)
