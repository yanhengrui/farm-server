package infrastructure

import (
	"context"
	"fmt"

	catalogdomain "github.com/photon/farm-server/server/internal/catalog/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedCatalogService keeps per-player catalog projections co-located with
// the player aggregate.
type ShardedCatalogService struct {
	router   *shard.Router
	services map[string]*MySQLCatalogService
}

func NewShardedCatalogService(router *shard.Router, services map[string]*MySQLCatalogService) (*ShardedCatalogService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLCatalogService, len(services))
	for _, name := range router.Shards() {
		if services[name] == nil {
			return nil, fmt.Errorf("missing catalog service for shard %q", name)
		}
		copyServices[name] = services[name]
	}
	return &ShardedCatalogService{router: router, services: copyServices}, nil
}

func (s *ShardedCatalogService) ListCatalogUnlocks(ctx context.Context, userID int64) ([]catalogdomain.Unlock, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return nil, err
	}
	return s.services[name].ListCatalogUnlocks(ctx, userID)
}

var _ catalogdomain.Service = (*ShardedCatalogService)(nil)
