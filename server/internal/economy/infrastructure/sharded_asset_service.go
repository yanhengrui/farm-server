package infrastructure

import (
	"context"
	"fmt"

	economydomain "github.com/photon/farm-server/server/internal/economy/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedAssetService routes each player's authoritative wallet and inventory
// read to the same physical shard as that player.
type ShardedAssetService struct {
	router   *shard.Router
	services map[string]*MySQLAssetService
}

func NewShardedAssetService(router *shard.Router, services map[string]*MySQLAssetService) (*ShardedAssetService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLAssetService, len(services))
	for _, name := range router.Shards() {
		if services[name] == nil {
			return nil, fmt.Errorf("missing asset service for shard %q", name)
		}
		copyServices[name] = services[name]
	}
	return &ShardedAssetService{router: router, services: copyServices}, nil
}

func (s *ShardedAssetService) GetPlayerAssets(ctx context.Context, userID int64) (economydomain.Assets, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return economydomain.Assets{}, err
	}
	return s.services[name].GetPlayerAssets(ctx, userID)
}

var _ economydomain.AssetService = (*ShardedAssetService)(nil)
