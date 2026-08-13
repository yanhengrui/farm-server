package infrastructure

import (
	"context"
	"fmt"

	petdomain "github.com/photon/farm-server/server/internal/pet/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedPetService routes every pet/farm/wallet transaction by owner user.
// Farm ID equals owner user ID in the current aggregate model, so this keeps
// the pet's farm lock and wallet mutation inside one shard transaction.
type ShardedPetService struct {
	router   *shard.Router
	services map[string]*MySQLPetService
}

func NewShardedPetService(router *shard.Router, services map[string]*MySQLPetService) (*ShardedPetService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLPetService, len(services))
	for _, name := range router.Shards() {
		if services[name] == nil {
			return nil, fmt.Errorf("missing pet service for shard %q", name)
		}
		copyServices[name] = services[name]
	}
	return &ShardedPetService{router: router, services: copyServices}, nil
}

func (s *ShardedPetService) service(userID int64) (*MySQLPetService, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return nil, err
	}
	return s.services[name], nil
}

func (s *ShardedPetService) BuyPet(ctx context.Context, userID int64) error {
	service, err := s.service(userID)
	if err != nil {
		return err
	}
	return service.BuyPet(ctx, userID)
}

func (s *ShardedPetService) HasPet(ctx context.Context, userID int64) (bool, error) {
	service, err := s.service(userID)
	if err != nil {
		return false, err
	}
	return service.HasPet(ctx, userID)
}

func (s *ShardedPetService) GetStatus(ctx context.Context, userID int64) (petdomain.PlayerStatus, error) {
	service, err := s.service(userID)
	if err != nil {
		return petdomain.PlayerStatus{}, err
	}
	return service.GetStatus(ctx, userID)
}

func (s *ShardedPetService) SetAutoHarvest(ctx context.Context, userID int64, enabled bool) error {
	service, err := s.service(userID)
	if err != nil {
		return err
	}
	return service.SetAutoHarvest(ctx, userID, enabled)
}
