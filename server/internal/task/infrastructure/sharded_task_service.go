package infrastructure

import (
	"context"
	"fmt"

	taskdomain "github.com/photon/farm-server/server/internal/task/domain"
	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedTaskService keeps task progress and reward wallet writes on the
// owning player's shard; Kafka projectors reach it through gamesvr RPC.
type ShardedTaskService struct {
	router   *shard.Router
	services map[string]*MySQLTaskService
}

func NewShardedTaskService(router *shard.Router, services map[string]*MySQLTaskService) (*ShardedTaskService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLTaskService, len(services))
	for _, name := range router.Shards() {
		if services[name] == nil {
			return nil, fmt.Errorf("missing task service for shard %q", name)
		}
		copyServices[name] = services[name]
	}
	return &ShardedTaskService{router: router, services: copyServices}, nil
}

func (s *ShardedTaskService) service(userID int64) (*MySQLTaskService, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return nil, err
	}
	return s.services[name], nil
}

func (s *ShardedTaskService) IncrProgress(ctx context.Context, userID int64, taskKey string, delta int) error {
	service, err := s.service(userID)
	if err != nil {
		return err
	}
	return service.IncrProgress(ctx, userID, taskKey, delta)
}
func (s *ShardedTaskService) ListTasks(ctx context.Context, userID int64) ([]taskdomain.TaskProgress, error) {
	service, err := s.service(userID)
	if err != nil {
		return nil, err
	}
	return service.ListTasks(ctx, userID)
}
func (s *ShardedTaskService) ClaimReward(ctx context.Context, userID int64, taskKey string) (int64, error) {
	service, err := s.service(userID)
	if err != nil {
		return 0, err
	}
	return service.ClaimReward(ctx, userID, taskKey)
}

var _ taskdomain.TaskService = (*ShardedTaskService)(nil)
