package taskrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	taskinfra "github.com/photon/farm-server/server/internal/task/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type grpcServer struct {
	rpcv1.UnimplementedTaskServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterTaskServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) IncrProgress(ctx context.Context, req *rpcv1.IncrProgressRequest) (*rpcv1.IncrProgressResponse, error) {
	if req.UserId == 0 || req.TaskKey == "" {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id and task_key required"))
	}
	delta := int(req.Delta)
	if delta <= 0 {
		delta = 1
	}
	if err := g.server.svc.IncrProgress(ctx, req.UserId, req.TaskKey, delta); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.IncrProgressResponse{}, nil
}

func (g *grpcServer) ListTasks(ctx context.Context, req *rpcv1.ListTasksRequest) (*rpcv1.ListTasksResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	items, err := g.server.svc.ListTasks(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	out := &rpcv1.ListTasksResponse{}
	for _, item := range items {
		cfg, _ := taskinfra.GetTaskConfig(item.TaskKey)
		out.Tasks = append(out.Tasks, &rpcv1.TaskProgress{TaskKey: item.TaskKey, Description: cfg.Description, Progress: int32(item.Progress), Target: int32(cfg.Target), Status: string(item.Status), CoinReward: cfg.CoinReward, UpdatedAt: timestamppb.New(item.UpdatedAt)})
	}
	return out, nil
}

func (g *grpcServer) ClaimReward(ctx context.Context, req *rpcv1.ClaimRewardRequest) (*rpcv1.ClaimRewardResponse, error) {
	if req.UserId == 0 || req.TaskKey == "" {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id and task_key required"))
	}
	reward, err := g.server.svc.ClaimReward(ctx, req.UserId, req.TaskKey)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.ClaimRewardResponse{CoinReward: reward}, nil
}
