package petrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedPetServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterPetServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) BuyPet(ctx context.Context, req *rpcv1.BuyPetRequest) (*rpcv1.BuyPetResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	if err := g.server.svc.BuyPet(ctx, req.UserId); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.BuyPetResponse{}, nil
}

func (g *grpcServer) HasPet(ctx context.Context, req *rpcv1.HasPetRequest) (*rpcv1.HasPetResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	status, err := g.server.svc.GetStatus(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.HasPetResponse{HasPet: status.HasPet, AutoHarvestEnabled: status.AutoHarvestEnabled}, nil
}

func (g *grpcServer) SetAutoHarvest(ctx context.Context, req *rpcv1.SetAutoHarvestRequest) (*rpcv1.SetAutoHarvestResponse, error) {
	if req.UserId <= 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	if err := g.server.svc.SetAutoHarvest(ctx, req.UserId, req.Enabled); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.SetAutoHarvestResponse{}, nil
}
