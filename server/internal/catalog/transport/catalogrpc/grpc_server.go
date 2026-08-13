package catalogrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type grpcServer struct {
	rpcv1.UnimplementedCatalogServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterCatalogServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) ListCatalogUnlocks(ctx context.Context, req *rpcv1.ListCatalogUnlocksRequest) (*rpcv1.ListCatalogUnlocksResponse, error) {
	if req.UserId <= 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	items, err := g.server.svc.ListCatalogUnlocks(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	out := &rpcv1.ListCatalogUnlocksResponse{}
	for _, item := range items {
		out.Unlocks = append(out.Unlocks, &rpcv1.CatalogUnlock{CatalogKey: item.CatalogKey, UnlockedAt: timestamppb.New(item.UnlockedAt)})
	}
	return out, nil
}
