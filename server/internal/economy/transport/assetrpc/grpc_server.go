package assetrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedAssetServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterAssetServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) GetPlayerAssets(ctx context.Context, req *rpcv1.GetPlayerAssetsRequest) (*rpcv1.GetPlayerAssetsResponse, error) {
	if req.UserId <= 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	assets, err := g.server.svc.GetPlayerAssets(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	out := &rpcv1.GetPlayerAssetsResponse{CoinBalance: assets.CoinBalance}
	for _, item := range assets.Inventory {
		out.Inventory = append(out.Inventory, &rpcv1.AssetInventoryItem{ItemType: item.ItemType, ItemId: item.ItemID, Quantity: item.Quantity})
	}
	return out, nil
}
