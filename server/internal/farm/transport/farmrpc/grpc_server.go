package farmrpc

import (
	"context"
	"sort"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/transport/rpcconvert"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedFarmServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterFarmServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) CommitFarmCommand(ctx context.Context, req *rpcv1.CommitFarmCommandRequest) (*rpcv1.CommitFarmCommandResponse, error) {
	r, err := g.server.committer.CommitFarmCommand(ctx, application.CommitRequest{Command: rpcconvert.CommandFromProto(req.Command)})
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.CommitFarmCommandResponse{Result: rpcconvert.ResultToProto(r)}, nil
}

func (g *grpcServer) LoadSnapshot(ctx context.Context, req *rpcv1.LoadSnapshotRequest) (*rpcv1.LoadSnapshotResponse, error) {
	s, err := g.server.loader.LoadSnapshot(ctx, req.FarmId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.LoadSnapshotResponse{Snapshot: rpcconvert.SnapshotToProto(s)}, nil
}

func (g *grpcServer) GetSnapshot(ctx context.Context, req *rpcv1.GetSnapshotRequest) (*rpcv1.GetSnapshotResponse, error) {
	s, err := g.server.loader.LoadSnapshot(ctx, req.FarmId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	ownerDisplayName, err := g.server.loadOwnerDisplayName(ctx, s.OwnerID)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	view := &rpcv1.FarmSnapshotView{FarmId: s.FarmID, OwnerUserId: s.OwnerID, OwnerDisplayName: ownerDisplayName, Version: s.Version}
	now := time.Now().UTC()
	for _, p := range s.Plots {
		view.Plots = append(view.Plots, &rpcv1.PlotView{PlotId: p.PlotID, Status: string(p.Status), CropId: p.CropID, GrowthStage: string(p.GrowthStage(now)), PlantedAt: formatTime(p.PlantedAt), MatureAt: formatTime(p.MatureAt), RemainingYield: p.RemainingYield})
	}
	sort.Slice(view.Plots, func(i, j int) bool { return view.Plots[i].PlotId < view.Plots[j].PlotId })
	return &rpcv1.GetSnapshotResponse{Snapshot: view}, nil
}

func (g *grpcServer) AdvanceRouteEpoch(ctx context.Context, req *rpcv1.AdvanceRouteEpochRequest) (*rpcv1.AdvanceRouteEpochResponse, error) {
	fencer, ok := g.server.committer.(application.RouteFencer)
	if !ok {
		return nil, rpcgrpc.ToError(errcode.New(errcode.Internal, "route fencer not configured"))
	}
	if err := fencer.AdvanceRouteEpoch(ctx, req.FarmId, req.RouteEpoch); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.AdvanceRouteEpochResponse{}, nil
}
