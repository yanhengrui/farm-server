package farmsvc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/transport/rpcconvert"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedFarmCommandServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterFarmCommandServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) SubmitCommand(ctx context.Context, req *rpcv1.SubmitCommandRequest) (*rpcv1.SubmitCommandResponse, error) {
	r, err := g.server.submitter.Submit(ctx, rpcconvert.CommandFromProto(req.Command))
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.SubmitCommandResponse{Result: rpcconvert.ResultToProto(r)}, nil
}
