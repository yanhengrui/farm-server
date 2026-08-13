package accountrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/account/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedAccountServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterAccountServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) GuestLogin(ctx context.Context, req *rpcv1.GuestLoginRequest) (*rpcv1.GuestLoginResponse, error) {
	if req.DeviceId == "" {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "device_id required"))
	}
	r, err := g.server.svc.GuestLogin(ctx, req.DeviceId, req.DisplayName)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.GuestLoginResponse{UserId: r.Account.UserID, FarmId: r.Account.FarmID, AccessToken: r.Session.AccessToken, RefreshToken: r.Session.RefreshToken, SessionId: r.Session.SessionID, ExpiresIn: int32(accessTokenTTL.Seconds()), DisplayName: r.Account.DisplayName}, nil
}

func sessionResponse(r infrastructure.GuestLoginResult) *rpcv1.GuestLoginResponse {
	return &rpcv1.GuestLoginResponse{UserId: r.Account.UserID, FarmId: r.Account.FarmID, AccessToken: r.Session.AccessToken, RefreshToken: r.Session.RefreshToken, SessionId: r.Session.SessionID, ExpiresIn: int32(accessTokenTTL.Seconds()), DisplayName: r.Account.DisplayName}
}

func (g *grpcServer) Register(ctx context.Context, req *rpcv1.RegisterRequest) (*rpcv1.GuestLoginResponse, error) {
	r, err := g.server.svc.Register(ctx, req.Username, req.Password, req.DisplayName)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return sessionResponse(r), nil
}

func (g *grpcServer) PasswordLogin(ctx context.Context, req *rpcv1.PasswordLoginRequest) (*rpcv1.GuestLoginResponse, error) {
	r, err := g.server.svc.PasswordLogin(ctx, req.Username, req.Password)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return sessionResponse(r), nil
}

func (g *grpcServer) RefreshSession(ctx context.Context, req *rpcv1.RefreshSessionRequest) (*rpcv1.RefreshSessionResponse, error) {
	r, err := g.server.svc.RefreshSession(ctx, req.SessionId, req.RefreshToken)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.RefreshSessionResponse{AccessToken: r.AccessToken, RefreshToken: r.RefreshToken, ExpiresIn: int32(accessTokenTTL.Seconds())}, nil
}

func (g *grpcServer) Logout(ctx context.Context, req *rpcv1.LogoutRequest) (*rpcv1.LogoutResponse, error) {
	if err := g.server.svc.Logout(ctx, req.SessionId, req.RefreshToken); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.LogoutResponse{Ok: true}, nil
}

func (g *grpcServer) Authenticate(_ context.Context, req *rpcv1.AuthenticateRequest) (*rpcv1.AuthenticateResponse, error) {
	r, err := g.server.svc.Authenticate(req.AccessToken)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.AuthenticateResponse{UserId: r.UserID, FarmId: r.FarmID}, nil
}
