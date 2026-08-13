package socialrpc

import (
	"context"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type grpcServer struct {
	rpcv1.UnimplementedSocialServiceServer
	server *Server
}

func (s *Server) RegisterGRPC(reg grpc.ServiceRegistrar) {
	rpcv1.RegisterSocialServiceServer(reg, &grpcServer{server: s})
}

func (g *grpcServer) CreateInvite(ctx context.Context, req *rpcv1.CreateInviteRequest) (*rpcv1.CreateInviteResponse, error) {
	if req.InviterUserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "inviter_user_id required"))
	}
	code, err := g.server.svc.CreateInvite(ctx, req.InviterUserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.CreateInviteResponse{InviteCode: code}, nil
}

func (g *grpcServer) AcceptInvite(ctx context.Context, req *rpcv1.AcceptInviteRequest) (*rpcv1.AcceptInviteResponse, error) {
	if req.InviteCode == "" || req.AccepterUserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "invite_code and accepter_user_id required"))
	}
	if err := g.server.svc.AcceptInvite(ctx, req.InviteCode, req.AccepterUserId); err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.AcceptInviteResponse{}, nil
}

func (g *grpcServer) AreFriends(ctx context.Context, req *rpcv1.AreFriendsRequest) (*rpcv1.AreFriendsResponse, error) {
	if req.UserIdA == 0 || req.UserIdB == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id_a and user_id_b required"))
	}
	ok, err := g.server.svc.AreFriends(ctx, req.UserIdA, req.UserIdB)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	return &rpcv1.AreFriendsResponse{Friends: ok}, nil
}

func (g *grpcServer) ListFriends(ctx context.Context, req *rpcv1.ListFriendsRequest) (*rpcv1.ListFriendsResponse, error) {
	if req.UserId == 0 {
		return nil, rpcgrpc.ToError(errcode.New(errcode.CommonInvalidArgument, "user_id required"))
	}
	items, err := g.server.svc.ListFriends(ctx, req.UserId)
	if err != nil {
		return nil, rpcgrpc.ToError(err)
	}
	out := &rpcv1.ListFriendsResponse{}
	for _, item := range items {
		friend := newFriendDTO(item.UserID, item.DisplayName)
		out.Friends = append(out.Friends, &rpcv1.Friend{UserId: friend.UserID, DisplayName: friend.DisplayName})
	}
	return out, nil
}
