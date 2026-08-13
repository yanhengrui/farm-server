// Package socialrpc — Client 侧，由 gatesvr 使用。
// 通过 HTTP/JSON 调用 gamesvr 的好友服务端点。
package socialrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
)

// Client 通过 HTTP/JSON 调用 gamesvr 好友服务。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.SocialServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.SocialServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。baseURL 是 gamesvr 内部地址，例如 "http://gamesvr:9090"。
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// CreateInvite 调用 gamesvr 生成邀请码。
func (c *Client) CreateInvite(ctx context.Context, inviterUserID int64) (string, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.CreateInvite(ctx, &rpcv1.CreateInviteRequest{InviterUserId: inviterUserID})
		if err == nil {
			return out.InviteCode, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return "", rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(CreateInviteReq{InviterUserID: inviterUserID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/social/create-invite", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("http_do create_invite: %w", err)
	}
	defer resp.Body.Close()

	var out CreateInviteResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode_create_invite_resp: %w", err)
	}
	if out.Err != nil {
		return "", errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.InviteCode, nil
}

// AcceptInvite 调用 gamesvr 消费邀请码并建立好友关系。
func (c *Client) AcceptInvite(ctx context.Context, inviteCode string, accepterUserID int64) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.AcceptInvite(ctx, &rpcv1.AcceptInviteRequest{InviteCode: inviteCode, AccepterUserId: accepterUserID})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(AcceptInviteReq{InviteCode: inviteCode, AccepterUserID: accepterUserID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/social/accept-invite", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http_do accept_invite: %w", err)
	}
	defer resp.Body.Close()

	var out AcceptInviteResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_accept_invite_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// AreFriends 调用 gamesvr 查询两个用户是否是好友。
func (c *Client) AreFriends(ctx context.Context, userIDA, userIDB int64) (bool, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.AreFriends(ctx, &rpcv1.AreFriendsRequest{UserIdA: userIDA, UserIdB: userIDB})
		if err == nil {
			return out.Friends, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return false, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(AreFriendsReq{UserIDA: userIDA, UserIDB: userIDB})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/social/are-friends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("http_do are_friends: %w", err)
	}
	defer resp.Body.Close()

	var out AreFriendsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, fmt.Errorf("decode_are_friends_resp: %w", err)
	}
	if out.Err != nil {
		return false, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.Friends, nil
}

// ListFriends 调用 gamesvr 获取 userID 的好友列表。
func (c *Client) ListFriends(ctx context.Context, userID int64) ([]FriendDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.ListFriends(ctx, &rpcv1.ListFriendsRequest{UserId: userID})
		if err == nil {
			items := make([]FriendDTO, 0, len(out.Friends))
			for _, item := range out.Friends {
				items = append(items, newFriendDTO(item.UserId, item.DisplayName))
			}
			return items, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(ListFriendsReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/social/list-friends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_do list_friends: %w", err)
	}
	defer resp.Body.Close()

	var out ListFriendsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_list_friends_resp: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	for i := range out.Friends {
		out.Friends[i] = newFriendDTO(out.Friends[i].UserID, out.Friends[i].DisplayName)
	}
	return out.Friends, nil
}
