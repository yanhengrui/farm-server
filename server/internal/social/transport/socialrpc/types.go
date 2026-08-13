// Package socialrpc 提供 gatesvr → gamesvr 的好友服务 HTTP/JSON RPC 传输层。
package socialrpc

import (
	"strconv"
	"strings"
)

// CreateInviteReq 是 /rpc/social/create-invite 请求体。
type CreateInviteReq struct {
	InviterUserID int64 `json:"inviter_user_id"`
}

// CreateInviteResp 是 /rpc/social/create-invite 响应体。
type CreateInviteResp struct {
	InviteCode string  `json:"invite_code,omitempty"`
	Err        *RPCErr `json:"error,omitempty"`
}

// AcceptInviteReq 是 /rpc/social/accept-invite 请求体。
type AcceptInviteReq struct {
	InviteCode     string `json:"invite_code"`
	AccepterUserID int64  `json:"accepter_user_id"`
}

// AcceptInviteResp 是 /rpc/social/accept-invite 响应体。
type AcceptInviteResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// AreFriendsReq 是 /rpc/social/are-friends 请求体。
type AreFriendsReq struct {
	UserIDA int64 `json:"user_id_a"`
	UserIDB int64 `json:"user_id_b"`
}

// AreFriendsResp 是 /rpc/social/are-friends 响应体。
type AreFriendsResp struct {
	Friends bool    `json:"friends"`
	Err     *RPCErr `json:"error,omitempty"`
}

// ListFriendsReq 是 /rpc/social/list-friends 请求体。
type ListFriendsReq struct {
	UserID int64 `json:"user_id"`
}

// FriendDTO 是好友列表中的单个好友信息。
type FriendDTO struct {
	UserID      int64  `json:"user_id"`
	DisplayName string `json:"display_name"`
}

func newFriendDTO(userID int64, displayName string) FriendDTO {
	if strings.TrimSpace(displayName) == "" {
		displayName = strconv.FormatInt(userID, 10)
	}
	return FriendDTO{UserID: userID, DisplayName: displayName}
}

// ListFriendsResp 是 /rpc/social/list-friends 响应体。
type ListFriendsResp struct {
	Friends []FriendDTO `json:"friends"`
	Err     *RPCErr     `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误；Code 对应 errcode.Code。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
