// Package accountrpc — Client 侧，由 gatesvr 使用。
// 通过 HTTP/JSON 调用 gamesvr 的账号服务端点。
package accountrpc

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

// Client 通过 HTTP/JSON 调用 gamesvr 账号服务。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.AccountServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.AccountServiceClient, mode rpcgrpc.Mode) *Client {
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

// GuestLoginResult 是 GuestLogin 的客户端视图。
type GuestLoginResult struct {
	UserID       int64
	FarmID       int64
	AccessToken  string
	RefreshToken string
	SessionID    string
	DisplayName  string
}

func guestLoginResult(out *rpcv1.GuestLoginResponse) GuestLoginResult {
	return GuestLoginResult{UserID: out.UserId, FarmID: out.FarmId, AccessToken: out.AccessToken, RefreshToken: out.RefreshToken, SessionID: out.SessionId, DisplayName: out.DisplayName}
}

func guestLoginResultFromHTTP(out GuestLoginResp) GuestLoginResult {
	return GuestLoginResult{UserID: out.UserID, FarmID: out.FarmID, AccessToken: out.AccessToken, RefreshToken: out.RefreshToken, SessionID: out.SessionID, DisplayName: out.DisplayName}
}

// GuestLogin 调用 gamesvr 游客登录端点。
func (c *Client) GuestLogin(ctx context.Context, deviceID, displayName string) (GuestLoginResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.GuestLogin(ctx, &rpcv1.GuestLoginRequest{DeviceId: deviceID, DisplayName: displayName})
		if err == nil {
			return guestLoginResult(out), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return GuestLoginResult{}, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(GuestLoginReq{DeviceID: deviceID, DisplayName: displayName})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/account/guest-login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("http_do guest_login: %w", err)
	}
	defer resp.Body.Close()

	var out GuestLoginResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return GuestLoginResult{}, fmt.Errorf("decode_guest_login_resp: %w", err)
	}
	if out.Err != nil {
		return GuestLoginResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return GuestLoginResult{
		UserID:       out.UserID,
		FarmID:       out.FarmID,
		AccessToken:  out.AccessToken,
		RefreshToken: out.RefreshToken,
		SessionID:    out.SessionID,
		DisplayName:  out.DisplayName,
	}, nil
}

func (c *Client) Register(ctx context.Context, username, password, displayName string) (GuestLoginResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.Register(ctx, &rpcv1.RegisterRequest{Username: username, Password: password, DisplayName: displayName})
		if err == nil {
			return guestLoginResult(out), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return GuestLoginResult{}, rpcgrpc.FromError(err)
		}
	}
	return c.doLogin(ctx, "/rpc/account/register", RegisterReq{Username: username, Password: password, DisplayName: displayName})
}

func (c *Client) PasswordLogin(ctx context.Context, username, password string) (GuestLoginResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.PasswordLogin(ctx, &rpcv1.PasswordLoginRequest{Username: username, Password: password})
		if err == nil {
			return guestLoginResult(out), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return GuestLoginResult{}, rpcgrpc.FromError(err)
		}
	}
	return c.doLogin(ctx, "/rpc/account/password-login", PasswordLoginReq{Username: username, Password: password})
}

func (c *Client) doLogin(ctx context.Context, path string, input any) (GuestLoginResult, error) {
	body, _ := json.Marshal(input)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("http_do login: %w", err)
	}
	defer resp.Body.Close()
	var out GuestLoginResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return GuestLoginResult{}, fmt.Errorf("decode login response: %w", err)
	}
	if out.Err != nil {
		return GuestLoginResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return guestLoginResultFromHTTP(out), nil
}

// RefreshResult 是 RefreshSession 的客户端视图。
type RefreshResult struct {
	AccessToken  string
	RefreshToken string
}

// RefreshSession 调用 gamesvr 刷新会话端点。
func (c *Client) RefreshSession(ctx context.Context, sessionID, refreshToken string) (RefreshResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.RefreshSession(ctx, &rpcv1.RefreshSessionRequest{SessionId: sessionID, RefreshToken: refreshToken})
		if err == nil {
			return RefreshResult{AccessToken: out.AccessToken, RefreshToken: out.RefreshToken}, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return RefreshResult{}, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(RefreshReq{SessionID: sessionID, RefreshToken: refreshToken})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/account/refresh", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return RefreshResult{}, fmt.Errorf("http_do refresh: %w", err)
	}
	defer resp.Body.Close()

	var out RefreshResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return RefreshResult{}, fmt.Errorf("decode_refresh_resp: %w", err)
	}
	if out.Err != nil {
		return RefreshResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return RefreshResult{AccessToken: out.AccessToken, RefreshToken: out.RefreshToken}, nil
}

func (c *Client) Logout(ctx context.Context, sessionID, refreshToken string) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.Logout(ctx, &rpcv1.LogoutRequest{SessionId: sessionID, RefreshToken: refreshToken})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(LogoutReq{SessionID: sessionID, RefreshToken: refreshToken})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/account/logout", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http_do logout: %w", err)
	}
	defer resp.Body.Close()
	var out LogoutResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode logout response: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// AuthenticateResult 是 Authenticate 的客户端视图。
type AuthenticateResult struct {
	UserID int64
	FarmID int64
}

// Authenticate 调用 gamesvr 令牌校验端点。
func (c *Client) Authenticate(ctx context.Context, accessToken string) (AuthenticateResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.Authenticate(ctx, &rpcv1.AuthenticateRequest{AccessToken: accessToken})
		if err == nil {
			return AuthenticateResult{UserID: out.UserId, FarmID: out.FarmId}, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return AuthenticateResult{}, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(AuthenticateReq{AccessToken: accessToken})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/account/authenticate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return AuthenticateResult{}, fmt.Errorf("http_do authenticate: %w", err)
	}
	defer resp.Body.Close()

	var out AuthenticateResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return AuthenticateResult{}, fmt.Errorf("decode_authenticate_resp: %w", err)
	}
	if out.Err != nil {
		return AuthenticateResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return AuthenticateResult{UserID: out.UserID, FarmID: out.FarmID}, nil
}
