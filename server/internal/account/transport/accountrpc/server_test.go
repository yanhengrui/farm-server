package accountrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	accountdomain "github.com/photon/farm-server/server/internal/account/domain"
	"github.com/photon/farm-server/server/internal/account/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// stubAccountSvc 是 AccountSvc 的内存 stub，不依赖 DB。
type stubAccountSvc struct {
	deviceID    string
	displayName string
}

func (s *stubAccountSvc) GuestLogin(_ context.Context, deviceID, displayName string) (infrastructure.GuestLoginResult, error) {
	s.deviceID = deviceID
	s.displayName = displayName
	if deviceID == "" {
		return infrastructure.GuestLoginResult{}, errcode.New(errcode.CommonInvalidArgument, "device_id empty")
	}
	return infrastructure.GuestLoginResult{
		Account: accountdomain.Account{UserID: 99, FarmID: 99, AccountType: "GUEST", Status: "ACTIVE"},
		Session: accountdomain.SessionRecord{
			SessionID:    "sess-abc",
			UserID:       99,
			AccessToken:  "access-token-99",
			RefreshToken: "refresh-token-99",
			ExpiresAt:    time.Now().Add(24 * time.Hour),
		},
	}, nil
}

func (s *stubAccountSvc) Register(ctx context.Context, username, password, displayName string) (infrastructure.GuestLoginResult, error) {
	return s.GuestLogin(ctx, username, displayName)
}

func (s *stubAccountSvc) PasswordLogin(ctx context.Context, username, password string) (infrastructure.GuestLoginResult, error) {
	return s.GuestLogin(ctx, username, "Player")
}

func (s *stubAccountSvc) Logout(context.Context, string, string) error { return nil }

func (s *stubAccountSvc) RefreshSession(_ context.Context, sessionID, _ string) (infrastructure.RefreshSessionResult, error) {
	if sessionID == "" {
		return infrastructure.RefreshSessionResult{}, errcode.New(errcode.AuthUnauthorized, "bad session")
	}
	return infrastructure.RefreshSessionResult{
		AccessToken: "new-access-token",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
	}, nil
}

func (s *stubAccountSvc) Authenticate(accessToken string) (infrastructure.AuthenticateResult, error) {
	if accessToken == "bad" {
		return infrastructure.AuthenticateResult{}, errcode.New(errcode.AuthUnauthorized, "invalid token")
	}
	return infrastructure.AuthenticateResult{UserID: 42, FarmID: 42}, nil
}

func newTestAccountPair(t *testing.T) (*Client, *stubAccountSvc) {
	t.Helper()
	stub := &stubAccountSvc{}
	srv := NewServer(stub)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL), stub
}

// TestAccountRPC_Authenticate_OK 验证正常令牌校验返回 user_id。
func TestAccountRPC_Authenticate_OK(t *testing.T) {
	client, _ := newTestAccountPair(t)
	res, err := client.Authenticate(t.Context(), "valid-token")
	if err != nil {
		t.Fatalf("authenticate failed: %v", err)
	}
	if res.UserID != 42 {
		t.Errorf("expected user_id 42, got %d", res.UserID)
	}
}

// TestAccountRPC_Authenticate_Invalid 验证 "bad" 令牌返回 AUTH_UNAUTHORIZED。
func TestAccountRPC_Authenticate_Invalid(t *testing.T) {
	client, _ := newTestAccountPair(t)
	_, err := client.Authenticate(t.Context(), "bad")
	if err == nil {
		t.Fatal("expected error for bad token")
	}
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.AuthUnauthorized {
		t.Errorf("expected AUTH_UNAUTHORIZED, got %v", err)
	}
}

// TestAccountRPC_GuestLogin_OK 验证有效 device_id 登录返回 token。
func TestAccountRPC_GuestLogin_OK(t *testing.T) {
	client, stub := newTestAccountPair(t)
	res, err := client.GuestLogin(t.Context(), "device-001", "小麦糖")
	if err != nil {
		t.Fatalf("guest login failed: %v", err)
	}
	if res.AccessToken == "" {
		t.Error("expected non-empty access_token")
	}
	if res.UserID != 99 {
		t.Errorf("expected user_id 99, got %d", res.UserID)
	}
	if stub.deviceID != "device-001" || stub.displayName != "小麦糖" {
		t.Fatalf("guest login input not preserved: device_id=%q display_name=%q", stub.deviceID, stub.displayName)
	}
}

// TestAccountRPC_GuestLogin_EmptyDeviceID 验证空 device_id 返回 400。
func TestAccountRPC_GuestLogin_EmptyDeviceID(t *testing.T) {
	client, _ := newTestAccountPair(t)
	body, _ := json.Marshal(GuestLoginReq{DeviceID: ""})
	resp, err := client.http.Post(client.baseURL+"/rpc/account/guest-login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

// TestAccountRPC_Refresh_OK 验证刷新会话返回新 access_token。
func TestAccountRPC_Refresh_OK(t *testing.T) {
	client, _ := newTestAccountPair(t)
	res, err := client.RefreshSession(t.Context(), "session-1", "some-refresh-token")
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if res.AccessToken != "new-access-token" {
		t.Errorf("unexpected access_token: %q", res.AccessToken)
	}
}
