package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/account/transport/accountrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// stubAccountClient 是 AccountClient 的内存 stub。
type stubAccountClient struct {
	deviceID    string
	displayName string
	username    string
	password    string
}

func (s *stubAccountClient) GuestLogin(_ context.Context, deviceID, displayName string) (accountrpc.GuestLoginResult, error) {
	s.deviceID = deviceID
	s.displayName = displayName
	return accountrpc.GuestLoginResult{
		UserID:       1001,
		FarmID:       1001,
		AccessToken:  "tok-" + deviceID,
		RefreshToken: "rt-" + deviceID,
		SessionID:    "sess-1",
	}, nil
}

func (s *stubAccountClient) Register(ctx context.Context, username, password, displayName string) (accountrpc.GuestLoginResult, error) {
	s.username, s.password = username, password
	return s.GuestLogin(ctx, username, displayName)
}

func (s *stubAccountClient) PasswordLogin(ctx context.Context, username, password string) (accountrpc.GuestLoginResult, error) {
	s.username, s.password = username, password
	return s.GuestLogin(ctx, username, "Player")
}

func (s *stubAccountClient) Logout(context.Context, string, string) error { return nil }

func (s *stubAccountClient) RefreshSession(_ context.Context, _, _ string) (accountrpc.RefreshResult, error) {
	return accountrpc.RefreshResult{AccessToken: "new-tok"}, nil
}

func newTestAuthServer(t *testing.T) (*httptest.Server, *stubAccountClient) {
	t.Helper()
	stub := &stubAccountClient{}
	h := NewAuthHandler(stub)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, stub
}

func TestGuestLogin_OK(t *testing.T) {
	ts, stub := newTestAuthServer(t)

	body, _ := json.Marshal(GuestLoginRequest{DeviceID: "device-abc", DisplayName: "小麦糖"})
	resp, err := ts.Client().Post(ts.URL+"/api/v1/auth/guest-login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out GuestLoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.AccessToken == "" {
		t.Error("expected non-empty access_token")
	}
	if stub.deviceID != "device-abc" || stub.displayName != "小麦糖" {
		t.Fatalf("gateway did not preserve guest login input: device_id=%q display_name=%q", stub.deviceID, stub.displayName)
	}
}

func TestRegisterAndPasswordLogin(t *testing.T) {
	ts, stub := newTestAuthServer(t)
	registerBody := `{"username":"farmer_01","password":"password-123","display_name":"麦芽糖"}`
	resp, err := ts.Client().Post(ts.URL+"/api/v1/auth/register", "application/json", strings.NewReader(registerBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register status=%d", resp.StatusCode)
	}
	if stub.username != "farmer_01" || stub.password != "password-123" || stub.displayName != "麦芽糖" {
		t.Fatalf("register input lost: %+v", stub)
	}

	loginBody := `{"username":"farmer_01","password":"password-123"}`
	resp, err = ts.Client().Post(ts.URL+"/api/v1/auth/login", "application/json", strings.NewReader(loginBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d", resp.StatusCode)
	}
}

func TestGuestLogin_SameDeviceStableToken(t *testing.T) {
	ts, _ := newTestAuthServer(t)

	login := func() GuestLoginResponse {
		body, _ := json.Marshal(GuestLoginRequest{DeviceID: "stable-device"})
		resp, err := ts.Client().Post(ts.URL+"/api/v1/auth/guest-login", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Errorf("post failed: %v", err)
			return GuestLoginResponse{}
		}
		defer resp.Body.Close()
		var out GuestLoginResponse
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
	r1, r2 := login(), login()
	if r1.UserID != r2.UserID {
		t.Errorf("expected stable user_id, got %d and %d", r1.UserID, r2.UserID)
	}
}

func TestGuestLogin_MissingDeviceID(t *testing.T) {
	ts, _ := newTestAuthServer(t)

	body, _ := json.Marshal(GuestLoginRequest{})
	resp, err := ts.Client().Post(ts.URL+"/api/v1/auth/guest-login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestRefresh_OK(t *testing.T) {
	ts, _ := newTestAuthServer(t)

	body, _ := json.Marshal(RefreshRequest{SessionID: "sess-1", RefreshToken: "rt-xxx"})
	resp, err := ts.Client().Post(ts.URL+"/api/v1/auth/refresh", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestWriteErrFromErrorPreservesRetryMetadata(t *testing.T) {
	rr := httptest.NewRecorder()
	writeErrFromError(rr, errcode.NewRetry(errcode.ResourceExhausted, "busy", 50*time.Millisecond))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After-Ms"); got != "50" {
		t.Fatalf("retry-after-ms=%q", got)
	}
}

func TestWriteErrFromErrorClassifiesDownstreamTimeout(t *testing.T) {
	rr := httptest.NewRecorder()
	writeErrFromError(rr, context.DeadlineExceeded)
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("X-Capacity-Reason") != errcode.CapacityReasonDownstreamTimeout {
		t.Fatalf("status=%d reason=%q body=%s", rr.Code, rr.Header().Get("X-Capacity-Reason"), rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), string(errcode.ResourceExhausted)) || !strings.Contains(rr.Body.String(), errcode.CapacityReasonDownstreamTimeout) {
		t.Fatalf("body=%s", rr.Body.String())
	}
}
