package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/social/transport/socialrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type socialClientStub struct {
	listUserID int64
	inviteCode string
}

func (s *socialClientStub) CreateInvite(context.Context, int64) (string, error) {
	if s.inviteCode != "" {
		return s.inviteCode, nil
	}
	return "invite", nil
}
func (*socialClientStub) AcceptInvite(context.Context, string, int64) error { return nil }
func (s *socialClientStub) ListFriends(_ context.Context, userID int64) ([]socialrpc.FriendDTO, error) {
	s.listUserID = userID
	return []socialrpc.FriendDTO{{UserID: 121343, DisplayName: "小麦糖"}, {UserID: 80500742443532289, DisplayName: "Henry"}}, nil
}

func TestSocialFriendsReturnsDisplayNames(t *testing.T) {
	secret := []byte("social-handler-secret")
	stub := &socialClientStub{}
	mux := http.NewServeMux()
	NewSocialHandler(secret, stub).RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/social/friends", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	var body struct {
		Friends []struct {
			UserID      string `json:"user_id"`
			DisplayName string `json:"display_name"`
		} `json:"friends"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if stub.listUserID != 42 || len(body.Friends) != 2 || body.Friends[0].DisplayName != "小麦糖" || body.Friends[1].DisplayName != "Henry" || body.Friends[1].UserID != "80500742443532289" {
		t.Fatalf("unexpected response: user=%d body=%+v", stub.listUserID, body)
	}
}

func TestSocialCreateInviteReturnsClientRelativeSharePath(t *testing.T) {
	secret := []byte("social-handler-secret")
	stub := &socialClientStub{inviteCode: "invite code/+"}
	mux := http.NewServeMux()
	NewSocialHandler(secret, stub).RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/social/invite", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	var body struct {
		InviteCode string `json:"invite_code"`
		InvitePath string `json:"invite_path"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.InviteCode != "invite code/+" || body.InvitePath != "/invite?code=invite+code%2F%2B" {
		t.Fatalf("unexpected response: %+v", body)
	}
}
