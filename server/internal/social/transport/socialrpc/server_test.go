package socialrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	socialdomain "github.com/photon/farm-server/server/internal/social/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// stubSocialSvc 是 SocialSvc 的内存 stub，不依赖 DB。
type stubSocialSvc struct {
	friends map[[2]int64]bool
	invites map[string]int64 // code → inviterID
}

func newStubSvc() *stubSocialSvc {
	return &stubSocialSvc{
		friends: map[[2]int64]bool{},
		invites: map[string]int64{"valid-code": 100},
	}
}

func (s *stubSocialSvc) CreateInvite(_ context.Context, inviterUserID int64) (string, error) {
	if inviterUserID == 0 {
		return "", errcode.New(errcode.CommonInvalidArgument, "zero user")
	}
	code := "inv-" + string(rune('0'+inviterUserID%10))
	s.invites[code] = inviterUserID
	return code, nil
}

func (s *stubSocialSvc) AcceptInvite(_ context.Context, inviteCode string, accepterUserID int64) error {
	inviterID, ok := s.invites[inviteCode]
	if !ok {
		return errcode.New(errcode.SocialInviteExpired, "invite not found")
	}
	if inviterID == accepterUserID {
		return errcode.New(errcode.SocialSelfInvite, "self invite")
	}
	a, b := inviterID, accepterUserID
	if a > b {
		a, b = b, a
	}
	s.friends[[2]int64{a, b}] = true
	return nil
}

func (s *stubSocialSvc) AreFriends(_ context.Context, userIDA, userIDB int64) (bool, error) {
	a, b := userIDA, userIDB
	if a > b {
		a, b = b, a
	}
	return s.friends[[2]int64{a, b}], nil
}

func (s *stubSocialSvc) ListFriends(_ context.Context, userID int64) ([]socialdomain.FriendInfo, error) {
	var result []socialdomain.FriendInfo
	for pair := range s.friends {
		if pair[0] == userID {
			result = append(result, socialdomain.FriendInfo{UserID: pair[1], DisplayName: "User_" + strconv.FormatInt(pair[1], 10)})
		} else if pair[1] == userID {
			result = append(result, socialdomain.FriendInfo{UserID: pair[0], DisplayName: "User_" + strconv.FormatInt(pair[0], 10)})
		}
	}
	return result, nil
}

func newTestPair(t *testing.T) *Client {
	t.Helper()
	srv := NewServer(newStubSvc())
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL)
}

// TestSocialRPC_CreateInvite_OK 验证生成邀请码返回非空字符串。
func TestSocialRPC_CreateInvite_OK(t *testing.T) {
	c := newTestPair(t)
	code, err := c.CreateInvite(t.Context(), 42)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if code == "" {
		t.Error("expected non-empty invite code")
	}
}

// TestSocialRPC_AcceptInvite_OK 验证正常接受邀请并查询好友关系。
func TestSocialRPC_AcceptInvite_OK(t *testing.T) {
	c := newTestPair(t)
	if err := c.AcceptInvite(t.Context(), "valid-code", 200); err != nil {
		t.Fatalf("accept invite: %v", err)
	}
	ok, err := c.AreFriends(t.Context(), 100, 200)
	if err != nil {
		t.Fatalf("are friends: %v", err)
	}
	if !ok {
		t.Error("expected friends after accepting invite")
	}
}

// TestSocialRPC_AcceptInvite_NotFound 验证未知邀请码返回 SOCIAL_INVITE_EXPIRED。
func TestSocialRPC_AcceptInvite_NotFound(t *testing.T) {
	c := newTestPair(t)
	err := c.AcceptInvite(t.Context(), "no-such-code", 200)
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.SocialInviteExpired {
		t.Errorf("expected SOCIAL_INVITE_EXPIRED, got %v", err)
	}
}

// TestSocialRPC_AcceptInvite_SelfInvite 验证自邀返回 SOCIAL_SELF_INVITE。
func TestSocialRPC_AcceptInvite_SelfInvite(t *testing.T) {
	c := newTestPair(t)
	err := c.AcceptInvite(t.Context(), "valid-code", 100) // inviter == accepter
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.SocialSelfInvite {
		t.Errorf("expected SOCIAL_SELF_INVITE, got %v", err)
	}
}

// TestSocialRPC_AreFriends_False 验证未建立好友关系时返回 false。
func TestSocialRPC_AreFriends_False(t *testing.T) {
	c := newTestPair(t)
	ok, err := c.AreFriends(t.Context(), 1, 2)
	if err != nil {
		t.Fatalf("are friends: %v", err)
	}
	if ok {
		t.Error("expected not friends")
	}
}

// TestSocialRPC_ListFriends_OK 验证建立好友关系后能列出对方。
func TestSocialRPC_ListFriends_OK(t *testing.T) {
	c := newTestPair(t)
	// 先接受邀请建立好友关系（inviter=100, accepter=200）。
	if err := c.AcceptInvite(t.Context(), "valid-code", 200); err != nil {
		t.Fatalf("accept invite: %v", err)
	}
	// 查询 100 的好友列表，应包含 200。
	friends, err := c.ListFriends(t.Context(), 100)
	if err != nil {
		t.Fatalf("list friends: %v", err)
	}
	if len(friends) != 1 || friends[0].UserID != 200 || friends[0].DisplayName != "User_200" {
		t.Errorf("expected [{UserID:200 DisplayName:User_200}], got %v", friends)
	}
}

// TestSocialRPC_ListFriends_Empty 验证没有好友时返回空列表。
func TestSocialRPC_ListFriends_Empty(t *testing.T) {
	c := newTestPair(t)
	friends, err := c.ListFriends(t.Context(), 999)
	if err != nil {
		t.Fatalf("list friends: %v", err)
	}
	if len(friends) != 0 {
		t.Errorf("expected empty, got %v", friends)
	}
}
