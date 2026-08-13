package infrastructure

import (
	"context"
	"fmt"
	"strings"

	"github.com/photon/farm-server/server/pkg/shard"
)

// ShardedAccountService routes account aggregate access to one physical shard.
// A device ID selects the first shard before a user ID exists; subsequent
// account reads use that signed user ID as the stable route key.
type ShardedAccountService struct {
	router   *shard.Router
	services map[string]*MySQLAccountService
	defaultS *MySQLAccountService
}

// Register deterministically places a local identity by its normalized
// username. The same username therefore always reaches the same shard, where
// the auth_identities unique key provides the final concurrency guard.
func (s *ShardedAccountService) Register(ctx context.Context, username, password, displayName string) (GuestLoginResult, error) {
	name, err := s.router.ShardForKey(strings.ToLower(strings.TrimSpace(username)))
	if err != nil {
		return GuestLoginResult{}, err
	}
	return s.services[name].Register(ctx, username, password, displayName)
}

// PasswordLogin uses the same normalized routing key as Register, so password
// verification and account loading stay local to the owning shard.
func (s *ShardedAccountService) PasswordLogin(ctx context.Context, username, password string) (GuestLoginResult, error) {
	name, err := s.router.ShardForKey(strings.ToLower(strings.TrimSpace(username)))
	if err != nil {
		return GuestLoginResult{}, err
	}
	return s.services[name].PasswordLogin(ctx, username, password)
}

func NewShardedAccountService(router *shard.Router, services map[string]*MySQLAccountService) (*ShardedAccountService, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyServices := make(map[string]*MySQLAccountService, len(services))
	var first *MySQLAccountService
	for _, name := range router.Shards() {
		svc := services[name]
		if svc == nil {
			return nil, fmt.Errorf("missing account service for shard %q", name)
		}
		copyServices[name] = svc
		if first == nil {
			first = svc
		}
	}
	return &ShardedAccountService{router: router, services: copyServices, defaultS: first}, nil
}

func (s *ShardedAccountService) GuestLogin(ctx context.Context, deviceID, displayName string) (GuestLoginResult, error) {
	// Legacy accounts were placed by user_id during the initial backfill, while
	// a never-seen device has no user_id and is placed by its device hash. Check
	// the small, fixed shard set for an existing identity first so a historical
	// device cannot be mistaken for a new account on the hash-selected shard.
	if existing, found, err := s.serviceForExistingGuestDevice(ctx, deviceID); err != nil {
		return GuestLoginResult{}, err
	} else if found {
		return existing.GuestLogin(ctx, deviceID, displayName)
	}
	name, err := s.router.ShardForKey(deviceID)
	if err != nil {
		return GuestLoginResult{}, err
	}
	return s.services[name].GuestLogin(ctx, deviceID, displayName)
}

func (s *ShardedAccountService) serviceForExistingGuestDevice(ctx context.Context, deviceID string) (*MySQLAccountService, bool, error) {
	for _, name := range s.router.Shards() {
		service := s.services[name]
		_, found, err := service.LookupGuestUserID(ctx, deviceID)
		if err != nil {
			return nil, false, fmt.Errorf("lookup guest identity on shard %s: %w", name, err)
		}
		if found {
			return service, true, nil
		}
	}
	return nil, false, nil
}

func (s *ShardedAccountService) RefreshSession(ctx context.Context, sessionID, refreshToken string) (RefreshSessionResult, error) {
	return s.defaultS.RefreshSession(ctx, sessionID, refreshToken)
}

// Logout only revokes Redis session state and does not require a MySQL shard.
func (s *ShardedAccountService) Logout(ctx context.Context, sessionID, refreshToken string) error {
	return s.defaultS.Logout(ctx, sessionID, refreshToken)
}

func (s *ShardedAccountService) Authenticate(accessToken string) (AuthenticateResult, error) {
	return s.defaultS.Authenticate(accessToken)
}

func (s *ShardedAccountService) LoadDisplayName(ctx context.Context, userID int64) (string, error) {
	name, err := s.router.ShardForUserID(userID)
	if err != nil {
		return "", err
	}
	return s.services[name].LoadDisplayName(ctx, userID)
}
