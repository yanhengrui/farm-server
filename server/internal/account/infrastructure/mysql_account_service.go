// Package infrastructure 提供账号相关端口的 MySQL 实现。
// MySQLAccountService 在单笔事务内完成游客登录、InitAccount 等幂等操作。
package infrastructure

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mysql "github.com/go-sql-driver/mysql"

	accountdomain "github.com/photon/farm-server/server/internal/account/domain"
	farmdomain "github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/session"
	"github.com/photon/farm-server/server/pkg/shard"
)

const (
	initCoinBalance  = int64(1000)
	initSeedQuantity = int64(5)
	initSeedItemID   = int64(1) // wheat seed
	accessTokenTTL   = 30 * time.Minute
	refreshTokenTTL  = 7 * 24 * time.Hour
	numInitPlots     = 12
)

var localUsernamePattern = regexp.MustCompile(`^[a-z0-9_]{4,32}$`)

// MySQLAccountService 提供账号、会话、初始化资产的 MySQL + Redis 实现。
type MySQLAccountService struct {
	db           *sql.DB
	clk          clock.Clock
	tokenSecret  []byte
	refreshStore *redisstore.RefreshStore
	idGenerator  *shard.IDGenerator
}

// NewMySQLAccountService 构造 MySQLAccountService。
func NewMySQLAccountService(db *sql.DB, clk clock.Clock, tokenSecret []byte, refreshStore *redisstore.RefreshStore) *MySQLAccountService {
	return &MySQLAccountService{db: db, clk: clk, tokenSecret: tokenSecret, refreshStore: refreshStore}
}

// WithIDGenerator opts a service into explicit globally unique account IDs.
// Nil preserves the legacy single-database auto-increment behavior.
func (s *MySQLAccountService) WithIDGenerator(generator *shard.IDGenerator) *MySQLAccountService {
	s.idGenerator = generator
	return s
}

// GuestLoginResult 是 GuestLogin 的返回值。
type GuestLoginResult struct {
	Account accountdomain.Account
	Session accountdomain.SessionRecord
}

// Register 创建本地用户名密码账号及其初始农场。
func (s *MySQLAccountService) Register(ctx context.Context, username, password, requestedDisplayName string) (GuestLoginResult, error) {
	now := s.clk.NowUTC()
	username, err := normalizeUsername(username)
	if err != nil {
		return GuestLoginResult{}, err
	}
	if err := validatePassword(password); err != nil {
		return GuestLoginResult{}, err
	}
	displayName, err := normalizeRequiredDisplayName(requestedDisplayName)
	if err != nil {
		return GuestLoginResult{}, err
	}
	credentialHash, err := hashPassword(password)
	if err != nil {
		return GuestLoginResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("begin register: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var exists int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM auth_identities WHERE provider='local' AND provider_subject=? LIMIT 1`, username).Scan(&exists)
	if err == nil {
		return GuestLoginResult{}, errcode.New(errcode.AuthIdentityExists, "username already exists")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return GuestLoginResult{}, fmt.Errorf("check username: %w", err)
	}

	userID, err := s.createRegisteredAccount(ctx, tx, username, credentialHash, displayName, now)
	if err != nil {
		if isDuplicateKey(err) {
			return GuestLoginResult{}, errcode.New(errcode.AuthIdentityExists, "username already exists")
		}
		return GuestLoginResult{}, fmt.Errorf("create registered account: %w", err)
	}
	acc, err := loadAccount(ctx, tx, userID)
	if err != nil {
		return GuestLoginResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GuestLoginResult{}, fmt.Errorf("commit register: %w", err)
	}
	sess, err := s.createSession(ctx, userID, "web", now)
	if err != nil {
		return GuestLoginResult{}, err
	}
	return GuestLoginResult{Account: acc, Session: sess}, nil
}

// PasswordLogin 校验本地身份并创建新的七天刷新会话。
func (s *MySQLAccountService) PasswordLogin(ctx context.Context, username, password string) (GuestLoginResult, error) {
	username, err := normalizeUsername(username)
	if err != nil || password == "" {
		return GuestLoginResult{}, errcode.New(errcode.AuthUnauthorized, "username or password is incorrect")
	}
	var (
		userID         int64
		credentialHash []byte
		identityStatus string
		accountStatus  string
	)
	err = s.db.QueryRowContext(ctx, `
		SELECT ai.user_id, ai.credential_hash, ai.status, a.status
		FROM auth_identities ai
		JOIN accounts a ON a.user_id = ai.user_id
		WHERE ai.provider='local' AND ai.provider_subject=?
		LIMIT 1`, username).Scan(&userID, &credentialHash, &identityStatus, &accountStatus)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !verifyPassword(credentialHash, password) {
		return GuestLoginResult{}, errcode.New(errcode.AuthUnauthorized, "username or password is incorrect")
	}
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("load local identity: %w", err)
	}
	if identityStatus != "ACTIVE" || accountStatus != accountdomain.AccountStatusActive {
		return GuestLoginResult{}, errcode.New(errcode.AuthForbidden, "account is not active")
	}

	now := s.clk.NowUTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("begin login: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `UPDATE accounts SET last_login_at=?, updated_at=? WHERE user_id=?`, now, now, uint64(userID)); err != nil {
		return GuestLoginResult{}, fmt.Errorf("update last login: %w", err)
	}
	acc, err := loadAccount(ctx, tx, userID)
	if err != nil {
		return GuestLoginResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GuestLoginResult{}, fmt.Errorf("commit login: %w", err)
	}
	sess, err := s.createSession(ctx, userID, "web", now)
	if err != nil {
		return GuestLoginResult{}, err
	}
	return GuestLoginResult{Account: acc, Session: sess}, nil
}

// GuestLogin 幂等地为设备 ID 签发游客账号会话。
// 相同 device_id → 相同 user_id（auth_identities 唯一约束保证）。
// 非空 display_name 会在同一事务内写入新账号或更新已有游客账号；
// 空白输入用于兼容旧客户端，并保留已有昵称。
func (s *MySQLAccountService) GuestLogin(ctx context.Context, deviceID, requestedDisplayName string) (GuestLoginResult, error) {
	now := s.clk.NowUTC()
	displayName, err := normalizeRequestedDisplayName(requestedDisplayName)
	if err != nil {
		return GuestLoginResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("begin_tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// 查找已有游客身份。
	var userID int64
	err = tx.QueryRowContext(ctx,
		`SELECT user_id FROM auth_identities WHERE provider='guest' AND provider_subject=? LIMIT 1`,
		deviceID,
	).Scan(&userID)

	if err == sql.ErrNoRows {
		// 新游客：创建账号 + 身份 + 初始资产。
		userID, err = s.createGuestAccount(ctx, tx, deviceID, displayName, now)
		if err != nil {
			return GuestLoginResult{}, fmt.Errorf("create_guest_account: %w", err)
		}
	} else if err != nil {
		return GuestLoginResult{}, fmt.Errorf("find_auth_identity: %w", err)
	} else if displayName != "" {
		if err := updateGuestDisplayName(ctx, tx, userID, displayName, now); err != nil {
			return GuestLoginResult{}, fmt.Errorf("update_guest_display_name: %w", err)
		}
	}

	acc, err := loadAccount(ctx, tx, userID)
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("load_account: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return GuestLoginResult{}, fmt.Errorf("commit: %w", err)
	}
	sess, err := s.createSession(ctx, userID, deviceID, now)
	if err != nil {
		return GuestLoginResult{}, fmt.Errorf("create_session: %w", err)
	}
	return GuestLoginResult{Account: acc, Session: sess}, nil
}

// LookupGuestUserID is a read-only compatibility lookup used by the sharded
// facade before it hashes a device that may belong to a legacy account. New
// devices still take the deterministic hash route; the lookup only avoids a
// second account being created for an already known identity.
func (s *MySQLAccountService) LookupGuestUserID(ctx context.Context, deviceID string) (int64, bool, error) {
	var userID int64
	err := s.db.QueryRowContext(ctx,
		`SELECT user_id FROM auth_identities WHERE provider='guest' AND provider_subject=? LIMIT 1`,
		deviceID,
	).Scan(&userID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return userID, true, nil
}

// RefreshSessionResult 是 RefreshSession 的返回值。
type RefreshSessionResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

// RefreshSession 校验 refresh token 并签发新 access token。
func (s *MySQLAccountService) RefreshSession(ctx context.Context, sessionID, refreshToken string) (RefreshSessionResult, error) {
	now := s.clk.NowUTC()
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(refreshToken) == "" {
		return RefreshSessionResult{}, errcode.New(errcode.AuthUnauthorized, "session not found or expired")
	}
	oldHash := hashRefreshToken(refreshToken)
	newRefreshToken, newHash, err := newRefreshCredential()
	if err != nil {
		return RefreshSessionResult{}, err
	}
	userID, err := s.refreshStore.Rotate(ctx, sessionID, oldHash, newHash)
	if err != nil {
		return RefreshSessionResult{}, err
	}
	if userID == 0 {
		// 滚动更新兼容：旧 gamesvr 只写 refresh:{hash}=userID，没有 session hash。
		userID, err = s.refreshStore.MigrateLegacy(ctx, sessionID, oldHash, newHash)
		if err != nil {
			return RefreshSessionResult{}, err
		}
		if userID == 0 {
			return RefreshSessionResult{}, errcode.New(errcode.AuthUnauthorized, "session not found or expired")
		}
	}

	accessToken := session.SignSession(userID, sessionID, accessTokenTTL, s.tokenSecret)
	return RefreshSessionResult{
		AccessToken:  accessToken,
		RefreshToken: newRefreshToken,
		ExpiresAt:    now.Add(accessTokenTTL),
	}, nil
}

// Logout 撤销指定登录会话。
func (s *MySQLAccountService) Logout(ctx context.Context, sessionID, refreshToken string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errcode.New(errcode.CommonInvalidArgument, "session_id required")
	}
	deleted, err := s.refreshStore.DeleteSession(ctx, sessionID, hashRefreshToken(refreshToken))
	if err != nil {
		return err
	}
	if !deleted {
		return errcode.New(errcode.AuthUnauthorized, "session not found or expired")
	}
	return nil
}

// AuthenticateResult 是 Authenticate 的返回值。
type AuthenticateResult struct {
	UserID int64
	FarmID int64
}

// Authenticate 校验 access token 的签名和有效期（stateless，不查 DB）。
func (s *MySQLAccountService) Authenticate(accessToken string) (AuthenticateResult, error) {
	userID, err := session.Parse(accessToken, s.tokenSecret)
	if err != nil {
		return AuthenticateResult{}, err
	}
	return AuthenticateResult{UserID: userID, FarmID: userID}, nil // P0
}

// LoadDisplayName returns the required public display name for an account.
// Legacy rows with an empty display_name fall back to the user ID so public
// DTOs never expose an empty required field.
func (s *MySQLAccountService) LoadDisplayName(ctx context.Context, userID int64) (string, error) {
	var displayName string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(TRIM(display_name), ''), CAST(user_id AS CHAR))
		FROM accounts
		WHERE user_id = ?`, uint64(userID)).Scan(&displayName)
	if errors.Is(err, sql.ErrNoRows) {
		// Historical farm snapshots can outlive an account row after local data
		// restores or partial imports. Keep public farm reads available by using
		// the stable user ID as the compatibility display name.
		return strconv.FormatInt(userID, 10), nil
	}
	if err != nil {
		return "", fmt.Errorf("load_display_name: %w", err)
	}
	if displayName == "" {
		return "", fmt.Errorf("load_display_name: account %d returned an empty display name", userID)
	}
	return displayName, nil
}

// ── private helpers ───────────────────────────────────────────────────────────

func (s *MySQLAccountService) createGuestAccount(ctx context.Context, tx *sql.Tx, deviceID, displayName string, now time.Time) (int64, error) {
	suffix := deviceID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	if displayName == "" {
		displayName = "Guest_" + suffix
	}
	var userID int64
	var err error
	if s.idGenerator != nil {
		userID, err = s.idGenerator.Next(now)
		if err != nil {
			return 0, fmt.Errorf("allocate_global_user_id: %w", err)
		}
		_, err = tx.ExecContext(ctx,
			"INSERT INTO accounts (user_id, farm_id, account_type, display_name, status, created_at, updated_at) VALUES (?, ?, 'GUEST', ?, 'ACTIVE', ?, ?)",
			uint64(userID), uint64(userID), displayName, now, now,
		)
		if err != nil {
			return 0, fmt.Errorf("insert_account_with_global_id: %w", err)
		}
	} else {
		res, err := tx.ExecContext(ctx,
			"INSERT INTO accounts (farm_id, account_type, display_name, status, created_at, updated_at) VALUES (NULL, 'GUEST', ?, 'ACTIVE', ?, ?)",
			displayName, now, now,
		)
		if err != nil {
			return 0, fmt.Errorf("insert_account: %w", err)
		}
		userID, _ = res.LastInsertId()
		if _, err := tx.ExecContext(ctx, "UPDATE accounts SET farm_id=? WHERE user_id=?",
			uint64(userID), uint64(userID)); err != nil {
			return 0, fmt.Errorf("update_farm_id: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO auth_identities (user_id, provider, provider_subject, status, created_at, updated_at)
		 VALUES (?, 'guest', ?, 'ACTIVE', ?, ?)`,
		uint64(userID), deviceID, now, now,
	); err != nil {
		return 0, fmt.Errorf("insert_auth_identity: %w", err)
	}

	if err := insertInitialFarm(ctx, tx, userID, now); err != nil {
		return 0, fmt.Errorf("init_farm: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO wallets (user_id, coin_balance, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		uint64(userID), initCoinBalance, now, now,
	); err != nil {
		return 0, fmt.Errorf("insert_wallet: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO inventory_items (user_id, item_type, item_id, quantity, created_at, updated_at)
		 VALUES (?, 'SEED', ?, ?, ?, ?)`,
		uint64(userID), uint64(initSeedItemID), initSeedQuantity, now, now,
	); err != nil {
		return 0, fmt.Errorf("insert_inventory: %w", err)
	}

	return userID, nil
}

func (s *MySQLAccountService) createRegisteredAccount(ctx context.Context, tx *sql.Tx, username string, credentialHash []byte, displayName string, now time.Time) (int64, error) {
	var userID int64
	var err error
	if s.idGenerator != nil {
		userID, err = s.idGenerator.Next(now)
		if err != nil {
			return 0, fmt.Errorf("allocate global registered user id: %w", err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO accounts (user_id, farm_id, account_type, display_name, status, created_at, updated_at, last_login_at)
			 VALUES (?, ?, 'REGISTERED', ?, 'ACTIVE', ?, ?, ?)`,
			uint64(userID), uint64(userID), displayName, now, now, now)
		if err != nil {
			return 0, fmt.Errorf("insert registered account with global id: %w", err)
		}
	} else {
		res, insertErr := tx.ExecContext(ctx,
			`INSERT INTO accounts (farm_id, account_type, display_name, status, created_at, updated_at, last_login_at)
			 VALUES (NULL, 'REGISTERED', ?, 'ACTIVE', ?, ?, ?)`, displayName, now, now, now)
		if insertErr != nil {
			return 0, fmt.Errorf("insert account: %w", insertErr)
		}
		userID, err = res.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("registered user id: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET farm_id=? WHERE user_id=?`, uint64(userID), uint64(userID)); err != nil {
			return 0, fmt.Errorf("update farm id: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO auth_identities (user_id, provider, provider_subject, credential_hash, status, verified_at, created_at, updated_at)
		VALUES (?, 'local', ?, ?, 'ACTIVE', ?, ?, ?)`, uint64(userID), username, credentialHash, now, now, now); err != nil {
		return 0, fmt.Errorf("insert local identity: %w", err)
	}
	if err := insertInitialFarm(ctx, tx, userID, now); err != nil {
		return 0, fmt.Errorf("init farm: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO wallets (user_id, coin_balance, created_at, updated_at) VALUES (?, ?, ?, ?)`, uint64(userID), initCoinBalance, now, now); err != nil {
		return 0, fmt.Errorf("insert wallet: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inventory_items (user_id, item_type, item_id, quantity, created_at, updated_at) VALUES (?, 'SEED', ?, ?, ?, ?)`, uint64(userID), uint64(initSeedItemID), initSeedQuantity, now, now); err != nil {
		return 0, fmt.Errorf("insert inventory: %w", err)
	}
	return userID, nil
}

func normalizeRequestedDisplayName(displayName string) (string, error) {
	displayName = strings.TrimSpace(displayName)
	if utf8.RuneCountInString(displayName) > 64 {
		return "", errcode.New(errcode.CommonInvalidArgument, "display_name must not exceed 64 characters")
	}
	return displayName, nil
}

func normalizeRequiredDisplayName(displayName string) (string, error) {
	displayName, err := normalizeRequestedDisplayName(displayName)
	if err != nil {
		return "", err
	}
	if utf8.RuneCountInString(displayName) < 2 {
		return "", errcode.New(errcode.CommonInvalidArgument, "display_name must contain 2 to 64 characters")
	}
	return displayName, nil
}

func normalizeUsername(username string) (string, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if !localUsernamePattern.MatchString(username) {
		return "", errcode.New(errcode.CommonInvalidArgument, "username must contain 4 to 32 lowercase letters, digits, or underscores")
	}
	return username, nil
}

func validatePassword(password string) error {
	if utf8.RuneCountInString(password) < 8 || len(password) > 128 {
		return errcode.New(errcode.CommonInvalidArgument, "password must contain 8 to 128 characters")
	}
	return nil
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func updateGuestDisplayName(ctx context.Context, tx *sql.Tx, userID int64, displayName string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE accounts
		SET display_name = ?, row_version = row_version + 1, updated_at = ?
		WHERE user_id = ? AND account_type = 'GUEST'`, displayName, now, uint64(userID))
	return err
}

// insertInitialFarm 写入 farm_snapshots 初始状态（numInitPlots 块空地）。
func insertInitialFarm(ctx context.Context, tx *sql.Tx, userID int64, now time.Time) error {
	type plotJSON struct {
		Status         string `json:"status"`
		RemainingYield int64  `json:"remaining_yield"`
	}
	type snapshotJSON struct {
		Plots map[string]plotJSON `json:"plots"`
	}
	sj := snapshotJSON{Plots: make(map[string]plotJSON, numInitPlots)}
	for i := 1; i <= numInitPlots; i++ {
		sj.Plots[strconv.Itoa(i)] = plotJSON{Status: string(farmdomain.PlotEmpty)}
	}
	raw, err := json.Marshal(sj)
	if err != nil {
		return fmt.Errorf("marshal_snapshot: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO farm_snapshots (farm_id, owner_user_id, version, snapshot, created_at, updated_at)
		 VALUES (?, ?, 0, ?, ?, ?)`,
		uint64(userID), uint64(userID), raw, now, now,
	)
	return err
}

func (s *MySQLAccountService) createSession(ctx context.Context, userID int64, deviceID string, now time.Time) (accountdomain.SessionRecord, error) {
	var sidBytes [16]byte
	if _, err := rand.Read(sidBytes[:]); err != nil {
		return accountdomain.SessionRecord{}, fmt.Errorf("gen_session_id: %w", err)
	}
	sessionID := hex.EncodeToString(sidBytes[:])

	refreshToken, rtHash, err := newRefreshCredential()
	if err != nil {
		return accountdomain.SessionRecord{}, err
	}

	accessToken := session.SignSession(userID, sessionID, accessTokenTTL, s.tokenSecret)
	expiresAt := now.Add(refreshTokenTTL)

	// 写 Redis（替代 MySQL sessions INSERT）：tokenHash → userID，TTL 自动过期。
	if err := s.refreshStore.Create(ctx, sessionID, rtHash, userID); err != nil {
		return accountdomain.SessionRecord{}, fmt.Errorf("refresh_store set: %w", err)
	}

	return accountdomain.SessionRecord{
		SessionID:        sessionID,
		UserID:           userID,
		DeviceID:         deviceID,
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		RefreshTokenHash: hashBytes(rtHash),
		ExpiresAt:        expiresAt,
		CreatedAt:        now,
	}, nil
}

func newRefreshCredential() (string, string, error) {
	var rtBytes [32]byte
	if _, err := rand.Read(rtBytes[:]); err != nil {
		return "", "", fmt.Errorf("gen_refresh_token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(rtBytes[:])
	return token, hashRefreshToken(token), nil
}

func loadAccount(ctx context.Context, tx *sql.Tx, userID int64) (accountdomain.Account, error) {
	var acc accountdomain.Account
	var farmIDNull sql.NullInt64
	err := tx.QueryRowContext(ctx,
		`SELECT user_id, COALESCE(display_name,''), account_type, status, farm_id, created_at
		 FROM accounts WHERE user_id=?`, uint64(userID),
	).Scan(&acc.UserID, &acc.DisplayName, &acc.AccountType, &acc.Status, &farmIDNull, &acc.CreatedAt)
	if err == sql.ErrNoRows {
		return accountdomain.Account{}, errcode.New(errcode.AuthUnauthorized, "account not found")
	}
	if err != nil {
		return accountdomain.Account{}, fmt.Errorf("load_account: %w", err)
	}
	if farmIDNull.Valid {
		acc.FarmID = farmIDNull.Int64
	}
	return acc, nil
}

// hashRefreshToken 返回 refresh token 的 hex SHA256 哈希，用作 Redis key。
func hashRefreshToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// hashBytes 将 hex 哈希字符串转为 []byte 供 domain 层使用。
func hashBytes(hash string) []byte {
	b, _ := hex.DecodeString(hash)
	return b
}
