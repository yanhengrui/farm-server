// Package accountv1 定义 AccountService 的 Go 接口契约。
// 对应 proto/account/v1，gatesvr → gamesvr 内部 gRPC 调用。
// 实现在 gamesvr；gatesvr 通过此接口调用，不引入具体实现依赖。
package accountv1

import (
	"context"
	"time"
)

// AccountType 账号类型。
type AccountType string

const (
	AccountTypeGuest      AccountType = "GUEST"
	AccountTypeRegistered AccountType = "REGISTERED"
)

// AccountStatus 账号状态。
type AccountStatus string

const (
	AccountStatusActive  AccountStatus = "ACTIVE"
	AccountStatusBanned  AccountStatus = "BANNED"
	AccountStatusDeleted AccountStatus = "DELETED"
)

// Account 是账号公开信息（不含凭证）。
type Account struct {
	UserID      int64
	DisplayName string
	AccountType AccountType
	Status      AccountStatus
	FarmID      int64 // 0 表示尚未创建农场
	CreatedAt   time.Time
}

// Session 是签发给客户端的会话摘要（不含明文 refresh token）。
type Session struct {
	SessionID             string // hex encoded binary(16)
	UserID                int64
	AccessToken           string
	RefreshTokenExpiresAt time.Time
	ExpiresAt             time.Time
}

// GuestLoginRequest 游客登录请求。
type GuestLoginRequest struct {
	DeviceID      string
	DisplayName   string
	ClientVersion string
}

// GuestLoginResponse 游客登录响应。
type GuestLoginResponse struct {
	Account Account
	Session Session
}

// RefreshSessionRequest 刷新会话请求，Refresh Token 由调用方从 HttpOnly Cookie 读取。
type RefreshSessionRequest struct {
	SessionID    string
	RefreshToken string // 明文，服务端校验哈希
}

// RefreshSessionResponse 刷新会话响应。
type RefreshSessionResponse struct {
	AccessToken string
	ExpiresAt   time.Time
}

// AuthenticateRequest 验证 Access Token 并返回账号信息（无流量则 panic-fast-path）。
type AuthenticateRequest struct {
	AccessToken string
}

// AuthenticateResponse 账号信息。
type AuthenticateResponse struct {
	Account Account
	Session Session
}

// InitAccountRequest 账号初始化（首次登录后创建钱包和初始农场）。
type InitAccountRequest struct {
	UserID    int64
	FarmID    int64 // 预分配的 farm_id，P0 等于 user_id
	InitCoins int64 // 初始金币，由服务端配置决定
}

// InitAccountResponse 初始化结果。
type InitAccountResponse struct {
	Account Account
}

// AccountService 是 gamesvr 实现、gatesvr 调用的账号服务接口。
// gRPC metadata 见 03-接口契约.md §5.1；实现必须校验必要字段。
type AccountService interface {
	// GuestLogin 游客登录，device_id 相同且已有游客账号时直接返回原账号。
	GuestLogin(ctx context.Context, req GuestLoginRequest) (GuestLoginResponse, error)

	// RefreshSession 用 refresh token 签发新 access token。
	RefreshSession(ctx context.Context, req RefreshSessionRequest) (RefreshSessionResponse, error)

	// Authenticate 校验 access token 并返回绑定账号；Token 过期返回 AUTH_TOKEN_EXPIRED。
	Authenticate(ctx context.Context, req AuthenticateRequest) (AuthenticateResponse, error)

	// InitAccount 幂等地初始化账号附属资源（钱包、初始农场、初始种子）。
	// 同一 user_id 重复调用安全返回原结果。
	InitAccount(ctx context.Context, req InitAccountRequest) (InitAccountResponse, error)
}
