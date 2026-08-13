// Package domain 定义账号聚合的核心实体与值对象。
// 不依赖 HTTP/gRPC/MySQL；见依赖方向约束。
package domain

import "time"

// Account 是账号聚合根。
type Account struct {
	UserID      int64
	DisplayName string
	AccountType string // "GUEST" | "REGISTERED"
	Status      string // "ACTIVE" | "BANNED" | "DELETED"
	FarmID      int64  // 0 表示尚未创建农场
	CreatedAt   time.Time
}

// SessionRecord 是已签发会话的持久化记录。
// AccessToken 是签名后的 HMAC 令牌；RefreshToken 只在创建时返回明文，
// DB 中存储 RefreshTokenHash（SHA-256）。
type SessionRecord struct {
	SessionID        string
	UserID           int64
	DeviceID         string
	AccessToken      string
	RefreshToken     string // 明文，仅在 Create 时有值；DB 存哈希
	RefreshTokenHash []byte // sha256(RefreshToken)
	ExpiresAt        time.Time
	CreatedAt        time.Time
}

const (
	AccountTypeGuest      = "GUEST"
	AccountTypeRegistered = "REGISTERED"
	AccountStatusActive   = "ACTIVE"
)
