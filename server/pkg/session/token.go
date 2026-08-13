// Package session 提供无状态 HMAC-SHA256 Access Token 的签发与校验。
// Token v2 格式："v2:<user_id>:<session_id>:<unix_expiry>.<signature>"。
// Parse 继续兼容旧的 "<user_id>:<unix_expiry>.<signature>"，便于滚动更新。
// 无外部依赖；secret 应来自环境变量，不可硬编码。
package session

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/photon/farm-server/server/pkg/errcode"
)

// Sign 签发一个 TTL 为 ttl 的访问令牌。
func Sign(userID int64, ttl time.Duration, secret []byte) string {
	exp := time.Now().Add(ttl).Unix()
	msg := fmt.Sprintf("%d:%d", userID, exp)
	return msg + "." + sign(msg, secret)
}

// Claims 是访问令牌中经过签名保护的身份声明。
type Claims struct {
	UserID    int64
	SessionID string
	ExpiresAt time.Time
}

// SignSession 签发绑定真实登录会话的 v2 访问令牌。
func SignSession(userID int64, sessionID string, ttl time.Duration, secret []byte) string {
	exp := time.Now().Add(ttl).Unix()
	msg := fmt.Sprintf("v2:%d:%s:%d", userID, sessionID, exp)
	return msg + "." + sign(msg, secret)
}

// Parse 校验签名和有效期，成功则返回 user_id。
// 签名不匹配返回 AUTH_UNAUTHORIZED；已过期返回 AUTH_TOKEN_EXPIRED。
func Parse(token string, secret []byte) (int64, error) {
	claims, err := ParseClaims(token, secret)
	return claims.UserID, err
}

// ParseClaims 校验令牌并返回完整声明，同时兼容 v1 令牌。
func ParseClaims(token string, secret []byte) (Claims, error) {
	dot := strings.LastIndex(token, ".")
	if dot < 0 {
		return Claims{}, errcode.New(errcode.AuthUnauthorized, "malformed token: missing dot")
	}
	msg, sig := token[:dot], token[dot+1:]

	if !hmac.Equal([]byte(sig), []byte(sign(msg, secret))) {
		return Claims{}, errcode.New(errcode.AuthUnauthorized, "invalid token signature")
	}

	parts := strings.Split(msg, ":")
	var userPart, sessionID, expPart string
	switch {
	case len(parts) == 4 && parts[0] == "v2":
		userPart, sessionID, expPart = parts[1], parts[2], parts[3]
		if sessionID == "" {
			return Claims{}, errors.New("malformed token session")
		}
	case len(parts) == 2:
		userPart, expPart = parts[0], parts[1]
	default:
		return Claims{}, errors.New("malformed token message")
	}
	userID, err := strconv.ParseInt(userPart, 10, 64)
	if err != nil {
		return Claims{}, fmt.Errorf("malformed user_id in token: %w", err)
	}
	exp, err := strconv.ParseInt(expPart, 10, 64)
	if err != nil {
		return Claims{}, fmt.Errorf("malformed expiry in token: %w", err)
	}
	if time.Now().Unix() > exp {
		return Claims{}, errcode.New(errcode.AuthTokenExpired, "token expired")
	}
	return Claims{UserID: userID, SessionID: sessionID, ExpiresAt: time.Unix(exp, 0)}, nil
}

func sign(msg string, secret []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(msg))
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
