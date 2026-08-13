package http

import (
	"context"
	"net/http"
	"strings"

	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// SessionValidator checks the Redis-backed lifecycle of a signed session.
type SessionValidator interface {
	IsActive(ctx context.Context, sessionID string, userID int64) (bool, error)
}

// RequireActiveSession adds immediate logout invalidation to authenticated
// public APIs. Signature and expiry remain locally verifiable; only v2 tokens
// carrying a session ID require the Redis lookup. Legacy v1 tokens are accepted
// during the rolling-upgrade compatibility window and expire naturally.
func RequireActiveSession(secret []byte, validator SessionValidator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if validator == nil || !requiresActiveSession(r) {
			next.ServeHTTP(w, r)
			return
		}
		token := bearerToken(r)
		if token == "" {
			token = r.URL.Query().Get("token")
		}
		if token == "" {
			writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "token required")
			return
		}
		claims, err := session.ParseClaims(token, secret)
		if err != nil {
			writeErrFromError(w, err)
			return
		}
		if claims.SessionID != "" {
			active, validateErr := validator.IsActive(r.Context(), claims.SessionID, claims.UserID)
			if validateErr != nil {
				writeError(w, http.StatusServiceUnavailable, errcode.Internal, "session store unavailable")
				return
			}
			if !active {
				writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "session logged out or expired")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func requiresActiveSession(r *http.Request) bool {
	if r.Method == http.MethodOptions || !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		return false
	}
	return !strings.HasPrefix(r.URL.Path, "/api/v1/auth/") && r.URL.Path != "/api/v1/ping"
}
