package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/pkg/session"
)

type stubSessionValidator struct {
	active bool
	err    error
}

func (s stubSessionValidator) IsActive(context.Context, string, int64) (bool, error) {
	return s.active, s.err
}

func TestRequireActiveSessionRejectsLoggedOutV2Token(t *testing.T) {
	secret := []byte("session-middleware-secret")
	token := session.SignSession(42, "sid-42", time.Minute, secret)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	for _, tc := range []struct {
		name      string
		validator stubSessionValidator
		want      int
	}{
		{name: "active", validator: stubSessionValidator{active: true}, want: http.StatusNoContent},
		{name: "logged out", validator: stubSessionValidator{}, want: http.StatusUnauthorized},
		{name: "redis unavailable", validator: stubSessionValidator{err: errors.New("redis down")}, want: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/player/assets", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			resp := httptest.NewRecorder()
			RequireActiveSession(secret, tc.validator, next).ServeHTTP(resp, req)
			if resp.Code != tc.want {
				t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestRequireActiveSessionKeepsRollingAndPublicCompatibility(t *testing.T) {
	secret := []byte("session-middleware-secret")
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	validator := stubSessionValidator{}

	legacy := httptest.NewRequest(http.MethodGet, "/api/v1/player/assets", nil)
	legacy.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	legacyResp := httptest.NewRecorder()
	RequireActiveSession(secret, validator, next).ServeHTTP(legacyResp, legacy)
	if legacyResp.Code != http.StatusNoContent {
		t.Fatalf("legacy rolling token status=%d", legacyResp.Code)
	}

	publicResp := httptest.NewRecorder()
	RequireActiveSession(secret, validator, next).ServeHTTP(publicResp, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil))
	if publicResp.Code != http.StatusNoContent {
		t.Fatalf("public auth status=%d", publicResp.Code)
	}
}
