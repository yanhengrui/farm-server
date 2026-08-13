package session

import (
	"errors"
	"testing"
	"time"

	"github.com/photon/farm-server/server/pkg/errcode"
)

var testSecret = []byte("test-secret-key-32-bytes-exactly!")

func TestSign_ThenParse_OK(t *testing.T) {
	token := Sign(42, time.Minute, testSecret)
	uid, err := Parse(token, testSecret)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if uid != 42 {
		t.Errorf("expected user_id 42, got %d", uid)
	}
}

func TestParse_WrongSecret(t *testing.T) {
	token := Sign(1, time.Minute, testSecret)
	_, err := Parse(token, []byte("wrong-secret"))
	if err == nil {
		t.Fatal("expected auth error for wrong secret")
	}
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.AuthUnauthorized {
		t.Errorf("expected AUTH_UNAUTHORIZED, got %v", err)
	}
}

func TestParse_Expired(t *testing.T) {
	token := Sign(1, -time.Second, testSecret) // already expired
	_, err := Parse(token, testSecret)
	if err == nil {
		t.Fatal("expected expiry error")
	}
	var e *errcode.Error
	if !errors.As(err, &e) || e.Code != errcode.AuthTokenExpired {
		t.Errorf("expected AUTH_TOKEN_EXPIRED, got %v", err)
	}
}

func TestParse_Tampered(t *testing.T) {
	token := Sign(1, time.Minute, testSecret)
	// flip last char of signature
	b := []byte(token)
	b[len(b)-1] ^= 0x01
	_, err := Parse(string(b), testSecret)
	if err == nil {
		t.Fatal("expected error for tampered token")
	}
}

func TestParse_Malformed(t *testing.T) {
	if _, err := Parse("notavalidtoken", testSecret); err == nil {
		t.Fatal("expected error for malformed token")
	}
}

func TestSignSessionCarriesStableSessionID(t *testing.T) {
	token := SignSession(42, "session-abc", time.Minute, testSecret)
	claims, err := ParseClaims(token, testSecret)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != 42 || claims.SessionID != "session-abc" {
		t.Fatalf("claims=%+v", claims)
	}
}
