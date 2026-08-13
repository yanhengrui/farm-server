package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/catalog/transport/catalogrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type catalogClientStub struct {
	gotUserID int64
	items     []catalogrpc.UnlockDTO
}

func (s *catalogClientStub) ListCatalogUnlocks(_ context.Context, userID int64) ([]catalogrpc.UnlockDTO, error) {
	s.gotUserID = userID
	return s.items, nil
}

func TestCatalogHandlerListAndAuth(t *testing.T) {
	secret := []byte("catalog-test-secret")
	when := time.Date(2026, 8, 3, 9, 10, 11, 123000000, time.UTC)
	stub := &catalogClientStub{items: []catalogrpc.UnlockDTO{{CatalogKey: "crop_WHEAT", UnlockedAt: when}}}
	mux := http.NewServeMux()
	NewCatalogHandler(secret, stub).RegisterRoutes(mux)

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/catalog/list", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/list", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || stub.gotUserID != 42 {
		t.Fatalf("status=%d user_id=%d body=%s", resp.Code, stub.gotUserID, resp.Body.String())
	}
	var body struct {
		Unlocks []catalogrpc.UnlockDTO `json:"unlocks"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Unlocks) != 1 || body.Unlocks[0].CatalogKey != "crop_WHEAT" || !body.Unlocks[0].UnlockedAt.Equal(when) {
		t.Fatalf("body=%+v", body)
	}
}
