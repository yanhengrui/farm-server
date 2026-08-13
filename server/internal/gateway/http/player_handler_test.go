package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type assetClientStub struct{ gotUserID int64 }

func (s *assetClientStub) GetPlayerAssets(_ context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	s.gotUserID = userID
	return &assetrpc.AssetsDTO{CoinBalance: 99, Inventory: []assetrpc.InventoryItemDTO{}}, nil
}

func TestPlayerAssetsUsesTokenUserOnly(t *testing.T) {
	secret := []byte("asset-test-secret")
	stub := &assetClientStub{}
	mux := http.NewServeMux()
	NewPlayerHandler(secret, stub).RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/player/assets?user_id=999", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || stub.gotUserID != 42 {
		t.Fatalf("status=%d forwarded_user=%d body=%s", resp.Code, stub.gotUserID, resp.Body.String())
	}
	var body assetrpc.AssetsDTO
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.CoinBalance != 99 || body.Inventory == nil {
		t.Fatalf("body=%+v", body)
	}
}
