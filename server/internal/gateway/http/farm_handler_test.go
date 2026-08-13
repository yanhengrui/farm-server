package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type farmSnapshotClientStub struct{ gotFarmID int64 }

func (s *farmSnapshotClientStub) GetSnapshot(_ context.Context, farmID int64) (*farmrpc.FarmSnapshotDTO, error) {
	s.gotFarmID = farmID
	return &farmrpc.FarmSnapshotDTO{FarmID: farmID, OwnerUserID: farmID, OwnerDisplayName: "小麦糖", Version: 7, Plots: []farmrpc.PlotDTO{}}, nil
}

func TestFarmSnapshotFriendContainsNoOwnerEconomy(t *testing.T) {
	secret := []byte("farm-snapshot-secret")
	stub := &farmSnapshotClientStub{}
	mux := http.NewServeMux()
	NewFarmHandler(secret, stub).RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/farm/snapshot?farm_id=99", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || stub.gotFarmID != 99 {
		t.Fatalf("status=%d farm_id=%d body=%s", resp.Code, stub.gotFarmID, resp.Body.String())
	}
	body := resp.Body.String()
	if strings.Contains(body, "coin_balance") || strings.Contains(body, "inventory") {
		t.Fatalf("friend snapshot leaked economy fields: %s", body)
	}
	if !strings.Contains(body, `"owner_display_name":"小麦糖"`) {
		t.Fatalf("friend snapshot missing owner display name: %s", body)
	}
}
