package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/pet/transport/petrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type petClientStub struct {
	setUserID int64
	setValue  bool
}

func (*petClientStub) BuyPet(context.Context, int64) error { return nil }
func (*petClientStub) GetStatus(context.Context, int64) (petrpc.StatusDTO, error) {
	return petrpc.StatusDTO{HasPet: true, AutoHarvestEnabled: true}, nil
}
func (s *petClientStub) SetAutoHarvest(_ context.Context, userID int64, enabled bool) error {
	s.setUserID, s.setValue = userID, enabled
	return nil
}

func TestPetAutoHarvestAuthAndValidation(t *testing.T) {
	secret := []byte("pet-handler-secret")
	stub := &petClientStub{}
	mux := http.NewServeMux()
	NewPetHandler(secret, stub).RegisterRoutes(mux)

	missing := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pet/auto-harvest", bytes.NewBufferString(`{}`))
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	mux.ServeHTTP(missing, req)
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("missing enabled status=%d body=%s", missing.Code, missing.Body.String())
	}

	valid := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/pet/auto-harvest", bytes.NewBufferString(`{"enabled":false}`))
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	mux.ServeHTTP(valid, req)
	if valid.Code != http.StatusOK || stub.setUserID != 42 || stub.setValue {
		t.Fatalf("status=%d user=%d enabled=%v body=%s", valid.Code, stub.setUserID, stub.setValue, valid.Body.String())
	}
}
