package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/session"
)

type economicClientStub struct{ commands []application.CommitRequest }

func (s *economicClientStub) CommitFarmCommand(_ context.Context, req application.CommitRequest) (application.CommitResult, error) {
	s.commands = append(s.commands, req)
	return application.CommitResult{CoinBalance: 90}, nil
}

func TestShopRequiresStableIdempotencyKey(t *testing.T) {
	secret := []byte("shop-handler-secret")
	stub := &economicClientStub{}
	mux := http.NewServeMux()
	NewShopHandler(secret, stub).RegisterRoutes(mux)
	token := session.Sign(42, time.Minute, secret)

	missing := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/shop/purchase", bytes.NewBufferString(`{"crop_id":"WHEAT","quantity":1}`))
	req.Header.Set("Authorization", "Bearer "+token)
	mux.ServeHTTP(missing, req)
	if missing.Code != http.StatusBadRequest || len(stub.commands) != 0 {
		t.Fatalf("status=%d calls=%d body=%s", missing.Code, len(stub.commands), missing.Body.String())
	}

	key := id.NewV7()
	canonical := key[:8] + "-" + key[8:12] + "-" + key[12:16] + "-" + key[16:20] + "-" + key[20:]
	for range 2 {
		resp := httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/shop/purchase", bytes.NewBufferString(`{"crop_id":"WHEAT","quantity":1}`))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", canonical)
		mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
		}
	}
	if len(stub.commands) != 2 || stub.commands[0].Command.CmdID != key || stub.commands[1].Command.CmdID != key {
		t.Fatalf("commands=%+v", stub.commands)
	}
}
