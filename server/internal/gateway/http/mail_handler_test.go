package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	"github.com/photon/farm-server/server/pkg/session"
)

type stubMailboxClient struct{}

func (stubMailboxClient) ListMails(context.Context, int64, int) ([]mailrpc.MailDTO, error) {
	return nil, nil
}
func (stubMailboxClient) GetSummary(context.Context, int64) (mailrpc.MailboxSummaryDTO, error) {
	return mailrpc.MailboxSummaryDTO{UnreadCount: 4, Version: 7}, nil
}
func (stubMailboxClient) ClaimAttachment(context.Context, int64, int64) error { return nil }
func (stubMailboxClient) MarkRead(context.Context, int64, int64) error        { return nil }

func TestMailboxSummaryReturnsOnlyBadgeState(t *testing.T) {
	secret := []byte("mail-handler-test-secret")
	mux := http.NewServeMux()
	NewMailHandler(secret, stubMailboxClient{}).RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/mail/summary", nil)
	req.Header.Set("Authorization", "Bearer "+session.Sign(42, time.Minute, secret))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["unread_count"] != float64(4) || body["mailbox_version"] != float64(7) || len(body) != 2 {
		t.Fatalf("body=%v", body)
	}
}
