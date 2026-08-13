package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type captureTransport struct{ traceparent string }

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.traceparent = r.Header.Get(TraceparentHeader)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
}

func TestRoute10MetricsUseBoundedLabels(t *testing.T) {
	m := New("gamesvr", slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.EconomyStage("purchase", "wallet_lock", "ok", time.Millisecond)
	m.EconomyTransactionDelta("purchase", 1)
	m.EconomyTransactionDelta("purchase", -1)
	m.DBPoolAcquire("purchase", "ok", time.Millisecond)
	m.InternalError("mysql", "purchase", "ledger_insert", "mysql_1213")
	m.AdmissionQueueDelta("gamesvr_global", 1)
	m.AdmissionQueueDelta("gamesvr_global", -1)
	m.AdmissionRejected("gamesvr_global")
	m.MailboxPublishFailure()
	m.MailboxQueueDropped()
	m.MailboxOutOfOrderDropped()
	m.ReadCache("snapshot", "hit", time.Millisecond)

	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		`farm_economy_transaction_stage_duration_seconds_count{operation="purchase",result="ok",stage="wallet_lock"} 1`,
		`farm_db_pool_acquire_duration_seconds_count{database="primary",operation="purchase",result="ok"} 1`,
		`farm_internal_errors_total{code="mysql_1213",operation="purchase",source="mysql",stage="ledger_insert"} 1`,
		`farm_admission_rejected_total{reason="gamesvr_global"} 1`,
		`farm_mailbox_publish_failures_total 1`,
		`farm_mailbox_ws_queue_dropped_total 1`,
		`farm_mailbox_out_of_order_dropped_total 1`,
		`farm_read_cache_requests_total{resource="snapshot",result="hit"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metric %q missing", want)
		}
	}
}

func TestMiddlewarePreservesTraceAndExportsMetrics(t *testing.T) {
	m := New("test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := m.Middleware("test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := TraceID(r.Context()); got != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("trace id = %q", got)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	req := httptest.NewRequest(http.MethodPost, "/farm/submit", nil)
	req.Header.Set(TraceparentHeader, "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if !strings.Contains(rr.Header().Get(TraceparentHeader), "0123456789abcdef0123456789abcdef") {
		t.Fatalf("traceparent = %q", rr.Header().Get(TraceparentHeader))
	}
	mrr := httptest.NewRecorder()
	m.Handler().ServeHTTP(mrr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(mrr.Body.String(), `farm_actor_requests_total{result="ok"} 1`) {
		t.Fatal("actor metric missing")
	}
}

func TestInvalidTraceparentStartsNewTrace(t *testing.T) {
	m := New("test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := m.Middleware("test", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if len(TraceID(r.Context())) != 32 {
			t.Fatalf("trace id = %q", TraceID(r.Context()))
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.Header.Set(TraceparentHeader, "invalid")
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestTransportPropagatesTraceIDWithChildSpan(t *testing.T) {
	m := New("test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	base := &captureTransport{}
	client := &http.Client{Transport: m.Transport("test", base)}
	ctx := ContextWithTraceID(context.Background(), "0123456789abcdef0123456789abcdef")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.test/rpc/farm/commit", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if !strings.HasPrefix(base.traceparent, "00-0123456789abcdef0123456789abcdef-") {
		t.Fatalf("traceparent = %q", base.traceparent)
	}
}
