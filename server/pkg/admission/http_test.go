package admission

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/pkg/errcode"
)

type gateObserver struct {
	queued, inflight int64
	rejected         atomic.Int64
	waits            atomic.Int64
}

func (o *gateObserver) AdmissionQueueDelta(_ string, delta int) {
	atomic.AddInt64(&o.queued, int64(delta))
}
func (o *gateObserver) AdmissionInflightDelta(_ string, delta int) {
	atomic.AddInt64(&o.inflight, int64(delta))
}
func (o *gateObserver) AdmissionWait(_, _ string, _ time.Duration) { o.waits.Add(1) }
func (o *gateObserver) AdmissionRejected(_ string)                 { o.rejected.Add(1) }

func TestInflightRejectsWhenSaturated(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	h := Inflight(1, time.Millisecond, 25*time.Millisecond, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { close(started); <-release; w.WriteHeader(http.StatusOK) }))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/rpc/farm/commit", nil))
	}()
	<-started
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/rpc/farm/commit", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
	if rr.Header().Get("Retry-After-Ms") != "25" {
		t.Fatalf("retry=%q", rr.Header().Get("Retry-After-Ms"))
	}
	close(release)
	wg.Wait()
}

func TestObservedGateBalancesStateAndExportsStableReason(t *testing.T) {
	observer := &gateObserver{}
	gate := NewObservedGate(1, ReasonGamesvrGlobal, observer)
	if !gate.Acquire(context.Background(), time.Millisecond) {
		t.Fatal("first acquire rejected")
	}
	if gate.Acquire(context.Background(), time.Millisecond) {
		t.Fatal("saturated gate acquired")
	}
	gate.Release()
	if observer.queued != 0 || observer.inflight != 0 || observer.rejected.Load() != 1 || observer.waits.Load() != 2 {
		t.Fatalf("observer queued=%d inflight=%d rejected=%d waits=%d", observer.queued, observer.inflight, observer.rejected.Load(), observer.waits.Load())
	}

	rr := httptest.NewRecorder()
	if !gate.Acquire(context.Background(), time.Millisecond) {
		t.Fatal("setup acquire rejected")
	}
	InflightWithGate(gate, time.Millisecond, 25*time.Millisecond, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/", nil))
	gate.Release()
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	errorPayload, _ := payload["error"].(map[string]any)
	if rr.Header().Get("X-Capacity-Reason") != ReasonGamesvrGlobal || errorPayload["reason"] != ReasonGamesvrGlobal {
		t.Fatalf("header=%q payload=%v", rr.Header().Get("X-Capacity-Reason"), payload)
	}
}

func TestPublicInflightBypassesWebSocketAndUsesPublicEnvelope(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	gate := NewGate(1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/work" {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	})
	h := PublicInflightWithGate(gate, time.Millisecond, 25*time.Millisecond, func(r *http.Request) bool {
		return r.URL.Path == "/ws"
	}, next)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/work", nil))
	}()
	<-started

	ws := httptest.NewRecorder()
	h.ServeHTTP(ws, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if ws.Code != http.StatusOK {
		t.Fatalf("WebSocket bypass status=%d", ws.Code)
	}

	rejected := httptest.NewRecorder()
	h.ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/api/other", nil))
	if rejected.Code != http.StatusServiceUnavailable || rejected.Body.String() == "" {
		t.Fatalf("rejected status=%d body=%q", rejected.Code, rejected.Body.String())
	}
	if got := rejected.Body.String(); !strings.Contains(got, string(errcode.ResourceExhausted)) || strings.Contains(got, `"error"`) {
		t.Fatalf("expected public resource-exhausted envelope, got %q", got)
	}
	close(release)
	wg.Wait()
}

func TestLimiterRefills(t *testing.T) {
	l := NewLimiter(1000, 1, time.Minute)
	if !l.Allow("u") || l.Allow("u") {
		t.Fatal("burst limit not enforced")
	}
	time.Sleep(2 * time.Millisecond)
	if !l.Allow("u") {
		t.Fatal("token did not refill")
	}
}

func TestRateLimitByUsesCanonicalUserKey(t *testing.T) {
	l := NewLimiter(1, 1, time.Minute)
	h := RateLimitBy(nil, l, 25*time.Millisecond, func(*http.Request) string {
		return UserKey(42)
	}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d", first.Code)
	}
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status=%d", second.Code)
	}
}

func TestLimiterConcurrentSameKeyHonorsBurst(t *testing.T) {
	const burst = 100
	l := NewLimiter(0.000001, burst, time.Minute)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("same-user") {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed=%d, want %d", got, burst)
	}
}
