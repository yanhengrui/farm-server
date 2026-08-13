package admission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/photon/farm-server/server/pkg/errcode"
)

// Stable capacity reasons are shared by service metrics, public error payloads
// and the load driver. Keep this vocabulary low-cardinality.
const (
	ReasonGatewayGlobal     = errcode.CapacityReasonGatewayGlobal
	ReasonGamesvrGlobal     = errcode.CapacityReasonGamesvrGlobal
	ReasonEconomyGate       = errcode.CapacityReasonEconomyGate
	ReasonDBPoolWait        = errcode.CapacityReasonDBPoolWait
	ReasonDownstreamTimeout = errcode.CapacityReasonDownstreamTimeout
	ReasonLoadgenQueue      = errcode.CapacityReasonLoadgenQueue
)

// Observer receives gate state transitions without learning request or entity
// identifiers. Implementations must be safe for concurrent use.
type Observer interface {
	AdmissionQueueDelta(reason string, delta int)
	AdmissionInflightDelta(reason string, delta int)
	AdmissionWait(reason, result string, duration time.Duration)
	AdmissionRejected(reason string)
}

type Gate struct {
	sem      chan struct{}
	reason   string
	observer Observer
}

func NewGate(max int) *Gate {
	return NewObservedGate(max, "global", nil)
}

func NewObservedGate(max int, reason string, observer Observer) *Gate {
	if max < 1 {
		max = 1
	}
	if reason == "" {
		reason = "global"
	}
	return &Gate{sem: make(chan struct{}, max), reason: reason, observer: observer}
}

func (g *Gate) Acquire(ctx context.Context, wait time.Duration) bool {
	started := time.Now()
	if g.observer != nil {
		g.observer.AdmissionQueueDelta(g.reason, 1)
		defer g.observer.AdmissionQueueDelta(g.reason, -1)
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case g.sem <- struct{}{}:
		if g.observer != nil {
			g.observer.AdmissionWait(g.reason, "acquired", time.Since(started))
			g.observer.AdmissionInflightDelta(g.reason, 1)
		}
		return true
	case <-t.C:
		if g.observer != nil {
			g.observer.AdmissionWait(g.reason, "rejected", time.Since(started))
			g.observer.AdmissionRejected(g.reason)
		}
		return false
	case <-ctx.Done():
		if g.observer != nil {
			g.observer.AdmissionWait(g.reason, "canceled", time.Since(started))
		}
		return false
	}
}

func (g *Gate) Release() {
	<-g.sem
	if g.observer != nil {
		g.observer.AdmissionInflightDelta(g.reason, -1)
	}
}

func (g *Gate) Reason() string { return g.reason }

func Inflight(max int, wait, retryAfter time.Duration, next http.Handler) http.Handler {
	return InflightWithGate(NewGate(max), wait, retryAfter, next)
}

func InflightWithGate(gate *Gate, wait, retryAfter time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !gate.Acquire(r.Context(), wait) {
			if r.Context().Err() != nil {
				return
			}
			writeLimited(w, errcode.ResourceExhausted, retryAfter, true, gate.Reason())
			return
		}
		defer gate.Release()
		next.ServeHTTP(w, r)
	})
}

// PublicInflightWithGate bounds public HTTP work while allowing long-lived
// connections such as WebSockets to bypass the request gate. Public callers
// receive the normal top-level API error envelope so clients can classify
// RESOURCE_EXHAUSTED without parsing an internal RPC envelope.
func PublicInflightWithGate(gate *Gate, wait, retryAfter time.Duration, bypass func(*http.Request) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bypass != nil && bypass(r) {
			next.ServeHTTP(w, r)
			return
		}
		if !gate.Acquire(r.Context(), wait) {
			if r.Context().Err() != nil {
				return
			}
			writeLimited(w, errcode.ResourceExhausted, retryAfter, false, gate.Reason())
			return
		}
		defer gate.Release()
		next.ServeHTTP(w, r)
	})
}

type bucket struct {
	tokens  float64
	updated time.Time
	seen    time.Time
}
type limiterShard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type Limiter struct {
	rate, burst float64
	ttl         time.Duration
	shards      []limiterShard
	calls       atomic.Uint64
}

func NewLimiter(rate float64, burst int, ttl time.Duration) *Limiter {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	l := &Limiter{rate: rate, burst: float64(burst), ttl: ttl, shards: make([]limiterShard, 64)}
	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket)
	}
	return l
}
func (l *Limiter) Allow(key string) bool {
	if l.calls.Add(1)%1024 == 0 {
		l.Cleanup()
	}
	now := time.Now()
	s := l.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, updated: now}
		s.buckets[key] = b
	}
	b.tokens += now.Sub(b.updated).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.updated = now
	b.seen = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
func (l *Limiter) Cleanup() {
	cutoff := time.Now().Add(-l.ttl)
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if b.seen.Before(cutoff) {
				delete(s.buckets, k)
			}
		}
		s.mu.Unlock()
	}
}

func (l *Limiter) shard(key string) *limiterShard {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &l.shards[int(h%uint32(len(l.shards)))]
}

func RateLimit(ip, identity *Limiter, retryAfter time.Duration, next http.Handler) http.Handler {
	return RateLimitBy(ip, identity, retryAfter, identityKey, next)
}

func RateLimitBy(ip, identity *Limiter, retryAfter time.Duration, key func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if ip != nil && !ip.Allow(host) {
			writeLimited(w, errcode.CommonRateLimited, retryAfter, false, "ip_rate_limit")
			return
		}
		if identity != nil {
			if identityID := key(r); identityID != "" && !identity.Allow(identityID) {
				writeLimited(w, errcode.CommonRateLimited, retryAfter, false, "user_rate_limit")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func UserKey(userID int64) string { return "user:" + strconv.FormatInt(userID, 10) }

func identityKey(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if v == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:8])
}
func writeLimited(w http.ResponseWriter, code errcode.Code, after time.Duration, rpcEnvelope bool, reason string) {
	ms := after.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Retry-After-Ms", strconv.FormatInt(ms, 10))
	w.Header().Set("X-Capacity-Reason", reason)
	w.WriteHeader(errcode.HTTPStatus(code))
	payload := map[string]any{"code": string(code), "message": "capacity temporarily exhausted", "reason": reason, "retry_after_ms": ms}
	if rpcEnvelope {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": payload})
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}
