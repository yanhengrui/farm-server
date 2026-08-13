// Package discovery provides the small etcd-backed service registry used by
// route 9.4.  It deliberately stores only ephemeral instance endpoints; farm
// ownership and its persistent epoch live in internal/farm/routing and MySQL.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Endpoint struct {
	InstanceID string `json:"instance_id"`
	HTTPAddr   string `json:"http_addr"`
	GRPCAddr   string `json:"grpc_addr"`
}

func NewEtcdClient(endpoints, username, password string) (*clientv3.Client, error) {
	parts := strings.Split(endpoints, ",")
	out := parts[:0]
	for _, endpoint := range parts {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			out = append(out, endpoint)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("etcd endpoints are empty")
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   out,
		DialTimeout: 3 * time.Second,
		Username:    username,
		Password:    password,
	})
}

func serviceKey(prefix, service, instanceID string) string {
	return path.Join(prefix, "instances", service, instanceID)
}

func servicePrefix(prefix, service string) string {
	return path.Join(prefix, "instances", service) + "/"
}

// Register keeps one instance key attached to a lease until ctx is cancelled.
// Loss of keepalive is returned so the owning process can fail readiness rather
// than continuing as an undiscoverable instance.
func Register(ctx context.Context, cli *clientv3.Client, prefix, service string, endpoint Endpoint, ttl time.Duration) error {
	raw, err := json.Marshal(endpoint)
	if err != nil {
		return err
	}
	lease, err := cli.Grant(ctx, maxInt64(5, int64(ttl/time.Second)))
	if err != nil {
		return fmt.Errorf("grant %s lease: %w", service, err)
	}
	if _, err = cli.Put(ctx, serviceKey(prefix, service, endpoint.InstanceID), string(raw), clientv3.WithLease(lease.ID)); err != nil {
		return fmt.Errorf("register %s: %w", service, err)
	}
	keepalive, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("keepalive %s: %w", service, err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case reply, ok := <-keepalive:
			if !ok || reply == nil {
				return fmt.Errorf("%s lease keepalive closed", service)
			}
		}
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Resolver maintains a revision-ordered local view of one service's live
// instances. A compacted watch is recovered by taking a fresh snapshot.
type Resolver struct {
	cli     *clientv3.Client
	prefix  string
	service string

	mu         sync.RWMutex
	endpoints  map[string]Endpoint
	revision   int64
	roundRobin atomic.Uint64
	changed    chan struct{}
}

func NewResolver(cli *clientv3.Client, prefix, service string) *Resolver {
	return &Resolver{cli: cli, prefix: prefix, service: service, endpoints: make(map[string]Endpoint), changed: make(chan struct{}, 1)}
}

func (r *Resolver) Start(ctx context.Context) error {
	if err := r.sync(ctx); err != nil {
		return err
	}
	go r.watch(ctx)
	return nil
}

func (r *Resolver) sync(ctx context.Context) error {
	resp, err := r.cli.Get(ctx, servicePrefix(r.prefix, r.service), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("discover %s: %w", r.service, err)
	}
	next := make(map[string]Endpoint, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var endpoint Endpoint
		if json.Unmarshal(kv.Value, &endpoint) == nil && endpoint.InstanceID != "" {
			next[endpoint.InstanceID] = endpoint
		}
	}
	r.mu.Lock()
	r.endpoints, r.revision = next, resp.Header.Revision
	r.mu.Unlock()
	r.notify()
	return nil
}

func (r *Resolver) watch(ctx context.Context) {
	for ctx.Err() == nil {
		r.mu.RLock()
		revision := r.revision + 1
		r.mu.RUnlock()
		watch := r.cli.Watch(ctx, servicePrefix(r.prefix, r.service), clientv3.WithPrefix(), clientv3.WithRev(revision))
		resync := false
		for response := range watch {
			if response.Canceled || response.CompactRevision != 0 {
				resync = true
				break
			}
			r.mu.Lock()
			for _, event := range response.Events {
				instanceID := path.Base(string(event.Kv.Key))
				if event.Type == clientv3.EventTypeDelete {
					delete(r.endpoints, instanceID)
					continue
				}
				var endpoint Endpoint
				if json.Unmarshal(event.Kv.Value, &endpoint) == nil {
					r.endpoints[instanceID] = endpoint
				}
			}
			r.revision = response.Header.Revision
			r.mu.Unlock()
			r.notify()
		}
		if ctx.Err() != nil {
			return
		}
		_ = resync
		if r.sync(ctx) != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
}

func (r *Resolver) notify() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *Resolver) Changed() <-chan struct{} { return r.changed }

func (r *Resolver) Endpoints() []Endpoint {
	r.mu.RLock()
	out := make([]Endpoint, 0, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		out = append(out, endpoint)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out
}

func (r *Resolver) Next() (Endpoint, error) {
	all := r.Endpoints()
	if len(all) == 0 {
		return Endpoint{}, fmt.Errorf("no live %s instance", r.service)
	}
	idx := r.roundRobin.Add(1) - 1
	return all[idx%uint64(len(all))], nil
}

func (r *Resolver) Ready(context.Context) error {
	if len(r.Endpoints()) == 0 {
		return fmt.Errorf("no live %s instance", r.service)
	}
	return nil
}

// HTTPTransport rewrites one logical host to a live endpoint for every request.
// This gives all legacy HTTP clients discovery/LB without duplicating business
// clients and keeps the route 9.3 HTTP rollback path available.
type HTTPTransport struct {
	Base        http.RoundTripper
	LogicalHost string
	Resolver    *Resolver
}

func (t *HTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != t.LogicalHost {
		return t.base().RoundTrip(req)
	}
	endpoint, err := t.Resolver.Next()
	if err != nil {
		return nil, err
	}
	target, err := url.Parse(endpoint.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid discovered HTTP address %q: %w", endpoint.HTTPAddr, err)
	}
	clone := req.Clone(req.Context())
	clone.URL.Scheme, clone.URL.Host = target.Scheme, target.Host
	clone.Host = target.Host
	return t.base().RoundTrip(clone)
}

func (t *HTTPTransport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}
