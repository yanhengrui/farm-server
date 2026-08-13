// Package routing implements route 9.4's virtual farm buckets. etcd owns only
// bounded control-plane state; MySQL route_epoch remains the write fence.
package routing

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/errcode"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	RouteStateActive    = "ACTIVE"
	RouteStateMigrating = "MIGRATING"
)

type Route struct {
	Bucket   int                `json:"bucket"`
	Owner    discovery.Endpoint `json:"owner"`
	Epoch    int64              `json:"epoch"`
	State    string             `json:"state"`
	Revision int64              `json:"-"`
}

func BucketFor(farmID int64, bucketCount int) int {
	if bucketCount <= 0 {
		return 0
	}
	value := uint64(farmID)
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	return int(value % uint64(bucketCount))
}

func routePrefix(prefix string) string          { return path.Join(prefix, "farm-routes") + "/" }
func routeKey(prefix string, bucket int) string { return routePrefix(prefix) + strconv.Itoa(bucket) }
func epochKey(prefix string, bucket int) string {
	return path.Join(prefix, "farm-route-epochs", strconv.Itoa(bucket))
}
func instancePrefix(prefix string) string          { return path.Join(prefix, "instances", "farmsvr") + "/" }
func instanceKey(prefix, instanceID string) string { return instancePrefix(prefix) + instanceID }

// Controller registers a farmsvr and converges every bucket to a deterministic
// rendezvous-hash owner. Ownership updates and monotonically increasing bucket
// epochs are committed in one etcd transaction.
type Controller struct {
	cli       *clientv3.Client
	prefix    string
	buckets   int
	endpoint  discovery.Endpoint
	leaseTTL  time.Duration
	reconcile time.Duration
	leaseID   clientv3.LeaseID
	ready     atomic.Bool
}

func NewController(cli *clientv3.Client, prefix string, buckets int, endpoint discovery.Endpoint, leaseTTL, reconcile time.Duration) *Controller {
	return &Controller{cli: cli, prefix: prefix, buckets: buckets, endpoint: endpoint, leaseTTL: leaseTTL, reconcile: reconcile}
}

func (c *Controller) Run(ctx context.Context) error {
	lease, err := c.cli.Grant(ctx, maxInt64(5, int64(c.leaseTTL/time.Second)))
	if err != nil {
		return fmt.Errorf("grant farmsvr lease: %w", err)
	}
	c.leaseID = lease.ID
	raw, _ := json.Marshal(c.endpoint)
	if _, err = c.cli.Put(ctx, instanceKey(c.prefix, c.endpoint.InstanceID), string(raw), clientv3.WithLease(c.leaseID)); err != nil {
		return fmt.Errorf("register farmsvr: %w", err)
	}
	keepalive, err := c.cli.KeepAlive(ctx, c.leaseID)
	if err != nil {
		return fmt.Errorf("keepalive farmsvr: %w", err)
	}
	if c.reconcile <= 0 {
		c.reconcile = time.Second
	}
	ticker := time.NewTicker(c.reconcile)
	defer ticker.Stop()
	for {
		if err := c.reconcileOnce(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		c.ready.Store(true)
		select {
		case <-ctx.Done():
			return nil
		case reply, ok := <-keepalive:
			if !ok || reply == nil {
				return fmt.Errorf("farmsvr lease keepalive closed")
			}
		case <-ticker.C:
		}
	}
}

func (c *Controller) Ready(context.Context) error {
	if !c.ready.Load() {
		return fmt.Errorf("farm route controller has not completed initial reconciliation")
	}
	return nil
}

func (c *Controller) reconcileOnce(ctx context.Context) error {
	resp, err := c.cli.Get(ctx, instancePrefix(c.prefix), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("list farmsvr instances: %w", err)
	}
	instances := make([]discovery.Endpoint, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var endpoint discovery.Endpoint
		if json.Unmarshal(kv.Value, &endpoint) == nil && endpoint.InstanceID != "" {
			instances = append(instances, endpoint)
		}
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].InstanceID < instances[j].InstanceID })
	routeResp, err := c.cli.Get(ctx, routePrefix(c.prefix), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("list farm routes: %w", err)
	}
	current := make(map[int]Route, len(routeResp.Kvs))
	for _, kv := range routeResp.Kvs {
		var route Route
		if json.Unmarshal(kv.Value, &route) == nil {
			current[route.Bucket] = route
		}
	}
	for bucket := 0; bucket < c.buckets; bucket++ {
		if desiredOwner(bucket, instances).InstanceID != c.endpoint.InstanceID {
			continue
		}
		route := current[bucket]
		if route.Owner.InstanceID == c.endpoint.InstanceID && route.State != RouteStateActive {
			if err := c.activate(ctx, route); err != nil {
				return err
			}
		} else if route.Owner.InstanceID != c.endpoint.InstanceID {
			if err := c.claim(ctx, bucket); err != nil {
				return err
			}
		}
	}
	return nil
}

func desiredOwner(bucket int, instances []discovery.Endpoint) discovery.Endpoint {
	var owner discovery.Endpoint
	var best uint64
	for _, instance := range instances {
		h := fnv.New64a()
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(bucket))
		_, _ = h.Write(encoded[:])
		_, _ = h.Write([]byte(instance.InstanceID))
		score := h.Sum64()
		if owner.InstanceID == "" || score > best {
			owner, best = instance, score
		}
	}
	return owner
}

func (c *Controller) claim(ctx context.Context, bucket int) error {
	routeResp, err := c.cli.Get(ctx, routeKey(c.prefix, bucket))
	if err != nil {
		return err
	}
	if len(routeResp.Kvs) == 1 {
		var current Route
		if json.Unmarshal(routeResp.Kvs[0].Value, &current) == nil && current.Owner.InstanceID == c.endpoint.InstanceID && current.State == RouteStateActive {
			return nil
		}
	}
	epochResp, err := c.cli.Get(ctx, epochKey(c.prefix, bucket))
	if err != nil {
		return err
	}
	var oldEpoch int64
	var epochCmp clientv3.Cmp
	if len(epochResp.Kvs) == 0 {
		epochCmp = clientv3.Compare(clientv3.Version(epochKey(c.prefix, bucket)), "=", 0)
	} else {
		oldEpoch, _ = strconv.ParseInt(string(epochResp.Kvs[0].Value), 10, 64)
		epochCmp = clientv3.Compare(clientv3.ModRevision(epochKey(c.prefix, bucket)), "=", epochResp.Kvs[0].ModRevision)
	}
	state := RouteStateActive
	if len(routeResp.Kvs) != 0 {
		state = RouteStateMigrating
	}
	next := Route{Bucket: bucket, Owner: c.endpoint, Epoch: oldEpoch + 1, State: state}
	raw, _ := json.Marshal(next)
	comparisons := []clientv3.Cmp{epochCmp}
	if len(routeResp.Kvs) == 0 {
		comparisons = append(comparisons, clientv3.Compare(clientv3.Version(routeKey(c.prefix, bucket)), "=", 0))
	} else {
		comparisons = append(comparisons, clientv3.Compare(clientv3.ModRevision(routeKey(c.prefix, bucket)), "=", routeResp.Kvs[0].ModRevision))
	}
	txn, err := c.cli.Txn(ctx).If(comparisons...).Then(
		clientv3.OpPut(epochKey(c.prefix, bucket), strconv.FormatInt(next.Epoch, 10)),
		clientv3.OpPut(routeKey(c.prefix, bucket), string(raw), clientv3.WithLease(c.leaseID)),
	).Commit()
	if err != nil {
		return fmt.Errorf("claim route bucket %d: %w", bucket, err)
	}
	// A failed comparison means another healthy controller won this iteration.
	_ = txn
	return nil
}

func (c *Controller) activate(ctx context.Context, route Route) error {
	key := routeKey(c.prefix, route.Bucket)
	resp, err := c.cli.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return err
	}
	var current Route
	if json.Unmarshal(resp.Kvs[0].Value, &current) != nil || current.Owner.InstanceID != c.endpoint.InstanceID || current.Epoch != route.Epoch {
		return nil
	}
	current.State = RouteStateActive
	raw, _ := json.Marshal(current)
	_, err = c.cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)).Then(clientv3.OpPut(key, string(raw), clientv3.WithLease(c.leaseID))).Commit()
	return err
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Resolver is the gatesvr revision-watch cache for farm routes.
type Resolver struct {
	cli      *clientv3.Client
	prefix   string
	buckets  int
	mu       sync.RWMutex
	routes   map[int]Route
	revision int64
}

func NewResolver(cli *clientv3.Client, prefix string, buckets int) *Resolver {
	return &Resolver{cli: cli, prefix: prefix, buckets: buckets, routes: make(map[int]Route)}
}

func (r *Resolver) Start(ctx context.Context) error {
	if err := r.sync(ctx); err != nil {
		return err
	}
	go r.watch(ctx)
	return nil
}

func (r *Resolver) Resolve(farmID int64) (Route, error) {
	bucket := BucketFor(farmID, r.buckets)
	r.mu.RLock()
	route, ok := r.routes[bucket]
	r.mu.RUnlock()
	if !ok || route.State != RouteStateActive || route.Owner.InstanceID == "" {
		return Route{}, errcode.New(errcode.RoutingOwnerNotFound, fmt.Sprintf("no active owner for route bucket %d", bucket))
	}
	return route, nil
}

func (r *Resolver) Ready(context.Context) error {
	r.mu.RLock()
	count := len(r.routes)
	r.mu.RUnlock()
	if count == 0 {
		return errcode.New(errcode.RoutingOwnerNotFound, "farm route table is empty")
	}
	return nil
}

func (r *Resolver) sync(ctx context.Context) error {
	resp, err := r.cli.Get(ctx, routePrefix(r.prefix), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("load farm routes: %w", err)
	}
	next := make(map[int]Route, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var route Route
		if json.Unmarshal(kv.Value, &route) == nil {
			route.Revision = kv.ModRevision
			next[route.Bucket] = route
		}
	}
	r.mu.Lock()
	r.routes, r.revision = next, resp.Header.Revision
	r.mu.Unlock()
	return nil
}

func (r *Resolver) watch(ctx context.Context) {
	for ctx.Err() == nil {
		r.mu.RLock()
		revision := r.revision + 1
		r.mu.RUnlock()
		watch := r.cli.Watch(ctx, routePrefix(r.prefix), clientv3.WithPrefix(), clientv3.WithRev(revision))
		resync := false
		for response := range watch {
			if response.Canceled || response.CompactRevision != 0 {
				resync = true
				break
			}
			r.mu.Lock()
			for _, event := range response.Events {
				bucket, parseErr := strconv.Atoi(strings.TrimPrefix(string(event.Kv.Key), routePrefix(r.prefix)))
				if parseErr != nil {
					continue
				}
				if event.Type == clientv3.EventTypeDelete {
					delete(r.routes, bucket)
					continue
				}
				var route Route
				if json.Unmarshal(event.Kv.Value, &route) == nil {
					route.Revision = event.Kv.ModRevision
					r.routes[bucket] = route
				}
			}
			r.revision = response.Header.Revision
			r.mu.Unlock()
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
