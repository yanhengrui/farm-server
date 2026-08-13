package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/pkg/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func startRoute95Etcd(t *testing.T) (*embed.Etcd, *clientv3.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("route 9.5 embedded etcd integration test")
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.UnsafeNoFsync = true
	peerURL, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clientURL, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		server.Server.Stop()
		t.Fatal("embedded etcd did not become ready")
	}
	endpoint := "http://" + server.Clients[0].Addr().String()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("create etcd client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

type runningController struct {
	controller *Controller
	cancel     context.CancelFunc
	done       chan error
}

func startRoute95Controller(t *testing.T, parent context.Context, cli *clientv3.Client, prefix string, buckets int, instanceID string) *runningController {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	c := NewController(cli, prefix, buckets, discovery.Endpoint{
		InstanceID: instanceID,
		HTTPAddr:   "http://" + instanceID,
		GRPCAddr:   "dns:///" + instanceID,
	}, 5*time.Second, 20*time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return &runningController{controller: c, cancel: cancel, done: done}
}

func waitRoute95(t *testing.T, timeout time.Duration, condition func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var detail string
	for time.Now().Before(deadline) {
		if ok, current := condition(); ok {
			return
		} else {
			detail = current
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met in %s: %s", timeout, detail)
}

func loadRoute95Routes(ctx context.Context, t *testing.T, cli *clientv3.Client, prefix string) map[int]Route {
	t.Helper()
	resp, err := cli.Get(ctx, routePrefix(prefix), clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("load routes: %v", err)
	}
	routes := make(map[int]Route, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var route Route
		if err := json.Unmarshal(kv.Value, &route); err != nil {
			t.Fatalf("decode route: %v", err)
		}
		routes[route.Bucket] = route
	}
	return routes
}

func farmForRoute95Bucket(bucket, bucketCount int) int64 {
	for farmID := int64(1); ; farmID++ {
		if BucketFor(farmID, bucketCount) == bucket {
			return farmID
		}
	}
}

// TestRoute95EtcdScaleAndOwnerFailover is the route 9.5 control-plane gate:
// real embedded etcd, 1/2/4/8 live farmsvr instances, revision-watch routing,
// lease revocation, deterministic redistribution, and monotonically increasing
// persistent epochs.
func TestRoute95EtcdScaleAndOwnerFailover(t *testing.T) {
	_, cli := startRoute95Etcd(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const (
		prefix  = "/route95"
		buckets = 64
	)

	resolver := NewResolver(cli, prefix, buckets)
	if err := resolver.Start(ctx); err != nil {
		t.Fatalf("start route resolver: %v", err)
	}
	controllers := make([]*runningController, 0, 8)
	for _, target := range []int{1, 2, 4, 8} {
		for len(controllers) < target {
			controllers = append(controllers, startRoute95Controller(t, ctx, cli, prefix, buckets, fmt.Sprintf("farm-%d", len(controllers))))
		}
		waitRoute95(t, 15*time.Second, func() (bool, string) {
			routes := loadRoute95Routes(ctx, t, cli, prefix)
			counts := make(map[string]int)
			for _, route := range routes {
				if route.State != RouteStateActive || route.Owner.InstanceID == "" {
					return false, fmt.Sprintf("target=%d route=%+v", target, route)
				}
				counts[route.Owner.InstanceID]++
			}
			if len(routes) != buckets || len(counts) != target {
				return false, fmt.Sprintf("target=%d routes=%d owners=%v", target, len(routes), counts)
			}
			return true, ""
		})
		routes := loadRoute95Routes(ctx, t, cli, prefix)
		counts := make(map[string]int)
		for _, route := range routes {
			counts[route.Owner.InstanceID]++
		}
		t.Logf("scale=%d route_distribution=%v", target, counts)
	}

	before := loadRoute95Routes(ctx, t, cli, prefix)
	victimID := "farm-0"
	victimEpochs := make(map[int]int64)
	for bucket, route := range before {
		if route.Owner.InstanceID == victimID {
			victimEpochs[bucket] = route.Epoch
		}
	}
	if len(victimEpochs) == 0 {
		t.Fatal("victim owns no route bucket")
	}
	controllers[0].cancel()
	if _, err := cli.Revoke(ctx, controllers[0].controller.leaseID); err != nil {
		t.Fatalf("revoke victim lease: %v", err)
	}
	waitRoute95(t, 15*time.Second, func() (bool, string) {
		routes := loadRoute95Routes(ctx, t, cli, prefix)
		if len(routes) != buckets {
			return false, fmt.Sprintf("routes=%d", len(routes))
		}
		for bucket, route := range routes {
			if route.State != RouteStateActive || route.Owner.InstanceID == victimID {
				return false, fmt.Sprintf("bucket=%d route=%+v", bucket, route)
			}
			if oldEpoch, changed := victimEpochs[bucket]; changed && route.Epoch <= oldEpoch {
				return false, fmt.Sprintf("bucket=%d epoch did not advance: %d <= %d", bucket, route.Epoch, oldEpoch)
			}
		}
		return true, ""
	})

	after := loadRoute95Routes(ctx, t, cli, prefix)
	bucketsChanged := make([]int, 0, len(victimEpochs))
	for bucket := range victimEpochs {
		bucketsChanged = append(bucketsChanged, bucket)
	}
	sort.Ints(bucketsChanged)
	farmID := farmForRoute95Bucket(bucketsChanged[0], buckets)
	waitRoute95(t, 5*time.Second, func() (bool, string) {
		route, err := resolver.Resolve(farmID)
		if err != nil {
			return false, err.Error()
		}
		want := after[bucketsChanged[0]]
		return route.Owner.InstanceID == want.Owner.InstanceID && route.Epoch == want.Epoch,
			fmt.Sprintf("resolver=%+v want=%+v", route, want)
	})
	t.Logf("failover victim=%s moved_buckets=%d sample_farm=%d", victimID, len(victimEpochs), farmID)

	for _, controller := range controllers[1:] {
		controller.cancel()
	}
}

// TestRoute95GameDiscoveryScale exercises the legacy HTTP rollback path against
// real etcd registration/watch and 1/2/4/8 live gamesvr endpoints. It records
// local routing throughput and tail latency while requiring exact round-robin
// distribution; it is a control-plane efficiency gate, not a MySQL capacity
// claim.
func TestRoute95GameDiscoveryScale(t *testing.T) {
	_, cli := startRoute95Etcd(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const prefix = "/route95-discovery"
	resolver := discovery.NewResolver(cli, prefix, "gamesvr")
	if err := resolver.Start(ctx); err != nil {
		t.Fatalf("start discovery resolver: %v", err)
	}
	transport := &discovery.HTTPTransport{LogicalHost: "gamesvr.internal", Resolver: resolver}
	httpClient := &http.Client{Transport: transport, Timeout: 2 * time.Second}

	type instance struct {
		server *httptest.Server
		hits   atomic.Int64
	}
	instances := make([]*instance, 0, 8)
	for _, target := range []int{1, 2, 4, 8} {
		for len(instances) < target {
			id := len(instances)
			current := &instance{}
			current.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				current.hits.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(current.server.Close)
			instances = append(instances, current)
			endpoint := discovery.Endpoint{
				InstanceID: fmt.Sprintf("game-%d", id),
				HTTPAddr:   current.server.URL,
				GRPCAddr:   fmt.Sprintf("127.0.0.1:%d", 20000+id),
			}
			go func() { _ = discovery.Register(ctx, cli, prefix, "gamesvr", endpoint, 5*time.Second) }()
		}
		waitRoute95(t, 10*time.Second, func() (bool, string) {
			return len(resolver.Endpoints()) == target, fmt.Sprintf("endpoints=%v", resolver.Endpoints())
		})
		for _, current := range instances {
			current.hits.Store(0)
		}
		const requests = 4000
		latencies := make([]time.Duration, 0, requests)
		started := time.Now()
		for range requests {
			requestStarted := time.Now()
			resp, err := httpClient.Get("http://gamesvr.internal/ping")
			if err != nil {
				t.Fatalf("scale=%d request: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("scale=%d status=%d", target, resp.StatusCode)
			}
			latencies = append(latencies, time.Since(requestStarted))
		}
		elapsed := time.Since(started)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		wantHits := int64(requests / target)
		counts := make([]int64, target)
		for i, current := range instances {
			counts[i] = current.hits.Load()
			if counts[i] != wantHits {
				t.Fatalf("scale=%d instance=%d hits=%d want=%d", target, i, counts[i], wantHits)
			}
		}
		t.Logf("scale=%d requests=%d rps=%.0f p95=%s p99=%s distribution=%v",
			target, requests, float64(requests)/elapsed.Seconds(), latencies[requests*95/100], latencies[requests*99/100], counts)
	}
}
