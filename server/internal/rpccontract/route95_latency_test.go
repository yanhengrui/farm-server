package rpccontract

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func route95Percentile(values []time.Duration, percentile int) time.Duration {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[(len(values)-1)*percentile/100]
}

// TestRoute95HTTPGRPCLatencyComparison keeps the route 9.3 rollback path and
// native gRPC path under the same fixture and records directly comparable local
// latency/allocation evidence. It is a regression gate, not a network capacity
// benchmark.
func TestRoute95HTTPGRPCLatencyComparison(t *testing.T) {
	fixture := &fixture{now: time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)}
	server := farmrpc.NewServer(fixture, fixture, fixture)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	server.RegisterGRPC(grpcServer)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()
	conn, err := grpc.NewClient("passthrough:///route95-bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	httpClient := farmrpc.NewClient(httpServer.URL)
	grpcClient := farmrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewFarmServiceClient(conn), rpcgrpc.ModeGRPC)

	const requests = 2000
	measure := func(name string, call func() error) (time.Duration, time.Duration, float64) {
		latencies := make([]time.Duration, 0, requests)
		started := time.Now()
		for range requests {
			requestStarted := time.Now()
			if err := call(); err != nil {
				t.Fatalf("%s request: %v", name, err)
			}
			latencies = append(latencies, time.Since(requestStarted))
		}
		elapsed := time.Since(started)
		return route95Percentile(latencies, 95), route95Percentile(latencies, 99), float64(requests) / elapsed.Seconds()
	}
	httpCall := func() error { _, err := httpClient.GetSnapshot(t.Context(), 11); return err }
	grpcCall := func() error { _, err := grpcClient.GetSnapshot(t.Context(), 11); return err }
	// Warm both connection paths before measuring.
	if err := httpCall(); err != nil {
		t.Fatal(err)
	}
	if err := grpcCall(); err != nil {
		t.Fatal(err)
	}
	httpP95, httpP99, httpRPS := measure("http", httpCall)
	grpcP95, grpcP99, grpcRPS := measure("grpc", grpcCall)
	httpAllocs := testing.AllocsPerRun(100, func() { _ = httpCall() })
	grpcAllocs := testing.AllocsPerRun(100, func() { _ = grpcCall() })
	if httpP99 > 20*time.Millisecond || grpcP99 > 20*time.Millisecond {
		t.Fatalf("unexplained local tail regression: HTTP p99=%s gRPC p99=%s", httpP99, grpcP99)
	}
	t.Logf("HTTP rps=%.0f p95=%s p99=%s allocs/op=%.1f; gRPC rps=%.0f p95=%s p99=%s allocs/op=%.1f",
		httpRPS, httpP95, httpP99, httpAllocs, grpcRPS, grpcP95, grpcP99, grpcAllocs)
}
