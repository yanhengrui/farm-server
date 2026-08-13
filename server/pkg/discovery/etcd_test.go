package discovery

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type captureTransport struct{ host string }

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.host = req.URL.Host
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: req}, nil
}

func TestHTTPTransportRewritesLogicalHost(t *testing.T) {
	resolver := &Resolver{service: "gamesvr", endpoints: map[string]Endpoint{"game-1": {InstanceID: "game-1", HTTPAddr: "http://10.0.0.8:9090"}}}
	base := &captureTransport{}
	transport := &HTTPTransport{Base: base, LogicalHost: "gamesvr.internal", Resolver: resolver}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://gamesvr.internal/rpc", nil)
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if base.host != "10.0.0.8:9090" {
		t.Fatalf("host=%q", base.host)
	}
	if req.URL.Host != "gamesvr.internal" {
		t.Fatalf("original request was mutated: %q", req.URL.Host)
	}
}

func TestResolverRoundRobin(t *testing.T) {
	r := &Resolver{service: "gamesvr", endpoints: map[string]Endpoint{"b": {InstanceID: "b"}, "a": {InstanceID: "a"}}}
	first, _ := r.Next()
	second, _ := r.Next()
	third, _ := r.Next()
	if first.InstanceID != "a" || second.InstanceID != "b" || third.InstanceID != "a" {
		t.Fatalf("sequence=%s,%s,%s", first.InstanceID, second.InstanceID, third.InstanceID)
	}
}
