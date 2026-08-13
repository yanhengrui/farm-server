package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
)

type captureSubmitter struct{ command domain.Command }

func (s *captureSubmitter) Submit(_ context.Context, command domain.Command) (application.CommitResult, error) {
	s.command = command
	return application.CommitResult{NewVersion: 2}, nil
}

func TestRoutedClientStampsResolvedEpoch(t *testing.T) {
	submitter := &captureSubmitter{}
	mux := http.NewServeMux()
	farmsvc.NewServer(submitter).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	resolver := fixedResolver{route: Route{Bucket: 1, Owner: discovery.Endpoint{InstanceID: "farm-a", HTTPAddr: server.URL}, Epoch: 17, State: RouteStateActive}}
	client := NewClient(resolver, rpcgrpc.ModeHTTP, rpcgrpc.ClientConfig{})
	_, err := client.SubmitCommand(t.Context(), domain.Command{CmdID: "cmd-1", FarmID: 42, RouteEpoch: 3})
	if err != nil {
		t.Fatal(err)
	}
	if submitter.command.RouteEpoch != 17 {
		t.Fatalf("route_epoch=%d", submitter.command.RouteEpoch)
	}
}
