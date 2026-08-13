package farmsvc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// memSubmitter wraps MemCommitter to satisfy CommandSubmitter.
type memSubmitter struct {
	inner *infrastructure.MemCommitter
}

func (m *memSubmitter) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	return m.inner.CommitFarmCommand(ctx, application.CommitRequest{Command: cmd})
}

func newTestPairFarmsvc(t *testing.T) *Client {
	t.Helper()
	sub := &memSubmitter{inner: infrastructure.NewMemCommitter(clock.System{})}
	srv := NewServer(sub)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL)
}

func TestFarmsvc_SubmitPlant(t *testing.T) {
	client := newTestPairFarmsvc(t)
	ctx := t.Context()

	res, err := client.SubmitCommand(ctx, domain.Command{
		CmdID: "s1", FarmID: 1001, ActorUser: 1001,
		Type: domain.CmdPlant, PlotID: 0, CropID: "wheat",
	})
	if err != nil {
		t.Fatalf("SubmitCommand failed: %v", err)
	}
	if res.NewVersion != 1 {
		t.Errorf("expected version 1, got %d", res.NewVersion)
	}
}

func TestFarmsvc_SubmitError_Propagated(t *testing.T) {
	client := newTestPairFarmsvc(t)
	ctx := t.Context()

	// Plant once
	if _, err := client.SubmitCommand(ctx, domain.Command{
		CmdID: "s2", FarmID: 2001, ActorUser: 2001,
		Type: domain.CmdPlant, PlotID: 1, CropID: "wheat",
	}); err != nil {
		t.Fatalf("first plant failed: %v", err)
	}
	// Plant same plot → error
	_, err := client.SubmitCommand(ctx, domain.Command{
		CmdID: "s3", FarmID: 2001, ActorUser: 2001,
		Type: domain.CmdPlant, PlotID: 1, CropID: "wheat",
	})
	if err == nil {
		t.Fatal("expected error for occupied plot")
	}
	var e *errcode.Error
	if !errors.As(err, &e) {
		t.Fatalf("expected errcode.Error, got %T", err)
	}
}

type overloadedSubmitter struct{}

func (overloadedSubmitter) Submit(context.Context, domain.Command) (application.CommitResult, error) {
	return application.CommitResult{}, errcode.NewRetry(errcode.ResourceExhausted, "busy", 50*time.Millisecond)
}

func TestFarmsvc_RetryAfterPropagated(t *testing.T) {
	mux := http.NewServeMux()
	NewServer(overloadedSubmitter{}).RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, err := NewClient(ts.URL).SubmitCommand(t.Context(), domain.Command{CmdID: "busy", FarmID: 1})
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ResourceExhausted {
		t.Fatalf("error=%v", err)
	}
	if got := errcode.RetryAfter(err); got != 50*time.Millisecond {
		t.Fatalf("retry_after=%s", got)
	}
}
