package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	wscontract "github.com/photon/farm-server/server/contracts/ws"
)

type staticTransport func(*http.Request) (*http.Response, error)

func (f staticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig([]string{"-urls", "http://gate-a:8080,http://gate-b:8080", "-users", "20", "-target-rps", "10000", "-min-accepted-ratio", "0.995", "-duration", "30m", "-warmup", "10s", "-mix", "purchase=40,sell=40,plant=10,water=5,harvest=5", "-hot-viewers", "1,20"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.URLs) != 2 || cfg.Users != 20 || cfg.TargetRPS != 10000 || cfg.MinAcceptRatio != 0.995 || cfg.Duration != 30*time.Minute || cfg.Mix.total() != 100 || len(cfg.HotViewers) != 2 {
		t.Fatalf("cfg=%+v", cfg)
	}
	if _, err := parseConfig([]string{"-mix", "unknown=1"}); err == nil {
		t.Fatal("expected unknown command error")
	}
	if _, err := parseConfig([]string{"-min-accepted-ratio", "1.01"}); err == nil {
		t.Fatal("expected invalid accepted ratio error")
	}
}

func TestFarmVersionsAdvanceIndependently(t *testing.T) {
	a, _ := newFarmState(snapshotResponse{FarmID: "1", Version: "4", Plots: []snapshotPlot{{PlotID: 1, Status: "EMPTY"}}})
	b, _ := newFarmState(snapshotResponse{FarmID: "2", Version: "9", Plots: []snapshotPlot{{PlotID: 1, Status: "EMPTY"}}})
	if err := a.advance(5, wscontract.PlotPatch{PlotID: 1, State: "PLANTED", CropID: "WHEAT", RemainingYield: 5}); err != nil {
		t.Fatal(err)
	}
	if a.snapshotVersion() != 5 || b.snapshotVersion() != 9 {
		t.Fatalf("versions a=%d b=%d", a.snapshotVersion(), b.snapshotVersion())
	}
	if err := a.advance(4, wscontract.PlotPatch{}); err == nil {
		t.Fatal("expected version regression rejection")
	}
}

func TestPerVirtualUserSequenceDoesNotPinUserToSell(t *testing.T) {
	farm, _ := newFarmState(snapshotResponse{FarmID: "1", Version: "0", Plots: []snapshotPlot{{PlotID: 1, Status: "EMPTY"}}})
	user := &virtualUser{}
	mix := workloadMix{Purchase: 50, Sell: 50}
	user.commandSequence++
	if got := chooseCommand(mix, user.commandSequence, farm).name; got != "Purchase" {
		t.Fatalf("first per-user command=%q want Purchase", got)
	}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		user.commandSequence++
		seen[chooseCommand(mix, user.commandSequence, farm).name] = true
	}
	if !seen["Purchase"] || !seen["Sell"] {
		t.Fatalf("per-user command cycle=%v want both Purchase and Sell", seen)
	}
}

func TestCommandIDsAreUniqueAndStableForReplay(t *testing.T) {
	farm, _ := newFarmState(snapshotResponse{FarmID: "1", Version: "0", Plots: []snapshotPlot{{PlotID: 1, Status: "EMPTY"}}})
	mix := workloadMix{Purchase: 1}
	seen := make(map[string]struct{})
	for i := uint64(0); i < 1000; i++ {
		cmd := chooseCommand(mix, i, farm)
		original := cmd.cmdID
		// A retry reuses the pending command instead of selecting a new one.
		retry := cmd
		if retry.cmdID != original {
			t.Fatal("retry changed cmd_id")
		}
		if _, exists := seen[cmd.cmdID]; exists {
			t.Fatalf("duplicate cmd_id %q", cmd.cmdID)
		}
		seen[cmd.cmdID] = struct{}{}
	}
}

func TestChooseCommandReportsUnsatisfiedFarmState(t *testing.T) {
	farm, _ := newFarmState(snapshotResponse{FarmID: "1", Version: "0", Plots: []snapshotPlot{{PlotID: 1, Status: "GROWING", MatureAt: time.Now().Add(time.Hour).Format(time.RFC3339Nano)}}})
	for _, tc := range []struct {
		mix    workloadMix
		name   string
		reason string
	}{
		{mix: workloadMix{Plant: 1}, name: "Plant", reason: "no_plantable_plot"},
		{mix: workloadMix{Harvest: 1}, name: "Harvest", reason: "no_harvestable_plot"},
	} {
		cmd := chooseCommand(tc.mix, 0, farm)
		if cmd.name != tc.name || cmd.skipReason != tc.reason {
			t.Fatalf("command=%+v want name=%q reason=%q", cmd, tc.name, tc.reason)
		}
	}
	farm.mu.Lock()
	plot := farm.plots[1]
	plot.watered = true
	farm.plots[1] = plot
	farm.mu.Unlock()
	cmd := chooseCommand(workloadMix{Water: 1}, 0, farm)
	if cmd.name != "Water" || cmd.skipReason != "no_waterable_plot" {
		t.Fatalf("command=%+v want unavailable Water", cmd)
	}
}

func TestACKLossTargetsReceiptBackedCommands(t *testing.T) {
	if !supportsDurableReplay("Plant") || !supportsDurableReplay("Harvest") {
		t.Fatal("receipt-backed farm commands must support the ACK-loss scenario")
	}
	if supportsDurableReplay("Water") || supportsDurableReplay("Purchase") {
		t.Fatal("ACK-loss WebSocket replay must not target commands without cmd_receipts")
	}
}

func TestErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   bool
	}{{429, "", true}, {409, "", true}, {0, "RESOURCE_EXHAUSTED", true}, {500, "INTERNAL", false}} {
		if got := isRejected(tc.status, tc.code); got != tc.want {
			t.Fatalf("status=%d code=%q got=%t", tc.status, tc.code, got)
		}
	}
}

func TestRequestJSONPreservesCapacityReason(t *testing.T) {
	client := &http.Client{Transport: staticTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header: http.Header{
				"Content-Type":      []string{"application/json"},
				"X-Capacity-Reason": []string{"gateway_global"},
			},
			Body: io.NopCloser(strings.NewReader(`{"code":"COMMON_RESOURCE_EXHAUSTED","message":"busy","reason":"gateway_global"}`)),
		}, nil
	})}
	status, code, err := requestJSON(context.Background(), client, http.MethodPost, "http://example.test", "", "", nil, nil)
	if err == nil || status != http.StatusServiceUnavailable || code != "COMMON_RESOURCE_EXHAUSTED@gateway_global" {
		t.Fatalf("status=%d code=%q err=%v", status, code, err)
	}
}

func TestTransportErrorReason(t *testing.T) {
	if got := transportErrorReason(context.DeadlineExceeded); got != "downstream_timeout" {
		t.Fatalf("reason=%q", got)
	}
}

func TestSuccessCountsCompletedCommandWithoutRetainingCommandID(t *testing.T) {
	stats := newCounters()
	stats.success(true, time.Millisecond)
	if stats.accepted.Load() != 1 || stats.ackOK.Load() != 1 {
		t.Fatalf("accepted=%d ack=%d", stats.accepted.Load(), stats.ackOK.Load())
	}
}

func TestResultStatus(t *testing.T) {
	cfg := runConfig{TargetRPS: 100, MinAcceptRatio: 1}
	tests := []struct {
		name        string
		accepted    uint64
		failed      uint64
		queueFull   uint64
		skipped     uint64
		interrupted bool
		want        string
	}{
		{name: "complete at target", accepted: 100, want: statusComplete},
		{name: "below target", accepted: 99, want: statusFailed},
		{name: "command failure", accepted: 100, failed: 1, want: statusFailed},
		{name: "load generator overflow", accepted: 100, queueFull: 1, want: statusFailed},
		{name: "unsatisfied workload mix", accepted: 100, skipped: 1, want: statusFailed},
		{name: "interrupted takes precedence", accepted: 100, failed: 1, interrupted: true, want: statusInterrupted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := newCounters()
			stats.accepted.Store(tt.accepted)
			stats.failed.Store(tt.failed)
			stats.queueFull.Store(tt.queueFull)
			stats.skipped.Store(tt.skipped)
			got, _ := resultStatus(cfg, stats, time.Second, tt.interrupted)
			if got != tt.want {
				t.Fatalf("status=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestDispatchIsCancelable(t *testing.T) {
	users := []*virtualUser{{jobs: make(chan struct{}, 1)}}
	stats := newCounters()
	ctx, cancel := context.WithCancel(context.Background())
	var samples []minuteSample
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		dispatch(ctx, users, 1000, stats, &samples, &mu)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop")
	}
	if stats.attempted.Load() == 0 {
		t.Fatal("dispatcher attempted no work")
	}
}
