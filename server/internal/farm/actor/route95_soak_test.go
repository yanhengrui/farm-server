package actor

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/pkg/clock"
)

type route95SoakCommitter struct{ version atomic.Int64 }

func (c *route95SoakCommitter) CommitFarmCommand(_ context.Context, req application.CommitRequest) (application.CommitResult, error) {
	version := c.version.Add(1)
	return application.CommitResult{
		NewVersion: version,
		Patch:      domain.Patch{FarmID: req.Command.FarmID, Version: version, ActorUser: req.Command.ActorUser},
	}, nil
}

func TestRoute95FarmServerRestartColdActivation(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	firstCtx, stopFirst := context.WithCancel(context.Background())
	first := NewRuntimeFull(4, 16, committer, committer, clock.System{})
	first.Start(firstCtx)
	plant := domain.Command{CmdID: "route95-before-restart", FarmID: 9501, ActorUser: 9501, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"}
	result, err := first.Submit(firstCtx, plant)
	if err != nil || result.NewVersion != 1 {
		t.Fatalf("commit before restart: result=%+v err=%v", result, err)
	}
	stopFirst()
	first.Wait()

	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	second := NewRuntimeFull(4, 16, committer, committer, clock.System{})
	second.Start(secondCtx)
	harvest := domain.Command{CmdID: "route95-after-restart", FarmID: 9501, ActorUser: 9501, Type: domain.CmdHarvest, PlotID: 1, BaseVersion: 1}
	result, err = second.Submit(secondCtx, harvest)
	if err != nil || result.NewVersion != 2 {
		t.Fatalf("cold activation after restart: result=%+v err=%v", result, err)
	}
	snapshot, err := committer.LoadSnapshot(secondCtx, 9501)
	if err != nil || snapshot.Version != 2 || snapshot.Plots[1].Status != domain.PlotEmpty {
		t.Fatalf("authoritative snapshot after restart: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestRoute95ACKLossReplayAfterFarmServerRestart(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	firstCtx, stopFirst := context.WithCancel(context.Background())
	first := NewRuntimeFull(4, 16, committer, committer, clock.System{})
	first.Start(firstCtx)
	if _, err := first.Submit(firstCtx, domain.Command{
		CmdID: "route95-restart-prior", FarmID: 9503, ActorUser: 9503,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}); err != nil {
		t.Fatalf("prior command: %v", err)
	}
	command := domain.Command{
		CmdID: "route95-restart-ack-loss", FarmID: 9503, ActorUser: 9503,
		Type: domain.CmdPlant, PlotID: 1, CropID: "WHEAT", BaseVersion: 1,
	}
	committed, err := first.Submit(firstCtx, command)
	if err != nil || committed.NewVersion != 2 {
		t.Fatalf("commit before ACK loss: result=%+v err=%v", committed, err)
	}
	stopFirst()
	first.Wait()

	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	second := NewRuntimeFull(4, 16, committer, committer, clock.System{})
	second.Start(secondCtx)
	replayed, err := second.Submit(secondCtx, command)
	if err != nil || !replayed.Replayed || replayed.NewVersion != committed.NewVersion {
		t.Fatalf("replay after restart: committed=%+v replay=%+v err=%v", committed, replayed, err)
	}
}

func TestRoute95GameServerFailureAndRecovery(t *testing.T) {
	persistent := infrastructure.NewMemCommitter(clock.System{})
	failing := &failAfterCommitter{inner: persistent, failAfter: 0}
	runtimeCtx, stop := context.WithCancel(context.Background())
	defer stop()
	rt := NewRuntime(4, 16, failing)
	rt.Start(runtimeCtx)
	command := domain.Command{CmdID: "route95-gamesvr-down", FarmID: 9502, ActorUser: 9502, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"}
	started := time.Now()
	if _, err := rt.Submit(runtimeCtx, command); err == nil {
		t.Fatal("command unexpectedly succeeded while gamesvr was unavailable")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("gamesvr failure was not bounded: %s", elapsed)
	}
	failing.setFailAfter(999)
	command.CmdID = "route95-gamesvr-recovered"
	if result, err := rt.Submit(runtimeCtx, command); err != nil || result.NewVersion != 1 {
		t.Fatalf("command after gamesvr recovery: result=%+v err=%v", result, err)
	}
}

// TestRoute95SchedulerSoak is opt-in because the formal gate runs for 30-60
// minutes. ROUTE95_SOAK_DURATION controls the duration and ROUTE95_SOAK_RPS
// controls the paced request rate (default 20k/s). The in-memory committer
// isolates scheduler bounds; it does not replace the external MySQL gate.
func TestRoute95SchedulerSoak(t *testing.T) {
	durationText := os.Getenv("ROUTE95_SOAK_DURATION")
	if durationText == "" {
		t.Skip("set ROUTE95_SOAK_DURATION=30m to run the route 9.5 scheduler soak")
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid ROUTE95_SOAK_DURATION=%q", durationText)
	}
	targetRPS := 20000
	if value := os.Getenv("ROUTE95_SOAK_RPS"); value != "" {
		targetRPS, err = strconv.Atoi(value)
		if err != nil || targetRPS < 1000 {
			t.Fatalf("invalid ROUTE95_SOAK_RPS=%q", value)
		}
	}

	cfg := DefaultConfig()
	cfg.SchedulerShards = 64
	cfg.Workers = 64
	cfg.ReadyCap = 256
	cfg.IngressCap = 512
	cfg.FarmQueueCap = 8
	cfg.ExecutionTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	committer := &route95SoakCommitter{}
	rt := NewRuntimeWithConfig(cfg, committer, nil, clock.System{})
	rt.Start(ctx)

	// Warm all fixed farm states before measuring heap growth.
	for farmID := int64(1); farmID <= 4096; farmID++ {
		if _, err := rt.Submit(ctx, domain.Command{CmdID: "warm", FarmID: farmID, ActorUser: farmID}); err != nil {
			t.Fatalf("warm farm %d: %v", farmID, err)
		}
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	beforeGoroutines := runtime.NumGoroutine()

	jobs := make(chan int64, targetRPS/2)
	var success, rejected atomic.Int64
	var workers sync.WaitGroup
	for range 128 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				farmID := sequence%4096 + 1
				if _, err := rt.Submit(ctx, domain.Command{CmdID: "soak", FarmID: farmID, ActorUser: farmID}); err != nil {
					rejected.Add(1)
				} else {
					success.Add(1)
				}
			}
		}()
	}

	started := time.Now()
	deadline := started.Add(duration)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	sequence := int64(0)
	batch := targetRPS / 100
	for now := range ticker.C {
		if !now.Before(deadline) {
			break
		}
		for range batch {
			jobs <- sequence
			sequence++
		}
	}
	close(jobs)
	workers.Wait()
	elapsed := time.Since(started)
	cancel()
	rt.Wait()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	afterGoroutines := runtime.NumGoroutine()

	total := success.Load() + rejected.Load()
	rps := float64(total) / elapsed.Seconds()
	heapDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if rejected.Load() != 0 {
		t.Fatalf("soak rejected=%d success=%d stats=%+v", rejected.Load(), success.Load(), rt.Stats())
	}
	if rps < float64(targetRPS)*0.95 {
		t.Fatalf("soak rps %.0f below paced target %d", rps, targetRPS)
	}
	if heapDelta > 64<<20 {
		t.Fatalf("heap grew by %d bytes", heapDelta)
	}
	if afterGoroutines > beforeGoroutines+10 {
		t.Fatalf("goroutine leak: before=%d after=%d", beforeGoroutines, afterGoroutines)
	}
	t.Logf("duration=%s target_rps=%d achieved_rps=%.0f success=%d rejected=0 heap_delta_bytes=%d goroutines_before=%d goroutines_after=%d",
		elapsed, targetRPS, rps, success.Load(), heapDelta, beforeGoroutines, afterGoroutines)
}
