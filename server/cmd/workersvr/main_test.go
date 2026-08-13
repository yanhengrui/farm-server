package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/worker"
)

type countingRelay struct {
	calls atomic.Int32
	busy  int32
	block bool
}

func (r *countingRelay) ScanAndPublish(ctx context.Context) (int, error) {
	call := r.calls.Add(1)
	if r.block {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if call <= r.busy {
		return 1, nil
	}
	return 0, nil
}

type countingPet struct{ calls atomic.Int32 }

func (p *countingPet) Scan(context.Context, int) (worker.PetScanResult, error) {
	p.calls.Add(1)
	return worker.PetScanResult{}, nil
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestConsumerGroupKeepsDefaultAndSupportsIsolationPrefix(t *testing.T) {
	if got := consumerGroup("workersvr", "task"); got != "workersvr-task" {
		t.Fatalf("default group=%q", got)
	}
	if got := consumerGroup("route95-workersvr", "task"); got != "route95-workersvr-task" {
		t.Fatalf("isolated group=%q", got)
	}
}

func TestRunRelayLoopContinuouslyDrainsBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	relay := &countingRelay{busy: 3}
	done := make(chan struct{})
	go func() {
		runRelayLoop(ctx, relay, testLogger(), time.Second, time.Millisecond)
		close(done)
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for relay.calls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay loop did not stop after cancellation")
	}
	if calls := relay.calls.Load(); calls < 4 {
		t.Fatalf("calls=%d; backlog was not continuously drained", calls)
	}
}

func TestRunLoopWithPetKeepsPetSchedulingIndependentAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	relay := &countingRelay{block: true}
	pet := &countingPet{}
	done := make(chan error, 1)
	go func() {
		done <- runLoopWithPet(ctx, relay, pet, testLogger(), time.Second, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond, 50)
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for pet.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pet.calls.Load() == 0 {
		t.Fatal("pet scanner was starved by blocked relay")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("combined loop did not stop after cancellation")
	}
}
