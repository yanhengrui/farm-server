package actor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
)

type benchmarkCommitter struct{}

func (benchmarkCommitter) CommitFarmCommand(context.Context, application.CommitRequest) (application.CommitResult, error) {
	return application.CommitResult{NewVersion: 1}, nil
}

func BenchmarkSchedulerSubmit(b *testing.B) {
	cfg := DefaultConfig()
	cfg.SchedulerShards = 64
	cfg.Workers = 64
	cfg.ReadyCap = 128
	cfg.IngressCap = 256
	cfg.ExecutionTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := NewRuntimeWithConfig(cfg, benchmarkCommitter{}, nil, clock.System{})
	rt.Start(ctx)
	var seq, rejected atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			_, err := rt.Submit(ctx, domain.Command{CmdID: "bench", FarmID: (n % 4096) + 1, Type: domain.CmdWater})
			if err != nil {
				rejected.Add(1)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(rejected.Load()), "rejected")
}
