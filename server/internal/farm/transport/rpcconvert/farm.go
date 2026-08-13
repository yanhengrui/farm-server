package rpcconvert

import (
	"sort"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func CommandToProto(c domain.Command) *rpcv1.FarmCommand {
	return &rpcv1.FarmCommand{CmdId: c.CmdID, FarmId: c.FarmID, ActorUser: c.ActorUser, Type: string(c.Type), PlotId: c.PlotID, CropId: c.CropID, Quantity: c.Quantity, BaseVersion: c.BaseVersion, RouteEpoch: c.RouteEpoch, PetScheduledAt: timestamp(c.PetScheduledAt)}
}

func CommandFromProto(c *rpcv1.FarmCommand) domain.Command {
	if c == nil {
		return domain.Command{}
	}
	return domain.Command{CmdID: c.CmdId, FarmID: c.FarmId, ActorUser: c.ActorUser, Type: domain.CommandType(c.Type), PlotID: c.PlotId, CropID: c.CropId, Quantity: c.Quantity, BaseVersion: c.BaseVersion, RouteEpoch: c.RouteEpoch, PetScheduledAt: timeValue(c.PetScheduledAt)}
}

func PlotToProto(p domain.Plot) *rpcv1.FarmPlot {
	return &rpcv1.FarmPlot{PlotId: p.PlotID, CropId: p.CropID, Status: string(p.Status), PlantedAt: timestamp(p.PlantedAt), MatureAt: timestamp(p.MatureAt), WateredCount: int32(p.WateredCount), RemainingYield: p.RemainingYield}
}

func PlotFromProto(p *rpcv1.FarmPlot) domain.Plot {
	if p == nil {
		return domain.Plot{}
	}
	return domain.Plot{PlotID: p.PlotId, CropID: p.CropId, Status: domain.PlotStatus(p.Status), PlantedAt: timeValue(p.PlantedAt), MatureAt: timeValue(p.MatureAt), WateredCount: int(p.WateredCount), RemainingYield: p.RemainingYield}
}

func SnapshotToProto(s domain.Snapshot) *rpcv1.FarmSnapshot {
	plots := make([]*rpcv1.FarmPlot, 0, len(s.Plots))
	for _, p := range s.Plots {
		plots = append(plots, PlotToProto(p))
	}
	sort.Slice(plots, func(i, j int) bool { return plots[i].PlotId < plots[j].PlotId })
	return &rpcv1.FarmSnapshot{FarmId: s.FarmID, OwnerId: s.OwnerID, Version: s.Version, Plots: plots, RouteEpoch: s.RouteEpoch}
}

func SnapshotFromProto(s *rpcv1.FarmSnapshot) domain.Snapshot {
	if s == nil {
		return domain.Snapshot{}
	}
	out := domain.Snapshot{FarmID: s.FarmId, OwnerID: s.OwnerId, Version: s.Version, RouteEpoch: s.RouteEpoch, Plots: make(map[int32]domain.Plot, len(s.Plots))}
	for _, p := range s.Plots {
		v := PlotFromProto(p)
		out.Plots[v.PlotID] = v
	}
	return out
}

func ResultToProto(r application.CommitResult) *rpcv1.CommitResult {
	plots := make([]*rpcv1.FarmPlot, 0, len(r.Patch.Plots))
	for _, p := range r.Patch.Plots {
		plots = append(plots, PlotToProto(p))
	}
	return &rpcv1.CommitResult{NewVersion: r.NewVersion, Patch: &rpcv1.FarmPatch{FarmId: r.Patch.FarmID, Version: r.Patch.Version, Plots: plots, ActorUser: r.Patch.ActorUser}, EventId: r.EventID, Replayed: r.Replayed, CoinBalance: r.CoinBalance}
}

func ResultFromProto(r *rpcv1.CommitResult) application.CommitResult {
	if r == nil {
		return application.CommitResult{}
	}
	out := application.CommitResult{NewVersion: r.NewVersion, EventID: r.EventId, Replayed: r.Replayed, CoinBalance: r.CoinBalance}
	if r.Patch != nil {
		out.Patch = domain.Patch{FarmID: r.Patch.FarmId, Version: r.Patch.Version, ActorUser: r.Patch.ActorUser}
		for _, p := range r.Patch.Plots {
			out.Patch.Plots = append(out.Patch.Plots, PlotFromProto(p))
		}
	}
	return out
}

func timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeValue(t *timestamppb.Timestamp) time.Time {
	if t == nil || !t.IsValid() {
		return time.Time{}
	}
	return t.AsTime()
}
