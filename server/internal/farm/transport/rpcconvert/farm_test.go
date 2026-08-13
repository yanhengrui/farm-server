package rpcconvert

import (
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/domain"
)

func TestCommandProtoRoundTripPreservesPetSchedule(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 3, 12, 0, 0, 123000000, time.UTC)
	want := domain.Command{
		CmdID: "pet-auto", FarmID: 11, ActorUser: 11, Type: domain.CmdPetAutoHarvest,
		PlotID: 3, RouteEpoch: 7, PetScheduledAt: scheduledAt,
	}
	got := CommandFromProto(CommandToProto(want))
	if got.CmdID != want.CmdID || got.FarmID != want.FarmID || got.Type != want.Type || got.PlotID != want.PlotID || !got.PetScheduledAt.Equal(scheduledAt) {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
}

func TestPlotProtoRoundTripPreservesRemainingYield(t *testing.T) {
	want := domain.Plot{PlotID: 2, CropID: "WHEAT", Status: domain.PlotGrowing, RemainingYield: 4}
	got := PlotFromProto(PlotToProto(want))
	if got.PlotID != want.PlotID || got.CropID != want.CropID || got.Status != want.Status || got.RemainingYield != 4 {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
}
