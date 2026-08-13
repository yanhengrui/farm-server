package worker

import (
	"encoding/json"
	"testing"
	"time"
)

// TestFindSmallestMaturePlot 覆盖纯函数 findSmallestMaturePlot 的各分支。
func TestFindSmallestMaturePlot_EmptyJSON(t *testing.T) {
	_, ok := findSmallestMaturePlot([]byte("{}"), time.Now())
	if ok {
		t.Fatal("expected false for empty snapshot")
	}
}

func TestFindSmallestMaturePlot_InvalidJSON(t *testing.T) {
	_, ok := findSmallestMaturePlot([]byte("not-json"), time.Now())
	if ok {
		t.Fatal("expected false for invalid JSON")
	}
}

func TestFindSmallestMaturePlot_NoMaturePlots(t *testing.T) {
	now := time.Now().UTC()
	snap := petSnapRaw{
		Plots: map[string]petPlotRaw{
			"1": {Status: "GROWING", MatureAt: now.Add(10 * time.Minute)},
			"2": {Status: "EMPTY"},
		},
	}
	raw, _ := json.Marshal(snap)
	_, ok := findSmallestMaturePlot(raw, now)
	if ok {
		t.Fatal("expected false: no mature plots yet")
	}
}

func TestFindSmallestMaturePlot_SingleMature(t *testing.T) {
	now := time.Now().UTC()
	snap := petSnapRaw{
		Plots: map[string]petPlotRaw{
			"3": {Status: "GROWING", MatureAt: now.Add(-1 * time.Second)},
		},
	}
	raw, _ := json.Marshal(snap)
	plotID, ok := findSmallestMaturePlot(raw, now)
	if !ok {
		t.Fatal("expected true for single mature plot")
	}
	if plotID != 3 {
		t.Fatalf("expected plotID=3, got %d", plotID)
	}
}

func TestFindSmallestMaturePlot_MultiMature_ReturnsSmallest(t *testing.T) {
	now := time.Now().UTC()
	snap := petSnapRaw{
		Plots: map[string]petPlotRaw{
			"5": {Status: "GROWING", MatureAt: now.Add(-5 * time.Second)},
			"2": {Status: "GROWING", MatureAt: now.Add(-2 * time.Second)},
			"8": {Status: "GROWING", MatureAt: now.Add(-1 * time.Second)},
		},
	}
	raw, _ := json.Marshal(snap)
	plotID, ok := findSmallestMaturePlot(raw, now)
	if !ok {
		t.Fatal("expected true")
	}
	if plotID != 2 {
		t.Fatalf("expected plotID=2 (smallest), got %d", plotID)
	}
}

func TestFindSmallestMaturePlot_ZeroMatureAt_Skipped(t *testing.T) {
	now := time.Now().UTC()
	snap := petSnapRaw{
		Plots: map[string]petPlotRaw{
			"1": {Status: "GROWING"}, // MatureAt 零值，跳过
		},
	}
	raw, _ := json.Marshal(snap)
	_, ok := findSmallestMaturePlot(raw, now)
	if ok {
		t.Fatal("expected false: zero MatureAt should be skipped")
	}
}

func TestPetAutoHarvestKeyStableForPersistedSchedule(t *testing.T) {
	scheduled := time.Date(2026, 8, 3, 10, 11, 12, 123000000, time.UTC)
	first := petAutoHarvestKey(42, scheduled, 3)
	second := petAutoHarvestKey(42, scheduled, 3)
	if first != second {
		t.Fatalf("first=%q second=%q", first, second)
	}
	if first == petAutoHarvestKey(42, scheduled.Add(time.Millisecond), 3) {
		t.Fatal("key must change when the persisted schedule changes")
	}
	if first == petAutoHarvestKey(42, scheduled, 4) {
		t.Fatal("key must change when the selected plot changes")
	}
}
