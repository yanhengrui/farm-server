package domain

import (
	"testing"
	"time"
)

func TestGrowthStage(t *testing.T) {
	planted := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	mature := planted.Add(60 * time.Minute)

	p := Plot{
		PlotID:    1,
		CropID:    "wheat",
		Status:    PlotGrowing,
		PlantedAt: planted,
		MatureAt:  mature,
	}

	cases := []struct {
		name string
		now  time.Time
		want CropGrowthStage
	}{
		{"刚播种", planted, CropGrowthStageSeedling},
		{"成长 1/4", planted.Add(15 * time.Minute), CropGrowthStageSeedling},
		{"成长 49%", planted.Add(29 * time.Minute), CropGrowthStageSeedling},
		{"成长 50%", planted.Add(30 * time.Minute), CropGrowthStageSemiMature},
		{"成长 75%", planted.Add(45 * time.Minute), CropGrowthStageSemiMature},
		{"成长 99%", planted.Add(59 * time.Minute), CropGrowthStageSemiMature},
		{"恰好成熟", mature, CropGrowthStageMature},
		{"超过成熟", mature.Add(10 * time.Minute), CropGrowthStageMature},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := p.GrowthStage(c.now)
			if got != c.want {
				t.Errorf("GrowthStage at %v: got %s, want %s", c.now, got, c.want)
			}
		})
	}
}

func TestGrowthStage_EmptyPlot(t *testing.T) {
	p := Plot{PlotID: 1, Status: PlotEmpty}
	if got := p.GrowthStage(time.Now()); got != CropGrowthStageSeedling {
		t.Errorf("empty plot should return Seedling sentinel, got %s", got)
	}
}

func TestEffectiveStatus(t *testing.T) {
	planted := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	mature := planted.Add(5 * time.Minute)
	p := Plot{Status: PlotGrowing, PlantedAt: planted, MatureAt: mature}

	if s := p.EffectiveStatus(mature.Add(-time.Second)); s != PlotGrowing {
		t.Errorf("want GROWING before mature_at, got %s", s)
	}
	if s := p.EffectiveStatus(mature); s != PlotMature {
		t.Errorf("want MATURE at mature_at, got %s", s)
	}
}
