package farmrpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

type testOwnerDisplayNames struct{}

func (testOwnerDisplayNames) LoadDisplayName(_ context.Context, userID int64) (string, error) {
	return fmt.Sprintf("Farmer_%d", userID), nil
}

func newTestPair(t *testing.T) *Client {
	t.Helper()
	committer := infrastructure.NewMemCommitter(clock.System{})
	srv := NewServer(committer, committer, testOwnerDisplayNames{})
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL)
}

// TestClient_CommitFarmCommand_Plant 验证 HTTP/JSON 全链路：Plant 成功，版本递增。
func TestClient_CommitFarmCommand_Plant(t *testing.T) {
	client := newTestPair(t)
	ctx := context.Background()

	res, err := client.CommitFarmCommand(ctx, application.CommitRequest{
		Command: domain.Command{
			CmdID:     "plant-1",
			FarmID:    1001,
			ActorUser: 1001,
			Type:      domain.CmdPlant,
			PlotID:    0,
			CropID:    "wheat",
		},
	})
	if err != nil {
		t.Fatalf("CommitFarmCommand failed: %v", err)
	}
	if res.NewVersion != 1 {
		t.Errorf("expected version 1, got %d", res.NewVersion)
	}
	if res.Replayed {
		t.Error("first commit should not be replayed")
	}
}

// TestClient_CommitFarmCommand_Idempotent 验证同一 cmd_id 重试命中幂等回执。
func TestClient_CommitFarmCommand_Idempotent(t *testing.T) {
	client := newTestPair(t)
	ctx := context.Background()

	cmd := application.CommitRequest{
		Command: domain.Command{
			CmdID: "plant-idem", FarmID: 2001, ActorUser: 2001,
			Type: domain.CmdPlant, PlotID: 1, CropID: "wheat",
		},
	}
	res1, err := client.CommitFarmCommand(ctx, cmd)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}
	res2, err := client.CommitFarmCommand(ctx, cmd)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if !res2.Replayed {
		t.Error("second commit should be replayed")
	}
	if res2.NewVersion != res1.NewVersion {
		t.Errorf("replay must not advance version: %d vs %d", res2.NewVersion, res1.NewVersion)
	}
}

// TestClient_CommitFarmCommand_ErrPropagation 验证错误码通过 HTTP/JSON 正确传播。
func TestClient_CommitFarmCommand_ErrPropagation(t *testing.T) {
	client := newTestPair(t)
	ctx := context.Background()

	// 先播种
	if _, err := client.CommitFarmCommand(ctx, application.CommitRequest{
		Command: domain.Command{CmdID: "p1", FarmID: 3001, ActorUser: 3001, Type: domain.CmdPlant, PlotID: 2, CropID: "corn", BaseVersion: 0},
	}); err != nil {
		t.Fatalf("plant failed: %v", err)
	}

	// 对同一地块再次播种，应返回 FarmPlotState 错误
	_, err := client.CommitFarmCommand(ctx, application.CommitRequest{
		Command: domain.Command{CmdID: "p2", FarmID: 3001, ActorUser: 3001, Type: domain.CmdPlant, PlotID: 2, CropID: "corn", BaseVersion: 1},
	})
	if err == nil {
		t.Fatal("expected error for planting occupied plot, got nil")
	}
	var e *errcode.Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *errcode.Error, got %T: %v", err, err)
	}
	if e.Code != errcode.FarmPlotState {
		t.Errorf("expected FarmPlotState, got %s", e.Code)
	}
}

// TestClient_LoadSnapshot_RoundTrip 验证 LoadSnapshot 正确返回空快照。
func TestClient_LoadSnapshot_RoundTrip(t *testing.T) {
	client := newTestPair(t)
	ctx := context.Background()

	snap, err := client.LoadSnapshot(ctx, 4001)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}
	if snap.FarmID != 4001 {
		t.Errorf("expected farm_id 4001, got %d", snap.FarmID)
	}
	if snap.Version != 0 {
		t.Errorf("expected version 0, got %d", snap.Version)
	}
}

func TestClient_GetSnapshotIncludesOwnerDisplayName(t *testing.T) {
	client := newTestPair(t)
	snapshot, err := client.GetSnapshot(t.Context(), 4001)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snapshot.OwnerUserID != 4001 || snapshot.OwnerDisplayName != "Farmer_4001" {
		t.Fatalf("unexpected owner identity: %+v", snapshot)
	}
}
