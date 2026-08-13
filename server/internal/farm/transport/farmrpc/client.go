// Package farmrpc — Client 侧，由 farmsvr 使用。
// 实现 application.Committer + application.SnapshotLoader，通过 HTTP/JSON 调用 gamesvr。
package farmrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/transport/rpcconvert"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
)

// Client 通过 HTTP/JSON 调用 gamesvr 的 farmrpc 端点。
// 同时实现 application.Committer 和 application.SnapshotLoader。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.FarmServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.FarmServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。baseURL 是 gamesvr 内部地址，例如 "http://gamesvr:9090"。
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

var _ application.Committer = (*Client)(nil)
var _ application.SnapshotLoader = (*Client)(nil)
var _ application.RouteFencer = (*Client)(nil)

func (c *Client) AdvanceRouteEpoch(ctx context.Context, farmID, epoch int64) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.AdvanceRouteEpoch(ctx, &rpcv1.AdvanceRouteEpochRequest{FarmId: farmID, RouteEpoch: epoch})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, err := json.Marshal(AdvanceRouteEpochReq{FarmID: farmID, RouteEpoch: epoch})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/farm/advance-route-epoch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("advance route epoch farm_id=%d: %w", farmID, err)
	}
	defer resp.Body.Close()
	var out AdvanceRouteEpochResp
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// CommitFarmCommand 调用 gamesvr /rpc/farm/commit。
func (c *Client) CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.CommitFarmCommand(ctx, &rpcv1.CommitFarmCommandRequest{Command: rpcconvert.CommandToProto(req.Command)})
		if err == nil {
			return rpcconvert.ResultFromProto(out.Result), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return application.CommitResult{}, rpcgrpc.FromError(err)
		}
	}
	body, err := json.Marshal(CommitReq{Command: req.Command})
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("marshal_commit_req: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/farm/commit", bytes.NewReader(body))
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("new_request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("http_do commit farm_id=%d: %w", req.Command.FarmID, err)
	}
	defer resp.Body.Close()

	var out CommitResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return application.CommitResult{}, fmt.Errorf("decode_commit_resp: %w", err)
	}
	if out.Err != nil {
		return application.CommitResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	if out.Result == nil {
		return application.CommitResult{}, errcode.New(errcode.Internal, "gamesvr: empty commit result")
	}
	return *out.Result, nil
}

// GetSnapshot 调用 gamesvr /rpc/farm/get-snapshot，返回带 GrowthStage 的农场快照。
func (c *Client) GetSnapshot(ctx context.Context, farmID int64) (*FarmSnapshotDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.GetSnapshot(ctx, &rpcv1.GetSnapshotRequest{FarmId: farmID})
		if err == nil {
			return snapshotDTOFromProto(out.Snapshot), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, err := json.Marshal(GetSnapshotReq{FarmID: farmID})
	if err != nil {
		return nil, fmt.Errorf("marshal_get_snapshot_req: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/farm/get-snapshot", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new_request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http_do get_snapshot farm_id=%d: %w", farmID, err)
	}
	defer resp.Body.Close()

	var out GetSnapshotResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_get_snapshot_resp: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return normalizeSnapshotDTO(out.Snapshot), nil
}
func (c *Client) LoadSnapshot(ctx context.Context, farmID int64) (domain.Snapshot, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.LoadSnapshot(ctx, &rpcv1.LoadSnapshotRequest{FarmId: farmID})
		if err == nil {
			return rpcconvert.SnapshotFromProto(out.Snapshot), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return domain.Snapshot{}, rpcgrpc.FromError(err)
		}
	}
	body, err := json.Marshal(LoadSnapshotReq{FarmID: farmID})
	if err != nil {
		return domain.Snapshot{}, fmt.Errorf("marshal_load_snapshot_req: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/farm/load-snapshot", bytes.NewReader(body))
	if err != nil {
		return domain.Snapshot{}, fmt.Errorf("new_request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return domain.Snapshot{}, fmt.Errorf("http_do load_snapshot farm_id=%d: %w", farmID, err)
	}
	defer resp.Body.Close()

	var out LoadSnapshotResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return domain.Snapshot{}, fmt.Errorf("decode_load_snapshot_resp: %w", err)
	}
	if out.Err != nil {
		return domain.Snapshot{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	if out.Snapshot == nil {
		return domain.Snapshot{}, errcode.New(errcode.Internal, "gamesvr: empty snapshot result")
	}
	return *out.Snapshot, nil
}

func snapshotDTOFromProto(s *rpcv1.FarmSnapshotView) *FarmSnapshotDTO {
	if s == nil {
		return nil
	}
	out := &FarmSnapshotDTO{FarmID: s.FarmId, OwnerUserID: s.OwnerUserId, OwnerDisplayName: s.OwnerDisplayName, Version: s.Version, Plots: make([]PlotDTO, 0, len(s.Plots))}
	for _, p := range s.Plots {
		out.Plots = append(out.Plots, PlotDTO{PlotID: p.PlotId, Status: p.Status, CropID: p.CropId, GrowthStage: p.GrowthStage, PlantedAt: p.PlantedAt, MatureAt: p.MatureAt, RemainingYield: p.RemainingYield})
	}
	return normalizeSnapshotDTO(out)
}

func normalizeSnapshotDTO(snapshot *FarmSnapshotDTO) *FarmSnapshotDTO {
	if snapshot != nil && strings.TrimSpace(snapshot.OwnerDisplayName) == "" {
		snapshot.OwnerDisplayName = strconv.FormatInt(snapshot.OwnerUserID, 10)
	}
	return snapshot
}
