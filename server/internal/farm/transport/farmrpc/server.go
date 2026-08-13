// Package farmrpc — Server 侧，由 gamesvr 使用。
// 将 application.Committer / SnapshotLoader 暴露为 HTTP/JSON 端点，
// 供 farmsvr 通过 Client 调用。
package farmrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// Server 将农场 RPC 暴露为 HTTP/JSON；注册在 gamesvr 的内部监听端口（GRPC_LISTEN_ADDR）。
type Server struct {
	committer  application.Committer
	loader     application.SnapshotLoader
	ownerNames OwnerDisplayNameLoader
}

// OwnerDisplayNameLoader resolves the public account name shown on a farm.
type OwnerDisplayNameLoader interface {
	LoadDisplayName(ctx context.Context, userID int64) (string, error)
}

// NewServer 构造 Server。ownerNames 是公共 Snapshot 必需的账号资料依赖。
func NewServer(committer application.Committer, loader application.SnapshotLoader, ownerNames OwnerDisplayNameLoader) *Server {
	return &Server{committer: committer, loader: loader, ownerNames: ownerNames}
}

// RegisterRoutes 将 RPC 端点注册到 mux。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/farm/commit", s.handleCommit)
	mux.HandleFunc("/rpc/farm/load-snapshot", s.handleLoadSnapshot)
	mux.HandleFunc("/rpc/farm/get-snapshot", s.handleGetSnapshot)
	mux.HandleFunc("/rpc/farm/advance-route-epoch", s.handleAdvanceRouteEpoch)
}

func (s *Server) handleAdvanceRouteEpoch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AdvanceRouteEpochReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, AdvanceRouteEpochResp{Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error()}})
		return
	}
	fencer, ok := s.committer.(application.RouteFencer)
	if !ok {
		writeResp(w, http.StatusNotImplemented, AdvanceRouteEpochResp{Err: &RPCErr{Code: string(errcode.Internal), Message: "route fencer not configured"}})
		return
	}
	if err := fencer.AdvanceRouteEpoch(r.Context(), req.FarmID, req.RouteEpoch); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), AdvanceRouteEpochResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, AdvanceRouteEpochResp{})
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CommitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, CommitResp{
			Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error()},
		})
		return
	}
	result, err := s.committer.CommitFarmCommand(r.Context(), application.CommitRequest{Command: req.Command})
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), CommitResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, CommitResp{Result: &result})
}

func (s *Server) handleLoadSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req LoadSnapshotReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, LoadSnapshotResp{
			Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error()},
		})
		return
	}
	snap, err := s.loader.LoadSnapshot(r.Context(), req.FarmID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), LoadSnapshotResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, LoadSnapshotResp{Snapshot: &snap})
}

// toRPCErr 将内部错误转为可跨服务传输的 RPCErr。
func toRPCErr(err error) *RPCErr {
	var e *errcode.Error
	if errors.As(err, &e) {
		return &RPCErr{Code: string(e.Code), Message: e.Message, Reason: e.Reason, RetryAfterMs: errcode.RetryAfter(err).Milliseconds()}
	}
	return &RPCErr{Code: string(errcode.Internal), Message: err.Error()}
}

func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GetSnapshotReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, GetSnapshotResp{
			Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error()},
		})
		return
	}
	raw, err := s.loader.LoadSnapshot(r.Context(), req.FarmID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GetSnapshotResp{Err: rpcErr})
		return
	}
	ownerDisplayName, err := s.loadOwnerDisplayName(r.Context(), raw.OwnerID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), GetSnapshotResp{Err: rpcErr})
		return
	}

	now := time.Now().UTC()
	plots := make([]PlotDTO, 0, len(raw.Plots))
	for _, p := range raw.Plots {
		stage := p.GrowthStage(now)
		plots = append(plots, PlotDTO{
			PlotID:         p.PlotID,
			Status:         string(p.Status),
			CropID:         p.CropID,
			GrowthStage:    string(stage),
			PlantedAt:      formatTime(p.PlantedAt),
			MatureAt:       formatTime(p.MatureAt),
			RemainingYield: p.RemainingYield,
		})
	}
	sort.Slice(plots, func(i, j int) bool { return plots[i].PlotID < plots[j].PlotID })

	dto := &FarmSnapshotDTO{
		FarmID:           raw.FarmID,
		OwnerUserID:      raw.OwnerID,
		OwnerDisplayName: ownerDisplayName,
		Version:          raw.Version,
		Plots:            plots,
	}

	writeResp(w, http.StatusOK, GetSnapshotResp{Snapshot: dto})
}

func (s *Server) loadOwnerDisplayName(ctx context.Context, ownerUserID int64) (string, error) {
	if s.ownerNames == nil {
		return "", errcode.New(errcode.Internal, "farm owner display-name loader is not configured")
	}
	displayName, err := s.ownerNames.LoadDisplayName(ctx, ownerUserID)
	if err != nil {
		return "", fmt.Errorf("load farm owner display name: %w", err)
	}
	if strings.TrimSpace(displayName) == "" {
		return "", errcode.New(errcode.Internal, "farm owner display name is empty")
	}
	return displayName, nil
}

func writeResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
