// Package taskrpc — Server 侧，由 gamesvr 使用。
package taskrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	taskdomain "github.com/photon/farm-server/server/internal/task/domain"
	taskinfra "github.com/photon/farm-server/server/internal/task/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// TaskSvc 是 Server 持有的任务服务接口。
type TaskSvc interface {
	IncrProgress(ctx context.Context, userID int64, taskKey string, delta int) error
	ListTasks(ctx context.Context, userID int64) ([]taskdomain.TaskProgress, error)
	ClaimReward(ctx context.Context, userID int64, taskKey string) (coinReward int64, err error)
}

// Server 将任务 RPC 暴露为 HTTP/JSON，注册在 gamesvr 内部端口。
type Server struct {
	svc TaskSvc
}

// NewServer 构造 Server。
func NewServer(svc TaskSvc) *Server { return &Server{svc: svc} }

// RegisterRoutes 注册任务 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/rpc/task/incr-progress", s.handleIncrProgress)
	mux.HandleFunc("/rpc/task/list", s.handleList)
	mux.HandleFunc("/rpc/task/claim", s.handleClaim)
}

func (s *Server) handleIncrProgress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req IncrProgressReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 || req.TaskKey == "" {
		writeResp(w, http.StatusBadRequest, IncrProgressResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id and task_key required",
		}})
		return
	}
	if req.Delta <= 0 {
		req.Delta = 1
	}
	if err := s.svc.IncrProgress(r.Context(), req.UserID, req.TaskKey, req.Delta); err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), IncrProgressResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, IncrProgressResp{})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ListTasksReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 {
		writeResp(w, http.StatusBadRequest, ListTasksResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id required",
		}})
		return
	}
	tasks, err := s.svc.ListTasks(r.Context(), req.UserID)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ListTasksResp{Err: rpcErr})
		return
	}
	dtos := make([]TaskProgressDTO, len(tasks))
	for i, t := range tasks {
		cfg, _ := taskinfra.GetTaskConfig(t.TaskKey)
		dtos[i] = TaskProgressDTO{
			TaskKey:     t.TaskKey,
			Description: cfg.Description,
			Progress:    t.Progress,
			Target:      cfg.Target,
			Status:      string(t.Status),
			CoinReward:  cfg.CoinReward,
			UpdatedAt:   t.UpdatedAt,
		}
	}
	writeResp(w, http.StatusOK, ListTasksResp{Tasks: dtos})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ClaimRewardReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == 0 || req.TaskKey == "" {
		writeResp(w, http.StatusBadRequest, ClaimRewardResp{Err: &RPCErr{
			Code: string(errcode.CommonInvalidArgument), Message: "user_id and task_key required",
		}})
		return
	}
	coinReward, err := s.svc.ClaimReward(r.Context(), req.UserID, req.TaskKey)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), ClaimRewardResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, ClaimRewardResp{CoinReward: coinReward})
}

func toRPCErr(err error) *RPCErr {
	var e *errcode.Error
	if errors.As(err, &e) {
		return &RPCErr{Code: string(e.Code), Message: e.Message, Reason: e.Reason, RetryAfterMs: errcode.RetryAfter(err).Milliseconds()}
	}
	return &RPCErr{Code: string(errcode.Internal), Message: err.Error()}
}

func writeResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
