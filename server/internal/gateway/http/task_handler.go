// Package http — 任务系统 HTTP 处理器。
// GET  /api/v1/task/list：获取当前用户任务列表（需登录）。
// POST /api/v1/task/claim：领取任务奖励（需登录）。
package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/session"
)

// TaskClient 是 gatesvr 调用 gamesvr 任务服务的最小接口。
type TaskClient interface {
	ListTasks(ctx context.Context, userID int64) ([]taskrpc.TaskProgressDTO, error)
	ClaimReward(ctx context.Context, userID int64, taskKey string) (coinReward int64, err error)
}

// TaskHandler 处理任务相关 HTTP 请求。
type TaskHandler struct {
	tokenSecret []byte
	task        TaskClient
	readCache   *ReadCache
}

// NewTaskHandler 构造 TaskHandler。
func NewTaskHandler(tokenSecret []byte, task TaskClient) *TaskHandler {
	return &TaskHandler{tokenSecret: tokenSecret, task: task}
}

func (h *TaskHandler) WithReadCache(cache *ReadCache) *TaskHandler {
	h.readCache = cache
	return h
}

// RegisterRoutes 注册 /api/v1/task/* 端点。
func (h *TaskHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/task/list", h.handleList)
	mux.HandleFunc("/api/v1/task/claim", h.handleClaim)
}

type taskListResp struct {
	Tasks []taskrpc.TaskProgressDTO `json:"tasks"`
}

func (h *TaskHandler) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	tasks, err := h.task.ListTasks(r.Context(), userID)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if tasks == nil {
		tasks = []taskrpc.TaskProgressDTO{}
	}
	writeJSON(w, http.StatusOK, taskListResp{Tasks: tasks})
}

type claimTaskReq struct {
	TaskKey string `json:"task_key"`
}

type claimTaskResp struct {
	CoinReward int64 `json:"coin_reward"`
}

func (h *TaskHandler) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID, ok := h.authUser(w, r)
	if !ok {
		return
	}
	var req claimTaskReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TaskKey == "" {
		writeError(w, http.StatusBadRequest, errcode.CommonInvalidArgument, "task_key required")
		return
	}
	coin, err := h.task.ClaimReward(r.Context(), userID, req.TaskKey)
	if err != nil {
		writeErrFromError(w, err)
		return
	}
	if h.readCache != nil {
		h.readCache.Invalidate(r.Context(), 0, userID)
	}
	writeJSON(w, http.StatusOK, claimTaskResp{CoinReward: coin})
}

func (h *TaskHandler) authUser(w http.ResponseWriter, r *http.Request) (int64, bool) {
	token := bearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, errcode.AuthUnauthorized, "token required")
		return 0, false
	}
	userID, err := session.Parse(token, h.tokenSecret)
	if err != nil {
		writeErrFromError(w, err)
		return 0, false
	}
	return userID, true
}
