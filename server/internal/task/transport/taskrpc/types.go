// Package taskrpc 提供 gatesvr/workersvr → gamesvr 的任务服务 HTTP/JSON RPC 传输层。
package taskrpc

import "time"

// TaskProgressDTO 任务进度 DTO。
type TaskProgressDTO struct {
	TaskKey     string    `json:"task_key"`
	Description string    `json:"description"`
	Progress    int       `json:"progress"`
	Target      int       `json:"target"`
	Status      string    `json:"status"`
	CoinReward  int64     `json:"coin_reward"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// IncrProgressReq 是 /rpc/task/incr-progress 请求体。
type IncrProgressReq struct {
	UserID  int64  `json:"user_id"`
	TaskKey string `json:"task_key"`
	Delta   int    `json:"delta"`
}

// IncrProgressResp 是 /rpc/task/incr-progress 响应体。
type IncrProgressResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// ListTasksReq 是 /rpc/task/list 请求体。
type ListTasksReq struct {
	UserID int64 `json:"user_id"`
}

// ListTasksResp 是 /rpc/task/list 响应体。
type ListTasksResp struct {
	Tasks []TaskProgressDTO `json:"tasks"`
	Err   *RPCErr           `json:"error,omitempty"`
}

// ClaimRewardReq 是 /rpc/task/claim 请求体。
type ClaimRewardReq struct {
	UserID  int64  `json:"user_id"`
	TaskKey string `json:"task_key"`
}

// ClaimRewardResp 是 /rpc/task/claim 响应体。
type ClaimRewardResp struct {
	CoinReward int64   `json:"coin_reward,omitempty"`
	Err        *RPCErr `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
