// Package taskrpc — Client 侧，由 gatesvr 和 workersvr 使用。
package taskrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client 通过 HTTP/JSON 调用 gamesvr 任务服务。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.TaskServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.TaskServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。baseURL 如 "http://gamesvr:9090"。
func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

// IncrProgress 调用 gamesvr 推进任务进度。
func (c *Client) IncrProgress(ctx context.Context, userID int64, taskKey string, delta int) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.IncrProgress(ctx, &rpcv1.IncrProgressRequest{UserId: userID, TaskKey: taskKey, Delta: int32(delta)})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(IncrProgressReq{UserID: userID, TaskKey: taskKey, Delta: delta})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/task/incr-progress", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http_do incr_progress: %w", err)
	}
	defer resp.Body.Close()

	var out IncrProgressResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_incr_progress_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// ListTasks 调用 gamesvr 获取用户任务列表。
func (c *Client) ListTasks(ctx context.Context, userID int64) ([]TaskProgressDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.ListTasks(ctx, &rpcv1.ListTasksRequest{UserId: userID})
		if err == nil {
			items := make([]TaskProgressDTO, 0, len(out.Tasks))
			for _, item := range out.Tasks {
				items = append(items, TaskProgressDTO{TaskKey: item.TaskKey, Description: item.Description, Progress: int(item.Progress), Target: int(item.Target), Status: item.Status, CoinReward: item.CoinReward, UpdatedAt: taskProtoTime(item.UpdatedAt)})
			}
			return items, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(ListTasksReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/task/list", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_do list_tasks: %w", err)
	}
	defer resp.Body.Close()

	var out ListTasksResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_list_tasks_resp: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.Tasks, nil
}

// ClaimReward 调用 gamesvr 领取任务奖励。
func (c *Client) ClaimReward(ctx context.Context, userID int64, taskKey string) (coinReward int64, err error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, grpcErr := c.grpc.ClaimReward(ctx, &rpcv1.ClaimRewardRequest{UserId: userID, TaskKey: taskKey})
		if grpcErr == nil {
			return out.CoinReward, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(grpcErr) {
			return 0, rpcgrpc.FromError(grpcErr)
		}
	}
	body, _ := json.Marshal(ClaimRewardReq{UserID: userID, TaskKey: taskKey})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/task/claim", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http_do claim_reward: %w", err)
	}
	defer resp.Body.Close()

	var out ClaimRewardResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode_claim_reward_resp: %w", err)
	}
	if out.Err != nil {
		return 0, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return out.CoinReward, nil
}

func taskProtoTime(t *timestamppb.Timestamp) time.Time {
	if t == nil || !t.IsValid() {
		return time.Time{}
	}
	return t.AsTime()
}
