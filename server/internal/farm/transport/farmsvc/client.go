// Package farmsvc — Client 侧，由 gatesvr 使用。
// 通过 HTTP/JSON 将农场命令提交到 farmsvr Actor。
package farmsvc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/transport/rpcconvert"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
)

// Client 通过 HTTP/JSON 向 farmsvr 提交农场命令。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.FarmCommandServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.FarmCommandServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。baseURL 是 farmsvr HTTPAddr，例如 "http://farmsvr:8080"。
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// SubmitCommand 将 domain.Command 投递到 farmsvr，等待 Actor 处理完毕返回结果。
func (c *Client) SubmitCommand(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.SubmitCommand(ctx, &rpcv1.SubmitCommandRequest{Command: rpcconvert.CommandToProto(cmd)})
		if err == nil {
			return rpcconvert.ResultFromProto(out.Result), nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return application.CommitResult{}, rpcgrpc.FromError(err)
		}
	}
	body, err := json.Marshal(SubmitCmdReq{Command: cmd})
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("marshal_submit_req: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/farm/submit", bytes.NewReader(body))
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("new_request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return application.CommitResult{}, fmt.Errorf("http_do submit farm_id=%d: %w", cmd.FarmID, err)
	}
	defer resp.Body.Close()

	var out SubmitCmdResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return application.CommitResult{}, fmt.Errorf("decode_submit_resp: %w", err)
	}
	if out.Err != nil {
		return application.CommitResult{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	if out.Result == nil {
		return application.CommitResult{}, errcode.New(errcode.Internal, "farmsvr: empty submit result")
	}
	return *out.Result, nil
}
