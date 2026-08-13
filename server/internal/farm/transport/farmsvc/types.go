// Package farmsvc 提供 gatesvr → farmsvr 的 HTTP/JSON RPC 传输层。
// gatesvr 通过 Client 将农场命令提交到 farmsvr Actor；
// farmsvr 通过 Server 接收并投递到 actor.Runtime。
package farmsvc

import (
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

// SubmitCmdReq 是 /farm/submit 请求体。
type SubmitCmdReq struct {
	Command domain.Command `json:"command"`
}

// SubmitCmdResp 是 /farm/submit 响应体。
type SubmitCmdResp struct {
	Result *application.CommitResult `json:"result,omitempty"`
	Err    *RPCErr                   `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误；Code 对应 errcode.Code。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
