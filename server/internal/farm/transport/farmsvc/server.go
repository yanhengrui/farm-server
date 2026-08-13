// Package farmsvc — Server 侧，由 farmsvr 使用。
// 将 CommandSubmitter（actor.Runtime）暴露为 HTTP/JSON，供 gatesvr 通过 Client 调用。
package farmsvc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// CommandSubmitter 由 actor.Runtime 实现：接收命令并串行执行。
type CommandSubmitter interface {
	Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error)
}

// Server 将 CommandSubmitter 暴露为 HTTP/JSON；注册在 farmsvr 的 HTTPAddr。
type Server struct {
	submitter CommandSubmitter
}

// NewServer 构造 Server。
func NewServer(submitter CommandSubmitter) *Server {
	return &Server{submitter: submitter}
}

// RegisterRoutes 注册 RPC 端点。
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/farm/submit", s.handleSubmit)
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SubmitCmdReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeResp(w, http.StatusBadRequest, SubmitCmdResp{
			Err: &RPCErr{Code: string(errcode.CommonInvalidArgument), Message: "decode: " + err.Error()},
		})
		return
	}
	result, err := s.submitter.Submit(r.Context(), req.Command)
	if err != nil {
		rpcErr := toRPCErr(err)
		writeResp(w, errcode.HTTPStatus(errcode.Code(rpcErr.Code)), SubmitCmdResp{Err: rpcErr})
		return
	}
	writeResp(w, http.StatusOK, SubmitCmdResp{Result: &result})
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
