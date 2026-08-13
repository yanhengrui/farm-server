// Package catalogrpc provides the internal catalog HTTP/gRPC transport.
package catalogrpc

import "time"

type UnlockDTO struct {
	CatalogKey string    `json:"catalog_key"`
	UnlockedAt time.Time `json:"unlocked_at"`
}

type ListReq struct {
	UserID int64 `json:"user_id"`
}

type ListResp struct {
	Unlocks []UnlockDTO `json:"unlocks"`
	Err     *RPCErr     `json:"error,omitempty"`
}

type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
