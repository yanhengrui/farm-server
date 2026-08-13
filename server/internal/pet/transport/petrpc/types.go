// Package petrpc 提供 gatesvr → gamesvr 的宠物服务 HTTP/JSON RPC 传输层。
package petrpc

// BuyPetReq 是 /rpc/pet/buy 请求体。
type BuyPetReq struct {
	UserID int64 `json:"user_id"`
}

// BuyPetResp 是 /rpc/pet/buy 响应体。
type BuyPetResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// HasPetReq 是 /rpc/pet/has 请求体。
type HasPetReq struct {
	UserID int64 `json:"user_id"`
}

// HasPetResp 是 /rpc/pet/has 响应体。
type HasPetResp struct {
	HasPet             bool    `json:"has_pet"`
	AutoHarvestEnabled bool    `json:"auto_harvest_enabled"`
	Err                *RPCErr `json:"error,omitempty"`
}

type StatusDTO struct {
	HasPet             bool `json:"has_pet"`
	AutoHarvestEnabled bool `json:"auto_harvest_enabled"`
}

type SetAutoHarvestReq struct {
	UserID  int64 `json:"user_id"`
	Enabled bool  `json:"enabled"`
}

type SetAutoHarvestResp struct {
	Err *RPCErr `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
