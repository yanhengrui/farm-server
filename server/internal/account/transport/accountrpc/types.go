// Package accountrpc 提供 gatesvr → gamesvr 的账号服务 HTTP/JSON RPC 传输层。
package accountrpc

// GuestLoginReq 是 /rpc/account/guest-login 请求体。
type GuestLoginReq struct {
	DeviceID    string `json:"device_id"`
	DisplayName string `json:"display_name"`
}

// GuestLoginResp 是 /rpc/account/guest-login 响应体。
type GuestLoginResp struct {
	UserID       int64   `json:"user_id,omitempty"`
	FarmID       int64   `json:"farm_id,omitempty"`
	AccessToken  string  `json:"access_token,omitempty"`
	RefreshToken string  `json:"refresh_token,omitempty"`
	SessionID    string  `json:"session_id,omitempty"`
	ExpiresIn    int     `json:"expires_in,omitempty"`
	DisplayName  string  `json:"display_name,omitempty"`
	Err          *RPCErr `json:"error,omitempty"`
}

type RegisterReq struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

type PasswordLoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// RefreshReq 是 /rpc/account/refresh 请求体。
type RefreshReq struct {
	SessionID    string `json:"session_id"`
	RefreshToken string `json:"refresh_token"`
}

// RefreshResp 是 /rpc/account/refresh 响应体。
type RefreshResp struct {
	AccessToken  string  `json:"access_token,omitempty"`
	RefreshToken string  `json:"refresh_token,omitempty"`
	ExpiresIn    int     `json:"expires_in,omitempty"`
	Err          *RPCErr `json:"error,omitempty"`
}

type LogoutReq struct {
	SessionID    string `json:"session_id"`
	RefreshToken string `json:"refresh_token"`
}

type LogoutResp struct {
	OK  bool    `json:"ok"`
	Err *RPCErr `json:"error,omitempty"`
}

// AuthenticateReq 是 /rpc/account/authenticate 请求体。
type AuthenticateReq struct {
	AccessToken string `json:"access_token"`
}

// AuthenticateResp 是 /rpc/account/authenticate 响应体。
type AuthenticateResp struct {
	UserID int64   `json:"user_id,omitempty"`
	FarmID int64   `json:"farm_id,omitempty"`
	Err    *RPCErr `json:"error,omitempty"`
}

// RPCErr 跨服务稳定错误；Code 对应 errcode.Code。
type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
