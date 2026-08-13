// Package ws — WS 帧层应用错误码，独立于 HTTP/gRPC 错误码。
// 客户端依据此码决定断线后动作（重登录/刷新token/全量同步等）。
// 编号 1001-1007 映射到 WebSocket close code 4001-4007（4000-4999 为应用私有范围）。
package ws

// WSCode 是 WS 帧层应用错误码（1001-1007）。
type WSCode int

const (
	WSCodeAuthFailed    WSCode = 1001 // 鉴权失败 → 重新登录
	WSCodeTokenExpired  WSCode = 1002 // token 过期/无效 → 刷新 token 后重连
	WSCodeParamInvalid  WSCode = 1003 // 参数非法（farm_id/last_seq）→ HTTP 全量同步
	WSCodeCursorExpired WSCode = 1004 // 游标过期 → GET snapshot 后续流
	WSCodeKicked        WSCode = 1005 // 重复连接被踢旧 → 提示在其他设备登录
	WSCodeRateLimited   WSCode = 1006 // 限流触发 → 退避重试
	WSCodeServerError   WSCode = 1007 // 服务内部错误 → 退避重连
)

// CloseCode 将 WSCode 映射为 WebSocket 协议 close code（4001-4007）。
func (c WSCode) CloseCode() int { return 4000 + int(c) - 1000 }
