// Package errcode 定义跨服务稳定错误码与领域错误类型。
// 领域层返回 *Error；transport 层统一映射为 HTTP/gRPC/WS 错误，不泄露内部堆栈。
// 分段参考 04-错误码规范.md：COMMON/AUTH/ECONOMY/FARM/SOCIAL/TASK/PET/MAIL/ROUTING/INTERNAL。
package errcode

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Code 是稳定错误码字符串，禁止依赖错误消息文本做分支判断。
type Code string

// ── COMMON ────────────────────────────────────────────────────────────────────
const (
	CommonInvalidArgument Code = "COMMON_INVALID_ARGUMENT"
	CommonRateLimited     Code = "COMMON_RATE_LIMITED"
	CommonServerSeqGap    Code = "COMMON_SERVER_SEQ_GAP"
	CommonInvalidMetadata Code = "COMMON_INVALID_METADATA"
	ResourceExhausted     Code = "COMMON_RESOURCE_EXHAUSTED"
)

// Capacity reasons refine RESOURCE_EXHAUSTED without creating endpoint- or
// entity-specific error codes.
const (
	CapacityReasonGatewayGlobal     = "gateway_global"
	CapacityReasonGamesvrGlobal     = "gamesvr_global"
	CapacityReasonEconomyGate       = "economy_gate"
	CapacityReasonDBPoolWait        = "db_pool_wait"
	CapacityReasonDownstreamTimeout = "downstream_timeout"
	CapacityReasonLoadgenQueue      = "loadgen_queue"
)

// ── AUTH ──────────────────────────────────────────────────────────────────────
const (
	AuthUnauthorized   Code = "AUTH_UNAUTHORIZED"
	AuthTokenExpired   Code = "AUTH_TOKEN_EXPIRED"
	AuthForbidden      Code = "AUTH_FORBIDDEN"
	AuthIdentityExists Code = "AUTH_IDENTITY_EXISTS"
)

// ── ECONOMY ───────────────────────────────────────────────────────────────────
const (
	EconomyInsufficient Code = "ECONOMY_INSUFFICIENT_BALANCE"
	EconomyDuplicate    Code = "ECONOMY_DUPLICATE_COMMIT"
	EconomyItemNotFound Code = "ECONOMY_ITEM_NOT_FOUND"
)

// ── FARM ──────────────────────────────────────────────────────────────────────
const (
	FarmVersionConflict Code = "FARM_VERSION_CONFLICT"
	FarmPlotState       Code = "FARM_PLOT_STATE_INVALID"
	FarmRoomFull        Code = "FARM_ROOM_FULL"
	FarmPlotNotMature   Code = "FARM_PLOT_NOT_MATURE"
	FarmCropNotFound    Code = "FARM_CROP_NOT_FOUND"
	FarmActorMigrating  Code = "FARM_ACTOR_MIGRATING"
	FarmNotFound        Code = "FARM_NOT_FOUND"
)

// ── SOCIAL ────────────────────────────────────────────────────────────────────
const (
	SocialAlreadyFriend   Code = "SOCIAL_ALREADY_FRIEND"
	SocialInviteExpired   Code = "SOCIAL_INVITE_EXPIRED"
	SocialInviteExhausted Code = "SOCIAL_INVITE_EXHAUSTED"
	SocialSelfInvite      Code = "SOCIAL_SELF_INVITE"
	SocialNotFriend       Code = "SOCIAL_NOT_FRIEND"     // 操作需要好友关系但不满足
	SocialNotFarmOwner    Code = "SOCIAL_NOT_FARM_OWNER" // 操作要求农场主人但执行者不是
)

// ── TASK ──────────────────────────────────────────────────────────────────────
const (
	TaskNotCompleted   Code = "TASK_NOT_COMPLETED"
	TaskAlreadyClaimed Code = "TASK_ALREADY_CLAIMED"
)

// ── PET ───────────────────────────────────────────────────────────────────────
const (
	PetNotOwned        Code = "PET_NOT_OWNED"
	PetAlreadyEquipped Code = "PET_ALREADY_EQUIPPED"
	PetAutoDisabled    Code = "PET_AUTO_HARVEST_DISABLED"
)

// ── MAIL ──────────────────────────────────────────────────────────────────────
const (
	MailNotFound       Code = "MAIL_NOT_FOUND"
	MailAlreadyClaimed Code = "MAIL_ALREADY_CLAIMED"
	MailExpired        Code = "MAIL_EXPIRED"
)

// ── ROUTING ───────────────────────────────────────────────────────────────────
const (
	RoutingEpochStale    Code = "ROUTING_EPOCH_STALE"
	RoutingOwnerNotFound Code = "ROUTING_OWNER_NOT_FOUND"
	RoutingFenced        Code = "ROUTING_FENCED"
)

// ── INTERNAL ──────────────────────────────────────────────────────────────────
const (
	Internal Code = "INTERNAL_ERROR"
)

// ─────────────────────────────────────────────────────────────────────────────

// Error 是领域可预期错误，携带稳定 code 与可选 cause。
type Error struct {
	Code    Code
	Message string
	Reason  string
	cause   error
}

// New 构造领域错误。
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// NewReason attaches a stable, low-cardinality diagnostic reason. It is safe
// to transport to clients and must never contain entity IDs or raw SQL text.
func NewReason(code Code, message, reason string) *Error {
	return &Error{Code: code, Message: message, Reason: reason}
}

// Wrap 用稳定 code 包装底层技术错误，保留 cause 以便 errors.Is/As。
func Wrap(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap 支持 errors.Is/As 链式判断。
func (e *Error) Unwrap() error { return e.cause }

type RetryError struct {
	Err        *Error
	RetryAfter time.Duration
}

func (e *RetryError) Error() string                     { return e.Err.Error() }
func (e *RetryError) Unwrap() error                     { return e.Err }
func (e *RetryError) RetryAfterDuration() time.Duration { return e.RetryAfter }
func NewRetry(code Code, message string, after time.Duration) error {
	return &RetryError{Err: New(code, message), RetryAfter: after}
}
func NewRetryReason(code Code, message, reason string, after time.Duration) error {
	return &RetryError{Err: NewReason(code, message, reason), RetryAfter: after}
}
func FromRemote(code, message string, retryAfterMs int64) error {
	return FromRemoteReason(code, message, "", retryAfterMs)
}
func FromRemoteReason(code, message, reason string, retryAfterMs int64) error {
	if retryAfterMs > 0 {
		return NewRetryReason(Code(code), message, reason, time.Duration(retryAfterMs)*time.Millisecond)
	}
	return NewReason(Code(code), message, reason)
}
func RetryAfter(err error) time.Duration {
	var p interface{ RetryAfterDuration() time.Duration }
	if errors.As(err, &p) {
		return p.RetryAfterDuration()
	}
	return 0
}

func Reason(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// ── Transport 映射 ────────────────────────────────────────────────────────────

// HTTPStatus 将领域错误码映射为 HTTP 状态码。
func HTTPStatus(code Code) int {
	switch code {
	case CommonInvalidArgument, FarmPlotState, FarmPlotNotMature,
		FarmCropNotFound, EconomyItemNotFound, SocialSelfInvite,
		CommonInvalidMetadata:
		return http.StatusBadRequest
	case AuthUnauthorized, AuthTokenExpired:
		return http.StatusUnauthorized
	case AuthForbidden:
		return http.StatusForbidden
	case FarmNotFound, MailNotFound:
		return http.StatusNotFound
	case AuthIdentityExists, FarmVersionConflict, EconomyDuplicate,
		SocialAlreadyFriend, TaskAlreadyClaimed,
		MailAlreadyClaimed, PetAlreadyEquipped:
		return http.StatusConflict
	case EconomyInsufficient, FarmRoomFull,
		TaskNotCompleted, PetNotOwned, PetAutoDisabled, MailExpired,
		SocialInviteExpired, SocialInviteExhausted,
		SocialNotFriend, SocialNotFarmOwner:
		return http.StatusUnprocessableEntity
	case CommonRateLimited:
		return http.StatusTooManyRequests
	case ResourceExhausted:
		return http.StatusServiceUnavailable
	case RoutingEpochStale, RoutingOwnerNotFound, RoutingFenced, FarmActorMigrating:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// GRPCCode 将领域错误码映射为 gRPC 状态码字符串（不引入 google.golang.org/grpc 依赖）。
func GRPCCode(code Code) string {
	switch code {
	case CommonInvalidArgument, FarmPlotState, FarmPlotNotMature,
		FarmCropNotFound, EconomyItemNotFound, SocialSelfInvite,
		CommonInvalidMetadata:
		return "INVALID_ARGUMENT"
	case AuthUnauthorized, AuthTokenExpired:
		return "UNAUTHENTICATED"
	case AuthForbidden:
		return "PERMISSION_DENIED"
	case FarmNotFound, MailNotFound:
		return "NOT_FOUND"
	case AuthIdentityExists, FarmVersionConflict, EconomyDuplicate,
		SocialAlreadyFriend, TaskAlreadyClaimed,
		MailAlreadyClaimed, PetAlreadyEquipped:
		return "ALREADY_EXISTS"
	case EconomyInsufficient, FarmRoomFull,
		TaskNotCompleted, PetNotOwned, PetAutoDisabled, MailExpired,
		SocialInviteExpired, SocialInviteExhausted,
		SocialNotFriend, SocialNotFarmOwner:
		return "FAILED_PRECONDITION"
	case CommonRateLimited, ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case RoutingEpochStale, RoutingOwnerNotFound, RoutingFenced, FarmActorMigrating:
		return "UNAVAILABLE"
	case CommonServerSeqGap:
		return "OUT_OF_RANGE"
	default:
		return "INTERNAL"
	}
}

// Retryable 返回客户端是否可以安全重试（写操作带同一 cmd_id）。
func Retryable(code Code) bool {
	switch code {
	case CommonRateLimited, ResourceExhausted,
		RoutingEpochStale, RoutingOwnerNotFound, RoutingFenced,
		FarmActorMigrating, Internal:
		return true
	default:
		return false
	}
}
