// Package rpcgrpc contains transport-only gRPC policy shared by internal services.
package rpcgrpc

import (
	"context"
	"errors"
	"strings"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/admission"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/observability"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// RegisterHealth exposes the standard gRPC health protocol. Call SetServingStatus
// during shutdown before stopping the server so clients stop sending new work.
func RegisterHealth(reg grpc.ServiceRegistrar) *health.Server {
	h := health.NewServer()
	healthv1.RegisterHealthServer(reg, h)
	h.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	return h
}

type Mode string

const (
	ModeHTTP         Mode = "http"
	ModeGRPC         Mode = "grpc"
	ModeGRPCFallback Mode = "grpc_fallback"
)

func ParseMode(v string) Mode {
	switch Mode(strings.ToLower(v)) {
	case ModeGRPC, ModeGRPCFallback:
		return Mode(strings.ToLower(v))
	default:
		return ModeHTTP
	}
}

type ClientConfig struct {
	DefaultTimeout    time.Duration
	KeepaliveTime     time.Duration
	KeepaliveTimeout  time.Duration
	MaxMessageBytes   int
	UnaryInterceptors []grpc.UnaryClientInterceptor
	Resolver          resolver.Builder
	LoadBalancing     string
}

func Dial(target string, cfg ClientConfig) (*grpc.ClientConn, error) {
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 3 * time.Second
	}
	if cfg.KeepaliveTime <= 0 {
		cfg.KeepaliveTime = 30 * time.Second
	}
	if cfg.KeepaliveTimeout <= 0 {
		cfg.KeepaliveTimeout = 10 * time.Second
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 1 << 20
	}
	interceptors := append([]grpc.UnaryClientInterceptor{ClientTraceInterceptor(cfg.DefaultTimeout)}, cfg.UnaryInterceptors...)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxMessageBytes), grpc.MaxCallSendMsgSize(cfg.MaxMessageBytes)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: cfg.KeepaliveTime, Timeout: cfg.KeepaliveTimeout, PermitWithoutStream: true}),
		grpc.WithChainUnaryInterceptor(interceptors...),
	}
	if cfg.Resolver != nil {
		opts = append(opts, grpc.WithResolvers(cfg.Resolver))
	}
	if cfg.LoadBalancing != "" {
		opts = append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+cfg.LoadBalancing+`":{}}]}`))
	}
	return grpc.NewClient(target, opts...)
}

func ServerOptions(maxMessageBytes, maxStreams int) []grpc.ServerOption {
	if maxMessageBytes <= 0 {
		maxMessageBytes = 1 << 20
	}
	if maxStreams <= 0 {
		maxStreams = 128
	}
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxConcurrentStreams(uint32(maxStreams)),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 5 * time.Minute, Time: 2 * time.Hour, Timeout: 20 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 15 * time.Second, PermitWithoutStream: true}),
	}
}

func ClientTraceInterceptor(defaultTimeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok && defaultTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
		}
		ctx = metadata.AppendToOutgoingContext(ctx, observability.TraceparentHeader, observability.Traceparent(ctx))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func ServerTraceInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(observability.TraceparentHeader); len(values) > 0 {
				ctx = observability.ContextFromTraceparent(ctx, values[0])
			}
		}
		return handler(ctx, req)
	}
}

func AdmissionInterceptor(gate *admission.Gate, wait, retryAfter time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !gate.Acquire(ctx, wait) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ToError(errcode.NewRetryReason(errcode.ResourceExhausted, "capacity temporarily exhausted", gate.Reason(), retryAfter))
		}
		defer gate.Release()
		return handler(ctx, req)
	}
}

func ToError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, context.Canceled.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
	}
	var ec *errcode.Error
	if !errors.As(err, &ec) {
		ec = errcode.New(errcode.Internal, err.Error())
	}
	st := status.New(grpcCode(ec.Code), ec.Message)
	with, detailErr := st.WithDetails(&rpcv1.ErrorDetail{Code: string(ec.Code), Message: ec.Message, RetryAfterMs: errcode.RetryAfter(err).Milliseconds(), Reason: ec.Reason})
	if detailErr == nil {
		st = with
	}
	return st.Err()
}

func FromError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, detail := range st.Details() {
		if d, ok := detail.(*rpcv1.ErrorDetail); ok {
			return errcode.FromRemoteReason(d.Code, d.Message, d.Reason, d.RetryAfterMs)
		}
	}
	if st.Code() == codes.Canceled {
		return context.Canceled
	}
	if st.Code() == codes.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	return errcode.New(errcode.Internal, st.Message())
}

func CanFallback(err error) bool {
	return status.Code(err) == codes.Unimplemented
}

func grpcCode(code errcode.Code) codes.Code {
	switch errcode.GRPCCode(code) {
	case "INVALID_ARGUMENT":
		return codes.InvalidArgument
	case "UNAUTHENTICATED":
		return codes.Unauthenticated
	case "PERMISSION_DENIED":
		return codes.PermissionDenied
	case "NOT_FOUND":
		return codes.NotFound
	case "ALREADY_EXISTS":
		return codes.AlreadyExists
	case "FAILED_PRECONDITION":
		return codes.FailedPrecondition
	case "RESOURCE_EXHAUSTED":
		return codes.ResourceExhausted
	case "UNAVAILABLE":
		return codes.Unavailable
	case "OUT_OF_RANGE":
		return codes.OutOfRange
	default:
		return codes.Internal
	}
}
