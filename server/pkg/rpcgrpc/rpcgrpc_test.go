package rpcgrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/photon/farm-server/server/pkg/errcode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorRoundTrip(t *testing.T) {
	err := NewRoundTripError()
	got := FromError(ToError(err))
	var ec *errcode.Error
	if !errors.As(got, &ec) || ec.Code != errcode.ResourceExhausted {
		t.Fatalf("error=%v", got)
	}
	if retry := errcode.RetryAfter(got); retry != 75*time.Millisecond {
		t.Fatalf("retry=%s", retry)
	}
	if reason := errcode.Reason(got); reason != "gamesvr_global" {
		t.Fatalf("reason=%q", reason)
	}
}

func NewRoundTripError() error {
	return errcode.NewRetryReason(errcode.ResourceExhausted, "busy", "gamesvr_global", 75*time.Millisecond)
}

func TestFallbackOnlyForUnimplemented(t *testing.T) {
	if !CanFallback(status.Error(codes.Unimplemented, "old server")) {
		t.Fatal("unimplemented should permit HTTP compatibility fallback")
	}
	if CanFallback(status.Error(codes.Unavailable, "ambiguous write outcome")) {
		t.Fatal("unavailable must not automatically replay a possibly accepted write over HTTP")
	}
}

func TestContextErrorsKeepCanonicalCodes(t *testing.T) {
	if got := status.Code(ToError(context.Canceled)); got != codes.Canceled {
		t.Fatalf("canceled mapped to %s", got)
	}
	if got := status.Code(ToError(context.DeadlineExceeded)); got != codes.DeadlineExceeded {
		t.Fatalf("deadline mapped to %s", got)
	}
	if !errors.Is(FromError(status.Error(codes.Canceled, "remote canceled")), context.Canceled) {
		t.Fatal("remote canceled did not restore context.Canceled")
	}
	if !errors.Is(FromError(status.Error(codes.DeadlineExceeded, "remote deadline")), context.DeadlineExceeded) {
		t.Fatal("remote deadline did not restore context.DeadlineExceeded")
	}
}
