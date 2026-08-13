// Package mysqlretry centralizes bounded retries for whole MySQL transactions.
package mysqlretry

import (
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

const MaxAttempts = 3

// IsTransient reports MySQL failures for which retrying the entire transaction
// is safe: deadlock (1213) and lock wait timeout (1205).
func IsTransient(err error) bool {
	return isNumber(err, 1213) || isNumber(err, 1205)
}

// IsDuplicateKey reports MySQL duplicate-key error 1062. Callers should only
// retry it when the conflicting unique key is their durable idempotency key.
func IsDuplicateKey(err error) bool {
	return isNumber(err, 1062)
}

func isNumber(err error, number uint16) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == number
}

// Value retries fn at most MaxAttempts times. fn must open and finish one whole
// transaction per call; retrying only a statement would violate lock ordering.
func Value[T any](ctx context.Context, shouldRetry func(error) bool, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		value, err := fn()
		if err == nil {
			return value, nil
		}
		if attempt == MaxAttempts || !shouldRetry(err) {
			return zero, err
		}
		if err := wait(ctx, time.Duration(attempt)*5*time.Millisecond); err != nil {
			return zero, err
		}
	}
	return zero, context.Canceled // unreachable
}

// Do is Value for operations without a return value.
func Do(ctx context.Context, shouldRetry func(error) bool, fn func() error) error {
	_, err := Value(ctx, shouldRetry, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
