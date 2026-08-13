package mysqlretry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestClassificationUnwrapsMySQLError(t *testing.T) {
	deadlock := fmt.Errorf("commit: %w", &mysql.MySQLError{Number: 1213})
	timeout := fmt.Errorf("update: %w", &mysql.MySQLError{Number: 1205})
	duplicate := fmt.Errorf("insert: %w", &mysql.MySQLError{Number: 1062})
	if !IsTransient(deadlock) || !IsTransient(timeout) {
		t.Fatal("deadlock and lock wait timeout must be transient")
	}
	if IsTransient(duplicate) || !IsDuplicateKey(duplicate) {
		t.Fatal("duplicate key must be classified separately")
	}
}

func TestValueRetriesTransientFailureWithinBound(t *testing.T) {
	attempts := 0
	got, err := Value(context.Background(), IsTransient, func() (int, error) {
		attempts++
		if attempts < MaxAttempts {
			return 0, &mysql.MySQLError{Number: 1213}
		}
		return 42, nil
	})
	if err != nil || got != 42 || attempts != MaxAttempts {
		t.Fatalf("got=%d attempts=%d err=%v", got, attempts, err)
	}
}

func TestValueDoesNotRetryBusinessFailure(t *testing.T) {
	want := errors.New("business failure")
	attempts := 0
	_, err := Value(context.Background(), IsTransient, func() (int, error) {
		attempts++
		return 0, want
	})
	if !errors.Is(err, want) || attempts != 1 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestValueStopsAtMaxAttempts(t *testing.T) {
	attempts := 0
	_, err := Value(context.Background(), IsTransient, func() (int, error) {
		attempts++
		return 0, &mysql.MySQLError{Number: 1205}
	})
	if err == nil || attempts != MaxAttempts {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}
