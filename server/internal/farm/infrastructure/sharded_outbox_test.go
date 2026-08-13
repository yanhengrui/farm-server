package infrastructure

import (
	"context"
	"errors"
	"testing"
)

type testOutboxScanner struct {
	n   int
	err error
}

func (s testOutboxScanner) ScanAndPublish(context.Context) (int, error) { return s.n, s.err }

func TestShardedOutboxScansHealthyShardAfterFailure(t *testing.T) {
	relay, err := NewShardedOutboxScanner(map[string]OutboxScanner{
		"shard-0": testOutboxScanner{n: 2, err: errors.New("db unavailable")},
		"shard-1": testOutboxScanner{n: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := relay.ScanAndPublish(context.Background())
	if n != 5 {
		t.Fatalf("published=%d, want 5", n)
	}
	if err == nil {
		t.Fatal("expected isolated shard error")
	}
}
