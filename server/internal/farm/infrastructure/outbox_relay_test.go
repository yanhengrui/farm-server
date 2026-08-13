package infrastructure

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/photon/farm-server/server/pkg/clock"
)

// testLog is a silent logger for unit tests.
var testLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// fixedClock returns a deterministic UTC time.
type fixedClock struct{ t time.Time }

func (c fixedClock) NowUTC() time.Time { return c.t }

var _ clock.Clock = fixedClock{}

var testNow = time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

type batchTestPublisher struct {
	messages []MemMessage
	results  []error
}

func (p *batchTestPublisher) Publish(context.Context, string, string, []byte) error {
	return errors.New("single publish must not be used")
}
func (p *batchTestPublisher) PublishBatch(_ context.Context, messages []MemMessage) []error {
	p.messages = append(p.messages, messages...)
	return append([]error(nil), p.results...)
}
func (p *batchTestPublisher) Close() error { return nil }

// newRelayWithMock creates an OutboxRelay backed by a sqlmock DB and MemPublisher.
func newRelayWithMock(t *testing.T, pub EventPublisher) (*OutboxRelay, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	relay := NewOutboxRelay(db, pub, fixedClock{t: testNow}, testLog, OutboxRelayConfig{
		BatchSize: 10,
		MaxRetry:  3,
		LockTTL:   30 * time.Second,
		Topic:     "test-topic",
	})
	return relay, mock
}

// ── TestOutboxRelay_ScanEmpty ─────────────────────────────────────────────────

// ScanAndPublish on an empty outbox returns 0 with no errors.
func TestOutboxRelay_ScanEmpty(t *testing.T) {
	pub := NewMemPublisher()
	relay, mock := newRelayWithMock(t, pub)

	// reclaimStale
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))

	// claimBatch: begin + select returns zero rows + commit
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").
		WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}))
	mock.ExpectCommit()

	n, err := relay.ScanAndPublish(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if msgs := pub.Messages(); len(msgs) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(msgs))
	}
}

// ── TestOutboxRelay_PublishSuccess ────────────────────────────────────────────

// A PENDING event is claimed, published, and marked PUBLISHED.
func TestOutboxRelay_PublishSuccess(t *testing.T) {
	pub := NewMemPublisher()
	relay, mock := newRelayWithMock(t, pub)

	payload := []byte(`{"event_id":"evt1"}`)

	// reclaimStale
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))

	// claimBatch
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").
		WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(1), "evt1", "farm-42", "farm.planted.v1", payload, 0))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(relay.workerID, testNow.Add(relay.lockTTL), testNow, uint64(1)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// publishOne → mark PUBLISHED
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(testNow, testNow, uint64(1)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	n, err := relay.ScanAndPublish(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 published, got %d", n)
	}
	msgs := pub.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in publisher, got %d", len(msgs))
	}
	if msgs[0].Topic != "test-topic" || msgs[0].PartitionKey != "farm-42" {
		t.Fatalf("unexpected message: %+v", msgs[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestOutboxRelay_BatchPublishMarksSuccessAndRetriesFailure(t *testing.T) {
	pub := &batchTestPublisher{results: []error{nil, errors.New("broker rejected")}}
	relay, mock := newRelayWithMock(t, pub)

	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(11), "evt-11", "farm-11", "farm.planted.v1", []byte(`{"event_id":"evt-11"}`), 0).
			AddRow(uint64(12), "evt-12", "farm-12", "farm.planted.v1", []byte(`{"event_id":"evt-12"}`), 0))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(relay.workerID, testNow.Add(relay.lockTTL), testNow, uint64(11), uint64(12)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(1, "broker rejected", testNow.Add(backoffBase), testNow, uint64(12)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(testNow, testNow, relay.workerID, uint64(11)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	published, err := relay.ScanAndPublish(t.Context())
	if err != nil || published != 1 {
		t.Fatalf("published=%d err=%v", published, err)
	}
	if len(pub.messages) != 2 || pub.messages[0].Topic != "test-topic" || pub.messages[1].PartitionKey != "farm-12" {
		t.Fatalf("messages=%+v", pub.messages)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ── TestOutboxRelay_PublishFailure_Retry ──────────────────────────────────────

// When publish fails and retry_count < maxRetry, the row is requeued to PENDING.
func TestOutboxRelay_PublishFailure_Retry(t *testing.T) {
	pub := NewMemPublisher()
	pub.SetPublishError(errors.New("broker unavailable"))
	relay, mock := newRelayWithMock(t, pub)

	payload := []byte(`{"event_id":"evt2"}`)
	retryCount := 0
	expectedNextCount := retryCount + 1
	expectedAvailableAt := testNow.Add(time.Duration(expectedNextCount) * backoffBase)

	// reclaimStale
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))

	// claimBatch
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").
		WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(2), "evt2", "farm-99", "farm.harvested.v1", payload, retryCount))
	mock.ExpectExec("UPDATE outbox_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// markRetryOrDead → PENDING retry
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(expectedNextCount, "broker unavailable", expectedAvailableAt, testNow, uint64(2)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	n, err := relay.ScanAndPublish(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// publish failed → 0 successfully published
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRoute95OutboxBrokerOutageThenRecovery(t *testing.T) {
	pub := NewMemPublisher()
	pub.SetPublishError(errors.New("broker unavailable"))
	relay, mock := newRelayWithMock(t, pub)
	payload := []byte(`{"event_id":"route95-kafka"}`)

	// First scan: the broker is down, so the claimed row returns to PENDING.
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(95), "route95-kafka", "farm-95", "farm.planted.v1", payload, 0))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(relay.workerID, testNow.Add(relay.lockTTL), testNow, uint64(95)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(1, "broker unavailable", testNow.Add(backoffBase), testNow, uint64(95)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if published, err := relay.ScanAndPublish(t.Context()); err != nil || published != 0 {
		t.Fatalf("outage scan: published=%d err=%v", published, err)
	}

	// Second scan: the same durable row is retried after the broker recovers and
	// is marked PUBLISHED only after the publish call succeeds.
	pub.SetPublishError(nil)
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(95), "route95-kafka", "farm-95", "farm.planted.v1", payload, 1))
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(relay.workerID, testNow.Add(relay.lockTTL), testNow, uint64(95)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE outbox_events").WithArgs(testNow, testNow, uint64(95)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if published, err := relay.ScanAndPublish(t.Context()); err != nil || published != 1 {
		t.Fatalf("recovery scan: published=%d err=%v", published, err)
	}
	if messages := pub.Messages(); len(messages) != 1 || messages[0].PartitionKey != "farm-95" {
		t.Fatalf("published messages=%+v", messages)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// ── TestOutboxRelay_PublishFailure_Dead ───────────────────────────────────────

// When publish fails and retry_count >= maxRetry, the row is marked DEAD.
func TestOutboxRelay_PublishFailure_Dead(t *testing.T) {
	pub := NewMemPublisher()
	pub.SetPublishError(errors.New("persistent failure"))
	relay, mock := newRelayWithMock(t, pub)

	payload := []byte(`{"event_id":"evt3"}`)
	retryCount := relay.maxRetry // already at max

	// reclaimStale
	mock.ExpectExec("UPDATE outbox_events").WillReturnResult(sqlmock.NewResult(0, 0))

	// claimBatch
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").
		WithArgs(testNow, 10).
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}).
			AddRow(uint64(3), "evt3", "farm-7", "farm.watered.v1", payload, retryCount))
	mock.ExpectExec("UPDATE outbox_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// markRetryOrDead → DEAD
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(retryCount+1, "persistent failure", testNow, uint64(3)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	n, err := relay.ScanAndPublish(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// ── TestOutboxRelay_ReclaimStale ──────────────────────────────────────────────

// reclaimStale resets PUBLISHING rows whose locked_until has expired.
func TestOutboxRelay_ReclaimStale(t *testing.T) {
	pub := NewMemPublisher()
	relay, mock := newRelayWithMock(t, pub)

	// reclaimStale: 1 stale row reclaimed
	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(testNow, testNow).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// claimBatch: empty
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT outbox_id").
		WillReturnRows(sqlmock.NewRows([]string{"outbox_id", "event_id", "partition_key", "event_type", "payload", "retry_count"}))
	mock.ExpectCommit()

	n, err := relay.ScanAndPublish(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// ── TestMemPublisher ──────────────────────────────────────────────────────────

func TestMemPublisher_PublishAndMessages(t *testing.T) {
	p := NewMemPublisher()
	ctx := context.Background()

	if err := p.Publish(ctx, "t1", "k1", []byte("msg1")); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(ctx, "t1", "k2", []byte("msg2")); err != nil {
		t.Fatal(err)
	}
	msgs := p.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2, got %d", len(msgs))
	}
	if msgs[0].PartitionKey != "k1" || string(msgs[0].Payload) != "msg1" {
		t.Fatalf("unexpected msg[0]: %+v", msgs[0])
	}
}

func TestMemPublisher_SetError(t *testing.T) {
	p := NewMemPublisher()
	p.SetPublishError(errors.New("boom"))
	err := p.Publish(context.Background(), "t", "k", []byte("x"))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}
