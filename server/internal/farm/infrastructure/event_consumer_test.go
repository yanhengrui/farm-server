package infrastructure

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	kafka "github.com/segmentio/kafka-go"

	farmv1 "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/clock"
)

type route97Reader struct {
	messages []kafka.Message
	next     int
	order    *[]string
	cancel   context.CancelFunc
}

func (r *route97Reader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.next >= len(r.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	m := r.messages[r.next]
	r.next++
	*r.order = append(*r.order, "fetch:"+string(m.Key))
	return m, nil
}

func (r *route97Reader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if len(messages) != 1 {
		return errors.New("expected exactly one message per commit")
	}
	*r.order = append(*r.order, "commit:"+string(messages[0].Key))
	if r.next == len(r.messages) {
		r.cancel()
	}
	return nil
}

type route97Projector struct {
	attempts map[string]int
	failures map[string]int
	order    *[]string
}

func (p *route97Projector) Name() string { return "route97-projector" }

func (p *route97Projector) Handle(_ context.Context, env farmv1.EventEnvelope) error {
	p.attempts[env.EventID]++
	*p.order = append(*p.order, "handle:"+env.EventID)
	if p.attempts[env.EventID] <= p.failures[env.EventID] {
		return errors.New("injected projector failure")
	}
	return nil
}

type route97FailureRecorder struct {
	failures  []ConsumerFailure
	failFirst int
	order     *[]string
}

func (r *route97FailureRecorder) Record(_ context.Context, failure ConsumerFailure) error {
	*r.order = append(*r.order, "park:"+string(failure.Key))
	if r.failFirst > 0 {
		r.failFirst--
		return errors.New("injected failure store outage")
	}
	r.failures = append(r.failures, failure)
	return nil
}

func route97ExpectUnseen(mock sqlmock.Sqlmock, eventID, consumerName string) {
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT 1 FROM consumed_events WHERE event_id = ? AND consumer_name = ? LIMIT 1`)).
		WithArgs(eventID, consumerName).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
}

func route97ExpectConsumed(mock sqlmock.Sqlmock, eventID, consumerName string) {
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO consumed_events (event_id, consumer_name, processed_at) VALUES (?, ?, ?)`)).
		WithArgs(eventID, consumerName, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func route97Message(t *testing.T, eventID string, offset int64) kafka.Message {
	t.Helper()
	raw, err := json.Marshal(farmv1.EventEnvelope{
		EventID: eventID, EventType: farmv1.EventTypeFarmPlanted,
		AggregateType: "farm", AggregateID: "97", SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "farm-events", Key: []byte(eventID), Value: raw, Partition: 0, Offset: offset, HighWaterMark: offset + 2}
}

func TestKafkaConsumerRetriesCurrentMessageBeforeFetchingNext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const consumerName = "route97-projector"
	// A fails before it can be marked consumed, then succeeds on retry. Only
	// after A is committed may the reader fetch and process B.
	route97ExpectUnseen(mock, "A", consumerName)
	route97ExpectUnseen(mock, "A", consumerName)
	route97ExpectConsumed(mock, "A", consumerName)
	route97ExpectUnseen(mock, "B", consumerName)
	route97ExpectConsumed(mock, "B", consumerName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make([]string, 0, 7)
	reader := &route97Reader{
		messages: []kafka.Message{route97Message(t, "A", 10), route97Message(t, "B", 11)},
		order:    &order,
		cancel:   cancel,
	}
	projector := &route97Projector{
		attempts: make(map[string]int),
		failures: map[string]int{"A": 1},
		order:    &order,
	}
	consumer := NewKafkaConsumer(
		"unused:9092", "farm-events", "route97-group",
		NewConsumerDedup(db), projector,
		slog.New(slog.NewTextHandler(io.Discard, nil)), clock.NewFixed(time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)),
	)
	consumer.retryMin = 0
	consumer.retryMax = 0

	if err := consumer.run(ctx, reader); err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	wantOrder := []string{
		"fetch:A", "handle:A", "handle:A", "commit:A",
		"fetch:B", "handle:B", "commit:B",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("consumer order=%v, want %v", order, wantOrder)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaConsumerParksPermanentMessageBeforeContinuing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const consumerName = "route97-projector"
	route97ExpectUnseen(mock, "B", consumerName)
	route97ExpectConsumed(mock, "B", consumerName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make([]string, 0, 6)
	reader := &route97Reader{
		messages: []kafka.Message{
			{Topic: "farm-events", Key: []byte("A"), Value: []byte(`{"broken"`), Partition: 0, Offset: 20, HighWaterMark: 22},
			route97Message(t, "B", 21),
		},
		order:  &order,
		cancel: cancel,
	}
	projector := &route97Projector{attempts: make(map[string]int), failures: make(map[string]int), order: &order}
	recorder := &route97FailureRecorder{order: &order}
	consumer := NewKafkaConsumer(
		"unused:9092", "farm-events", "route97-group",
		NewConsumerDedup(db), projector,
		slog.New(slog.NewTextHandler(io.Discard, nil)), clock.NewFixed(time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)),
	)
	consumer.failures = recorder
	consumer.retryMin, consumer.retryMax = 0, 0

	if err := consumer.run(ctx, reader); err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	wantOrder := []string{"fetch:A", "park:A", "commit:A", "fetch:B", "handle:B", "commit:B"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("consumer order=%v, want %v", order, wantOrder)
	}
	if len(recorder.failures) != 1 {
		t.Fatalf("parked failures=%d, want 1", len(recorder.failures))
	}
	parked := recorder.failures[0]
	if parked.Attempts != 1 || parked.EventID != "" || parked.Offset != 20 || string(parked.Payload) != `{"broken"` {
		t.Fatalf("parked failure=%+v", parked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaConsumerParksAfterRetryExhaustionAndRetriesFailureStore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const consumerName = "route97-projector"
	route97ExpectUnseen(mock, "A", consumerName)
	route97ExpectUnseen(mock, "A", consumerName)
	route97ExpectUnseen(mock, "B", consumerName)
	route97ExpectConsumed(mock, "B", consumerName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make([]string, 0, 9)
	reader := &route97Reader{
		messages: []kafka.Message{route97Message(t, "A", 30), route97Message(t, "B", 31)},
		order:    &order,
		cancel:   cancel,
	}
	projector := &route97Projector{
		attempts: make(map[string]int),
		failures: map[string]int{"A": 100},
		order:    &order,
	}
	recorder := &route97FailureRecorder{failFirst: 1, order: &order}
	consumer := NewKafkaConsumer(
		"unused:9092", "farm-events", "route97-group",
		NewConsumerDedup(db), projector,
		slog.New(slog.NewTextHandler(io.Discard, nil)), clock.NewFixed(time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)),
	).WithFailurePolicy(2, time.Nanosecond, time.Nanosecond)
	consumer.failures = recorder
	consumer.retryMin, consumer.retryMax = 0, 0

	if err := consumer.run(ctx, reader); err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	wantOrder := []string{
		"fetch:A", "handle:A", "handle:A", "park:A", "park:A", "commit:A",
		"fetch:B", "handle:B", "commit:B",
	}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("consumer order=%v, want %v", order, wantOrder)
	}
	if projector.attempts["A"] != 2 {
		t.Fatalf("A processing attempts=%d, want 2", projector.attempts["A"])
	}
	if len(recorder.failures) != 1 || recorder.failures[0].Attempts != 2 || recorder.failures[0].EventID != "A" {
		t.Fatalf("parked failures=%+v", recorder.failures)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
