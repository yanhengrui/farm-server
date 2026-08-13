package infrastructure

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestConsumerFailureStoreRecord(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	first := time.Date(2026, 8, 3, 1, 2, 3, 0, time.UTC)
	last := first.Add(5 * time.Second)
	mock.ExpectExec(`(?s)INSERT INTO consumer_failed_events.*ON DUPLICATE KEY UPDATE`).
		WithArgs(
			"workersvr-task", "task-projector", "farm-events", 2, int64(97),
			[]byte("farm-42"), "event-97", []byte(`{"event_id":"event-97"}`),
			5, "projector unavailable", first, last,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	store := NewConsumerFailureStore(db)
	err = store.Record(context.Background(), ConsumerFailure{
		ConsumerGroup: "workersvr-task",
		ConsumerName:  "task-projector",
		Topic:         "farm-events",
		Partition:     2,
		Offset:        97,
		Key:           []byte("farm-42"),
		EventID:       "event-97",
		Payload:       []byte(`{"event_id":"event-97"}`),
		Attempts:      5,
		LastError:     "projector unavailable",
		FirstFailedAt: first,
		LastFailedAt:  last,
	})
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTruncateConsumerFailureText(t *testing.T) {
	if got := truncateConsumerFailureText("错误详情", 2); got != "错误" {
		t.Fatalf("truncated=%q", got)
	}
}
