package infrastructure

import (
	"context"
	"database/sql"
	"time"
)

// ConsumerFailure is the durable recovery record for a Kafka message that
// cannot be processed normally. Operators can replay PENDING rows after the
// underlying defect is fixed; replay must retain the original event_id.
type ConsumerFailure struct {
	ConsumerGroup string
	ConsumerName  string
	Topic         string
	Partition     int
	Offset        int64
	Key           []byte
	EventID       string
	Payload       []byte
	Attempts      int
	LastError     string
	FirstFailedAt time.Time
	LastFailedAt  time.Time
}

type consumerFailureRecorder interface {
	Record(context.Context, ConsumerFailure) error
}

// ConsumerFailureStore persists exhausted or permanently invalid messages in
// MySQL before their Kafka offsets are committed.
type ConsumerFailureStore struct {
	db *sql.DB
}

func NewConsumerFailureStore(db *sql.DB) *ConsumerFailureStore {
	return &ConsumerFailureStore{db: db}
}

func (s *ConsumerFailureStore) Record(ctx context.Context, failure ConsumerFailure) error {
	const q = `
		INSERT INTO consumer_failed_events
			(consumer_group, consumer_name, topic, partition_id, message_offset,
			 message_key, event_id, payload, attempt_count, last_error, status,
			 first_failed_at, last_failed_at)
		VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, 'PENDING', ?, ?)
		ON DUPLICATE KEY UPDATE
			consumer_name = VALUES(consumer_name),
			message_key = VALUES(message_key),
			event_id = VALUES(event_id),
			payload = VALUES(payload),
			attempt_count = GREATEST(attempt_count, VALUES(attempt_count)),
			last_error = VALUES(last_error),
			last_failed_at = VALUES(last_failed_at)
	`
	payload := failure.Payload
	if payload == nil {
		payload = []byte{}
	}
	_, err := s.db.ExecContext(ctx, q,
		truncateConsumerFailureText(failure.ConsumerGroup, 255),
		truncateConsumerFailureText(failure.ConsumerName, 128),
		truncateConsumerFailureText(failure.Topic, 255),
		failure.Partition,
		failure.Offset,
		failure.Key,
		truncateConsumerFailureText(failure.EventID, 128),
		payload,
		failure.Attempts,
		truncateConsumerFailureText(failure.LastError, 2048),
		failure.FirstFailedAt,
		failure.LastFailedAt,
	)
	return err
}

func truncateConsumerFailureText(value string, maxRunes int) string {
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

var _ consumerFailureRecorder = (*ConsumerFailureStore)(nil)
