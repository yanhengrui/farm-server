package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/observability"
)

// FriendEdgeSaga owns the local transaction half of cross-shard friendship.
// It deliberately does not use a cross-database transaction: remote delivery
// is carried by the local outbox and source_event_id makes delivery idempotent.
type FriendEdgeSaga struct{ db *sql.DB }

func NewFriendEdgeSaga(db *sql.DB) *FriendEdgeSaga { return &FriendEdgeSaga{db: db} }

// Begin writes the requesting user's PENDING edge and its outbox record in one
// local transaction. A retry locks and reuses the persisted source event ID,
// even when the HTTP caller generated a new candidate ID.
func (s *FriendEdgeSaga) Begin(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID string) error {
	return s.BeginWithDisplayNames(ctx, sourceUserID, targetUserID, sourceEventID, "", "")
}

func (s *FriendEdgeSaga) BeginWithDisplayNames(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID, sourceDisplayName, targetDisplayName string) error {
	if sourceUserID <= 0 || targetUserID <= 0 || sourceUserID == targetUserID || sourceEventID == "" {
		return fmt.Errorf("invalid friend edge request")
	}
	sourceDisplayName = normalizeFriendDisplayName(sourceDisplayName, sourceUserID)
	targetDisplayName = normalizeFriendDisplayName(targetDisplayName, targetUserID)
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin friend edge saga: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx,
		"INSERT INTO friendship_edges (user_id, friend_user_id, state, source_event_id, created_at, updated_at) VALUES (?, ?, 'PENDING', ?, ?, ?) ON DUPLICATE KEY UPDATE user_id=user_id",
		uint64(sourceUserID), uint64(targetUserID), sourceEventID, now, now,
	)
	if err != nil {
		return fmt.Errorf("insert local friend edge: %w", err)
	}
	var state, persistedEventID string
	if err = tx.QueryRowContext(ctx,
		"SELECT state, source_event_id FROM friendship_edges WHERE user_id=? AND friend_user_id=? FOR UPDATE",
		uint64(sourceUserID), uint64(targetUserID),
	).Scan(&state, &persistedEventID); err != nil {
		return fmt.Errorf("lock local friend edge: %w", err)
	}
	switch state {
	case "ACTIVE":
		return tx.Commit()
	case "PENDING":
		if persistedEventID == "" {
			return fmt.Errorf("pending friend edge has no source event id")
		}
		sourceEventID = persistedEventID
	case "FAILED":
		return fmt.Errorf("friend edge request is terminally failed")
	default:
		return fmt.Errorf("friend edge request has unknown state %q", state)
	}
	payload := socialevents.FriendEdgeRequestedPayload{
		SourceEventID: sourceEventID, SourceUserID: strconv.FormatInt(sourceUserID, 10), TargetUserID: strconv.FormatInt(targetUserID, 10),
		SourceDisplayName: sourceDisplayName, TargetDisplayName: targetDisplayName, RequestedAt: now,
	}
	envelope := socialevents.EventEnvelope{EventID: sourceEventID, EventType: socialevents.EventTypeFriendEdgeRequested, AggregateType: "social", AggregateID: strconv.FormatInt(sourceUserID, 10), SchemaVersion: 1, OccurredAt: now, TraceID: observability.TraceID(ctx), CorrelationID: sourceEventID, Payload: payload}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal friend edge outbox: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type, schema_version, payload, status, available_at, created_at, updated_at) VALUES (?, 'social', ?, ?, ?, '1.0', ?, 'PENDING', ?, ?, ?) ON DUPLICATE KEY UPDATE event_id=event_id",
		sourceEventID, uint64(sourceUserID), strconv.FormatInt(targetUserID, 10), string(socialevents.EventTypeFriendEdgeRequested), raw, now, now, now,
	)
	if err != nil {
		return fmt.Errorf("insert friend edge outbox: %w", err)
	}
	return tx.Commit()
}

// NewSourceEventID is exposed so request retries persist one idempotency key.
func NewSourceEventID() string { return id.NewV7() }

// ApplyRemote creates the target user's local edge idempotently after Kafka
// delivery. The target half becomes ACTIVE only after this local transaction.
func (s *FriendEdgeSaga) ApplyRemote(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID string) error {
	return s.ApplyRemoteWithDisplayNames(ctx, sourceUserID, targetUserID, sourceEventID, "", "")
}

func (s *FriendEdgeSaga) ApplyRemoteWithDisplayNames(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID, sourceDisplayName, targetDisplayName string) error {
	if sourceUserID <= 0 || targetUserID <= 0 || sourceUserID == targetUserID || sourceEventID == "" {
		return fmt.Errorf("invalid remote friend edge")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin remote friend edge: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx,
		// Do not touch an already ACTIVE row. MySQL reports a changed duplicate
		// update as RowsAffected=2; updating updated_at on every redelivery would
		// therefore emit an unbounded sequence of duplicate ACK outbox events.
		// Only an insert or a PENDING -> ACTIVE transition may create the ACK.
		"INSERT INTO friendship_edges (user_id, friend_user_id, state, source_event_id, created_at, updated_at) VALUES (?, ?, 'ACTIVE', ?, ?, ?) ON DUPLICATE KEY UPDATE updated_at=IF(state='PENDING', VALUES(updated_at), updated_at), state=IF(state='PENDING', 'ACTIVE', state)",
		uint64(targetUserID), uint64(sourceUserID), sourceEventID, now, now,
	)
	if err != nil {
		return fmt.Errorf("apply remote friend edge: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("remote friend edge affected rows: %w", err)
	}
	// A retry after the first transaction must not create another ACK. The
	// original ACK is already durable in this shard's outbox.
	if changed > 0 {
		ackID := id.NewV7()
		payload := socialevents.FriendEdgeAppliedPayload{
			SourceEventID: sourceEventID, SourceUserID: strconv.FormatInt(sourceUserID, 10), TargetUserID: strconv.FormatInt(targetUserID, 10),
			SourceDisplayName: normalizeFriendDisplayName(sourceDisplayName, sourceUserID), TargetDisplayName: normalizeFriendDisplayName(targetDisplayName, targetUserID), AppliedAt: now,
		}
		envelope := socialevents.EventEnvelope{EventID: ackID, EventType: socialevents.EventTypeFriendEdgeApplied, AggregateType: "social", AggregateID: strconv.FormatInt(targetUserID, 10), SchemaVersion: 1, OccurredAt: now, TraceID: observability.TraceID(ctx), CorrelationID: sourceEventID, CausationID: sourceEventID, Payload: payload}
		raw, marshalErr := json.Marshal(envelope)
		if marshalErr != nil {
			return fmt.Errorf("marshal friend edge ack: %w", marshalErr)
		}
		if _, err = tx.ExecContext(ctx,
			"INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type, schema_version, payload, status, available_at, created_at, updated_at) VALUES (?, 'social', ?, ?, ?, '1.0', ?, 'PENDING', ?, ?, ?)",
			ackID, uint64(targetUserID), strconv.FormatInt(sourceUserID, 10), string(socialevents.EventTypeFriendEdgeApplied), raw, now, now, now,
		); err != nil {
			return fmt.Errorf("insert friend edge ack outbox: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit remote friend edge: %w", err)
	}
	return nil
}

// ConfirmSource activates the initiating user's edge only after the target
// shard's ACK has been durably relayed. Missing source state is retriable: it
// prevents an out-of-order or malformed ACK from being silently accepted.
func (s *FriendEdgeSaga) ConfirmSource(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID string) error {
	return s.ConfirmSourceWithDisplayNames(ctx, sourceUserID, targetUserID, sourceEventID, "", "")
}

func (s *FriendEdgeSaga) ConfirmSourceWithDisplayNames(ctx context.Context, sourceUserID, targetUserID int64, sourceEventID, sourceDisplayName, targetDisplayName string) error {
	if sourceUserID <= 0 || targetUserID <= 0 || sourceUserID == targetUserID || sourceEventID == "" {
		return fmt.Errorf("invalid friend edge confirmation")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin source friend edge confirmation: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx,
		"UPDATE friendship_edges SET state='ACTIVE', updated_at=? WHERE user_id=? AND friend_user_id=? AND source_event_id=? AND state='PENDING'",
		now, uint64(sourceUserID), uint64(targetUserID), sourceEventID,
	)
	if err != nil {
		return fmt.Errorf("confirm source friend edge: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm source friend edge affected rows: %w", err)
	}
	if changed == 0 {
		var state string
		err = tx.QueryRowContext(ctx, "SELECT state FROM friendship_edges WHERE user_id=? AND friend_user_id=? AND source_event_id=?", uint64(sourceUserID), uint64(targetUserID), sourceEventID).Scan(&state)
		if err != nil {
			return fmt.Errorf("confirm source friend edge lookup: %w", err)
		}
		if state != "ACTIVE" {
			return fmt.Errorf("confirm source friend edge has state %q", state)
		}
		return tx.Commit()
	}
	uidA, uidB := sourceUserID, targetUserID
	if uidA > uidB {
		uidA, uidB = uidB, uidA
	}
	// In the cross-shard AcceptInvite flow source is the accepter and target is
	// the inviter. Publish the same public event as the single-shard path in the
	// same transaction that activates the source edge.
	if err = insertFriendAcceptedOutbox(ctx, tx,
		targetUserID, sourceUserID, uidA, uidB,
		normalizeFriendDisplayName(targetDisplayName, targetUserID),
		normalizeFriendDisplayName(sourceDisplayName, sourceUserID),
		now,
	); err != nil {
		return fmt.Errorf("insert cross-shard friend accepted outbox: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit source friend edge confirmation: %w", err)
	}
	return nil
}

func normalizeFriendDisplayName(name string, userID int64) string {
	if name = strings.TrimSpace(name); name != "" {
		return name
	}
	return strconv.FormatInt(userID, 10)
}
