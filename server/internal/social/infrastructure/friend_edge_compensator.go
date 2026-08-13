package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// FriendEdgeCompensator provides the terminal compensation for a source-side
// friend request that can never leave its shard because its local Outbox has
// become DEAD. It deliberately does not timeout a PENDING request that was
// published successfully: Kafka lag or a temporary target outage is not proof
// that the relationship failed.
type FriendEdgeCompensator struct {
	db *sql.DB
}

func NewFriendEdgeCompensator(db *sql.DB) *FriendEdgeCompensator {
	return &FriendEdgeCompensator{db: db}
}

// CompensateDeadRequests marks source edges FAILED only when their correlated
// outbox row is terminal DEAD. The update is idempotent and bounded by limit.
func (c *FriendEdgeCompensator) CompensateDeadRequests(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("friend edge compensation limit must be positive")
	}
	now := time.Now().UTC()
	// MySQL does not permit LIMIT on a multi-table UPDATE. Select the bounded
	// candidate key set in a materialized derived table first, then join it back
	// to the target. The extra nesting prevents MySQL's target-table subquery
	// restriction while keeping this maintenance pass bounded and idempotent.
	result, err := c.db.ExecContext(ctx, `
		UPDATE friendship_edges AS edge
		JOIN (
			SELECT user_id, friend_user_id
			FROM (
				SELECT candidate.user_id, candidate.friend_user_id
				FROM friendship_edges AS candidate
				JOIN outbox_events AS outbox ON outbox.event_id = candidate.source_event_id
				WHERE candidate.state = 'PENDING'
				  AND candidate.source_event_id IS NOT NULL
				  AND outbox.status = 'DEAD'
				ORDER BY candidate.updated_at, candidate.user_id, candidate.friend_user_id
				LIMIT ?
			) AS limited_edges
		) AS candidates ON candidates.user_id = edge.user_id AND candidates.friend_user_id = edge.friend_user_id
		SET edge.state = 'FAILED', edge.updated_at = ?`, limit, now)
	if err != nil {
		return 0, fmt.Errorf("compensate dead friend edges: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("compensate dead friend edge affected rows: %w", err)
	}
	return int(n), nil
}
