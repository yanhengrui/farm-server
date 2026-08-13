-- Route 11: one local friendship edge per user aggregate.  This replaces the
-- single normalized cross-user row when two users live on different shards.
-- The event_id is the idempotency key for the remote half of the Saga.
CREATE TABLE friendship_edges (
    user_id BIGINT UNSIGNED NOT NULL,
    friend_user_id BIGINT UNSIGNED NOT NULL,
    state ENUM('PENDING','ACTIVE','FAILED') NOT NULL DEFAULT 'PENDING',
    source_event_id CHAR(36) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (user_id, friend_user_id),
    UNIQUE KEY uk_friendship_edges_source_event (source_event_id),
    KEY idx_friendship_edges_friend (friend_user_id),
    CONSTRAINT ck_friendship_edges_not_self CHECK (user_id <> friend_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
