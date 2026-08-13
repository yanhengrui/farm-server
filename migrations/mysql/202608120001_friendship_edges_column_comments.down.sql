-- Roll back only the Route 11 social edge column comments.
SET SESSION lock_wait_timeout = 10;

ALTER TABLE friendship_edges
    MODIFY COLUMN user_id BIGINT UNSIGNED NOT NULL,
    MODIFY COLUMN friend_user_id BIGINT UNSIGNED NOT NULL,
    MODIFY COLUMN state ENUM('PENDING','ACTIVE','FAILED') NOT NULL DEFAULT 'PENDING',
    MODIFY COLUMN source_event_id CHAR(36) NULL,
    MODIFY COLUMN created_at DATETIME(6) NOT NULL,
    MODIFY COLUMN updated_at DATETIME(6) NOT NULL,
    ALGORITHM=INPLACE, LOCK=NONE;
