-- Route 9.7: durable parking table for Kafka messages that exhaust processing
-- retries or contain a permanently invalid event envelope. The consumer may
-- commit the Kafka offset only after this row is durably written.
CREATE TABLE consumer_failed_events (
    failure_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    consumer_group   VARCHAR(255) NOT NULL,
    consumer_name    VARCHAR(128) NOT NULL,
    topic            VARCHAR(255) NOT NULL,
    partition_id     INT NOT NULL,
    message_offset   BIGINT NOT NULL,
    message_key      LONGBLOB NULL,
    event_id         VARCHAR(128) NULL,
    payload          LONGBLOB NOT NULL,
    attempt_count    INT UNSIGNED NOT NULL,
    last_error       VARCHAR(2048) NOT NULL,
    status           VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    first_failed_at  DATETIME(3) NOT NULL,
    last_failed_at   DATETIME(3) NOT NULL,
    resolved_at      DATETIME(3) NULL,
    PRIMARY KEY (failure_id),
    UNIQUE KEY uk_consumer_failed_position (consumer_group, topic, partition_id, message_offset),
    KEY idx_consumer_failed_status_time (status, last_failed_at),
    KEY idx_consumer_failed_event (event_id),
    CONSTRAINT ck_consumer_failed_status CHECK (status IN ('PENDING','REPLAYED','IGNORED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
