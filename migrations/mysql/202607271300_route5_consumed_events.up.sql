-- Route 5: consumed_events — Kafka 消费者幂等投影去重表。
-- 保证 event_id + consumer_name 全局唯一，防止重复投影（见 CONSTITUTION §1.4）。
CREATE TABLE consumed_events (
    consumed_id    BIGINT UNSIGNED     NOT NULL AUTO_INCREMENT,
    event_id       VARCHAR(64)         NOT NULL,
    consumer_name  VARCHAR(64)         NOT NULL,
    processed_at   DATETIME(3)         NOT NULL,
    PRIMARY KEY (consumed_id),
    UNIQUE KEY uk_consumed_event_consumer (event_id, consumer_name),
    KEY idx_consumed_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
