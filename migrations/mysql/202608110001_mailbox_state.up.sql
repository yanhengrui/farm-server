CREATE TABLE mailbox_states (
    user_id       BIGINT UNSIGNED NOT NULL COMMENT '玩家用户ID，与账号/邮件共分片',
    unread_count  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '权威未读邮件数',
    version       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '邮箱状态单调版本',
    updated_at    DATETIME(3) NOT NULL COMMENT '最后一次邮箱状态变更时间',
    PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Keep old and new gamesvr binaries compatible during a rolling release.
-- The database owns the counter transition, so an old binary that only
-- changes mails.status cannot create permanent drift.
CREATE TRIGGER trg_mails_mailbox_after_insert
AFTER INSERT ON mails
FOR EACH ROW
INSERT INTO mailbox_states (user_id, unread_count, version, updated_at)
SELECT NEW.user_id, 1, 1, UTC_TIMESTAMP(3)
WHERE NEW.status = 'UNREAD'
ON DUPLICATE KEY UPDATE
    unread_count = unread_count + 1,
    version = version + 1,
    updated_at = VALUES(updated_at);

CREATE TRIGGER trg_mails_mailbox_after_update
AFTER UPDATE ON mails
FOR EACH ROW
INSERT INTO mailbox_states (user_id, unread_count, version, updated_at)
SELECT NEW.user_id,
       IF(NEW.status = 'UNREAD', 1, 0),
       1,
       UTC_TIMESTAMP(3)
WHERE (OLD.status = 'UNREAD') <> (NEW.status = 'UNREAD')
ON DUPLICATE KEY UPDATE
    unread_count = GREATEST(
        CAST(unread_count AS SIGNED)
        + IF(NEW.status = 'UNREAD', 1, -1),
        0
    ),
    version = version + 1,
    updated_at = VALUES(updated_at);

CREATE TRIGGER trg_mails_mailbox_after_delete
AFTER DELETE ON mails
FOR EACH ROW
INSERT INTO mailbox_states (user_id, unread_count, version, updated_at)
SELECT OLD.user_id, 0, 1, UTC_TIMESTAMP(3)
WHERE OLD.status = 'UNREAD'
ON DUPLICATE KEY UPDATE
    unread_count = GREATEST(CAST(unread_count AS SIGNED) - 1, 0),
    version = version + 1,
    updated_at = VALUES(updated_at);

-- This is a short, explicit write barrier rather than an unsafe snapshot.
-- Once both tables are locked, the recomputation is exact. After UNLOCK,
-- triggers maintain every write made by old or new application versions.
SET @mailbox_migration_previous_autocommit = @@autocommit;
SET autocommit = 0;
LOCK TABLES mails WRITE, mailbox_states WRITE;

INSERT INTO mailbox_states (user_id, unread_count, version, updated_at)
SELECT user_id,
       SUM(CASE WHEN status = 'UNREAD' THEN 1 ELSE 0 END),
       1,
       UTC_TIMESTAMP(3)
FROM mails
WHERE status != 'DELETED'
GROUP BY user_id
ON DUPLICATE KEY UPDATE
    unread_count = VALUES(unread_count),
    version = version + 1,
    updated_at = VALUES(updated_at);

UPDATE mailbox_states
LEFT JOIN (
    SELECT user_id, SUM(CASE WHEN status = 'UNREAD' THEN 1 ELSE 0 END) AS exact_unread_count
    FROM mails
    WHERE status != 'DELETED'
    GROUP BY user_id
) AS exact ON exact.user_id = mailbox_states.user_id
SET mailbox_states.unread_count = COALESCE(exact.exact_unread_count, 0),
    mailbox_states.version = mailbox_states.version + 1,
    mailbox_states.updated_at = UTC_TIMESTAMP(3);

COMMIT;
UNLOCK TABLES;
SET autocommit = @mailbox_migration_previous_autocommit;
