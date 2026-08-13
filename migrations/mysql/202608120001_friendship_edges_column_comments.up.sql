-- Route 11 social edge data dictionary. Only column metadata changes.
-- Keep the operation online and fail instead of falling back to COPY DDL.
SET NAMES utf8mb4;
SET SESSION lock_wait_timeout = 10;

ALTER TABLE friendship_edges
    MODIFY COLUMN user_id BIGINT UNSIGNED NOT NULL COMMENT '源玩家账号标识；本分片好友边所属聚合',
    MODIFY COLUMN friend_user_id BIGINT UNSIGNED NOT NULL COMMENT '目标好友玩家账号标识',
    MODIFY COLUMN state ENUM('PENDING','ACTIVE','FAILED') NOT NULL DEFAULT 'PENDING' COMMENT '跨分片好友边状态：PENDING、ACTIVE 或 FAILED',
    MODIFY COLUMN source_event_id CHAR(36) NULL COMMENT '源端稳定 Saga 事件标识；用于远端确认幂等关联',
    MODIFY COLUMN created_at DATETIME(6) NOT NULL COMMENT '好友边创建时间',
    MODIFY COLUMN updated_at DATETIME(6) NOT NULL COMMENT '好友边最近更新时间',
    ALGORITHM=INPLACE, LOCK=NONE;
