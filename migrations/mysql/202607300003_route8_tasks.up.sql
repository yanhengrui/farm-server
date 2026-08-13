-- 路线 8：任务/图鉴系统。
-- player_tasks: 玩家任务进度，一用户一 task_key 唯一行，状态 ACTIVE→COMPLETED→REWARD_CLAIMED。
-- catalog_unlocks: 已解锁图鉴条目，任务奖励触发时写入，幂等。

CREATE TABLE player_tasks (
    task_id     BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    user_id     BIGINT UNSIGNED  NOT NULL,
    task_key    VARCHAR(64)      NOT NULL,
    progress    INT UNSIGNED     NOT NULL DEFAULT 0,
    status      VARCHAR(16)      NOT NULL DEFAULT 'ACTIVE',
    created_at  DATETIME(3)      NOT NULL,
    updated_at  DATETIME(3)      NOT NULL,
    PRIMARY KEY (task_id),
    UNIQUE KEY uk_player_task (user_id, task_key),
    KEY idx_player_tasks_status (user_id, status),
    CONSTRAINT ck_player_task_status CHECK (status IN ('ACTIVE','COMPLETED','REWARD_CLAIMED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE catalog_unlocks (
    unlock_id   BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    user_id     BIGINT UNSIGNED  NOT NULL,
    catalog_key VARCHAR(64)      NOT NULL,
    unlocked_at DATETIME(3)      NOT NULL,
    PRIMARY KEY (unlock_id),
    UNIQUE KEY uk_catalog_unlock (user_id, catalog_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
