-- 初始核心表（骨架子集）。完整 16 张表见 02-数据模型与迁移设计.md。
-- 由受控部署步骤执行，禁止应用启动时自动 migrate（见 06-6）。

CREATE TABLE accounts (
    user_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    farm_id       BIGINT UNSIGNED NOT NULL,
    account_type  VARCHAR(16) NOT NULL,
    status        VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    created_at    DATETIME(3) NOT NULL,
    updated_at    DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id),
    UNIQUE KEY uk_accounts_farm_id (farm_id),
    CONSTRAINT ck_accounts_type CHECK (account_type IN ('GUEST','REGISTERED')),
    CONSTRAINT ck_accounts_status CHECK (status IN ('ACTIVE','BANNED','DELETED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE sessions (
    session_id            BINARY(16) NOT NULL,
    user_id               BIGINT UNSIGNED NOT NULL,
    device_id             VARCHAR(128) NOT NULL,
    status                VARCHAR(24) NOT NULL DEFAULT 'ACTIVE',
    gateway_id            VARCHAR(64) NULL,
    owner_epoch           BIGINT UNSIGNED NOT NULL DEFAULT 1,
    current_farm_id       BIGINT UNSIGNED NULL,
    last_client_seq       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_server_seq       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_acked_server_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
    protocol_version      VARCHAR(16) NOT NULL,
    expires_at            DATETIME(3) NOT NULL,
    created_at            DATETIME(3) NOT NULL,
    updated_at            DATETIME(3) NOT NULL,
    PRIMARY KEY (session_id),
    KEY idx_sessions_user_status (user_id, status, expires_at),
    CONSTRAINT ck_sessions_status CHECK (status IN ('ACTIVE','HANDOFF_PENDING','REVOKED','EXPIRED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE wallets (
    user_id      BIGINT UNSIGNED NOT NULL,
    coin_balance BIGINT NOT NULL DEFAULT 0,
    row_version  BIGINT UNSIGNED NOT NULL DEFAULT 1,
    created_at   DATETIME(3) NOT NULL,
    updated_at   DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id),
    CONSTRAINT ck_wallet_coin_nonnegative CHECK (coin_balance >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE farm_snapshots (
    farm_id     BIGINT UNSIGNED NOT NULL,
    owner_id    BIGINT UNSIGNED NOT NULL,
    version     BIGINT UNSIGNED NOT NULL DEFAULT 0,
    snapshot    JSON NOT NULL,
    created_at  DATETIME(3) NOT NULL,
    updated_at  DATETIME(3) NOT NULL,
    PRIMARY KEY (farm_id),
    KEY idx_farm_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- cmd_receipts：缺少自然业务唯一键的资产命令使用的持久幂等回执（ADR-018）。
-- 经济命令改用 economy_transactions 的业务唯一键；按 user_id + cmd_id 唯一。
CREATE TABLE cmd_receipts (
    receipt_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id       BIGINT UNSIGNED NOT NULL,
    cmd_id        VARCHAR(64) NOT NULL,
    result_code   VARCHAR(48) NOT NULL,
    result_version BIGINT UNSIGNED NULL,
    response_digest VARBINARY(255) NULL,
    created_at    DATETIME(3) NOT NULL,
    PRIMARY KEY (receipt_id),
    UNIQUE KEY uk_receipt_user_cmd (user_id, cmd_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- outbox_events：与业务事务同库同事务写入，workersvr Relay 发布 Kafka（ADR-016）。
CREATE TABLE outbox_events (
    outbox_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id       VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL,
    aggregate_id   BIGINT UNSIGNED NOT NULL,
    partition_key  VARCHAR(64) NOT NULL,
    event_type     VARCHAR(48) NOT NULL,
    schema_version VARCHAR(16) NOT NULL,
    payload        JSON NOT NULL,
    status         VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    available_at   DATETIME(3) NOT NULL,
    created_at     DATETIME(3) NOT NULL,
    PRIMARY KEY (outbox_id),
    UNIQUE KEY uk_outbox_event_id (event_id),
    KEY idx_outbox_dispatch (status, available_at, outbox_id),
    CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING','PUBLISHING','PUBLISHED','DEAD'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
