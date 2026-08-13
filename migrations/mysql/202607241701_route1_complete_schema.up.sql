-- Route 1 完整 schema 补全。
-- 修复 init_core 中简化的表结构，并新增 auth_identities / inventory_items / economy_transactions。
-- 见 02-数据模型与迁移设计.md。禁止修改已执行的 migration 文件；此文件追加所有变更。

-- ── accounts：补全字段 ──────────────────────────────────────────────────────────
ALTER TABLE accounts
    ADD COLUMN display_name  VARCHAR(64)  NOT NULL DEFAULT '' AFTER account_type,
    ADD COLUMN profile_json  JSON         NULL,
    ADD COLUMN row_version   BIGINT UNSIGNED NOT NULL DEFAULT 1,
    ADD COLUMN last_login_at DATETIME(3)  NULL,
    ADD COLUMN deleted_at    DATETIME(3)  NULL,
    MODIFY COLUMN farm_id    BIGINT UNSIGNED NULL,  -- 农场创建前允许为 NULL
    ADD KEY idx_accounts_status_updated (status, updated_at);

-- ── sessions：补全字段 ─────────────────────────────────────────────────────────
ALTER TABLE sessions
    ADD COLUMN refresh_token_hash BINARY(32) NOT NULL DEFAULT '' AFTER device_id,
    ADD COLUMN last_seen_at       DATETIME(3) NOT NULL DEFAULT '1970-01-01 00:00:00.000' AFTER expires_at,
    ADD COLUMN revoked_at         DATETIME(3) NULL,
    ADD UNIQUE KEY uk_sessions_refresh_hash (refresh_token_hash),
    ADD KEY idx_sessions_gateway_status (gateway_id, status),
    ADD KEY idx_sessions_expires (expires_at);

-- ── farm_snapshots：对齐完整规格 ──────────────────────────────────────────────
-- 当前已有：farm_id, owner_id, version, snapshot, created_at, updated_at
-- 需补全：owner_user_id（别名）、last_event_seq、route_epoch、actor_state、
--         schema_version、snapshot_checksum、next_pet_action_at、checkpointed_at
-- 为向后兼容，保留原有 owner_id 列；同时新增 owner_user_id 作为规范列名。
ALTER TABLE farm_snapshots
    ADD COLUMN owner_user_id    BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER owner_id,
    ADD COLUMN last_event_seq   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN route_epoch      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN actor_state      VARCHAR(16) NOT NULL DEFAULT 'IDLE',
    ADD COLUMN schema_version   SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    ADD COLUMN snapshot_checksum BINARY(32) NOT NULL DEFAULT '',
    ADD COLUMN next_pet_action_at DATETIME(3) NULL,
    ADD COLUMN checkpointed_at  DATETIME(3) NOT NULL DEFAULT '1970-01-01 00:00:00.000',
    ADD UNIQUE KEY uk_farm_owner_user (owner_user_id),
    ADD KEY idx_farm_pet_action (next_pet_action_at, farm_id),
    ADD KEY idx_farm_updated (updated_at),
    ADD CONSTRAINT ck_farm_actor_state CHECK (actor_state IN ('IDLE','READY','MIGRATING'));

-- ── outbox_events：对齐完整规格 ──────────────────────────────────────────────
ALTER TABLE outbox_events
    ADD COLUMN headers_json  JSON     NULL AFTER payload,
    ADD COLUMN retry_count   INT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN locked_by     VARCHAR(64) NULL,
    ADD COLUMN locked_until  DATETIME(3) NULL,
    ADD COLUMN published_at  DATETIME(3) NULL,
    ADD COLUMN last_error    VARCHAR(512) NULL,
    ADD COLUMN updated_at    DATETIME(3) NOT NULL DEFAULT '1970-01-01 00:00:00.000',
    ADD KEY idx_outbox_aggregate (aggregate_type, aggregate_id, outbox_id);

-- ── auth_identities ───────────────────────────────────────────────────────────
CREATE TABLE auth_identities (
    identity_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id           BIGINT UNSIGNED NOT NULL,
    provider          VARCHAR(24) NOT NULL,
    provider_subject  VARCHAR(191) NOT NULL,
    credential_hash   VARBINARY(255) NULL,
    status            VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    verified_at       DATETIME(3) NULL,
    deleted_at        DATETIME(3) NULL,
    created_at        DATETIME(3) NOT NULL,
    updated_at        DATETIME(3) NOT NULL,
    PRIMARY KEY (identity_id),
    UNIQUE KEY uk_auth_provider_subject (provider, provider_subject),
    KEY idx_auth_user_status (user_id, status),
    CONSTRAINT ck_auth_status CHECK (status IN ('ACTIVE','REVOKED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ── inventory_items ───────────────────────────────────────────────────────────
CREATE TABLE inventory_items (
    inventory_item_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id           BIGINT UNSIGNED NOT NULL,
    item_type         VARCHAR(24) NOT NULL,
    item_id           BIGINT UNSIGNED NOT NULL,
    quantity          BIGINT NOT NULL DEFAULT 0,
    row_version       BIGINT UNSIGNED NOT NULL DEFAULT 1,
    deleted_at        DATETIME(3) NULL,
    created_at        DATETIME(3) NOT NULL,
    updated_at        DATETIME(3) NOT NULL,
    PRIMARY KEY (inventory_item_id),
    UNIQUE KEY uk_inventory_user_item (user_id, item_type, item_id),
    KEY idx_inventory_user_type (user_id, item_type, inventory_item_id),
    CONSTRAINT ck_inventory_quantity_nonnegative CHECK (quantity >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ── economy_transactions ──────────────────────────────────────────────────────
CREATE TABLE economy_transactions (
    transaction_id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id         BIGINT UNSIGNED NOT NULL,
    biz_type        VARCHAR(32) NOT NULL,
    biz_id          BINARY(16) NOT NULL,
    currency_type   VARCHAR(16) NOT NULL DEFAULT 'COIN',
    balance_before  BIGINT NOT NULL,
    balance_delta   BIGINT NOT NULL,
    balance_after   BIGINT NOT NULL,
    item_changes_json JSON NULL,
    source_type     VARCHAR(32) NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'COMMITTED',
    trace_id        VARCHAR(64) NULL,
    metadata_json   JSON NULL,
    created_at      DATETIME(3) NOT NULL,
    PRIMARY KEY (transaction_id),
    UNIQUE KEY uk_economy_user_biz (user_id, biz_type, biz_id),
    KEY idx_economy_user_created (user_id, created_at, transaction_id),
    KEY idx_economy_source_created (source_type, created_at),
    CONSTRAINT ck_economy_balance CHECK (balance_before >= 0 AND balance_after >= 0),
    CONSTRAINT ck_economy_status CHECK (status IN ('COMMITTED','REVERSED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
