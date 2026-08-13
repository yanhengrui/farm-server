-- Route 1 schema 补全 DOWN：回滚顺序与 UP 相反。
-- 删除新建的表，移除 ALTER TABLE 新增的列和索引。

-- ── 删除新建的表 ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS economy_transactions;
DROP TABLE IF EXISTS inventory_items;
DROP TABLE IF EXISTS auth_identities;

-- ── outbox_events：回滚新增列 ────────────────────────────────────────────────
ALTER TABLE outbox_events
    DROP INDEX idx_outbox_aggregate,
    DROP COLUMN updated_at,
    DROP COLUMN last_error,
    DROP COLUMN published_at,
    DROP COLUMN locked_until,
    DROP COLUMN locked_by,
    DROP COLUMN retry_count,
    DROP COLUMN headers_json;

-- ── farm_snapshots：回滚新增列和约束 ─────────────────────────────────────────
ALTER TABLE farm_snapshots
    DROP CONSTRAINT ck_farm_actor_state,
    DROP INDEX idx_farm_updated,
    DROP INDEX idx_farm_pet_action,
    DROP INDEX uk_farm_owner_user,
    DROP COLUMN checkpointed_at,
    DROP COLUMN next_pet_action_at,
    DROP COLUMN snapshot_checksum,
    DROP COLUMN schema_version,
    DROP COLUMN actor_state,
    DROP COLUMN route_epoch,
    DROP COLUMN last_event_seq,
    DROP COLUMN owner_user_id;

-- ── sessions：回滚新增列和索引 ───────────────────────────────────────────────
ALTER TABLE sessions
    DROP INDEX idx_sessions_expires,
    DROP INDEX idx_sessions_gateway_status,
    DROP INDEX uk_sessions_refresh_hash,
    DROP COLUMN revoked_at,
    DROP COLUMN last_seen_at,
    DROP COLUMN refresh_token_hash;

-- ── accounts：回滚新增列和索引 ───────────────────────────────────────────────
ALTER TABLE accounts
    DROP INDEX idx_accounts_status_updated,
    MODIFY COLUMN farm_id BIGINT UNSIGNED NOT NULL,
    DROP COLUMN deleted_at,
    DROP COLUMN last_login_at,
    DROP COLUMN row_version,
    DROP COLUMN profile_json,
    DROP COLUMN display_name;
