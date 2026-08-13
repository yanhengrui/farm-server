-- 回滚：恢复 owner_id 列。
ALTER TABLE farm_snapshots
    ADD COLUMN owner_id BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER farm_id,
    ADD KEY idx_farm_owner (owner_id);

-- 从 owner_user_id 回填
UPDATE farm_snapshots SET owner_id = owner_user_id;
