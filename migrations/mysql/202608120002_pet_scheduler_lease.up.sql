-- Multi-workersvr safe pet-scheduler claims. Explicit UTF-8 prevents server
-- defaults from corrupting the Chinese column comments.
SET NAMES utf8mb4;

ALTER TABLE farm_snapshots
    ADD COLUMN pet_scan_lease_owner VARCHAR(128) NULL COMMENT '宠物扫描任务租约持有者' AFTER next_pet_action_at,
    ADD COLUMN pet_scan_lease_until DATETIME(6) NULL COMMENT '宠物扫描任务租约到期时间' AFTER pet_scan_lease_owner,
    ADD INDEX idx_farm_pet_action_due (next_pet_action_at, farm_id);
