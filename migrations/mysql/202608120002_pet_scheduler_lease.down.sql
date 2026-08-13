ALTER TABLE farm_snapshots
    DROP INDEX idx_farm_pet_action_due,
    DROP COLUMN pet_scan_lease_until,
    DROP COLUMN pet_scan_lease_owner;
