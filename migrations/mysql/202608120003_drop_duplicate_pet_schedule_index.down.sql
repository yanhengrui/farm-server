ALTER TABLE farm_snapshots
    ADD INDEX idx_farm_pet_action_due (next_pet_action_at, farm_id);
