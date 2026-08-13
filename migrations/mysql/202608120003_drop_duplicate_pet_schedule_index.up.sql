-- farm_snapshots already has idx_farm_pet_action(next_pet_action_at, farm_id)
-- from the base schema. Remove the duplicate added by the lease migration.
ALTER TABLE farm_snapshots DROP INDEX idx_farm_pet_action_due;
