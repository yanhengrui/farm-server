-- Route 9.8: persisted player switch for pet auto-harvest.
-- DEFAULT 1 preserves the behavior of every existing purchased pet.
ALTER TABLE player_pets
    ADD COLUMN auto_harvest_enabled TINYINT(1) NOT NULL DEFAULT 1 AFTER status;
