-- Route 9.5: make a fresh, filename-ordered migration run match the schema
-- expected by the application.
--
-- 202607271200_drop_farm_snapshots_owner_id.up.sql was intentionally recorded
-- as a no-op after the development database had already been changed by hand.
-- A new database therefore retained owner_id NOT NULL, while every current
-- INSERT writes only owner_user_id.  Apply the missing DDL conditionally so
-- this roll-forward is also safe on databases where owner_id is already gone.

SET @route95_owner_id_exists = (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'farm_snapshots'
      AND column_name = 'owner_id'
);

SET @route95_drop_owner_id = IF(
    @route95_owner_id_exists > 0,
    'ALTER TABLE farm_snapshots DROP COLUMN owner_id',
    'DO 0'
);

PREPARE route95_drop_owner_id_stmt FROM @route95_drop_owner_id;
EXECUTE route95_drop_owner_id_stmt;
DEALLOCATE PREPARE route95_drop_owner_id_stmt;
