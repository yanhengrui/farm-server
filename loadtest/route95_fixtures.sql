-- Route 9.5 repeatable capacity fixtures.
--
-- Required caller variable (include the SQL LIKE wildcard):
--   SET @route95_device_prefix = 'route95-cap-<run-id>-%';
--
-- This script refuses any database except farm_route95 and any prefix outside
-- the route95 namespace. It changes setup data only; load-time transactions
-- must still travel through the public gatesvr protocol.

DROP PROCEDURE IF EXISTS route95_fixture_guard;
DELIMITER //
CREATE PROCEDURE route95_fixture_guard()
BEGIN
    IF DATABASE() <> 'farm_route95' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'route95 fixtures require farm_route95';
    END IF;
    IF @route95_device_prefix IS NULL OR @route95_device_prefix NOT LIKE 'route95-%' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'route95 fixture device prefix is required';
    END IF;
END//
DELIMITER ;

CALL route95_fixture_guard();
DROP PROCEDURE route95_fixture_guard;

-- A large but finite balance/inventory prevents fixture exhaustion during a
-- 60-minute Purchase/Sell workload. These rows remain ordinary authoritative
-- tables and every load-time mutation still uses the gamesvr transaction.
UPDATE wallets AS w
JOIN auth_identities AS ai ON ai.user_id = w.user_id
SET w.coin_balance = 1000000000,
    w.row_version = w.row_version + 1,
    w.updated_at = UTC_TIMESTAMP(3)
WHERE ai.provider = 'guest'
  AND ai.provider_subject LIKE @route95_device_prefix;

INSERT INTO inventory_items
    (user_id, item_type, item_id, quantity, row_version, created_at, updated_at)
SELECT ai.user_id, fixture.item_type, 1, 1000000000, 1, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)
FROM auth_identities AS ai
JOIN (
    SELECT 'SEED' AS item_type
    UNION ALL
    SELECT 'CROP' AS item_type
) AS fixture
WHERE ai.provider = 'guest'
  AND ai.provider_subject LIKE @route95_device_prefix
ON DUPLICATE KEY UPDATE
    quantity = 1000000000,
    row_version = row_version + 1,
    updated_at = UTC_TIMESTAMP(3);

-- Four plots were planted more than ten minutes ago and are now mature. Four
-- more retain the production ten-minute growth duration but mature at spread
-- times during the run. This does not alter crop configuration or clocks.
UPDATE farm_snapshots AS fs
JOIN auth_identities AS ai ON ai.user_id = fs.owner_user_id
SET fs.snapshot = JSON_SET(
        fs.snapshot,
        '$.plots."1".status', 'GROWING', '$.plots."1".crop_id', 'WHEAT',
        '$.plots."1".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 12 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."1".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 2 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."1".remaining_yield', 5, '$.plots."1".watered_count', 2,
        '$.plots."2".status', 'GROWING', '$.plots."2".crop_id', 'WHEAT',
        '$.plots."2".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 11 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."2".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 1 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."2".remaining_yield', 5, '$.plots."2".watered_count', 2,
        '$.plots."3".status', 'GROWING', '$.plots."3".crop_id', 'WHEAT',
        '$.plots."3".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 13 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."3".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 3 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."3".remaining_yield', 5, '$.plots."3".watered_count', 2,
        '$.plots."4".status', 'GROWING', '$.plots."4".crop_id', 'WHEAT',
        '$.plots."4".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 14 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."4".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 4 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."4".remaining_yield', 5, '$.plots."4".watered_count', 2,
        '$.plots."5".status', 'GROWING', '$.plots."5".crop_id', 'WHEAT',
        '$.plots."5".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 7 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."5".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) + INTERVAL 3 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."5".remaining_yield', 5, '$.plots."5".watered_count', 1,
        '$.plots."6".status', 'GROWING', '$.plots."6".crop_id', 'WHEAT',
        '$.plots."6".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 5 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."6".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) + INTERVAL 5 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."6".remaining_yield', 5, '$.plots."6".watered_count', 1,
        '$.plots."7".status', 'GROWING', '$.plots."7".crop_id', 'WHEAT',
        '$.plots."7".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 3 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."7".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) + INTERVAL 7 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."7".remaining_yield', 5, '$.plots."7".watered_count', 0,
        '$.plots."8".status', 'GROWING', '$.plots."8".crop_id', 'WHEAT',
        '$.plots."8".planted_at', DATE_FORMAT(UTC_TIMESTAMP(3) - INTERVAL 1 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'),
        '$.plots."8".mature_at', DATE_FORMAT(UTC_TIMESTAMP(3) + INTERVAL 9 MINUTE, '%Y-%m-%dT%H:%i:%s.000Z'), '$.plots."8".remaining_yield', 5, '$.plots."8".watered_count', 0,
        '$.plots."9".status', 'EMPTY', '$.plots."9".crop_id', '', '$.plots."9".planted_at', NULL, '$.plots."9".mature_at', NULL, '$.plots."9".remaining_yield', 0, '$.plots."9".watered_count', 0,
        '$.plots."10".status', 'EMPTY', '$.plots."10".crop_id', '', '$.plots."10".planted_at', NULL, '$.plots."10".mature_at', NULL, '$.plots."10".remaining_yield', 0, '$.plots."10".watered_count', 0,
        '$.plots."11".status', 'EMPTY', '$.plots."11".crop_id', '', '$.plots."11".planted_at', NULL, '$.plots."11".mature_at', NULL, '$.plots."11".remaining_yield', 0, '$.plots."11".watered_count', 0,
        '$.plots."12".status', 'EMPTY', '$.plots."12".crop_id', '', '$.plots."12".planted_at', NULL, '$.plots."12".mature_at', NULL, '$.plots."12".remaining_yield', 0, '$.plots."12".watered_count', 0
    ),
    fs.updated_at = UTC_TIMESTAMP(3)
WHERE ai.provider = 'guest'
  AND ai.provider_subject LIKE @route95_device_prefix;

SELECT COUNT(*) AS fixture_users
FROM auth_identities
WHERE provider = 'guest'
  AND provider_subject LIKE @route95_device_prefix;
