-- 路线 8：宠物系统。
-- player_pets: 玩家宠物，一用户最多一只（UNIQUE user_id），购买即激活。
-- farm_snapshots.next_pet_action_at 已在路线 1 migration 中建好，无需重建。

CREATE TABLE player_pets (
    pet_id          BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    user_id         BIGINT UNSIGNED  NOT NULL,
    pet_type        VARCHAR(32)      NOT NULL DEFAULT 'CHICKEN',
    status          VARCHAR(16)      NOT NULL DEFAULT 'ACTIVE',
    purchased_at    DATETIME(3)      NOT NULL,
    PRIMARY KEY (pet_id),
    UNIQUE KEY uk_player_pet_user (user_id),
    CONSTRAINT ck_pet_status CHECK (status IN ('ACTIVE','INACTIVE'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
