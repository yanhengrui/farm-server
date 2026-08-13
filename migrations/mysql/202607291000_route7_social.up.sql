-- 路线 7 §10.2：好友系统 —— 邀请码 + 双向好友关系表。
-- social_invites: 邀请码有效期 7 天，每码限用一次。
-- friendships: 规范化存储，user_id_a < user_id_b，单行代表双向好友关系。

CREATE TABLE social_invites (
    invite_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    invite_code     VARCHAR(64)     NOT NULL,
    inviter_user_id BIGINT UNSIGNED NOT NULL,
    used_by_user_id BIGINT UNSIGNED NULL,
    status          VARCHAR(16)     NOT NULL DEFAULT 'PENDING',
    expires_at      DATETIME(3)     NOT NULL,
    used_at         DATETIME(3)     NULL,
    created_at      DATETIME(3)     NOT NULL,
    PRIMARY KEY (invite_id),
    UNIQUE KEY uk_social_invite_code (invite_code),
    KEY idx_social_inviter_status (inviter_user_id, status),
    CONSTRAINT ck_social_invite_status CHECK (status IN ('PENDING','USED','EXPIRED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE friendships (
    friendship_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id_a     BIGINT UNSIGNED NOT NULL,
    user_id_b     BIGINT UNSIGNED NOT NULL,
    created_at    DATETIME(3)     NOT NULL,
    PRIMARY KEY (friendship_id),
    UNIQUE KEY uk_friendships_pair (user_id_a, user_id_b),
    KEY idx_friendships_b (user_id_b),
    CONSTRAINT ck_friendships_order CHECK (user_id_a < user_id_b)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
