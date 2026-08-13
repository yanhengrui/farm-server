-- 回滚：重新创建 social_invites 表（仅用于紧急回滚，业务代码已不再使用该表）。
CREATE TABLE IF NOT EXISTS social_invites (
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
