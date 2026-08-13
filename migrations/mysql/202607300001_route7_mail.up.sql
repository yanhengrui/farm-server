-- 路线 7 §10.4：邮件系统 —— 站内信 + 附件。
-- mails: 站内信主体，支持多种 mail_type（FRIEND_ACCEPTED/STEAL_NOTIFY/SYSTEM）。
-- mail_attachments: 邮件附件（作物/金币等），领取后标记 claimed_at，不可重复领取。

CREATE TABLE mails (
    mail_id       BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    user_id       BIGINT UNSIGNED  NOT NULL,
    sender_id     BIGINT UNSIGNED  NULL COMMENT '发件人 user_id；NULL 表示系统邮件',
    mail_type     VARCHAR(32)      NOT NULL COMMENT 'FRIEND_ACCEPTED|STEAL_NOTIFY|SYSTEM',
    title         VARCHAR(128)     NOT NULL,
    content       VARCHAR(512)     NOT NULL DEFAULT '',
    status        VARCHAR(16)      NOT NULL DEFAULT 'UNREAD' COMMENT 'UNREAD|READ|DELETED',
    read_at       DATETIME(3)      NULL,
    expires_at    DATETIME(3)      NULL,
    created_at    DATETIME(3)      NOT NULL,
    PRIMARY KEY (mail_id),
    KEY idx_mails_user_status (user_id, status, created_at),
    CONSTRAINT ck_mail_status CHECK (status IN ('UNREAD','READ','DELETED')),
    CONSTRAINT ck_mail_type CHECK (mail_type IN ('FRIEND_ACCEPTED','STEAL_NOTIFY','SYSTEM'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE mail_attachments (
    attachment_id BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    mail_id       BIGINT UNSIGNED  NOT NULL,
    item_type     VARCHAR(16)      NOT NULL COMMENT 'CROP|SEED|COIN',
    item_id       BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT 'COIN 时为 0',
    quantity      BIGINT UNSIGNED  NOT NULL DEFAULT 1,
    claimed_at    DATETIME(3)      NULL,
    PRIMARY KEY (attachment_id),
    KEY idx_mail_attach_mail (mail_id),
    CONSTRAINT fk_mail_attach_mail FOREIGN KEY (mail_id) REFERENCES mails (mail_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
