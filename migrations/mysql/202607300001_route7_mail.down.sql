-- 路线 7 §10.4 回滚：删除邮件相关表。
-- 先删子表再删主表（外键约束）。
DROP TABLE IF EXISTS mail_attachments;
DROP TABLE IF EXISTS mails;
