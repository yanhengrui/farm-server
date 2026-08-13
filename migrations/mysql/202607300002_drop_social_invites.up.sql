-- 路线 7.5：邀请码迁移到 Redis，social_invites 表不再使用。
-- 邀请码改为 Redis TTL 存储（invite:{code} + invite:user:{uid}，各 30min）。
DROP TABLE IF EXISTS social_invites;
