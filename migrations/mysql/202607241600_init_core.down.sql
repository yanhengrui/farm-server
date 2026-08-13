-- 骨架回滚。生产环境 destructive down 不默认执行（见 06-6）。
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS cmd_receipts;
DROP TABLE IF EXISTS farm_snapshots;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS accounts;
