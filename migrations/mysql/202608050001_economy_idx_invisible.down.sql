-- 回滚：将两个索引恢复为 VISIBLE，优化器重新可选择它们。
ALTER TABLE economy_transactions
    ALTER INDEX idx_economy_user_created   VISIBLE,
    ALTER INDEX idx_economy_source_created VISIBLE;
