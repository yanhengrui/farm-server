-- 路线 10.2 P0：将 economy_transactions 上运行时无代码依赖的两个二级索引设为 INVISIBLE。
--
-- 背景：
--   仓库运行时代码只通过 uk_economy_user_biz 做幂等读取，不使用下面两个索引。
--
-- 注意：INVISIBLE 只影响查询优化器（不再被 SELECT 选中），
--   INSERT / UPDATE 仍然维护这两个索引，写放大不会因此减少。
--   本迁移的唯一目的是：在生产环境安全观察一段时间（建议 1~2 周），
--   通过慢查询日志和 index_io 统计确认没有外部（运维/客服/BI）查询依赖它们。
--   确认无依赖后，再通过独立 migration 执行 DROP INDEX，届时才真正减少写放大。
--
--   INVISIBLE 是 MySQL 8.0 可在线完成的元数据操作，可秒级回滚（down migration 改回 VISIBLE）。

ALTER TABLE economy_transactions
    ALTER INDEX idx_economy_user_created   INVISIBLE,
    ALTER INDEX idx_economy_source_created INVISIBLE;
