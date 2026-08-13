# Route 9.5 长稳容量验收报告

最终状态：**PARTIAL**

本轮完成了专用环境、全量 migration、真实 MySQL/Redis/Kafka/etcd 链路、独占 workersvr/Relay/consumer、双 gatesvr 正确性 E2E、1/2/4/8 应用实例实验、30 分钟有状态长稳以及独占 Kafka outage/recovery。正式 30 分钟档位只有 649.91 accepted commands/s，低于 PASS 所需的 10,000/s；5,000/s 逐级探测时，512 MiB 限额的单 gatesvr 在 warm-up 被 OOM kill。因此禁止标记 PASS。

## 身份与适用范围

- 仓库：`/data/code/farm-server`
- 分支：`main`
- 基线 commit：`a276922954cd30a5de08d13c7ad5256513a3ced7`
- 开始时工作区：clean；报告时：dirty（均为本次路线 9.5 修改，未 commit/push）
- 预检：`READY_WITH_LIMITS`
- 限制：负载发生器与所有被测服务位于同一台 16 vCPU / 32.85 GB 主机。本报告只证明单机资源池容量，不证明跨主机高可用或线性扩展。

完整环境、版本、磁盘和资源限制见 `environment.json`、`topology.json`；配置见脱敏的 `config-sanitized.json`。

## 专用拓扑与迁移

使用 `route95-mysql`（全新 `farm_route95`）、`route95-redis`、`route95-kafka`、`route95-etcd`、唯一 `route95-workersvr` 及名称带 route95 的应用实例。Kafka topic 为 `route95-events-a276922`，consumer group 使用专用前缀；etcd prefix 为 `/farm-server/route95/a276922-20260803`。未修改或停止任何非 route95 容器、进程、数据库或卷。

按文件名顺序执行了全部 MySQL `*.up.sql`，未执行 down migration。全部文件的 SHA256 在 `migration-checksums.json`。结构检查确认 accounts、farm_snapshots、wallets、inventory_items、economy_transactions、cmd_receipts、outbox_events、consumed_events、consumer_failed_events、friendships、mails、mail_attachments、player_tasks、catalog_unlocks、player_pets 均存在；`player_pets.auto_harvest_enabled`、Outbox 状态/锁/非空时间字段有效；`remaining_yield` 位于 snapshot JSON，兼容 migration 与反序列化逻辑通过。

发现历史 migration 在全新 schema 上保留了 `farm_snapshots.owner_id NOT NULL`，已新增条件 migration `202608030004_drop_legacy_farm_owner_id.up.sql` 修正，未手工绕过 migration。

## 质量与正确性门禁

下列门禁均通过：

- `make check`
- `make route9-acceptance`（含 race）
- `make route9-soak`：60.001 秒，内存 scheduler 19,996 commands/s，1,199,800 成功、0 拒绝；仅作 scheduler 回归，不作为正式容量证据
- 专用 MySQL 上 `make route9-mysql`：ACK-loss 与 epoch fence 通过
- 专用连接执行 `make route9-external`：MySQL、Redis Pub/Sub、Kafka 发布/消费通过
- 两个 gatesvr 执行 `route95_e2e.go`：ACK-loss 重放、跨 gateway EVENT、完整 Patch 通过
- stateful driver 参数、farm 独立版本、cmd_id 唯一/重放、错误分类、统计去重、取消和优雅停止测试通过
- `bash -n loadtest/route95_kafka_outage.sh`、格式检查与 `git diff --check` 通过

正确性最终计数全部为 0：状态分叉、重复资产结算、farm_version 倒退、未授权成功、负 remaining_yield、幂等重放重复事件、永久消费失败。数据库重复 receipt、经济流水、消费记录、任务进度、图鉴记录以及负钱包/库存审计均为 0。详见 `correctness.json`。

## 工作负载

所有负载通过公开 gatesvr HTTP/WebSocket 协议进入，不直接调用内部数据库提交接口。使用 5,000 个独立用户/农场，各 farm 独立维护 base_version、client_seq 和稳定合法 cmd_id；只有 ACK-loss 场景复用 cmd_id，重放不重复计 accepted。

扩展档命令比例为 Purchase 47%、Sell 47%、Plant 2%、Water 2%、Harvest 2%；长稳档因作物成熟周期限制使用 Purchase 49%、Sell 49%、Plant 1%、Water 1%，未缩短生产成熟时间。观看者覆盖普通 farm 1–4 与热点 farm 1/2/4/8/20。每用户限流保持 10/s、burst 20；只将同机 loadgen 的 IP 限流从 50/100 调为 20,000/40,000，并记录在脱敏配置中。

## 容量结果

最高满足 30 分钟定义的稳定档位为 **649.91 accepted commands/s，持续 1,800.02 秒**：attempted 1,169,850，accepted 1,169,850，rejected 0，failed 0，p50/p95/p99 为 16.625/46.317/129.039 ms。另有 60 秒 1,000/s 探针达到 999.38 accepted/s（60,000/60,000，0 拒绝、0 失败），但不能替代 30–60 分钟正式证据。

逐级探测到 5,000/s 时，单 `route95-gatesvr-1` 在 warm-up 阶段触发 512 MiB 容器 OOM，进程退出码 137；随后测量阶段因服务不存在而 accepted 为 0。依据停止条件，未继续 10k/20k/30k/50k。首个容量瓶颈是 **单 gatesvr 在 5k/s offered load 下的内存/OOM**，不是 Relay。

### 1/2/4/8 实例

每档的 gamesvr/farmsvr/gatesvr 实例数相同，保持 650/s 相同 offered workload、同一 MySQL/Redis/Kafka、200 总 MySQL 连接预算，并固定单 workersvr。

| 每类应用实例数 | accepted/s | p95 ms | p99 ms | 扩展效率 | 备注 |
|---:|---:|---:|---:|---:|---|
| 1 | 649.782 | 42.011 | 91.706 | 1.0000 | 60.02 s，0 错误 |
| 2 | 649.773 | 59.114 | 142.266 | 0.5000 | 60.02 s，0 错误 |
| 4 | 649.676 | 44.112 | 91.706 | 0.2500 | 60.02 s，0 错误 |
| 8 | 649.235 | 48.633 | 96.291 | 0.1249 | 28.83 s，停止信号导致 15 个在途请求失败 |

效率按 `throughput_N / (N × throughput_1)` 计算。由于 offered load 固定在 650/s 且所有实例共享同一物理主机与中间件，这组结果验证了部署、路由和功能，但不能证明或否定跨主机线性扩展。8 实例档因同机资源竞争提前停止，按 PARTIAL 保留原始数据。

## Outbox Relay

原实现每秒扫描一次、batch 50 且逐消息同步发布；500 条事件实测约 510 秒（约 0.98 events/s）。改为可配置 batch/poll、积压连续 drain、Kafka batch publish，并将 PetScanner 与 Relay 调度解耦后，500 条为 94 ms（约 5,319 events/s）。长稳期间 23,408 个事件 create→published 延迟 p50/p95/p99/max 为 67/117/148/386 ms，正常结束 PENDING 0、consumer lag 0。

历史低 admission 基线产生过 155 次可重试 task projector 错误；最终长稳和 outage 期间该计数未增长，记录均已恢复，`consumer_failed_events=0`。Kafka outage 期间出现 820 次预期 producer error，恢复后全部收敛。

## Kafka outage/recovery

只停止 `route95-kafka`，未操作其他 broker。有效演练于 2026-08-04T03:05:22Z 开始，停 broker 30 秒，03:07:59Z 恢复并完成收敛，共 127 秒。期间 10 分钟业务负载为 299.97 accepted/s，179,991/179,991 accepted，0 rejected、0 failed、0 version error。

Outbox 峰值为 948（PENDING 690、PUBLISHING 258），最老事件 30 秒；业务事务持续提交，无事件被错误标记 PUBLISHED。恢复后 PENDING/PUBLISHING/DEAD 均为 0，4 个专用消费组、64 个 partition 汇总 lag 为 0，consumer_failed_events 为 0，任务/图鉴/邮件无重复。未发现非 route95 consumer 消费测试 topic。详见 `kafka-outage.json`。

## 修改

- 新增真实有状态 gatesvr 压测驱动、fixtures、gRPC health 与 Kafka outage runner，并增加 Makefile 入口和 README。
- 为 Kafka topic、consumer group prefix、Relay batch/poll/drain 与 PetScanner poll 增加默认兼容配置和单测。
- Relay 增加积压连续 drain、Kafka batch publish、安全部分失败处理，并解耦 PetScanner 调度；保留 Outbox 至少一次、锁租约、状态语义与 consumer dedup。
- 修正全新 MySQL schema 的 legacy owner_id migration。
- 修正 E2E 生成合法 UUIDv7 cmd_id，增强外部 Kafka 测试的 consumer assignment 等待。
- 新增本报告目录和脱敏证据。未开展路线 10 工作，未 commit/push。

## 判定、风险与下一步

判定为 **PARTIAL**：实验链路和故障演练完成且正确性为 0，但正式稳定 throughput 低于 10k/s，8 实例档受单机资源限制。

剩余风险：单 gatesvr 5k/s warm-up 在 512 MiB 下 OOM；1/2/4/8 只在恒定 650/s offered load 上验证且 8 档不足 60 秒；loadgen 与服务共享 CPU/内存/磁盘；物理 SSD 属性和 NIC 带宽不可见；目前只有单 broker/单 worker 的恢复证据。

最小下一步是先用 heap/profile 和容器内存曲线定位 gatesvr 5k/s warm-up 分配源，在不放宽正确性与权威事务的前提下修复或据实调整内存预算；随后用独立负载机逐级重跑 1k→5k→10k，并在每个 1/2/4/8 档寻找各自稳定拐点、至少稳定 30 分钟。达到 10k accepted state-changing commands/s 且所有异步投影收敛后，才能将路线 9.5 改为 PASS。
