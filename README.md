# farm-server

经典农场小游戏后端。仓库包含四服务应用代码、协议、数据库迁移、测试与压测工具；部署镜像、Kubernetes、Compose 等基础设施
另建独立仓库维护（参考 rx-server 与 infra-test 的关系）。

## 架构基线

- 方案：`经典农场小游戏-三周架构与交付方案-v1.2.md`
- 开发基线文档：`经典农场小游戏-开发基线文档-v1.0/`
- 四个部署单元、五类逻辑边界（见 ADR-001）：
  - **gatesvr** — HTTP/WSS 接入、认证限流、全局单连接、`client_seq/server_seq + ACK`、连接层 handoff。断线后重新拉 Snapshot，不回放 EVENT；不持有 Farm Actor，不写权威业务事实。
  - **farmsvr** — Farm Router + Farm Actor 逻辑串行、预校验、热快照、patch/广播编排、checkpoint/epoch/lease/fencing。不直写 MySQL 业务事实。
  - **gamesvr** — 唯一 MySQL 权威持久化提交层。接收 `CommitFarmCommand`，在单事务内提交农场快照、库存/钱包、经济流水、`cmd_receipts`、`outbox_events`（见 ADR-017）。
  - **workersvr** — Outbox Relay 发布 Kafka、Kafka 消费者、调度、重试/死信/清理（见 ADR-016）。

## 关键约束

- **唯一持久化提交点**：farmsvr 只做串行与预校验，资产事实统一由 gamesvr 单事务提交，避免跨服务双写。
- **持久幂等**（ADR-018）：经济命令使用 `economy_transactions` 自然业务唯一键；Plant/Harvest/Steal/PetAutoHarvest 等资产命令使用 MySQL `cmd_receipts`；外部农场命令同时受 `base_version` 乐观并发控制。
- **逻辑 Actor + 固定执行分片**：`hash(farm_id) % N` 分片 + 有界邮箱，单 farm 单 in-flight，禁止一农场一常驻 goroutine。
- **作物成长惰性计算**：只存 `planted_at/mature_at`，禁止每地块定时器。

## 目录

```
server/              Go module（github.com/photon/farm-server/server）
  cmd/               gatesvr / farmsvr / gamesvr / workersvr 启动入口
  contracts/         WebSocket、领域事件及共享 Go 契约
  proto/             HTTP/gRPC 双栈的 protobuf 源文件
  gen/               protobuf 生成代码
  internal/          account / catalog / economy / farm / gateway / mail / pet / social / task / worker
  pkg/               app / config / discovery / errcode / logging / observability / Redis / gRPC 等共享能力
migrations/mysql/    只追加迁移（受控执行，禁止应用启动时自动 migrate）
loadtest/            路线 9 基线/正确性、路线 10 单机优化及路线 11 集群验收工具与报告
doc/                 架构和开发基线文档
.ai-rules/           当前状态、工作流和工程约束
```

## 开发环境

- Go 版本：**1.26.3**，与 `server/go.mod` 声明一致。

## 本地运行

```bash
cd server
cp .env.example .env   # 按需修改；不要提交 .env

# 分别以不同 SERVICE_NAME 启动四个进程（示例）
APP_ENV=local SERVICE_NAME=gamesvr  MYSQL_DSN=<local> go run ./cmd/gamesvr
APP_ENV=local SERVICE_NAME=farmsvr  go run ./cmd/farmsvr
APP_ENV=local SERVICE_NAME=gatesvr  go run ./cmd/gatesvr
APP_ENV=local SERVICE_NAME=workersvr MYSQL_DSN=<local> go run ./cmd/workersvr

# 健康检查
curl http://127.0.0.1:9091/live
curl http://127.0.0.1:9091/ready
```

## 测试

```bash
cd server
go build ./...
go test ./...
go test -race ./internal/farm/...   # 并发/Actor 相关
```

## 当前状态

- 路线 1–8.5 已完成：经济闭环、社交/邮件、任务/图鉴/宠物、完整 migration 与 HTTP/gRPC 双栈均已落地。
- 路线 9 已按“真实链路、正确性、故障恢复、容量基线和首瓶颈定位”完成：最新 Economy 稳定点约 2k accepted/s，3k/s 首先暴露共享 MySQL 事务/索引/提交与并发拐点；这不是最终业务容量承诺。
- 当前进入路线 10：只做单机/单 MySQL 分片优化。当前先完成事务观测代码、静态依赖审计、无效索引/宽流水/重复查询/无消费者事件等确定性瘦身，并准备固定大表材料；此阶段不重跑远端正式压测。设计实现收口后才在同一快照上集中执行一次前后短 A/B，候选达标后再跑一次完整阶梯和长稳。
- 路线 11 承担多服务器集群、MySQL 分片、跨用户事务语义、Redis/Kafka 高可用及最终容量验收。当前 30M DAU 模型建议 30k accepted/s 稳定、40k offered/s 短时突发，最终以路线 11 冻结模型为准。
- 其他产品/生产接入项：好友邀请 deeplink 正式域名、可信代理链，以及跨不可信网络时的 gRPC TLS/mTLS。

权威任务状态和阻塞原因以 [`.ai-rules/STATUS.md`](.ai-rules/STATUS.md) 为准。
