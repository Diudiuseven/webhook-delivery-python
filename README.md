# Webhook 事件投递层

在现有 FastAPI 服务之上补充的一层**可靠事件推送与投递能力**：事件从产生、
匹配订阅、入队、重试、死信到补投的完整链路持久化在本地 SQLite 中，不依赖
任何外部服务（投递目标为进程内可注入的接收方或本地回环 HTTP）。

## 架构

```
POST /events ──► 事件 + 每个匹配订阅的投递任务在同一事务落库（pending）
                          │
              DeliveryEngine 调度循环（每 scheduler_interval）
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   到期任务领取       超时任务回收       耗尽任务死信
   （条件 UPDATE）   （claim timeout）  （retries_exhausted）
        │
   全局并发信号量 + 每订阅并发上限
        │
   签名（当前 active 密钥）→ 传输层投递
        │
   success / retryable / permanent
```

- **事件与投递任务同生共死**：`POST /events` 在一个数据库事务里写入事件和
  所有匹配订阅的投递任务，进程在提交与投递之间崩溃也不会丢事件；重启后
  遗留的 `pending` / `in_flight` 任务会被重新领取。
- **允许重复投递，但接收方必须去重**：每次投递携带固定的
  `X-Webhook-Delivery-Id`，接收方按此去重（进程内 `Receiver` 已实现）。
- **按订阅 FIFO**：每个有序键（默认即订阅 id）同一时刻只有一个在途任务，
  较早任务未到终态（成功/死信）前，较晚任务不会被投递；重试不会让较晚
  事件插队。不同有序键可并发（受每订阅并发上限约束）。
- **隔离与背压**：全局并发上限 + 每订阅在途上限，单个慢订阅无法占满全部
  投递能力。`GET /metrics` 提供积压判断依据：
  - 每订阅 `pending` 积压增长而全局 `inflight` 未饱和 → **接收方慢**；
  - 全局 `inflight` 打满且 `saturated=true` → **投递侧能力不足**。

## 重试策略

每次尝试的结果分为三类：

| 结果 | 触发 | 后续 |
| --- | --- | --- |
| `success` | 接收方 2xx | 任务成功 |
| `retryable` | 5xx / 超时 / 连接异常 / 接收方抛出可重试错误 | 退避后重试，直到最大次数 |
| `permanent` | 4xx / 签名错误 / 接收方抛出永久错误 | 不再重试，直接进死信 |

退避公式：`delay = min(base * factor^(attempt-1), max) * (1 ± jitter)`，
可配置项见下表。接收方响应缓慢或连接异常只会让该次尝试超时，不会拖垮
投递进程。

## 签名与密钥轮换

- 每次尝试都用**当前 active 密钥**重新签名（HMAC-SHA256，头
  `X-Webhook-Signature: v1=...`，签名内容为 `timestamp.body`），因此跨
  轮换点的重试会自动使用新密钥。
- 头部 `X-Webhook-Key-Id` 标识所用密钥；接收方接受状态为 `active` 或
  `rotated` 的密钥，拒绝 `retired` 密钥，并校验时间戳容差。
- 轮换：旧 active 密钥置为 `rotated`（过渡期内仍被接受），新密钥成为
  `active`。轮换不会导致在途/重试中的事件失效或丢失。

## 死信与重放

- 重试耗尽（`retries_exhausted`）或永久失败（`permanent_failure`）的任务
  进入死信存储，保留失败原因、最后错误与全部尝试记录（`attempts_json`）。
- 运维可按订阅与时间范围重放：`POST /admin/subscriptions/{id}/replay`。

### 重放语义

- **重放 = 一次新的真实投递**。重放不会复活旧的投递任务，而是为同一事件
  创建一条**全新的投递任务**（新的 delivery id、新的尝试预算），由正常
  调度器领取投递。接收方看到的是新的 `X-Webhook-Delivery-Id`，因此会把
  重放当作新事件处理，而不会与历史投递混淆后跳过。
- **去重边界**：同一条投递任务的重试、超时重投递、进程恢复产生的重复
  投递都携带相同的 delivery id，接收方必须按此去重（只处理一次）；
  只有重放产生的新投递任务才携带新 id、被当作新事件。
- **尝试计数**：每条投递任务的 `attempt_count` 独立从 0 重新计数；历史
  投递任务（含其全部尝试记录）原样保留，可通过
  `GET /events/{id}`（`replay_of` / `replay_job_id` 标明来源）与
  `GET /deliveries/{id}` 查询每次真实投递的结果与失败原因。
- **幂等重放**：同一条死信记录只会被重放一次（`replayed_at` 标记）。
  对同一时间范围重复或并发发起重放是安全的：选择与标记在同一事务内
  串行完成，并发重放的结果确定——恰好一个重放生效，接收方恰好多处理
  一次，不重复不丢失。事件再次失败进入死信会产生**新的死信记录**
  （新的 delivery id），可以被再次重放，循环任意多次都稳定。
- **终态保证**：重放产生的投递与正常投递走同一条状态机，必然到达
  `succeeded` 或 `dead_letter` 终态；重放失败（如存储故障）不会留下
  卡在中间态的任务，且会记录一条 `failed` 状态的重放任务。
- **背压**：重放产生的投递同样受全局并发上限与每订阅并发上限约束，
  大批量重放不会压垮投递能力，也不会让单个订阅挤占全部容量。
- 每次重放生成一条 `replay_jobs` 记录（范围、数量、操作者、状态、
  失败详情），可通过 `GET /admin/replay-jobs` 查询；重叠范围或成批
  重放按 job 区分，成功与失败均可追溯。

## 权限与审计

- `X-API-Key` 认证：`viewer` 可查询死信、重放任务与指标，`admin` 可
  重放、轮换密钥、查看审计。
- 越权访问返回 401/403 并写入审计日志（`GET /admin/audit`，admin 可查），
  所有允许/拒绝操作均可追溯。

## 重启恢复

- 进程启动时，所有 `in_flight` 任务（定义上都是孤儿）被重置为 `pending`
  并记录一条 `interrupted` 尝试。
- 运行期周期性回收领取后超过 `claim_timeout` 未完成的任务，任何事件都不会
  永久卡死。
- 事件当前状态可通过 `GET /events/{event_id}`（含每个投递任务状态）与
  `GET /deliveries/{delivery_id}`（含全部尝试记录）查询。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `WEBHOOK_DB_PATH` | `webhook.db` | SQLite 路径 |
| `WEBHOOK_MAX_ATTEMPTS` | `5` | 最大尝试次数 |
| `WEBHOOK_BACKOFF_BASE` | `1.0` | 退避基数（秒） |
| `WEBHOOK_BACKOFF_FACTOR` | `2.0` | 退避指数因子 |
| `WEBHOOK_BACKOFF_MAX` | `60.0` | 退避上限（秒） |
| `WEBHOOK_JITTER_RATIO` | `0.2` | 抖动比例（±） |
| `WEBHOOK_DELIVERY_TIMEOUT` | `10.0` | 单次投递超时（秒） |
| `WEBHOOK_GLOBAL_CONCURRENCY` | `10` | 全局并发上限 |
| `WEBHOOK_PER_SUB_CONCURRENCY` | `1` | 每订阅在途上限 |
| `WEBHOOK_CLAIM_TIMEOUT` | `30.0` | 领取超时（秒） |
| `WEBHOOK_SCHEDULER_INTERVAL` | `0.1` | 调度间隔（秒） |
| `WEBHOOK_SIGNATURE_TOLERANCE` | `300.0` | 签名时间戳容差（秒） |
| `WEBHOOK_ADMIN_KEY` | `admin-dev-key` | 管理员 API Key |
| `WEBHOOK_VIEWER_KEY` | `viewer-dev-key` | 只读 API Key |

## HTTP API

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| GET | `/` | - | 原有接口（Hello World） |
| POST | `/events` | - | 产生事件（`event_type`, `payload`, 可选 `ordering_key`） |
| GET | `/events/{id}` | - | 查询事件及投递状态 |
| POST | `/subscriptions` | - | 创建订阅（`name`, `target`, `event_types`） |
| GET | `/subscriptions` `/subscriptions/{id}` | - | 查询订阅 |
| GET | `/deliveries/{id}` | - | 查询投递任务与尝试记录 |
| GET | `/metrics` | - | 积压/并发/死信指标 |
| GET | `/admin/dead-letters` | viewer | 死信查询（可按订阅/时间范围过滤） |
| POST | `/admin/subscriptions/{id}/replay` | admin | 按订阅与时间范围重放死信 |
| GET | `/admin/replay-jobs` | viewer | 重放任务记录（范围/数量/状态/失败详情） |
| POST | `/admin/keys/rotate` | admin | 轮换签名密钥 |
| POST | `/admin/keys/{id}/retire` | admin | 退役密钥（拒绝其签名） |
| GET | `/admin/keys` | viewer | 密钥列表 |
| GET | `/admin/audit` | admin | 审计日志 |

投递目标 `inprocess://<name>` 对应进程内注册的接收方；其余按本地回环
HTTP 目标处理。

## 测试

```bash
pytest tests/ -q
```

覆盖：崩溃不丢事件、订阅匹配、可重试失败后成功、永久失败死信、重试耗尽、
按订阅 FIFO、重试不插队、键级并发隔离、慢订阅不阻塞、超时重投递去重、
密钥轮换（含跨重试轮换、退役拒绝）、启动回收、周期回收、指标积压；
死信重放：重放作为新事件真正送达（不判重）、重放的重试仍去重、连续
重放与二次进入死信的稳定性、同范围并发重放的确定性、重放与正常投递
交织、重放中重启恢复、批量重放的并发上限、失败重放记录、越权重放与
审计、旧库 schema 迁移。

### 验证重放行为

1. 产生事件并让接收方永久失败 → `GET /events/{id}` 可见 `dead_letter`。
2. 修复接收方后 `POST /admin/subscriptions/{id}/replay`（admin key）→
   响应含 `job_id` 与新的 `delivery_ids`。
3. `GET /events/{id}` 可见两条投递：旧的 `dead_letter`（历史完整）与
   新的 `succeeded`（`replay_of` 指向旧投递，`attempt_count` 重新计数）。
4. 接收方侧可按 `X-Webhook-Delivery-Id` 确认：重放携带新 id，被当作
   新事件处理；`GET /admin/replay-jobs` 与 `GET /admin/audit` 可追溯
   本次重放。
