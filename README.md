# webhook-delivery

在原有 FastAPI 服务（`GET /` 行为不变）之上，补充一层可靠的事件推送与投递能力：
事件从产生到送达、重试、死信与补投的完整链路。投递目标使用本地回环地址或
进程内注入的传输层，不依赖任何外部服务。

## 架构

```
POST /events ──► Storage.enqueue_event ──► deliveries(pending)
                 (事件+匹配投递同一事务)         │
                                  Dispatcher.claim (租约 + 顺序门控)
                                                 │
                                  Transport.post (签名 + 单次超时)
                                                 │
                    ┌──────────────┬─────────────┴──────────────┐
                 2xx 成功      可重试失败(5xx/超时/连接异常)   永久失败(4xx)
                    │            退避+抖动后重新入队             │
                delivered      次数耗尽 ──► dead_letters ◄──────┘
                                          │ POST /dead-letters/replay (鉴权+审计)
                                          ▼
                                     新投递(尝试数归零)
```

模块：`app/models.py` 数据模型 · `app/storage.py` SQLite 持久化与状态机 ·
`app/dispatcher.py` 调度（并发/顺序/租约）· `app/retry.py` 重试策略 ·
`app/signing.py` HMAC 签名与轮换 · `app/transport.py` 可注入传输层 ·
`app/auth.py` 鉴权 · `app/api.py` HTTP 接口。

## 投递语义

- **不丢事件**：事件与匹配到的订阅投递在同一事务提交；提交与投递之间崩溃，
  重启后 `pending` 事件会被重新领取投递。
- **允许重复、可去重**：每次投递携带 `X-Event-Id`（幂等键）与 `X-Delivery-Id`，
  接收方按 `X-Event-Id` 去重。租约超时重领、迟到完成（token 失效被拒）都不会
  产生重复生效。
- **至少一次（at-least-once）**：只有收到 2xx 才标记 `delivered`。
- **按订阅有序**：同一订阅的投递按入队序号串行放行，较早事件未终态（成功/死信）
  前，较晚事件不会被领取；较早事件的重试会挡住较晚事件。订阅之间互不影响。

## 重试参数（`RetryPolicy`）

| 参数 | 默认 | 说明 |
|---|---|---|
| `max_attempts` | 5 | 最大尝试次数（含首次），耗尽进入死信 |
| `base_delay` | 0.1s | 首次重试基础间隔 |
| `backoff_multiplier` | 2.0 | 指数退避倍数 |
| `max_delay` | 30s | 退避上限 |
| `jitter` | 0.2 | 抖动比例，在 ±20% 内随机扰动 |
| `timeout` | 5s | 单次投递超时，慢接收方不会拖垮进程 |
| `lease_seconds` | 30s | 领取租约，超时未完成可被重新领取 |

结果分类：2xx 成功；4xx（除 408/429）永久失败，不再重试；5xx/408/429/超时/
连接异常为可重试失败。

## 并发与背压

- 全局并发上限（`Dispatcher(global_concurrency=...)`）+ 按订阅并发上限
  （订阅的 `max_concurrency`）；有序订阅有效并发为 1。
- `GET /metrics` 返回：`by_status` 各状态计数、`per_subscription` 每订阅
  待投递数与最老积压年龄、`dispatcher.inflight/available_slots`。
  `inflight` 打满且积压增长 → 接收方慢或容量不足；`available_slots` 充裕而
  积压增长 → 检查退避中的失败订阅。

## 签名与密钥轮换

- 请求头：`X-Signature-Timestamp`、`X-Signature: t=<ts>,kid=<kid>,v1=<hmac>`，
  签名内容 `HMAC_SHA256(secret, "<ts>." + body)`。
- 接收方用任一有效期内密钥验签，并校验时间戳容差（默认 300s）防重放。
- `POST /keys/rotate`（需 `keys:rotate` 权限）：新密钥成为唯一签名密钥，
  旧密钥在 `grace_seconds` 过渡期内仍可验签。每次重试现场签名，跨越轮换点
  自动改用新密钥，重试中的事件不失效、不丢失。

## 死信与补投（运维操作）

所有运维操作需 `X-Admin-Token`（或 `Authorization: Bearer`）鉴权，
允许与拒绝均写入审计日志（`GET /audits`，需 `audit:read`）。

```bash
# 查询死信（需 deadletters:read），支持按订阅与时间范围过滤
curl -H 'X-Admin-Token: ...' \
  'http://127.0.0.1:8000/dead-letters?subscription_id=s1&since=0&until=9999999999'

# 重放（需 replay）：按 ID 列表，或按订阅+时间范围
curl -X POST -H 'X-Admin-Token: ...' -H 'Content-Type: application/json' \
  -d '{"delivery_ids": ["..."]}' http://127.0.0.1:8000/dead-letters/replay
curl -X POST -H 'X-Admin-Token: ...' -H 'Content-Type: application/json' \
  -d '{"subscription_id": "s1", "since": 0}' http://127.0.0.1:8000/dead-letters/replay
```

重放语义：以原事件生成新投递（`replay_of` 指向原投递），尝试次数从零开始、
不累计原次数；新投递获得订阅内新序号，与正常投递共用同一事务序号分配，
并发重放结果确定。原死信记录保留全部尝试历史并标记 `replayed_at`。

## 重启恢复

- 启动时 `reset_claims_on_startup`：崩溃遗留的 `claimed` 投递回到 `pending`。
- 运行中租约超时未完成的投递由 `release_expired_claims` 重新放出。
- 任何事件的状态（`pending/claimed/delivered/dead`）都可经
  `GET /deliveries/{id}`（含全部尝试记录）查询，不存在永久卡死状态。

## 接口一览

| 方法/路径 | 说明 | 权限 |
|---|---|---|
| `GET /` | 原有接口，行为不变 | - |
| `POST /subscriptions` · `GET /subscriptions` | 订阅管理 | - |
| `POST /events` | 事件入队（同事务生成匹配投递） | - |
| `GET /deliveries/{id}` | 投递状态与尝试记录 | - |
| `GET /subscriptions/{id}/deliveries` | 订阅的投递列表 | - |
| `GET /dead-letters` | 死信查询 | `deadletters:read` |
| `POST /dead-letters/replay` | 死信重放 | `replay` |
| `POST /keys/rotate` | 签名密钥轮换 | `keys:rotate` |
| `GET /metrics` | 积压与调度指标 | - |
| `GET /audits` | 审计日志 | `audit:read` |

## 运行与测试

```bash
uv run python main.py        # 启动服务（SQLite 文件 webhook.db，可用 WEBHOOK_DB 覆盖）
uv run pytest -q             # 全部测试（26 个）
```

测试覆盖：崩溃不丢事件、部分重试失败、永久失败不重试、重试耗尽死信、
按订阅顺序与故障隔离、密钥轮换跨重试、死信查询/重放、并发重放确定性、
越权重放与审计、重启恢复与租约重领、并发上限与背压指标、回环 HTTP 集成。
