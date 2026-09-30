# 事件投递服务（至少一次 / 顺序投递）

一个最小但完整的 webhook 投递系统，包含三个独立进程：

| 组件 | 职责 |
| --- | --- |
| `api` | 后端事件入口（FastAPI）。按 `X-Request-Id` 幂等入库，原始 JSON 字节原样保存。 |
| `worker` | 独立投递进程。每个配置目标一个协程，严格按入库顺序投递，负责重试 / 死信 / 崩溃恢复。 |
| `db` | Postgres，持久投递箱（事件 + 每次投递尝试历史 + 虚拟时钟）。 |

另有两个假目标（`target-a` / `target-b`）和一次性验收服务 `verify`。

## 语义（务必先读）

- **投递保证是 at-least-once（至少一次），不是恰好一次。** worker 在 HTTP 请求
  在途时崩溃，重启后会把该事件再投一次，因此重复投递是被允许且预期的；但事件不会丢失。
- **目标白名单是部署时配置**：生产者只能按名字投递（`target-a` / `target-b`），
  永远不能指定 URL。URL 与该目标专属密钥来自环境变量 `WEBHOOK_TARGETS`
  （格式 `name=url:secret,name=url:secret`），未知目标返回 404。
- **同一目标严格按事件入库顺序投递**：worker 只取该目标最老的未终结事件
  （`pending` / `in_flight` / `retrying` 中 `min(id)`）。前一事件还在重试等待或
  在途时，后面的事件绝不会越过它。
- **不同目标互不阻塞**：每个目标是独立协程，一个目标持续 500 / 超时不影响另一个。
- **重试策略**：非 2xx（含连接错误、超时）按 **1s、2s、4s** 间隔重试，
  **第 4 次尝试仍失败 → 死信（`dead`）**。
- **死信重投**：`POST /events/{id}/redeliver` 将死信重新置为待投，事件 id、
  `request_id`、原始字节与**全部历史尝试记录都保留**；新一轮尝试以连续的
  `attempt_no` 追加（每轮重新获得 4 次尝试预算）。
- 死信是该目标的“队头”，在被重投之前会一直挡住其后的事件（不会被悄悄越过）。

## 签名

每次投递使用**目标专属密钥**做 HMAC-SHA256，签名内容是时间戳与**原始 JSON 字节**：

```
signing_string = "<unix秒>." + body_raw_bytes      # body 是入库时的原始字节，不重新序列化
X-Signature:    hex(HMAC_SHA256(secret, signing_string))
X-Timestamp:    <unix秒>
X-Event-Id:     <事件 id>
X-Request-Id:   <生产方请求 id>
```

2xx 视为成功，其余一律按失败重试。

## HTTP 接口

生产方：

- `POST /events/{target}`，Header `X-Request-Id: <幂等键>`，Body 为 JSON。
  首次 `200 {"duplicate": false}`；同一 `(target, request id)` 重放
  `200 {"duplicate": true, "id": <同一事件id>}`。目标不存在 404，非 JSON 400。

查询 / 运维：

- `GET /events?target=...`、`GET /events/{id}`（含 `delivery_attempts` 尝试历史）
- `POST /events/{id}/redeliver`：重投死信
- `POST /admin/clock/advance?seconds=N`：推进虚拟时钟（仅 `virtual` 模式）
- `GET /healthz`

worker 自带一个最小 admin 端口（8081，stdlib HTTP）：`GET /healthz`、
`POST /admin/crash`（硬退出，用于重启测试；配合 compose 的 `restart: unless-stopped`）。

## 时钟

`WEBHOOK_CLOCK_MODE=virtual` 时，系统时间是保存在 `clock_state` 表里的单调微秒计数器，
只能通过 `/admin/clock/advance` 向前推进。worker 不靠真睡秒数等退避——时间不推进就不重试，
推进后立刻到期，从而让 1/2/4 秒退避可在测试中瞬间、确定性地发生。生产环境设为 `real`
（默认）即使用墙上时钟与真实睡眠。

## 崩溃恢复

事件被取出投递前，`in_flight` 状态与 `running` 的尝试行会**先落库**（独立事务，
autocommit 连接池）。worker 启动时：

1. 所有残留 `running` 尝试 → 标记 `interrupted`；
2. 所有残留 `in_flight` 事件 → 立即重新置为可投递（已用尽本轮 4 次预算的才进死信）。

因此崩溃可能导致同一事件被收到两次，但不会丢事件，也不会声称“恰好一次”。

## 运行验收

固定三步：

```bash
docker compose config --quiet
docker compose build
docker compose run --rm verify
```

`verify` 是一次性服务，依次断言：白名单拒绝、请求 ID 去重、HMAC 签名正确性、
同目标顺序 / 队头不可越过、1/2/4 秒重试后第四次进死信、目标间隔离、
超时计失败并重试、worker 崩溃重启后不丢事件且允许重复（保留中断尝试历史）、
死信重投保留原身份与完整尝试历史。全部通过退出码为 0。

本地起栈手工调试：

```bash
docker compose up -d --build db api worker target-a target-b
curl -s -XPOST localhost:8000/events/target-a \
  -H 'X-Request-Id: demo-1' -H 'Content-Type: application/json' \
  -d '{"hello":"world"}'
curl -s localhost:8000/events/1
```

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `WEBHOOK_TARGETS` | — | `name=url:secret,...`，部署时固定的目标白名单 |
| `WEBHOOK_RETRY_DELAYS` | `1,2,4` | 每次失败后的退避秒数 |
| `WEBHOOK_MAX_ATTEMPTS` | `4` | 每轮尝试次数（第 4 次失败进死信） |
| `WEBHOOK_CLOCK_MODE` | `real` | `real` 或 `virtual` |
| `WEBHOOK_HTTP_TIMEOUT` | `2` | 单次投递 HTTP 超时秒数 |
| `WEBHOOK_POLL_INTERVAL` | `0.03` | 无事件 / 等待虚拟时间推进时的轮询间隔 |
| `DATABASE_URL` | `postgresql://dispatch:dispatch@db:5432/dispatch` | Postgres DSN |

## 目录

```
app/config.py       部署期配置（白名单、退避、时钟）
app/signing.py      HMAC 签名 / 校验
app/clock.py        实时时钟 / 虚拟时钟
app/db.py           连接池与 schema（events / delivery_attempts / clock_state）
app/api.py          事件入口与查询、死信重投、时钟推进
app/worker.py       独立 worker：按目标协程、队头投递、重试、死信、恢复
app/fake_target.py  可控假 HTTP 目标（失败/挂起/收据）
app/verify.py       一次性验收服务
```
