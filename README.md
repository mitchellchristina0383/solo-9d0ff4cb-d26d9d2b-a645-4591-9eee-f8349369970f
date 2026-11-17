# 部署完成通知 · 幂等送达演示系统

发布值班员通过网页向审计端发送「部署完成通知」。网络超时时无法判断对方是否已记账，
盲目重发又会产生重复记录。本系统用 **FastAPI + SQLite** 实现两端（各自独立数据库），
以「**幂等键 + 回执核对**」解决该问题：

- 相同「环境 + 发布号」的同内容重试，两端都只记一次；
- 同键不同内容 → **409 冲突**；
- 丢回执 / 接收端不可达 / 发送端重启 → 状态保持 **待核**，**绝不虚报成功**，
  恢复后凭接收端回执核对定案（已入账→已确认；未入账→回退待发送，可安全重发）。

## 目录结构

```
├── docker-compose.yml      # 一键启动两端
├── sender/                 # 发送端（值班网页 + 发送/核对 + 封存/复核逻辑），端口 8000
│   ├── Dockerfile
│   └── app/{main.py, seal.py, static/index.html}
├── receiver/               # 审计接收端（演示用，含故障注入与演示篡改台），端口 8001
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
└── tests/
    ├── selftest.py         # 42 项关键情形自测（仅标准库）
    ├── audit_selftest.py   # 42 项封存/复核审计自测（仅标准库）
    └── run_local.sh        # 无 Docker 时的本地进程运行器
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 发送端网页：http://localhost:8000 （填写环境、发布号、结果，提交并发送，查看接收状态）
- 接收端台账：http://localhost:8001 （实时查看已入账回执）

两端各持有独立 SQLite 库（compose 命名卷 `sender_data` / `receiver_data`）；发送端的封存凭据
另存于**第三个独立库** `seals.db`（命名卷 `sender_seal_data`），与业务库物理隔离、只追加不覆盖。

无 Docker 的本地环境（需 Python 3.11+ 及 fastapi/uvicorn/httpx）：

```bash
tests/run_local.sh start-all     # 两个独立进程 + 两个独立 db 文件
tests/run_local.sh stop-all
```

## 状态机（发送端）

| 状态 | 含义 | 进入条件 |
|---|---|---|
| 待发送 PENDING | 本地已登记，未发送或已确认对方未记账 | 登记成功；核对时接收端明确无此记录 |
| 待核 UNCERTAIN | 已发出但回执缺失/对方不可达，送达与否未知 | 发送前落库；超时、连接失败、对方 5xx |
| 已确认 ACKED | 收到接收端内容一致的回执 | 发送响应 2xx 且内容哈希一致；核对命中 |
| 冲突 CONFLICT | 同环境同发布号在接收端已存在不同内容 | 发送/核对时发现内容不一致（需人工处理） |

关键约束：**只有接收端回执能把记录变为「已确认」**。发送动作会先把「待核」落库再发 HTTP，
即使进程在发送途中崩溃，重启后也知道必须核对，而不是误报成功或误当未发。

## 演示场景

1. **正常送达**：网页填写 `prod / R2026-001 / 成功` → 提交 → 状态「已确认」，接收端台账出现一条。
2. **重复提交**：再次提交相同内容 → 发送端幂等去重、接收端仍只有一条。
3. **内容冲突**：同环境同发布号改选「失败」→ 提交即报 409 冲突，原记录不被篡改。
4. **入库后丢回执**：勾选「模拟接收端故障」再提交 → 状态变「待核」（接收端其实已入账）；
   点「重试」→ 接收端幂等去重，凭回执转「已确认」，仍只有一条。
5. **接收端不可用**：`docker compose stop receiver` → 发送/核对均保持「待核」（不虚报成功）；
   `docker compose start receiver` 后点「核对」→ 若对方无记录则回退「待发送」，可安全重发。
6. **发送端重启**：`docker compose restart sender` → 状态持久不丢；启动后自动核对所有待核记录，
   凭接收端回执结案（接收端不可达时保持待核）。

## API 摘要

发送端（:8000）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/notifications` | 登记（同键同内容幂等，同键不同内容 409） |
| GET | `/api/notifications` | 列表 |
| POST | `/api/notifications/{id}/send[?simulate=lose_response\|slow]` | 发送/重发 |
| POST | `/api/notifications/{id}/reconcile` | 凭接收端回执核对 |
| POST | `/api/reconcile-all` | 核对全部待核/待发送 |

接收端（:8001）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/receipts` | 幂等入账；body 可带 `"simulate":"lose_response"`（入库后丢响应）或 `"slow"`（慢回执） |
| GET | `/api/receipts?env=..&release_no=..` | 查询单条回执（404 表示无记录） |
| GET | `/api/receipts` | 台账列表 |

## 幂等与并发设计

- 两端均以 `UNIQUE(env, release_no)` 为幂等键，内容身份为 `{env, release_no, result}` 的 SHA-256；
- 写入路径 `BEGIN IMMEDIATE` 事务内「先查后插」，配合 WAL 与 busy_timeout：
  并发下同键只落一行，后到者走幂等返回或 409；
- 重发安全：接收端对同内容重试返回既有回执（200, `deduplicated`），不新增记录；
- 任一端写入失败都不会留下错误的成功状态：接收端写失败→5xx→发送端保持待核；
  发送端本地写失败→500 且无记录；拿到回执但本地更新失败→仍为待核，可靠核对补回。

## 审计封存与复核（旧回执防篡改）

审计值班员复盘部署通知时，需要判断接收端台账里的**旧回执是否被误删或改动**。发送端网页提供
「回执封存与复核」能力，**封存凭据保存在发送端独立 SQLite 库（`seals.db`，compose 独立卷
`sender_seal_data`），与业务库 `sender.db` 物理隔离**：

- **封存**：一次性拉取接收端同一只读事务内的**已提交快照**（`GET /api/snapshot`，含全部行、
  最大回执号 `max_id`、行数与快照哈希），校验读取完整后，逐行固化不可变凭据
  （整行哈希 + 内容哈希）。封存编号 `seal_no` 为主键，**新封存只追加、绝不覆盖旧凭据**。
- **复核（对既有封存重新核验）**：重新拉取当前快照，**以封存时的最大回执号为界**，仅比对
  `id <= max_id` 的旧号，分别报告：
  - `missing` 缺失：封存时有、现在没有（旧回执被删）；
  - `changed` 内容改变：整行哈希或内容哈希不一致（字段被改）；
  - `inserted` 后来插入的旧号：边界内出现封存时不存在的 id；
  - 封存后正常新增（`id > max_id`）的回执计入 `new_after_seal`，**不算异常**。
- **失败绝不显示成功/通过**：接收端失联、HTTP 非 2xx、快照读取不完整（行数/最大号/快照哈希
  任一校验不过）、本地凭据保存失败，接口一律返回错误且不落半条凭据；复核失败也会留痕
  （`verifications.passed=0` + 错误原因），重启后旧封存仍可核验。
- **与入账交错安全**：接收端快照在单一只读事务内读取，并发入账要么整行可见、要么整行不可见，
  不会漏行或混入部分新记录。

接收端网页另带**演示用「台账篡改台」**（红框标注，正常业务接口不提供）：可删除回执、改动回执
内容、或用旧号补插记录，便于在发送端对既有封存执行复核并观察三类告警。

### 封存相关 API

发送端（:8000，凭据落在独立库 `seals.db`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/seals[?simulate=truncated\|save_fail]` | 封存接收端当前回执；失败返回 4xx/5xx，绝不显示成功 |
| GET | `/api/seals` | 封存列表（含最近复核结论） |
| GET | `/api/seals/{seal_no}` | 封存凭据明细与历次复核记录 |
| POST | `/api/seals/{seal_no}/verify` | 重新核验；失联/不完整/保存失败返回错误，绝不显示通过 |

接收端（:8001）：

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/snapshot[?simulate=truncated]` | 同一只读事务的已提交快照（含 snapshot_id/max_id/count/rows） |
| POST | `/api/demo/tamper` | **仅演示**：`op=delete/change/insert_old` 模拟台账被篡改 |

## 自测

```bash
# Docker 模式（先 docker compose up --build）
python3 tests/selftest.py --lifecycle        # 含停接收端/重启发送端场景（钩子默认 docker compose）
python3 tests/audit_selftest.py --lifecycle  # 封存/复核 42 项审计场景

# 无 Docker 本地模式
tests/run_local.sh test          # 原投递/回执 42 项
tests/run_local.sh audit-test    # 封存/复核 42 项
```

原自测覆盖：正常送达、同内容重试去重、同键冲突、丢回执后重发/核对、慢回执超时、
12 线程并发同内容、10 线程并发不同内容、接收端停机不虚报、发送端重启后待核持久与自动补核。

审计自测覆盖（42 项）：封存与干净复核通过、封存后新增不算异常、删除→missing、改动→changed、
旧号插入→inserted、三类异常同报、接收端失联时封存/复核均失败且留痕、截断快照与本地保存失败
均报错且无凭据残留、不存在封存 404、新封存不覆盖旧凭据、发送端重启后旧封存仍可核验、
16 线程入账与 4 次封存交错无漏行/混行、原投递与回执查询回归。
