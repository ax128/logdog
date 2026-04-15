# LogWatch

LogWatch 是一个基于 FastAPI 的容器监控与告警服务，聚焦以下能力：
- 多主机 Docker 状态监控（本地 `unix://` + 远程 `ssh://`）
- 容器日志/事件实时告警（规则匹配 + 冷却 + 静默 + 告警风暴抑制）
- 容器指标与宿主机指标采样落库（SQLite）
- 定时巡检与全局汇总报告
- Web API + WebSocket Chat + 内置前端控制台
- 配置热重载（admin 接口）

详细设计见：`docs/superpowers/specs/2026-04-10-logwatch-design.md`

## 快速开始

### 1) 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2) 准备配置

```bash
cp config/logwatch.yaml.example config/logwatch.yaml
```

默认配置路径：`config/logwatch.yaml`。
可通过环境变量覆盖：
- `LOGWATCH_CONFIG`：配置文件路径
- `LOGWATCH_METRICS_DB_PATH`：指标数据库路径

### 3) 启动服务（推荐显式注入 token）

```bash
python - <<'PY'
import uvicorn
from logwatch.main import create_app

app = create_app(
    web_auth_token="your-web-token",
    web_admin_token="your-admin-token",
)
uvicorn.run(app, host="0.0.0.0", port=8000)
PY
```

本地开发可选两种方式：

1) 使用环境变量 + `--factory`（推荐）：

```bash
WEB_AUTH_TOKEN=dev-web-token \
WEB_ADMIN_TOKEN=dev-admin-token \
uvicorn logwatch.main:create_app --factory --host 0.0.0.0 --port 8000
```

2) 仅本地临时调试使用不安全默认 token：

```bash
python - <<'PY'
import uvicorn
from logwatch.main import create_app

app = create_app(allow_insecure_default_tokens=True)
uvicorn.run(app, host="0.0.0.0", port=8000)
PY
```

不安全默认 token：
- web token：`web-token`
- admin token：`admin-token`

### 4) 本机运行 + SSH 远程 Docker + Telegram BotToken 自动 chat_id + 企业微信 webhook

可直接参考以下配置片段（放进 `config/logwatch.yaml`）：

```yaml
notify:
  telegram:
    enabled: true
    chat_ids: []          # 留空，走 auto_chat_id 自动解析
    auto_chat_id: true
    message_mode: text
  wecom:
    enabled: true
    targets: []           # 可留空，走 WECOM_WEBHOOK_URL 环境变量
    message_mode: md

hosts:
  - name: prod
    url: ssh://root@1.2.3.4
    ssh_key: /home/you/.ssh/id_rsa
```

启动命令示例：

```bash
WEB_AUTH_TOKEN='your-web-token' \
WEB_ADMIN_TOKEN='your-admin-token' \
TELEGRAM_BOT_TOKEN='123456:ABCDEF' \
WECOM_WEBHOOK_URL='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx' \
uvicorn logwatch.main:create_app --factory --host 0.0.0.0 --port 8000
```

注意：
- Telegram 自动解析 `chat_id` 前，需要先用该 Bot 给它发过一条消息（或把 Bot 拉进群并有新消息）。
- SSH 免密直连依赖本机私钥可用，且目标机 Docker 可被该账号访问（通常需要 root 或在 docker 组中）。

## 运行流程（代码实现口径）

1. `create_app()` 组装 `HostManager`、采样调度器、`WatchManager`、`NotifyRouter`、`chat_runtime`、API/WS 路由。
2. 应用启动后执行 `HostManager.startup_check()`，再启动各类 scheduler 与 watcher。
3. 日志/事件触发 `run_alert_once()`：规则匹配 -> 冷却 -> 静默检查 -> 模板分析 -> 通知 -> 落库。
4. 指标采样和告警数据统一写入 SQLite，由 retention 任务定期清理。
5. `POST /api/reload` 在“基于配置文件启动”场景下执行热重载，并带失败回滚。

## 鉴权规则

请求头统一：`Authorization: Bearer <token>`

- **无需 token**：
  - `GET /health`

- **web token**：
  - `GET /api/hosts`
  - `GET /api/hosts/{host}/containers`
  - `GET /api/alerts`
  - `GET /api/hosts/{host}/metrics/{container}`
  - `GET /api/hosts/{host}/system-metrics`
  - `GET /api/mutes`
  - `GET /api/storm-events`
  - `GET /api/storm-events/stats`
  - `POST /api/ws-ticket`（签发一次性 WS ticket）
  - `WS /ws/chat`（使用 ticket）

- **admin token**：
  - `POST /api/reload`
  - `GET /api/send-failed`

## 已实现路由

- `/`
- `/health`
- `/static/{asset_path:path}`（例如 `/static/app.js`）
- `/api/hosts`
- `/api/hosts/{host}/containers`
- `/api/alerts`
- `/api/hosts/{host}/metrics/{container}`
- `/api/hosts/{host}/system-metrics`
- `/api/ws-ticket`
- `/api/mutes`
- `/api/reload`
- `/api/send-failed`
- `/api/storm-events`
- `/api/storm-events/stats`
- `/ws/chat`

## 核心配置项（示例）

```yaml
metrics:
  sample_interval_seconds: 30
  host_system:
    enabled: false
    sample_interval_seconds: 30
    collect_load: true
    collect_network: true
    security:
      enabled: false
      push_on_issue: true
    report:
      include_in_schedule: true
      warn_thresholds:
        cpu_percent: 85
        mem_used_percent: 90
        disk_used_percent: 90

storage:
  db_path: data/logwatch.db
  retention:
    cleanup_interval_seconds: 86400
    alerts_days: 30
    audit_days: 30
    send_failed_days: 14
    metrics_days: 7
    host_metrics_days: 7
    storm_days: 14

docker:
  pool:
    max_clients: 8
    max_idle_seconds: 60

alert_storm:
  enabled: true
  window_seconds: 120
  threshold: 5
  suppress_minutes: 10

notify:
  # 兼容旧写法
  telegram:
    enabled: true
    chat_ids: [] # 为空时可配合 auto_chat_id + TELEGRAM_BOT_TOKEN 自动解析
    auto_chat_id: true
    message_mode: text # text|txt|md|doc
  wechat:
    enabled: false
    webhook_urls: []
    message_mode: text # text|txt|md|doc
  wecom:
    enabled: false
    targets: [] # 填完整 webhook URL；可留空并使用 WECOM_WEBHOOK_URL
    message_mode: md # text|txt|md|doc

  # 命名多通道（推荐）
  channels:
    tel1:
      type: telegram
      enabled: true
      chat_ids: ["123456"]
    tel2:
      type: telegram
      enabled: true
      chat_ids: ["789012"]
    weixin-user:
      type: weixin
      enabled: true
      targets: ["wx-user-id"]
    wecom-prod:
      type: wecom
      enabled: true
      targets: ["wecom-room-id"]
  routing:
    default_channels: [tel1]
    rules:
      - name: api-critical
        priority: 100
        match:
          hosts: [prod-*]
          containers: [api, gateway-*]
          categories: [ERROR, OOM, SECURITY_CHECK]
        deliver:
          channels: [tel2, wecom-prod]

llm:
  enabled: true # true=LLM/agent 中间层；false=脚本直推
  permissions:
    dangerous_tools: [restart_container]
    dangerous_host_allowlist: [prod]
  tools:
    rate_limit:
      limit: 20
      window_seconds: 60
```

## 性能与稳定性优化开关

- `storage.sqlite.journal_mode`: 建议 `wal`（提升并发写吞吐）
- `storage.sqlite.synchronous`: 建议 `normal`（平衡一致性与写性能）
- `storage.sqlite.busy_timeout_ms`: 建议 `5000`（降低短时锁竞争失败）
- `scheduler.max_instances`: 默认 `1`（防止周期任务重叠执行）
- `scheduler.coalesce`: 默认 `true`（错过触发时合并执行）
- `scheduler.misfire_grace_time`: 默认 `30`（秒，控制 misfire 补偿窗口）
- `defaults.watch.queue_maxsize / worker_count / drop_when_full`: 控制日志告警背压策略与丢弃行为
- `notify.routing.default_channels / rules / deliver.channels`: 按 host/container/category 做通知路由
- `notify.channels.<name>`: 支持命名多通道（如 `tel1/tel2/weixin-user/wecom-prod`），`type` 支持 `telegram|wechat|weixin|wecom`
- `hosts[].notify.channels`: 指定该主机允许使用的命名通道（例如某主机仅允许 `tel1`）

## SSH/系统安全检查

- 开关：`metrics.host_system.security.enabled`
- 发现风险后是否推送：`metrics.host_system.security.push_on_issue`
- 当前实现会在宿主机指标采样周期执行轻量安全分析；发现新风险状态时会：
  - 写入 `alerts`（`category=SECURITY_CHECK`）
  - 可选发送通知推送（受 `push_on_issue` 控制）
- 查询可通过 `/api/alerts?category=SECURITY_CHECK` 按类别过滤。

## 环境变量

已生效：
- `LOGWATCH_CONFIG`
- `LOGWATCH_METRICS_DB_PATH`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `WECHAT_WEBHOOK_URL`
- `WEIXIN_TARGET`
- `WECOM_TARGET`
- `WECOM_WEBHOOK_URL`
- `WEB_AUTH_TOKEN`
- `WEB_ADMIN_TOKEN`
- `LOGWATCH_ENABLE_USER_PREPROCESSORS`

说明：
- `WEB_AUTH_TOKEN` / `WEB_ADMIN_TOKEN` 由服务启动与 API/WS 鉴权直接消费。
- `TELEGRAM_CHAT_ID` / `WECHAT_WEBHOOK_URL` / `WEIXIN_TARGET` / `WECOM_TARGET` / `WECOM_WEBHOOK_URL` 可作为目标兜底来源。
- Telegram 支持仅配置 `TELEGRAM_BOT_TOKEN`：当 `notify.telegram.auto_chat_id=true` 且未配置 `chat_ids` 时，会自动从 Bot 更新流解析最近会话 chat_id。
- WeCom 通道支持内置 webhook 发送（target 填完整 webhook URL）。
- `wechat` 与 `wecom` 通道在当前实现中存在发送函数回退：若仅注入 `wechat_send_func` 或仅注入 `wecom_send_func`，两者会复用同一个发送函数。
- `LOGWATCH_ENABLE_USER_PREPROCESSORS` 默认关闭；仅当值为 `1/true/yes/on` 时才会加载 `config/preprocessors/*.py` 的用户预处理器。

## 消息格式热更新（Telegram）

- 前置条件：`app_config.agent.authorized_users.telegram` 需包含允许操作 bot 的 Telegram user id。
  - 示例：`agent.authorized_users.telegram: ["123456789"]`
- 已授权 Telegram 用户可发送命令热切换消息模式：
  - `/msg txt`
  - `/msg md`
  - `/msg doc`
  - `/msg text`
- 不带参数时会返回当前模式与用法（`/msg`）。
- `notify.telegram.message_mode` / `notify.wechat.message_mode` 可作为启动默认值。
- `notify.weixin.message_mode` / `notify.wecom.message_mode` 同样可配置启动默认值。
- 命名通道也可单独设置 `notify.channels.<name>.message_mode`。

## 热重载行为

- 仅当应用可解析到配置文件路径时，`POST /api/reload` 才会真正重载。
- 成功时可更新：host 配置、通知路由、报告调度、宿主机指标调度、chat runtime、telegram runtime。
- 失败时会执行回滚并尝试恢复旧 runtime/scheduler，同时发送 reload failure 通知（若通知路由可用）。
- 移除主机返回 `removed_requires_restart`，需重启进程彻底释放相关资源。

## 查询参数约束

- `GET /api/alerts`：`limit` 取值 `1..1000`。
- `GET /api/mutes`：`limit` 取值 `1..1000`。
- `GET /api/send-failed`：`limit` 取值 `1..1000`（admin token）。
- `GET /api/storm-events`：`limit` 取值 `1..1000`。
- `GET /api/hosts/{host}/metrics/{container}`：`limit` 取值 `1..10000`。
- `GET /api/hosts/{host}/system-metrics`：`limit` 取值 `1..10000`。
- `start/end`（含 storm stats 的 `start/end`）支持：
  - ISO 时间字符串（可被 Python `datetime.fromisoformat` 解析，可带时区）
  - Unix 时间戳（秒/毫秒/微秒/纳秒，数值或纯数字字符串）

## WebSocket 连接约束

- 路径：`/ws/chat`
- 浏览器侧推荐流程：
  - 先用 `Authorization: Bearer <token>` 调用 `POST /api/ws-ticket`
  - 再以 query `ticket=<one-time-ticket>` 建立 WS 连接
- `Sec-WebSocket-Protocol: bearer,<token>` 已禁用（避免 token 在代理/网关日志泄露）。
- query 参数中若出现 `token`（大小写不敏感）会被拒绝（关闭码 `1008`）。
- 若请求包含 `Origin`，其 host 必须与当前 WS host 相同；否则拒绝（关闭码 `1008`）。
- 默认限制：
  - `max_messages_per_connection=1000`
  - `max_connections=200`
  - `max_connections_per_user=20`
  - `receive_timeout_seconds=60`

## 已知限制

- `create_app(...)` 默认不再回退到内置 token；若未显式传入 `web_auth_token/web_admin_token`，需通过环境变量 `WEB_AUTH_TOKEN/WEB_ADMIN_TOKEN` 提供。仅本地临时调试可显式开启 `allow_insecure_default_tokens=True`。
- `/api/reload` 只做 admin token 校验；来源 IP 限制需由反向代理/防火墙处理。
- `WatchManager` 依赖 Docker 流式能力；若注入的 docker pool 不支持 stream primitives，会自动禁用 watcher。

## 测试

```bash
pytest -q
```
