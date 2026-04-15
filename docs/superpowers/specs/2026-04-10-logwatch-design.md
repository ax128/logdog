# LogWatch 设计文档（实现对齐版）

## 1. 文档目标

本文档以当前代码实现为准，描述 LogWatch 的：
- 架构分层与模块职责
- 启动/关闭生命周期
- 采集、告警、通知、存储的数据流
- 配置模型与热重载机制
- API/Chat/工具权限边界

非目标：
- 不描述未实现功能
- 不约束外部部署编排（如是否与其他 UI 工具同机部署）

---

## 2. 系统概览

LogWatch 是一个基于 FastAPI 的容器监控与告警服务，核心能力：
- 多主机 Docker 连接管理（`unix://` / `ssh://`）
- 容器日志与 Docker 事件实时告警
- 容器指标与宿主机指标采样入库（SQLite）
- 定时巡检与全局汇总报告
- 多通道通知（Telegram / WeChat / Weixin / WeCom）
- Web API + WebSocket Chat + 内置前端
- 配置热重载与失败回滚

应用入口：`logwatch/main.py:create_app()`

---

## 3. 分层与模块职责

### 3.1 目录结构（核心）

```text
logwatch/
├── main.py
├── core/
│   ├── config.py
│   ├── host_manager.py
│   ├── docker_connector.py
│   ├── db.py
│   ├── metrics_writer.py
│   └── retention_scheduler.py
├── collector/
│   ├── watch_manager.py
│   ├── log_stream.py
│   ├── event_stream.py
│   ├── sampler.py
│   ├── metrics.py
│   ├── host_metrics_sampler.py
│   ├── host_metrics_probe.py
│   ├── reports.py
│   ├── scheduler.py
│   └── storm.py
├── pipeline/
│   ├── filter.py
│   ├── cooldown.py
│   └── preprocessor/
├── llm/
│   ├── analyzer.py
│   ├── agent_runtime.py
│   ├── tools.py
│   ├── permissions.py
│   └── prompts/
├── notify/
│   ├── base.py
│   ├── router.py
│   ├── telegram.py
│   ├── wechat.py
│   ├── weixin.py
│   ├── wecom.py
│   └── render.py
└── web/
    ├── api.py
    ├── chat.py
    ├── frontend.py
    └── static/
```

### 3.2 职责边界

- `main.py`：依赖装配、生命周期管理、状态注入、热重载事务/回滚。
- `core/host_manager.py`：主机配置持有、连接检查、重试/熔断、状态变化事件。
- `core/docker_connector.py`：Docker SDK 访问封装（查询、流式日志、流式事件、重启容器）。
- `collector/*`：
  - 实时链路：`LogStreamWatcher` / `EventStreamWatcher` / `WatchManager`
  - 定时链路：指标采样、宿主机采样、巡检报告、保留策略清理
  - 风暴聚合：`AlertStormController`
- `pipeline/*`：规则判定、冷却窗口、可插拔预处理。
- `llm/*`：模板渲染、Agent runtime 包装、工具注册、权限策略。
- `notify/*`：通道抽象、路由分发、重试与失败记录、消息模板渲染。
- `web/*`：REST API、WS Chat、静态前端挂载。

---

## 4. 启动与生命周期

### 4.1 create_app 装配阶段

`create_app()` 的关键装配顺序：

1. 解析配置来源
- 显式 `app_config` / `config_path`
- 或 `LOGWATCH_CONFIG`
- 或默认 `config/logwatch.yaml`（若存在）

2. 解析运行时设置（`resolve_runtime_settings`）
- 采样周期
- 保留策略
- host_system 开关与阈值
- Docker pool 参数

3. 构建基础组件
- `HostManager`
- `MetricsSqliteWriter`（惰性创建）
- `NotifyRouter`
- `ScheduleReportRunner`
- `AlertStormController`（可选）

4. 构建后台任务
- `MetricsSamplingScheduler`
- `HostMetricsSamplingScheduler`（`metrics.host_system.enabled=true` 时）
- `RetentionCleanupScheduler`
- `ReportScheduler`（存在 host schedules 或 global_schedules 时）
- `WatchManager`（流式能力可用时）

5. 构建交互层
- `tool_registry`
- `chat_runtime`
- `telegram_runtime`（有 bot token 时）
- API Router + Chat Router + 前端挂载

### 4.2 应用生命周期（lifespan）

启动阶段：
1. `HostManager.startup_check()`
2. 启动 metrics/host_metrics/retention scheduler
3. 启动 `WatchManager`
4. 启动 `ReportScheduler`
5. 启动 `telegram_runtime`

关闭阶段（反向释放）：
1. telegram/report/watcher/scheduler 依次关闭
2. docker client pool 关闭
3. metrics writer 关闭

---

## 5. 主机连接管理

文件：`core/host_manager.py`

### 5.1 状态模型

每个 host 维护：
- `status`：`connected` / `disconnected`
- `last_connected_at`
- `last_error` / `last_error_kind`
- `failure_count` / `consecutive_failures`
- `circuit_open_until`

### 5.2 连接策略

- 启动检查和新增主机都会调用 `_connect_host()`。
- 支持 `max_retries` 退避重试。
- 连续失败达到阈值后打开熔断窗口，窗口内不再尝试真实连接。
- `ssh://` 主机会预检查 `ssh_key` 是否存在。

### 5.3 状态变化事件

当连接状态变化时触发 `_on_status_change`，`main.py` 将其组合为：
- 主机状态通知
- `WatchManager.handle_host_status_change`（自动启停对应 watcher）

---

## 6. Docker 接入层

文件：`core/docker_connector.py`

提供两种调用模式：
- 单次调用（每次临时 client）
- `DockerClientPool`（复用 client，支持 max_clients/max_idle_seconds）

核心能力：
- `list_containers_for_host`
- `fetch_container_stats`
- `query_container_logs`
- `stream_container_logs`
- `stream_docker_events`
- `restart_container_for_host`

流式接口通过后台线程消费 Docker iterator，再桥接到异步迭代器，保证可取消与资源释放。

---

## 7. 采样与存储链路

### 7.1 容器指标采样

链路：
`MetricsSamplingScheduler -> MetricsSampler -> build_metric_sample -> MetricsSqliteWriter.write`

采样字段：
- CPU、内存、网络 I/O、磁盘 I/O
- `status`、`restart_count`
- `host_name/container_id/container_name/timestamp`

说明：
- CPU 计算依赖前后两次 stats 差值；首样本 CPU 可能为 0。
- 采样失败按容器粒度记录异常，不中断整个周期。

### 7.2 宿主机指标采样（可选）

链路：
`HostMetricsSamplingScheduler -> HostMetricsSampler -> collect_host_metrics_for_host -> write_host_metric`

支持来源：
- `unix://` 主机：本地 `/proc` 与 `disk_usage('/')`
- `ssh://` 主机：Paramiko 执行白名单命令并解析 JSON

可配置项：
- `enabled`
- `sample_interval_seconds`
- `collect_load`
- `collect_network`

### 7.3 数据库与 writer

文件：`core/db.py`, `core/metrics_writer.py`

`MetricsSqliteWriter`：
- 维护单连接（惰性初始化）
- 通过内部锁串行化写入与查询调用
- 提供统一 API：alerts/mutes/metrics/host_metrics/audit/send_failed/storm

SQLite 主要表：
- `alerts`
- `audit_log`
- `send_failed`
- `storm_events`
- `mutes`
- `metrics`
- `host_metrics`

Retention 清理：
- 定时执行 `run_retention_cleanup`
- 必选 retention keys：`alerts_days/audit_days/send_failed_days`
- 可选：`metrics_days/host_metrics_days/storm_days`
- 同时清理过期 `mutes`

---

## 8. 实时告警链路

### 8.1 日志流告警

文件：`collector/log_stream.py`

链路（单条日志）：
1. `LogStreamWatcher.watch_container()` 读取日志记录
2. 经预处理器链处理（默认 + `config/preprocessors/*.py`）
3. `run_alert_once()` 执行判定

`run_alert_once()` 顺序：
1. `apply_rules()`：`ignore -> redact -> custom_alerts -> alert_keywords`
2. 冷却检查（`CooldownStore`）
3. 静默检查（`find_active_mute`）
4. `analyze_with_template("alert", ...)`
5. `render_output(output_template, ...)`
6. 风暴到期消息刷新（`flush_due`）
7. 记录当前事件并应用风暴策略（开始/抑制/正常）
8. 按风暴策略执行通知发送 + 告警落库

冷却与去重行为：
- 冷却内新告警不会立刻推送。
- 当使用系统当前时间分支时，会安排去重窗口摘要消息（dedup summary）在窗口结束后发送并落库。

### 8.2 事件流告警

文件：`collector/event_stream.py`

监听 Docker `container` 事件，对以下 action 生成标准告警语句并复用 `run_alert_once()`：
- `restart`
- `die`
- `oom`

对 `start/restart/die/destroy/stop` 事件触发 `refresh_host()`，让容器列表与 watcher 集合保持同步。

### 8.3 WatchManager 协调

文件：`collector/watch_manager.py`

职责：
- 启动时仅为 `connected` host 建立 log/event watcher
- host 状态变化时动态启停 watcher
- reload 后对变更 host 执行重建/刷新

---

## 9. 规则、预处理、风暴控制

### 9.1 规则引擎（pipeline/filter.py）

支持配置：
- `rules.ignore`
- `rules.redact`
- `rules.custom_alerts`
- `alert_keywords`

分类结果：
- 自定义规则命中：取 `category`（默认 `BUSINESS`）
- 关键词命中：`OOM` 或 `ERROR`

### 9.2 预处理器机制

文件：`pipeline/preprocessor/*`

- 默认预处理器：`DefaultPreprocessor`（当前透传）
- 用户预处理器：放在 `config/preprocessors/*.py`
- 约束：必须暴露 `PREPROCESSOR` 且继承 `BasePreprocessor`
- 加载失败：跳过该脚本并记录 warning

### 9.3 告警风暴

文件：`collector/storm.py`

按 `category` 统计滑动窗口事件，达到阈值时：
- 发送 `STORM` 开始汇总
- 抑制窗口内同类单条推送
- 窗口结束发送 `STORM_END` 汇总

配置路径：`alert_storm`。

---

## 10. 定时报告链路

### 10.1 调度器

文件：`collector/scheduler.py`

`ReportScheduler` 支持两类计划：
- host schedule：来自每个 host 的 `schedules`
- global schedule：来自顶层 `global_schedules` 且 `scope=all_hosts`

支持触发方式：
- `interval_seconds`
- `cron`（5 字段）

### 10.2 主机报告

文件：`collector/reports.py` -> `run_host_schedule()`

输入：
- host 状态
- container 列表
- 最近告警
- 容器指标摘要
- 宿主机指标摘要（若启用）

行为：
- 识别是否 abnormal（host 断连/有告警/容器状态异常）
- 正常场景可按 `push_on_normal` 与 `heartbeat_interval_seconds` 决定是否降级为 heartbeat 推送
- 场景模板：`interval/hourly/daily/heartbeat`

### 10.3 全局报告

`run_global_schedule()`：
- 并发收集各 host 摘要
- 按长度预算压缩摘要
- 用场景模板分析后发送全局通知

注意：全局通知优先使用 schedule 级 `notify_target` 解析出的 channel targets。

---

## 11. 通知系统

### 11.1 路由与重试

文件：`notify/router.py`

- `NotifyRouter.send(host, message, category)`
- 支持 host 级 notifier 集合
- Router 级统一重试次数与指数退避（对该次发送涉及的通道生效）
- 任一通道发送成功即返回 `True`（全部失败返回 `False`）

失败记录：
- 失败 payload 会脱敏（Bearer/API key/password）
- 写入 `send_failed` 表（best-effort）

### 11.2 通道实现

- `TelegramNotifier`
- `WechatNotifier`
- `WeixinNotifier`
- `WecomNotifier`

以上通道都基于 `BaseNotifier`，支持超长消息分片发送。

### 11.3 Telegram Bot 交互

文件：`notify/telegram.py`

`TelegramBotRuntime`：
- 管理 bot 生命周期
- 校验 `authorized_user_ids`
- 调用 `chat_runtime.invoke_text()` 返回回复

---

## 12. Chat Runtime 与工具调用

### 12.1 WebSocket Chat

文件：`web/chat.py`

- 路径：`/ws/chat`
- 鉴权：
  - `Authorization: Bearer <token>`
  - 或 `Sec-WebSocket-Protocol: bearer,<token>`
- 明确拒绝 query 参数 `token`
- 若请求携带 `Origin`，要求其 host 与当前 WS host 一致
- 单连接最大消息数默认 `1000`
- 默认连接限制：`max_connections=200`、`max_connections_per_user=20`
- 空闲接收超时默认 `60` 秒（超时后关闭连接）

### 12.2 Agent Runtime

文件：`llm/agent_runtime.py`

- 若 `deepagents` 不可用，降级为 fallback 文本
- 支持 `thread_id`（由 `user_id + session_key` 派生）
- 尝试挂载 memory checkpointer（可选依赖）

### 12.3 工具注册与审计

文件：`llm/tools.py`

内置工具：
- `list_hosts`
- `list_containers`
- `query_logs`
- `get_metrics`
- `get_alerts`
- `mute_alert`
- `unmute_alert`
- `restart_container`

统一保护层（`guarded_invoke`）：
- 每用户滑动窗口限流（默认 20 次 / 60 秒）
- 权限检查（`ensure_tool_allowed`）
- 全量审计写入 `audit_log`

危险操作默认策略（`llm/permissions.py`）：
- 默认危险工具：`restart_container`
- 需要：
  - host 在 `dangerous_host_allowlist`
  - 参数包含显式确认（`confirmed/confirmation`）

---

## 13. API 设计

文件：`web/api.py`

Web token 保护：
- `GET /api/hosts`
- `GET /api/hosts/{host}/containers`
- `GET /api/alerts`
- `GET /api/hosts/{host}/metrics/{container}`
- `GET /api/hosts/{host}/system-metrics`
- `GET /api/mutes`
- `GET /api/storm-events`
- `GET /api/storm-events/stats`

Admin token 保护：
- `POST /api/reload`
- `GET /api/send-failed`

公开端点（无需 token）：
- `GET /health`

设计要点：
- action 可注入（便于测试）
- 参数边界在 API 层做基本校验（`limit` 区间、host not found 映射 404）

---

## 14. 配置模型

文件：`core/config.py`

### 14.1 配置来源优先级

1. 显式传入 `app_config`
2. 显式 `config_path`
3. 环境变量 `LOGWATCH_CONFIG`
4. 默认 `config/logwatch.yaml`

### 14.2 defaults + hosts 合并

`merge_host_config(defaults, host)` 合并字段：
- `schedules`（支持 `append/replace`）
- `containers.include/exclude`
- `rules.ignore/redact/custom_alerts`（支持去重与覆盖）
- `notify`（按 channel 合并）

### 14.3 运行时参数

`resolve_runtime_settings()` 负责类型校验与默认值：
- `metrics.sample_interval_seconds`
- `storage.retention.cleanup_interval_seconds`
- `storage.db_path`
- `metrics.host_system.*`
- `docker.pool.max_clients/max_idle_seconds`

---

## 15. 热重载设计

`main.py` 的 `default_reload_action()` 包含以下阶段：

1. 读取新配置并校验
2. `HostManager.reload_hosts(new_hosts)`
3. 构建候选对象：
- notify router
- status sender
- storm controller
- tool registry / chat runtime
- telegram runtime
- report scheduler
- host metrics scheduler
4. 若应用正在运行，先启动候选 scheduler/runtime，再停旧实例
5. 全部成功后切换 `app.state` 与闭包内引用
6. 若任一步失败：
- 关闭已启动候选实例
- 尝试恢复旧实例
- 恢复旧状态引用
- 发送 reload failure notice（若通知链可用）
- 抛出异常返回 503

限制：
- 未绑定配置文件路径时，reload 返回 `{"ok": true}`（不执行真实重载）
- 主机移除仅返回 `removed_requires_restart`，不做热卸载资源回收

---

## 16. 前端控制台

文件：`web/static/index.html`, `web/static/app.js`

功能：
- token 录入（不持久化）
- `session` 标识写入 `sessionStorage`
- 轮询 `/api/hosts` 与 `/api/hosts/{host}/containers`
- 展示 host 状态与容器简表
- 建立 `/ws/chat` 会话并展示对话流

WebSocket 建链方式：
- 子协议：`['bearer', token]`
- query 里仅带 `session`，不传 token

---

## 17. 安全与可靠性要点

- 鉴权分层：web token / admin token 分离。
- API token 比较使用 `hmac.compare_digest`。
- WS 明确禁止 query token，避免 token 落入 URL 日志。
- 通知失败消息脱敏后落库。
- Agent 工具危险操作需 allowlist + 显式确认。
- host manager 提供重试、错误分类、熔断，降低抖动。
- reload 具备回滚路径，避免半更新态。

---

## 18. 当前实现约束

- 已提供 `/health`（返回 `{"ok": true}`）。
- `create_app()` 默认不会回退到内置 token；需显式传入 `web_auth_token/web_admin_token`，或设置环境变量 `WEB_AUTH_TOKEN/WEB_ADMIN_TOKEN`。仅本地临时调试可显式开启 `allow_insecure_default_tokens=True`。
- `POST /api/reload` 只做 token 鉴权，来源网络限制需外部网关实现。
- 默认 `DefaultPreprocessor` 当前为透传实现，复杂规范化逻辑需用户脚本补充。
- 全局通知在未配置可用目标时可能返回发送失败（需在 `notify` 或 `global_schedules.notify_target` 补齐目标）。

---

## 19. 测试覆盖（目录视角）

- `tests/core/`：配置、DB、host manager、connector、retention、writer
- `tests/collector/`：watcher、scheduler、report、metrics sampler、host probe
- `tests/pipeline/`：规则、冷却、预处理加载
- `tests/notify/`：router、render、telegram runtime
- `tests/llm/`：prompt loader、analyzer fallback、chat runtime
- `tests/web/`：API、WS 鉴权、main 装配与生命周期
- `tests/integration/`：告警流水线与 stream 生命周期
