# LogWatch 项目审计提示词（项目定制版）

你是一位审计提示词执行者。请先阅读“第一阶段项目检查结果”，再按“第二阶段审计提示词”逐项执行并输出审计报告。

## 第一阶段：项目全面检查结果（基于当前仓库）

### 1. 技术栈识别
- 语言与运行时：Python `>=3.11`。
- Web 框架：FastAPI + Uvicorn。
- 核心依赖：`docker`、`aiosqlite`、`apscheduler`、`pyyaml`、`paramiko`、`deepagents`、`python-telegram-bot`。
- 构建工具：`setuptools.build_meta` + `wheel`（`pyproject.toml`）。
- 包管理方式：基于 `pyproject.toml`（README 以 `pip install -e .` 为主，`uv` 仅作为可选说明，未发现锁文件）。
- 测试框架：`pytest`、`pytest-asyncio`、`httpx`。
- 质量与 CI：存在 pytest 配置（`[tool.pytest.ini_options]`）；未发现 `.github/workflows` 或显式 `ruff/mypy/black` 配置。

### 2. 架构与设计理解
- 主入口：`logwatch/main.py:create_app()`，负责依赖装配、生命周期管理、状态注入、热重载回滚。
- 目录分层：
  - `core/`：配置、主机管理、Docker 接入、数据库与 writer、retention。
  - `collector/`：日志/事件 watcher、采样、报告、调度、风暴控制。
  - `pipeline/`：规则过滤、冷却、日志预处理插件。
  - `notify/`：通知通道抽象、路由策略、失败记录。
  - `llm/`：模板分析、agent runtime、工具注册与权限控制。
  - `web/`：REST API、WebSocket Chat、静态前端。
- 路由/调度机制：
  - HTTP：`/api/*` + `/health`。
  - WS：`/ws/chat`。
  - 定时任务：APScheduler（容器指标采样、宿主机采样、报告、retention 清理）。
- 数据流向（请求 -> 处理 -> 存储 -> 响应）：
  - HTTP 查询链：`web/api.py` -> `main.py` 注入 action -> `MetricsSqliteWriter` -> `core/db.py` -> JSON 响应。
  - 告警链：Docker 日志/事件流 -> `run_alert_once()` -> `filter/cooldown/mute/storm` -> 通知路由 -> SQLite 落库。
  - Chat 工具链：`/ws/chat` -> `AgentRuntime` -> `llm/tools` -> host/docker/db -> 审计日志 + 工具结果返回。
- 外部集成点：
  - Docker Daemon（`unix://` / `ssh://`）
  - SSH（Paramiko，用于远程宿主机指标）
  - SQLite（指标、告警、审计、静默、发送失败、风暴事件）
  - 第三方通知（Telegram / WeChat / Weixin / WeCom）
  - deepagents/langgraph（可选运行时）
  - 未见独立缓存与消息队列（如 Redis/Kafka/RabbitMQ）。
- 设计模式与状态管理：
  - 依赖注入（`create_app` 参数注入大量可替换 action）
  - 策略模式（通知路由策略、危险工具权限策略）
  - 状态机（`HostManager` 连接/熔断状态、storm 会话状态）
  - 错误处理风格：接口层映射 HTTP 错误；后台任务常用“记录日志并降级继续”。

### 3. 功能板块梳理（模块、调用关系、核心/辅助）
| 模块 | 主要职责 | 调用关系（入 -> 出） | 属性 |
|---|---|---|---|
| 应用装配与生命周期（`main.py`） | 装配 host/writer/scheduler/watcher/router/runtime，处理热重载回滚 | create_app -> web/collector/core/llm/notify | 核心 |
| 配置解析（`core/config.py`） | 读取 YAML、合并 default+host、运行时参数校验 | main -> config -> host/runtime settings | 核心 |
| 主机管理（`core/host_manager.py`） | 主机状态检查、重试、熔断、状态变更事件 | main/scheduler -> host_manager -> connector | 核心 |
| Docker 连接层（`core/docker_connector.py`） | 容器列表、stats、日志/事件流、重启容器、连接池 | host_manager/llm/tools/collector -> docker SDK | 核心 |
| 日志告警链（`collector/log_stream.py`） | 日志解析、规则过滤、冷却、静默、风暴、通知、落库 | watch_manager -> log_stream -> notify/db | 核心 |
| 事件告警链（`collector/event_stream.py`） | Docker event 映射告警并复用 run_alert_once | watch_manager -> event_stream -> log_stream | 核心 |
| watcher 协调（`collector/watch_manager.py`） | 按 host 状态启停/替换 log+event watcher | host_manager/main -> watch_manager -> watchers | 核心 |
| 指标采样与调度（`collector/sampler.py`、`host_metrics_sampler.py`、`scheduler.py`） | 容器/宿主机指标周期采样与调度 | scheduler -> sampler -> metrics_writer | 核心 |
| 报告与巡检（`collector/reports.py`） | host/global schedule 汇总分析并通知 | report_scheduler -> reports -> notify | 核心 |
| 风暴控制（`collector/storm.py`） | storm_start/suppressed/storm_end 控制 | run_alert_once -> storm_controller | 核心 |
| 存储层（`core/metrics_writer.py`、`core/db.py`） | sqlite schema/CRUD/审计脱敏/retention | 各模块 -> writer -> db/sqlite | 核心 |
| 通知层（`notify/*`） | 路由策略、多通道发送、失败记录与脱敏 | log_stream/reports/main -> notify router/notifier | 核心 |
| Web API（`web/api.py`） | REST 接口、token 鉴权、错误映射 | frontend/client -> api -> main action | 核心 |
| WebSocket Chat（`web/chat.py`） | WS 鉴权、session、runtime 调用、安全降级 | frontend/telegram -> chat -> agent runtime | 核心 |
| LLM runtime 与提示词（`llm/agent_runtime.py`、`llm/analyzer.py`、`llm/prompts/*`） | prompt 渲染与 agent fallback | log_stream/reports/chat -> analyzer/runtime | 核心 |
| LLM 工具与权限（`llm/tools.py`、`llm/permissions.py`） | 工具调用、限流、危险操作权限、审计日志 | chat runtime -> tools -> docker/db | 核心 |
| 静态前端（`web/frontend.py` + `web/static/*`） | Token 输入、主机/容器轮询、WS chat UI | browser -> api/ws | 辅助 |
| 预处理插件系统（`pipeline/preprocessor/*`） | 动态加载用户预处理器 | log_stream -> preprocessors | 辅助 |

### 4. 风险预判（按项目特征）
- 高风险模块优先级（建议）：
  1. `llm/tools.py` + `llm/permissions.py`（危险动作授权边界）
  2. `main.py`（热重载事务与回滚一致性）
  3. `collector/log_stream.py`（告警正确性、并发队列与抑制逻辑）
  4. `core/db.py` + `core/metrics_writer.py`（数据一致性、锁竞争、保留策略）
  5. `web/chat.py` + `web/api.py`（认证鉴权与输入边界）
- 风险类型预判：
  - 鉴权绕过、危险工具误调用、敏感信息泄露。
  - 冷却/静默/风暴逻辑导致漏报或误报。
  - 热重载半失败导致新旧组件混用。
  - SQLite 高并发下的锁等待、查询/清理性能劣化。
  - 流式 watcher 和后台任务资源泄漏（任务、连接、线程、队列）。

---

## 第二阶段：可直接执行的审计提示词

请直接按下述提示词执行审计：

---

你是一位资深代码审计工程师。请针对 LogWatch 当前仓库实现执行审计，并严格按本文档输出问题。

### A. 项目概要
LogWatch 是一个基于 FastAPI 的容器监控与告警系统，入口是 `logwatch/main.py:create_app()`，并通过 `app.state` 组合 host 管理、watcher、scheduler、通知、LLM runtime 与 API/WS 路由。系统核心业务链路是 Docker 日志/事件经过规则匹配、冷却/静默/风暴抑制后发送通知并持久化到 SQLite。项目同时提供容器/宿主机指标采样、定时巡检报告、配置热重载回滚，以及基于 WebSocket 的 Chat 工具调用能力。外部关键边界涉及 Docker/SSH/SQLite/消息通道与危险工具权限控制。该项目无独立缓存或队列，状态主要在内存与 SQLite 中维护。

### B. 功能板块逐一审计清单

#### 模块 1：应用装配与热重载（`logwatch/main.py`）
- 业务逻辑正确性：校验 create_app 的依赖装配是否和 state 注入一致；重载后 router/scheduler/runtime 是否全部指向新实例。
- 异常与错误处理：校验 `default_reload_action` 中候选资源启动失败后的回滚路径是否完整、是否留下半切换状态。
- 输入校验：校验配置来源优先级（显式参数、环境变量、默认路径）和空配置/错误配置行为。
- 安全性：校验重载失败通知与错误文本脱敏（token/password/api key）。
- 资源管理：校验 lifespan 启停顺序，确保 watcher/scheduler/docker pool/sqlite 在关闭阶段都能释放。

#### 模块 2：配置解析与合并（`logwatch/core/config.py`）
- 业务逻辑正确性：校验 defaults/hosts/rules/notify/schedules 的合并规则是否符合预期。
- 异常与错误处理：校验类型不匹配、非法枚举、重复 host 名称时是否立即失败且错误明确。
- 输入校验：重点检查整数、布尔、百分比、sqlite 枚举、路径字段校验边界。
- 安全性：校验配置中敏感字段在错误输出中的泄露风险。
- 资源管理：关注大配置（大量 hosts/rules）下的合并复杂度和启动耗时。

#### 模块 3：主机连接管理（`logwatch/core/host_manager.py`）
- 业务逻辑正确性：校验连接重试、熔断、半开恢复、状态变更通知触发条件。
- 异常与错误处理：校验错误分类与错误信息截断/脱敏是否一致。
- 输入校验：校验 host.name/url/ssh_key 等关键字段为空或非法时行为。
- 安全性：校验 SSH key 预检查与 URL 脱敏逻辑。
- 资源管理：校验连续失败与重试退避是否可控，避免过度重试压垮系统。

#### 模块 4：Docker 连接层（`logwatch/core/docker_connector.py`）
- 业务逻辑正确性：校验 list/stats/logs/events/restart 的参数和返回语义。
- 异常与错误处理：校验 Docker SDK 异常在调用方是否被正确转义与隔离。
- 输入校验：校验 `container_id`、`tail`、`max_lines`、`timeout` 边界。
- 安全性：校验 `ssh://` 连接参数及潜在注入/权限扩大风险。
- 资源管理：校验 client 池容量与空闲淘汰、stream handle 关闭、后台线程退出。

#### 模块 5：规则引擎与预处理（`logwatch/pipeline/filter.py`、`pipeline/preprocessor/*`）
- 业务逻辑正确性：校验规则顺序必须为 `ignore -> redact -> custom_alerts -> alert_keywords`。
- 异常与错误处理：校验非法规则类型、regex 编译失败是否明确报错。
- 输入校验：校验 regex 最大长度、关键词列表类型、预处理插件加载合法性。
- 安全性：校验 redact 是否覆盖敏感字段，插件加载是否存在路径/执行风险。
- 资源管理：关注高频 regex 和插件链条对 CPU/延迟的影响。

#### 模块 6：日志/事件告警链路（`collector/log_stream.py`、`collector/event_stream.py`）
- 业务逻辑正确性：校验 run_alert_once 主流程、event action 映射（restart/die/oom）及容器刷新逻辑。
- 异常与错误处理：校验 notify/save/mute 检查失败时是否降级且不阻断主循环。
- 输入校验：校验日志行、事件字段缺失、时间戳异常时行为。
- 安全性：校验通知消息与落库 payload 对敏感文本的处理。
- 资源管理：校验队列上限、worker 数、drop 策略、重连退避、pending dedup/storm task 清理。

#### 模块 7：watcher 协调与热替换（`collector/watch_manager.py`）
- 业务逻辑正确性：校验 host 上线/下线/重载变更时 watcher 的创建、替换和回收。
- 异常与错误处理：校验替换中任一步失败的回滚与清理。
- 输入校验：校验 host 名称去重、空值过滤逻辑。
- 安全性：校验跨主机 watcher 误绑定风险。
- 资源管理：校验 stale watcher shutdown、任务 cancel 与 await 完整性。

#### 模块 8：风暴控制（`collector/storm.py`）
- 业务逻辑正确性：校验 `normal/suppressed/storm_start/storm_end` 状态迁移和计数。
- 异常与错误处理：校验 flush_due 和会话过期边界。
- 输入校验：校验 `window_seconds/threshold/suppress_minutes` 非法值。
- 安全性：校验 storm 汇总消息是否包含不应暴露的原始信息。
- 资源管理：校验 active/recent 集合增长与释放。

#### 模块 9：采样、报告与调度（`collector/sampler.py`、`host_metrics_sampler.py`、`scheduler.py`、`reports.py`）
- 业务逻辑正确性：校验采样字段正确性、报告场景切换（interval/hourly/daily/heartbeat）、heartbeat 抑制逻辑。
- 异常与错误处理：校验单 host 失败隔离、job 失败后调度器持续性。
- 输入校验：校验 `interval_seconds`、cron、阈值、窗口等配置边界。
- 安全性：校验报告文本是否泄露敏感日志/地址信息。
- 资源管理：校验调度重叠、misfire、长任务挤压及执行并发参数。

#### 模块 10：存储层与保留策略（`core/metrics_writer.py`、`core/db.py`、`core/retention_scheduler.py`）
- 业务逻辑正确性：校验 alerts/audit/send_failed/storm/mutes/metrics/host_metrics 的写读一致性。
- 异常与错误处理：校验写失败/查询失败/cleanup 失败的传播与可观测性。
- 输入校验：校验 `limit`、`timestamp`、retention 天数、mute 必填字段。
- 安全性：校验 audit redact 的覆盖范围与截断策略。
- 资源管理：校验 sqlite 连接复用、锁粒度、索引使用、cleanup 删除成本。

#### 模块 11：通知路由与通道（`notify/router.py`、`notify/policy.py`、`notify/*.py`）
- 业务逻辑正确性：校验 route rule 优先级、default channel 回退、多通道 fan-out。
- 异常与错误处理：校验 retry/backoff 与 failure_recorder 失败的隔离。
- 输入校验：校验 routing rules、channel config、message_mode 合法性。
- 安全性：校验失败消息脱敏与通道目标配置风险。
- 资源管理：校验大消息分片与高并发发送下的开销放大。

#### 模块 12：Web API（`logwatch/web/api.py`）
- 业务逻辑正确性：校验路由到 action 绑定准确性及响应结构一致性。
- 异常与错误处理：校验 403/404/400/503 映射是否精确，避免吞错。
- 输入校验：校验 Query 参数范围（尤其 `limit`、`start/end`）。
- 安全性：校验 admin/web token 隔离和 `hmac.compare_digest` 使用一致。
- 资源管理：校验大分页请求与异常重试对 DB 的压力。

#### 模块 13：WebSocket Chat 与 LLM 运行时（`web/chat.py`、`llm/agent_runtime.py`、`llm/analyzer.py`、`llm/prompts/*`）
- 业务逻辑正确性：校验 WS 鉴权、session 绑定、消息循环与 fallback 行为。
- 异常与错误处理：校验 runtime 构建失败、invoke 失败是否稳定降级。
- 输入校验：校验 token 来源（header/subprotocol）、template 名称、scene/context 缺失处理。
- 安全性：校验 Origin 检查、query token 拒绝、异常信息外泄风险。
- 资源管理：校验连接数、每连接消息上限、超长 prompt/响应的处理。

#### 模块 14：LLM 工具与权限（`llm/tools.py`、`llm/permissions.py`）
- 业务逻辑正确性：校验工具参数解析、host/container 解析、结果结构。
- 异常与错误处理：校验 `rate_limited/denied/error` 三类路径审计记录是否完整。
- 输入校验：校验 `hours/max_lines/limit/timeout/category` 边界。
- 安全性：重点校验危险工具 `restart_container` 的 allowlist + 显式确认是否可绕过。
- 资源管理：校验 RateLimiter bucket 清理、上限、TTL 与内存增长风险。

#### 模块 15：前端控制台（`web/frontend.py`、`web/static/*`）
- 业务逻辑正确性：校验 token 注入、主机轮询、容器展示、WS 重连流程。
- 异常与错误处理：校验 REST/WS 失败提示是否可定位问题。
- 输入校验：校验 host path 构造与 session id 生成逻辑。
- 安全性：校验前端错误展示脱敏与 token 暴露风险。
- 资源管理：校验轮询与 WS 重连对后端连接压力。

### C. 关键链路审计

至少覆盖以下 3 条核心链路，并在每个节点给出具体检查点：

```text
入口 -> 参数校验 -> 认证鉴权 -> 业务处理 -> 数据持久化 -> 响应返回
 ↓        ↓          ↓          ↓            ↓            ↓
检查点   检查点     检查点      检查点       检查点       检查点
```

#### 链路 1：指标查询链路
```text
GET /api/hosts/{host}/metrics/{container}
 -> Query(limit/start/end)
 -> verify_web_token
 -> main.query_metric_points
 -> MetricsSqliteWriter.query_metrics
 -> core.db.query_metrics
 -> JSON 响应
```
检查点：
- 参数范围与时间格式是否严格验证。
- host 不存在、时间非法是否返回正确状态码。
- SQL where/order/limit 是否符合预期且可走索引。
- 响应字段语义与 DB 字段一致。

#### 链路 2：日志/事件告警链路
```text
Docker logs/events
 -> WatchManager / LogStreamWatcher / EventStreamWatcher
 -> run_alert_once
 -> apply_rules -> cooldown -> mute -> storm
 -> analyze_with_template / render_output
 -> NotifyRouter.send
 -> MetricsSqliteWriter.write_alert/write_storm_event
```
检查点：
- 规则顺序、分类结果、去重窗口是否正确。
- 静默命中时是否确实阻断通知与落库策略符合设计。
- storm_start/storm_end 是否成对并具备可追踪性。
- 通知失败与落库失败是否会导致状态不一致。

#### 链路 3：危险工具调用链路
```text
/ws/chat (或 Telegram runtime)
 -> AgentRuntime.invoke_text
 -> llm/tools.guarded_invoke
 -> ensure_tool_allowed (host allowlist + confirmation)
 -> restart_container_for_host
 -> write_audit(ok/denied/error)
 -> chat 返回结果
```
检查点：
- 危险工具必须具备 host allowlist + 显式确认，且不可绕过。
- 限流命中、权限拒绝、执行异常都必须写审计日志。
- 重启结果与异常信息是否存在敏感信息泄露。

### D. 潜在风险分析（重点排查）
- 并发与竞态条件：watcher 刷新与替换、dedup/storm 异步任务、reload 与生命周期并发。
- 数据一致性与事务完整性：通知与落库顺序、部分失败补偿、retention 清理窗口。
- 性能瓶颈：高频 regex、日志队列堆积、SQLite 锁竞争、连接池容量与 stream 资源占用。
- 密钥/凭证管理：Bearer token、SSH key、通知目标、错误日志脱敏。
- 依赖安全：FastAPI/docker/paramiko/python-telegram-bot/deepagents 供应链与版本漏洞。
- 项目特有风险：
  - 热重载失败后新旧 runtime/scheduler 混用。
  - 冷却/静默/风暴组合导致漏告警。
  - LLM 工具误授权引发生产容器操作风险。
  - 动态预处理器加载导致运行时不可控行为。

### E. 问题分级标准

| 级别 | 定义 | 典型场景 |
|------|------|---------|
| **P0 致命** | 必须立即修复，阻塞交付 | 安全漏洞、数据丢失/损坏、资金风险、服务完全不可用 |
| **P1 严重** | 应尽快修复，影响核心功能 | 核心逻辑错误、严重性能问题、数据不一致、关键边界缺失 |
| **P2 一般** | 需要修复，但不阻塞交付 | 非核心功能缺陷、异常处理不完整、测试覆盖不足 |
| **P3 建议** | 改进项，提升质量和可维护性 | 代码规范、命名优化、死代码清理、架构改进建议 |

### F. 输出格式

每个问题必须使用以下格式：

```text
[P等级] 模块名 - 问题标题

位置：文件路径:行号
描述：具体问题是什么
影响：会导致什么后果
建议：如何修复
```

审计报告结尾必须包含：
- **问题统计**：各级别问题数量汇总
- **高风险模块排名**：按问题密度排序
- **优先修复建议**：最应该先修的 3-5 个问题

### 执行约束
- 仅以当前仓库代码为准，不基于愿景或猜测。
- 每个结论必须给出文件与行号。
- 避免重复报同一问题；跨模块影响放在“影响”中说明链路关联。
- 优先覆盖高风险模块，再向辅助模块扩展。

---

以上为 LogWatch 项目定制审计提示词。
