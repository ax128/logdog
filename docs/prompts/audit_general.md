# LogWatch 项目审计提示词（可直接执行）

你是一位资深代码审计工程师。请仅基于当前仓库实现执行审计，不要根据设计愿景或历史文档臆测行为。审计阶段默认只读，不修改业务代码与测试代码。

## A. 项目概要
LogWatch 是一个 Python 3.11+ 的容器监控与告警服务，核心通过 `logwatch/main.py:create_app` 装配 FastAPI API、WebSocket Chat、调度器、watcher、通知路由与持久化组件。系统主链路是 Docker 日志/事件输入后，经过规则匹配、冷却/静默/风暴抑制、模板分析与通知发送，再写入 SQLite（`alerts/storm_events/metrics/host_metrics/mutes/audit/send_failed`）。外部集成点包含 Docker SDK、SSH/Paramiko、SQLite、Telegram/Wechat/Weixin/Wecom 通道，以及可选 `deepagents` 运行时。鉴权边界分为 web token 与 admin token，HTTP 与 WS 均要求 Bearer 令牌（WS 同时支持 `Sec-WebSocket-Protocol: bearer,<token>`）。项目测试体系为 `pytest + pytest-asyncio + httpx`，仓库内 CI 工作流与静态检查配置未识别到。

## B. 功能板块逐一审计清单

### B1. 模块全量索引
1. 应用装配与生命周期：`logwatch/main.py`
2. HTTP API：`logwatch/web/api.py`
3. WebSocket Chat：`logwatch/web/chat.py`
4. 前端控制台：`logwatch/web/frontend.py` + `logwatch/web/static/*`
5. 主机管理与连接状态：`logwatch/core/host_manager.py`
6. Docker 连接池与流式采集：`logwatch/core/docker_connector.py`
7. 持久化与数据库模型：`logwatch/core/metrics_writer.py` + `logwatch/core/db.py`
8. 规则与冷却：`logwatch/pipeline/filter.py` + `logwatch/pipeline/cooldown.py`
9. 日志告警主链路：`logwatch/collector/log_stream.py`
10. 事件告警链路：`logwatch/collector/event_stream.py`
11. watcher 编排：`logwatch/collector/watch_manager.py`
12. 调度与报告：`logwatch/collector/scheduler.py` + `logwatch/collector/reports.py`
13. 宿主机指标与 SSH 探测：`logwatch/collector/host_metrics_probe.py` + `host_metrics_sampler.py`
14. 通知与路由策略：`logwatch/notify/*`
15. LLM 运行时与模板分析：`logwatch/llm/agent_runtime.py` + `analyzer.py` + `prompts/*`
16. LLM 工具与权限：`logwatch/llm/tools.py` + `permissions.py`

### B2. Top 8 高风险模块深度审计（按风险排序）

#### 1) 鉴权与访问边界（`logwatch/web/api.py`, `logwatch/web/chat.py`）
- 业务逻辑正确性：校验 web/admin token 的路由权限边界是否与接口职责一致，确认 `/api/reload` 与 `/api/send-failed` 仅 admin 可访问。
- 异常与错误处理：核查 `PermissionError/KeyError/ValueError` 到 HTTP 状态码映射是否稳定且不误报。
- 输入校验：核查 query 参数（`limit/start/end/host/container/category`）边界是否在入口统一限制。
- 安全性：核查 Bearer 解析、`hmac.compare_digest` 使用、WS token 来源（header/subprotocol/query）拒绝策略、Origin 校验是否可绕过。
- 资源管理：核查 WS 断开与异常路径是否正确关闭连接，避免僵尸连接。

#### 2) 热重载与生命周期原子性（`logwatch/main.py`）
- 业务逻辑正确性：核查 `default_reload_action` 对 host/router/scheduler/chat_runtime/telegram_runtime 的切换顺序是否保持一致性。
- 异常与错误处理：核查 reload 失败时回滚逻辑是否覆盖 host_manager/watch_manager/scheduler/runtime 全链路。
- 输入校验：核查配置加载路径与结构校验（非 dict、字段类型错误）是否立即失败。
- 安全性：核查 reload 失败通知与错误消息脱敏（token/password/api_key）是否全覆盖。
- 资源管理：核查启动/关闭顺序，确保 watcher、scheduler、docker pool、sqlite 连接可回收。

#### 3) 危险工具权限与审计（`logwatch/llm/tools.py`, `logwatch/llm/permissions.py`）
- 业务逻辑正确性：核查工具注册、参数解析、审计写入（`write_audit`）状态字段是否一致。
- 异常与错误处理：核查 `rate_limited/denied/error` 分支是否都落审计并返回可诊断错误。
- 输入校验：核查 `hours/max_lines/limit/timeout/container_id/host` 的范围、类型、必填约束。
- 安全性：核查 `restart_container` 的 allowlist + 显式确认字段要求是否可绕过。
- 资源管理：核查 RateLimiter 桶清理与上限约束，防止长期运行内存膨胀。

#### 4) 日志告警正确性（`logwatch/collector/log_stream.py`, `logwatch/pipeline/filter.py`, `logwatch/pipeline/cooldown.py`）
- 业务逻辑正确性：核查规则执行顺序（ignore/redact/custom_alerts/keywords）、category 判定与 dedup 汇总语义。
- 异常与错误处理：核查通知失败、落库失败、mute 查询失败时是否仅降级且保留可观测日志。
- 输入校验：核查 regex 长度、规则结构、关键词列表类型约束。
- 安全性：核查 redaction 是否在通知与持久化前统一生效，避免敏感日志泄露。
- 资源管理：核查队列上限、worker 并发、`drop_when_full`、pending dedup/storm task 清理。

#### 5) 事件流与 watcher 稳定性（`logwatch/collector/event_stream.py`, `watch_manager.py`）
- 业务逻辑正确性：核查事件 action 到告警消息/category 的映射准确性与 refresh 触发条件。
- 异常与错误处理：核查 stream 中断、handler 抛错后的重连退避与自愈能力。
- 输入校验：核查 event 字段缺失（`container_id/action/time`）下行为是否可预测。
- 安全性：核查跨 host refresh/reload 是否可能误影响其他主机 watcher。
- 资源管理：核查 watcher 替换、shutdown、reload 时旧 task 取消与回收是否完整。

#### 6) 通知路由与失败补偿（`logwatch/notify/router.py`, `notify/policy.py`, `main.py` 通知装配段）
- 业务逻辑正确性：核查 routing rule priority、default channel、命名通道映射与 host 允许通道限制。
- 异常与错误处理：核查重试退避、部分通道失败、failure_recorder 异常时的降级策略。
- 输入校验：核查 routing/channel 配置类型检查（list/dict/bool）是否严格。
- 安全性：核查失败消息与错误文本脱敏策略是否覆盖 token/password/api_key/bearer。
- 资源管理：核查多通道并发 fan-out 的重试放大与消息分片策略。

#### 7) 数据一致性与持久化（`logwatch/core/metrics_writer.py`, `core/db.py`）
- 业务逻辑正确性：核查 alerts/mutes/storm/metrics/host_metrics/audit/send_failed 的写入与查询字段语义一致性。
- 异常与错误处理：核查连接初始化失败、写入失败、查询失败、cleanup 失败的上抛与可观测性。
- 输入校验：核查 timestamp/limit/retention/mute 必填字段校验是否完整。
- 安全性：核查审计与失败记录的脱敏、截断、JSON 解码兜底路径。
- 资源管理：核查连接复用、读写锁、PRAGMA、索引命中与 retention 删除成本。

#### 8) 外部连接与系统探测（`logwatch/core/docker_connector.py`, `core/host_manager.py`, `collector/host_metrics_probe.py`）
- 业务逻辑正确性：核查 host 状态机（重试/熔断/半开恢复）与 Docker 客户端池复用逻辑。
- 异常与错误处理：核查 SSH/Docker/network/auth 错误分类与恢复路径是否一致。
- 输入校验：核查 host url/container id/timeout/tail/max_lines/ssh 参数合法性。
- 安全性：核查 SSH key 权限检查、strict host key 策略、远程命令白名单与 root 登录风险提示。
- 资源管理：核查 stream handle/client close、空闲连接淘汰、容量控制与 `close_all` 释放。

## C. 关键链路审计（端到端）

### C0. 通用节点检查框架
```text
入口 ──→ 参数校验 ──→ 认证鉴权 ──→ 业务处理 ──→ 数据持久化 ──→ 响应返回
  │          │            │            │              │              │
  ▼          ▼            ▼            ▼              ▼              ▼
注入/畸形  类型/范围    越权/伪造   状态一致性    事务/回滚     结构/泄露
```

### C1. 指标查询链路（HTTP 读链路）
```text
GET /api/hosts/{host}/metrics/{container}
→ Query(limit/start/end)
→ verify_web_token
→ main.query_metric_points
→ MetricsSqliteWriter.query_metrics
→ core.db.query_metrics
→ JSON points
```
- 检查点：host 是否存在、limit 上界是否生效、时间解析失败是否 400、SQL 过滤条件是否准确、响应字段是否稳定。

### C2. 日志告警链路（实时写链路）
```text
docker stream logs
→ LogStreamWatcher.watch_container
→ run_alert_once
→ apply_rules + cooldown + mute + storm
→ analyze_with_template / script fallback
→ NotifyRouter.send
→ MetricsSqliteWriter.write_alert / write_storm_event
```
- 检查点：规则顺序与类别准确性、静默与风暴抑制是否导致漏告警、通知失败与落库失败是否一致可追踪、敏感信息是否先脱敏后输出。

### C3. 配置热重载链路（高风险状态链路）
```text
POST /api/reload
→ verify_admin_token
→ default_reload_action(load config + rebuild runtime)
→ 成功: 替换 app.state 组件
→ 失败: 回滚旧组件 + reload failure notice
→ 返回 ok/summary 或 503
```
- 检查点：切换是否原子、回滚是否完整、degraded 标记是否准确、失败通知是否脱敏、removed host 是否正确标注 `removed_requires_restart`。

### C4. LLM 危险动作链路（工具写操作链路）
```text
WS /ws/chat
→ AgentRuntime.invoke_text
→ tool registry invoke(restart_container)
→ rate limit + permission policy + explicit confirmation
→ docker restart
→ write_audit(ok/denied/error)
→ chat reply
```
- 检查点：权限拒绝是否先于执行、host allowlist 是否强制、确认字段是否绕过、审计日志是否完整且脱敏。

## D. 潜在风险分析（重点排查）
- 并发与竞态条件：watcher reload、dedup/storm 延迟任务、scheduler 与 reload 并发、host 状态切换竞态。
- 数据一致性与事务完整性：通知成功/落库失败或通知失败/落库成功导致的状态分裂，retention 清理窗口边界。
- 性能瓶颈：高频正则匹配、日志队列堆积、SQLite 锁等待、Docker 客户端池不足、WS 长连接压力。
- 密钥/凭证管理：WEB token、admin token、SSH key、Telegram token、Webhook 目标在日志/错误中的泄露。
- 依赖安全：`fastapi`、`docker`、`paramiko`、`python-telegram-bot`、`deepagents` 的已知漏洞与供应链风险。
- 项目特有风险：热重载失败导致部分组件新旧混用；告警风暴抑制参数错误导致持续漏报；LLM 工具触发危险操作的权限绕过。

## E. 问题分级标准

| 级别 | 定义 | 典型场景 |
|------|------|---------|
| **P0 致命** | 必须立即修复，阻塞交付 | 安全漏洞、数据丢失/损坏、资金风险、服务完全不可用 |
| **P1 严重** | 应尽快修复，影响核心功能 | 核心逻辑错误、严重性能问题、数据不一致、关键边界缺失 |
| **P2 一般** | 需要修复，但不阻塞交付 | 非核心功能缺陷、异常处理不完整、测试覆盖不足 |
| **P3 建议** | 改进项，提升质量和可维护性 | 代码规范、命名优化、死代码清理、架构改进建议 |

## F. 审计输出格式

每个问题必须严格按以下格式输出：

```text
[P等级] 模块名 - 问题标题

位置：文件路径:行号
描述：具体问题是什么
影响：会导致什么后果
建议：如何修复
```

审计报告末尾必须包含：
- 问题统计：各级别问题数量汇总。
- 高风险模块排名：按问题密度排序。
- 优先修复建议：最应该先修的 3-5 个问题。
- 覆盖范围声明：已覆盖模块、未覆盖模块及未覆盖原因。

## 执行约束
- 仅以当前仓库代码为准，不引用仓库外假设。
- 不得编造未出现的模块、链路或中间件；识别不到请明确写“未识别到”。
- 结论必须可定位到实际文件与行号。
- 默认只读审计；若必须建议改动，仅给出修复建议，不直接改代码。
