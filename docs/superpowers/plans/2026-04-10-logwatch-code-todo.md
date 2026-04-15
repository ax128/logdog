# LogWatch Code Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `2026-04-10-logwatch-design.md` 落地为可运行的 LogWatch MVP，优先实现安全边界、监控主链路和可验证测试。

**Architecture:** 采用 FastAPI + asyncio + SQLite 的单进程服务，按 `core/collector/pipeline/llm/notify/web` 分层实现。先完成配置、存储、连接管理和规则链，再接入 LLM/通知/Web API。所有高风险入口（WebSocket 认证、配置重载、危险操作）先实现硬约束再开放功能。

**Tech Stack:** Python >=3.11, FastAPI, docker-py, aiosqlite, APScheduler, python-telegram-bot, pytest, httpx

---

## Assumptions

- 代码根目录为 `logwatch/`（若实际包名不同，路径按同等职责平移）。
- 测试框架使用 `pytest`，异步测试使用 `pytest-asyncio`。
- 当前仓库尚未存在同名实现文件，允许按本计划创建新文件。

## File Map

### Create

- `logwatch/main.py`：FastAPI 启动入口、生命周期管理
- `logwatch/core/config.py`：配置 schema、加载、合并、热更新验证
- `logwatch/core/db.py`：SQLite 初始化、查询接口、保留期清理
- `logwatch/core/host_manager.py`：多主机连接、状态维护、重连
- `logwatch/collector/log_stream.py`：日志流订阅与容器级弹性
- `logwatch/collector/metrics.py`：Docker stats 采样转换
- `logwatch/collector/sampler.py`：APScheduler 周期任务
- `logwatch/pipeline/filter.py`：ignore/redact/custom_alerts/keywords 规则执行
- `logwatch/pipeline/cooldown.py`：分类冷却窗口
- `logwatch/pipeline/preprocessor/base.py`：预处理基类
- `logwatch/pipeline/preprocessor/default.py`：默认预处理实现
- `logwatch/pipeline/preprocessor/loader.py`：用户预处理加载与校验
- `logwatch/llm/analyzer.py`：LLM 调用、模板校验、降级策略
- `logwatch/llm/prompts/base.py`：Prompt 模板基类（scene 校验）
- `logwatch/llm/prompts/loader.py`：模板加载、内置回退
- `logwatch/llm/tools.py`：Agent 工具定义与参数约束
- `logwatch/notify/base.py`：通知通道基类
- `logwatch/notify/router.py`：多通道路由与并发发送
- `logwatch/notify/telegram.py`：Telegram 发送/接收
- `logwatch/notify/wechat.py`：企业微信 webhook 发送
- `logwatch/web/api.py`：REST API（含 `/api/reload`）
- `logwatch/web/chat.py`：WebSocket chat（header/subprotocol 认证）
- `templates/prompts/default_alert.md`
- `templates/prompts/default_interval.md`
- `templates/prompts/default_hourly.md`
- `templates/prompts/default_daily.md`
- `templates/prompts/default_heartbeat.md`
- `tests/core/test_config_merge.py`
- `tests/core/test_reload_auth.py`
- `tests/core/test_db_retention.py`
- `tests/pipeline/test_filter_rules.py`
- `tests/pipeline/test_cooldown.py`
- `tests/llm/test_prompt_validate.py`
- `tests/llm/test_fallback_plaintext.py`
- `tests/web/test_ws_auth.py`
- `tests/web/test_api_reload.py`
- `tests/tools/test_tool_limits.py`
- `tests/notify/test_router_retry.py`

### Modify

- `docs/superpowers/specs/2026-04-10-logwatch-design.md`：实现后补充“已实现能力/限制/已验证命令”

---

### Task 1: 初始化工程骨架与依赖

**Files:**
- Create: `logwatch/main.py`
- Create: `pyproject.toml`
- Test: `tests/test_smoke_import.py`

- [ ] **Step 1: 写失败测试（项目可导入）**

```python
def test_import_main():
    import logwatch.main as m
    assert hasattr(m, "create_app")
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_smoke_import.py -v`  
Expected: FAIL (`ModuleNotFoundError: No module named 'logwatch'`)

- [ ] **Step 3: 最小实现包结构与入口**

```python
# logwatch/main.py
from fastapi import FastAPI

def create_app() -> FastAPI:
    app = FastAPI(title="LogWatch")
    return app
```

- [ ] **Step 4: 运行测试确认通过**

Run: `pytest tests/test_smoke_import.py -v`  
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add pyproject.toml logwatch/main.py tests/test_smoke_import.py
git commit -m "chore: bootstrap logwatch package and app factory"
```

### Task 2: 配置加载与合并规则（含 schedules_mode）

**Files:**
- Create: `logwatch/core/config.py`
- Test: `tests/core/test_config_merge.py`

- [ ] **Step 1: 写失败测试（host 覆盖 + schedules_mode）**

```python
def test_merge_with_replace_schedule_mode():
    merged = merge_host_config(
        defaults={"schedules_mode": "append", "schedules": [{"name": "five", "interval_seconds": 300}]},
        host={"schedules_mode": "replace", "schedules": [{"name": "fast", "interval_seconds": 60}]},
    )
    assert [x["name"] for x in merged["schedules"]] == ["fast"]
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/core/test_config_merge.py::test_merge_with_replace_schedule_mode -v`  
Expected: FAIL (`NameError: merge_host_config is not defined`)

- [ ] **Step 3: 实现最小合并逻辑**

```python
def merge_host_config(defaults: dict, host: dict) -> dict:
    merged = {**defaults, **host}
    mode = host.get("schedules_mode", defaults.get("schedules_mode", "append"))
    if mode == "replace":
        merged["schedules"] = host.get("schedules", [])
    return merged
```

- [ ] **Step 4: 增加规则并完善测试**

Run: `pytest tests/core/test_config_merge.py -v`  
Expected: PASS（覆盖 include/exclude、rules 合并、notify 替换、schedules_mode）

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/config.py tests/core/test_config_merge.py
git commit -m "feat(config): implement host merge and schedules_mode behavior"
```

### Task 3: SQLite 模型、保留期与审计脱敏

**Files:**
- Create: `logwatch/core/db.py`
- Test: `tests/core/test_db_retention.py`

- [ ] **Step 1: 写失败测试（alerts/audit/send_failed 清理）**

```python
async def test_retention_cleanup_removes_expired_rows(db):
    await insert_old_rows(db)
    await run_retention_cleanup(db, {"alerts_days": 30, "audit_days": 30, "send_failed_days": 14})
    assert await count_expired_rows(db) == 0
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/core/test_db_retention.py -v`  
Expected: FAIL（清理函数不存在）

- [ ] **Step 3: 实现 DB 初始化与清理**

```python
async def run_retention_cleanup(conn, retention: dict) -> None:
    await conn.execute("DELETE FROM alerts WHERE created_at < datetime('now', ?)", (f"-{retention['alerts_days']} days",))
    await conn.execute("DELETE FROM audit_log WHERE created_at < datetime('now', ?)", (f"-{retention['audit_days']} days",))
    await conn.execute("DELETE FROM send_failed WHERE created_at < datetime('now', ?)", (f"-{retention['send_failed_days']} days",))
    await conn.commit()
```

- [ ] **Step 4: 加审计脱敏单元测试并通过**

Run: `pytest tests/core/test_db_retention.py -v`  
Expected: PASS（含 token/password 脱敏）

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/db.py tests/core/test_db_retention.py
git commit -m "feat(db): add retention cleanup and audit redaction"
```

### Task 4: 规则引擎 + 冷却期主链路

**Files:**
- Create: `logwatch/pipeline/filter.py`
- Create: `logwatch/pipeline/cooldown.py`
- Test: `tests/pipeline/test_filter_rules.py`
- Test: `tests/pipeline/test_cooldown.py`

- [ ] **Step 1: 写失败测试（ignore/redact/custom_alerts 顺序）**

```python
def test_rule_order_ignore_then_redact_then_alert():
    result = apply_rules("Bearer abc ERROR", config=sample_config())
    assert result.redacted_line == "Bearer ***REDACTED*** ERROR"
    assert result.matched_category == "ERROR"
```

- [ ] **Step 2: 写失败测试（冷却窗口抑制）**

```python
def test_cooldown_blocks_same_host_container_category():
    cd = CooldownStore()
    assert cd.allow("h1", "c1", "ERROR", now=0)
    assert not cd.allow("h1", "c1", "ERROR", now=10)
```

- [ ] **Step 3: 实现最小规则与冷却逻辑**

```python
def apply_rules(line: str, config: dict) -> RuleResult:
    if any(re.search(x["pattern"], line) for x in config["rules"]["ignore"]):
        return RuleResult(triggered=False, redacted_line=line, matched_category=None)
    redacted = line
    for x in config["rules"]["redact"]:
        redacted = re.sub(x["pattern"], x["replace"], redacted)
    if re.search(r"(?i)fatal|panic|error|failed|timeout", redacted):
        return RuleResult(triggered=True, redacted_line=redacted, matched_category="ERROR")
    return RuleResult(triggered=False, redacted_line=redacted, matched_category=None)
```

- [ ] **Step 4: 跑测试**

Run: `pytest tests/pipeline/test_filter_rules.py tests/pipeline/test_cooldown.py -v`  
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/pipeline/filter.py logwatch/pipeline/cooldown.py tests/pipeline/
git commit -m "feat(pipeline): implement rule engine and cooldown control"
```

### Task 5: Prompt 模板校验与 LLM 降级

**Files:**
- Create: `logwatch/llm/prompts/base.py`
- Create: `logwatch/llm/prompts/loader.py`
- Create: `logwatch/llm/analyzer.py`
- Create: `tests/llm/test_prompt_validate.py`
- Create: `tests/llm/test_fallback_plaintext.py`

- [ ] **Step 1: 写失败测试（heartbeat 场景不要求 logs）**

```python
class DummyPrompt(BasePromptTemplate):
    def render(self, context: dict) -> str:
        return "ok"

def test_heartbeat_required_vars():
    t = DummyPrompt()
    assert "logs" not in t.required_vars("heartbeat")
```

- [ ] **Step 2: 写失败测试（模板失败时降级纯文本）**

```python
def test_fallback_to_plaintext_when_prompt_invalid():
    msg = analyze_with_template(scene="alert", context={}, template="broken")
    assert msg.startswith("[FALLBACK]")
```

- [ ] **Step 3: 实现场景化 required_vars + 回退机制**

```python
def analyze_with_template(scene: str, context: dict, template: str) -> str:
    tpl = load_template(template_name=template)
    missing = tpl.validate(scene, context)
    if missing:
        return f"[FALLBACK] scene={scene} missing={','.join(missing)}"
    rendered = tpl.render(context)
    if not rendered.strip():
        return f"[FALLBACK] scene={scene} empty-rendered-template"
    return rendered
```

- [ ] **Step 4: 跑测试**

Run: `pytest tests/llm/test_prompt_validate.py tests/llm/test_fallback_plaintext.py -v`  
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/llm tests/llm
git commit -m "feat(llm): add scene-based template validation and fallback path"
```

### Task 6: Web 认证与配置热更新管理面

**Files:**
- Create: `logwatch/web/api.py`
- Create: `logwatch/web/chat.py`
- Test: `tests/web/test_api_reload.py`
- Test: `tests/web/test_ws_auth.py`
- Test: `tests/core/test_reload_auth.py`

- [ ] **Step 1: 写失败测试（/api/reload 必须 admin token）**

```python
def test_reload_requires_admin_token(client):
    r = client.post("/api/reload", headers={"Authorization": "Bearer user-token"})
    assert r.status_code == 403
```

- [ ] **Step 2: 写失败测试（WebSocket 禁止 query token）**

```python
def test_ws_rejects_query_token(client):
    with pytest.raises(WebSocketDisconnect):
        client.websocket_connect("/ws/chat?token=abc")
```

- [ ] **Step 3: 实现 API 与 WS 认证**

```python
def verify_admin_token(auth_header: str, admin_token: str) -> None:
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("missing bearer token")
    token = auth_header.split(" ", 1)[1].strip()
    if token != admin_token:
        raise PermissionError("invalid admin token")
```

- [ ] **Step 4: 跑测试**

Run: `pytest tests/web/test_api_reload.py tests/web/test_ws_auth.py tests/core/test_reload_auth.py -v`  
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/web tests/web tests/core/test_reload_auth.py
git commit -m "feat(web): secure reload API and websocket auth channel"
```

### Task 7: Agent 工具限流与参数上限

**Files:**
- Create: `logwatch/llm/tools.py`
- Create: `tests/tools/test_tool_limits.py`

- [ ] **Step 1: 写失败测试（query_logs 限制）**

```python
def test_query_logs_range_limit():
    with pytest.raises(ValueError):
        validate_query_logs_args(hours=72, max_hours=24)
```

- [ ] **Step 2: 写失败测试（每用户每分钟限流）**

```python
def test_rate_limit_per_user():
    rl = RateLimiter(limit=2, window_seconds=60)
    assert rl.allow("u1")
    assert rl.allow("u1")
    assert not rl.allow("u1")
```

- [ ] **Step 3: 实现限流器与参数校验**

```python
from collections import defaultdict, deque
import time

class RateLimiter:
    def __init__(self, limit: int, window_seconds: int) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, user_id: str) -> bool:
        now = time.time()
        q = self._buckets[user_id]
        while q and (now - q[0]) > self.window_seconds:
            q.popleft()
        if len(q) >= self.limit:
            return False
        q.append(now)
        return True
```

- [ ] **Step 4: 跑测试**

Run: `pytest tests/tools/test_tool_limits.py -v`  
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/llm/tools.py tests/tools/test_tool_limits.py
git commit -m "feat(agent): enforce tool limits and per-user rate limit"
```

### Task 8: 通知路由（并发发送、分段、重试）

**Files:**
- Create: `logwatch/notify/base.py`
- Create: `logwatch/notify/router.py`
- Create: `logwatch/notify/telegram.py`
- Create: `logwatch/notify/wechat.py`
- Create: `tests/notify/test_router_retry.py`

- [ ] **Step 1: 写失败测试（失败重试一次并记录）**

```python
async def test_retry_once_and_record_failure():
    ok = await router.send("h1", "msg", "ERROR")
    assert ok is False
    assert await has_send_failed_row("h1")
```

- [ ] **Step 2: 实现路由并发 + 重试**

```python
results = await asyncio.gather(*tasks, return_exceptions=True)
```

- [ ] **Step 3: 加消息分段测试（4096 限制）**

Run: `pytest tests/notify/test_router_retry.py -v`  
Expected: PASS（含 Telegram/WeChat 分段场景）

- [ ] **Step 4: 提交**

```bash
git add logwatch/notify tests/notify/test_router_retry.py
git commit -m "feat(notify): add concurrent routing retry and message chunking"
```

### Task 9: 集成主流程（日志触发 -> 分析 -> 推送 -> 落库）

**Files:**
- Create: `logwatch/collector/log_stream.py`
- Modify: `logwatch/main.py`
- Test: `tests/integration/test_alert_pipeline.py`

- [ ] **Step 1: 写失败集成测试**

```python
async def test_alert_pipeline_end_to_end(fake_container_log):
    result = await run_alert_once(fake_container_log)
    assert result.pushed is True
    assert result.saved is True
```

- [ ] **Step 2: 实现最小联通逻辑**

```python
async def run_alert_once(line: str):
    # rule -> cooldown -> llm -> notify -> db
    rule = apply_rules(line, config=load_runtime_config())
    if not rule.triggered:
        return AlertRunResult(pushed=False, saved=False)
    if not cooldown_allow(rule.host, rule.container_id, rule.category):
        return AlertRunResult(pushed=False, saved=False)
    analysis = analyze_with_template("alert", rule.context, "default_alert")
    pushed = await notify_router_send(rule.host, analysis, rule.severity)
    saved = await save_alert_history(rule, analysis, pushed)
    return AlertRunResult(pushed=pushed, saved=saved)
```

- [ ] **Step 3: 跑测试**

Run: `pytest tests/integration/test_alert_pipeline.py -v`  
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add logwatch/main.py logwatch/collector/log_stream.py tests/integration/test_alert_pipeline.py
git commit -m "feat(pipeline): wire end-to-end alert processing flow"
```

### Task 10: 文档回填与验收命令

**Files:**
- Modify: `docs/superpowers/specs/2026-04-10-logwatch-design.md`
- Create: `docs/superpowers/plans/2026-04-10-logwatch-verification.md`

- [ ] **Step 1: 回填设计文档“已实现/未实现”**

```md
## 实现状态（2026-04-10）
- ✅ 已实现: 配置合并、规则引擎、冷却期、模板校验、Web 认证、工具限流
- ⏳ 未实现: 多主机日报 SubAgent 并行聚合、Dozzle 远程配置自动同步
```

- [ ] **Step 2: 写验收脚本清单**

Run:
`pytest -q`  
`pytest tests/integration -q`  
`WEB_AUTH_TOKEN=web-token WEB_ADMIN_TOKEN=admin-token uvicorn logwatch.main:create_app --factory --port 8000`

Expected:
- 单元测试通过
- 集成测试通过
- 服务可启动并返回 `/api/hosts` 200

- [ ] **Step 3: 提交**

```bash
git add docs/superpowers/specs/2026-04-10-logwatch-design.md docs/superpowers/plans/2026-04-10-logwatch-verification.md
git commit -m "docs: add implementation status and verification checklist"
```

---

## Milestone Checklist

- [x] M1 安全基线完成：`/api/reload` admin token + WS 非 query token + 审计脱敏
- [x] M2 规则链完成：ignore/redact/custom_alerts/cooldown 可用
- [x] M3 LLM 降级完成：模板缺失不阻塞推送
- [x] M4 通知链完成：并发发送、分段、重试、失败落库（默认写入 `send_failed`，支持自定义 failure_recorder）
- [x] M5 E2E 可跑通：日志触发到推送全流程通过集成测试

## Risks

- `deepagents` API 与设计文档存在偏差，需先做 API spike。
- 远程 Docker 连接在不同主机 OpenSSH 配置下行为可能不同，需做真实主机验证。
- SQLite 在高并发告警风暴下可能成为瓶颈，需跟踪写队列长度与清理耗时。

## Out of Scope (MVP)

- Dozzle 远程主机自动同步配置
- 复杂 RBAC 用户体系
- PostgreSQL/Redis 迁移
