# LogWatch High-Impact Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不破坏现有行为的前提下，优先完成 6 项高收益优化：SQLite 吞吐、告警查询下推、报告链路去 N+1、日志告警背压、调度防重叠、多服务器多容器灵活通知路由。

**Architecture:** 采用“先可观测、再提速、最后稳态”的增量策略。第一批改动集中在数据库与调度链路，降低锁竞争与抖动；第二批改动集中在告警与报告链路，减少不必要查询与内存过滤；第三批改动集中在日志消费背压，保证高流量场景下服务退化可控；第四批改动集中在通知策略引擎，实现 host/container/category 维度的渠道与消息模式灵活路由。所有改动坚持 TDD、最小改动、可回滚。

**Tech Stack:** Python 3.11+, FastAPI, aiosqlite, APScheduler, pytest/pytest-asyncio

---

## Scope & Delivery Order

- P0（先做，收益最高）
  - SQLite 写路径与连接参数优化（WAL + busy_timeout + 批量写）
  - `/api/alerts` 查询下推到 SQL（避免内存过滤）
  - Scheduler 防重叠/错过触发补偿参数
- P1（紧随其后）
  - 报告链路去 N+1（按 host 窗口批量取指标）
  - 日志告警链路背压与丢弃统计
  - 多服务器多容器通知策略（指定容器走特定渠道，其余走默认）

## File Structure (planned)

### Create

- `logwatch/notify/policy.py`：新增通知规则解析与匹配（host/container/category）。
- `tests/notify/test_routing_policy.py`：通知策略匹配与优先级测试。

### Modify

- `logwatch/core/config.py`：新增 runtime 优化配置解析（sqlite 与 scheduler）。
- `logwatch/core/db.py`：新增 SQLite PRAGMA 初始化、批量写接口、alerts 结构化字段与索引、批量指标查询接口。
- `logwatch/core/metrics_writer.py`：新增批量写 API、读写锁拆分、连接初始化 PRAGMA 注入。
- `logwatch/main.py`：下推 `/api/alerts` 查询参数，注入 report bulk-query 与 scheduler 配置。
- `logwatch/collector/reports.py`：新增 host 级批量指标读取分支，消除 per-container N+1。
- `logwatch/collector/log_stream.py`：新增有界队列 + worker 背压模型，提供 dropped 统计。
- `logwatch/collector/scheduler.py`：统一 job 默认参数（max_instances/coalesce/misfire_grace_time）。
- `logwatch/notify/router.py`：支持基于上下文的规则选路与默认回退。
- `config/logwatch.yaml.example`：新增 sqlite/scheduler/watcher 优化配置示例。
- `README.md`：更新优化配置说明与容量建议。

### Test (modify/create)

- `tests/core/test_config_merge.py`
- `tests/core/test_metrics_db.py`
- `tests/core/test_metrics_writer.py`
- `tests/web/test_api_monitoring.py`
- `tests/collector/test_report_runner.py`
- `tests/collector/test_log_stream_watcher.py`
- `tests/collector/test_scheduler.py`
- `tests/notify/test_router_retry.py`
- `tests/notify/test_routing_policy.py`
- `tests/web/test_main_notify_binding.py`

---

### Task 1: SQLite 吞吐优化（WAL + busy_timeout + 批量写）

**Files:**
- Modify: `logwatch/core/config.py`
- Modify: `logwatch/core/db.py`
- Modify: `logwatch/core/metrics_writer.py`
- Modify: `config/logwatch.yaml.example`
- Test: `tests/core/test_config_merge.py`
- Test: `tests/core/test_metrics_db.py`
- Test: `tests/core/test_metrics_writer.py`

- [ ] **Step 1: 写失败测试（配置解析 + PRAGMA + 批量写）**

```python
# tests/core/test_config_merge.py

def test_resolve_runtime_settings_parses_sqlite_tuning():
    out = resolve_runtime_settings(
        {
            "storage": {
                "sqlite": {
                    "journal_mode": "wal",
                    "synchronous": "normal",
                    "busy_timeout_ms": 5000,
                }
            }
        }
    )
    assert out["sqlite"]["journal_mode"] == "wal"
    assert out["sqlite"]["synchronous"] == "normal"
    assert out["sqlite"]["busy_timeout_ms"] == 5000
```

```python
# tests/core/test_metrics_db.py
@pytest.mark.asyncio
async def test_apply_sqlite_pragmas_sets_expected_values(patch_aiosqlite_connect):
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await apply_sqlite_pragmas(
            conn,
            journal_mode="wal",
            synchronous="normal",
            busy_timeout_ms=5000,
        )
        cur = await conn.execute("PRAGMA busy_timeout;")
        row = await cur.fetchone()
    assert row[0] == 5000
```

```python
# tests/core/test_metrics_writer.py
@pytest.mark.asyncio
async def test_metrics_writer_write_many_metrics_calls_batch_insert_once():
    calls = []

    async def insert_metric_samples_fn(_conn, samples):
        calls.append(list(samples))

    writer = MetricsSqliteWriter(
        db_path=":memory:",
        db_connect=lambda _p: _ConnStub(),
        init_db_fn=lambda _c: None,
        insert_metric_samples_fn=insert_metric_samples_fn,
    )
    await writer.write_many(
        [
            {"host_name": "h1", "container_id": "c1", "container_name": "api", "timestamp": "2026-04-12T00:00:01+00:00"},
            {"host_name": "h1", "container_id": "c2", "container_name": "worker", "timestamp": "2026-04-12T00:00:02+00:00"},
        ]
    )
    assert len(calls) == 1
    assert len(calls[0]) == 2
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/core/test_config_merge.py tests/core/test_metrics_db.py tests/core/test_metrics_writer.py -q`
Expected: FAIL（缺少 sqlite tuning 字段、`apply_sqlite_pragmas`、`write_many`/`insert_metric_samples_fn`）

- [ ] **Step 3: 最小实现（配置、PRAGMA、批量写）**

```python
# logwatch/core/db.py
async def apply_sqlite_pragmas(
    conn: Any,
    *,
    journal_mode: str,
    synchronous: str,
    busy_timeout_ms: int,
) -> None:
    await conn.execute(f"PRAGMA journal_mode={journal_mode.upper()};")
    await conn.execute(f"PRAGMA synchronous={synchronous.upper()};")
    await conn.execute("PRAGMA busy_timeout=?;", (int(busy_timeout_ms),))
```

```python
# logwatch/core/db.py
async def insert_metric_samples(conn: Any, samples: list[dict[str, Any]]) -> None:
    rows = [
        (
            str(s["host_name"]),
            str(s["container_id"]),
            str(s["container_name"]),
            _normalize_sqlite_utc_timestamp(s["timestamp"]),
            float(s.get("cpu", 0.0)),
            int(s.get("mem_used", 0)),
            int(s.get("mem_limit", 0)),
            int(s.get("net_rx", 0)),
            int(s.get("net_tx", 0)),
            int(s.get("disk_read", 0)),
            int(s.get("disk_write", 0)),
            str(s.get("status", "unknown")),
            int(s.get("restart_count", 0)),
        )
        for s in samples
    ]
    await conn.executemany(
        """
        INSERT INTO metrics(host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit, net_rx, net_tx, disk_read, disk_write, status, restart_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """.strip(),
        rows,
    )
    await conn.commit()
```

```python
# logwatch/core/metrics_writer.py
async def write_many(self, samples: list[dict[str, Any]]) -> None:
    if self._closed:
        raise RuntimeError("metrics writer is closed")
    if not samples:
        return
    async with self._write_lock:
        conn = await self._ensure_conn()
        await _maybe_await(self._insert_metric_samples_fn(conn, list(samples)))
```

```python
# logwatch/core/config.py (resolve_runtime_settings return payload)
"sqlite": {
    "journal_mode": journal_mode,
    "synchronous": synchronous,
    "busy_timeout_ms": busy_timeout_ms,
}
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/core/test_config_merge.py tests/core/test_metrics_db.py tests/core/test_metrics_writer.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/config.py logwatch/core/db.py logwatch/core/metrics_writer.py config/logwatch.yaml.example tests/core/test_config_merge.py tests/core/test_metrics_db.py tests/core/test_metrics_writer.py
git commit -m "perf(sqlite): add pragma tuning and batch metric writes"
```

**Acceptance:**
- SQLite 初始化时应用 WAL/同步级别/busy timeout。
- 指标写入支持批量事务接口且测试覆盖。

---

### Task 2: `/api/alerts` 查询下推（结构化列 + 索引）

**Files:**
- Modify: `logwatch/core/db.py`
- Modify: `logwatch/core/metrics_writer.py`
- Modify: `logwatch/main.py`
- Test: `tests/core/test_metrics_db.py`
- Test: `tests/web/test_api_monitoring.py`

- [ ] **Step 1: 写失败测试（按 host/container 走 SQL 过滤）**

```python
# tests/core/test_metrics_db.py
@pytest.mark.asyncio
async def test_query_alerts_filters_by_host_and_container_columns(patch_aiosqlite_connect):
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_alert(conn, {"host": "host-a", "container_id": "c1", "line": "a"})
        await insert_alert(conn, {"host": "host-a", "container_id": "c2", "line": "b"})
        await insert_alert(conn, {"host": "host-b", "container_id": "c1", "line": "c"})

        rows = await query_alerts(conn, limit=100, host="host-a", container="c1")

    assert len(rows) == 1
    assert rows[0]["payload"]["line"] == "a"
```

```python
# tests/web/test_api_monitoring.py
# 在现有 test_api_alerts_metrics_and_mutes_surface_stored_data 中新增断言
assert alerts.json()["alerts"][0]["payload"]["host"] == "host-a"
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/core/test_metrics_db.py tests/web/test_api_monitoring.py -q`
Expected: FAIL（`query_alerts` 不支持 host/container 参数；main 仍在 Python 侧过滤）

- [ ] **Step 3: 最小实现（schema + query + 主入口改造）**

```python
# logwatch/core/db.py (alerts schema)
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    host TEXT NOT NULL DEFAULT '',
    container_id TEXT NOT NULL DEFAULT '',
    container_name TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    pushed INTEGER NOT NULL DEFAULT 0,
    payload TEXT
);
```

```python
# logwatch/core/db.py
async def insert_alert(conn: Any, payload: dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO alerts(host, container_id, container_name, category, pushed, payload)
        VALUES (?, ?, ?, ?, ?, ?);
        """.strip(),
        (
            str(payload.get("host") or ""),
            str(payload.get("container_id") or ""),
            str(payload.get("container_name") or ""),
            str(payload.get("category") or ""),
            1 if bool(payload.get("pushed")) else 0,
            _dump_json(payload),
        ),
    )
    await conn.commit()
```

```python
# logwatch/core/db.py
async def query_alerts(
    conn: Any,
    *,
    limit: int = 100,
    host: str | None = None,
    container: str | None = None,
) -> list[dict[str, Any]]:
    # WHERE 条件按 host/container 下推到 SQL，再统一 decode payload
```

```python
# logwatch/main.py
async def list_alert_rows(limit: int, host: str | None = None, container: str | None = None):
    writer = ensure_metrics_writer()
    return await writer.list_alerts(limit=int(limit), host=host, container=container)
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/core/test_metrics_db.py tests/web/test_api_monitoring.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/db.py logwatch/core/metrics_writer.py logwatch/main.py tests/core/test_metrics_db.py tests/web/test_api_monitoring.py
git commit -m "perf(alerts): push host/container filtering down to sqlite"
```

**Acceptance:**
- `/api/alerts?host=&container=` 不再依赖内存过滤。
- `alerts` 表具备结构化过滤列与索引。

---

### Task 3: 报告链路去 N+1（按 host + 时间窗批量取指标）

**Files:**
- Modify: `logwatch/core/db.py`
- Modify: `logwatch/core/metrics_writer.py`
- Modify: `logwatch/collector/reports.py`
- Modify: `logwatch/main.py`
- Test: `tests/collector/test_report_runner.py`
- Test: `tests/core/test_metrics_db.py`

- [ ] **Step 1: 写失败测试（批量查询接口只调用一次）**

```python
# tests/collector/test_report_runner.py
@pytest.mark.asyncio
async def test_report_runner_prefers_bulk_metrics_query_over_per_container_queries():
    bulk_calls = []
    single_calls = []

    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={"name": "host-a", "notify": {"push_on_normal": True}},
            state={"name": "host-a", "status": "connected"},
        ),
        list_containers=lambda _h: [{"id": "c1", "name": "api"}, {"id": "c2", "name": "worker"}],
        list_alerts=lambda **_kw: [],
        query_metrics=lambda *_a, **_kw: (single_calls.append(1) or []),
        query_host_metrics=lambda *_a, **_kw: [],
        query_host_container_metrics=lambda host, start, end, limit: (
            bulk_calls.append((host, start, end, limit))
            or [
                {"container_id": "c1", "container_name": "api", "cpu": 11, "mem_used": 100},
                {"container_id": "c2", "container_name": "worker", "cpu": 22, "mem_used": 200},
            ]
        ),
        send_host_notification=lambda *_a, **_kw: True,
        analyze=lambda s, c, t: f"{s}|{t}|{c['host_name']}",
        time_fn=lambda: 1000.0,
    )

    await runner.run_host_schedule("host-a", {"name": "fast", "interval_seconds": 300, "template": "interval"})

    assert len(bulk_calls) == 1
    assert len(single_calls) == 0
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/collector/test_report_runner.py tests/core/test_metrics_db.py -q`
Expected: FAIL（`ScheduleReportRunner` 不支持 `query_host_container_metrics`）

- [ ] **Step 3: 最小实现（bulk query + runner 分支）**

```python
# logwatch/core/db.py
async def query_host_container_latest_metrics(
    conn: Any,
    *,
    host_name: str,
    start_time: str,
    end_time: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    # 返回每个 container 最新一条 metrics 行
```

```python
# logwatch/core/metrics_writer.py
async def query_host_container_latest_metrics(...):
    async with self._read_lock:
        conn = await self._ensure_conn()
        return list(await _maybe_await(self._query_host_container_latest_metrics_fn(...)))
```

```python
# logwatch/collector/reports.py
if self._query_host_container_metrics is not None:
    rows = await _maybe_await(self._query_host_container_metrics(host_name, since, now, 500))
    return "\n".join(f"{row['container_name']}: cpu={row['cpu']} mem={row['mem_used']}" for row in rows)
# fallback 保留旧逻辑
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/collector/test_report_runner.py tests/core/test_metrics_db.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/db.py logwatch/core/metrics_writer.py logwatch/collector/reports.py logwatch/main.py tests/collector/test_report_runner.py tests/core/test_metrics_db.py
git commit -m "perf(reports): remove per-container metrics N+1 with bulk host query"
```

**Acceptance:**
- 报告生成对容器指标查询从 N 次降为 1 次。
- 保留 fallback，避免对旧注入点破坏。

---

### Task 4: 日志告警链路背压（有界队列 + worker）

**Files:**
- Modify: `logwatch/collector/log_stream.py`
- Modify: `config/logwatch.yaml.example`
- Test: `tests/collector/test_log_stream_watcher.py`

- [ ] **Step 1: 写失败测试（队列满时可控丢弃 + 统计）**

```python
# tests/collector/test_log_stream_watcher.py
@pytest.mark.asyncio
async def test_log_stream_watcher_drops_events_when_queue_full():
    started = asyncio.Event()

    async def slow_alert(*_args, **_kwargs):
        started.set()
        await asyncio.sleep(0.05)

    async def stream_logs(_host, _container, **_kwargs):
        for i in range(20):
            yield {"timestamp": "2026-04-12T00:00:00Z", "line": f"ERROR {i}"}

    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock", "watch": {"queue_maxsize": 2, "worker_count": 1, "drop_when_full": True}},
        stream_logs=stream_logs,
        run_alert=slow_alert,
    )

    await watcher.watch_container({"id": "c1", "name": "api"})
    await watcher.shutdown()

    assert watcher.dropped_events > 0
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/collector/test_log_stream_watcher.py -q`
Expected: FAIL（`LogStreamWatcher` 无 `dropped_events` 与队列模型）

- [ ] **Step 3: 最小实现（队列 + worker + 丢弃计数）**

```python
# logwatch/collector/log_stream.py
self._queue_maxsize = int(((self._host.get("watch") or {}).get("queue_maxsize", 1024)))
self._worker_count = int(((self._host.get("watch") or {}).get("worker_count", 2)))
self._drop_when_full = bool(((self._host.get("watch") or {}).get("drop_when_full", True)))
self._queue: asyncio.Queue[_AlertItem] = asyncio.Queue(maxsize=self._queue_maxsize)
self._dropped_events = 0
```

```python
# logwatch/collector/log_stream.py
@property
def dropped_events(self) -> int:
    return int(self._dropped_events)
```

```python
# logwatch/collector/log_stream.py (watch_container)
try:
    self._queue.put_nowait(item)
except asyncio.QueueFull:
    if self._drop_when_full:
        self._dropped_events += 1
    else:
        await self._queue.put(item)
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/collector/test_log_stream_watcher.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/collector/log_stream.py config/logwatch.yaml.example tests/collector/test_log_stream_watcher.py
git commit -m "perf(log-stream): add bounded queue backpressure and dropped-event counters"
```

**Acceptance:**
- 高流量时不会无限堆积 await 链路。
- 丢弃行为可观测（`dropped_events`）。

---

### Task 5: Scheduler 稳定性参数（max_instances/coalesce/misfire）

**Files:**
- Modify: `logwatch/core/config.py`
- Modify: `logwatch/collector/scheduler.py`
- Modify: `logwatch/main.py`
- Modify: `config/logwatch.yaml.example`
- Test: `tests/core/test_config_merge.py`
- Test: `tests/collector/test_scheduler.py`

- [ ] **Step 1: 写失败测试（job kwargs 注入）**

```python
# tests/collector/test_scheduler.py
@pytest.mark.asyncio
async def test_metrics_scheduler_applies_job_guardrails():
    fake_scheduler = _FakeScheduler()
    scheduler = MetricsSamplingScheduler(
        host_manager=_HostManagerStub([]),
        sampler=_SamplerStub(),
        list_containers=lambda _h: [],
        interval_seconds=30,
        scheduler_factory=lambda: fake_scheduler,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    await scheduler.start()

    job = fake_scheduler.jobs[0]
    assert job["max_instances"] == 1
    assert job["coalesce"] is True
    assert job["misfire_grace_time"] == 30
```

```python
# tests/core/test_config_merge.py

def test_resolve_runtime_settings_parses_scheduler_guardrails():
    out = resolve_runtime_settings(
        {"scheduler": {"max_instances": 1, "coalesce": True, "misfire_grace_time": 30}}
    )
    assert out["scheduler"] == {"max_instances": 1, "coalesce": True, "misfire_grace_time": 30}
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/collector/test_scheduler.py tests/core/test_config_merge.py -q`
Expected: FAIL（scheduler 构造参数不支持 guardrails）

- [ ] **Step 3: 最小实现（配置透传 + add_job 参数）**

```python
# logwatch/collector/scheduler.py
class MetricsSamplingScheduler:
    def __init__(..., max_instances: int = 1, coalesce: bool = True, misfire_grace_time: int = 30):
        self._max_instances = int(max_instances)
        self._coalesce = bool(coalesce)
        self._misfire_grace_time = int(misfire_grace_time)

    async def start(self) -> None:
        ...
        scheduler.add_job(
            self._run_cycle_job,
            "interval",
            seconds=self._interval_seconds,
            id="metrics-sampling",
            replace_existing=True,
            max_instances=self._max_instances,
            coalesce=self._coalesce,
            misfire_grace_time=self._misfire_grace_time,
        )
```

```python
# logwatch/core/config.py (resolve_runtime_settings return payload)
"scheduler": {
    "max_instances": scheduler_max_instances,
    "coalesce": scheduler_coalesce,
    "misfire_grace_time": scheduler_misfire_grace_time,
}
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/collector/test_scheduler.py tests/core/test_config_merge.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/core/config.py logwatch/collector/scheduler.py logwatch/main.py config/logwatch.yaml.example tests/core/test_config_merge.py tests/collector/test_scheduler.py
git commit -m "reliability(scheduler): add coalesce/misfire/max_instances guardrails"
```

**Acceptance:**
- 所有周期任务默认具备防重叠与错过触发治理能力。
- 配置可控，默认值对现有行为无破坏。

---

### Task 6: 通知策略路由（多服务器多容器灵活投递）

**Files:**
- Create: `logwatch/notify/policy.py`
- Modify: `logwatch/notify/router.py`
- Modify: `logwatch/main.py`
- Modify: `logwatch/collector/log_stream.py`
- Modify: `config/logwatch.yaml.example`
- Modify: `README.md`
- Test: `tests/notify/test_router_retry.py`
- Create: `tests/notify/test_routing_policy.py`
- Modify: `tests/web/test_main_notify_binding.py`
- Modify: `tests/collector/test_log_stream_watcher.py`

- [ ] **Step 1: 写失败测试（按 host/container/category 规则选路 + 默认兜底）**

```python
# tests/notify/test_routing_policy.py
def test_notify_policy_matches_container_specific_rule_then_fallback():
    policy = build_notify_routing_policy(
        {
            "default_channels": ["telegram"],
            "rules": [
                {
                    "name": "api-critical",
                    "priority": 100,
                    "match": {
                        "hosts": ["prod-*"],
                        "containers": ["api", "gateway-*"],
                        "categories": ["ERROR", "OOM"],
                    },
                    "deliver": {
                        "channels": ["wechat"],
                        "message_mode": "md",
                    },
                }
            ],
        }
    )

    matched = policy.resolve(
        host="prod-a",
        category="ERROR",
        context={"container_name": "api"},
    )
    fallback = policy.resolve(
        host="prod-a",
        category="ERROR",
        context={"container_name": "worker"},
    )

    assert matched["channels"] == ["wechat"]
    assert matched["message_mode"] == "md"
    assert fallback["channels"] == ["telegram"]
```

```python
# tests/notify/test_router_retry.py
@pytest.mark.asyncio
async def test_router_prefers_route_selector_when_context_matches():
    chosen = _SlowNotifier("chosen", delay=0)
    default = _SlowNotifier("default", delay=0)

    def selector(host: str, category: str, context: dict | None):
        if (context or {}).get("container_name") == "api":
            return [chosen]
        return [default]

    router = NotifyRouter([default], route_selector=selector)
    ok = await router.send("prod-a", "msg", "ERROR", context={"container_name": "api"})

    assert ok is True
    assert chosen.calls == 1
    assert default.calls == 0
```

```python
# tests/web/test_main_notify_binding.py
def test_main_app_notify_router_routes_specific_container_to_specific_channel() -> None:
    telegram_calls: list[tuple[str, str]] = []
    wechat_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str) -> None:
        telegram_calls.append((target, message))

    async def wechat_send(target: str, message: str) -> None:
        wechat_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "telegram": {"enabled": True, "chat_ids": ["tg-default"]},
                    "wechat": {"enabled": True, "webhook_urls": ["wx-api"]},
                    "routing": {
                        "default_channels": ["telegram"],
                        "rules": [
                            {
                                "name": "api-route",
                                "match": {"hosts": ["prod-a"], "containers": ["api"]},
                                "deliver": {"channels": ["wechat"], "message_mode": "md"},
                            }
                        ],
                    },
                }
            },
            telegram_send_func=telegram_send,
            wechat_send_func=wechat_send,
        )

        await app.state.notify_router.send(
            "prod-a", "api down", "ERROR", context={"container_name": "api"}
        )
        await app.state.notify_router.send(
            "prod-a", "worker down", "ERROR", context={"container_name": "worker"}
        )

    asyncio.run(run_case())

    assert [x[0] for x in wechat_calls] == ["wx-api"]
    assert [x[0] for x in telegram_calls] == ["tg-default"]
```

- [ ] **Step 2: 运行测试确认失败（RED）**

Run: `pytest tests/notify/test_router_retry.py tests/notify/test_routing_policy.py tests/web/test_main_notify_binding.py tests/collector/test_log_stream_watcher.py -q`
Expected: FAIL（缺少 `notify.policy`、`NotifyRouter.route_selector/context`、主入口未绑定 routing）

- [ ] **Step 3: 最小实现（规则引擎 + Router 扩展 + 告警链路上下文）**

```python
# logwatch/notify/policy.py
@dataclass(frozen=True, slots=True)
class NotifyRoutingPolicy:
    default_channels: tuple[str, ...]
    rules: tuple[dict[str, Any], ...]

    def resolve(self, *, host: str, category: str, context: dict[str, Any] | None) -> dict[str, Any]:
        # priority 从大到小，首条命中即返回
        # 支持 hosts/containers/categories 的通配匹配
        # 未命中返回 default_channels
```

```python
# logwatch/notify/router.py
class NotifyRouter:
    def __init__(..., route_selector: Any | None = None):
        self._route_selector = route_selector

    async def send(
        self,
        host: str,
        message: str,
        category: str,
        context: dict[str, Any] | None = None,
    ) -> bool:
        if self._route_selector is not None:
            selected_notifiers = list(
                self._route_selector(host, category, dict(context or {})) or []
            )
        else:
            selected_notifiers = self._host_notifiers.get(host, self._notifiers)
```

```python
# logwatch/collector/log_stream.py (run_alert_once 内通知调用)
notify_context = {
    "container_id": container_id,
    "container_name": container_name or container_id,
}
try:
    pushed = bool(
        await _maybe_call_notify(
            notify_impl,
            host=host,
            message=message,
            category=rule.matched_category,
            context=notify_context,
        )
    )
except Exception:
    ...
```

```python
# logwatch/main.py（构建 route_selector 并注入 NotifyRouter）
routing_policy = build_notify_routing_policy(_resolve_global_notify_config(app_config))

def route_selector(host: str, category: str, context: dict[str, Any]) -> list[BaseNotifier]:
    plan = routing_policy.resolve(host=host, category=category, context=context)
    return _resolve_notifiers_from_plan(host=host, plan=plan, host_notifiers=host_notifiers)
```

- [ ] **Step 4: 运行测试确认通过（GREEN）**

Run: `pytest tests/notify/test_router_retry.py tests/notify/test_routing_policy.py tests/web/test_main_notify_binding.py tests/collector/test_log_stream_watcher.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add logwatch/notify/policy.py logwatch/notify/router.py logwatch/main.py logwatch/collector/log_stream.py config/logwatch.yaml.example README.md tests/notify/test_router_retry.py tests/notify/test_routing_policy.py tests/web/test_main_notify_binding.py tests/collector/test_log_stream_watcher.py
git commit -m "feat(notify): add flexible host/container/category routing policy with fallback"
```

**Acceptance:**
- 支持同一服务器下不同容器走不同通知渠道。
- 支持指定容器走独立方式，其余容器走统一默认渠道。
- 支持跨服务器通配匹配规则并保持未命中兜底行为。

---

### Task 7: 文档与回归验证

**Files:**
- Modify: `README.md`
- Modify: `config/logwatch.yaml.example`
- Modify: `docs/superpowers/plans/2026-04-12-logwatch-high-impact-optimizations.md`

- [ ] **Step 1: 更新 README 的优化配置和运行建议**

```md
## 性能与稳定性优化开关
- storage.sqlite.journal_mode: wal
- storage.sqlite.busy_timeout_ms: 5000
- scheduler.max_instances: 1
- scheduler.coalesce: true
- scheduler.misfire_grace_time: 30
- host.watch.queue_maxsize / worker_count / drop_when_full
- notify.routing.rules / default_channels / deliver.message_mode
```

- [ ] **Step 2: 运行分层回归（核心 + collector + web）**

Run: `pytest tests/core/test_config_merge.py tests/core/test_metrics_db.py tests/core/test_metrics_writer.py -q`
Expected: PASS

Run: `pytest tests/collector/test_scheduler.py tests/collector/test_report_runner.py tests/collector/test_log_stream_watcher.py tests/notify/test_router_retry.py tests/notify/test_routing_policy.py -q`
Expected: PASS

Run: `pytest tests/web/test_api_monitoring.py tests/web/test_main_notify_binding.py -q`
Expected: PASS

- [ ] **Step 3: 运行全量回归**

Run: `pytest -q`
Expected: PASS

- [ ] **Step 4: 记录验证结果与已知风险**

```md
- 批量写在极端峰值下仍可能触发 sqlite busy（通过 busy_timeout + retry 兜底）
- queue drop 模式可能丢低优先级日志（通过 dropped_events 指标可观测）
- guardrails 生效后，长耗时任务将被 coalesce，需关注报告频率预期
- notify 路由规则重叠时可能命中优先级较高规则（通过 priority + dry-run 日志校验）
```

- [ ] **Step 5: 提交**

```bash
git add README.md config/logwatch.yaml.example docs/superpowers/plans/2026-04-12-logwatch-high-impact-optimizations.md
git commit -m "docs: add rollout and verification notes for high-impact optimizations"
```

**Acceptance:**
- 文档配置项与实现一致。
- 核心回归和全量回归可复现。

---

## Rollback Strategy

- 若出现写入延迟抖动：临时关闭批量写（回退到单条写）并保留 WAL。
- 若出现日志漏报争议：将 `drop_when_full` 调整为 `false`（阻塞模式）并临时降低 worker 处理复杂度。
- 若报告频率异常：提高 `misfire_grace_time` 或关闭 `coalesce`，恢复旧调度行为。
- 若通知策略误配：将 `notify.routing.rules` 置空，仅保留 `default_channels`，回退为统一渠道。

## Self-Review Checklist (completed)

- Spec coverage: 本计划覆盖 6 个高收益优化点，且每项均有测试、实现、验收、回滚。
- Placeholder scan: 未包含“后续补充型”占位描述。
- Type consistency: 计划中新增方法命名在任务间保持一致（`write_many`、`query_host_container_latest_metrics`、`dropped_events`、`build_notify_routing_policy`）。
