# LogWatch Next Steps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保留当前 MVP 稳定性的前提下，先完成文档与实现一致化，再把 Web/API 入口收口到单一 `create_app`，为后续多主机真实采集能力铺路。

**Architecture:** 保持现有分层不重构；第一阶段只改文档与验收记录，第二阶段最小改动收口主入口（复用现有 `web/api.py` 与 `web/chat.py`），通过新增测试锁定行为不回退。后续第三阶段再进入 `host_manager + metrics/sampler` 功能开发。

**Tech Stack:** Python >=3.11, FastAPI, pytest, httpx

---

## 执行顺序

### Task 1: 文档与进度对齐（先做）

**Files:**
- Modify: `docs/superpowers/specs/2026-04-10-logwatch-design.md`
- Modify: `docs/superpowers/plans/2026-04-10-logwatch-verification.md`
- Modify: `docs/superpowers/plans/2026-04-10-logwatch-code-todo.md`

- [x] 把“实现状态”日期和内容同步到 2026-04-11 的真实状态。
- [x] 把验证结果从 `64/3` 更新为最新基线（当前以 verification 文档记录为准：`pytest -q` 为 `254 passed`），保留验证命令。
- [x] 勾选已完成 Task/Milestone（仅勾选已在代码中可验证的项）。

验收：
- 文档中不再出现过期通过数。
- 计划文件的勾选状态与当前实现一致。

### Task 2: 主入口收口（第二步）

**Files:**
- Create: `tests/web/test_main_app_wiring.py`
- Modify: `logwatch/main.py`
- Modify: `logwatch/web/api.py`
- Modify: `logwatch/web/chat.py`

- [x] 先新增/更新测试，要求 `create_app` 提供 `/api/hosts`、`/api/reload`、`/ws/chat`。
- [x] 运行新测试，确认在改实现前至少一个失败（RED）。
- [x] 最小实现：在 `create_app` 中复用现有子模块，挂载 API/WS 路由。
- [x] 重新运行测试确认全部通过（GREEN）。

验收：
- `POST /api/reload` 在主入口可访问并遵守 admin token。
- `WS /ws/chat` 在主入口可用并回显。

### Task 3: 回归验证与记录（第三步）

**Files:**
- Modify: `docs/superpowers/plans/2026-04-10-logwatch-verification.md`

- [x] 运行：`pytest -q`、`pytest tests/integration -q`、`pytest tests/web -q`。
- [x] 将结果写入验证文档，记录执行日期与通过数。

验收：
- 关键测试全绿。
- 文档可复现当前状态。

### Task 4: 多主机与指标链路（本轮已部分实现）

**目标模块:**
- `logwatch/core/host_manager.py`
- `logwatch/collector/metrics.py`
- `logwatch/collector/sampler.py`
- `logwatch/core/db.py`（metrics 表与索引）

- [x] 落地 `host_manager` 最小能力（预检、重试退避、新增主机热加载）。
- [x] 落地 `metrics/sampler` 最小能力（stats 解析、单轮采样写入）。
- [x] 落地 `core/db.py` metrics 表、索引、写入与查询接口。
- [x] 接入真实 Docker connector 封装（host 连通性、容器列表、stats 拉取）。
- [x] 接入 APScheduler 调度器封装与主应用生命周期挂载（可注入替换，缺依赖时降级禁用）。
- [x] `/api/hosts` 切换为返回 HostManager 实时状态。
- [x] 状态变更事件接入可注入 sender（`HOST_STATUS` 类别消息）。
- [x] 把 sender 与 `notify.router`/配置体系绑定：未显式传 `notify_router` 时，从 `app_config.notify` 自动构建路由（`chat_ids/webhook_urls`，空列表回退环境变量），并将通知失败默认落库到 `send_failed`；同时提供 `GET /api/send-failed`（admin）查询接口。
- [x] 落地周期采样结果入库的生产调度编排（host 状态驱动采样 + 生命周期统一管理 + 失败隔离日志）。
- [x] 落地 SQLite 指标写入串行化器（单连接懒初始化 + 并发写串行化）。
- [x] 落地 retention cleanup 调度与生命周期挂载（默认日级调度，可注入 `retention_config`）。
- [x] 打通运行时配置到 `create_app`（`metrics.sample_interval_seconds`、`storage.db_path`、`storage.retention.*`）。
- [x] 落地 Host 级 Docker client 池（按 host 复用连接，URL 变更自动重建，应用生命周期统一关闭）。
- [x] 为 Docker client 池补充健康探测与失效连接惰性重建（优先 `ping`，回退 `version`）。
- [x] 为 Docker client 池补充容量治理（`max_clients` + LRU 淘汰）与空闲回收（`max_idle_seconds`）。
- [x] 修复健康失败重建路径的重复 `close` 风险，并通过测试锁定“旧连接只关闭一次”。
