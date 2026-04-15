# Host System Metrics Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为本地与远程主机增加“宿主机资源”定时采集、落库、报告分析与推送能力，并通过配置开关控制是否启用。

**Architecture:** 在现有容器指标链路旁新增“宿主机指标”链路：`HostMetricsSampler -> MetricsSqliteWriter(host_metrics) -> ReportRunner`。采集端采用中心节点 pull 模式：本地主机用本地探针，远程主机通过 SSH 执行只读探针脚本并回传 JSON。报告阶段把宿主机指标摘要注入 LLM 上下文，复用现有通知路由推送。

**Tech Stack:** Python 3.11+, asyncio, aiosqlite, APScheduler, paramiko, FastAPI, pytest/pytest-asyncio

---

## Scope & Assumptions

- 监控目标以现有 `hosts` 配置为准，不引入独立资产管理系统。
- 新功能默认关闭，只有显式 `enabled: true` 才采集。
- 报告沿用现有 `ScheduleReportRunner` 与通知通道（Telegram/WeChat）。
- 本期不做独立前端图表；先提供 API 与报告文本集成。

## File Structure (planned)

- Create: `logwatch/collector/host_metrics_probe.py`
- Create: `logwatch/collector/host_metrics_sampler.py`
- Create: `tests/collector/test_host_metrics_probe.py`
- Create: `tests/collector/test_host_metrics_sampler.py`
- Modify: `logwatch/core/config.py`
- Modify: `config/logwatch.yaml.example`
- Modify: `logwatch/core/db.py`
- Modify: `logwatch/core/metrics_writer.py`
- Modify: `logwatch/collector/scheduler.py`
- Modify: `logwatch/main.py`
- Modify: `logwatch/collector/reports.py`
- Modify: `logwatch/web/api.py`
- Modify: `README.md`
- Modify: `tests/core/test_config_merge.py`
- Modify: `tests/core/test_metrics_db.py`
- Modify: `tests/core/test_metrics_writer.py`
- Modify: `tests/collector/test_report_runner.py`
- Modify: `tests/web/test_api_monitoring.py`

## Data Model (host_metrics)

建议新增 `host_metrics` 表（仅示例字段，最终以实现为准）：

- `id` INTEGER PK
- `host_name` TEXT NOT NULL
- `timestamp` TEXT NOT NULL
- `cpu_percent` REAL NOT NULL
- `load_1` REAL NOT NULL
- `load_5` REAL NOT NULL
- `load_15` REAL NOT NULL
- `mem_total` INTEGER NOT NULL
- `mem_used` INTEGER NOT NULL
- `mem_available` INTEGER NOT NULL
- `disk_root_total` INTEGER NOT NULL
- `disk_root_used` INTEGER NOT NULL
- `disk_root_free` INTEGER NOT NULL
- `net_rx` INTEGER NOT NULL
- `net_tx` INTEGER NOT NULL
- `source` TEXT NOT NULL (`local`/`ssh`)

索引建议：
- `idx_host_metrics_query(host_name, timestamp)`
- `idx_host_metrics_cleanup(timestamp)`

## Config Contract (draft)

```yaml
metrics:
  sample_interval_seconds: 30  # 现有容器指标
  host_system:
    enabled: false
    sample_interval_seconds: 30
    collect_load: true
    collect_network: true
    report:
      include_in_schedule: true
      warn_thresholds:
        cpu_percent: 85
        mem_used_percent: 90
        disk_used_percent: 90
```

---

### Task 1: 配置开关与运行时参数打通

**Files:**
- Modify: `logwatch/core/config.py`
- Modify: `config/logwatch.yaml.example`
- Modify: `tests/core/test_config_merge.py`
- Modify: `tests/core/test_config_loader.py`

- [x] 在 `resolve_runtime_settings()` 增加 `metrics.host_system.*` 解析与类型校验（bool、正整数、阈值范围）。
- [x] 为 `host_system.enabled` 设默认值 `false`，并保证缺省配置下行为不变。
- [x] 更新示例配置 `config/logwatch.yaml.example`，补充开关与最小字段说明。
- [x] 新增/更新配置测试：默认值、布尔解析、非法阈值报错、显式启用路径。

**Acceptance:**
- 未配置 `host_system` 时，应用启动和现有测试结果不变。
- 配置启用后，`create_app` 可拿到完整 host-system runtime settings。

### Task 2: 宿主机采集探针（本地 + SSH 远程）

**Files:**
- Create: `logwatch/collector/host_metrics_probe.py`
- Create: `tests/collector/test_host_metrics_probe.py`

- [x] 实现 `collect_local_host_metrics()`：采集 CPU/Load/Memory/Disk/Network，输出统一 dict。
- [x] 实现 `collect_remote_host_metrics(host_cfg)`：通过 SSH 执行只读探针命令并解析 JSON。
- [x] 实现 URL 解析（`unix://` 走本地、`ssh://user@host` 走 SSH），并统一异常模型（timeout/auth/parse）。
- [x] 加入安全约束：命令白名单、超时、禁用 shell 拼接用户输入、日志脱敏。
- [x] 为正常采集、SSH失败、JSON格式异常、字段缺失补零等场景补测试。

**Acceptance:**
- 在 stub/mocked 环境下可稳定返回标准化指标结构。
- 任何单主机采集失败不会中断其他主机采集。

### Task 3: 落库与查询能力（host_metrics）

**Files:**
- Modify: `logwatch/core/db.py`
- Modify: `logwatch/core/metrics_writer.py`
- Modify: `tests/core/test_metrics_db.py`
- Modify: `tests/core/test_metrics_writer.py`

- [x] 在 `init_db()` 增加 `host_metrics` 表和索引创建。
- [x] 增加 `insert_host_metric_sample()` / `query_host_metrics()`。
- [x] 在 retention 清理中增加 `host_metrics_days`（复用现有 retention 机制）。
- [x] 在 `MetricsSqliteWriter` 暴露 `write_host_metric()` / `query_host_metrics()`。
- [x] 为写入并发、查询时间窗、清理策略补充测试。

**Acceptance:**
- 启动后数据库自动拥有新表。
- 可按 `host_name + time range` 稳定查询，且清理策略可生效。

### Task 4: 定时采集调度与 App 生命周期接入

**Files:**
- Create: `logwatch/collector/host_metrics_sampler.py`
- Modify: `logwatch/collector/scheduler.py`
- Modify: `logwatch/main.py`
- Modify: `tests/collector/test_scheduler.py`
- Modify: `tests/web/test_main_metrics_persistence.py`

- [x] 新增 `HostMetricsSampler`：遍历 connected hosts，逐主机采集并写入。
- [x] 新增 `HostMetricsSamplingScheduler`（结构对齐 `MetricsSamplingScheduler`）。
- [x] `create_app()` 中按 `host_system.enabled` 条件创建/启动/关闭 scheduler。
- [x] reload 后沿用新配置（开关可动态生效，建议与重载逻辑保持一致语义）。
- [x] 覆盖生命周期测试：start、run_once、shutdown、disabled 跳过。

**Acceptance:**
- 开关关闭：不会产生宿主机采样写入。
- 开关开启：周期性写入 `host_metrics`，应用关闭时 scheduler 正常释放。

### Task 5: 报告分析与推送集成

**Files:**
- Modify: `logwatch/collector/reports.py`
- Modify: `tests/collector/test_report_runner.py`
- (Optional) Modify: `templates/prompts/default_interval.md`
- (Optional) Modify: `templates/prompts/default_hourly.md`
- (Optional) Modify: `templates/prompts/default_daily.md`

- [x] 在 `ScheduleReportRunner` 注入 `query_host_metrics` 能力。
- [x] 构建 `host_system_metrics` 文本摘要（latest + window avg/max）。
- [x] 在报告 context 新增字段（如 `host_metrics`），并合并到 `metrics` 或独立注入模板。
- [x] 基于阈值生成 `host_resource_warnings`，确保 LLM 能看到风险提示。
- [x] 验证 push_on_normal/heartbeat 分支下也能携带宿主机摘要。

**Acceptance:**
- 周期报告里出现宿主机资源摘要。
- 超阈值场景下分析文本可明确指出宿主机瓶颈并走现有推送链路。

### Task 6: 查询接口与文档

**Files:**
- Modify: `logwatch/web/api.py`
- Modify: `tests/web/test_api_monitoring.py`
- Modify: `README.md`

- [x] 新增 API：`GET /api/hosts/{host}/system-metrics?start=&end=&limit=`。
- [x] 复用 web token 鉴权；错误语义与现有 metrics 接口保持一致（404/503）。
- [x] README 增加配置说明、接口说明、开关行为说明。

**Acceptance:**
- API 可返回主机级指标点；鉴权和异常语义符合现有风格。

### Task 7: 回归验证与发布准备

**Files:**
- Modify: `README.md`（如需补充验证命令）

- [x] 运行最小回归：
  - `pytest tests/core/test_config_merge.py tests/core/test_metrics_db.py tests/core/test_metrics_writer.py -q`
  - `pytest tests/collector/test_host_metrics_probe.py tests/collector/test_host_metrics_sampler.py tests/collector/test_report_runner.py tests/collector/test_scheduler.py -q`
  - `pytest tests/web/test_api_monitoring.py tests/web/test_main_metrics_persistence.py -q`
- [x] 运行全量：`pytest -q`
- [x] 记录风险与回滚策略（开关一键关闭、保留已采集数据、不影响容器监控主链路）。

**Acceptance:**
- 新功能由配置控制、默认无影响、测试全绿。

---

## Risks & Mitigations

- SSH 执行稳定性：加超时、重试上限、单主机失败隔离。
- 指标口径差异（不同 Linux 发行版）：探针输出统一 schema + 缺失字段补零。
- 报告噪音过高：增加阈值与最小持续时间（可后续扩展）。

## Rollout Strategy

1. 先在单 host 打开 `host_system.enabled=true` 验证 24h。
2. 观察数据库增长与报告质量，再按 host 批量开启。
3. 若异常，直接关闭开关回退，保留历史数据用于排障。
