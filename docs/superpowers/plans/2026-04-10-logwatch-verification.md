# LogWatch Verification Checklist (2026-04-14)

## 1. 测试命令

### 全量测试

```bash
pytest -q
```

期望：
- 所有测试通过

历史结果（2026-04-12）：
- `254 passed`

本次补充检查（2026-04-14）：
- `pytest --collect-only -q`：`347 tests collected`
- 本次未重新记录完整 `pytest -q` 结束输出，因此不更新“通过数”结论。

### 关键专项回归

```bash
pytest tests/web/test_api_monitoring.py tests/web/test_static_frontend.py tests/integration/test_alert_pipeline.py tests/collector/test_report_runner.py -q
```

期望：
- API / 静态前端 / storm / report scheduler 关键回归通过

本次结果（2026-04-12）：
- 已通过（包含在 `pytest -q` 全量结果中）

## 2. 服务启动与接口验收

### 应用工厂 smoke

```bash
WEB_AUTH_TOKEN=web-token WEB_ADMIN_TOKEN=admin-token python - <<'PY'
from logwatch.main import create_app
app = create_app(enable_metrics_scheduler=False, enable_retention_cleanup=False, enable_watch_manager=False)
print(app.title)
print(sorted([getattr(r, 'path', '') for r in app.router.routes if getattr(r, 'path', '')]))
PY
```

期望：
- 输出应用标题 `LogWatch`
- 路由列表包含：`/`、`/static/{asset_path:path}`、`/api/hosts`、`/api/hosts/{host}/containers`、`/api/alerts`、`/api/hosts/{host}/metrics/{container}`、`/api/hosts/{host}/system-metrics`、`/api/mutes`、`/api/reload`、`/api/send-failed`、`/api/storm-events`、`/api/storm-events/stats`、`/ws/chat`

注意：
- 当前实现要求 `WEB_AUTH_TOKEN/WEB_ADMIN_TOKEN`（或显式参数）存在；上面的命令已包含该前提。

本次结果（2026-04-14）：
- 通过（按上述带 token 方式执行）

### 接口检查（示例）

```bash
curl -i http://127.0.0.1:8000/api/hosts \
  -H 'Authorization: Bearer <WEB_AUTH_TOKEN>'
```

期望响应：
- `HTTP/1.1 200 OK`
- Body：`{"hosts":[{"name":"...","status":"connected|disconnected",...}]}`

说明：
- 当前 `create_app` 已挂载 `/`、`/static/{asset_path:path}`、`/api/hosts`、`/api/hosts/{host}/containers`、`/api/alerts`、`/api/hosts/{host}/metrics/{container}`、`/api/hosts/{host}/system-metrics`、`/api/mutes`、`/api/reload`、`/api/send-failed`、`/api/storm-events`、`/api/storm-events/stats` 与 `/ws/chat`。
- 普通 REST 端点（含 storm 相关接口）要求 web bearer token；`/api/reload` 与 `/api/send-failed` 走 admin bearer token。
- 当前验证还覆盖了：Telegram bot lifespan、host-aware notify routing、output rendering、report scheduler、atomic reload 默认控制器与 alert storm 最小实现。
