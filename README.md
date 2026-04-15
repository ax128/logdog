# LogDog

**LogDog** 是一个轻量、可自托管的容器监控与告警服务。通过统一的 Web 控制台、REST API 和 Telegram/企业微信通知，帮助你掌握多台机器上 Docker 容器的运行状态。

## 功能亮点

- **多主机监控**：本地 `unix://` + 远程 `ssh://`，无需在目标机部署 agent
- **实时告警**：容器日志/事件关键字匹配，支持冷却期、静默规则、告警风暴抑制
- **指标采样**：容器与宿主机指标定时采集，持久化至 SQLite，带保留期自动清理
- **定时巡检**：周期性全局状态汇总报告
- **AI 对话**：内置 LLM Agent，支持自然语言查询状态、分析日志、重启容器（需配置 LLM）
- **Telegram Bot**：双向交互，支持流式回复、`/status`、`/help`、`/msg` 切换消息格式
- **热重载**：`POST /api/reload` 无停机更新配置，失败自动回滚
- **Web 控制台**：内置前端，API + WebSocket 实时 Chat

## 快速开始

### 1. 安装

```bash
git clone https://github.com/ax128/logdog.git
cd logdog
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. 配置

```bash
cp .env.example .env
# 编辑 .env，填入必要的 token
```

复制并修改配置文件：

```bash
cp config/logdog.yaml.example config/logdog.yaml
```

最小配置示例（`config/logdog.yaml`）：

```yaml
hosts:
  - name: local
    url: unix:///var/run/docker.sock

notify:
  telegram:
    enabled: true
    auto_chat_id: true   # 自动从 Bot 更新流解析 chat_id
    message_mode: text
```

### 3. 启动

```bash
WEB_AUTH_TOKEN=your-web-token \
WEB_ADMIN_TOKEN=your-admin-token \
TELEGRAM_BOT_TOKEN=123456:ABCDEF \
uvicorn logdog.main:create_app --factory --host 0.0.0.0 --port 8000
```

本地调试时可用不安全默认 token（`web-token` / `admin-token`）：

```bash
python -c "
import uvicorn
from logdog.main import create_app
uvicorn.run(create_app(allow_insecure_default_tokens=True), host='0.0.0.0', port=8000)
"
```

打开 `http://localhost:8000` 即可访问控制台。

## 多主机 + SSH 远程

在 `config/logdog.yaml` 中添加远程主机：

```yaml
hosts:
  - name: local
    url: unix:///var/run/docker.sock
  - name: prod
    url: "ssh://${PROD_USER:-root}@${PROD_HOST}:${PROD_PORT:-22}"
    ssh_key: "${PROD_SSH_KEY:-/root/.ssh/id_ed25519}"
```

在 `.env` 中填入：

```
PROD_HOST=your-server-ip
PROD_PORT=22
PROD_USER=root
PROD_SSH_KEY=/root/.ssh/id_ed25519
```

目标机需要 Docker 访问权限（root 或 docker 组），SSH 私钥需免密可用。

## 通知配置

### Telegram

```yaml
notify:
  telegram:
    enabled: true
    chat_ids: []         # 留空 + auto_chat_id: true 时自动解析
    auto_chat_id: true
    message_mode: text   # text | md | doc
```

启动后先给 Bot 发一条消息，它会自动记住你的 chat_id。

Bot 支持的命令：

| 命令 | 说明 |
|------|------|
| `/status` | 查看 Agent 状态 |
| `/help` | 显示所有命令 |
| `/msg md` | 切换消息格式（txt / md / doc） |

### 企业微信

```yaml
notify:
  wecom:
    enabled: true
    targets: []   # 填完整 webhook URL，或留空用 WECOM_WEBHOOK_URL 环境变量
    message_mode: md
```

### 多通道路由

```yaml
notify:
  channels:
    ops-tel:
      type: telegram
      enabled: true
      chat_ids: ["123456"]
    oncall-wecom:
      type: wecom
      enabled: true
      targets: ["https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"]
  routing:
    default_channels: [ops-tel]
    rules:
      - name: prod-critical
        match:
          hosts: [prod]
          categories: [ERROR, OOM]
        deliver:
          channels: [ops-tel, oncall-wecom]
```

## LLM Agent（可选）

配置后可通过 Telegram Bot 或 Web Chat 使用自然语言交互：

```yaml
llm:
  enabled: true
  providers:
    openai:
      api_key: "${OPENAI_API_KEY}"
      model: gpt-4o
  permissions:
    dangerous_tools: [restart_container]
    dangerous_host_allowlist: [prod]
```

Agent 具备的能力：查询容器状态、读取日志、重启容器（需明确确认）、查看告警历史等。

## 环境变量

| 变量 | 说明 |
|------|------|
| `WEB_AUTH_TOKEN` | Web API 鉴权 token（必须） |
| `WEB_ADMIN_TOKEN` | 管理接口 token（必须） |
| `LOGDOG_CONFIG` | 配置文件路径（默认 `config/logdog.yaml`） |
| `LOGDOG_METRICS_DB_PATH` | 指标数据库路径（默认 `data/logdog.db`） |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot token |
| `TELEGRAM_CHAT_ID` | 兜底 chat_id |
| `WECOM_WEBHOOK_URL` | 企业微信 webhook URL |
| `WECHAT_WEBHOOK_URL` | 微信群机器人 webhook URL |
| `OPENAI_API_KEY` | OpenAI API key（LLM 功能） |
| `PROD_HOST` | 生产服务器地址 |
| `PROD_PORT` | SSH 端口（默认 22） |
| `PROD_USER` | SSH 用户（默认 root） |
| `PROD_SSH_KEY` | SSH 私钥路径 |

## API 参考

所有请求头：`Authorization: Bearer <token>`

| 端点 | Token | 说明 |
|------|-------|------|
| `GET /health` | 无 | 健康检查 |
| `GET /api/hosts` | web | 主机列表 |
| `GET /api/hosts/{host}/containers` | web | 容器列表 |
| `GET /api/alerts` | web | 告警记录 |
| `GET /api/hosts/{host}/metrics/{container}` | web | 容器指标 |
| `GET /api/hosts/{host}/system-metrics` | web | 宿主机指标 |
| `GET /api/mutes` | web | 静默规则 |
| `GET /api/storm-events` | web | 告警风暴事件 |
| `POST /api/ws-ticket` | web | 签发 WebSocket ticket |
| `WS /ws/chat` | ticket | 实时 Chat |
| `POST /api/reload` | admin | 热重载配置 |
| `GET /api/send-failed` | admin | 发送失败记录 |

查询参数 `limit` 范围：`/api/alerts`、`/api/mutes`、`/api/storm-events` 为 `1..1000`；指标接口为 `1..10000`。

## 热重载

```bash
curl -X POST http://localhost:8000/api/reload \
  -H "Authorization: Bearer your-admin-token"
```

- 成功时更新：主机配置、通知路由、调度、chat/telegram runtime
- 失败时自动回滚，并推送失败通知
- 移除主机需重启进程完全释放资源

## 运行测试

```bash
pytest -q
```

## 安全说明

- 生产环境请通过反向代理（Nginx/Caddy）提供 TLS，并限制 `/api/reload` 的来源 IP
- 不要使用 `allow_insecure_default_tokens=True`
- SSH 私钥权限建议设为 `600`
- Telegram Bot 的 `authorized_users` 列表务必配置，防止未授权用户控制 Agent

## License

MIT
