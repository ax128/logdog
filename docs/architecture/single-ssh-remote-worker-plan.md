# Single SSH Remote Worker Plan

## 1. 文档目的

本文档用于固定 LogDog 远程 SSH 访问链路的目标约束，作为后续实现、测试、审查的统一基线。

重点不是把 agent、LLM、告警系统搬到远端，而是把远端主机访问收敛为:

1. 单 host 单 SSH transport
2. 单 host 单长驻 worker channel
3. 远端只承接受限、确定性的轻量数据处理
4. 本地继续保留 agent/LLM/工具链主控

本文档以当前代码已经形成的主结构为基础，明确哪些语义必须保持，哪些行为禁止扩张。

## 2. 目标与非目标

### 2.1 目标

- 每个启用 `remote_worker` 的 SSH host，在稳态下只保留一条 SSH transport。
- 每个启用 `remote_worker` 的 SSH host，在稳态下只保留一个长驻 worker channel，通过该 channel 复用后续请求、流式日志、事件、心跳和关闭指令。
- 远端 worker 只部署到临时目录，不做常驻安装，不写入长期状态。
- SSH 断开、心跳超时、进程退出、显式关闭时，远端临时目录和工作目录要自动清理。
- 远端解释器按 `python3 -> python` 顺序探测，避免硬编码单一命令。
- 远端仅允许确定性轻处理: 过滤、裁剪、去重、脱敏、告警关键词分类等；不允许承接任意自定义 Python 预处理器。
- agent 调度、LLM 分析、通知、策略编排、工具注册仍由本地主进程负责。

### 2.2 非目标

- 不把 LogDog 变成远端常驻 daemon 安装方案。
- 不在单 host 上保留多个长期 SSH transport 作为并行池。
- 不把任意模板目录、任意 Python 文件、任意用户脚本下发到远端执行。
- 不把 chat runtime、tool registry、watch manager、通知路由、LLM provider 调用搬到远端。

## 3. 核心语义

### 3.1 单 host 单 SSH transport

对每个 `ssh://` 且 `remote_worker.enabled: true` 的 host，LogDog 在 `RUNNING` 稳态必须满足:

- 仅有一条存活的 SSH transport
- 仅有一个对应的远端 worker 进程
- 仅有一个与该 worker 绑定的长驻 channel

这里的“单 SSH”语义明确为“单 transport + 单长驻 worker channel”，不是“每次调用重新建连”。

### 3.2 bootstrap 与稳态的边界

允许在建连握手阶段执行以下短时操作:

- 解释器探测
- 远端临时目录创建
- 运行时文件上传
- 启动 worker channel

但这些操作必须服务于建立单个长驻 worker channel，不能演化为多个长期常驻 channel，更不能在稳态中继续为每次功能调用重复开新 transport。

### 3.3 retry 语义

- retry 只能发生在上一条 host 会话已经进入 `CLOSED` 后。
- 旧会话没有完成关闭和 tombstone 之前，不允许为同一 host 再创建新的长期 worker 会话。
- `CONNECTING -> HANDSHAKING -> RUNNING -> DRAINING -> CLOSED` 是单 host 会话的基本生命周期。

## 4. 架构边界

### 4.1 本地职责

以下能力必须保持在本地主进程:

- host 管理与 reload
- watch manager / 监控编排
- agent 调配
- tool registry
- chat runtime / LLM 调用
- 通知路由与告警分发
- 指标落库、历史查询、报表和策略控制

远端 worker 只是“本地主控的远端执行分身”，不是独立调度中心。

### 4.2 远端职责

远端 worker 只负责通过单长驻 channel 提供受限能力:

- 连接并访问目标主机上的 Docker CLI
- 容器列表查询
- 容器 stats 查询
- 容器日志查询 / 流式日志
- Docker events 流
- 容器 restart / exec
- 主机系统指标采集
- 受限的日志轻处理
- 接收 heartbeat
- 接收 shutdown 并执行清理

### 4.3 禁止的远端扩张

以下行为不属于本方案允许范围:

- 远端动态 import 本地自定义 Python preprocessor
- 远端执行来自配置文件的任意 shell / Python 代码
- 远端持久化 agent 状态、LLM 上下文、通知状态
- 远端绕过本地 tool registry 直接完成分析决策

## 5. 远端部署与清理约束

### 5.1 临时目录约束

- 远端 worker 部署目录必须放在临时根目录下，默认 `/tmp`
- 目录前缀固定为 `logdog-remote-worker.*`
- 该目录只用于本次连接期间的运行时文件和工作区
- 不要求远端机器预安装 LogDog

### 5.2 连接建立时行为

首次建连阶段按以下顺序执行:

1. 解析 `ssh://` host 配置
2. 建立单条 SSH transport
3. 探测远端解释器: `python3` 优先，失败再试 `python`
4. 在 `remote_worker.temp_root` 下创建临时目录
5. 上传远端 worker 运行时文件
6. 通过单个长驻 channel 启动 `logdog.remote.worker_main`
7. 本地完成 `connect_host` 握手并登记为 `RUNNING`

### 5.3 连接断开与自动清理

以下场景都必须触发远端 cleanup:

- 本地显式 `close_host`
- 应用 shutdown
- 配置 reload 删除该 host
- SSH channel EOF / connection closed
- 心跳超时

清理目标包括:

- 远端 deploy 目录
- 远端 workspace 目录
- worker 进程自身占用的临时数据

清理原则:

- 只删除本方案创建的临时目录
- 只清理前缀受控的 `logdog-remote-worker.*` 目录
- 清理失败按 best-effort 处理，但必须尽量在退出前执行

### 5.4 心跳与断链退出

- 本地定时向远端 worker 发送 heartbeat。
- 远端 worker 在超过 `heartbeat_timeout_seconds` 未收到活动后，必须自我关闭。
- 远端底层 stdio 读取到 EOF 或连接关闭时，必须立即进入清理和退出。
- 心跳链路的目的不是保活多个连接，而是保证单连接死亡后远端分身能自清理。

## 6. 远端处理能力边界

### 6.1 允许的确定性轻处理

远端 worker 允许执行以下确定性、可审计、低风险的轻处理:

- include / exclude 过滤
- 最低日志级别筛选
- redact 脱敏
- alert keyword / category 规则分类
- head / tail 裁剪
- 固定窗口去重
- ring buffer 限流与内存约束

这些处理的目标是减少链路流量、降低本地无效数据量、在不改变整体主控架构的前提下做轻量预处理。

### 6.2 禁止的预处理类型

远端 worker 明确不承接:

- 任意自定义 Python preprocessor
- 从 `templates/preprocessors/` 或其他目录动态加载的用户代码
- 任意插件式扩展逻辑
- 任意脚本执行型“预处理”

结论很明确: 远端只允许规则型、确定性、内建轻处理；复杂预处理和自定义 Python 逻辑仍留在本地。

## 7. 配置字段

以下字段构成当前方案的主机侧配置契约。

### 7.1 host 基础字段

```yaml
hosts:
  - name: prod
    url: ssh://root@10.0.1.1:22
    ssh_key: /root/.ssh/id_ed25519
    ssh_password: ""
    strict_host_key: true
    docker_timeout_seconds: 30
    remote_worker:
      enabled: true
      temp_root: /tmp
      heartbeat_interval_seconds: 15
      heartbeat_timeout_seconds: 45
      heartbeat_poll_interval_seconds: 1
```

### 7.2 字段说明

- `url`
  - 必须是 `ssh://` 才允许启用 remote worker
- `ssh_key`
  - SSH 私钥路径
- `ssh_password`
  - 密码认证场景使用；优先级低于 URL 内嵌密码
- `strict_host_key`
  - 是否严格校验 host key（默认 `false`；如需严格校验需显式配置为 `true` 并确保 `known_hosts` 已包含目标主机指纹）
- `docker_timeout_seconds`
  - 远端 Docker CLI 调用超时
- `remote_worker.enabled`
  - 打开单 SSH remote worker 模式
  - 对 `ssh://` host 默认视为 `true`（opt-out），如需回退 direct backend 必须显式配置 `false`
- `remote_worker.temp_root`
  - 远端临时根目录，默认 `/tmp`
- `remote_worker.heartbeat_interval_seconds`
  - 本地发送 heartbeat 的间隔
- `remote_worker.heartbeat_timeout_seconds`
  - 远端判定主链路失活并自清理退出的超时
- `remote_worker.heartbeat_poll_interval_seconds`
  - 远端 stdio 读循环的轮询间隔

### 7.3 配置兼容约束

- 非 `ssh://` host，即使误配 `remote_worker.enabled: true`，也不应进入 remote worker 模式。
- `remote_worker.enabled: false` 时，仍走现有 direct backend。
- `ssh://` host 未显式配置 `remote_worker.enabled` 时，默认走 remote worker backend（远端下发）。
- 本方案不引入“每个动作单独 SSH 参数”的新配置分支，避免再次分裂链路。

## 8. 生命周期时序

### 8.1 建连时序

1. 本地发现 host 为 `ssh://` 且 `remote_worker.enabled: true`
2. `HybridHostAdapter` 将该 host 路由到 remote worker backend
3. backend 申请该 host 的 lifecycle session
4. 建立 SSH transport
5. 探测 `python3 -> python`
6. 创建远端临时目录
7. 上传 worker 运行时文件
8. 打开单个长驻 worker channel
9. 启动远端 `worker_main`
10. 本地发送 `connect_host`
11. 会话进入 `RUNNING`
12. 后续日志、事件、stats、exec、metrics 全部复用该长驻 channel

### 8.2 稳态时序

稳态下每个 host 只存在:

- 一个本地 host session 缓存条目
- 一条 SSH transport
- 一个远端 worker 进程
- 一个 heartbeat 循环
- 一个长驻 worker channel

### 8.3 关闭时序

1. 本地取消 heartbeat task
2. 本地把 lifecycle 状态切到 `DRAINING`
3. 本地通过长驻 channel 发送 `shutdown`
4. 远端执行 cleanup hook
5. 远端删除 deploy/workspace 临时目录
6. 远端关闭 stdio/channel
7. 本地关闭 session
8. lifecycle 进入 `CLOSED`

### 8.4 异常断链时序

1. 底层 channel 断开或 heartbeat 丢失
2. 本地 heartbeat 失败，移除 host session
3. 或远端因 EOF / idle timeout 检测到链路失活
4. 远端执行 cleanup
5. 远端退出
6. 本地把生命周期状态标记为 `CLOSED`
7. 只有在 `CLOSED` 后，才允许 retry

## 9. 与当前代码对齐的目标约束

后续实现和审查必须以以下对齐目标为准:

### 9.1 入口与路由

- `logdog.main` 继续负责在存在 remote worker host 时装配 `HybridHostAdapter`
- `HybridHostAdapter` 继续统一 direct backend 与 worker backend 路由
- agent / tool / LLM 侧依然只感知统一的 host 访问抽象，不直接感知 SSH 细节

### 9.2 会话与生命周期

- `logdog.remote.worker_backend` 继续作为单 host 会话缓存、心跳、关闭、请求复用的主实现
- `logdog.remote.ssh_lifecycle` 继续负责 `CONNECTING/HANDSHAKING/RUNNING/DRAINING/CLOSED` 生命周期约束
- 不允许为追求“简单”而回退到“每次请求重新 SSH exec”的做法

### 9.3 远端入口

- `logdog.remote.worker_main` 继续作为远端唯一 worker 入口
- 远端入口继续使用 stdio 作为 transport
- 清理逻辑继续由远端 cleanup hook + workspace cleanup 完成

### 9.4 轻处理边界

- `logdog.remote.worker_pipeline` 的职责只限确定性轻处理
- 本地自定义 preprocessor 体系与模板体系不得被隐式搬到远端执行
- 如果将来要扩展远端处理能力，只能增加可审计的内建规则，不能开放任意代码执行

### 9.5 指标与工具链

- host metrics 必须优先复用 remote worker 通道，而不是另起 SSH 采集链路
- tool registry、chat runtime、告警分析、通知发送继续保留在本地
- remote worker 的输出必须服务于现有工具链，不能反向定义本地主流程

## 10. 验收约束

后续代码若要认为“符合本方案”，至少必须满足:

1. 单 host 稳态下只有一条 SSH transport 和一个长驻 worker channel
2. host reload / close / shutdown 能关闭对应 worker 并清理远端临时目录
3. 远端解释器探测遵循 `python3 -> python`
4. 远端不执行任意自定义 Python preprocessor
5. agent / LLM / tool registry 仍由本地持有主控
6. host metrics、日志、事件、exec 等调用复用同一 worker 会话
7. retry 不会在旧会话未关闭前并发创建新的长期 session

## 11. 结论

本方案不是“把 LogDog 搬到远端”，而是“把 SSH 访问收敛成单链路远端分身”，同时保持本地 agent/LLM/工具体系不变。

判断实现是否正确，核心只看三件事:

1. 单 host 是否真的维持单 transport / 单长驻 worker channel
2. 远端是否真的只使用临时目录并在断链时自清理
3. 远端能力是否始终被限制在确定性轻处理与受控 Docker/metrics 访问之内
