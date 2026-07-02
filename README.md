# LLM API Router

> 一个**对客户端只暴露一个虚拟 API key**、**对上游持有多个真实 key**的 LLM 网关。
> Claude Code / Codex 把它当作普通 Anthropic / OpenAI endpoint 使用，
> 限额、轮换、故障转移在网关内部透明发生，agent 不感知、不修改、能力不丢。
> **支持反向隧道模式**：防火墙内设备通过 WebSocket 长连接注册为外网 relay 的资源池，无需开放入站端口。

```
┌────────────────────────────────────────┐
│  用户层  Claude Code / Codex / SDK     │
│  - 多模态、工具调用、长上下文           │
│  - 只持有一个虚拟 key (vk-xxx)         │
└─────────────────┬──────────────────────┘
                  │  HTTP/SSE
┌─────────────────▼──────────────────────┐
│  代理层  llm_api_router  (本项目)      │
│  - Anthropic / OpenAI Chat / Responses │
│  - Key 池轮换 + 速率监控 + 故障转移    │
│  - SSE 流式透传 + token 用量统计       │
│  - 反向隧道：防火墙内设备 WS 注册      │
└───────┬─────────────────┬──────────────┘
        │                 │
        │ HTTP            │ WebSocket (长连接)
        ▼                 ▼
┌───────────────┐ ┌────────────────────────┐
│  上游 API     │ │  防火墙内 inner 节点    │
│  (直连可达)   │ │  (通过 tunnel 反向接入) │
└───────────────┘ └────────────────────────┘
```

---

## 它解决什么

| 痛点 | 解法 |
|---|---|
| 单 key 撞限额，agent 任务中断 | 池内自动切下一个 key，对客户端透明 |
| 手头有多个 key 但无法轮换 | 池统一管理，按策略调度 |
| key 失效后无法及时发现 | 健康检查 + 冷却 + 自动剔除 + 告警钩子 |
| 不想把多个 key 暴露给每个客户端 | 客户端只持一个 vk-xxx，真实 key 留在网关 |
| Claude Code 与 Codex 协议不同 | 网关同时暴露 Anthropic / OpenAI / Responses 三套兼容端点 |
| 内网设备有资源但防火墙限制入站 | 反向隧道：inner 主动连接 relay，通过 WebSocket 注册为资源池 |

---

## 快速上手

### 1. 安装

```powershell
cd I:\spirite_project\llm_api_router
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

### 2. 配 key 池

`keys.yaml`：
```yaml
pools:
  anthropic:
    type: anthropic                      # 决定协议: anthropic | openai_chat | openai_responses
    default_upstream: https://api.anthropic.com
    keys:
      - { id: ak1, key: "sk-ant-xxxx-1", weight: 1 }
      - { id: ak2, key: "sk-ant-xxxx-2", weight: 2 }             # 配额大→权重高
      - { id: ak3, key: "中转key", upstream: "https://relay.xxx/anthropic", support_models: [claude-sonnet-4-6] }  # key 级覆盖 upstream + 模型白名单
  openai:
    type: openai_responses               # Codex 默认走的协议
    default_upstream: https://api.openai.com
    keys:
      - { id: ok1, key: "sk-xxxx-1" }
```
> 端点按 `type` 自动路由，无需手写 endpoints：`anthropic`→`/anthropic/v1/messages`，`openai_responses`→`/v1/responses`，`openai_chat`→`/v1/chat/completions`。

`config.yaml`：
```yaml
listen: { host: 127.0.0.1, port: 4000 }
virtual_keys:
  - { key: "vk-local-001", pools: [anthropic, openai] }
strategy: cache_affinity_ttl   # 默认：300s 内相同内容复用同 key 吃缓存；超窗口则负载均衡
cache_ttl_seconds: 300
default_cooldown_seconds: 60
hot_reload: true               # 改 keys.yaml 增删资源无需重启
```

### 3. 启动

```powershell
llm-router serve
```

### 4. 接入 Claude Code

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:4000/anthropic"
$env:ANTHROPIC_AUTH_TOKEN = "vk-local-001"
claude
```

### 5. 接入 Codex

`~/.codex/config.toml`：
```toml
[model_providers.router]
name = "router"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"
env_key = "OPENAI_API_KEY"

[profiles.router]
model_provider = "router"
model = "gpt-5"
```

```powershell
$env:OPENAI_API_KEY = "vk-local-001"
codex --profile router
```

### 6. 验证

```powershell
llm-router validate       # 离线校验 keys.yaml/config.yaml
llm-router stats          # 各 key 当前用量、冷却、健康度（拉 /stats）
llm-router reload         # 改了 keys.yaml 想立即生效（自动用 config.yaml 的 admin_token）

# 真实调用冒烟（Anthropic 协议）
curl -s http://127.0.0.1:4000/healthz
curl -s -X POST http://127.0.0.1:4000/anthropic/v1/messages `
  -H "x-api-key: vk-local-001" -H "anthropic-version: 2023-06-01" `
  -d '{"model":"glm-5.2","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}'
```

### 7. 管理面（admin_token）

`/admin/*` 由 `config.yaml` 的 `admin_token` 保护（不配则全部 403，业务调用不受影响）：

```powershell
$tok = "<config.yaml 里的 admin_token>"
curl -s http://127.0.0.1:4000/admin/pools  -H "authorization: Bearer $tok"   # 看池/key 详情
curl -s -X POST http://127.0.0.1:4000/admin/reload -H "authorization: Bearer $tok"  # 强制热加载
```

`admin_token` ≠ vk：vk 给业务调用（claude/codex）走业务路径；admin_token 给运维走 `/admin/*`。详见 DESIGN §10.4。

### 8. 前端管理界面

一键启动后自动打开 `http://127.0.0.1:4000/ui/`（也可手动访问）。无构建单文件 UI（原生 JS，后端 `StaticFiles` 托管），5 个 Tab：

- **概览**：池/key 健康、近 1h 请求/成功率、冷却中 key 列表（一键重置冷却）
- **池/Key**：每个 key 的权重/upstream/支持模型/状态，启用·禁用·重置冷却
- **Virtual Keys**：vk 列表 + 新增（含随机生成）/ 删除（写 config.yaml + 热加载）
- **日志**：最近 200 条请求（5s 自动刷新），vk/池/key/状态/耗时/重试
- **配置**：config.yaml + keys.yaml 只读视图（token/真实 key 已脱敏）+ 强制热加载按钮

进入需 `admin_token`（来自 config.yaml，仅存浏览器 sessionStorage）。详见 DESIGN §16。
完整 Vue+Vite 升级路径见 DESIGN §16.3（仅当管理面显著变复杂时才需要）。

### 一键启动（前后端）

```powershell
# 方式一：双击 start.bat
# 方式二：命令行
powershell -ExecutionPolicy Bypass -File start.ps1
```

脚本会：检查/安装依赖 → 校验配置 → **打印服务信息横幅**（对外 endpoint、虚拟 key、支持模型、各池真实资源(脱敏)、Claude Code/Codex 接入示例）→ 启动 uvicorn（同时托管 UI）→ 就绪后自动开浏览器到 `/ui/`。`Ctrl+C` 停止。

随时想看这份信息：`llm-router info`（或 `py -3 -m llm_api_router.cli info`），不启动服务只打印。

---

## 反向隧道模式（Tunnel Pool）

> 防火墙内设备无法被外部连接，但持有真实 LLM key。通过 WebSocket 长连接主动注册到外网 relay，外部请求透过隧道到达内网设备。

```
外部客户端 (Claude Code / Codex)
   │ HTTP/SSE
   ▼
relay (llm_api_router, 可被外部访问, port 4000)
   │  持久 WebSocket (/tunnel/connect)
   ▼
inner (llm_api_router, 防火墙内, port 4001)
   │ HTTP
   ▼
真实 LLM 上游 (Anthropic / OpenAI / 兼容网关)
```

### Relay 侧配置

`config_relay.yaml`：
```yaml
listen: { host: 0.0.0.0, port: 4000 }
tunnel_token: "your-shared-secret"    # inner 连接时的认证令牌
virtual_keys:
  vk-relay-001: []                    # 对外暴露的虚拟 key
```

`keys_relay.yaml`：
```yaml
pools:
  inner_tunnel:
    type: tunnel                       # 隧道池类型
    auth_scheme: x-api-key
    keys:
      - id: inner01                    # 需与 inner 的 --tunnel-id 一致
        key: "dummy-not-used"          # type=tunnel 时 key 字段无意义
        upstream: "tunnel://inner01"   # 固定格式 tunnel://<tunnel_id>
        weight: 1
        support_models:
          - claude-sonnet-4-6
          - claude-opus-4-7
```

### Inner 侧配置

`config_inner.yaml`：
```yaml
listen: { host: 127.0.0.1, port: 4001 }
virtual_keys:
  vk-inner-local-only: []
```

`keys_inner.yaml`：
```yaml
pools:
  real_upstream:
    type: anthropic
    auth_scheme: x-api-key
    keys:
      - id: real01
        key: "sk-your-real-key"
        upstream: https://api.anthropic.com
        support_models: [claude-sonnet-4-6, claude-opus-4-7]
```

### 启动

```powershell
# 1. 启动 relay（外网可达机器）
py -3 -m llm_api_router.cli --config config_relay.yaml --keys keys_relay.yaml serve

# 2. 启动 inner（防火墙内机器）
py -3 -m llm_api_router.cli --config config_inner.yaml --keys keys_inner.yaml serve

# 3. 启动 tunnel client（内网机器上，连接 relay）
py -3 -m llm_api_router.cli tunnel-client \
  --relay-url ws://relay-host:4000/tunnel/connect \
  --tunnel-id inner01 \
  --token "your-shared-secret" \
  --forward-url http://127.0.0.1:4001 \
  --forward-vk vk-inner-local-only
```

### 接入

与普通模式完全一致，客户端无感知：
```powershell
$env:ANTHROPIC_BASE_URL = "http://relay-host:4000/anthropic"
$env:ANTHROPIC_AUTH_TOKEN = "vk-relay-001"
claude
```

### 特性

- **多路复用**：同一 WebSocket 连接承载多个并发请求（req_id 区分）
- **流式支持**：SSE 流式响应通过二进制帧高效传输（4字节 tag + chunk，无 base64 开销）
- **断线重连**：inner 断线后指数退避重连（1s → 2s → 4s → ... → 60s）
- **应用层心跳**：relay 发 `{"type":"ping"}`，inner 回 `{"type":"pong"}`，50s 超时检测
- **认证链**：relay vk → tunnel WS → inner vk → 真实 key，每层独立鉴权
- **健康检查**：`GET /healthz` 返回当前连接的 tunnel 列表

---

## 与既有 agent_scope_ranger 的协作

```
agent_scope_ranger  ─►  llm_api_router  ─►  Anthropic / OpenAI
       (长期任务编排)            (key 轮换网关)         (上游)
```

`agent_scope_ranger` 把所有 agent 子进程的 base_url 指向 `llm_api_router`，
撞限由网关自动切 key，**长期任务不会因为单个 key 配额耗尽而断**。

---

## 动态扩缩容（不重启）

`keys.yaml` 是唯一的大模型资源清单。改它存盘 → 默认 2 秒内自动生效，**无需重启服务**：

- **加资源**：在 `keys.yaml` 新增一个池（新厂商/新 key）→ 存盘 → 立即可用。
- **减资源 / 临时下线**：删 key 条目，或给 key 标 `disabled: true` → 存盘生效。
- **立即生效**：不想等 2s 轮询，`llm-router reload`（或 `POST /admin/reload`）强制热加载。
- **校验**：`llm-router validate` 离线检查配置不实际加载。

> CLI 的 `keys add/rm`、`rotate`、`pools drain` 等在线增删命令为 roadmap（DESIGN §9.3）；当前增删走"编辑 keys.yaml + 热加载"，效果等价。

热加载覆盖：池/上游/key/权重/并发、virtual_key、端点→池路由、策略与超时参数。
需重启的只有：监听地址、日志配置、admin_token。

详见 [DESIGN.md §15](DESIGN.md)。

---

## 文档索引

- [DESIGN.md](DESIGN.md) — 完整设计：分层架构、协议矩阵、调度策略、流式透传、反向隧道、可观测性、安全模型
- [config.example.yaml](config.example.yaml) — 完整配置示例（含全部可调参数注释）
- [keys.example.yaml](keys.example.yaml) — Key 池配置示例

---

## 与 LiteLLM / One API 的关系

**两者都能解决同样的问题。** 选自建的理由：

- 代码量极小（< 1000 行），可控可改
- 与 `agent_scope_ranger` 同栈（Python），共享日志/部署/看门狗
- 针对 Claude Code 与 Codex 实测两条路径，避免通用网关在边角协议上的踩坑（如 Anthropic 的 `prompt-caching` header、Codex 的 Responses API、SSE 心跳）

需要更多功能（费用核算、观察面板、多用户、配额）时可平滑迁移到 LiteLLM。
