# LLM API Router — 设计文档 (DESIGN)

> 版本：v2 ｜ 路径：`D:\Users\hs.wu\Desktop\llm_api_router`
> 关联文件：`README.md`、`config.example.yaml`、`keys.example.yaml`

---

## 1. 背景与目标

### 1.1 问题陈述
- 直接给 Claude Code / Codex 配单个真实 API key，撞限额即任务中断；
- 手头有多个 key，但 agent CLI 不支持运行时切换；
- 自己写代码做 key 池又会丢掉 Claude Code/Codex 的 agentic 能力（工具/多模态/规划）。

### 1.2 设计目标
- **客户端单 key**：每个客户端只持有一个虚拟 key（vk-xxx），真实 key 不暴露给客户端。
- **协议无缝**：Claude Code（Anthropic Messages API）、Codex（OpenAI Chat / Responses API）改 base_url 即可接入，**不修改任何业务逻辑**。
- **撞限自愈**：429/529/`overloaded_error` 自动切换下一个 key 重试，对客户端透明。
- **流式透传**：SSE 不缓冲（含工具调用、思考内容、结构化输出全程流式）。
- **零特殊处理**：尽量做"哑代理"，不解析业务字段；只在路由/认证/计量层动手。
- **可观测**：每条请求有 trace 日志，每个 key 有用量、冷却、错误率统计。
- **可热更新**：keys.yaml 改动后自动加载，不重启服务。

### 1.3 非目标
- 不改写请求体语义（不做 prompt 重写、模型替换、内容过滤）。
- 不做跨厂商协议转换（不把 OpenAI 请求翻译成 Anthropic）——一个 endpoint 对应一种上游协议。
- 不做计费、配额、用户系统（仅 vk → 池映射）。
- 不做分布式部署（单机即可满足 agent 永续运行需求）。

---

## 2. 总体架构

```
┌────────────────────────────────────────────────────────────────────┐
│  Admin UI  (浏览器单页应用，可选)                                   │
│   ├─ 池/key 健康看板 + 用量图表 + 冷却倒计时                        │
│   ├─ vk 管理 + 池/key 启用/禁用/重置冷却 + 强制 reload              │
│   └─ 实时日志流（SSE）                                              │
│   通过 X-Admin-Token 调 /admin/* 端点；详见 §17                     │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────────────┐
│  HTTP Server  (FastAPI + uvicorn, async)                           │
│   ├─ POST /anthropic/v1/messages           Anthropic 兼容          │
│   ├─ POST /v1/chat/completions             OpenAI Chat 兼容        │
│   ├─ POST /v1/responses                    OpenAI Responses (codex)│
│   ├─ GET  /v1/models  /anthropic/v1/models  本地聚合(不转发上游)   │
│   ├─ GET  /healthz / /stats                公开探活                │
│   ├─ WS   /tunnel/connect                  反向隧道接入点          │
│   ├─ /admin/* (admin_token 鉴权)           reload / pools / ...   │
│   ├─ 静态资源 /ui/*                         Admin UI 由本进程托管    │
│   └─ Auth 中间件：校验 vk → 解析允许的池                            │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────────────┐
│  Router 调度层                                                      │
│   ├─ Selector：cache_affinity_ttl(默认) / rr / wrr / lru / la       │
│   ├─ model 过滤：按 key.support_models 白名单                       │
│   ├─ Cooldown 检查 + 单 key 并发限流                                │
│   └─ 粘性表 fingerprint→(key,ts)：窗口内复用，超窗口负载均衡         │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────────────┐
│  Forwarder 转发层  (httpx.AsyncClient + SSE / TunnelManager)       │
│   ├─ 直连模式：httpx 发请求到上游，auth_scheme 注入，SSE 透传       │
│   ├─ 隧道模式：TunnelManager 通过 WS 帧转发请求到 inner 节点        │
│   ├─ 非流式：aread() / WS body 帧；4xx 原样透传                    │
│   ├─ 流式：aiter_raw() / WS binary chunk 帧；首字节前可重试切 key    │
│   └─ 错误分流：429/529 冷却+重试；401/403 标 disabled；4xx 透传     │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────────────┐
│  KeyPool 数据层                                                     │
│   ├─ keys.yaml + config.yaml 热加载（mtime 轮询，原子 swap）        │
│   ├─ KeyState：cooldown / fails / requests / tokens / active        │
│   └─ 周期 flush 到 state.json（重启不丢冷却/统计）                   │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│  反向隧道层  (inner → relay WebSocket)                              │
│   ├─ TunnelClient (inner 侧)：主动连接 relay /tunnel/connect       │
│   │   ├─ 接收 request 帧 → httpx 执行本地请求                       │
│   │   ├─ 流式响应逐 chunk 通过 binary 帧发回 relay                   │
│   │   └─ 断线指数退避重连 (1s→2s→4s→...→60s)                        │
│   └─ TunnelManager (relay 侧)：管理 tunnel_id → WS 连接映射         │
│       ├─ proxy_request()：通过 WS 帧转发请求并收集响应               │
│       ├─ 并发复用：同一 WS 承载多 in-flight 请求 (req_id 区分)       │
│       └─ 心跳检测：app-level ping/pong，50s 无活动踢出               │
└────────────────────────────────────────────────────────────────────┘
```

**关键不变量**：每个 endpoint 对应**一种上游协议**；网关只在 header 层改写鉴权，不动 body；流式开始下行后不切 key。

---

## 3. 核心概念

| 概念 | 说明 |
|---|---|
| **Virtual Key (vk)** | 客户端持有的单一 key，形如 `vk-local-001`，由本网关签发，存于 `config.yaml` |
| **Real Key** | 真实上游 key（sk-ant-xxx / sk-xxx），存于 `keys.yaml`，永不出网关 |
| **Pool** | 一组真实 key 的集合，由 `type` 声明协议（`anthropic`/`openai_chat`/`openai_responses`/`tunnel`），可有 `default_upstream` + 多个 key。pool 名任意，如 `anthropic`、`openai`、`deepseek` |
| **type** | pool 的协议类型，决定鉴权头/SSE/token 字段/默认端点（§4.1）；`tunnel` 类型走 WebSocket 反向隧道 |
| **Endpoint** | 网关对外 URL，按 pool 的 `type` 约定式默认路由挂载（§4.2），可被 config.yaml `endpoints` 覆盖 |
| **Strategy** | Pool 内选 key 的策略：`cache_affinity_ttl`(默认) / `cache_affinity` / `round_robin` / `weighted_round_robin` / `lru` / `least_active` |
| **Cooldown** | key 失效/限流后进入的冷却期，期间不被调度 |
| **Tunnel** | 反向隧道模式：防火墙内 inner 节点通过 WebSocket 长连接注册为 relay 的资源池（§19） |

---

## 4. 协议矩阵

### 4.1 pool 的 type —— 协议判定依据

每个 pool 必须声明 `type`，这是协议判定的唯一依据（决定鉴权头、SSE 格式、token 字段、默认挂载端点）：

| type | 上游协议 | 鉴权头注入 | token 字段 | 默认挂载端点 | 典型客户端 |
|---|---|---|---|---|---|
| `anthropic` | Anthropic Messages | `x-api-key` | `input_tokens`/`output_tokens` | `/anthropic/v1/messages`、`/anthropic/v1/messages/count_tokens` | Claude Code, Anthropic SDK |
| `openai_responses` | OpenAI Responses | `Authorization: Bearer` | `input_tokens`/`output_tokens`/`reasoning_tokens` | `/v1/responses`、`/v1/models` | Codex (`wire_api=responses`，**默认**) |
| `openai_chat` | OpenAI Chat Completions | `Authorization: Bearer` | `prompt_tokens`/`completion_tokens` | `/v1/chat/completions`、`/v1/models` | Codex (`wire_api=chat`), OpenAI SDK |
| `tunnel` | 反向隧道（WS 转发） | `x-api-key`（默认） | 由 inner 端上游决定 | 全部端点（anthropic + openai） | 防火墙内设备通过 WS 注册 |

> pool 名任意（描述"哪批 key"），`type` 才决定协议。一个 type 可有多个 pool（如 `openai` 官方池 + `deepseek` 兼容池都 `type: openai_chat`）。

### 4.2 约定式默认路由

**无需在 config.yaml 写 endpoints**。router 启动时按每个 pool 的 `type` 自动挂载到对应端点（见上表"默认挂载端点"）。请求到达时，按 URL 找出该端点绑定的所有同 type pool，按 `strategy` 在其可用 key 间选择。

例如：
- 外部调 `/anthropic/v1/messages` → 找 `type:anthropic` 的所有 pool（默认即 `anthropic` 池）→ 选 key。
- 外部调 `/v1/responses` → 找 `type:openai_responses` 的所有 pool → 选 key。
- 外部调 `/v1/chat/completions` → 找 `type:openai_chat` 的所有 pool（`openai_chat`、`deepseek`…）→ 按 strategy 选。

### 4.3 endpoints 覆盖表（可选，仅特殊绑定时用）

config.yaml 的 `endpoints` 可**覆盖默认路由**，用于：
- 让某端点在**指定的池子集**间选择（默认是"所有同 type 池"）：
  ```yaml
  endpoints:
    "/v1/chat/completions": [openai_chat, deepseek]   # 数组=多池,按 strategy 选
    "/v1/responses": openai                            # 字符串=单池
  ```
- 让某 type 的池挂到非默认端点（如让 deepseek 也能走 responses，前提其 upstream 真支持）：
  ```yaml
  endpoints:
    "/v1/responses": [openai, deepseek]
  ```

未在 endpoints 列出的端点走 §4.2 默认路由。

### 4.4 客户端鉴权头识别

| 客户端 | 携带方式 | 网关识别字段 |
|---|---|---|
| Claude Code | `x-api-key: vk-...` 或 `Authorization: Bearer vk-...` | 二者皆可 |
| Anthropic SDK | `x-api-key: vk-...` | x-api-key |
| Codex | `Authorization: Bearer vk-...` | Authorization |
| OpenAI SDK | `Authorization: Bearer vk-...` | Authorization |

网关 Auth 中间件按上述规则提取 vk，校验后**移除并替换为真实 key 的对应头**再转发。
⚠️ 客户端可能同时发 `x-api-key` 和 `Authorization`（SDK 行为）：网关必须**两个都识别、按 pool type 只保留一个**，否则上游双鉴权头报错。

### 4.5 上游 key 注入与 upstream 解析

注入规则按 pool 的 **`auth_scheme`** 字段（不再硬绑 type，因 Anthropic 兼容网关鉴权方式各异）：

| auth_scheme | 注入 | 默认 type |
|---|---|---|
| `x-api-key` | 设 `x-api-key: <real>`，删除 `Authorization` | `anthropic`（官方 Anthropic） |
| `bearer` | 设 `Authorization: Bearer <real>`，删除 `x-api-key` | `openai_chat` / `openai_responses` |

> **关键**：火山引擎 Ark 等中转端点虽是 Anthropic 协议（`type: anthropic`），但用 Bearer 鉴权（`ANTHROPIC_AUTH_TOKEN` 模式）。此时 pool 须显式 `auth_scheme: bearer`，否则用 x-api-key 注入会被上游 401 拒绝。`auth_scheme` 默认按 type（anthropic→x-api-key，openai→bearer），Ark 类中转需覆盖。

upstream URL 解析优先级（每请求按被选 key 决定）：
1. **key 级 `upstream`**（若该 key 声明）—— 用于中转商 key 与官方 key 同池
2. **pool 级 `default_upstream`** —— 池内 key 的默认上游
3. type 内置官方地址（兜底，如 `anthropic` → `https://api.anthropic.com`）

> **路径拼接**：`anthropic` 端点剥离 `/anthropic` 前缀（`/anthropic/v1/messages` → upstream + `/v1/messages`）；`openai` 端点保留 `/v1/*`。upstream 即客户端原 `ANTHROPIC_BASE_URL` / OpenAI base_url 值（claude 会自动追加 `/v1/messages`）。
>
> **content-encoding 处理**：网关用 httpx 转发，非流式响应 `aread()` 已自动解压 body，故须**剥离 `content-encoding` 头**（否则客户端二次解压报 `incorrect header check`）；流式响应用 `aiter_raw()` 透传原始（压缩）字节，保留 `content-encoding`。

### 4.6 必须透传的 header（不改写）

- `anthropic-version`、`anthropic-beta`
- `OpenAI-Beta`
- 任何 `x-stainless-*`（SDK 自带，部分上游会校验）
- `accept`、`content-type`、`user-agent`

### 4.7 需要重写/剥离的 header（避免误导客户端）

| header | 处理 | 原因 |
|---|---|---|
| `anthropic-ratelimit-*`（requests/tokens/reset） | **剥离**，或重写为聚合保守值 | 单 key 的配额头不代表全池；Claude Code 会据此退避，可能过度退避或过发 |
| `x-ratelimit-*` / `retry-after`（429 响应） | 全池冷却时按最短恢复时间重算 | 同上 |
| `OpenAI-Organization` / `OpenAI-Project` | **按被选中 key 所属 org 覆盖** | 池内多 key 跨 org 时，客户端头与 key 不匹配 → "model not found" 或计费错乱。约束：同池 key 建议同 org；跨 org 必须由网关覆盖 |
| 上游回传的 `x-api-key` 残留 | 删除 | 防泄漏 |
| `set-cookie` / 任何上游会话 cookie | 删除 | 网关无状态，不应继承上游会话 |

### 4.8 辅助端点也走 key 注入（易漏）

下列端点虽非主对话路径，但 Claude Code / Codex 会调用，**必须同样经过 vk 校验 + 真实 key 注入**，否则会暴露"未鉴权可用"或 401：

- `POST /anthropic/v1/messages/count_tokens` —— Claude Code 用它做上下文管理
- `GET /v1/models` —— 多种 SDK/Codex 启动时枚举模型；返回**池内聚合后的稳定模型列表**（按 §4.9 过滤后去重，按字母序），不随被选 key 抖动
- `GET /anthropic/v1/models`（若有客户端探测）—— 同上

### 4.9 model 过滤（support_models）

key 可声明 `support_models: [...]` 白名单。两个过滤点：

1. **选 key 时**：从请求体读 `model` 字段，**只在该端点绑定的池中、`support_models` 包含该 model（或为空=不限制）的 key 间选择**。
   - 例：请求 `model: claude-opus-4-8`，anthropic 池中 ak3 的 `support_models` 只有 sonnet/haiku → ak3 被跳过，只在 ak1/ak2 间选。
   - 例：请求 `model: deepseek-reasoner`，openai 官方池 key 无此 model → 该 key 被跳过，落到 deepseek 池。
2. **`/v1/models` 聚合时**：返回所有可用 key 的 `support_models` 并集（key 未声明=该 key 不贡献约束，模型列表不因此收窄）。

效果：中转商只支持部分模型的场景下，不会把高阶模型请求误发给不支持的中转 key 导致上游 404/计费错乱。

---

## 5. Key 调度模型

### 5.1 选择策略
| 策略 | 行为 | 适用 |
|---|---|---|
| **`cache_affinity_ttl`** | **时间窗口感知粘性**：缓存窗口内复用同 key，超窗口则负载均衡重选 | **默认**。兼顾缓存命中与负载均衡 |
| `cache_affinity` | 纯粘性，同指纹永久粘一个 key（仅冷却才换） | 不推荐，慢速场景单 key 扛压 |
| `round_robin` | 顺序轮转 | key 配额相等且不在乎缓存 |
| `weighted_round_robin` | 按权重 | key 配额差异大、低速率场景 |
| `lru` | 选最久没用的 | 平摊速率限制窗口 |
| `least_active` | 选当前并发最少的 | 高并发场景 |

实现：`Selector.pick(pools, fingerprint=None, model=None, exclude=set()) -> Key`。

**候选集先按 model 过滤**（§4.9）：从该端点绑定的所有池中，筛出 `support_models` 包含 `model`（或为空）且未冷却、未 disabled、并发未满的 key，再按 strategy 在候选集内选。候选集为空 → 按 §8.2 返回 503（all keys for model X unavailable）。

#### cache_affinity_ttl 核心算法（默认策略）
维持粘性表 `fingerprint → (key_id, last_used_ts)`。请求到来时：

```
if 指纹在表中 ∧ (now - last_used_ts) < cache_ttl_seconds ∧ 该 key 健康:
    复用该 key                          # 窗口内：吃缓存，省钱省延迟
    last_used_ts = now
elif 指纹在表中 ∧ 超窗口:
    按 fallback 策略(weighted_round_robin) 重选负载最轻 key   # 缓存已失效，粘着无意义→负载均衡
    更新粘性表 (key_id, now)
elif 指纹不在表中:
    按 fallback 策略选 key
    写入粘性表 (key_id, now)
if 粘性 key 冷却中:
    临时选 least_active 作为 fallback（不覆盖粘性表）
    原 key 恢复后自动回归
```

- **语义**：短时间内相同内容连发 → 同一 key（缓存命中）；长时间间隔后 → 负载均衡到不同 key（摊薄速率配额）。
- **窗口**：`cache_ttl_seconds`（默认 300s，对齐 Anthropic 缓存 TTL）。保守取值：误判失效代价仅一次 miss（等同纯轮换），而误判存活会压制负载均衡，故宁短勿长。
- **指纹**：`hash(system + tools_definition)`，不含 user message（user 内容每次不同，无法缓存）。
- **粘性表清理**：超窗口的条目惰性淘汰（被下次访问重写），后台每 5min 全表扫一次清 >2×窗口未用的条目，防内存增长。
- `exclude` 用于重试时排除已失败 key。

### 5.2 冷却机制（核心）
触发冷却的事件：
- `429 Too Many Requests` → 读 `Retry-After` 头；无则用 `default_cooldown` (60s)
- `529 Overloaded` / Anthropic `overloaded_error` → 30s
- `401/403` → 标记 `disabled`，需人工核查（防误用废 key 持续报错）
- 连续 `5xx` ≥3 次 → 60s

冷却期内，该 key 在 `Selector.pick` 中被跳过；过期自动恢复。

### 5.3 故障转移
请求失败且可重试（429/529/网络错误），从同池**剩余未冷却 key** 中再选一个，最多重试 `max_retries`（默认 3）。全部冷却 → 返回 503 给客户端，并在响应体写明"all keys cooling down"。

### 5.4 并发与背压
单 key `max_concurrent`（默认 8）：超出排队（带 timeout）。这是为了对齐上游的并发限制（Anthropic Tier 1 仅 5 RPM/请求级并发约束）。

### 5.5 用量追踪
- 每次成功响应：累计 `request_count` + `prompt_tokens` + `completion_tokens`（从响应 `usage` 字段读，流式从最后一个事件读）
- 每分钟落一次到 `state.json`
- `--stats` 可看每 key 的 RPM / TPM 估计

---

## 6. 流式透传

### 6.1 SSE 处理
- 不缓冲：用 `httpx.AsyncClient.stream()` + `StreamingResponse` 把 chunk 即时回吐
- 心跳：上游若发 `event: ping` / 注释行，原样透传，避免客户端超时
- 中途错误：上游中途返回错误事件（如 `error: overloaded_error`），需识别并触发故障转移
  - **限制**：只有"在收到任何业务字节之前"出错才能切 key 重试；客户端已经看到部分输出后切 key 会破坏序列。所以 SSE 重试只覆盖"建立流"阶段。
- 客户端断开：`asyncio.CancelledError` 上游连接也立即关闭，避免泄漏并发槽。

### 6.2 非流式
- `response.aread()` 读全量 + 透传，重试覆盖整个请求-响应。

### 6.3 工具调用与思考
- Anthropic `thinking`、tool_use、tool_result 块：网关全部当作不透明字节流，**不解析、不修改**。
- OpenAI Responses 流式事件 (`response.created` / `output_text.delta` / `tool_calls.delta` / ...)：同上。

### 6.4 超时分级（agent 场景调大）

agent 编码单 turn 常超 10 分钟（高 reasoning effort + 大上下文 + 工具调用链）。单一 600s 会误杀正在工作的请求：

| 阶段 | 默认 | 说明 |
|---|---|---|
| `connect_timeout` | 10s | 建连 |
| `first_byte_timeout`（流式） | 60s | 首字节；过载时上游在此阶段慢 |
| `stream_idle_timeout`（流式） | 120s | 两个 chunk 间最大间隔；超时判定上游卡住，**关闭上游连接**但已下行的字节保留给客户端 |
| `total_timeout`（非流式） | 1200s | 整体上限 |
| `total_timeout`（流式） | 1800s | 流式允许更长，因 reasoning 可达数十分钟 |

这些值均可按端点/池在 config 覆盖。

---

## 7. 数据模型

### 7.1 keys.yaml
```yaml
pools:
  anthropic:                              # pool 名任意
    type: anthropic                       # 必填：anthropic | openai_chat | openai_responses
    auth_scheme: x-api-key                # 可选：x-api-key(默认,官方) | bearer(Ark等中转)
    default_upstream: https://api.anthropic.com   # 池默认上游
    keys:
      - id: ak1
        key: "sk-ant-xxx-1"
        weight: 1
      - id: ak3                           # 中转商 key：key 级覆盖 upstream + model 白名单
        key: "中转key"
        upstream: "https://relay.xxx/anthropic"
        weight: 1
        max_concurrent: 8
        support_models: [claude-sonnet-4-6, claude-haiku-4-5-20251001]
        # disabled: false

  openai:
    type: openai_responses
    default_upstream: https://api.openai.com
    keys:
      - { id: ok1, key: "sk-xxx-1" }

  deepseek:                               # 兼容厂商，只支持 chat
    type: openai_chat
    default_upstream: https://api.deepseek.com
    keys:
      - { id: ds1, key: "sk-deepseek-xxx", support_models: [deepseek-v4-pro, deepseek-chat] }

  inner_tunnel:                           # 反向隧道：防火墙内设备注册
    type: tunnel
    auth_scheme: x-api-key
    keys:
      - id: inner01                       # tunnel_id，inner 连接时需匹配
        key: "dummy-not-used"             # type=tunnel 时 key 无意义
        upstream: "tunnel://inner01"      # 固定格式 tunnel://<tunnel_id>
        weight: 1
        support_models: [claude-sonnet-4-6, claude-opus-4-7]
```

字段语义：
- `type`：协议判定（§4.1），决定 SSE/token 字段/默认端点。`tunnel` 表示反向隧道池。
- `auth_scheme`：上游鉴权头注入方式（§4.5），`x-api-key`（默认，官方 Anthropic）或 `bearer`（Ark 等 Anthropic 兼容中转）。可不写，按 type 默认。
- `default_upstream`：池默认上游；key 未写 `upstream` 时用之。
- key.`upstream`：key 级覆盖，用于官方 key 与中转 key 同池。`tunnel://` 前缀表示隧道模式。
- key.`weight`：调度权重（加权策略用）。
- key.`max_concurrent`：该 key 并发上限，覆盖全局。
- key.`support_models`：模型白名单，空=不限制（§4.9 过滤）。
- key.`disabled`：临时禁用，不删配置。
- key.`tunnel_id`：type=tunnel 时，标识该 key 对应的 tunnel 连接 ID（通常等于 key.id）。

### 7.2 config.yaml
```yaml
listen: { host: 127.0.0.1, port: 4000 }
virtual_keys:
  - key: "vk-local-001"
    pools: [anthropic, openai, deepseek]    # 该 vk 可访问的池；省略/空=所有池

# endpoints 可选：默认走约定式路由(§4.2)，仅特殊绑定时覆盖
# endpoints:
#   "/v1/responses": [openai, deepseek]

strategy: cache_affinity_ttl        # 默认：窗口内粘性保缓存，超窗口负载均衡
cache_ttl_seconds: 300              # 粘性窗口，对齐上游缓存 TTL
fallback_strategy: weighted_round_robin
default_cooldown_seconds: 60
max_retries: 3
max_concurrent_per_key: 8
connect_timeout: 10
first_byte_timeout: 60
stream_idle_timeout: 120
total_timeout: 1200                 # 非流式
total_timeout_stream: 1800          # 流式
expose_rate_limits: false
hot_reload: true                    # 增删 keys.yaml/config.yaml 资源无需重启
tunnel_token: ""                    # 反向隧道认证（inner 连接时携带，空=禁止 tunnel）
state_file: ./state.json
log:
  file: ./router.log
  level: INFO
  rotate_mb: 10
  rotate_keep: 5
```

### 7.3 state.json
```json
{
  "updated_at": "ISO8601",
  "keys": {
    "anthropic::ak1": {
      "cooldown_until": 0.0, "consecutive_fails": 0,
      "request_count": 1234, "prompt_tokens": 5678901, "completion_tokens": 234567,
      "last_status": "ok", "last_error": "", "last_used": 1750200000.0,
      "disabled": false
    }
  }
}
```

---

## 8. 错误处理与状态码

### 8.1 上游响应 → 网关动作

| 上游响应 | 网关动作 | 客户端看到 |
|---|---|---|
| 2xx | 透传 body + header（剥离 §4.5 的头） | 原样 |
| 429 (`Retry-After: N`) | 该 key 冷却 N 秒 + 重试下一个 | 重试成功则透传；最终失败见 §8.2 |
| 429 (无 Retry-After) | 默认 60s 冷却 + 重试 | 同上 |
| 529 / `overloaded_error` | 30s 冷却 + 重试 | 同上 |
| 401 / 403 | 标记 disabled + 告警 + 重试下一个 | 同上 |
| 5xx (一次) | 重试 | 通常透传成功 |
| 5xx (连续 3 次同 key) | 60s 冷却 | 重试下一个 |
| 网络超时 | 重试 | 重试成功则透明；最终 504 |

### 8.2 重试耗尽后返回客户端（保持原生语义）

**原则：非重试错误一律原样透传上游 body + status**（`_passthrough_error` 函数实现），让客户端的错误处理 UX 与直连一致。

| 情形 | 状态码 | body | header |
|---|---|---|---|
| 上游 4xx 不可重试（如 400/404/451） | **原上游码** | **原上游 JSON body** | 透传 + 剥离限流头 + 剥离 content-encoding |
| 上游 4xx 重试用尽（429/401/403 轮遍全池） | 429 | `{"type":"error","error":{"type":"overloaded_error","message":"all keys exhausted"}}` | `Retry-After: <最短冷却秒>` |
| 全池冷却 | **503** | `{"error":"all keys cooling down","retry_after":N}` | `Retry-After: N` |
| 上游 5xx 重试用尽 | **502** | 原上游 body（若有） | — |
| 网络错误重试用尽 | **504** | `{"error":"upstream unreachable"}` | `Retry-After: 30` |

**关键**：503/502/504 必须带 `Retry-After`，因为 Claude Code / Codex 对 503 有"无 Retry-After 即不退避直接报错"的行为，带头才能触发优雅退避而非硬失败。

### 8.3 限流头剥离（避免误导）

上游的 `anthropic-ratelimit-*` / `x-ratelimit-*` 反映的是**被选中那一个 key** 的剩余配额，不是全池。若透传，客户端会基于单 key 头做退避决策，与网关的轮换/粘性语义冲突。处理：
- **剥离**所有上游限流头；
- 若 `expose_rate_limits: true`（默认 false），则重写为**全池聚合保守值**（取所有可用 key 的最小剩余 + 最近窗口的滚动 RPM）。

### 8.4 content-encoding 处理（实测必需）

httpx `aread()` 会自动 gunzip 响应体；若网关把 `content-encoding: gzip` 头继续透传给客户端，client（如 Python httpx 默认 `Accept-Encoding: gzip`）会**二次解压已解压字节** → `incorrect header check` 报错。

| 路径 | body 处理 | content-encoding 头 |
|---|---|---|
| 非流式（`proxy_buffered`） | `aread()` 自动解压 | **必须剥离**（否则客户端二次解压） |
| 流式（`proxy_stream`） | `aiter_raw()` 透传压缩字节 | **保留**（客户端按头解压一次） |
| 错误透传（`_passthrough_error`） | 已 `aread()`，同非流式 | 必须剥离 |

实现：`response_headers()` 函数按 `expose_rate_limits` + 一组固定列表过滤；非流式路径在过滤后再额外 `pop("content-encoding")`。

### 8.5 success 计费的边界条件

只有 `2xx` 才算 `mark_success`（计 `request_count` / `prompt_tokens` / `completion_tokens`）。`3xx`/`4xx`（非 401/403/429）虽然不重试也不冷却，但**不计为成功**，按 §8.2 透传上游 body 给客户端。这避免"上游 400 被误统计为可用调用"。


- 若 `expose_rate_limits: true`（默认 false），则重写为**全池聚合保守值**（取所有可用 key 的最小剩余 + 最近窗口的滚动 RPM）。

**关键约束**：流式响应已开始下行后不重试（见 §6.1）；此时若上游中途错误，原样把错误事件透传给客户端，由客户端自身的整体重试再打网关（换新 key）。

---

## 9. 可观测性

### 9.1 请求日志
每条请求一行结构化 JSON：
```json
{"ts":"...", "vk":"vk-local-001", "endpoint":"/anthropic/v1/messages",
 "pool":"anthropic", "key_id":"ak2", "model":"claude-sonnet-4-6",
 "status":200, "stream":true, "duration_ms":12345,
 "prompt_tokens":1234, "completion_tokens":567, "retried":0}
```

### 9.2 管理端点

**已实现（v0.3.0）**：
- ✅ `GET /healthz` → `{"ok":true,"pools":[...],"tunnels":[...]}`（公开，无鉴权；含已连接 tunnel 列表）
- ✅ `GET /` → 服务信息（公开）
- ✅ `GET /stats` → 池/key 统计 JSON（**注意**：当前公开，无鉴权；若需保护移到 `/admin/stats`）
- ✅ `WS /tunnel/connect?tunnel_id=xxx&token=yyy` → 反向隧道接入点（tunnel_token 鉴权）
- ✅ `POST /admin/reload` → 强制热加载 keys.yaml + config.yaml 可热改字段
- ✅ `GET /admin/pools` → 池配置详情（type / auth_scheme / upstream / key 列表 + runtime disabled，**不含真实 key 明文**）
- ✅ `GET /admin/config` → 当前 config.yaml + keys.yaml 原文（admin_token / 真实 key 已脱敏）
- ✅ `GET /admin/vk` / `POST /admin/vk` / `DELETE /admin/vk/{key}` → vk CRUD（写 config.yaml + 热加载，ruamel 保注释）
- ✅ `POST /admin/keys/{pool}/{key_id}/{disable|enable|reset-cooldown}` → runtime 立即生效；disable/enable 持久化 keys.yaml
- ✅ `GET /admin/logs/recent?limit=N` → 近 N 条结构化请求日志（环形缓冲，max 500）
- ✅ `GET /admin/usage?window=1h|24h|7d` → 时序聚合（总数/成功/失败 + 按 pool/vk 分组）

**Roadmap（待实现）**：
- ⏳ `GET /admin/logs/stream` → SSE 推送实时请求日志（当前 UI 用 5s 轮询 `/logs/recent` 降级）
- ⏳ `POST /admin/pools/{pool}/drain` / `undrain` → 在途排空
- ⏳ `/admin/usage` 细分到 key 维度 + token/RPM 时间桶

所有 `/admin/*` 鉴权方式：`Authorization: Bearer <admin_token>` 或 `X-Admin-Token: <admin_token>`。`admin_token` 未配置 → 全部 403（管理面禁用，最安全）。

### 9.3 CLI

**已实现（v0.3.0）**：
```
llm-router serve              # 启动服务（启动时打印服务信息横幅：端点/vk/模型/池/接入示例）
llm-router info               # 仅打印服务信息横幅（不启动），供 start.ps1 / 排查用
llm-router validate           # 校验 keys.yaml/config.yaml，不实际加载（CI 友好）
llm-router stats              # 拉 /stats 打印 JSON
llm-router reload             # 触发 POST /admin/reload（自动用 config.yaml 的 admin_token）
llm-router tunnel-client      # 启动反向隧道客户端（内网设备连接外网 relay）
  --relay-url <ws://...>      #   relay 的 WebSocket 地址
  --tunnel-id <id>            #   唯一标识，对应 relay keys.yaml 中的 key.id
  --token <secret>            #   relay 的 tunnel_token
  --forward-url <url>         #   转发到的本地地址
  --forward-vk <vk>           #   可选：替换为内网 router 的 vk
```

**Roadmap**：
```
llm-router test <pool>        # 用池里下一个 key 打一发简单请求
llm-router rotate <key_id>    # 强制让某 key 进入冷却
llm-router pools ls / drain <pool> / add <pool> --type <T> --upstream <url>
llm-router keys add <pool> --key <sk> --id <id> --weight <n> [--upstream <url>] [--support-models m1,m2]
llm-router keys rm <pool> <key_id>
llm-router vk add / rm / ls   # 在线增删 vk
```

### 9.4 告警钩子（可选）
`config.yaml` 配 `alert.webhook`：当某 key 转 disabled / 全池冷却 / 错误率突增 → POST 一条 JSON 到 webhook（可对接钉钉/飞书/Slack）。

---

## 10. 安全模型

### 10.1 vk 鉴权
- vk 是网关签发的纯随机字符串；不验签、不到期，仅做白名单校验（够用）。
- 多 vk 互不感知；可按 vk 限制可访问的池（如某 vk 只能用 deepseek）。

### 10.2 真实 key 防泄漏
- 真实 key 只存于 `keys.yaml`，文件权限建议 `icacls keys.yaml /inheritance:r /grant:r "%USERNAME%:R"`
- 日志/响应/错误信息**永不打印真实 key**；只打印 `key_id`
- 响应头中删除任何上游回传的 `x-api-key` 残留

### 10.3 监听面
- 默认 `127.0.0.1`，仅本机可达
- 如需局域网访问，必须开 `auth_required: true` 并设 admin token，避免 vk 在内网被暴力枚举

### 10.4 admin_token 与管理面

**作用面**：保护 `/admin/*`（reload / pools / 未来的 vk CRUD / 排空 / 日志推送等）。**不影响业务调用**，业务调用走 vk 鉴权（§4.4）。

**配置**（`config.yaml`）：
```yaml
admin_token: "<一段长随机字符串>"
```

**留空或不写 → `_check_admin` 直接返回 false → 全部 `/admin/*` 一律 403**（管理面禁用，最安全的开发期默认）。

**校验规则**（`app.py:_check_admin`）：
```python
def _check_admin(request) -> bool:
    if not config.admin_token:
        return False                       # 不配 = 全 403
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {config.admin_token}" \
        or request.headers.get("x-admin-token") == config.admin_token
```
两个头任选其一：`Authorization: Bearer <token>` 或 `X-Admin-Token: <token>`。

**调用示例**：
```bash
# 看池
curl -s http://127.0.0.1:4000/admin/pools \
     -H "authorization: Bearer <token>"

# 强制热加载（改了 keys.yaml 立即生效，不等 2s 轮询）
curl -s -X POST http://127.0.0.1:4000/admin/reload \
     -H "authorization: Bearer <token>"

# 等价 CLI
llm-router reload    # 自动从 config.yaml 读 admin_token
```

**生成建议**（PowerShell）：
```powershell
"admin-" + [guid]::NewGuid().ToString('N')
```

**与 vk 的区别**：
| | vk | admin_token |
|---|---|---|
| 谁用 | claude / codex / SDK 业务调用 | 你（运维） |
| 端点 | `/anthropic/*`、`/v1/*` 业务路径 | 仅 `/admin/*` |
| 配置位置 | `config.yaml` 的 `virtual_keys` 列表 | `config.yaml` 的 `admin_token` 单字段 |
| 数量 | 多个，按用途/项目分 | 单个全局 |
| 校验通过后 | 选真实 key 转发上游 | 允许查/改池配置 |
| 留空效果 | 没 vk = 401（业务不可用） | 没 token = 403（管理面禁用，业务正常） |

---

## 11. 部署与自愈

### 11.1 服务化
推荐通过 NSSM / Windows 任务计划程序常驻，开机自启。同 `agent_scope_ranger` 的 `start.ps1` 模式：循环拉起、退出 10s 重启。

### 11.2 进程模型
单进程异步（FastAPI + uvicorn workers=1）。Python asyncio 已能撑起 agent 场景的并发（个位数到几十 RPS），无需多 worker，避免跨 worker 的状态同步成本。

### 11.3 资源占用
冷启动内存约 80MB；每个并发请求增加 1-2MB（流式缓冲极小）。

### 11.4 与 agent_scope_ranger 共存
两者均常驻：

```
任务计划程序 ─► llm_api_router (4000)        长跑：HTTP 服务
            ─► agent_scope_ranger.ps1  长跑：调度 agent
                       │
                       └─► 子进程 claude/codex
                              │
                              └─► http://127.0.0.1:4000 (本地网关)
```

agent_scope_ranger 启动前确认网关健康（`curl /healthz`），不健康时短暂等待。

---

## 12. 配置项一览

| 配置 | 默认 | 说明 |
|---|---|---|
| `listen.host` / `port` | 127.0.0.1 / 4000 | 监听（**改需重启**） |
| `strategy` | **cache_affinity_ttl** | 调度策略，默认时间窗口感知粘性 |
| `cache_ttl_seconds` | 300 | `cache_affinity_ttl` 的粘性窗口，对齐上游缓存 TTL；超窗口即负载均衡 |
| `fallback_strategy` | weighted_round_robin | 粘性失效/超窗口时用的负载均衡策略 |
| `default_cooldown_seconds` | 60 | 无 Retry-After 时的冷却 |
| `max_retries` | 3 | 单请求最大重试次数 |
| `max_concurrent_per_key` | 8 | 单 key 并发上限 |
| `connect_timeout` / `first_byte_timeout` | 10 / 60s | 建连 / 首字节 |
| `stream_idle_timeout` | 120s | 流式 chunk 间隔上限 |
| `total_timeout` | 1200（非流式）/ 1800（流式） | 上游总超时 |
| `expose_rate_limits` | false | 是否向客户端暴露聚合限流头 |
| `hot_reload` | true | keys.yaml/config.yaml 可热改部分自动加载（见 §15） |
| `hot_reload_poll_seconds` | 2 | 热加载文件 mtime 轮询间隔 |
| `log.rotate_mb` / `rotate_keep` | 10 / 5 | 日志轮转 |
| `alert.webhook` | (空) | 告警 webhook URL |
| `admin_token` | (空，必填若开管理面) | /admin 鉴权（**改需重启或受保护 reload**） |
| `tunnel_token` | (空) | 反向隧道接入认证（inner 连接时必须携带此 token） |

---

## 13. 接入示例（端到端）

### 13.1 Claude Code
```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:4000/anthropic"
$env:ANTHROPIC_AUTH_TOKEN = "vk-local-001"
# 或: $env:ANTHROPIC_API_KEY = "vk-local-001"
claude
```
Claude Code 仍把所有请求发到 `/v1/messages`，网关在 `/anthropic/v1/messages` 接收（path 前缀 `/anthropic` 是为了同一网关也能挂 OpenAI 路径，互不冲突）。

### 13.2 Codex (Responses API，默认)
```toml
# ~/.codex/config.toml
[model_providers.router]
name = "router"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"
env_key = "OPENAI_API_KEY"

[profiles.router]
model_provider = "router"
model = "gpt-5"

[profiles.router.model_reasoning_effort]
effort = "medium"
```
```powershell
$env:OPENAI_API_KEY = "vk-local-001"
codex --profile router exec "在 panorama-app 跑测试并修复失败用例"
```

### 13.3 Codex (Chat Completions 模式)
```toml
[model_providers.router_chat]
base_url = "http://127.0.0.1:4000/v1"
wire_api = "chat"
env_key = "OPENAI_API_KEY"
```

### 13.4 通过 OpenAI / Anthropic SDK
```python
# Anthropic SDK
import anthropic
c = anthropic.Anthropic(api_key="vk-local-001",
                        base_url="http://127.0.0.1:4000/anthropic")

# OpenAI SDK
from openai import OpenAI
o = OpenAI(api_key="vk-local-001", base_url="http://127.0.0.1:4000/v1")
```

### 13.5 下游编排器接入约定（agent_scope_ranger / agent_generate_forger）

两个下游编排器都不直连上游，而是把 agent 子进程的 base_url 指向本网关。约定如下，二者一致：

| 项 | claude 子进程 | codex 子进程 |
|---|---|---|
| 端点前缀 | `ANTHROPIC_BASE_URL = <router>/anthropic` | `model_providers.*.base_url = <router>/v1` |
| 鉴权 | `ANTHROPIC_AUTH_TOKEN = <vk>`（清掉 `ANTHROPIC_API_KEY`） | `env_key` 指向环境变量（如 `FORGE_VK`/`OPENAI_API_KEY`）= `<vk>` |
| vk 来源 | 编排器配置（见下），spawn 前注入 | 同左 |
| 协议路由 | vk 可访问的 `type:anthropic` 池 | vk 可访问的 `type:openai_responses`/`openai_chat` 池 |

**vk 的分配由编排器决定，网关只做白名单校验**：

- **agent_scope_ranger**：每个被维护项目可配一个 vk（`projects.json` 的 `router` 块），用于该项目所有维护任务；agent 跑维护时用该 vk。vk 在网关 `config.yaml` `virtual_keys` 登记并绑定可访问的池。
- **agent_generate_forger**：每个项目红蓝两角色各配一个 vk（`forge_projects.json` 的 `forger_agent.vk` / `generator_agent.vk`），实现角色级用量分离与熔断隔离；`paths[]` 子路径可覆盖 vk。两 vk 在网关登记。

**编排器侧的隔离职责**（非网关职责）：
- claude：用独立 `CLAUDE_CONFIG_DIR`，清掉官方 `ANTHROPIC_API_KEY`，只设 `ANTHROPIC_AUTH_TOKEN=<vk>`，防偷用登录态。
- codex：用独立 `CODEX_HOME`，`-c model_provider=...` + `env_key` 强制走网关，清掉 `OPENAI_API_KEY`/`OPENAI_BASE_URL`。

**资源耗尽语义**：vk 无效（403）或其可访问池全冷却 → 网关返回 503 + `Retry-After`。编排器据此判定为资源绑定失败（非代码 bug），按各自韧性策略处理（ranger 退避+熔断；forger 抛 `ResourceBindingError` 终止该 run）。

> 端到端链路：编排器 → agent 子进程 → `http://127.0.0.1:4000`（本网关）→ 真实上游。编排器只认一个 endpoint + 一个 vk，key 池轮换/限流/故障转移对本网关上游透明。

### 13.6 实测端到端验证（火山 Ark / Anthropic 协议 + Bearer 鉴权）

v0.2.0 的实测覆盖（基于 3 个真实 Ark token，单池 `ark_anthropic` + `auth_scheme: bearer` + upstream `https://ark.cn-beijing.volces.com/api/coding`）：

| 测试项 | 结果 | 关键观察 |
|---|---|---|
| `GET /healthz` | ✅ 200 | `{"ok":true,"pools":["ark_anthropic"]}` |
| 缺 vk → 401 | ✅ | `extract_virtual_key` 返回 None |
| 错 vk → 403 | ✅ | `assert_virtual_key` 抛 PermissionError |
| `POST /anthropic/v1/messages` 非流式 | ✅ 200 + 完整 JSON + usage 计费 | `auth_scheme: bearer` 正确注入 `Authorization: Bearer ark-...`；`x-api-key` 删除；`prompt_tokens` / `completion_tokens` 入 stats |
| `POST /anthropic/v1/messages` 流式 (SSE) | ✅ Anthropic event 流原样透传 | `event:` + `data:` 行序保留；ping 透传；`aiter_raw` 使 `content-encoding` 仍生效 |
| `cache_affinity_ttl` 粘性 | ✅ 3 次同 prompt → 全打到同一 key (ark2:+3, ark1/ark3:0) | 指纹 = `hash(system+tools)` 命中即复用 |
| 多请求负载均衡 | ✅ 不同指纹分散 | wrr fallback 正常 |
| `GET /anthropic/v1/models` 聚合 | ✅ `[deepseek-v4-pro, glm-5.1, glm-5.2, kimi-k2.6]` | `support_models` 并集 |
| 热加载（加 ark4） | ✅ 改 keys.yaml 存盘 → 4s 内生效 | mtime 轮询触发 `maybe_reload_keys_locked` |
| 热加载（删 ark4） | ✅ 同上 | 无重启 |
| `/admin/pools` + token | ✅ 显示池详情，不含真实 key 明文 | `Authorization: Bearer <admin_token>` 校验 |
| `pytest -q` | ✅ 7 passed | 含 bearer auth_scheme + model 过滤 + /v1/models 聚合 |

**实测发现并已修复的 2 个真 bug**：

1. **content-encoding 二次解压**：httpx `aread()` 已自动 gunzip 响应体，但网关把 `content-encoding: gzip` 头继续透传，客户端（Python httpx 默认 `Accept-Encoding: gzip`）二次解压报 `incorrect header check`。修复：非流式路径 `pop("content-encoding")`；流式路径 `aiter_raw()` 透传压缩字节，保留头（§8.4）。
2. **non-2xx 计为 success**：早期实现里上游 400 走 `mark_success` 路径 → token 计 0 但请求计数 +1，污染统计。修复：仅 `status<400` 才 `mark_success`，否则走 `_passthrough_error` 透传（§8.5）。

**仍受上游限制的边角**（§14 已列）：低速率（>5min 间隔）粘性也救不了 cache 命中；Codex 的 `/v1/responses` 兼容厂商极少；订阅登录无法代理。

---

## 14. 与原生调用的已知偏差与缓解

> 客观结论：**做不到"完美调用、零区别"**。能做到"功能等价、日常编码无感"，但存在一组必须主动处理的偏差。本节是设计承诺的边界，而非过度宣传。

### 14.1 🔴 prompt cache 碎片化（最关键）
- **现象**：Anthropic / OpenAI 的 prompt cache 按 key（组织）维度缓存。纯轮换会让同一 system+tools 被多个 key 各缓存一次，TTL（Anthropic 5min）内来不及复用 → 命中率塌方，输入 token 计费从 ~1/10 回到全价，首 token 延迟上升。
- **缓解**：默认策略 **`cache_affinity_ttl`**（§5.1）——**时间窗口感知粘性**：
  - 缓存窗口内（默认 300s）相同内容 → 复用同一 key（吃缓存）；
  - 超出窗口（缓存已失效，粘着无意义）→ 负载均衡重选，摊薄速率配额；
  - 粘性 key 冷却 → 临时 fallback，恢复后回归。
- **为什么不是纯粘性 `cache_affinity`**：纯粘性下，慢速高频项目会把单一指纹的所有压力堆在一个 key 上直至限流，其余 key 闲置；而那些超窗口的粘性本就吃不到缓存，纯粘等于白白牺牲负载均衡。`cache_affinity_ttl` 在"有缓存收益时粘、无收益时均"之间取最优。
- **残留偏差**：低速场景（间隔 >5min）下任何策略都难命中——这是上游缓存语义本身决定的，网关无法消除，仅能不加剧。

### 14.2 🟡 限流头失真
- **现象**：上游 `anthropic-ratelimit-*` 反映单 key 配额，透传会误导客户端退避。
- **缓解**：§4.5 / §8.3 剥离或聚合重写。

### 14.3 🟡 503/502/504 必须带 Retry-After
- **现象**：Claude Code / Codex 对无 `Retry-After` 的 503 倾向直接报错而非退避。
- **缓解**：§8.2 所有重试耗尽响应带 `Retry-After`。

### 14.4 🟡 错误体原样透传
- **现象**：若网关用自定义错误体覆盖上游错误，客户端拿不到真实 cause，调试困难。
- **缓解**：§8.2 非重试错误原样透传上游 status + body。

### 14.5 🟡 只能代理 API key，不能代理订阅登录
- **现象**："原生"可以是 Claude Pro/Max 订阅（OAuth）或 ChatGPT Plus；网关只认 vk（API-key 风格），无法代理 OAuth 会话。
- **结论**：本网关面向**按量 API key 池**场景；用订阅额度不在支持范围。这是架构边界，非缺陷。

### 14.6 🟡 Codex Responses 兼容性陷阱
- **现象**：`/v1/responses` 仅 OpenAI 官方支持；DeepSeek 等"兼容"厂商只支持 `/v1/chat/completions`。
- **缓解**：用 pool `type` 明确区分（`openai_responses` vs `openai_chat`）；`llm-router validate` 校验"声明 `openai_responses` 的池其 upstream 确实支持 Responses"（厂商能力需人工在 keys.yaml 注释/标记，网关无法自动探测）。DeepSeek 类厂商应配 `type: openai_chat`。

### 14.7 🟡 OpenAI org/project 头跨 key 冲突
- **现象**：池内 key 跨 org 时，客户端 `OpenAI-Organization` 头与被选 key 不匹配 → "model not found" / 计费错乱。
- **缓解**：§4.7 网关按被选 key 所属 org 覆盖该头；约束同池 key 同 org。

### 14.8 🟢 流式中途故障
- **现象**：客户端已收到部分字节后上游断流，网关不能换 key 重发。
- **缓解**：原样透传错误事件，由客户端整体重试再打网关（换新 key）。行为接近原生"断流→重试"。

### 14.9 🟢 辅助端点
- count_tokens / /v1/models 已纳入 key 注入与聚合（§4.8）。

### 14.10 🟢 本地一跳延迟
- localhost HTTP，首 token +1ms 级，可忽略。

---

## 15. 动态扩缩容与热加载

> 你可以在**不重启服务**的情况下，增删改可对接的 OpenAI / Anthropic（及兼容厂商）资源。

### 15.1 资源清单文件
两份文件分工，**`keys.yaml` 是唯一的大模型资源清单**：

| 文件 | 内容 | 角色 |
|---|---|---|
| `keys.yaml` | 所有池、上游、真实 key、权重、并发上限 | **大模型资源清单**（你日常增删改的对象） |
| `config.yaml` | 监听、策略、超时、端点→池路由、virtual_key、日志、告警 | 服务配置 |

### 15.2 可热改 vs 需重启

| 项 | 文件 | 热改？ | 说明 |
|---|---|---|---|
| 池（新增/删除一个 anthropic/openai/deepseek 池） | keys.yaml | ✅ | 扩缩容核心 |
| 池 upstream 变更 | keys.yaml | ✅ | 切上游地址 |
| key 增/删/改权重/改并发 | keys.yaml | ✅ | 最常用 |
| key 标 `disabled` | keys.yaml | ✅ | 临时下线 |
| virtual_key 增删、可访问池调整 | config.yaml | ✅ | 多客户端管理 |
| 端点→池 路由 | config.yaml | ✅ | 新增端点绑定 |
| `strategy` / 超时 / 冷却参数 | config.yaml | ✅ | 调参 |
| `alert.webhook` / `expose_rate_limits` | config.yaml | ✅ | |
| `listen.host/port` | config.yaml | ❌ 需重启 | 改监听套接字 |
| `log.*` | config.yaml | ❌ 需重启 | 日志 handler 重建 |
| `admin_token` | config.yaml | ❌ 受保护 | 安全敏感，仅 `POST /admin/reload?token=<old>` 且 old token 校验通过才换 |
| `hot_reload*` 自身 | config.yaml | ❌ 需重启 | 元配置 |

### 15.3 触发方式（四种等价）
1. **文件监视自动触发**：watchdog（有）或 mtime 轮询（fallback，默认 2s）检测 `keys.yaml`/`config.yaml` 变化。
2. **`POST /admin/reload`**：显式触发（admin token 鉴权）。
3. **`SIGHUP`**（POSIX）/ 管理端点（Windows）：信号触发。
4. **CLI**：`llm-router reload`（等价 2）。

### 15.4 热加载流程（原子 + 校验）
```
1. 读取 keys.yaml/config.yaml（写端用 tmp + os.replace 保证半写不被读到）
2. YAML 解析 + schema 校验 + 语义校验：
   - 每个 pool 必有 type ∈ {anthropic, openai_chat, openai_responses}
   - default_upstream 缺省时按 type 兜底官方地址；key.upstream 覆盖时 URL 合法
   - key_id 在 (pool, key_id) 维度唯一
   - endpoints 覆盖表引用的 pool 都存在（若配置了 endpoints）
   - virtual_key 引用的 pool 都存在
   - weight/max_concurrent 为正整数
   - 警告（不阻断）：openai_responses 池的 upstream 若非官方域名，提示人工确认该中转商支持 Responses
3. 校验失败 → 保留旧配置，写 ERROR 日志 + 告警，HTTP reload 返回 400 + 原因
4. 校验通过 → 按 type 重建约定式默认路由表 + endpoints 覆盖 → 构建新的 Pool/Selector 快照
5. 在写锁内原子替换内存中的 selector/路由表 引用（指针 swap，纳秒级）
6. 状态继承：按 (pool, key_id) 保留旧 KeyState（cooldown/stats/disabled）；
   已删除的 key 的状态归档到 state.json 的 `retired` 段，保留 7 天；
   新增 key 的 support_models 立即纳入 model 过滤候选集
7. 写 INFO 日志：新增/移除/变更的 diff（脱敏，只列 key_id 与池名与 type）
```

### 15.5 在途请求与排空（drain）语义

| 操作 | 行为 |
|---|---|
| 新增 key/pool | 立即可用 |
| 修改 key 权重/并发/support_models | 即时生效；粘性表中对应该 key 的指纹保持粘性；support_models 变更影响后续 model 过滤候选集 |
| 移除 key | 进入 `draining`：**不接受新请求**，已派发给它的在途请求跑完；完成后从内存移除 |
| 移除整个池 | 池内所有 key 进入 drain；该 type 的端点在池排空前若无其他同 type 池（或 endpoints 覆盖未指他池）则对**新请求**返回 503 |
| `POST /admin/pools/{pool}/drain` | 显式排空，不删配置；可 `undrain` 恢复 |

drain 期间 `cache_affinity_ttl` 粘性到该 key 的指纹会**临时迁移**到 fallback key，原 key 移除后 fallback 转正（若仍在窗口内则回粘，否则按负载均衡重选）。

### 15.6 并发安全
- selector 引用用原子 swap；读路径无锁。
- KeyState 读写用 per-key 细粒度锁。
- reload 全程不阻塞在途请求（最多纳秒级 swap 窗口）。

### 15.7 可观测
- `GET /admin/pools` 实时反映当前生效配置（含 draining 标记）。
- 每次 reload 写一条结构化日志：`{action:reload, added:[...], removed:[...], changed:[...], duration_ms}`。
- `llm-router validate` 离线校验，不触发加载，CI 友好。

### 15.8 典型扩缩容操作

**扩容（加一个新厂商池）**：编辑 keys.yaml 加一段 → 存盘 → 2s 内自动生效，无需重启。
```yaml
pools:
  moonshot:
    upstream: https://api.moonshot.cn
    keys:
      - { id: mk1, key: "sk-xxx" }
```
再在 config.yaml 的 `endpoints` 把 `/v1/chat/completions` 也允许路由到 moonshot（端点可绑多池，按策略选）。

**缩容（下线某 key）**：`llm-router keys rm anthropic ak2` → 排空 → 移除，配置写回 keys.yaml。

**临时限流某个 key**：`llm-router rotate ak2`（进冷却）或 keys.yaml 标 `disabled: true` 存盘。

---

## 16. 前端管理界面（Admin UI）

> 本章定义一个**可选的、与后端同进程托管**的浏览器管理界面，覆盖运维日常需要的"看 / 改 / 测"三类操作，避免对着 `config.yaml` 手改和 `curl /admin/*` 拼命令。

### 16.1 设计目标
- **零额外服务**：UI 静态资源由 router 自身的 FastAPI 进程通过 `StaticFiles` 在 `/ui/*` 路由托管，单端口部署，部署即可访问。
- **本机优先**：默认 `127.0.0.1`，UI 与后端同源，免 CORS。开放局域网时强制 admin_token。
- **薄客户端**：UI 不持久化任何业务数据，所有状态读自 `/admin/*` API；token 仅放在浏览器 sessionStorage，关闭即丢。
- **配置同源**：UI 的"改"操作必须最终落到 `config.yaml` / `keys.yaml`，避免内存改动重启即丢；写盘后由热加载链路（§15.4）原子 swap 生效。
- **不重写已有的稳定路径**：业务请求路径（`/anthropic/*` / `/v1/*`）与 UI 路径（`/ui/*` + `/admin/*`）正交，UI 故障不影响业务。

### 16.2 非目标
- 不做多用户系统（只有一个 admin_token）。
- 不做 prompt 调试器 / 请求重放器（这是 LangSmith / Helicone 类工具的领域）。
- 不做计费/账单（`/admin/usage` 仅给原始数字，可视化在 UI 端）。
- 不做集群视图（单机网关）。

### 16.3 技术选型

> **实现现状（v0.2.0）**：v1 采用**无构建单文件 UI**（`llm_api_router/static/ui/index.html`，纯原生 JS + 内联 CSS，零 toolchain），而非下表的 Vue+Vite 方案。原因：本地单机管理面，无构建链 = 无 `npm install`/build 脆弱性、即开即用、后端直接 `StaticFiles` 托管、可独立验证。下表的 Vue+Vite 方案保留为**管理面变复杂时（多视图组件化、复杂表单、图表交互多）的升级路径**。

**v1 实际实现**：单个 `index.html`，5 个 tab（Dashboard/Pools/VKs/Logs/Config），`fetch()` 调 `/admin/*`（注入 `X-Admin-Token`），sessionStorage 存 token，看板/日志 5s 轮询刷新。约 300 行，无依赖、无打包。

**升级路径（Vue+Vite，可选）**：

| 维度 | 选择 | 理由 |
|---|---|---|
| 框架 | **Vue 3 + `<script setup>`** | 单文件组件、模板直观、与项目栈对齐 |
| 构建 | **Vite 5** | 产物输出到 `static/ui/` |
| UI 库 | **Element Plus** 或 **Naive UI** | 表格/表单/通知齐全 |
| 图表 | **ECharts**（按需引入） | RPM/TPM 时序、池占比 |
| 路由 | **Vue Router**（hash 模式） | 避免后端 SPA 兜底配置 |
| 状态 | **Pinia**（单 store） | 缓存 `pools` / `vk` / `stats` / `logs` |
| 包管理 | **pnpm**（或 npm） | `web/` 子包独立 |

**升级后目录结构**：
```
llm_api_router/
├─ llm_api_router/
│  └─ static/ui/                    # 构建产物，由 StaticFiles 挂载到 /ui/*（v1 是手写 index.html）
└─ web/                             # 前端源码（升级后的独立 Vite 项目）
   ├─ package.json
   ├─ vite.config.ts                # build.outDir = "../llm_api_router/static/ui"
   ├─ src/
   │  ├─ main.ts / App.vue / api.ts
   │  ├─ stores/                    # pinia
   │  ├─ views/
   │  │  ├─ Dashboard.vue / Pools.vue / VirtualKeys.vue / Logs.vue / Config.vue
   │  └─ components/
   └─ tsconfig.json
```

**构建**：`pnpm -C web build` → 产物写入 `llm_api_router/static/ui/`（覆盖 v1 手写文件）。Python 包发布时 `package_data` 携带 `static/ui/**`（已配，见 pyproject.toml）。

### 16.4 核心视图（5 个 Tab）

#### 16.4.1 Dashboard（首页）
**用途**：30 秒内回答"网关现在健康吗？瓶颈在哪？"

**面板**：
- 顶部 4 卡片：总池数 / 健康 key 数 / 1h 内请求数 / 1h 内成功率（数字 + 同比小箭头）
- 中部柱状图：每个 pool 的"成功 / 限流 / 错误"三色堆叠（最近 1h）
- 右上角：当前 strategy + cache_ttl_seconds + 是否启用热加载（小标签）
- 列表：**正在冷却的 key**（`cooldown_until > now`），含 `pool::id` / 剩余秒 / 触发原因 / 一键"重置冷却"按钮

**数据源**：`/stats` + `/admin/usage?window=1h`（roadmap）；轮询 5s。

#### 16.4.2 Pools（池/key 详情）
**用途**：粒度到单个 key 的"看 + 改"。

**布局**：左侧池列表（树形：pool → keys），右侧选中后展示详情。

**池详情**：type、auth_scheme、default_upstream、可访问的 vk 列表。
**key 详情表**（每行一个 key）：

| 列 | 说明 |
|---|---|
| id | 比如 `ark2` |
| upstream | 若 key 级覆盖则显示；否则空（继承 default） |
| weight / max_concurrent / support_models | 调度参数 |
| 状态 | `ok` / `cooling(N秒)` / `disabled` |
| 用量 | request_count / prompt_tokens / completion_tokens（千分位） |
| 最近错误 | last_error 截断 100 字 |
| 操作 | 启用/禁用 / 重置冷却 / 删除（确认弹窗） |

**写操作落点**：调 `POST /admin/keys/{pool}/{id}/disable|enable|reset_cooldown`（roadmap 实现），后端最终改 `keys.yaml` + 触发热加载，UI 刷新。

#### 16.4.3 VirtualKeys（vk 管理）
**用途**：替代手改 `config.yaml virtual_keys` 列表。

**列表**：每行一个 vk，列：`key` / 可访问池（chips） / 创建时间（roadmap 加 metadata） / 1h 用量 / 操作。

**新增表单**：
- `key` 输入框 + 一键"生成随机"按钮（形如 `vk-{生成时戳}-{16字节hex}`）
- `pools` 多选（从已有池中选；空 = 所有）
- 提交 → `POST /admin/vk` → 后端写 `config.yaml` → 热加载

**编辑/删除**：与列表同。

**注意**：UI 修改 `config.yaml` 时**必须保留所有非 vk 字段不动**，且 yaml 注释会丢失（PyYAML 默认行为）。可选改进：用 `ruamel.yaml` 保留注释。

#### 16.4.4 Logs（实时日志）
**用途**：观察请求是否真的命中粘性 / 哪条调用撞限。

**主区**：滚动列表，每行一条结构化请求日志（DESIGN §9.1 schema）。
- 行内字段：时间、vk、endpoint、pool、key_id、model、status、stream、duration_ms、retried
- 颜色：2xx 灰、4xx 黄、5xx 红、重试 N 次蓝色 chip
- 顶部过滤器：vk / pool / status 区间 / 关键词

**实现**：通过 `/admin/logs/stream`（roadmap，SSE）建立长连接，后端把 `mark_success` / `mark_failure` 时的事件以 `event: log` 推流；前端追加到环形缓冲（最多 500 条）。

**降级**：SSE 不可用时退化到 `/admin/logs/recent?limit=200` 5s 轮询。

#### 16.4.5 Config（生效配置只读）
**用途**：定位"当前实际跑的是哪份配置"，特别是热加载后。

**面板**：
- 左侧：`config.yaml` 视图（YAML 高亮，admin_token 自动遮蔽为 `***`）
- 右侧：`keys.yaml` 视图（**真实 key 自动遮蔽为 `ark-...****-xxxx`**，仅显示前后 4 字符；UI 永远不展示明文 key）
- 顶部按钮：「强制热加载」→ `POST /admin/reload` → 全屏 toast 显示 added/removed/changed 摘要

数据源：`/admin/config`（roadmap，返回脱敏后的两个 yaml 字符串 + 文件最近 mtime）。

### 16.5 后端需要新增的 API（roadmap）

为支撑 UI，后端需补齐这些端点（已在 §9.2 标 ⏳）：

| 端点 | 方法 | 用途 | UI 视图 |
|---|---|---|---|
| `/admin/config` | GET | 返回脱敏后的 config.yaml + keys.yaml 文本 | Config |
| `/admin/vk` | GET | 列出全部 vk + 元数据 | VirtualKeys |
| `/admin/vk` | POST | 新增 vk（写盘 + 热加载） | VirtualKeys 表单 |
| `/admin/vk/{key}` | DELETE | 删除 vk（写盘 + 热加载） | VirtualKeys 操作列 |
| `/admin/keys/{pool}/{id}/disable` | POST | 禁用某 key | Pools 操作列 |
| `/admin/keys/{pool}/{id}/enable` | POST | 启用某 key | 同上 |
| `/admin/keys/{pool}/{id}/reset-cooldown` | POST | 清零冷却 | 同上 + Dashboard 冷却列表 |
| `/admin/logs/recent?limit=N` | GET | 最近 N 条请求日志（落盘环形缓冲） | Logs 降级 |
| `/admin/logs/stream` | GET | SSE 推流实时日志 | Logs 主路径 |
| `/admin/usage?window=1h\|24h\|7d` | GET | 时序统计（请求数/RPM/TPM 按 vk/pool/key 分组） | Dashboard / Logs 顶栏 |

写操作的落点**必须经 yaml 文件**（不在内存改）：
1. 加载现有 yaml → 解析为 dict
2. 应用变更 → 序列化回写（`tmp + os.replace` 原子写）
3. 触发现有热加载链路（mtime 变化或显式 `state.maybe_reload_keys_locked()`）

这样保证：UI 改的、CLI 改的、用户手改 yaml 的，**最终态完全等价**，没有"内存有但盘上没有"的脏状态。

### 16.6 鉴权与会话

- UI 加载时弹出"输入 admin_token"的 modal，校验通过（请求 `GET /admin/pools` 试一发）后存 `sessionStorage`，所有后续请求自动带 `X-Admin-Token`。
- 401/403 触发：清 sessionStorage → 弹回登录 modal。
- **不持久化到 localStorage**，避免浏览器关闭后再开仍带 token。
- 局域网部署时强烈建议同时配 TLS（uvicorn `--ssl-keyfile/--ssl-certfile`），否则 token 明文过网。

### 16.7 部署模式

#### 16.7.1 同进程托管（默认，推荐）
`app.py` 启动时挂载：
```python
from fastapi.staticfiles import StaticFiles
from pathlib import Path
ui_dist = Path(__file__).parent / "static" / "ui"
if ui_dist.exists():
    app.mount("/ui", StaticFiles(directory=ui_dist, html=True), name="ui")
```
访问：`http://127.0.0.1:4000/ui/`。

#### 16.7.2 独立 dev 服务器（开发期）
`pnpm -C web dev` 起 vite dev server（默认 5173），配 `vite.config.ts` 的 proxy：
```js
server: {
  proxy: {
    "/admin": "http://127.0.0.1:4000",
    "/stats": "http://127.0.0.1:4000",
    "/healthz": "http://127.0.0.1:4000",
  }
}
```
访问 `http://localhost:5173/`，热更新+后端代理同存。

#### 16.7.3 关闭 UI（生产收紧）
不打包 `static/ui/` 目录即可——`StaticFiles` 不挂载，`/ui/*` 返回 404，`/admin/*` 仍可被 CLI/curl 调用。适合"只跑业务、不暴露管理面"的场景。

### 16.8 与现有实现的增量

新增（**doc-only，未实现**）：
- `web/`：前端工程
- `llm_api_router/static/ui/`：前端构建产物
- `app.py`：`StaticFiles("/ui")` 挂载 + §16.5 列出的新 admin 端点
- `config.py`：vk schema 扩展（可选 `created_at` / `note` 元数据）
- 新增依赖：仅打包阶段需 Node/pnpm；运行时无新 Python 依赖（FastAPI 已含 `StaticFiles`）

不破坏：
- 业务路径 `/anthropic/*` `/v1/*` 不变
- 已有 `/healthz` `/stats` `/admin/reload` `/admin/pools` 不变
- 已有 CLI 与 keys.yaml/config.yaml schema 不变

### 16.9 实施分阶段

| 阶段 | 工作量 | 交付 | 阻断风险 |
|---|---|---|---|
| **P1: 静态托管 + Dashboard** | 1d | `/ui/` 可访问，看到当前 stats 与 cooldown 列表，能登录 | 无；纯只读 |
| **P2: Pools 页 + key 启停/重置冷却** | 1d | 后端补 `/admin/keys/.../disable\|enable\|reset-cooldown`；UI 表格 + 操作列 | yaml 写盘需要 `ruamel.yaml` 保注释 |
| **P3: VirtualKeys CRUD** | 1d | 后端 `/admin/vk` 三个端点；UI 列表+表单 | 无 |
| **P4: Config 只读视图 + 强制 reload** | 0.5d | 后端 `/admin/config` 脱敏返回；UI YAML 高亮 + reload 按钮 | 脱敏正则要覆盖 `ark-*`/`sk-*`/`Bearer` 等 |
| **P5: Logs（环形缓冲 + SSE 推流）** | 1.5d | 后端落 ring buffer；UI SSE 列表+过滤+降级轮询 | SSE 在反代后可能被缓冲，需 nginx `proxy_buffering off` |
| **P6: 时序统计 + 图表** | 1d | 后端 `/admin/usage` 按时间桶聚合；UI ECharts | 内存级桶足够本地；持久化进 SQLite 是更远 roadmap |

合计 ~6 人天，按 P1→P6 顺序，每阶段独立可发布。**P1 已能让"只想看一眼"的用户用上**。

---

## 17. 已知限制与未来扩展

- **协议不互转**：vk 调 `/anthropic/*` 不能落到 openai 池。如需 Codex 走 Anthropic，需协议翻译层（未来扩展）。
- **流式中途不可重试**（见 §6.1）。
- **单机部署**：高可用需引入第二节点 + 共享 state（Redis）。当前规模无需。
- **无 token 预算硬限**：仅统计；如需"超限即停"，可加 `budget_per_key` 配置。
- **无审计回放**：仅记 metadata，不存 prompt/response 全文（隐私+磁盘考虑）。需要时可加 `record_full_payload: true` 开关。
- **不内置缓存**：上游已有 prompt caching；网关不重复缓存以保证语义正确。
- **隧道模式延迟**：tunnel 路径多一跳 WS 转发，延迟略高于直连（约 +2-5ms per hop），但对 LLM 请求（秒级响应）可忽略。

---

## 18. 实施清单与完成状态

### 18.1 实际代码结构（v0.3.0，已实现）

```
llm_api_router/
├─ pyproject.toml              # 包名 llm-api-router；脚本 llm-router=llm_api_router.cli:main
├─ setup.py
├─ README.md
├─ DESIGN.md                   ← 本文
├─ config.example.yaml / keys.example.yaml
├─ config.yaml / keys.yaml     # 真实配置（.gitignore 屏蔽）
├─ config_relay.yaml / keys_relay.yaml   # 隧道 relay 侧示例配置
├─ config_inner.yaml / keys_inner.yaml   # 隧道 inner 侧示例配置
├─ .gitignore
├─ llm_api_router/             # Python 包
│  ├─ __init__.py
│  ├─ config.py                # ✅ 数据类 + schema 校验 + 约定式路由 + tunnel type 支持
│  ├─ router.py                # ✅ RouterState + Selector + model 过滤 + fingerprint
│  ├─ app.py                   # ✅ FastAPI app + 业务端点 + tunnel proxy 分支 + /tunnel/connect WS
│  ├─ tunnel.py                # ✅ TunnelManager (relay) + TunnelClient (inner) + 帧协议
│  ├─ cli.py                   # ✅ serve / validate / stats / reload / tunnel-client
│  └─ static/ui/               # ✅ Admin UI
└─ tests/test_router.py        # ✅ 12 passed
```

### 18.2 完成度对照

| 能力 | 状态 | 位置 |
|---|---|---|
| 三协议端点（anthropic/chat/responses） | ✅ | app.py `proxy` |
| pool.type + auth_scheme（bearer/x-api-key） | ✅ | config.py / app.py `copy_headers` |
| 约定式默认路由 + endpoints 覆盖 | ✅ | config.py `build_path_map` |
| key 级 upstream 覆盖 + default_upstream | ✅ | config.py `PoolConfig.upstream_for` |
| support_models 过滤 | ✅ | router.py `_pick_key_locked` |
| cache_affinity_ttl（窗口内粘性，超窗口 wrr） | ✅ | router.py + `compute_fingerprint` |
| wrr / rr / lru / least_active | ✅ | router.py |
| 冷却（429/529/5xx）+ disabled（401/403） | ✅ | router.py `mark_failure` |
| 非流式重试 + 流式首字节前重试 | ✅ | app.py `proxy_buffered` / `proxy_stream` |
| 限流头剥离 + content-encoding 处理 | ✅ | app.py `response_headers` / §8.4 |
| 4xx 原样透传（_passthrough_error） | ✅ | app.py |
| 超时分级（connect/first_byte/idle/total） | ✅ | app.py `_httpx_timeout` |
| /v1/models 本地聚合 | ✅ | app.py `_aggregate_models` |
| 热加载（keys.yaml + config.yaml mtime 轮询） | ✅ | app.py `_reload_watcher` |
| state.json 周期 flush | ✅ | config.py `StateFlusher` |
| /admin/reload + /admin/pools | ✅ | app.py |
| /admin/config（脱敏）+ vk CRUD + key 启停/重置冷却 | ✅ | app.py（v0.2.1） |
| /admin/logs/recent + /admin/usage（环形缓冲聚合） | ✅ | app.py + router.py `request_log` |
| comment-preserving yaml 写入（ruamel） | ✅ | config.py `edit_yaml` / `sanitize_text` |
| 前端 Admin UI（无构建单文件，5 tab） | ✅ | static/ui/index.html + StaticFiles 挂 /ui |
| 一键启动脚本（前后端） | ✅ | start.ps1 / start.bat |
| CLI serve/validate/stats/reload/tunnel-client | ✅ | cli.py |
| **反向隧道 pool type=tunnel** | ✅ | tunnel.py + config.py + app.py |
| **TunnelManager (relay 侧 WS 管理)** | ✅ | tunnel.py `TunnelManager` |
| **TunnelClient (inner 侧 WS 客户端)** | ✅ | tunnel.py `TunnelClient` |
| **隧道非流式转发** | ✅ | tunnel.py `send_request` + app.py tunnel 分支 |
| **隧道流式转发（binary chunk 帧）** | ✅ | tunnel.py `_handle_stream_request` |
| **隧道心跳（app-level ping/pong）** | ✅ | tunnel.py `handle_ws` / `_connect_and_serve` |
| **隧道断线重连（指数退避）** | ✅ | tunnel.py `TunnelClient.run()` |
| **tunnel-client CLI 子命令** | ✅ | cli.py `tunnel-client` |
| pytest 覆盖（auth/failover/bearer/model 过滤/聚合/admin CRUD/日志/脱敏） | ✅ 12 项 | tests/ |
| /admin/logs/stream（SSE 实时推流） | ⏳ | §9.2（当前 UI 用 5s 轮询降级） |
| drain 排空语义 | ⏳ | §15.5（移除 key 时立即生效，未做在途排空） |
| OpenAI org 头覆盖 | ⏳ | §4.7（当前透传，未按 key 覆盖） |
| expose_rate_limits 聚合重写 | ⏳ | §8.3（当前仅"剥离"） |

### 18.3 下一步建议顺序
v0.3.0 已落地反向隧道 + 全部后端端点 + 无构建单文件 UI + 一键启动脚本。剩余：
1. **/admin/logs/stream（SSE）**（§9.2）—— UI Logs 页从 5s 轮询升级为实时推流。
2. **drain 排空语义**（§15.5）—— 移除 key 时优雅排空在途请求。
3. **OpenAI org 头覆盖**（§4.7）—— 跨 org 池的正确性。
4. **隧道多 inner 负载均衡**（§19.8）—— 同一 tunnel_id 多个 inner 节点连接时的请求分发。
5. **§16 升级到 Vue+Vite**（§16.3）—— 仅当管理面视图/交互显著变复杂时才需要。
6. **CLI 在线增删**（`keys add/rm`、`rotate`、`vk add/rm`）—— 当前走 UI 或编辑 yaml，CLI 是便利补充。

---

## 19. 反向隧道（Tunnel Pool）

> 使防火墙内的设备能以"被动注册"的方式成为外网 relay 的资源池，无需开放入站端口。

### 19.1 设计动机

- 部分内网设备持有真实 LLM key（如企业网关 key、内部 API），但防火墙不允许外部连入。
- 外网 relay 可被 Claude Code / Codex 等客户端访问。
- 需要一种机制让内网设备**主动连出**到 relay，注册为 relay 的一个"虚拟上游"。

### 19.2 架构

```
外部客户端
   │ HTTP/SSE (请求到 relay 的 vk-xxx)
   ▼
relay (llm_api_router, type=tunnel pool)
   │ 持久 WebSocket (/tunnel/connect?tunnel_id=inner01&token=xxx)
   ▼
inner (llm_api_router 或其他 HTTP 服务)
   │ HTTP (本地 forward)
   ▼
真实 LLM 上游
```

### 19.3 帧协议

所有帧为 JSON text message，流式 chunk 用 binary message（高效传输，避免 base64 开销）。

#### relay → inner
```json
// 请求帧
{"type":"request","req_id":"<hex-uuid>","method":"POST","path":"/anthropic/v1/messages",
 "headers":{"content-type":"application/json",...},"body_b64":"<base64>","stream":true}

// 取消帧
{"type":"cancel","req_id":"<hex-uuid>"}

// 心跳
{"type":"ping"}
```

#### inner → relay
```json
// 响应头帧
{"type":"response","req_id":"<hex-uuid>","status":200,"headers":{"content-type":"text/event-stream",...}}

// 非流式响应体帧
{"type":"body","req_id":"<hex-uuid>","body_b64":"<base64>"}

// 流式 chunk（binary message，高效）
[4字节 req_id_tag (uint32 BE)][chunk_bytes]

// 流结束帧
{"type":"done","req_id":"<hex-uuid>"}

// 错误帧
{"type":"error","req_id":"<hex-uuid>","message":"...","status":502}

// 心跳回复
{"type":"pong"}
```

#### req_id_tag 计算
```python
tag = int(req_id[:8], 16) & 0xFFFFFFFF  # req_id hex 的前 8 位截断为 uint32
```

### 19.4 连接管理

| 参数 | 值 | 说明 |
|---|---|---|
| `PING_INTERVAL` | 20s | inner 侧 websockets 库的 WS-level ping 间隔 |
| `PING_TIMEOUT` | 45s | relay 侧无消息超时后发送 app-level ping |
| `REQUEST_TIMEOUT` | 1200s | 单请求超时 |
| 重连退避 | 1s → 2s → 4s → ... → 60s | 指数退避，cap 60s |
| 认证 | `?token=<tunnel_token>` query 参数 | relay config.yaml 配置 `tunnel_token` |

**心跳流程**：
1. relay 的 WS 读循环设 45s 超时（`asyncio.wait_for`）
2. 超时 → relay 发 `{"type":"ping"}` text frame
3. inner 收到 → 回复 `{"type":"pong"}`
4. relay 收到 pong → `conn.touch()` 刷新活跃时间
5. 连续两次无 pong → relay 关闭连接，清理 pending requests

### 19.5 并发模型

- 同一 WebSocket 连接支持多个并发 in-flight 请求
- 每个请求用 `req_id`（UUID hex）唯一标识
- relay 侧 `_PendingRequest` 持有 `asyncio.Event`（等待响应头）和 `asyncio.Queue`（收集 stream chunks）
- inner 侧用 `asyncio.create_task` 为每个 request 帧创建独立 handler task

### 19.6 流式 vs 非流式处理差异

| 路径 | relay 行为 | inner 行为 |
|---|---|---|
| 非流式 | 等待 `body` 帧 → 返回完整 `(status, headers, bytes)` | httpx `request()` → 发 `response` + `body` 帧 |
| 流式 | `response` 帧到达即 unblock → 返回 `(status, headers, AsyncIterator)` | httpx `client.stream()` → 发 `response` 帧 + binary chunks + `done` 帧 |

**关键**：非流式时，`response_event` 在收到 `body` 帧时 set（而非 `response` 帧），确保 body 已就位再返回。

### 19.7 配置方式

**config.py 修改**：
- `VALID_TYPES` 新增 `"tunnel"`
- `PATHS_BY_TYPE["tunnel"]` 覆盖全部端点（anthropic + openai），因为 inner 的实际 type 未知
- `TYPE_DEFAULT_AUTH["tunnel"]` = `"x-api-key"`
- `RealKey` 新增 `tunnel_id: str = ""`
- `RouterConfig` 新增 `tunnel_token: str | None = None`
- `load_pools()` 对 `tunnel://` upstream 跳过 URL 校验，提取 tunnel_id

**keys.yaml（relay 侧）**：
```yaml
pools:
  inner_tunnel:
    type: tunnel
    auth_scheme: x-api-key
    keys:
      - id: inner01              # 必须与 inner 的 --tunnel-id 一致
        key: "dummy-not-used"    # type=tunnel 时无意义
        upstream: "tunnel://inner01"
        weight: 1
        support_models: [claude-sonnet-4-6, claude-opus-4-7]
```

**app.py 集成**：
- `create_app()` 初始化 `TunnelManager(token=config.tunnel_token)`
- 注册 `@app.websocket("/tunnel/connect")` 路由
- `proxy_buffered` / `proxy_stream` 检测 `pool.type == "tunnel"` 走隧道分支
- `/healthz` 返回 `"tunnels": [connected_tunnel_ids]`

### 19.8 已知限制与 Roadmap

- **单连接**：当前同一 `tunnel_id` 只保留最新一个 WS 连接（后连覆盖先连）。Roadmap：支持多 inner 节点注册同一 tunnel_id，relay 侧做 round-robin 分发。
- **无加密**：WS 通道本身不加密。生产环境应使用 `wss://`（relay 配 TLS）或 VPN 通道。
- **无流量限制**：inner 节点可无限制收发请求。Roadmap：relay 侧可配置 per-tunnel 并发上限。
- **无 tunnel 健康检查**：当前只有心跳检测连接存活，无主动探测 inner 后端是否健康。

### 19.9 CLI 用法

```
llm-router tunnel-client \
  --relay-url ws://relay:4000/tunnel/connect \
  --tunnel-id inner01 \
  --token "shared-secret" \
  --forward-url http://127.0.0.1:4001 \
  --forward-vk vk-inner-local-only
```

| 参数 | 说明 |
|---|---|
| `--relay-url` | relay 的 WebSocket 地址 |
| `--tunnel-id` | 唯一标识，需与 relay keys.yaml 中的 key.id 对应 |
| `--token` | relay config.yaml 的 `tunnel_token` |
| `--forward-url` | 请求转发到的本地地址 |
| `--forward-vk` | 转发到内网 router 时替换为此 vk（认证内网 router） |

