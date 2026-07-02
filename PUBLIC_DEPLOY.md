# LLM API Router · 公网部署信息

> 公网域名：`https://api.snamibo.com`
> 部署方式：Cloudflare Tunnel（`cloudflared`）
> 隧道 ID：`ae53442a-d54d-4580-8719-787ff998d861`
> 更新日期：2026-06-24

---

## 公网路径

| Path | 用途 | 鉴权 |
|---|---|---|
| `https://api.snamibo.com/anthropic/v1/messages` | Claude Code / Anthropic SDK 调用 | vk |
| `https://api.snamibo.com/anthropic/v1/messages/count_tokens` | Claude Code 上下文管理 | vk |
| `https://api.snamibo.com/anthropic/v1/models` | 模型列表 | vk |
| `https://api.snamibo.com/healthz` | 健康检查 | 公开 |
| `https://api.snamibo.com/stats` | Key 池统计 | 公开 |
| `https://api.snamibo.com/ui/` | 管理面 UI（浏览器打开） | admin_token |
| `https://api.snamibo.com/admin/pools` | 池/Key 详情 | admin_token |
| `https://api.snamibo.com/admin/reload` | 强制热加载 | admin_token |

---

## 接入方式

### Claude Code

```powershell
$env:ANTHROPIC_BASE_URL = "https://api.snamibo.com/anthropic"
$env:ANTHROPIC_AUTH_TOKEN = "vk-tp5Jya1szPFiYDhR7HdlqcWU9eSNrBnGuQf43LKCwmkxojEOIg"
claude
```

### Codex

```toml
# ~/.codex/config.toml
[model_providers.router]
name = "router"
base_url = "https://api.snamibo.com/v1"
wire_api = "responses"
env_key = "OPENAI_API_KEY"
```
```powershell
$env:OPENAI_API_KEY = "vk-tp5Jya1szPFiYDhR7HdlqcWU9eSNrBnGuQf43LKCwmkxojEOIg"
codex --profile router
```

### agent_scope_ranger / agent_generate_forger

已更新：

- `I:\spirite_project\agent_scope_ranger\projects.json`
- `I:\spirite_project\agent_generate_forger\forge_projects.json`

router_url 已改为 `https://api.snamibo.com`，vk 已改为 `vk-tp5Jya1szPFiYDhR7HdlqcWU9eSNrBnGuQf43LKCwmkxojEOIg`。

### Anthropic SDK

```python
import anthropic
c = anthropic.Anthropic(
    api_key="vk-tp5Jya1szPFiYDhR7HdlqcWU9eSNrBnGuQf43LKCwmkxojEOIg",
    base_url="https://api.snamibo.com/anthropic"
)
```

---

## 管理面

### UI

浏览器打开：

```text
https://api.snamibo.com/ui/
```

登录需要 `admin_token`（见 `config.yaml`）。

### CLI

```powershell
llm-router stats
llm-router reload
llm-router validate
```

### curl

```powershell
curl https://api.snamibo.com/admin/pools -H "x-admin-token: <admin_token>"
curl -X POST https://api.snamibo.com/admin/reload -H "x-admin-token: <admin_token>"
```

---

## 隧道管理

### 配置

```text
C:\Users\whs\.cloudflared\config.yml
```

```yaml
tunnel: ae53442a-d54d-4580-8719-787ff998d861
credentials-file: C:\Users\whs\.cloudflared\ae53442a-d54d-4580-8719-787ff998d861.json
ingress:
  - hostname: api.snamibo.com
    service: http://localhost:4000
  - service: http_status:404
```

### 常用命令

```powershell
# 启动隧道（前台）
C:\Users\whs\tools\cloudflared.exe tunnel run llm-router

# 查看隧道列表
C:\Users\whs\tools\cloudflared.exe tunnel list

# 删除隧道
C:\Users\whs\tools\cloudflared.exe tunnel delete llm-router
```

---

## 安全

| 项目 | 值 |
|---|---|
| vk | 50 位强随机串（见 config.yaml） |
| admin_token | 50 位强随机串（见 config.yaml） |
| keys.yaml | 建议 ACL 仅本人可读 |
| TLS | Cloudflare 自动签发 |

---

## 与主站的关系

```text
snamibo.com          → 主站（Cloudflare Pages，colorful-toolhub）
api.snamibo.com      → llm_api_router（Cloudflare Tunnel）
```

两者独立，互不影响。

---

## 架构

```text
外部用户
  │
  ├─ https://snamibo.com/*        → Cloudflare Pages (主站)
  │
  └─ https://api.snamibo.com/*    → Cloudflare Tunnel
                                      │
                                      ▼
                                 cloudflared (本机)
                                      │  localhost:4000
                                      ▼
                                 llm_api_router
                                      │
                                      ▼
                                 Ark / Anthropic / OpenAI
```
