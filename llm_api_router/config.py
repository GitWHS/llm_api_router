from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

VALID_TYPES = {"anthropic", "openai_chat", "openai_responses", "tunnel"}
VALID_AUTH = {"x-api-key", "bearer"}
VALID_STRATEGIES = {"cache_affinity_ttl", "cache_affinity", "weighted_round_robin", "round_robin", "lru", "least_active", "priority"}

# 约定式默认路由：pool.type → 自动挂载的端点（DESIGN §4.2）
PATHS_BY_TYPE: dict[str, list[str]] = {
    "anthropic": ["/anthropic/v1/messages", "/anthropic/v1/messages/count_tokens", "/anthropic/v1/models"],
    "openai_chat": ["/v1/chat/completions", "/v1/models"],
    "openai_responses": ["/v1/responses", "/v1/models"],
    # tunnel pool: relay 侧全路由透传，inner 决定实际执行路径
    "tunnel": [
        "/anthropic/v1/messages", "/anthropic/v1/messages/count_tokens", "/anthropic/v1/models",
        "/v1/chat/completions", "/v1/responses", "/v1/models",
    ],
}

TYPE_DEFAULT_AUTH = {
    "anthropic": "x-api-key",
    "openai_chat": "bearer",
    "openai_responses": "bearer",
    "tunnel": "x-api-key",
}

# 本地聚合、不转发的端点
LOCAL_ENDPOINTS = {"/v1/models", "/anthropic/v1/models"}


@dataclass
class RealKey:
    id: str
    key: str
    upstream: str                       # 必填，无默认（每个 key 显式指定上游）
    weight: int = 1
    priority: int = 10                  # 值越大优先级越高，用于 priority 策略
    max_concurrent: int | None = None   # None = 并发无上限（默认）
    support_models: list[str] = field(default_factory=list)
    disabled: bool = False
    tunnel_id: str = ""                 # type=tunnel 时，指向 TunnelManager 中的 tunnel_id


@dataclass
class PoolConfig:
    name: str
    type: str
    auth_scheme: str
    keys: list[RealKey] = field(default_factory=list)


@dataclass
class KeyRuntime:
    cooldown_until: float = 0.0
    consecutive_fails: int = 0
    request_count: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    last_status: str = "never"
    last_error: str = ""
    last_used: float = 0.0
    active: int = 0
    disabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "cooldown_until": self.cooldown_until,
            "consecutive_fails": self.consecutive_fails,
            "request_count": self.request_count,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.prompt_tokens + self.completion_tokens,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "last_used": self.last_used,
            "active": self.active,
            "disabled": self.disabled,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KeyRuntime":
        return cls(
            cooldown_until=float(data.get("cooldown_until", 0)),
            consecutive_fails=int(data.get("consecutive_fails", 0)),
            request_count=int(data.get("request_count", 0)),
            prompt_tokens=int(data.get("prompt_tokens", 0)),
            completion_tokens=int(data.get("completion_tokens", 0)),
            last_status=str(data.get("last_status", "never")),
            last_error=str(data.get("last_error", "")),
            last_used=float(data.get("last_used", 0)),
            disabled=bool(data.get("disabled", False)),
        )


@dataclass
class RouterConfig:
    host: str = "127.0.0.1"
    port: int = 4000
    virtual_keys: dict[str, set[str]] = field(default_factory=dict)   # vk -> set(pool names); empty set = all
    endpoints_override: dict[str, list[str]] = field(default_factory=dict)
    strategy: str = "cache_affinity_ttl"
    cache_ttl_seconds: int = 300
    fallback_strategy: str = "weighted_round_robin"
    default_cooldown_seconds: int = 60
    max_retries: int = 3
    max_concurrent_per_key: int = 8
    connect_timeout: float = 10.0
    first_byte_timeout: float = 60.0
    stream_idle_timeout: float = 600.0
    total_timeout: float = 1200.0
    total_timeout_stream: float = 1800.0
    expose_rate_limits: bool = False
    hot_reload: bool = True
    hot_reload_poll_seconds: float = 2.0
    state_file: Path = Path("state.json")
    log_file: Path = Path("router.log")
    log_level: str = "INFO"
    admin_token: str | None = None
    tunnel_token: str | None = None     # 验证 inner 连接的 shared secret（relay 侧配置）


def load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(path: Path) -> RouterConfig:
    data = load_yaml_file(path)
    listen = data.get("listen", {}) or {}
    virtual_keys: dict[str, set[str]] = {}
    for item in data.get("virtual_keys", []) or []:
        vk = str(item.get("key", "")).strip()
        if not vk:
            continue
        pools = item.get("pools", []) or []
        virtual_keys[vk] = set(str(p) for p in pools) if pools else set()  # empty = all

    endpoints_override: dict[str, list[str]] = {}
    for path_key, val in (data.get("endpoints") or {}).items():
        if isinstance(val, list):
            endpoints_override[str(path_key)] = [str(p) for p in val]
        else:
            endpoints_override[str(path_key)] = [str(val)]

    log = data.get("log", {}) or {}
    return RouterConfig(
        host=str(listen.get("host", "127.0.0.1")),
        port=int(listen.get("port", 4000)),
        virtual_keys=virtual_keys,
        endpoints_override=endpoints_override,
        strategy=str(data.get("strategy", "cache_affinity_ttl")),
        cache_ttl_seconds=int(data.get("cache_ttl_seconds", 300)),
        fallback_strategy=str(data.get("fallback_strategy", "weighted_round_robin")),
        default_cooldown_seconds=int(data.get("default_cooldown_seconds", data.get("cooldown_seconds", 60))),
        max_retries=int(data.get("max_retries", 3)),
        max_concurrent_per_key=int(data.get("max_concurrent_per_key", 8)),
        connect_timeout=float(data.get("connect_timeout", 10)),
        first_byte_timeout=float(data.get("first_byte_timeout", 60)),
        stream_idle_timeout=float(data.get("stream_idle_timeout", 600)),
        total_timeout=float(data.get("total_timeout", 1200)),
        total_timeout_stream=float(data.get("total_timeout_stream", 1800)),
        expose_rate_limits=bool(data.get("expose_rate_limits", False)),
        hot_reload=bool(data.get("hot_reload", data.get("hot_reload_keys", True))),
        hot_reload_poll_seconds=float(data.get("hot_reload_poll_seconds", 2)),
        state_file=Path(data.get("state_file", "./state.json")),
        log_file=Path(log.get("file", "./router.log")),
        log_level=str(log.get("level", "INFO")),
        admin_token=data.get("admin_token"),
        tunnel_token=data.get("tunnel_token"),
    )


def load_pools(path: Path) -> dict[str, PoolConfig]:
    data = load_yaml_file(path)
    pools: dict[str, PoolConfig] = {}
    for name, raw in (data.get("pools") or {}).items():
        if not isinstance(raw, dict):
            continue
        ptype = str(raw.get("type", "")).strip()
        if ptype not in VALID_TYPES:
            raise ValueError(f"pool {name!r}: invalid/missing type {ptype!r} (must be one of {VALID_TYPES})")
        auth_scheme = str(raw.get("auth_scheme", TYPE_DEFAULT_AUTH[ptype])).strip()
        if auth_scheme not in VALID_AUTH:
            raise ValueError(f"pool {name!r}: invalid auth_scheme {auth_scheme!r}")
        keys: list[RealKey] = []
        for item in raw.get("keys", []) or []:
            if not isinstance(item, dict) or not item.get("id") or not item.get("key"):
                continue
            upstream = item.get("upstream")
            if not upstream:
                raise ValueError(f"pool {name!r} key {item.get('id')!r}: 'upstream' is required (无默认值，必须填)")
            # tunnel pool 的 upstream 格式: tunnel://<tunnel_id> 或直接写 key id
            key_id = str(item["id"])
            tunnel_id = ""
            raw_upstream = str(upstream)
            if ptype == "tunnel":
                if raw_upstream.startswith("tunnel://"):
                    tunnel_id = raw_upstream[len("tunnel://"):]
                else:
                    tunnel_id = raw_upstream
                raw_upstream = f"tunnel://{tunnel_id}"
            keys.append(RealKey(
                id=key_id,
                key=str(item["key"]),
                upstream=raw_upstream.rstrip("/"),
                weight=max(1, int(item.get("weight", 1))),
                priority=int(item.get("priority", 10)),
                max_concurrent=int(item["max_concurrent"]) if item.get("max_concurrent") else None,
                support_models=[str(m) for m in item.get("support_models", []) or []],
                disabled=bool(item.get("disabled", False)),
                tunnel_id=tunnel_id,
            ))
        pools[str(name)] = PoolConfig(name=str(name), type=ptype, auth_scheme=auth_scheme, keys=keys)
    return pools


def build_path_map(pools: dict[str, PoolConfig], endpoints_override: dict[str, list[str]]) -> dict[str, list[str]]:
    """path -> list of pool names serving it (约定式默认 + endpoints 覆盖)."""
    path_map: dict[str, list[str]] = {}
    for pname, pool in pools.items():
        for p in PATHS_BY_TYPE.get(pool.type, []):
            path_map.setdefault(p, []).append(pname)
    # 覆盖：覆盖表完全替换该 path 的池列表
    for p, names in endpoints_override.items():
        path_map[p] = names
    return path_map


def load_state(path: Path) -> dict[str, KeyRuntime]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {key: KeyRuntime.from_dict(value) for key, value in raw.get("keys", {}).items()}
    except Exception:
        return {}


def save_state(path: Path, states: dict[str, KeyRuntime]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    payload = {"updated_at": time.time(), "keys": {key: value.to_dict() for key, value in states.items()}}
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ─────────────────────────────────────────────────────────────────────────────
# Comment-preserving YAML writes (for Admin UI / CLI edits) — DESIGN §16.4.3
# ─────────────────────────────────────────────────────────────────────────────
def _ruamel():
    from ruamel.yaml import YAML
    y = YAML()
    y.preserve_quotes = True
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def edit_yaml(path: Path, mutate) -> None:
    """Load yaml preserving comments, apply mutate(data), atomic write back.

    mutate(data) receives the parsed ruamel structure and edits it in place.
    """
    yaml_rt = _ruamel()
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            data = yaml_rt.load(f) or {}
    else:
        data = {}
    mutate(data)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        yaml_rt.dump(data, f)
    tmp.replace(path)


def mask_secret(value: str, keep: int = 4) -> str:
    """ark-1234...****...cdef — show only first/last `keep` chars."""
    s = str(value)
    if len(s) <= keep * 2 + 3:
        return "****"
    return f"{s[:keep]}…****…{s[-keep:]}"


def sanitize_text(text: str) -> str:
    """Mask obvious secrets in raw yaml text for read-only display.
    Anything after `key:` (real key or vk) and `admin_token:` is masked,
    regardless of length (it's always a credential field)."""
    import re
    text = re.sub(r'(\bkey:\s*["\']?)([A-Za-z0-9\-_.]{4,})(["\']?)',
                  lambda m: m.group(1) + mask_secret(m.group(2)) + m.group(3), text)
    text = re.sub(r'(\badmin_token:\s*["\']?)([^\s"\']{3,})(["\']?)',
                  lambda m: m.group(1) + mask_secret(m.group(2)) + m.group(3), text)
    return text


class StateFlusher:
    def __init__(self, path: Path, flush_interval: float = 30.0):
        self.path = path
        self.flush_interval = flush_interval
        self._dirty = False
        self._task: asyncio.Task | None = None

    def mark_dirty(self) -> None:
        self._dirty = True

    async def start(self, get_states, loop: asyncio.AbstractEventLoop | None = None) -> None:
        async def _loop():
            while True:
                await asyncio.sleep(self.flush_interval)
                if self._dirty:
                    save_state(self.path, get_states())
                    self._dirty = False

        self._task = (loop or asyncio.get_event_loop()).create_task(_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
