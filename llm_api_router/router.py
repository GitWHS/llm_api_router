from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from pathlib import Path
from typing import Any

import httpx

from .config import (KeyRuntime, PoolConfig, RealKey, RouterConfig, StateFlusher,
                     build_path_map, load_pools, load_state)


class PoolExhausted(RuntimeError):
    def __init__(self, retry_after: float = 30.0, reason: str = "all keys cooling down"):
        super().__init__(reason)
        self.retry_after = retry_after


class RouterState:
    def __init__(self, config: RouterConfig, keys_path: Path):
        self.config = config
        self.keys_path = keys_path
        self.pools = load_pools(keys_path)
        self.key_mtime = keys_path.stat().st_mtime if keys_path.exists() else 0.0
        self.cfg_mtime = 0.0
        self.runtime = load_state(config.state_file)
        self.cursors: dict[str, int] = {}
        # cache_affinity_ttl 粘性表: fingerprint -> (key_global_id, last_used_ts)
        self.sticky: dict[str, tuple[str, float]] = {}
        # 请求日志环形缓冲（Admin UI Logs 页 / usage 聚合用）
        self.request_log: deque[dict[str, Any]] = deque(maxlen=500)
        self._lock = asyncio.Lock()
        self._flusher = StateFlusher(config.state_file)

    def record_request(self, entry: dict[str, Any]) -> None:
        entry.setdefault("ts", time.time())
        self.request_log.append(entry)

    def rebuild_path_map(self) -> dict[str, list[str]]:
        return build_path_map(self.pools, self.config.endpoints_override)

    def maybe_reload_keys_locked(self) -> bool:
        """Reload keys.yaml if mtime changed. Returns True if reloaded."""
        if not self.config.hot_reload or not self.keys_path.exists():
            return False
        mtime = self.keys_path.stat().st_mtime
        if mtime == self.key_mtime:
            return False
        try:
            new_pools = load_pools(self.keys_path)
        except Exception:
            return False  # invalid file, keep current
        self.pools = new_pools
        self.key_mtime = mtime
        return True

    def runtime_for(self, pool: str, key: RealKey) -> KeyRuntime:
        rid = f"{pool}::{key.id}"
        state = self.runtime.setdefault(rid, KeyRuntime())
        if key.disabled:
            state.disabled = True
        return state

    def assert_virtual_key(self, virtual_key: str, pool_names: list[str]) -> None:
        allowed = self.config.virtual_keys.get(virtual_key)
        if allowed is None:
            raise PermissionError("invalid virtual key")
        if allowed:  # non-empty = explicit allowlist; empty set = all pools
            if not any(p in allowed for p in pool_names):
                raise PermissionError("virtual key cannot access these pools")

    async def pick_key(self, pool_names: list[str], model: str | None,
                       fingerprint: str | None, exclude: set[str] | None = None
                       ) -> tuple[PoolConfig, RealKey, KeyRuntime]:
        async with self._lock:
            return self._pick_key_locked(pool_names, model, fingerprint, exclude)

    def _pick_key_locked(self, pool_names: list[str], model: str | None,
                         fingerprint: str | None, exclude: set[str] | None
                         ) -> tuple[PoolConfig, RealKey, KeyRuntime]:
        self.maybe_reload_keys_locked()
        exclude = exclude or set()
        now = time.time()
        # 收集候选：(pool, key, state)
        candidates: list[tuple[PoolConfig, RealKey, KeyRuntime]] = []
        soonest = 0.0
        total_keys = 0            # 所有池的 key 总数（未 exclude/disabled）
        filtered_by_model = 0     # 因 model 白名单不匹配被排除的 key 数
        model_whitelists: set[str] = set()
        for pname in pool_names:
            pool = self.pools.get(pname)
            if not pool:
                continue
            for key in pool.keys:
                if key.id in exclude:
                    continue
                state = self.runtime_for(pname, key)
                if state.disabled:
                    continue
                total_keys += 1
                if state.cooldown_until and state.cooldown_until > now:
                    wait = state.cooldown_until - now
                    if soonest == 0.0 or wait < soonest:
                        soonest = wait
                    continue
                # model 过滤（§4.9）：support_models 非空时必须包含请求 model。
                # 但 tunnel 池豁免——relay 是中转站，不知道 inner 后面真正支持哪些
                # 模型（客户端可能发带版本后缀的模型名如 claude-opus-4-6-v-1）。
                # 一律透传给 inner，由 inner/上游裁决，避免 relay 误判导致 PoolExhausted。
                if (model and key.support_models and pool.type != "tunnel"
                        and model not in key.support_models):
                    filtered_by_model += 1
                    model_whitelists.update(key.support_models)
                    continue
                # 并发限流：仅当 key 显式设了 max_concurrent 才生效；None = 无上限（默认）
                if key.max_concurrent is not None and state.active >= key.max_concurrent:
                    continue
                candidates.append((pool, key, state))

        if not candidates:
            # 区分原因，避免把"模型不在白名单"误报为"all keys cooling down"
            if filtered_by_model and filtered_by_model == total_keys and soonest == 0.0:
                wl = ", ".join(sorted(model_whitelists)) or "(none)"
                raise PoolExhausted(
                    retry_after=30.0,
                    reason=(f"no key supports model '{model}'. "
                            f"Pools {pool_names} only allow: [{wl}]. "
                            f"Fix: add '{model}' to that pool's support_models, "
                            f"or clear support_models to accept any model."),
                )
            raise PoolExhausted(retry_after=max(soonest, 30.0) if soonest > 0 else 30.0)

        strategy = self.config.strategy
        chosen: tuple[PoolConfig, RealKey, KeyRuntime] | None = None

        # cache_affinity_ttl / cache_affinity：粘性优先
        if strategy in ("cache_affinity_ttl", "cache_affinity") and fingerprint:
            sticky = self.sticky.get(fingerprint)
            if sticky:
                gid, last_used = sticky
                in_window = (strategy == "cache_affinity") or (now - last_used < self.config.cache_ttl_seconds)
                if in_window:
                    for pool, key, state in candidates:
                        if f"{pool.name}::{key.id}" == gid:
                            chosen = (pool, key, state)
                            break

        if chosen is None:
            fallback = self.config.fallback_strategy if strategy == "cache_affinity_ttl" else strategy
            if fallback == "priority":
                chosen = max(candidates, key=lambda item: item[1].priority)
            elif fallback == "least_active":
                chosen = min(candidates, key=lambda item: item[2].active)
            elif fallback == "lru":
                chosen = min(candidates, key=lambda item: item[2].last_used)
            elif fallback == "weighted_round_robin":
                expanded: list[tuple[PoolConfig, RealKey, KeyRuntime]] = []
                for item in candidates:
                    expanded.extend([item] * item[1].weight)
                cursor_key = "|".join(pool_names)
                cursor = self.cursors.get(cursor_key, 0) % len(expanded)
                chosen = expanded[cursor]
                self.cursors[cursor_key] = cursor + 1
            else:  # round_robin
                cursor_key = "|".join(pool_names)
                cursor = self.cursors.get(cursor_key, 0) % len(candidates)
                chosen = candidates[cursor]
                self.cursors[cursor_key] = cursor + 1

            if strategy in ("cache_affinity_ttl", "cache_affinity") and fingerprint:
                self.sticky[fingerprint] = (f"{chosen[0].name}::{chosen[1].id}", now)

        pool, key, state = chosen
        state.active += 1
        state.last_used = now
        return pool, key, state

    async def release_key(self, pool: str, key: RealKey) -> None:
        async with self._lock:
            state = self.runtime_for(pool, key)
            state.active = max(0, state.active - 1)

    async def mark_success(self, pool: str, key: RealKey, usage: dict[str, Any] | None = None) -> None:
        async with self._lock:
            state = self.runtime_for(pool, key)
            state.request_count += 1
            state.consecutive_fails = 0
            state.last_status = "ok"
            state.last_error = ""
            state.last_used = time.time()
            if usage:
                state.prompt_tokens += int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
                state.completion_tokens += int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
            self._flusher.mark_dirty()

    async def mark_failure(self, pool: str, key: RealKey, status_code: int | None,
                           message: str, retry_after: float | None = None) -> None:
        async with self._lock:
            state = self.runtime_for(pool, key)
            state.consecutive_fails += 1
            state.last_status = f"error:{status_code or 'network'}"
            state.last_error = message[:500]
            now = time.time()
            if status_code in {401, 403}:
                # 鉴权类错误不再永久禁用 key（disabled 仅由人工通过 keys.yaml /
                # admin 接口控制）。上游可能只是临时不可用（如反代/令牌短暂失效），
                # 这里改为冷却，使其在冷却到期后自动恢复参与调度。
                state.cooldown_until = now + (retry_after or self.config.default_cooldown_seconds)
            elif status_code == 429:
                state.cooldown_until = now + (retry_after or self.config.default_cooldown_seconds)
            elif status_code == 529 or "overloaded_error" in message:
                state.cooldown_until = now + 30
            elif status_code and status_code >= 500 and state.consecutive_fails >= 3:
                state.cooldown_until = now + self.config.default_cooldown_seconds
            self._flusher.mark_dirty()


def compute_fingerprint(body_json: dict[str, Any] | None) -> str | None:
    """hash(system + tools) for cache_affinity stickiness. None if unparseable."""
    if not isinstance(body_json, dict):
        return None
    system = body_json.get("system") or body_json.get("instructions")
    if system is None and isinstance(body_json.get("messages"), list):
        system = next((m.get("content") for m in body_json["messages"] if isinstance(m, dict) and m.get("role") == "system"), None)
    tools = body_json.get("tools")
    if system is None and tools is None:
        return None
    payload = json.dumps({"s": system, "t": tools}, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
