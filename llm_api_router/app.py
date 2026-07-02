from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .config import LOCAL_ENDPOINTS, KeyRuntime, RouterConfig, load_config
from .router import PoolExhausted, RouterState, compute_fingerprint

logger = logging.getLogger("llm_api_router.app")

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
              "te", "trailers", "transfer-encoding", "upgrade", "content-length"}
# 客户端不应看到的头（单 key 配额/会话残留）
STRIP_RESP_HEADERS = HOP_BY_HOP | {
    "x-api-key", "set-cookie", "x-ratelimit-limit-requests", "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-requests", "x-ratelimit-remaining-tokens", "x-ratelimit-reset-requests",
    "x-ratelimit-reset-tokens",
} | {h for h in (
    "anthropic-ratelimit-requests-limit", "anthropic-ratelimit-requests-remaining",
    "anthropic-ratelimit-requests-reset", "anthropic-ratelimit-tokens-limit",
    "anthropic-ratelimit-tokens-remaining", "anthropic-ratelimit-tokens-reset",
)}
MAX_BODY_BYTES = 20 * 1024 * 1024  # 20MB（agent 单请求体可能很大，Codex 重发多轮 input）


def _error_response(message: str, status_code: int, **extra) -> JSONResponse:
    body: dict = {"error": message}
    body.update(extra)
    headers = {"X-Router-Error": "1"}
    if "retry_after" in extra:
        headers["Retry-After"] = str(extra["retry_after"])
    return JSONResponse(body, status_code=status_code, headers=headers)


def extract_virtual_key(request: Request) -> str | None:
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key.strip()
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def upstream_path(path: str) -> str:
    """Strip router protocol prefix so upstream gets a clean path.
    /anthropic/v1/messages -> /v1/messages ; /v1/responses -> /v1/responses"""
    for prefix in ("/anthropic", "/openai"):
        if path.startswith(prefix + "/"):
            return path[len(prefix):]
    return path


def copy_headers(request: Request, auth_scheme: str, real_key: str) -> dict[str, str]:
    headers = {
        key: value for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP and key.lower() not in {"host", "authorization", "x-api-key"}
    }
    if auth_scheme == "bearer":
        headers["authorization"] = f"Bearer {real_key}"
    else:  # x-api-key
        headers["x-api-key"] = real_key
    return headers


def response_headers(headers: httpx.Headers, expose_rate_limits: bool) -> dict[str, str]:
    strip = STRIP_RESP_HEADERS if not expose_rate_limits else HOP_BY_HOP
    return {key: value for key, value in headers.items() if key.lower() not in strip}


def _is_streaming_request(body: bytes) -> bool:
    if not body:
        return False
    try:
        data = json.loads(body.decode("utf-8"))
        return isinstance(data, dict) and data.get("stream") is True
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False


def _wants_stream(request: Request, body: bytes) -> bool:
    return _is_streaming_request(body) or "text/event-stream" in request.headers.get("accept", "")


def usage_from_json(content: bytes) -> dict | None:
    try:
        data = json.loads(content.decode("utf-8"))
    except Exception:
        return None
    return data.get("usage") if isinstance(data, dict) else None


def normalize_usage(usage: dict | None) -> dict[str, int]:
    usage = usage or {}
    prompt = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
    completion = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
    total = int(usage.get("total_tokens") or prompt + completion)
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total}


def _new_usage_bucket() -> dict:
    return {
        "requests": 0,
        "ok": 0,
        "errors": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "retries": 0,
        "stream": 0,
        "duration_ms_total": 0,
        "duration_ms_avg": 0,
    }


def _add_usage(bucket: dict, entry: dict) -> None:
    status = int(entry.get("status", 0))
    good = 200 <= status < 400
    prompt = int(entry.get("prompt_tokens") or 0)
    completion = int(entry.get("completion_tokens") or 0)
    total = int(entry.get("total_tokens") or prompt + completion)
    duration = int(entry.get("duration_ms") or 0)
    retried = int(entry.get("retried") or 0)
    bucket["requests"] += 1
    bucket["ok"] += 1 if good else 0
    bucket["errors"] += 0 if good else 1
    bucket["prompt_tokens"] += prompt
    bucket["completion_tokens"] += completion
    bucket["total_tokens"] += total
    bucket["retries"] += retried
    bucket["stream"] += 1 if entry.get("stream") else 0
    bucket["duration_ms_total"] += duration
    bucket["duration_ms_avg"] = round(bucket["duration_ms_total"] / max(bucket["requests"], 1), 2)


def _percentile(values: list[int], pct: float) -> int:
    if not values:
        return 0
    values = sorted(values)
    idx = min(len(values) - 1, max(0, round((len(values) - 1) * pct)))
    return values[idx]


def _status_family(status: int) -> str:
    if status == 0:
        return "network"
    if status in {401, 403, 429, 529}:
        return str(status)
    if 400 <= status < 500:
        return "4xx"
    if status >= 500:
        return "5xx"
    return str(status)


def is_retryable(status_code: int, body: bytes) -> bool:
    text = body[:2000].decode("utf-8", errors="ignore")
    return status_code in {429, 529} or status_code >= 500 or "overloaded_error" in text


def _parse_body_json(body: bytes) -> dict | None:
    if not body:
        return None
    try:
        data = json.loads(body.decode("utf-8"))
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _passthrough_error(content: bytes, resp: httpx.Response, expose_rate_limits: bool) -> Response:
    """Forward a non-retryable upstream error (status+body) verbatim to the client."""
    hdrs = response_headers(resp.headers, expose_rate_limits)
    hdrs.pop("content-encoding", None)
    hdrs.pop("Content-Encoding", None)
    return Response(content=content, status_code=resp.status_code,
                    headers=hdrs, media_type=resp.headers.get("content-type"))


def _httpx_timeout(config: RouterConfig, stream: bool) -> httpx.Timeout:
    return httpx.Timeout(
        connect=config.connect_timeout,
        read=config.stream_idle_timeout if stream else None,
        write=config.connect_timeout,
        pool=config.connect_timeout,
        timeout=config.total_timeout_stream if stream else config.total_timeout,
    )


def _setup_logging(config: RouterConfig) -> None:
    """Wire our package loggers to console + rotating file at configured level.
    Without this, logger.info() calls are swallowed (root defaults to WARNING)."""
    from logging.handlers import RotatingFileHandler
    level = getattr(logging, str(config.log_level).upper(), logging.INFO)
    pkg_logger = logging.getLogger("llm_api_router")
    pkg_logger.setLevel(level)
    pkg_logger.propagate = False
    # Avoid duplicate handlers on reload
    if pkg_logger.handlers:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    pkg_logger.addHandler(console)
    try:
        fh = RotatingFileHandler(str(config.log_file), maxBytes=10 * 1024 * 1024,
                                 backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        pkg_logger.addHandler(fh)
    except Exception:
        pass


def create_app(config_path: str | Path = "config.yaml", keys_path: str | Path = "keys.yaml",
               transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    config = load_config(Path(config_path))
    _setup_logging(config)
    state = RouterState(config, Path(keys_path))
    app = FastAPI(title="LLM API Router", version="0.2.0")
    app.state.router_state = state
    app.state.transport = transport
    app.state.config_path = str(config_path)
    app.state.keys_path = str(keys_path)

    # TunnelManager（仅当有 tunnel pool 或配置了 tunnel_token 时初始化）
    from .tunnel import TunnelManager
    tunnel_mgr = TunnelManager(token=config.tunnel_token)
    app.state.tunnel_mgr = tunnel_mgr

    # Admin UI：同进程托管静态资源（DESIGN §16.7.1）。必须在 catch-all proxy 路由之前
    # 注册，否则 /ui/* 会被 /{full_path:path} 抢先匹配返回 404。
    ui_dist = Path(__file__).parent / "static" / "ui"
    if ui_dist.exists():
        from fastapi.staticfiles import StaticFiles
        app.mount("/ui", StaticFiles(directory=str(ui_dist), html=True), name="ui")

    @app.on_event("startup")
    async def _startup() -> None:
        await state._flusher.start(lambda: state.runtime)
        if config.hot_reload:
            app.state.reload_task = asyncio.create_task(_reload_watcher(app))

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        task = getattr(app.state, "reload_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await state._flusher.stop()

    async def _reload_watcher(app: FastAPI) -> None:
        while True:
            await asyncio.sleep(config.hot_reload_poll_seconds)
            try:
                # config.yaml 热改（可热改部分）
                cfg_mtime = Path(app.state.config_path).stat().st_mtime if Path(app.state.config_path).exists() else 0
                if cfg_mtime != state.cfg_mtime and cfg_mtime:
                    new_cfg = load_config(Path(app.state.config_path))
                    # 只更新可热改字段；listen/log/admin_token 不动
                    state.config.virtual_keys = new_cfg.virtual_keys
                    state.config.endpoints_override = new_cfg.endpoints_override
                    state.config.strategy = new_cfg.strategy
                    state.config.cache_ttl_seconds = new_cfg.cache_ttl_seconds
                    state.config.fallback_strategy = new_cfg.fallback_strategy
                    state.config.default_cooldown_seconds = new_cfg.default_cooldown_seconds
                    state.config.max_retries = new_cfg.max_retries
                    state.config.max_concurrent_per_key = new_cfg.max_concurrent_per_key
                    state.cfg_mtime = cfg_mtime
                # keys.yaml 热改
                async with state._lock:
                    state.maybe_reload_keys_locked()
            except Exception:
                pass

    @app.get("/healthz")
    async def healthz() -> dict:
        connected_tunnels = [tid for tid, conn in tunnel_mgr._conns.items() if not conn._closed]
        return {"ok": True, "pools": sorted(state.pools.keys()), "tunnels": connected_tunnels}

    @app.get("/")
    async def root() -> dict:
        return {"service": "llm_api_router", "ok": True, "pools": sorted(state.pools.keys())}

    @app.get("/stats")
    async def stats() -> dict:
        pools_data = {}
        now = time.time()
        for pool_name, pool in state.pools.items():
            keys_data, pool_requests, pool_fails = {}, 0, 0
            prompt_tokens = completion_tokens = active = cooling = disabled = 0
            for key in pool.keys:
                rt = state.runtime_for(pool_name, key)
                keys_data[key.id] = rt.to_dict()
                pool_requests += rt.request_count
                pool_fails += rt.consecutive_fails
                prompt_tokens += rt.prompt_tokens
                completion_tokens += rt.completion_tokens
                active += rt.active
                disabled += 1 if rt.disabled else 0
                cooling += 1 if rt.cooldown_until and rt.cooldown_until > now else 0
            total = pool_requests + pool_fails
            pools_data[pool_name] = {
                "type": pool.type, "auth_scheme": pool.auth_scheme,
                "upstreams": sorted({k.upstream for k in pool.keys}),
                "keys": keys_data, "total_requests": pool_requests,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
                "active": active,
                "success_rate": round(pool_requests / max(total, 1), 4),
                "key_count": len(pool.keys),
                "enabled_keys": len(pool.keys) - disabled,
                "healthy_keys": sum(1 for k in pool.keys if not state.runtime_for(pool_name, k).disabled),
                "available_keys": sum(1 for k in pool.keys if not state.runtime_for(pool_name, k).disabled and not (state.runtime_for(pool_name, k).cooldown_until and state.runtime_for(pool_name, k).cooldown_until > now)),
                "cooling_keys": cooling,
                "disabled_keys": disabled,
            }
        return {"pools": pools_data, "strategy": state.config.strategy, "cache_ttl_seconds": state.config.cache_ttl_seconds}

    @app.post("/admin/reload")
    async def admin_reload(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        reloaded = False
        async with state._lock:
            reloaded = state.maybe_reload_keys_locked()
        new_cfg = load_config(Path(app.state.config_path))
        state.config.virtual_keys = new_cfg.virtual_keys
        state.config.endpoints_override = new_cfg.endpoints_override
        return {"reloaded_keys": reloaded, "pools": sorted(state.pools.keys())}

    @app.get("/admin/pools")
    async def admin_pools(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        return {
            name: {"type": p.type, "auth_scheme": p.auth_scheme,
                   "keys": [{"id": k.id, "weight": k.weight, "support_models": k.support_models,
                             "disabled": state.runtime_for(name, k).disabled,
                             "upstream": k.upstream,
                             "max_concurrent": k.max_concurrent,
                             "runtime": state.runtime_for(name, k).to_dict()} for k in p.keys]}
            for name, p in state.pools.items()
        }

    @app.get("/admin/routes")
    async def admin_routes(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        path_map = state.rebuild_path_map()
        routes = []
        for path, pool_names in sorted(path_map.items()):
            pool_items = []
            models: set[str] = set()
            available = key_count = cooling = disabled = active = 0
            for pname in pool_names:
                pool = state.pools.get(pname)
                if not pool:
                    pool_items.append({"name": pname, "missing": True})
                    continue
                key_rows = []
                for key in pool.keys:
                    rt = state.runtime_for(pname, key)
                    key_count += 1
                    active += rt.active
                    if key.support_models:
                        models.update(key.support_models)
                    is_cooling = bool(rt.cooldown_until and rt.cooldown_until > time.time())
                    cooling += 1 if is_cooling else 0
                    disabled += 1 if rt.disabled else 0
                    available += 0 if rt.disabled or is_cooling else 1
                    key_rows.append({
                        "id": key.id,
                        "support_models": key.support_models,
                        "disabled": rt.disabled,
                        "cooldown_until": rt.cooldown_until,
                        "active": rt.active,
                    })
                pool_items.append({
                    "name": pname,
                    "type": pool.type,
                    "auth_scheme": pool.auth_scheme,
                    "key_count": len(pool.keys),
                    "keys": key_rows,
                })
            allowed_vks = []
            for vk, allowed in state.config.virtual_keys.items():
                if not allowed or any(p in allowed for p in pool_names):
                    allowed_vks.append(vk)
            routes.append({
                "path": path,
                "pools": pool_items,
                "pool_names": pool_names,
                "models": sorted(models),
                "key_count": key_count,
                "available_keys": available,
                "cooling_keys": cooling,
                "disabled_keys": disabled,
                "active": active,
                "virtual_keys": sorted(allowed_vks),
            })
        return {"routes": routes, "strategy": state.config.strategy, "fallback_strategy": state.config.fallback_strategy}

    @app.get("/admin/config")
    async def admin_config(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        from .config import sanitize_text
        cfg_p = Path(app.state.config_path)
        keys_p = Path(app.state.keys_path)
        return {
            "config_yaml": sanitize_text(cfg_p.read_text(encoding="utf-8")) if cfg_p.exists() else "",
            "keys_yaml": sanitize_text(keys_p.read_text(encoding="utf-8")) if keys_p.exists() else "",
            "config_mtime": cfg_p.stat().st_mtime if cfg_p.exists() else 0,
            "keys_mtime": keys_p.stat().st_mtime if keys_p.exists() else 0,
            "strategy": state.config.strategy,
            "cache_ttl_seconds": state.config.cache_ttl_seconds,
        }

    @app.get("/admin/vk")
    async def admin_vk_list(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        return {"virtual_keys": [
            {"key": vk, "pools": sorted(pools) if pools else []}  # 空 = 所有池
            for vk, pools in state.config.virtual_keys.items()
        ]}

    @app.post("/admin/vk")
    async def admin_vk_add(request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        payload = await request.json()
        vk = str(payload.get("key", "")).strip()
        pools = [str(p) for p in payload.get("pools", []) or []]
        if not vk:
            return _error_response("key required", 400)
        from .config import edit_yaml
        def _mut(data):
            vks = data.setdefault("virtual_keys", [])
            for item in vks:
                if item.get("key") == vk:
                    item["pools"] = pools
                    return
            vks.append({"key": vk, "pools": pools})
        edit_yaml(Path(app.state.config_path), _mut)
        _hot_reload_config()
        return {"ok": True, "key": vk, "pools": pools}

    @app.delete("/admin/vk/{vk}")
    async def admin_vk_del(vk: str, request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        from .config import edit_yaml
        def _mut(data):
            vks = data.get("virtual_keys", []) or []
            data["virtual_keys"] = [i for i in vks if i.get("key") != vk]
        edit_yaml(Path(app.state.config_path), _mut)
        _hot_reload_config()
        return {"ok": True, "removed": vk}

    @app.post("/admin/keys/{pool}/{key_id}/{action}")
    async def admin_key_action(pool: str, key_id: str, action: str, request: Request) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        if action not in {"disable", "enable", "reset-cooldown"}:
            return _error_response("unknown action", 400)
        p = state.pools.get(pool)
        if not p or not any(k.id == key_id for k in p.keys):
            return _error_response("unknown pool/key", 404)
        rid = f"{pool}::{key_id}"
        async with state._lock:
            rt = state.runtime.setdefault(rid, KeyRuntime())
            if action == "disable":
                rt.disabled = True
            elif action == "enable":
                rt.disabled = False
                rt.cooldown_until = 0.0
                rt.consecutive_fails = 0
            elif action == "reset-cooldown":
                rt.cooldown_until = 0.0
                rt.consecutive_fails = 0
            state._flusher.mark_dirty()
        # disable/enable 持久化到 keys.yaml（reset-cooldown 纯 runtime）
        if action in {"disable", "enable"}:
            from .config import edit_yaml
            want = action == "disable"
            def _mut(data):
                for pname, praw in (data.get("pools") or {}).items():
                    if pname != pool:
                        continue
                    for item in praw.get("keys", []) or []:
                        if item.get("id") == key_id:
                            item["disabled"] = want
            edit_yaml(Path(app.state.keys_path), _mut)
            async with state._lock:
                state.maybe_reload_keys_locked()
        return {"ok": True, "pool": pool, "key": key_id, "action": action}

    @app.get("/admin/logs/recent")
    async def admin_logs(request: Request, limit: int = 200) -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        items = list(state.request_log)[-max(1, min(limit, 500)):]
        return {"logs": list(reversed(items)), "count": len(items)}

    @app.get("/admin/usage")
    async def admin_usage(request: Request, window: str = "1h") -> dict:
        if not _check_admin(request):
            return _error_response("forbidden", 403)
        import time as _t
        seconds = {"15m": 900, "1h": 3600, "24h": 86400, "7d": 604800, "30d": 2592000}.get(window, 3600)
        now = _t.time()
        cutoff = now - seconds
        recent = [e for e in state.request_log if e.get("ts", 0) >= cutoff]
        by_pool: dict[str, dict] = {}
        by_vk: dict[str, dict] = {}
        by_key: dict[str, dict] = {}
        by_model: dict[str, dict] = {}
        by_endpoint: dict[str, dict] = {}
        by_status: dict[str, int] = {}
        by_status_family: dict[str, int] = {}
        retry_histogram: dict[str, int] = {}
        durations: list[int] = []
        totals = _new_usage_bucket()
        bucket_count = 12
        bucket_seconds = max(1, int(seconds / bucket_count))
        series = [
            {
                "start": cutoff + i * bucket_seconds,
                "end": cutoff + (i + 1) * bucket_seconds,
                "requests": 0,
                "errors": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            }
            for i in range(bucket_count)
        ]
        for e in recent:
            st = int(e.get("status", 0))
            good = 200 <= st < 400
            _add_usage(totals, e)
            durations.append(int(e.get("duration_ms") or 0))
            by_status[str(st)] = by_status.get(str(st), 0) + 1
            family = _status_family(st)
            by_status_family[family] = by_status_family.get(family, 0) + 1
            retry_key = str(int(e.get("retried") or 0))
            retry_histogram[retry_key] = retry_histogram.get(retry_key, 0) + 1

            for mapping, key in (
                (by_pool, e.get("pool") or "unassigned"),
                (by_vk, e.get("vk") or "unknown"),
                (by_key, f"{e.get('pool') or 'unassigned'}::{e.get('key_id') or 'none'}"),
                (by_model, e.get("model") or "unknown"),
                (by_endpoint, e.get("endpoint") or "unknown"),
            ):
                _add_usage(mapping.setdefault(str(key), _new_usage_bucket()), e)

            idx = min(bucket_count - 1, max(0, int((float(e.get("ts", cutoff)) - cutoff) / bucket_seconds)))
            series[idx]["requests"] += 1
            series[idx]["errors"] += 0 if good else 1
            series[idx]["prompt_tokens"] += int(e.get("prompt_tokens") or 0)
            series[idx]["completion_tokens"] += int(e.get("completion_tokens") or 0)
            series[idx]["total_tokens"] += int(e.get("total_tokens") or 0)

        rpm = round(totals["requests"] / max(seconds / 60, 1), 2)
        tpm = round(totals["total_tokens"] / max(seconds / 60, 1), 2)
        return {
            "window": window,
            "seconds": seconds,
            "total": totals["requests"],
            "ok": totals["ok"],
            "errors": totals["errors"],
            "prompt_tokens": totals["prompt_tokens"],
            "completion_tokens": totals["completion_tokens"],
            "total_tokens": totals["total_tokens"],
            "retries": totals["retries"],
            "stream": totals["stream"],
            "non_stream": totals["requests"] - totals["stream"],
            "success_rate": round(totals["ok"] / max(totals["requests"], 1), 4),
            "avg_tokens_per_request": round(totals["total_tokens"] / max(totals["requests"], 1), 2),
            "rpm": rpm,
            "tpm": tpm,
            "latency": {
                "avg_ms": totals["duration_ms_avg"],
                "p50_ms": _percentile(durations, 0.50),
                "p95_ms": _percentile(durations, 0.95),
                "p99_ms": _percentile(durations, 0.99),
            },
            "by_pool": by_pool,
            "by_vk": by_vk,
            "by_key": by_key,
            "by_model": by_model,
            "by_endpoint": by_endpoint,
            "by_status": by_status,
            "by_status_family": by_status_family,
            "retry_histogram": retry_histogram,
            "series": series,
        }

    def _hot_reload_config() -> None:
        new_cfg = load_config(Path(app.state.config_path))
        state.config.virtual_keys = new_cfg.virtual_keys
        state.config.endpoints_override = new_cfg.endpoints_override

    def _check_admin(request: Request) -> bool:
        if not config.admin_token:
            return False
        auth = request.headers.get("authorization", "")
        return auth == f"Bearer {config.admin_token}" or request.headers.get("x-admin-token") == config.admin_token

    @app.websocket("/tunnel/connect")
    async def tunnel_connect(websocket: WebSocket, tunnel_id: str = "", token: str = ""):
        await tunnel_mgr.handle_ws(websocket, tunnel_id, token)

    @app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(full_path: str, request: Request):
        path = "/" + full_path
        path_map = state.rebuild_path_map()
        pool_names = path_map.get(path) or path_map.get(path.rstrip("/"))
        if not pool_names:
            return _error_response("unknown endpoint", 404, path=path)

        virtual_key = extract_virtual_key(request)
        if not virtual_key:
            return _error_response("missing virtual key", 401)
        try:
            state.assert_virtual_key(virtual_key, pool_names)
        except PermissionError as exc:
            return _error_response(str(exc), 403)

        # 本地聚合端点
        if path in LOCAL_ENDPOINTS:
            return _aggregate_models(pool_names)

        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            return _error_response(f"body exceeds {MAX_BODY_BYTES // 1024 // 1024}MB", 413)
        body_json = _parse_body_json(body)
        model = body_json.get("model") if body_json else None
        fingerprint = compute_fingerprint(body_json)
        wants_stream = _wants_stream(request, body)

        if wants_stream:
            return await proxy_stream(request, state, pool_names, path, body, model, fingerprint, transport, virtual_key, tunnel_mgr)
        return await proxy_buffered(request, state, pool_names, path, body, model, fingerprint, transport, virtual_key, tunnel_mgr)

    def _aggregate_models(pool_names: list[str]) -> JSONResponse:
        models: set[str] = set()
        for pname in pool_names:
            pool = state.pools.get(pname)
            if not pool:
                continue
            for key in pool.keys:
                if key.support_models:
                    models.update(key.support_models)
        data = {"object": "list", "data": [{"id": m, "object": "model"} for m in sorted(models)]}
        return JSONResponse(data)

    return app


async def proxy_buffered(request: Request, state: RouterState, pool_names: list[str], path: str,
                         body: bytes, model: str | None, fingerprint: str | None,
                         transport: httpx.AsyncBaseTransport | None, vk: str = "",
                         tunnel_mgr=None):
    import time as _t
    from .tunnel import TunnelError, TunnelManager
    t0 = _t.time()
    excluded: set[str] = set()
    last_error = "all keys cooling down"
    attempts = state.config.max_retries + 1
    for attempt in range(attempts):
        try:
            pool, key, runtime = await state.pick_key(pool_names, model, fingerprint, excluded)
        except PoolExhausted as exc:
            logger.warning("[buffered] PoolExhausted pools=%s model=%s excluded=%s reason=%s",
                           pool_names, model, excluded, str(exc))
            state.record_request({"vk": vk, "endpoint": path, "pool": None, "key_id": None,
                                  "model": model, "status": 503, "stream": False,
                                  "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                  **normalize_usage(None)})
            return _error_response(str(exc) or "all keys cooling down", 503, retry_after=exc.retry_after)

        # ── Tunnel pool 分支 ──────────────────────────────────────────────────
        if pool.type == "tunnel" and tunnel_mgr is not None:
            tid = key.tunnel_id or key.id
            connected = tunnel_mgr.is_connected(tid)
            logger.info("[buffered] attempt=%d pool=%s key=%s tunnel=%s connected=%s model=%s path=%s",
                        attempt, pool.name, key.id, tid, connected, model, path)
            fwd_headers = copy_headers(request, pool.auth_scheme, key.key)
            fwd_headers.pop("anthropic-beta", None)  # 非 Anthropic 上游不识此 header
            try:
                status, resp_headers, resp_body = await tunnel_mgr.proxy_request(
                    tid,
                    request.method, path, fwd_headers, body, stream=False,
                    timeout=state.config.total_timeout,
                    params=str(request.url.query) if request.url.query else "",
                )
            except TunnelError as exc:
                # tunnel 未连接/断线是秒级恢复的临时故障：等待 inner 重连后重试同一 key，
                # 不 exclude（单 key 池 exclude 后会立刻 PoolExhausted）、不长冷却。
                last_error = str(exc)
                logger.warning("[buffered] TunnelError status=%s msg=%s attempt=%d/%d",
                               exc.status, last_error, attempt, attempts - 1)
                await state.release_key(pool.name, key)
                if exc.status == 503 and attempt < attempts - 1:
                    await asyncio.sleep(1.5)
                    continue
                await state.mark_failure(pool.name, key, exc.status, last_error)
                excluded.add(key.id)
                continue
            await state.release_key(pool.name, key)
            logger.info("[buffered] tunnel returned status=%s bytes=%d",
                        status, len(resp_body) if isinstance(resp_body, bytes) else 0)
            # tunnel 瞬时网关错误（502/503/504）：重试同一 key，绝不冷却单 key 池
            if status in {502, 503, 504}:
                snippet = (resp_body[:300].decode("utf-8", "ignore")
                           if isinstance(resp_body, bytes) else "")
                logger.warning("[buffered] transient %s from tunnel; body=%s attempt=%d/%d",
                               status, snippet, attempt, attempts - 1)
                if attempt < attempts - 1:
                    await asyncio.sleep(1.5)
                    continue
                state.record_request({"vk": vk, "endpoint": path, "pool": pool.name, "key_id": key.id,
                                      "model": model, "status": status, "stream": False,
                                      "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                      **normalize_usage(None)})
                return _error_response(f"tunnel upstream returned {status}", 503, retry_after=5)
            content = resp_body if isinstance(resp_body, bytes) else b""
            usage = normalize_usage(usage_from_json(content))
            if 200 <= status < 400:
                await state.mark_success(pool.name, key, usage)
            state.record_request({"vk": vk, "endpoint": path, "pool": pool.name, "key_id": key.id,
                                  "model": model, "status": status, "stream": False,
                                  "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                  **usage})
            clean_headers = {k: v for k, v in resp_headers.items()
                             if k.lower() not in {"content-encoding", "transfer-encoding"}}
            return Response(content=content, status_code=status, headers=clean_headers,
                            media_type=resp_headers.get("content-type"))
        # ── End tunnel branch ─────────────────────────────────────────────────

        url = f"{key.upstream}{upstream_path(path)}"
        headers = copy_headers(request, pool.auth_scheme, key.key)
        logger.info("[buffered] direct upstream pool=%s key=%s model=%s -> %s",
                    pool.name, key.id, model, url)
        try:
            async with httpx.AsyncClient(timeout=_httpx_timeout(state.config, stream=False), transport=transport) as client:
                resp = await client.request(request.method, url, content=body, headers=headers, params=request.query_params)
                content = await resp.aread()
        except httpx.HTTPError as exc:
            last_error = str(exc)
            logger.warning("[buffered] direct upstream NETWORK error pool=%s key=%s: %s -> cooldown",
                           pool.name, key.id, last_error)
            await state.mark_failure(pool.name, key, None, last_error)
            excluded.add(key.id)
            await state.release_key(pool.name, key)
            continue
        await state.release_key(pool.name, key)
        if is_retryable(resp.status_code, content):
            retry_after = float(resp.headers.get("retry-after", "0") or 0) or None
            logger.warning("[buffered] direct upstream RETRYABLE status=%s pool=%s key=%s retry_after=%s body=%s -> cooldown+exclude",
                           resp.status_code, pool.name, key.id, retry_after,
                           content[:300].decode("utf-8", "ignore"))
            await state.mark_failure(pool.name, key, resp.status_code, content.decode("utf-8", errors="ignore"), retry_after)
            excluded.add(key.id)
            continue
        if resp.status_code in {401, 403}:
            logger.warning("[buffered] direct upstream AUTH status=%s pool=%s key=%s body=%s -> cooldown+exclude",
                           resp.status_code, pool.name, key.id, content[:300].decode("utf-8", "ignore"))
            await state.mark_failure(pool.name, key, resp.status_code, content.decode("utf-8", errors="ignore"))
            excluded.add(key.id)
            continue
        usage = normalize_usage(usage_from_json(content))
        if resp.status_code < 400:
            await state.mark_success(pool.name, key, usage)
        else:
            await state.release_key(pool.name, key)
            state.record_request({"vk": vk, "endpoint": path, "pool": pool.name, "key_id": key.id,
                                  "model": model, "status": resp.status_code, "stream": False,
                                  "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                  **usage})
            return _passthrough_error(content, resp, state.config.expose_rate_limits)
        state.record_request({"vk": vk, "endpoint": path, "pool": pool.name, "key_id": key.id,
                              "model": model, "status": resp.status_code, "stream": False,
                              "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                              **usage})
        hdrs = response_headers(resp.headers, state.config.expose_rate_limits)
        hdrs.pop("content-encoding", None)
        hdrs.pop("Content-Encoding", None)
        return Response(content=content, status_code=resp.status_code,
                        headers=hdrs, media_type=resp.headers.get("content-type"))
    state.record_request({"vk": vk, "endpoint": path, "pool": None, "key_id": None, "model": model,
                          "status": 503, "stream": False, "duration_ms": int((_t.time() - t0) * 1000),
                          "retried": attempts, **normalize_usage(None)})
    logger.warning("[buffered] exhausted all %d attempts; last_error=%s", attempts, last_error)
    return _error_response(last_error, 503, retry_after=30)


async def proxy_stream(request: Request, state: RouterState, pool_names: list[str], path: str,
                       body: bytes, model: str | None, fingerprint: str | None,
                       transport: httpx.AsyncBaseTransport | None, vk: str = "",
                       tunnel_mgr=None):
    import time as _t
    from .tunnel import TunnelError, TunnelManager
    t0 = _t.time()
    excluded: set[str] = set()
    last_error = "all keys cooling down"
    attempts = state.config.max_retries + 1
    for attempt in range(attempts):
        try:
            pool, key, runtime = await state.pick_key(pool_names, model, fingerprint, excluded)
        except PoolExhausted as exc:
            logger.warning("[stream] PoolExhausted pools=%s model=%s excluded=%s reason=%s",
                           pool_names, model, excluded, str(exc))
            return _error_response(str(exc) or "all keys cooling down", 503, retry_after=exc.retry_after)

        # ── Tunnel pool 分支 ──────────────────────────────────────────────────
        if pool.type == "tunnel" and tunnel_mgr is not None:
            tid = key.tunnel_id or key.id
            connected = tunnel_mgr.is_connected(tid)
            logger.info("[stream] attempt=%d pool=%s key=%s tunnel=%s connected=%s model=%s path=%s",
                        attempt, pool.name, key.id, tid, connected, model, path)
            fwd_headers = copy_headers(request, pool.auth_scheme, key.key)
            fwd_headers.pop("anthropic-beta", None)  # 非 Anthropic 上游不识此 header
            try:
                status, resp_headers, body_or_iter = await tunnel_mgr.proxy_request(
                    tid,
                    request.method, path, fwd_headers, body, stream=True,
                    timeout=state.config.total_timeout_stream,
                    params=str(request.url.query) if request.url.query else "",
                )
            except TunnelError as exc:
                # tunnel 未连接/断线是秒级恢复的临时故障：等待 inner 重连后重试同一 key，
                # 不 exclude（单 key 池 exclude 后会立刻 PoolExhausted）、不长冷却。
                last_error = str(exc)
                logger.warning("[stream] TunnelError status=%s msg=%s attempt=%d/%d",
                               exc.status, last_error, attempt, attempts - 1)
                await state.release_key(pool.name, key)
                if exc.status == 503 and attempt < attempts - 1:
                    await asyncio.sleep(1.5)
                    continue
                await state.mark_failure(pool.name, key, exc.status, str(exc))
                excluded.add(key.id)
                continue

            logger.info("[stream] tunnel returned status=%s", status)
            # tunnel 瞬时网关错误（502/503/504）：inner 或其上游秒级抖动。重试同一 key，
            # 绝不冷却——单 key 池冷却会锁死后续 30s 全部请求（这正是 503 反复的根因）。
            if status in {502, 503, 504}:
                logger.warning("[stream] transient %s from tunnel; attempt=%d/%d",
                               status, attempt, attempts - 1)
                await state.release_key(pool.name, key)
                if attempt < attempts - 1:
                    await asyncio.sleep(1.5)
                    continue
                state.record_request({"vk": vk, "endpoint": path, "pool": pool.name, "key_id": key.id,
                                      "model": model, "status": status, "stream": True,
                                      "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                      **normalize_usage(None)})
                return _error_response(f"tunnel upstream returned {status}", 503, retry_after=5)

            # 真实信号（限流/过载/鉴权）或其他 5xx：exclude + 冷却
            if is_retryable(status, b"") or status in {401, 403, 429, 529} or status >= 500:
                logger.warning("[stream] signal status=%s -> cooldown+exclude key=%s", status, key.id)
                await state.mark_failure(pool.name, key, status, f"tunnel status {status}")
                excluded.add(key.id)
                await state.release_key(pool.name, key)
                continue

            pool_name, key_obj = pool.name, key
            state.record_request({"vk": vk, "endpoint": path, "pool": pool_name, "key_id": key_obj.id,
                                  "model": model, "status": status, "stream": True,
                                  "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                                  **normalize_usage(None)})

            async def tunnel_iterator(iter_src) -> AsyncIterator[bytes]:
                try:
                    async for chunk in iter_src:
                        yield chunk
                    await state.mark_success(pool_name, key_obj)
                except TunnelError as exc:
                    await state.mark_failure(pool_name, key_obj, exc.status, str(exc))
                finally:
                    await state.release_key(pool_name, key_obj)

            clean_headers = {k: v for k, v in resp_headers.items()
                             if k.lower() not in {"content-encoding", "transfer-encoding"}}
            return StreamingResponse(
                tunnel_iterator(body_or_iter),
                media_type=resp_headers.get("content-type", "text/event-stream"),
                headers=clean_headers,
            )
        # ── End tunnel branch ─────────────────────────────────────────────────

        url = f"{key.upstream}{upstream_path(path)}"
        headers = copy_headers(request, pool.auth_scheme, key.key)
        logger.info("[stream] direct upstream pool=%s key=%s model=%s -> %s",
                    pool.name, key.id, model, url)
        client = httpx.AsyncClient(timeout=_httpx_timeout(state.config, stream=True), transport=transport)
        try:
            req = client.build_request(request.method, url, content=body, headers=headers, params=request.query_params)
            resp = await client.send(req, stream=True)
        except httpx.HTTPError as exc:
            logger.warning("[stream] direct upstream NETWORK error pool=%s key=%s: %s -> cooldown",
                           pool.name, key.id, exc)
            await state.mark_failure(pool.name, key, None, str(exc))
            excluded.add(key.id)
            await state.release_key(pool.name, key)
            await client.aclose()
            continue
        if is_retryable(resp.status_code, b"") or resp.status_code in {401, 403, 429, 529} or resp.status_code >= 500:
            err_body = await resp.aread()
            await resp.aclose()
            await client.aclose()
            logger.warning("[stream] direct upstream error status=%s pool=%s key=%s body=%s",
                           resp.status_code, pool.name, key.id, err_body[:300].decode("utf-8", "ignore"))
            if is_retryable(resp.status_code, err_body):
                retry_after = float(resp.headers.get("retry-after", "0") or 0) or None
                await state.mark_failure(pool.name, key, resp.status_code, err_body.decode("utf-8", errors="ignore"), retry_after)
                excluded.add(key.id)
                await state.release_key(pool.name, key)
                continue
            if resp.status_code in {401, 403}:
                await state.mark_failure(pool.name, key, resp.status_code, err_body.decode("utf-8", errors="ignore"))
                excluded.add(key.id)
                await state.release_key(pool.name, key)
                continue
            await state.release_key(pool.name, key)
            return Response(content=err_body, status_code=resp.status_code,
                            headers=response_headers(resp.headers, state.config.expose_rate_limits),
                            media_type=resp.headers.get("content-type"))

        pool_name, key_obj = pool.name, key
        state.record_request({"vk": vk, "endpoint": path, "pool": pool_name, "key_id": key_obj.id,
                              "model": model, "status": resp.status_code, "stream": True,
                              "duration_ms": int((_t.time() - t0) * 1000), "retried": attempt,
                              **normalize_usage(None)})

        async def iterator() -> AsyncIterator[bytes]:
            success = False
            try:
                async for chunk in resp.aiter_raw():
                    yield chunk
                success = True
            finally:
                await resp.aclose()
                await client.aclose()
                if success:
                    await state.mark_success(pool_name, key_obj)
                await state.release_key(pool_name, key_obj)

        return StreamingResponse(iterator(), media_type=resp.headers.get("content-type", "text/event-stream"),
                                 headers=response_headers(resp.headers, state.config.expose_rate_limits))
    return _error_response("all keys cooling down", 503, retry_after=30)
