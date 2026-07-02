from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx
import uvicorn

from .app import create_app
from .config import (build_path_map, load_config, load_pools, mask_secret)


ENDPOINT_LABELS = {
    "/anthropic/v1/messages": "Anthropic Messages (Claude Code)",
    "/anthropic/v1/messages/count_tokens": "Anthropic count_tokens",
    "/anthropic/v1/models": "模型列表 (Anthropic)",
    "/v1/responses": "OpenAI Responses (Codex 默认)",
    "/v1/chat/completions": "OpenAI Chat (Codex chat / OpenAI SDK)",
    "/v1/models": "模型列表 (OpenAI)",
}


def print_info(config_path: Path, keys_path: Path) -> None:
    cfg = load_config(config_path)
    pools = load_pools(keys_path)
    path_map = build_path_map(pools, cfg.endpoints_override)
    base = f"http://{cfg.host}:{cfg.port}"
    line = "=" * 64

    print(line)
    print("  LLM API Router  ·  服务信息 (start banner)")
    print(line)
    print(f"监听地址:    {base}")
    print(f"管理面 UI:   {base}/ui/    (需 admin_token)")
    if cfg.admin_token:
        print(f"admin_token: 已设置  ({mask_secret(cfg.admin_token)})")
    else:
        print("admin_token: 未设置  → /admin/* 全部 403（管理面禁用）")
    print(f"调度策略:    {cfg.strategy}  (cache_ttl={cfg.cache_ttl_seconds}s, fallback={cfg.fallback_strategy})")

    # ── 对外 API Key (vk) ──
    print("\n── 对外 API Key (virtual key，发给 claude/codex 客户端) " + "─" * 8)
    if not cfg.virtual_keys:
        print("  (无 vk！客户端无法调用，请在 config.yaml virtual_keys 添加)")
    for vk, allowed in cfg.virtual_keys.items():
        scope = "全部池" if not allowed else ", ".join(sorted(allowed))
        print(f"  {vk}    可访问: {scope}")

    # ── 对外端点 ──
    print("\n── 对外 API Endpoint (按 pool.type 自动路由) " + "─" * 12)
    for path in sorted(path_map):
        label = ENDPOINT_LABELS.get(path, "")
        served = ", ".join(path_map[path])
        print(f"  {base}{path}")
        print(f"      {label}   ← 池: {served}")

    # ── 池与真实资源 ──
    print("\n── 池与真实资源 (真实 key 已脱敏) " + "─" * 22)
    all_models: set[str] = set()
    unconstrained = False
    for name, p in pools.items():
        healthy = sum(1 for k in p.keys if not k.disabled)
        print(f"  ● {name}   type={p.type}  auth={p.auth_scheme}  keys={len(p.keys)}(健康{healthy})")
        for k in p.keys:
            tag = " [disabled]" if k.disabled else ""
            conc = f"  max_concurrent={k.max_concurrent}" if k.max_concurrent is not None else "  max_concurrent=无上限"
            models = ", ".join(k.support_models) if k.support_models else "(不限/全部)"
            if k.support_models:
                all_models.update(k.support_models)
            else:
                unconstrained = True
            print(f"        - {k.id}{tag}  key={mask_secret(k.key)}  weight={k.weight}{conc}")
            print(f"            upstream: {k.upstream}")
            print(f"            models:   {models}")

    # ── 支持的模型 ──
    print("\n── 支持的模型 (所有池声明的并集) " + "─" * 22)
    if all_models:
        print("  " + ", ".join(sorted(all_models)))
    if unconstrained:
        print("  (另有 key 未声明 support_models = 接受任意模型，透传由上游决定)")
    if not all_models and not unconstrained:
        print("  (无)")

    # ── 接入示例 ──
    has_anthropic = any(p.type == "anthropic" for p in pools.values())
    has_responses = any(p.type == "openai_responses" for p in pools.values())
    has_chat = any(p.type == "openai_chat" for p in pools.values())
    first_vk = next(iter(cfg.virtual_keys), "vk-xxx")
    print("\n── 接入示例 " + "─" * 40)
    if has_anthropic:
        print("[Claude Code]")
        print(f'  $env:ANTHROPIC_BASE_URL  = "{base}/anthropic"')
        print(f'  $env:ANTHROPIC_AUTH_TOKEN = "{first_vk}"')
        print("  claude")
    if has_responses or has_chat:
        wire = "responses" if has_responses else "chat"
        print("[Codex]  ~/.codex/config.toml")
        print("  [model_providers.router]")
        print(f'  base_url = "{base}/v1"')
        print(f'  wire_api = "{wire}"')
        print('  env_key  = "OPENAI_API_KEY"')
        print(f'  然后:  $env:OPENAI_API_KEY = "{first_vk}"  ;  codex --profile router')
    print(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM API Router")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--keys", default="keys.yaml")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("serve")
    sub.add_parser("stats")
    sub.add_parser("validate")
    sub.add_parser("reload")
    sub.add_parser("info")
    # tunnel-client 子命令
    tc_parser = sub.add_parser("tunnel-client", help="启动反向隧道客户端（内网设备连接外网 relay）")
    tc_parser.add_argument("--relay-url", required=True,
                           help="relay 的 WebSocket 地址，例如 ws://relay:4000/tunnel/connect")
    tc_parser.add_argument("--tunnel-id", required=True,
                           help="此内网节点的唯一标识，需与 relay keys.yaml 中的 key.id 对应")
    tc_parser.add_argument("--token", default="",
                           help="relay 侧 config.yaml 中配置的 tunnel_token")
    tc_parser.add_argument("--forward-url", default="http://127.0.0.1:4000",
                           help="将请求转发到的本地地址（内网本地 llm_api_router 或直接上游）")
    tc_parser.add_argument("--forward-vk", default="",
                           help="转发到内网 router 时使用的 virtual key（用于认证内网 router，若不填则透传原始 key）")
    args = parser.parse_args()
    args.cmd = args.cmd or "serve"

    if args.cmd == "info":
        print_info(Path(args.config), Path(args.keys))
        return

    if args.cmd == "serve":
        config = load_config(Path(args.config))
        try:
            print_info(Path(args.config), Path(args.keys))
        except Exception:
            pass
        app = create_app(args.config, args.keys)
        uvicorn.run(app, host=config.host, port=config.port)
        return

    if args.cmd == "validate":
        try:
            pools = load_pools(Path(args.keys))
            cfg = load_config(Path(args.config))
            for name, p in pools.items():
                if not p.keys:
                    print(f"[WARN] pool {name}: no keys")
            print(f"[OK] {len(pools)} pool(s): {', '.join(pools)}")
            print(f"[OK] {len(cfg.virtual_keys)} virtual key(s); strategy={cfg.strategy}")
        except Exception as exc:
            print(f"[ERROR] {exc}")
            raise SystemExit(1)
        return

    if args.cmd == "reload":
        config = load_config(Path(args.config))
        url = f"http://{config.host}:{config.port}/admin/reload"
        token = config.admin_token or ""
        resp = httpx.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        print(resp.text)
        return

    if args.cmd == "stats":
        config = load_config(Path(args.config))
        url = f"http://{config.host}:{config.port}/stats"
        resp = httpx.get(url, timeout=10)
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
        return

    if args.cmd == "tunnel-client":
        import asyncio as _asyncio
        import logging as _logging
        from .tunnel import TunnelClient
        # 配置日志输出到控制台，让 [inner] 请求/转发/上游状态可见
        _root = _logging.getLogger("llm_api_router")
        if not _root.handlers:
            _h = _logging.StreamHandler()
            _h.setFormatter(_logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
            _root.addHandler(_h)
        _root.setLevel(_logging.INFO)
        _root.propagate = False
        client = TunnelClient(
            relay_ws_url=args.relay_url,
            tunnel_id=args.tunnel_id,
            token=args.token,
            forward_url=args.forward_url,
            forward_vk=args.forward_vk,
        )
        print(f"[tunnel-client] 启动: tunnel_id={args.tunnel_id}")
        print(f"  relay      : {args.relay_url}")
        print(f"  forward    : {args.forward_url}")
        print(f"  forward-vk : {'(已设置)' if args.forward_vk else '(未设置，透传原始 key)'}")
        print(f"  token      : {'(已设置)' if args.token else '(未设置)'}")
        print("  Ctrl+C 停止")
        try:
            _asyncio.run(client.run())
        except KeyboardInterrupt:
            print("\n[tunnel-client] 已停止")
        return


if __name__ == "__main__":
    main()
