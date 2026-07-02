import asyncio
import json

import httpx
from fastapi.testclient import TestClient

from llm_api_router.app import create_app


def write_config(tmpdir, *, anthropic_auth="x-api-key"):
    config = tmpdir.join("config.yaml")
    keys = tmpdir.join("keys.yaml")
    state = tmpdir.join("state.json")
    config.write(
        f"""
listen: {{ host: 127.0.0.1, port: 4000 }}
virtual_keys:
  - {{ key: vk-test, pools: [anthropic, openai] }}
strategy: cache_affinity_ttl
cache_ttl_seconds: 300
default_cooldown_seconds: 60
max_retries: 2
state_file: "{str(state).replace(chr(92), '/')}"
admin_token: adm
"""
    )
    keys.write(
        f"""
pools:
  anthropic:
    type: anthropic
    auth_scheme: {anthropic_auth}
    keys:
      - {{ id: a1, key: real-a1, upstream: https://anthropic.local, support_models: [claude-x] }}
      - {{ id: a2, key: real-a2, upstream: https://anthropic.local, support_models: [claude-x] }}
  openai:
    type: openai_responses
    keys:
      - {{ id: o1, key: real-o1, upstream: https://openai.local }}
"""
    )
    return str(config), str(keys)


def test_auth_and_header_rewrite(tmpdir):
    config, keys = write_config(tmpdir)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["x-api-key"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"ok": True, "usage": {"input_tokens": 2, "output_tokens": 3}})

    app = create_app(config, keys, transport=httpx.MockTransport(handler))
    client = TestClient(app)

    resp = client.post("/anthropic/v1/messages", headers={"x-api-key": "vk-test"}, json={"model": "claude-x"})

    assert resp.status_code == 200
    assert seen["url"] == "https://anthropic.local/v1/messages"
    assert seen["x-api-key"] == "real-a1"
    stats = client.get("/stats").json()
    assert stats["pools"]["anthropic"]["keys"]["a1"]["prompt_tokens"] == 2


def test_bearer_auth_scheme_for_anthropic(tmpdir):
    """Ark-style upstreams use Authorization: Bearer even on anthropic protocol."""
    config, keys = write_config(tmpdir, anthropic_auth="bearer")
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["authorization"] = request.headers.get("authorization")
        seen["x-api-key"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"ok": True})

    app = create_app(config, keys, transport=httpx.MockTransport(handler))
    client = TestClient(app)
    resp = client.post("/anthropic/v1/messages", headers={"x-api-key": "vk-test"}, json={"model": "claude-x"})
    assert resp.status_code == 200
    assert seen["authorization"] == "Bearer real-a1"
    assert seen["x-api-key"] is None


def test_failover_on_429(tmpdir):
    config, keys = write_config(tmpdir)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers.get("x-api-key"))
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "5"}, json={"error": "rate limited"})
        return httpx.Response(200, json={"ok": True})

    app = create_app(config, keys, transport=httpx.MockTransport(handler))
    client = TestClient(app)
    # 非 stream，触发 buffered 重试
    resp = client.post("/anthropic/v1/messages", headers={"Authorization": "Bearer vk-test"}, json={"model": "claude-x"})
    assert resp.status_code == 200
    assert calls == ["real-a1", "real-a2"]
    stats = client.get("/stats").json()
    assert stats["pools"]["anthropic"]["keys"]["a1"]["cooldown_until"] > 0


def test_openai_authorization_rewrite(tmpdir):
    config, keys = write_config(tmpdir)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["authorization"] = request.headers.get("authorization")
        seen["x-api-key"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"id": "resp_1"})

    app = create_app(config, keys, transport=httpx.MockTransport(handler))
    client = TestClient(app)
    resp = client.post("/v1/responses", headers={"Authorization": "Bearer vk-test"}, json={"model": "gpt-x"})
    assert resp.status_code == 200
    assert seen["authorization"] == "Bearer real-o1"
    assert seen["x-api-key"] is None


def test_rejects_unknown_virtual_key(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    client = TestClient(app)
    resp = client.post("/v1/responses", headers={"Authorization": "Bearer bad"}, json={"model": "gpt-x"})
    assert resp.status_code == 403


def test_model_filter_skips_unsupported_key(tmpdir):
    """A key whose support_models doesn't include the request model is skipped."""
    config = tmpdir.join("config.yaml")
    keys = tmpdir.join("keys.yaml")
    state = tmpdir.join("state.json")
    config.write(
        f"""
listen: {{ host: 127.0.0.1, port: 4000 }}
virtual_keys:
  - {{ key: vk-test, pools: [] }}
strategy: round_robin
state_file: "{str(state).replace(chr(92), '/')}"
"""
    )
    keys.write(
        """
pools:
  anthropic:
    type: anthropic
    keys:
      - { id: a1, key: real-a1, upstream: https://anthropic.local, support_models: [sonnet] }
      - { id: a2, key: real-a2, upstream: https://anthropic.local, support_models: [opus] }
"""
    )
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["key"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"ok": True})

    app = create_app(str(config), str(keys), transport=httpx.MockTransport(handler))
    client = TestClient(app)
    resp = client.post("/anthropic/v1/messages", headers={"x-api-key": "vk-test"}, json={"model": "opus"})
    assert resp.status_code == 200
    assert seen["key"] == "real-a2"  # a1 (sonnet-only) skipped


def test_v1_models_aggregation(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = TestClient(app)
    # /anthropic/v1/models 由 anthropic 池聚合（a1/a2 support_models=[claude-x]）
    resp = client.get("/anthropic/v1/models", headers={"Authorization": "Bearer vk-test"})
    assert resp.status_code == 200
    ids = {m["id"] for m in resp.json()["data"]}
    assert "claude-x" in ids


def _admin(client, method, path, **kw):
    return getattr(client, method)(path, headers={"X-Admin-Token": "adm"}, **kw)


def test_admin_requires_token(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = TestClient(app)
    assert client.get("/admin/pools").status_code == 403          # 无 token
    assert _admin(client, "get", "/admin/pools").status_code == 200  # 有 token


def test_admin_vk_crud(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = TestClient(app)
    # 新增 vk
    r = _admin(client, "post", "/admin/vk", json={"key": "vk-new", "pools": ["anthropic"]})
    assert r.status_code == 200
    keys_after = {v["key"] for v in _admin(client, "get", "/admin/vk").json()["virtual_keys"]}
    assert "vk-new" in keys_after
    # 新 vk 立即可用（热加载）
    seen = {}
    def handler(request):
        seen["k"] = request.headers.get("x-api-key"); return httpx.Response(200, json={"ok": True})
    app2 = create_app(config, keys, transport=httpx.MockTransport(handler))
    c2 = TestClient(app2)
    assert c2.post("/anthropic/v1/messages", headers={"x-api-key": "vk-new"}, json={"model": "claude-x"}).status_code == 200
    # 删除 vk
    assert _admin(client, "delete", "/admin/vk/vk-new").status_code == 200
    keys_after2 = {v["key"] for v in _admin(client, "get", "/admin/vk").json()["virtual_keys"]}
    assert "vk-new" not in keys_after2


def test_admin_key_disable_enable(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True})))
    client = TestClient(app)
    # 禁用 a1 → 该 key 被跳过
    assert _admin(client, "post", "/admin/keys/anthropic/a1/disable").status_code == 200
    pools = _admin(client, "get", "/admin/pools").json()
    a1 = next(k for k in pools["anthropic"]["keys"] if k["id"] == "a1")
    assert a1["disabled"] is True
    # 启用回来
    assert _admin(client, "post", "/admin/keys/anthropic/a1/enable").status_code == 200
    pools2 = _admin(client, "get", "/admin/pools").json()
    a1b = next(k for k in pools2["anthropic"]["keys"] if k["id"] == "a1")
    assert a1b["disabled"] is False


def test_admin_logs_and_usage(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True, "usage": {"input_tokens": 5, "output_tokens": 7}})))
    client = TestClient(app)
    client.post("/anthropic/v1/messages", headers={"x-api-key": "vk-test"}, json={"model": "claude-x"})
    logs = _admin(client, "get", "/admin/logs/recent?limit=10").json()
    assert logs["count"] >= 1
    assert logs["logs"][0]["endpoint"] == "/anthropic/v1/messages"
    assert logs["logs"][0]["status"] == 200
    assert logs["logs"][0]["prompt_tokens"] == 5
    assert logs["logs"][0]["completion_tokens"] == 7
    assert logs["logs"][0]["total_tokens"] == 12
    usage = _admin(client, "get", "/admin/usage?window=1h").json()
    assert usage["total"] >= 1 and usage["ok"] >= 1
    assert usage["prompt_tokens"] == 5
    assert usage["completion_tokens"] == 7
    assert usage["total_tokens"] == 12
    assert usage["by_vk"]["vk-test"]["total_tokens"] == 12
    assert usage["by_pool"]["anthropic"]["requests"] == 1
    assert usage["by_key"]["anthropic::a1"]["prompt_tokens"] == 5
    assert usage["by_model"]["claude-x"]["completion_tokens"] == 7
    assert usage["by_endpoint"]["/anthropic/v1/messages"]["requests"] == 1
    assert usage["by_status"]["200"] == 1
    assert usage["latency"]["p95_ms"] >= 0
    assert len(usage["series"]) == 12


def test_admin_routes_matrix(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = TestClient(app)
    routes = _admin(client, "get", "/admin/routes").json()["routes"]
    messages = next(r for r in routes if r["path"] == "/anthropic/v1/messages")
    assert messages["pool_names"] == ["anthropic"]
    assert messages["key_count"] == 2
    assert messages["available_keys"] == 2
    assert "claude-x" in messages["models"]
    assert "vk-test" in messages["virtual_keys"]

    responses = next(r for r in routes if r["path"] == "/v1/responses")
    assert responses["pool_names"] == ["openai"]


def test_admin_config_masks_secrets(tmpdir):
    config, keys = write_config(tmpdir)
    app = create_app(config, keys, transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = TestClient(app)
    cfg = _admin(client, "get", "/admin/config").json()
    # 真实 key real-a1 不应明文出现
    assert "real-a1" not in cfg["keys_yaml"]
    assert "adm" not in cfg["config_yaml"] or "admin_token" in cfg["config_yaml"]  # token 被脱敏
