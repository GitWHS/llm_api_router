"""
Reverse-tunnel support for llm_api_router.

Architecture:
  inner (firewall) ──WS──► relay ◄── external HTTP clients
  inner holds real LLM keys; relay exposes them via tunnel pool.

TunnelManager  – lives in relay, receives WS connections from inner nodes,
                 dispatches inbound HTTP requests over the WS and streams
                 responses back to FastAPI callers.

TunnelClient   – lives in inner, connects to relay's /tunnel/connect,
                 receives request frames, executes them locally via httpx,
                 sends response frames back.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
import time
import uuid
from typing import Any, AsyncIterator

import httpx

logger = logging.getLogger("llm_api_router.tunnel")

# ── Frame protocol ────────────────────────────────────────────────────────────
# All frames are JSON text messages except stream-chunk binary frames.
#
# relay → inner:
#   {"type":"request","req_id":"<uuid>","method":"POST","path":"/v1/messages",
#    "headers":{...},"body_b64":"<base64>","stream":true}
#   {"type":"cancel","req_id":"<uuid>"}
#
# inner → relay:
#   {"type":"response","req_id":"<uuid>","status":200,"headers":{...}}
#   {"type":"body","req_id":"<uuid>","body_b64":"<base64>"}   (non-stream)
#   binary: [4-byte req_id_tag (uint32 BE)][chunk bytes]      (stream chunks)
#   {"type":"done","req_id":"<uuid>"}
#   {"type":"error","req_id":"<uuid>","message":"...","status":502}
# ─────────────────────────────────────────────────────────────────────────────

PING_INTERVAL = 20      # seconds inner sends WS pings
PING_TIMEOUT  = 45      # seconds relay waits before evicting dead connection
REQUEST_TIMEOUT = 1200  # seconds per request (stream)


class TunnelError(RuntimeError):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


# ── Relay side ────────────────────────────────────────────────────────────────

class _PendingRequest:
    """Holds state for one in-flight proxied request on the relay side."""

    def __init__(self, req_id: str, stream: bool):
        self.req_id = req_id
        self.stream = stream
        # non-stream: set when response frame + body frame received
        self.response_event: asyncio.Event = asyncio.Event()
        self.status: int = 0
        self.headers: dict[str, str] = {}
        self.body: bytes = b""
        self.error: str | None = None
        # stream: Queue of chunks; None sentinel = done
        self.chunk_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=512)


class TunnelConnection:
    """Wraps a single WebSocket connection from an inner node."""

    def __init__(self, tunnel_id: str, websocket):
        self.tunnel_id = tunnel_id
        self.ws = websocket
        self._pending: dict[str, _PendingRequest] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._last_seen = time.time()

    def touch(self) -> None:
        self._last_seen = time.time()

    def is_stale(self) -> bool:
        return time.time() - self._last_seen > PING_TIMEOUT

    async def send_request(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        stream: bool,
        timeout: float = REQUEST_TIMEOUT,
        params: str = "",
    ) -> tuple[int, dict[str, str], bytes | AsyncIterator[bytes]]:
        """Send an HTTP request over this WS tunnel and return (status, headers, body_or_iterator)."""
        req_id = uuid.uuid4().hex
        pending = _PendingRequest(req_id, stream)
        async with self._lock:
            self._pending[req_id] = pending

        frame = {
            "type": "request",
            "req_id": req_id,
            "method": method,
            "path": path,
            "headers": headers,
            "body_b64": base64.b64encode(body).decode(),
            "stream": stream,
            "params": params,
        }
        try:
            await self.ws.send_text(json.dumps(frame))
        except Exception as exc:
            async with self._lock:
                self._pending.pop(req_id, None)
            raise TunnelError(f"ws send failed: {exc}")

        if not stream:
            # Wait for full response
            try:
                await asyncio.wait_for(pending.response_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                async with self._lock:
                    self._pending.pop(req_id, None)
                await self._try_cancel(req_id)
                raise TunnelError("tunnel request timed out", 504)
            async with self._lock:
                self._pending.pop(req_id, None)
            if pending.error:
                raise TunnelError(pending.error, pending.status or 502)
            return pending.status, pending.headers, pending.body
        else:
            # Return an async generator that yields chunks from the queue
            async def _iter() -> AsyncIterator[bytes]:
                try:
                    deadline = time.time() + timeout
                    while True:
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            await self._try_cancel(req_id)
                            raise TunnelError("tunnel stream timed out", 504)
                        try:
                            chunk = await asyncio.wait_for(
                                pending.chunk_queue.get(), timeout=min(remaining, 600)
                            )
                        except asyncio.TimeoutError:
                            await self._try_cancel(req_id)
                            raise TunnelError("tunnel stream idle timeout", 504)
                        if chunk is None:
                            break
                        if isinstance(chunk, TunnelError):
                            raise chunk
                        yield chunk
                finally:
                    async with self._lock:
                        self._pending.pop(req_id, None)

            # Wait for response headers first (they come before chunks)
            try:
                await asyncio.wait_for(pending.response_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                async with self._lock:
                    self._pending.pop(req_id, None)
                await self._try_cancel(req_id)
                raise TunnelError("tunnel stream header timeout", 504)
            if pending.error:
                async with self._lock:
                    self._pending.pop(req_id, None)
                raise TunnelError(pending.error, pending.status or 502)
            return pending.status, pending.headers, _iter()

    async def _try_cancel(self, req_id: str) -> None:
        try:
            await self.ws.send_text(json.dumps({"type": "cancel", "req_id": req_id}))
        except Exception:
            pass

    async def dispatch_message(self, message) -> None:
        """Called by the WS reader loop for each incoming message from inner."""
        self.touch()
        if isinstance(message, bytes):
            # Binary stream chunk: [4-byte tag BE][data]
            if len(message) < 4:
                return
            tag = struct.unpack(">I", message[:4])[0]
            chunk = message[4:]
            # find pending by tag (lower 32 bits of req_id hash)
            async with self._lock:
                matched = None
                for req_id, p in self._pending.items():
                    if _req_tag(req_id) == tag:
                        matched = p
                        break
            if matched and matched.stream:
                await matched.chunk_queue.put(chunk)
            return

        # Text frame
        try:
            frame = json.loads(message)
        except Exception:
            return
        ftype = frame.get("type")
        req_id = frame.get("req_id", "")

        if ftype == "pong":
            # app-level pong from inner
            return

        async with self._lock:
            pending = self._pending.get(req_id)

        if ftype == "response":
            if pending:
                pending.status = int(frame.get("status", 200))
                pending.headers = frame.get("headers", {})
                if pending.stream:
                    # 流式：response 帧给出 headers 后立即 unblock relay（它会开始迭代 chunks）
                    pending.response_event.set()
                # 非流式：等 body 帧再 set，避免 relay 在 body 尚未到达时就返回空 body
        elif ftype == "body":
            if pending and not pending.stream:
                pending.body = base64.b64decode(frame.get("body_b64", ""))
                pending.response_event.set()
        elif ftype == "done":
            if pending and pending.stream:
                await pending.chunk_queue.put(None)
        elif ftype == "error":
            if pending:
                pending.status = int(frame.get("status", 502))
                pending.error = frame.get("message", "tunnel error")
                if pending.stream:
                    err = TunnelError(pending.error, pending.status)
                    await pending.chunk_queue.put(err)  # type: ignore[arg-type]
                pending.response_event.set()

    def close(self) -> None:
        self._closed = True
        # Unblock all pending requests
        for pending in list(self._pending.values()):
            if not pending.response_event.is_set():
                pending.error = "tunnel connection closed"
                pending.status = 503
                pending.response_event.set()
            if pending.stream:
                asyncio.get_event_loop().call_soon_threadsafe(
                    pending.chunk_queue.put_nowait, None
                )


def _req_tag(req_id: str) -> int:
    """Lower 32 bits used to route binary chunk frames."""
    return int(req_id[:8], 16) & 0xFFFFFFFF


class TunnelManager:
    """Relay-side manager: holds tunnel connections and dispatches requests."""

    def __init__(self, token: str | None = None):
        self._token = token
        self._conns: dict[str, TunnelConnection] = {}
        self._lock = asyncio.Lock()

    def is_connected(self, tunnel_id: str) -> bool:
        conn = self._conns.get(tunnel_id)
        return conn is not None and not conn._closed

    async def handle_ws(self, websocket, tunnel_id: str, token: str | None) -> None:
        """FastAPI WebSocket handler for /tunnel/connect."""
        if self._token and token != self._token:
            await websocket.close(code=4001, reason="unauthorized")
            return

        await websocket.accept()
        conn = TunnelConnection(tunnel_id, websocket)

        async with self._lock:
            old = self._conns.get(tunnel_id)
            if old:
                old.close()
            self._conns[tunnel_id] = conn

        logger.info("tunnel connected: %s", tunnel_id)
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(websocket.receive(), timeout=PING_TIMEOUT + 5)
                except asyncio.TimeoutError:
                    # 发送 WS ping 检测活跃性（FastAPI/Starlette 底层 ping，非 app-level）
                    try:
                        await websocket.send_text('{"type":"ping"}')
                        conn.touch()
                        continue
                    except Exception:
                        logger.warning("tunnel %s: ping failed, closing", tunnel_id)
                        break
                mtype = msg.get("type")
                if mtype == "websocket.disconnect":
                    break
                if mtype == "websocket.receive":
                    data = msg.get("bytes") or msg.get("text")
                    if data is not None:
                        await conn.dispatch_message(data)
        except Exception as exc:
            logger.warning("tunnel %s error: %s", tunnel_id, exc)
        finally:
            conn.close()
            async with self._lock:
                if self._conns.get(tunnel_id) is conn:
                    del self._conns[tunnel_id]
            logger.info("tunnel disconnected: %s", tunnel_id)

    async def proxy_request(
        self,
        tunnel_id: str,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        stream: bool,
        timeout: float = REQUEST_TIMEOUT,
        params: str = "",
    ) -> tuple[int, dict[str, str], bytes | AsyncIterator[bytes]]:
        async with self._lock:
            conn = self._conns.get(tunnel_id)
        if conn is None or conn._closed:
            raise TunnelError(f"tunnel '{tunnel_id}' not connected", 503)
        return await conn.send_request(method, path, headers, body, stream, timeout, params=params)


# ── Inner (client) side ───────────────────────────────────────────────────────

class TunnelClient:
    """
    Runs on the inner (firewall-behind) node.
    Connects to relay's /tunnel/connect WS, receives request frames,
    executes them via httpx against forward_url, sends back response frames.
    """

    def __init__(
        self,
        relay_ws_url: str,
        tunnel_id: str,
        token: str,
        forward_url: str,
        forward_vk: str = "",
        reconnect_max: float = 60.0,
    ):
        self._relay_ws_url = relay_ws_url  # e.g. ws://relay:4000/tunnel/connect
        self._tunnel_id = tunnel_id
        self._token = token
        self._forward_url = forward_url.rstrip("/")
        self._forward_vk = forward_vk      # 注入到转发请求中的 vk（用于认证 inner router）
        self._reconnect_max = reconnect_max
        self._running = False

    async def run(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                await self._connect_and_serve()
                delay = 1.0  # successful run → reset backoff
            except Exception as exc:
                logger.warning("[inner] connection lost: %s — reconnecting in %.0fs", exc, delay)
            if not self._running:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._reconnect_max)

    def stop(self) -> None:
        self._running = False

    async def _connect_and_serve(self) -> None:
        import websockets  # type: ignore[import]
        url = f"{self._relay_ws_url}?tunnel_id={self._tunnel_id}&token={self._token}"
        logger.info("[inner] connecting to relay %s (tunnel_id=%s, forward=%s)",
                    self._relay_ws_url, self._tunnel_id, self._forward_url)
        async with websockets.connect(url, ping_interval=PING_INTERVAL, ping_timeout=30) as ws:
            logger.info("[inner] CONNECTED as '%s' — ready to serve tunnel requests", self._tunnel_id)
            # Serve requests concurrently
            pending_tasks: set[asyncio.Task] = set()
            try:
                async for raw in ws:
                    if isinstance(raw, bytes):
                        continue  # inner never receives binary
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        continue
                    ftype = frame.get("type")
                    if ftype == "ping":
                        await _ws_send_safe(ws, '{"type":"pong"}')
                        continue
                    if ftype == "request":
                        task = asyncio.create_task(
                            self._handle_request(ws, frame),
                            name=f"tunnel-req-{frame.get('req_id', '')[:8]}",
                        )
                        pending_tasks.add(task)
                        task.add_done_callback(pending_tasks.discard)
                    elif ftype == "cancel":
                        pass
            finally:
                for t in pending_tasks:
                    t.cancel()

    async def _handle_request(self, ws, frame: dict) -> None:
        req_id: str = frame.get("req_id", "")
        method: str = frame.get("method", "POST")
        path: str = frame.get("path", "/")
        headers: dict[str, str] = frame.get("headers", {})
        body_b64: str = frame.get("body_b64", "")
        do_stream: bool = frame.get("stream", False)
        params: str = frame.get("params", "")

        try:
            body = base64.b64decode(body_b64)
        except Exception:
            body = b""

        # 提取 model 用于日志（不影响转发）
        model = "?"
        try:
            model = (json.loads(body.decode("utf-8")) or {}).get("model", "?")
        except Exception:
            pass

        url = self._forward_url + path
        if params:
            url += "?" + params.lstrip("?")
        logger.info("[inner] recv req=%s %s model=%s stream=%s -> %s (%d bytes)",
                    req_id[:8], method, model, do_stream, url, len(body))
        # Remove hop-by-hop + Anthropic-specific headers before forwarding
        fwd_headers = {
            k: v for k, v in headers.items()
            if k.lower() not in {"host", "connection", "transfer-encoding", "content-length", "anthropic-beta"}
        }
        # 若 inner 侧需要独立认证（forward_vk 已配置），则覆盖 x-api-key/authorization
        if self._forward_vk:
            fwd_headers.pop("authorization", None)
            fwd_headers.pop("Authorization", None)
            fwd_headers["x-api-key"] = self._forward_vk

        try:
            if do_stream:
                await self._handle_stream_request(ws, req_id, method, url, fwd_headers, body)
            else:
                await self._handle_buffered_request(ws, req_id, method, url, fwd_headers, body)
        except Exception as exc:
            logger.warning("[inner] req=%s forward FAILED: %s -> returning 502", req_id[:8], exc)
            await _ws_send_safe(ws, json.dumps({
                "type": "error", "req_id": req_id,
                "message": str(exc), "status": 502,
            }))

    async def _handle_buffered_request(
        self, ws, req_id: str, method: str, url: str,
        headers: dict[str, str], body: bytes,
    ) -> None:
        async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
            resp = await client.request(method, url, content=body, headers=headers)
            content = await resp.aread()

        if resp.status_code >= 400:
            snippet = content[:300].decode("utf-8", "ignore")
            logger.warning("[inner] req=%s upstream status=%s body=%s",
                           req_id[:8], resp.status_code, snippet)
        else:
            logger.info("[inner] req=%s upstream status=%s (%d bytes) OK",
                        req_id[:8], resp.status_code, len(content))
        resp_headers = {k: v for k, v in resp.headers.items()
                        if k.lower() not in {"transfer-encoding", "connection"}}
        await _ws_send_safe(ws, json.dumps({
            "type": "response", "req_id": req_id,
            "status": resp.status_code, "headers": resp_headers,
        }))
        await _ws_send_safe(ws, json.dumps({
            "type": "body", "req_id": req_id,
            "body_b64": base64.b64encode(content).decode(),
        }))

    async def _handle_stream_request(
        self, ws, req_id: str, method: str, url: str,
        headers: dict[str, str], body: bytes,
    ) -> None:
        tag = _req_tag(req_id)
        tag_bytes = struct.pack(">I", tag)
        nchunks = 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT, read=None)) as client:
            async with client.stream(method, url, content=body, headers=headers) as resp:
                if resp.status_code >= 400:
                    err = await resp.aread()
                    logger.warning("[inner] req=%s stream upstream status=%s body=%s",
                                   req_id[:8], resp.status_code, err[:300].decode("utf-8", "ignore"))
                    resp_headers = {k: v for k, v in resp.headers.items()
                                    if k.lower() not in {"transfer-encoding", "connection"}}
                    await _ws_send_safe(ws, json.dumps({
                        "type": "response", "req_id": req_id,
                        "status": resp.status_code, "headers": resp_headers,
                    }))
                    await _ws_send_safe(ws, json.dumps({"type": "body", "req_id": req_id,
                                                        "body_b64": base64.b64encode(err).decode()}))
                    await _ws_send_safe(ws, json.dumps({"type": "done", "req_id": req_id}))
                    return
                logger.info("[inner] req=%s stream upstream status=%s, streaming...",
                            req_id[:8], resp.status_code)
                resp_headers = {k: v for k, v in resp.headers.items()
                                if k.lower() not in {"transfer-encoding", "connection"}}
                await _ws_send_safe(ws, json.dumps({
                    "type": "response", "req_id": req_id,
                    "status": resp.status_code, "headers": resp_headers,
                }))
                async for chunk in resp.aiter_raw():
                    if chunk:
                        nchunks += 1
                        await ws.send(tag_bytes + chunk)

        logger.info("[inner] req=%s stream done (%d chunks)", req_id[:8], nchunks)
        await _ws_send_safe(ws, json.dumps({"type": "done", "req_id": req_id}))


async def _ws_send_safe(ws, text: str) -> None:
    try:
        await ws.send(text)
    except Exception as exc:
        logger.debug("ws send_safe error: %s", exc)
