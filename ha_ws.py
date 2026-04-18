from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, Dict, Optional

from helpers import redis_client


def _text(value: Any) -> str:
    return str(value or "").strip()


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    settings = (client or redis_client).hgetall("homeassistant_settings") or {}
    base = _text(settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
    token = _text(settings.get("HA_TOKEN"))
    if required and not token:
        raise ValueError(
            "Home Assistant token is not set. Open WebUI -> Settings -> Home Assistant Settings and add HA_TOKEN."
        )
    return {"base": base, "token": token}


def ws_url(base_url: Any) -> str:
    base = _text(base_url).rstrip("/")
    if base.startswith("https://"):
        return base.replace("https://", "wss://", 1) + "/api/websocket"
    return base.replace("http://", "ws://", 1) + "/api/websocket"


async def _authenticate(ws: Any, token: str, *, timeout_s: float) -> None:
    hello = await ws.receive_json(timeout=timeout_s)
    hello_type = _text(hello.get("type"))
    if hello_type == "auth_required":
        await ws.send_json({"type": "auth", "access_token": token})
        auth = await ws.receive_json(timeout=timeout_s)
        if _text(auth.get("type")) != "auth_ok":
            raise RuntimeError(f"HA websocket auth failed: {auth}")
        return
    if hello_type == "auth_ok":
        return
    raise RuntimeError(f"Unexpected HA websocket hello/auth flow: {hello}")


async def call(base_url: Any, token: Any, payload: Dict[str, Any], *, timeout_s: float = 20.0) -> Any:
    try:
        import aiohttp
    except Exception as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(f"aiohttp is required for Home Assistant websocket calls: {exc}") from exc

    base = _text(base_url).rstrip("/")
    bearer = _text(token)
    if not base:
        raise ValueError("Home Assistant base URL is required.")
    if not bearer:
        raise ValueError("Home Assistant token is required.")

    message = dict(payload or {})
    message_type = _text(message.get("type"))
    if not message_type:
        raise ValueError("Home Assistant websocket payload must include a type.")
    request_id = int(message.get("id") or 1)
    message["id"] = request_id

    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(ws_url(base), heartbeat=30) as ws:
            await _authenticate(ws, bearer, timeout_s=timeout_s)
            await ws.send_json(message)

            loop = asyncio.get_running_loop()
            deadline = loop.time() + float(timeout_s)
            while True:
                remaining = max(0.1, deadline - loop.time())
                if remaining <= 0:
                    break
                msg = await ws.receive(timeout=remaining)
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if _text(data.get("type")) != "result" or int(data.get("id") or 0) != request_id:
                        continue
                    if not data.get("success", False):
                        raise RuntimeError(f"HA websocket call failed: {data}")
                    return data.get("result")
                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break

    raise TimeoutError(f"Timed out waiting for Home Assistant websocket result: {message_type}")


async def entity_registry_list(base_url: Any, token: Any, *, timeout_s: float = 30.0) -> list[dict]:
    result = await call(
        base_url,
        token,
        {"type": "config/entity_registry/list", "id": 1},
        timeout_s=timeout_s,
    )
    return result if isinstance(result, list) else []


async def device_registry_list(base_url: Any, token: Any, *, timeout_s: float = 30.0) -> list[dict]:
    result = await call(
        base_url,
        token,
        {"type": "config/device_registry/list", "id": 1},
        timeout_s=timeout_s,
    )
    return result if isinstance(result, list) else []


async def call_service(
    base_url: Any,
    token: Any,
    *,
    domain: str,
    service: str,
    service_data: Optional[Dict[str, Any]] = None,
    target: Optional[Dict[str, Any]] = None,
    return_response: bool = False,
    timeout_s: float = 20.0,
) -> Any:
    payload: Dict[str, Any] = {
        "type": "call_service",
        "id": 1,
        "domain": _text(domain),
        "service": _text(service),
        "service_data": dict(service_data or {}),
    }
    if isinstance(target, dict) and target:
        payload["target"] = dict(target)
    if return_response:
        payload["return_response"] = True
    return await call(base_url, token, payload, timeout_s=timeout_s)


def _run_sync(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: Dict[str, Any] = {}
    error: Dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - handoff guard
            error["exc"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "exc" in error:
        raise error["exc"]
    return result.get("value")


def entity_registry_list_sync(base_url: Any, token: Any, *, timeout_s: float = 30.0) -> list[dict]:
    return _run_sync(
        entity_registry_list(base_url, token, timeout_s=timeout_s)
    )


def device_registry_list_sync(base_url: Any, token: Any, *, timeout_s: float = 30.0) -> list[dict]:
    return _run_sync(
        device_registry_list(base_url, token, timeout_s=timeout_s)
    )


def call_service_sync(
    base_url: Any,
    token: Any,
    *,
    domain: str,
    service: str,
    service_data: Optional[Dict[str, Any]] = None,
    target: Optional[Dict[str, Any]] = None,
    return_response: bool = False,
    timeout_s: float = 20.0,
) -> Any:
    return _run_sync(
        call_service(
            base_url,
            token,
            domain=domain,
            service=service,
            service_data=service_data,
            target=target,
            return_response=return_response,
            timeout_s=timeout_s,
        )
    )
