from __future__ import annotations

import asyncio
import io
import json
import threading
from typing import Any, Dict, Optional

import requests

from helpers import redis_client

HOMEASSISTANT_SETTINGS_KEY = "homeassistant_settings"
HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"

INTEGRATION = {
    "id": "homeassistant",
    "name": "Home Assistant",
    "description": "Shared Home Assistant endpoint and token for portals, verbas, and announcement paths.",
    "badge": "HA",
    "order": 10,
    "fields": [
        {
            "key": "homeassistant_base_url",
            "label": "Base URL",
            "type": "text",
            "default": HOMEASSISTANT_DEFAULT_BASE_URL,
            "placeholder": HOMEASSISTANT_DEFAULT_BASE_URL,
        },
        {
            "key": "homeassistant_token",
            "label": "Long-Lived Access Token",
            "type": "password",
            "default": "",
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test Home Assistant",
            "status": "Checks the Home Assistant REST API with the current form values.",
        },
    ],
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    settings = (client or redis_client).hgetall(HOMEASSISTANT_SETTINGS_KEY) or {}
    base = _text(settings.get("HA_BASE_URL") or HOMEASSISTANT_DEFAULT_BASE_URL).rstrip("/")
    token = _text(settings.get("HA_TOKEN"))
    if required and not token:
        raise ValueError(
            "Home Assistant token is not set. Open WebUI -> Settings -> Home Assistant Settings and add HA_TOKEN."
        )
    return {"base": base, "token": token}


def save_homeassistant_config(*, base_url: Any = None, token: Any = None, client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    current = load_homeassistant_config(required=False, client=store)
    next_base = _text(current.get("base") if base_url is None else base_url) or HOMEASSISTANT_DEFAULT_BASE_URL
    next_token = _text(current.get("token") if token is None else token)
    store.hset(
        HOMEASSISTANT_SETTINGS_KEY,
        mapping={
            "HA_BASE_URL": next_base.rstrip("/"),
            "HA_TOKEN": next_token,
        },
    )
    return load_homeassistant_config(required=False, client=store)


def read_integration_settings() -> Dict[str, Any]:
    config = load_homeassistant_config(required=False)
    return {
        "homeassistant_base_url": config.get("base") or HOMEASSISTANT_DEFAULT_BASE_URL,
        "homeassistant_token": config.get("token", ""),
    }


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_homeassistant_config(
        base_url=(payload or {}).get("homeassistant_base_url"),
        token=(payload or {}).get("homeassistant_token"),
    )
    return {
        "homeassistant_base_url": saved.get("base") or HOMEASSISTANT_DEFAULT_BASE_URL,
        "homeassistant_token": saved.get("token", ""),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported Home Assistant action: {action_id}")

    current = read_integration_settings()
    base = _text((payload or {}).get("homeassistant_base_url") or current.get("homeassistant_base_url")).rstrip("/")
    token = _text((payload or {}).get("homeassistant_token") or current.get("homeassistant_token"))
    if not base:
        raise ValueError("Home Assistant base URL is required.")
    if not token:
        raise ValueError("Home Assistant token is required.")

    response = requests.get(
        f"{base}/api/",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=10,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Home Assistant test failed: HTTP {response.status_code}: {response.text[:200]}")
    return {"ok": True, "message": "Home Assistant connection worked."}


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


def _homeassistant_rest_get(base_url: Any, token: Any, path: str, *, timeout_s: float = 20.0) -> Any:
    base = _text(base_url).rstrip("/")
    bearer = _text(token)
    if not base or not bearer:
        return []
    response = requests.get(
        f"{base}{path if _text(path).startswith('/') else '/' + _text(path)}",
        headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"},
        timeout=max(5.0, float(timeout_s or 20.0)),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Home Assistant HTTP {response.status_code}: {response.text[:200]}")
    try:
        return response.json()
    except Exception:
        return []


def _homeassistant_entity_details(row: Dict[str, Any]) -> Dict[str, Any]:
    attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
    details: Dict[str, Any] = {}
    for key in (
        "unit_of_measurement",
        "device_class",
        "state_class",
        "battery_level",
        "last_changed",
        "last_updated",
    ):
        value = attrs.get(key) if key in attrs else row.get(key)
        if value not in (None, ""):
            details[key] = value
    for key in ("temperature", "humidity", "illuminance", "pressure", "voltage", "power", "energy"):
        value = attrs.get(key)
        if value not in (None, ""):
            details[key] = value
    return details


def integration_devices() -> Dict[str, Any]:
    config = load_homeassistant_config(required=False)
    base = _text(config.get("base"))
    token = _text(config.get("token"))
    if not token:
        return {"devices": [], "message": "Home Assistant is not configured."}
    states = _homeassistant_rest_get(base, token, "/api/states", timeout_s=20.0)
    devices: list[dict] = []
    for row in states if isinstance(states, list) else []:
        if not isinstance(row, dict):
            continue
        entity_id = _text(row.get("entity_id"))
        if not entity_id:
            continue
        attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
        domain = entity_id.split(".", 1)[0] if "." in entity_id else "entity"
        state = _text(row.get("state"))
        devices.append(
            {
                "id": entity_id,
                "name": _text(attrs.get("friendly_name")) or entity_id,
                "type": domain,
                "state": state,
                "status": state,
                "details": _homeassistant_entity_details(row),
            }
        )
    return {"devices": devices, "message": f"Home Assistant returned {len(devices)} current entities."}


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


def upload_local_media_source_file_sync(
    base_url: Any,
    token: Any,
    *,
    target_media_content_id: str,
    filename: str,
    content: bytes,
    content_type: str = "audio/wav",
    timeout_s: float = 60.0,
) -> str:
    base = _text(base_url).rstrip("/")
    bearer = _text(token)
    media_target = _text(target_media_content_id)
    file_name = _text(filename) or "audio.wav"
    payload = bytes(content or b"")
    if not base:
        raise ValueError("Home Assistant base URL is required.")
    if not bearer:
        raise ValueError("Home Assistant token is required.")
    if not media_target:
        raise ValueError("Home Assistant target media folder is required.")
    if not payload:
        raise ValueError("Media upload content is empty.")

    response = requests.post(
        f"{base}/api/media_source/local_source/upload",
        headers={"Authorization": f"Bearer {bearer}"},
        data={"media_content_id": media_target},
        files={"file": (file_name, io.BytesIO(payload), _text(content_type) or "audio/wav")},
        timeout=max(5.0, float(timeout_s)),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"HA media upload failed: HTTP {response.status_code}: {response.text}")
    try:
        parsed = response.json()
    except Exception as exc:
        raise RuntimeError(f"HA media upload returned invalid JSON: {exc}") from exc

    media_content_id = _text(parsed.get("media_content_id"))
    if not media_content_id:
        raise RuntimeError("HA media upload succeeded but returned no media_content_id.")
    return media_content_id


async def remove_local_media_source(
    base_url: Any,
    token: Any,
    *,
    media_content_id: str,
    timeout_s: float = 20.0,
) -> Any:
    return await call(
        base_url,
        token,
        {
            "type": "media_source/local_source/remove",
            "id": 1,
            "media_content_id": _text(media_content_id),
        },
        timeout_s=timeout_s,
    )


def remove_local_media_source_sync(
    base_url: Any,
    token: Any,
    *,
    media_content_id: str,
    timeout_s: float = 20.0,
) -> Any:
    return _run_sync(
        remove_local_media_source(
            base_url,
            token,
            media_content_id=media_content_id,
            timeout_s=timeout_s,
        )
    )
