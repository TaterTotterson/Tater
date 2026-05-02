from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import aiohttp

from helpers import redis_client as shared_redis_client
from integrations.hue import HUE_DEFAULT_TIMEOUT_SECONDS, hue_clip_v2_root, read_hue_settings
from integrations.homekit import (
    ecobee_homekit_paired,
    homekit_dependency_available,
    list_homekit_thermostats,
    read_ecobee_homekit_settings,
    watch_homekit_thermostats,
)
from integrations.homeassistant import load_homeassistant_config, ws_url as homeassistant_ws_url
from integrations.unifi_network import (
    get_unifi_clients_all,
    get_unifi_devices_all,
    get_unifi_sites,
    pick_unifi_site,
    read_unifi_network_settings,
    unifi_network_base,
    unifi_network_headers,
)
from integrations.unifi_protect import load_unifi_protect_config, unifi_protect_configured

logger = logging.getLogger("integration_runtime")

INTEGRATION_RUNTIME_EVENTS_KEY = "tater:integration_runtime:events"
INTEGRATION_RUNTIME_EVENT_SEQ_KEY = "tater:integration_runtime:event_seq"
INTEGRATION_RUNTIME_STATUS_KEY = "tater:integration_runtime:status"
INTEGRATION_RUNTIME_STATES_KEY = "tater:integration_runtime:states"

_DEFAULT_EVENT_MAX = 1000
_DEFAULT_RECONNECT_SECONDS = 5
_DEFAULT_ECOBEE_HOMEKIT_POLL_SECONDS = 30
_DEFAULT_UNIFI_NETWORK_POLL_SECONDS = 30
_TASKS: List[asyncio.Task] = []
_STOP_EVENT: Optional[asyncio.Event] = None
_RUNTIME_CLIENT: Any = None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value).strip()


def _as_int(value: Any, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(_text(value)))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(_text(value))
    except Exception:
        return float(default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _status_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return _text(value)


def _decode_status_value(key: str, value: Any) -> Any:
    token = _text(value)
    lowered = token.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if (
        key.endswith("_ts")
        or key.endswith("_seq")
        or key.endswith("_count")
        or key.endswith("_seconds")
        or key in {"started_at", "updated_at"}
    ):
        if key.endswith("_seq") or key.endswith("_count") or key.endswith("_seconds"):
            return _as_int(token, 0, minimum=0)
        return _as_float(token, 0.0)
    return token


def _runtime_client(client: Any = None) -> Any:
    return client or _RUNTIME_CLIENT or shared_redis_client


def _event_max() -> int:
    return _as_int(os.getenv("TATER_INTEGRATION_RUNTIME_EVENT_MAX"), _DEFAULT_EVENT_MAX, minimum=100, maximum=10000)


def _settings(client: Any, key: str) -> Dict[str, Any]:
    try:
        raw = (client or shared_redis_client).hgetall(key) or {}
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {_text(k): v for k, v in raw.items()}


def _setting_int(
    client: Any,
    settings_key: str,
    field: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    return _as_int(_settings(client, settings_key).get(field), default, minimum=minimum, maximum=maximum)


def _status_set(client: Any, **fields: Any) -> None:
    redis_obj = _runtime_client(client)
    if not redis_obj:
        return
    payload = {str(key): _status_value(value) for key, value in fields.items()}
    payload["updated_at"] = _status_value(time.time())
    redis_obj.hset(INTEGRATION_RUNTIME_STATUS_KEY, mapping=payload)


def _json_loads(raw: Any) -> Optional[Dict[str, Any]]:
    text = _text(raw)
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _state_set(client: Any, provider: str, state_id: Any, payload: Dict[str, Any]) -> None:
    redis_obj = _runtime_client(client)
    token = _text(state_id)
    if not redis_obj or not token:
        return
    record = {
        "provider": _text(provider),
        "id": token,
        "updated_at": time.time(),
        "payload": payload if isinstance(payload, dict) else {},
    }
    redis_obj.hset(
        INTEGRATION_RUNTIME_STATES_KEY,
        f"{_text(provider)}:{token}",
        json.dumps(record, separators=(",", ":"), default=str),
    )


def _publish_event(client: Any, provider: str, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    redis_obj = _runtime_client(client)
    if not redis_obj:
        return {}
    now = time.time()
    seq = _as_int(redis_obj.incr(INTEGRATION_RUNTIME_EVENT_SEQ_KEY), 0, minimum=0)
    record = {
        "seq": seq,
        "ts": now,
        "provider": _text(provider),
        "kind": _text(kind),
        "payload": payload if isinstance(payload, dict) else {},
    }
    redis_obj.lpush(INTEGRATION_RUNTIME_EVENTS_KEY, json.dumps(record, separators=(",", ":"), default=str))
    redis_obj.ltrim(INTEGRATION_RUNTIME_EVENTS_KEY, 0, _event_max() - 1)
    _status_set(
        redis_obj,
        last_event_seq=seq,
        last_event_ts=now,
        last_event_provider=provider,
        last_event_kind=kind,
        **{f"{_text(provider)}_last_event_ts": now},
    )
    return record


async def _sleep(stop_event: asyncio.Event, seconds: float) -> None:
    deadline = time.monotonic() + max(0.1, float(seconds or 0.1))
    while not stop_event.is_set() and time.monotonic() < deadline:
        await asyncio.sleep(min(0.5, max(0.05, deadline - time.monotonic())))


async def _receive_json(ws: Any, *, timeout_s: float) -> Dict[str, Any]:
    msg = await ws.receive(timeout=timeout_s)
    if msg.type != aiohttp.WSMsgType.TEXT:
        raise RuntimeError("Unexpected websocket payload")
    data = json.loads(msg.data)
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected websocket JSON payload")
    return data


async def _authenticate_homeassistant(ws: Any, token: str) -> None:
    hello = await _receive_json(ws, timeout_s=10.0)
    hello_type = _text(hello.get("type"))
    if hello_type == "auth_required":
        await ws.send_json({"type": "auth", "access_token": token})
        auth = await _receive_json(ws, timeout_s=10.0)
        if _text(auth.get("type")) != "auth_ok":
            raise RuntimeError(f"Home Assistant websocket auth failed: {auth}")
        return
    if hello_type == "auth_ok":
        return
    raise RuntimeError(f"Unexpected Home Assistant websocket hello: {hello}")


async def _homeassistant_loop(stop_event: asyncio.Event, client: Any) -> None:
    redis_obj = _runtime_client(client)
    while not stop_event.is_set():
        reconnect_seconds = _setting_int(
            redis_obj,
            "awareness_core_settings",
            "ws_reconnect_seconds",
            _DEFAULT_RECONNECT_SECONDS,
            minimum=1,
            maximum=60,
        )
        try:
            conf = load_homeassistant_config(required=False, client=redis_obj)
            token = _text(conf.get("token"))
            base = _text(conf.get("base"))
            if not token:
                _status_set(
                    redis_obj,
                    homeassistant_configured=False,
                    homeassistant_connected=False,
                    homeassistant_ws_connected=False,
                    homeassistant_last_error="",
                )
                await _sleep(stop_event, reconnect_seconds)
                continue

            ws_addr = homeassistant_ws_url(base)
            _status_set(redis_obj, homeassistant_configured=True, homeassistant_ws_url=ws_addr)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_addr, heartbeat=30) as ws:
                    await _authenticate_homeassistant(ws, token)
                    await ws.send_json({"id": 1, "type": "subscribe_events", "event_type": "state_changed"})
                    _status_set(
                        redis_obj,
                        homeassistant_connected=True,
                        homeassistant_ws_connected=True,
                        homeassistant_ws_url=ws_addr,
                        homeassistant_last_error="",
                    )
                    logger.info("[integrations] Home Assistant websocket connected: %s", ws_addr)
                    while not stop_event.is_set():
                        try:
                            msg = await ws.receive(timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if not isinstance(data, dict):
                                continue
                            if data.get("type") == "event":
                                event = data.get("event") if isinstance(data.get("event"), dict) else {}
                                if _text(event.get("event_type")) != "state_changed":
                                    continue
                                payload = event.get("data") if isinstance(event.get("data"), dict) else {}
                                if not payload:
                                    continue
                                entity_id = _text(payload.get("entity_id"))
                                new_state = payload.get("new_state") if isinstance(payload.get("new_state"), dict) else {}
                                if entity_id:
                                    _state_set(
                                        redis_obj,
                                        "homeassistant",
                                        entity_id,
                                        {
                                            "entity_id": entity_id,
                                            "state": new_state.get("state"),
                                            "attributes": new_state.get("attributes") if isinstance(new_state.get("attributes"), dict) else {},
                                            "raw": new_state,
                                        },
                                    )
                                _publish_event(redis_obj, "homeassistant", "state_changed", payload)
                            elif data.get("type") == "result" and data.get("id") == 1 and not data.get("success"):
                                raise RuntimeError(f"Home Assistant subscribe_events failed: {data}")
                        elif msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                            raise RuntimeError("Home Assistant websocket connection closed")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _status_set(
                redis_obj,
                homeassistant_connected=False,
                homeassistant_ws_connected=False,
                homeassistant_last_error=str(exc),
                last_error=str(exc),
            )
            if "token is not set" in _text(exc).lower():
                logger.debug("[integrations] Home Assistant runtime waiting for config.")
            else:
                logger.warning("[integrations] Home Assistant websocket error: %s", exc)
            await _sleep(stop_event, reconnect_seconds)
    _status_set(redis_obj, homeassistant_connected=False, homeassistant_ws_connected=False)


def _unifi_ws_url(base: str, override: str = "") -> str:
    candidate = _text(override)
    if candidate.startswith(("ws://", "wss://")):
        return candidate
    if candidate:
        path = candidate if candidate.startswith("/") else f"/{candidate}"
    else:
        path = "/proxy/protect/integration/v1/subscribe/events"
    if base.startswith("https://"):
        return base.replace("https://", "wss://", 1) + path
    if base.startswith("http://"):
        return base.replace("http://", "ws://", 1) + path
    return f"wss://{base.lstrip('/')}{path}"


def _unifi_ws_event_item(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    item = payload.get("item")
    if isinstance(item, dict):
        out = dict(item)
        action = _text(payload.get("action"))
        model_key = _text(payload.get("modelKey") or payload.get("model_key"))
        event_id = _text(payload.get("id")) if model_key.lower() in {"event", "events"} else ""
        if action:
            out.setdefault("__ws_action", action)
        if model_key:
            out.setdefault("__ws_model_key", model_key)
        if event_id:
            out.setdefault("__ws_event_id", event_id)
        return out
    model_key = _text(payload.get("modelKey") or payload.get("model_key")).lower()
    if model_key in {"event", "events"}:
        out = dict(payload)
        if _text(payload.get("action")):
            out.setdefault("__ws_action", _text(payload.get("action")))
        if _text(payload.get("id")):
            out.setdefault("__ws_event_id", _text(payload.get("id")))
        out.setdefault("__ws_model_key", model_key)
        return out
    return None


def _unifi_event_state_id(item: Dict[str, Any]) -> str:
    for key in ("camera", "cameraId", "camera_id", "sensor", "sensorId", "sensor_id", "device", "deviceId", "device_id"):
        token = _text(item.get(key))
        if token:
            return token.lower()
    return _text(item.get("id") or item.get("__ws_event_id")).lower()


async def _unifi_protect_loop(stop_event: asyncio.Event, client: Any) -> None:
    redis_obj = _runtime_client(client)
    while not stop_event.is_set():
        reconnect_seconds = _setting_int(
            redis_obj,
            "awareness_core_settings",
            "unifi_ws_reconnect_seconds",
            _DEFAULT_RECONNECT_SECONDS,
            minimum=1,
            maximum=60,
        )
        try:
            if not unifi_protect_configured(redis_obj):
                _status_set(
                    redis_obj,
                    unifi_protect_configured=False,
                    unifi_protect_connected=False,
                    unifi_protect_ws_connected=False,
                    unifi_protect_last_error="",
                )
                await _sleep(stop_event, reconnect_seconds)
                continue

            conf = load_unifi_protect_config(required=True, client=redis_obj)
            override = _text(_settings(redis_obj, "awareness_core_settings").get("unifi_ws_url"))
            ws_addr = _unifi_ws_url(conf["base"], override)
            headers = {"X-API-KEY": conf["api_key"], "Accept": "application/json"}
            _status_set(redis_obj, unifi_protect_configured=True, unifi_protect_ws_url=ws_addr)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_addr, headers=headers, heartbeat=30, ssl=False) as ws:
                    _status_set(
                        redis_obj,
                        unifi_protect_connected=True,
                        unifi_protect_ws_connected=True,
                        unifi_protect_ws_url=ws_addr,
                        unifi_protect_last_error="",
                    )
                    logger.info("[integrations] UniFi Protect websocket connected: %s", ws_addr)
                    while not stop_event.is_set():
                        try:
                            msg = await ws.receive(timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload_text = _text(msg.data)
                            if not payload_text or payload_text.lower() in {"ping", "pong"}:
                                continue
                            try:
                                parsed = json.loads(payload_text)
                            except Exception:
                                continue
                            if not isinstance(parsed, dict):
                                continue
                            item = _unifi_ws_event_item(parsed)
                            if item is None:
                                continue
                            state_id = _unifi_event_state_id(item)
                            if state_id:
                                _state_set(
                                    redis_obj,
                                    "unifi_protect",
                                    state_id,
                                    {
                                        "id": state_id,
                                        "event_type": _text(item.get("type") or item.get("eventType") or item.get("event_type")),
                                        "raw": item,
                                    },
                                )
                            _publish_event(redis_obj, "unifi_protect", "protect_event", item)
                        elif msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                            raise RuntimeError("UniFi Protect websocket connection closed")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _status_set(
                redis_obj,
                unifi_protect_connected=False,
                unifi_protect_ws_connected=False,
                unifi_protect_last_error=str(exc),
                last_error=str(exc),
            )
            if "api key is not set" in _text(exc).lower():
                logger.debug("[integrations] UniFi Protect runtime waiting for config.")
            else:
                logger.warning("[integrations] UniFi Protect websocket error: %s", exc)
            await _sleep(stop_event, reconnect_seconds)
    _status_set(redis_obj, unifi_protect_connected=False, unifi_protect_ws_connected=False)


def _first_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _unifi_network_status(row: Dict[str, Any]) -> str:
    for key in ("status", "state", "connectionState", "connection_state", "health"):
        value = _text(row.get(key))
        if value:
            return value
    for key in ("isConnected", "is_connected", "connected", "online", "adopted"):
        if key in row:
            return "online" if bool(row.get(key)) else "offline"
    return ""


def _unifi_network_row_id(row: Dict[str, Any], category: str) -> str:
    if category == "client":
        return _first_text(row, "id", "macAddress", "mac", "ipAddress", "ip")
    return _first_text(row, "id", "macAddress", "mac", "serial", "ipAddress", "ip")


def _unifi_network_name(row: Dict[str, Any], category: str, row_id: str) -> str:
    if category == "client":
        return _first_text(row, "name", "hostname", "displayName", "ipAddress", "ip", "macAddress", "mac") or row_id
    return _first_text(row, "name", "displayName", "model", "macAddress", "mac", "serial") or row_id


def _unifi_network_detail_map(row: Dict[str, Any], category: str) -> Dict[str, Any]:
    if category == "client":
        keys = [
            "ipAddress",
            "ip",
            "macAddress",
            "mac",
            "network",
            "networkName",
            "ssid",
            "wired",
            "wifiExperience",
            "signal",
            "rssi",
            "rxRate",
            "txRate",
            "channel",
            "band",
            "uplinkDeviceName",
            "uplinkDeviceId",
            "lastSeen",
        ]
    else:
        keys = [
            "ipAddress",
            "ip",
            "macAddress",
            "mac",
            "serial",
            "model",
            "version",
            "firmwareVersion",
            "type",
            "deviceType",
            "adopted",
            "upgradeAvailable",
            "updateAvailable",
            "uplinkDeviceName",
            "uplinkDeviceId",
        ]
    return {key: row.get(key) for key in keys if row.get(key) not in (None, "")}


def _unifi_network_state_payload(row: Dict[str, Any], category: str, site_name: str) -> Dict[str, Any]:
    row_id = _unifi_network_row_id(row, category)
    state = _unifi_network_status(row)
    return {
        "id": row_id,
        "name": _unifi_network_name(row, category, row_id),
        "category": category,
        "state": state,
        "status": state,
        "area": site_name,
        "details": _unifi_network_detail_map(row, category),
        "raw": row,
    }


def _unifi_network_fingerprint(row: Dict[str, Any], category: str) -> str:
    if category == "client":
        keys = [
            "name",
            "hostname",
            "displayName",
            "ipAddress",
            "ip",
            "network",
            "networkName",
            "ssid",
            "wired",
            "status",
            "state",
            "connectionState",
            "isConnected",
            "connected",
            "online",
            "wifiExperience",
            "signal",
            "rssi",
            "rxRate",
            "txRate",
            "channel",
            "band",
            "uplinkDeviceName",
            "uplinkDeviceId",
        ]
    else:
        keys = [
            "name",
            "displayName",
            "ipAddress",
            "ip",
            "status",
            "state",
            "connectionState",
            "isConnected",
            "connected",
            "online",
            "adopted",
            "model",
            "version",
            "firmwareVersion",
            "upgradeAvailable",
            "updateAvailable",
            "uplinkDeviceName",
            "uplinkDeviceId",
        ]
    payload = {key: row.get(key) for key in keys if key in row}
    payload["state"] = _unifi_network_status(row)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _unifi_network_snapshot_from_settings(settings: Dict[str, str]) -> Dict[str, Any]:
    api_key = _text((settings or {}).get("UNIFI_API_KEY"))
    if not api_key:
        return {"configured": False, "site_id": "", "site_name": "", "devices": [], "clients": []}
    base = unifi_network_base(settings)
    headers = unifi_network_headers(api_key)
    sites = get_unifi_sites(base, headers)
    site_id, site_name = pick_unifi_site(sites)
    devices_payload = get_unifi_devices_all(base, headers, site_id)
    clients_payload = get_unifi_clients_all(base, headers, site_id)
    devices = [dict(row) for row in devices_payload.get("data") or [] if isinstance(row, dict)]
    clients = [dict(row) for row in clients_payload.get("data") or [] if isinstance(row, dict)]
    return {
        "configured": True,
        "base": base,
        "site_id": site_id,
        "site_name": site_name,
        "devices": devices,
        "clients": clients,
    }


def _unifi_network_publish_changes(
    client: Any,
    snapshot: Dict[str, Any],
    previous: Dict[str, Dict[str, Any]],
    *,
    first_snapshot: bool,
) -> Dict[str, Dict[str, Any]]:
    redis_obj = _runtime_client(client)
    site_name = _text(snapshot.get("site_name")) or "Unknown"
    current: Dict[str, Dict[str, Any]] = {}

    for category in ("device", "client"):
        rows = snapshot.get(f"{category}s") if isinstance(snapshot.get(f"{category}s"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_id = _unifi_network_row_id(row, category)
            if not row_id:
                continue
            state_id = f"{category}:{row_id.lower()}"
            payload = _unifi_network_state_payload(row, category, site_name)
            fingerprint = _unifi_network_fingerprint(row, category)
            current[state_id] = {
                "category": category,
                "fingerprint": fingerprint,
                "payload": payload,
            }
            _state_set(redis_obj, "unifi_network", state_id, payload)

            old = previous.get(state_id)
            if first_snapshot:
                continue
            if not old:
                _publish_event(
                    redis_obj,
                    "unifi_network",
                    "client_connected" if category == "client" else "device_seen",
                    payload,
                )
                continue
            if _text(old.get("fingerprint")) != fingerprint:
                _publish_event(
                    redis_obj,
                    "unifi_network",
                    "client_update" if category == "client" else "device_update",
                    {
                        **payload,
                        "previous": old.get("payload") if isinstance(old.get("payload"), dict) else {},
                    },
                )

    if not first_snapshot:
        for state_id, old in previous.items():
            if state_id in current:
                continue
            old_payload = old.get("payload") if isinstance(old.get("payload"), dict) else {}
            category = _text(old.get("category")) or _text(old_payload.get("category")) or "device"
            payload = {
                **old_payload,
                "state": "offline" if category == "client" else "missing",
                "status": "offline" if category == "client" else "missing",
                "last_missing_at": time.time(),
            }
            _state_set(redis_obj, "unifi_network", state_id, payload)
            _publish_event(
                redis_obj,
                "unifi_network",
                "client_disconnected" if category == "client" else "device_missing",
                payload,
            )

    return current


async def _unifi_network_poll_loop(stop_event: asyncio.Event, client: Any) -> None:
    redis_obj = _runtime_client(client)
    previous: Dict[str, Dict[str, Any]] = {}
    first_snapshot = True
    while not stop_event.is_set():
        poll_seconds = _as_int(
            os.getenv("TATER_UNIFI_NETWORK_POLL_SECONDS"),
            _DEFAULT_UNIFI_NETWORK_POLL_SECONDS,
            minimum=5,
            maximum=300,
        )
        try:
            settings = read_unifi_network_settings(redis_obj)
            api_key = _text(settings.get("UNIFI_API_KEY"))
            if not api_key:
                previous = {}
                first_snapshot = True
                _status_set(
                    redis_obj,
                    unifi_network_configured=False,
                    unifi_network_connected=False,
                    unifi_network_poll_connected=False,
                    unifi_network_last_error="",
                    unifi_network_poll_interval_seconds=poll_seconds,
                )
                await _sleep(stop_event, poll_seconds)
                continue

            _status_set(
                redis_obj,
                unifi_network_configured=True,
                unifi_network_poll_interval_seconds=poll_seconds,
            )
            snapshot = await asyncio.to_thread(_unifi_network_snapshot_from_settings, settings)
            current = _unifi_network_publish_changes(redis_obj, snapshot, previous, first_snapshot=first_snapshot)
            device_count = len(snapshot.get("devices") or [])
            client_count = len(snapshot.get("clients") or [])
            _status_set(
                redis_obj,
                unifi_network_configured=True,
                unifi_network_connected=True,
                unifi_network_poll_connected=True,
                unifi_network_poll_interval_seconds=poll_seconds,
                unifi_network_site_id=_text(snapshot.get("site_id")),
                unifi_network_site_name=_text(snapshot.get("site_name")),
                unifi_network_device_count=device_count,
                unifi_network_client_count=client_count,
                unifi_network_last_poll_ts=time.time(),
                unifi_network_last_error="",
            )
            if first_snapshot:
                _publish_event(
                    redis_obj,
                    "unifi_network",
                    "network_snapshot",
                    {
                        "site_id": _text(snapshot.get("site_id")),
                        "site_name": _text(snapshot.get("site_name")),
                        "device_count": device_count,
                        "client_count": client_count,
                    },
                )
            previous = current
            first_snapshot = False
            await _sleep(stop_event, poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _status_set(
                redis_obj,
                unifi_network_connected=False,
                unifi_network_poll_connected=False,
                unifi_network_last_error=str(exc),
                last_error=str(exc),
            )
            logger.warning("[integrations] UniFi Network poll error: %s", exc)
            await _sleep(stop_event, poll_seconds)
    _status_set(redis_obj, unifi_network_connected=False, unifi_network_poll_connected=False)


def _hue_eventstream_url(root: Any) -> str:
    return f"{hue_clip_v2_root(root)}/eventstream/clip/v2"


def _hue_resource_state(resource: Dict[str, Any]) -> str:
    resource_type = _text(resource.get("type"))
    if resource_type == "light":
        on = resource.get("on") if isinstance(resource.get("on"), dict) else {}
        if "on" in on:
            return "on" if bool(on.get("on")) else "off"
    if resource_type == "temperature":
        temp = resource.get("temperature") if isinstance(resource.get("temperature"), dict) else {}
        value = temp.get("temperature")
        return f"{value} C" if value not in (None, "") else ""
    if resource_type == "motion":
        motion = resource.get("motion") if isinstance(resource.get("motion"), dict) else {}
        if "motion" in motion:
            return "motion" if bool(motion.get("motion")) else "clear"
    if resource_type == "contact":
        contact = resource.get("contact") if isinstance(resource.get("contact"), dict) else {}
        report = contact.get("contact_report") if isinstance(contact.get("contact_report"), dict) else {}
        return _text(report.get("state"))
    for key in ("status", "state"):
        value = _text(resource.get(key))
        if value:
            return value
    return ""


def _hue_resource_name(resource: Dict[str, Any], fallback: str) -> str:
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    return _text(metadata.get("name")) or fallback


def _hue_event_resources(container: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = container.get("data") if isinstance(container.get("data"), list) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


async def _hue_eventstream_loop(stop_event: asyncio.Event, client: Any) -> None:
    redis_obj = _runtime_client(client)
    while not stop_event.is_set():
        reconnect_seconds = _DEFAULT_RECONNECT_SECONDS
        try:
            settings = read_hue_settings(redis_obj)
            app_key = _text(settings.get("HUE_APP_KEY"))
            if not app_key:
                _status_set(
                    redis_obj,
                    hue_configured=False,
                    hue_connected=False,
                    hue_ws_connected=False,
                    hue_eventstream_connected=False,
                    hue_last_error="",
                )
                await _sleep(stop_event, reconnect_seconds)
                continue

            timeout_seconds = _as_int(
                settings.get("HUE_TIMEOUT_SECONDS"),
                HUE_DEFAULT_TIMEOUT_SECONDS,
                minimum=2,
                maximum=60,
            )
            stream_url = _hue_eventstream_url(settings.get("HUE_BRIDGE_HOST"))
            headers = {"hue-application-key": app_key, "Accept": "text/event-stream"}
            _status_set(redis_obj, hue_configured=True, hue_ws_url=stream_url)
            timeout = aiohttp.ClientTimeout(total=None, connect=max(2.0, float(timeout_seconds)), sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(stream_url, headers=headers, ssl=False) as response:
                    if response.status >= 400:
                        body = await response.text()
                        raise RuntimeError(f"Hue event stream HTTP {response.status}: {body[:200]}")
                    _status_set(
                        redis_obj,
                        hue_connected=True,
                        hue_ws_connected=True,
                        hue_eventstream_connected=True,
                        hue_ws_url=stream_url,
                        hue_last_error="",
                    )
                    logger.info("[integrations] Hue event stream connected: %s", stream_url)
                    async for raw_line in response.content:
                        if stop_event.is_set():
                            break
                        line = raw_line.decode("utf-8", "ignore").strip()
                        if not line or line.startswith(":"):
                            continue
                        if not line.startswith("data:"):
                            continue
                        payload_text = line[5:].strip()
                        if not payload_text:
                            continue
                        try:
                            containers = json.loads(payload_text)
                        except Exception:
                            continue
                        if isinstance(containers, dict):
                            containers = [containers]
                        if not isinstance(containers, list):
                            continue
                        for container in containers:
                            if not isinstance(container, dict):
                                continue
                            for resource in _hue_event_resources(container):
                                resource_type = _text(resource.get("type") or "resource")
                                resource_id = _text(resource.get("id") or resource.get("id_v1"))
                                if not resource_id:
                                    continue
                                state_id = f"{resource_type}:{resource_id}"
                                state = _hue_resource_state(resource)
                                _state_set(
                                    redis_obj,
                                    "hue",
                                    state_id,
                                    {
                                        "id": resource_id,
                                        "name": _hue_resource_name(resource, resource_id),
                                        "resource_type": resource_type,
                                        "state": state,
                                        "event_type": _text(container.get("type")),
                                        "raw": resource,
                                    },
                                )
                                _publish_event(
                                    redis_obj,
                                    "hue",
                                    "resource_update",
                                    {
                                        "id": resource_id,
                                        "resource_type": resource_type,
                                        "state": state,
                                        "container": {
                                            "id": container.get("id"),
                                            "type": container.get("type"),
                                            "creationtime": container.get("creationtime"),
                                        },
                                        "resource": resource,
                                    },
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _status_set(
                redis_obj,
                hue_connected=False,
                hue_ws_connected=False,
                hue_eventstream_connected=False,
                hue_last_error=str(exc),
                last_error=str(exc),
            )
            logger.warning("[integrations] Hue event stream error: %s", exc)
            await _sleep(stop_event, reconnect_seconds)
    _status_set(redis_obj, hue_connected=False, hue_ws_connected=False, hue_eventstream_connected=False)


def _ecobee_thermostat_state(row: Dict[str, Any]) -> str:
    temp = row.get("current_temperature_f")
    if temp not in (None, ""):
        return f"{temp} F"
    temp_c = row.get("current_temperature_c")
    if temp_c not in (None, ""):
        return f"{temp_c} C"
    return _text(row.get("current_hvac_state"))


def _ecobee_state_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": _text(row.get("id")),
        "name": _text(row.get("name")),
        "state": _ecobee_thermostat_state(row),
        "current_temperature_f": row.get("current_temperature_f"),
        "current_temperature_c": row.get("current_temperature_c"),
        "current_humidity": row.get("current_humidity"),
        "target_temperature_f": row.get("target_temperature_f"),
        "target_temperature_c": row.get("target_temperature_c"),
        "target_hvac_mode": row.get("target_hvac_mode"),
        "current_hvac_state": row.get("current_hvac_state"),
        "raw": row,
    }


def _ecobee_homekit_fingerprint(row: Dict[str, Any]) -> str:
    keys = [
        "name",
        "current_temperature_f",
        "current_temperature_c",
        "current_humidity",
        "target_temperature_f",
        "target_temperature_c",
        "target_hvac_mode",
        "current_hvac_state",
        "heating_threshold_f",
        "cooling_threshold_f",
    ]
    return json.dumps({key: row.get(key) for key in keys if key in row}, sort_keys=True, separators=(",", ":"), default=str)


async def _ecobee_homekit_poll_loop(
    stop_event: asyncio.Event,
    client: Any,
    *,
    alias: str,
    notice: str = "",
) -> None:
    redis_obj = _runtime_client(client)
    previous: Dict[str, Dict[str, Any]] = {}
    first_snapshot = True
    while not stop_event.is_set():
        poll_seconds = _as_int(
            os.getenv("TATER_ECOBEE_HOMEKIT_POLL_SECONDS"),
            _DEFAULT_ECOBEE_HOMEKIT_POLL_SECONDS,
            minimum=10,
            maximum=300,
        )
        try:
            rows = await asyncio.to_thread(list_homekit_thermostats, alias)
            current: Dict[str, Dict[str, Any]] = {}
            thermostats: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                thermostat_id = _text(row.get("id"))
                if not thermostat_id:
                    continue
                payload = _ecobee_state_payload(row)
                fingerprint = _ecobee_homekit_fingerprint(row)
                current[thermostat_id] = {"fingerprint": fingerprint, "payload": payload}
                thermostats.append(row)
                _state_set(redis_obj, "ecobee_homekit", thermostat_id, payload)

                old = previous.get(thermostat_id)
                if first_snapshot:
                    continue
                if not old or _text(old.get("fingerprint")) != fingerprint:
                    _publish_event(
                        redis_obj,
                        "ecobee_homekit",
                        "thermostat_update",
                        {
                            "type": "poll",
                            "alias": alias,
                            "thermostat": row,
                            "previous": old.get("payload") if isinstance(old, dict) and isinstance(old.get("payload"), dict) else {},
                        },
                    )

            if not first_snapshot:
                for thermostat_id, old in previous.items():
                    if thermostat_id in current:
                        continue
                    old_payload = old.get("payload") if isinstance(old.get("payload"), dict) else {}
                    _publish_event(
                        redis_obj,
                        "ecobee_homekit",
                        "thermostat_missing",
                        {"type": "poll", "alias": alias, "thermostat": old_payload},
                    )

            if first_snapshot:
                _publish_event(
                    redis_obj,
                    "ecobee_homekit",
                    "thermostat_poll_snapshot",
                    {
                        "type": "poll_snapshot",
                        "alias": alias,
                        "thermostats": thermostats,
                        "notice": notice,
                    },
                )

            previous = current
            first_snapshot = False
            _status_set(
                redis_obj,
                ecobee_homekit_configured=True,
                ecobee_homekit_connected=True,
                ecobee_homekit_ws_connected=False,
                ecobee_homekit_poll_connected=True,
                ecobee_homekit_poll_interval_seconds=poll_seconds,
                ecobee_homekit_monitor_mode="poll",
                ecobee_homekit_notice=notice,
                ecobee_homekit_last_error="",
            )
            await _sleep(stop_event, poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _status_set(
                redis_obj,
                ecobee_homekit_connected=False,
                ecobee_homekit_poll_connected=False,
                ecobee_homekit_poll_interval_seconds=poll_seconds,
                ecobee_homekit_last_error=str(exc),
                last_error=str(exc),
            )
            logger.warning("[integrations] Ecobee HomeKit poll error: %s", exc)
            await _sleep(stop_event, poll_seconds)


def _ecobee_homekit_event_subscription_unavailable(exc: Exception) -> bool:
    text = _text(exc).lower()
    return (
        "no event-capable thermostat characteristics" in text
        or "does not expose event callbacks" in text
        or "thermostat subscription failed" in text
    )


def _ecobee_homekit_transient_disconnect(exc: Exception) -> bool:
    text = _text(exc).lower()
    return exc.__class__.__name__ == "AccessoryDisconnectedError" or "connection closed" in text


async def _ecobee_homekit_loop(stop_event: asyncio.Event, client: Any) -> None:
    redis_obj = _runtime_client(client)
    while not stop_event.is_set():
        reconnect_seconds = _DEFAULT_RECONNECT_SECONDS
        alias = "ecobee"
        try:
            settings = read_ecobee_homekit_settings(redis_obj)
            alias = _text(settings.get("ECOBEE_HOMEKIT_ALIAS")) or "ecobee"
            if not homekit_dependency_available():
                _status_set(
                    redis_obj,
                    ecobee_homekit_configured=False,
                    ecobee_homekit_connected=False,
                    ecobee_homekit_ws_connected=False,
                    ecobee_homekit_last_error="aiohomekit and zeroconf are not installed.",
                )
                await _sleep(stop_event, reconnect_seconds)
                continue
            if not ecobee_homekit_paired(alias, client=redis_obj):
                _status_set(
                    redis_obj,
                    ecobee_homekit_configured=False,
                    ecobee_homekit_connected=False,
                    ecobee_homekit_ws_connected=False,
                    ecobee_homekit_last_error="",
                )
                await _sleep(stop_event, reconnect_seconds)
                continue

            _status_set(redis_obj, ecobee_homekit_configured=True, ecobee_homekit_alias=alias)

            async def on_update(update: Dict[str, Any]) -> None:
                update_type = _text(update.get("type")) or "update"
                thermostats = update.get("thermostats") if isinstance(update.get("thermostats"), list) else []
                for row in thermostats:
                    if not isinstance(row, dict):
                        continue
                    thermostat_id = _text(row.get("id"))
                    if not thermostat_id:
                        continue
                    _state_set(redis_obj, "ecobee_homekit", thermostat_id, _ecobee_state_payload(row))
                _publish_event(
                    redis_obj,
                    "ecobee_homekit",
                    "thermostat_update" if update_type != "snapshot" else "thermostat_snapshot",
                    update,
                )
                _status_set(
                    redis_obj,
                    ecobee_homekit_connected=True,
                    ecobee_homekit_ws_connected=True,
                    ecobee_homekit_poll_connected=False,
                    ecobee_homekit_monitor_mode="event",
                    ecobee_homekit_notice="",
                    ecobee_homekit_last_error="",
                )

            logger.info("[integrations] Ecobee HomeKit monitor connecting: %s", alias)
            await watch_homekit_thermostats(alias=alias, on_update=on_update, stop_event=stop_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            transient_disconnect = _ecobee_homekit_transient_disconnect(exc)
            error_text = "" if transient_disconnect else str(exc)
            _status_set(
                redis_obj,
                ecobee_homekit_connected=False,
                ecobee_homekit_ws_connected=False,
                ecobee_homekit_last_error=error_text,
                last_error=error_text,
            )
            if _ecobee_homekit_event_subscription_unavailable(exc):
                notice = "HomeKit event subscriptions are unavailable for this thermostat; polling instead."
                logger.info("[integrations] Ecobee HomeKit events unavailable for %s; polling instead.", alias)
                await _ecobee_homekit_poll_loop(stop_event, redis_obj, alias=alias, notice=notice)
                continue
            if transient_disconnect:
                logger.info("[integrations] Ecobee HomeKit monitor disconnected for %s; reconnecting.", alias)
                await _sleep(stop_event, reconnect_seconds)
                continue
            logger.warning("[integrations] Ecobee HomeKit monitor error: %s", exc)
            await _sleep(stop_event, reconnect_seconds)
    _status_set(redis_obj, ecobee_homekit_connected=False, ecobee_homekit_ws_connected=False, ecobee_homekit_poll_connected=False)


def start_integration_runtime(client: Any = None) -> Dict[str, Any]:
    global _RUNTIME_CLIENT, _STOP_EVENT, _TASKS
    running_tasks = [task for task in _TASKS if not task.done()]
    if running_tasks:
        _TASKS = running_tasks
        return integration_runtime_status(client)

    redis_obj = _runtime_client(client)
    _RUNTIME_CLIENT = redis_obj
    _STOP_EVENT = asyncio.Event()
    _TASKS = [
        asyncio.create_task(_homeassistant_loop(_STOP_EVENT, redis_obj), name="integration-runtime-homeassistant"),
        asyncio.create_task(_unifi_protect_loop(_STOP_EVENT, redis_obj), name="integration-runtime-unifi-protect"),
        asyncio.create_task(_unifi_network_poll_loop(_STOP_EVENT, redis_obj), name="integration-runtime-unifi-network"),
        asyncio.create_task(_hue_eventstream_loop(_STOP_EVENT, redis_obj), name="integration-runtime-hue"),
        asyncio.create_task(_ecobee_homekit_loop(_STOP_EVENT, redis_obj), name="integration-runtime-ecobee-homekit"),
    ]
    _status_set(
        redis_obj,
        running=True,
        started_at=time.time(),
        homeassistant_connected=False,
        homeassistant_ws_connected=False,
        unifi_protect_connected=False,
        unifi_protect_ws_connected=False,
        unifi_network_connected=False,
        unifi_network_poll_connected=False,
        hue_connected=False,
        hue_ws_connected=False,
        hue_eventstream_connected=False,
        ecobee_homekit_connected=False,
        ecobee_homekit_ws_connected=False,
        ecobee_homekit_poll_connected=False,
        last_error="",
    )
    logger.info("[integrations] runtime started")
    return integration_runtime_status(redis_obj)


async def stop_integration_runtime() -> Dict[str, Any]:
    global _STOP_EVENT, _TASKS
    redis_obj = _runtime_client()
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    for task in list(_TASKS):
        if not task.done():
            task.cancel()
    if _TASKS:
        await asyncio.gather(*_TASKS, return_exceptions=True)
    _TASKS = []
    _STOP_EVENT = None
    _status_set(
        redis_obj,
        running=False,
        homeassistant_connected=False,
        homeassistant_ws_connected=False,
        unifi_protect_connected=False,
        unifi_protect_ws_connected=False,
        unifi_network_connected=False,
        unifi_network_poll_connected=False,
        hue_connected=False,
        hue_ws_connected=False,
        hue_eventstream_connected=False,
        ecobee_homekit_connected=False,
        ecobee_homekit_ws_connected=False,
        ecobee_homekit_poll_connected=False,
    )
    logger.info("[integrations] runtime stopped")
    return integration_runtime_status(redis_obj)


def integration_runtime_status(client: Any = None) -> Dict[str, Any]:
    redis_obj = _runtime_client(client)
    raw = redis_obj.hgetall(INTEGRATION_RUNTIME_STATUS_KEY) or {} if redis_obj else {}
    status = {_text(k): _decode_status_value(_text(k), v) for k, v in raw.items()} if isinstance(raw, dict) else {}
    status["running"] = any(task for task in _TASKS if not task.done())
    try:
        status["last_event_seq"] = _as_int(redis_obj.get(INTEGRATION_RUNTIME_EVENT_SEQ_KEY), int(status.get("last_event_seq") or 0), minimum=0)
        status["event_count"] = _as_int(redis_obj.llen(INTEGRATION_RUNTIME_EVENTS_KEY), 0, minimum=0)
        status["state_count"] = _as_int(redis_obj.hlen(INTEGRATION_RUNTIME_STATES_KEY), 0, minimum=0)
    except Exception:
        status.setdefault("last_event_seq", 0)
        status.setdefault("event_count", 0)
        status.setdefault("state_count", 0)
    return status


def integration_runtime_events(client: Any = None, *, after_seq: Any = 0, limit: Any = 200) -> Dict[str, Any]:
    redis_obj = _runtime_client(client)
    after = _as_int(after_seq, 0, minimum=0)
    max_rows = _as_int(limit, 200, minimum=1, maximum=_event_max())
    rows = redis_obj.lrange(INTEGRATION_RUNTIME_EVENTS_KEY, 0, _event_max() - 1) if redis_obj else []
    events: List[Dict[str, Any]] = []
    for raw in rows or []:
        event = _json_loads(raw)
        if not event:
            continue
        seq = _as_int(event.get("seq"), 0, minimum=0)
        if seq <= after:
            continue
        event["seq"] = seq
        events.append(event)
    events.sort(key=lambda item: _as_int(item.get("seq"), 0, minimum=0))
    return {
        "events": events[:max_rows],
        "after_seq": after,
        "last_event_seq": _as_int(redis_obj.get(INTEGRATION_RUNTIME_EVENT_SEQ_KEY), 0, minimum=0) if redis_obj else 0,
    }


def integration_runtime_states(client: Any = None) -> Dict[str, Any]:
    redis_obj = _runtime_client(client)
    raw = redis_obj.hgetall(INTEGRATION_RUNTIME_STATES_KEY) or {} if redis_obj else {}
    states: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        for key, value in raw.items():
            record = _json_loads(value)
            if not record:
                continue
            record.setdefault("key", _text(key))
            states.append(record)
    states.sort(key=lambda item: (_text(item.get("provider")).casefold(), _text(item.get("id")).casefold()))
    return {"states": states, "count": len(states)}
