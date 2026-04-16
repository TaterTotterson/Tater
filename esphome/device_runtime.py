from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import logging
import re
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger("voice_core")

_ESPHOME_LOG_BUFFER_LIMIT = 500
_ESPHOME_LOG_IDLE_SECONDS = 120.0
_ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

_native_lock = asyncio.Lock()
_reconcile_lock = asyncio.Lock()
_native_clients: Dict[str, Dict[str, Any]] = {}
_native_stats: Dict[str, Any] = {
    "runs": 0,
    "last_run_ts": 0.0,
    "last_success_ts": 0.0,
    "last_error": "",
}
_discovery_stats: Dict[str, Any] = {
    "runs": 0,
    "last_run_ts": 0.0,
    "last_success_ts": 0.0,
    "last_error": "",
    "last_counts": {},
}


def _vp():
    from . import voice_pipeline as vp

    return vp


def _text(value: Any) -> str:
    return _vp()._text(value)


def _lower(value: Any) -> str:
    return _vp()._lower(value)


def _now() -> float:
    return _vp()._now()


def _as_float(value: Any, default: float = 0.0, *, minimum: float | None = None, maximum: float | None = None) -> float:
    return _vp()._as_float(value, default, minimum=minimum, maximum=maximum)


def _as_int(value: Any, default: int = 0, *, minimum: int | None = None, maximum: int | None = None) -> int:
    return _vp()._as_int(value, default, minimum=minimum, maximum=maximum)


def _as_bool(value: Any, default: bool = False) -> bool:
    return _vp()._as_bool(value, default)


def _native_debug(message: str) -> None:
    _vp()._native_debug(message)


def _voice_settings() -> Dict[str, Any]:
    return _vp()._voice_settings()


def _voice_config_snapshot() -> Dict[str, Any]:
    return _vp()._voice_config_snapshot()


def _get_int_setting(key: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    return _vp()._get_int_setting(key, default, minimum=minimum, maximum=maximum)


def _get_float_setting(key: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    return _vp()._get_float_setting(key, default, minimum=minimum, maximum=maximum)


def _load_satellite_registry() -> List[Dict[str, Any]]:
    return _vp()._load_satellite_registry()


def _upsert_satellite(row: Dict[str, Any]) -> Dict[str, Any]:
    return _vp()._upsert_satellite(row)


def _satellite_lookup(selector: str) -> Dict[str, Any]:
    return _vp()._satellite_lookup(selector)


def _load_wyoming_tts_voice_catalog() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return _vp()._load_wyoming_tts_voice_catalog()


def _load_piper_tts_model_catalog() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return _vp()._load_piper_tts_model_catalog()


def _selected_stt_backend() -> str:
    return _vp()._selected_stt_backend()


def _selected_tts_backend() -> str:
    return _vp()._selected_tts_backend()


def _resolve_stt_backend() -> tuple[str, str]:
    return _vp()._resolve_stt_backend()


def _resolve_tts_backend() -> tuple[str, str]:
    return _vp()._resolve_tts_backend()


def _voice_metrics_snapshot() -> Dict[str, Any]:
    return _vp()._voice_metrics_snapshot()


def _voice_metrics_record_connection_event(selector: str, *, event: str) -> None:
    _vp()._voice_metrics_record_connection_event(selector, event=event)


def _selector_runtime(selector: str) -> Dict[str, Any]:
    return _vp()._selector_runtime(selector)


def _is_voice_session(value: Any) -> bool:
    session_type = getattr(_vp(), "VoiceSessionRuntime", None)
    return bool(session_type is not None and isinstance(value, session_type))


def _clear_streamed_tts_state(runtime: Dict[str, Any]) -> None:
    _vp()._clear_streamed_tts_state(runtime)


def _cancel_announcement_wait(runtime: Dict[str, Any]) -> None:
    _vp()._cancel_announcement_wait(runtime)


def _cancel_audio_stall_watch(runtime: Dict[str, Any]) -> None:
    _vp()._cancel_audio_stall_watch(runtime)


async def _finalize_session(selector: str, client: Any, module: Any, *, session_id: str, abort: bool, reason: str) -> None:
    await _vp()._finalize_session(selector, client, module, session_id=session_id, abort=abort, reason=reason)


async def _esphome_subscribe_voice_assistant(selector: str, client: Any, module: Any, *, api_audio_supported: bool) -> Callable[[], None]:
    return await _vp()._esphome_subscribe_voice_assistant(
        selector,
        client,
        module,
        api_audio_supported=api_audio_supported,
    )


def _default_port() -> int:
    return int(getattr(_vp(), "DEFAULT_ESPHOME_API_PORT", 6053))


def _connect_timeout_s() -> float:
    return float(getattr(_vp(), "DEFAULT_ESPHOME_CONNECT_TIMEOUT_S", 10.0))


def _retry_seconds() -> int:
    return int(getattr(_vp(), "DEFAULT_ESPHOME_RETRY_SECONDS", 10))


def _discovery_timeout_s() -> float:
    return float(getattr(_vp(), "DEFAULT_DISCOVERY_MDNS_TIMEOUT_S", 2.0))


def _discovery_scan_seconds() -> int:
    return int(getattr(_vp(), "DEFAULT_DISCOVERY_SCAN_SECONDS", 30))


def _format_ts_label(ts_value: Any) -> str:
    ts = _as_float(ts_value, 0.0)
    if ts <= 0:
        return "-"
    with contextlib.suppress(Exception):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    return "-"


def target_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in _load_satellite_registry():
        selector = _text(row.get("selector"))
        host = _lower(row.get("host"))
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if not selector or not host:
            continue
        if bool(meta.get("esphome_selected")):
            out[selector] = host
    return out


def discovery_stats() -> Dict[str, Any]:
    return dict(_discovery_stats)


def native_stats() -> Dict[str, Any]:
    return dict(_native_stats)


async def client_row_snapshot(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    async with _native_lock:
        row = _native_clients.get(token) or {}
        return dict(row) if isinstance(row, dict) else {}


def client_row_snapshot_sync(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    row = _native_clients.get(token) or {}
    return dict(row) if isinstance(row, dict) else {}


def clients_snapshot_sync() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for selector, row in _native_clients.items():
        if isinstance(row, dict):
            out[_text(selector)] = dict(row)
    return out


# -------------------- mDNS Discovery --------------------
def _discover_mdns_sync(scan_seconds: float) -> List[Dict[str, Any]]:
    try:
        from zeroconf import ServiceBrowser, ServiceStateChange, Zeroconf  # type: ignore
    except Exception:
        return []

    service_types = ("_esphomelib._tcp.local.", "_esphome._tcp.local.")
    timeout_ms = max(200, int(float(scan_seconds) * 1000))
    found: Dict[str, Dict[str, Any]] = {}
    lock = threading.Lock()

    def _decode(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            with contextlib.suppress(Exception):
                return value.decode("utf-8", "ignore").strip()
            return ""
        return str(value).strip()

    def _collect_addresses(info: Any) -> List[str]:
        out: List[str] = []
        seen = set()
        parsed = None
        with contextlib.suppress(Exception):
            parsed = info.parsed_addresses()
        if isinstance(parsed, list):
            for addr in parsed:
                token = _lower(addr)
                if not token or token in seen:
                    continue
                seen.add(token)
                out.append(token)
        return out

    def _on_state(*args: Any, **kwargs: Any) -> None:
        zc = kwargs.get("zeroconf")
        service_type = kwargs.get("service_type")
        name = kwargs.get("name")
        state_change = kwargs.get("state_change")
        if zc is None and len(args) >= 1:
            zc = args[0]
        if service_type is None and len(args) >= 2:
            service_type = args[1]
        if name is None and len(args) >= 3:
            name = args[2]
        if state_change is None and len(args) >= 4:
            state_change = args[3]

        if zc is None or not service_type or not name:
            return
        if state_change not in (ServiceStateChange.Added, ServiceStateChange.Updated):
            return

        info = None
        with contextlib.suppress(Exception):
            info = zc.get_service_info(service_type, name, timeout=timeout_ms)
        if info is None:
            return

        addresses = _collect_addresses(info)
        host = ""
        for addr in addresses:
            if addr.startswith("127.") or addr == "::1":
                continue
            host = addr
            break
        if not host:
            host = _lower(_decode(getattr(info, "server", "")).rstrip("."))
        if not host:
            return

        props = getattr(info, "properties", None)
        props_map = props if isinstance(props, dict) else {}
        node_name = _decode(props_map.get(b"name")) or _decode(name).split(".", 1)[0] or host

        row = {
            "selector": f"host:{host}",
            "host": host,
            "name": node_name,
            "source": "mdns_esphome",
            "metadata": {
                "mdns_service": _decode(name),
                "mdns_type": _decode(service_type),
                "mdns_addresses": addresses,
            },
        }
        with lock:
            found[row["selector"]] = row

    zc = Zeroconf()
    browsers = []
    try:
        for st in service_types:
            with contextlib.suppress(Exception):
                browsers.append(ServiceBrowser(zc, st, handlers=[_on_state]))
        time.sleep(float(max(0.5, scan_seconds)))
    finally:
        for browser in browsers:
            with contextlib.suppress(Exception):
                browser.cancel()
        with contextlib.suppress(Exception):
            zc.close()

    return list(found.values())


async def discover_mdns_once() -> List[Dict[str, Any]]:
    cfg = _voice_config_snapshot()
    discovery = cfg.get("discovery") if isinstance(cfg.get("discovery"), dict) else {}
    timeout_s = float(discovery.get("mdns_timeout_s") or _discovery_timeout_s())
    return await asyncio.to_thread(_discover_mdns_sync, timeout_s)


async def discovery_loop() -> None:
    while True:
        try:
            cfg = _voice_config_snapshot()
            discovery = cfg.get("discovery") if isinstance(cfg.get("discovery"), dict) else {}
            enabled = bool(discovery.get("enabled"))

            if enabled:
                rows = await discover_mdns_once()
                for row in rows:
                    _upsert_satellite(row)
                _discovery_stats["last_counts"] = {"mdns_esphome": len(rows)}
            else:
                _discovery_stats["last_counts"] = {"mdns_esphome": 0}

            _discovery_stats["runs"] = int(_discovery_stats.get("runs") or 0) + 1
            _discovery_stats["last_run_ts"] = _now()
            _discovery_stats["last_success_ts"] = _now()
            _discovery_stats["last_error"] = ""
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _discovery_stats["last_error"] = str(exc)
            _discovery_stats["last_run_ts"] = _now()
            logger.warning("[native-voice] discovery loop error: %s", exc)

        interval = _get_int_setting("VOICE_DISCOVERY_SCAN_SECONDS", _discovery_scan_seconds(), minimum=5, maximum=600)
        await asyncio.sleep(float(interval))


# -------------------- ESPHome Native API --------------------
def esphome_import() -> Tuple[Optional[Any], str]:
    try:
        module = importlib.import_module("aioesphomeapi")
        return module, ""
    except Exception as exc:
        return None, str(exc)



def esphome_module_attr(module: Any, name: str) -> Any:
    if module is not None:
        value = getattr(module, name, None)
        if value is not None:
            return value
        sub_model = getattr(module, "model", None)
        if sub_model is not None:
            value = getattr(sub_model, name, None)
            if value is not None:
                return value
    with contextlib.suppress(Exception):
        model_module = importlib.import_module("aioesphomeapi.model")
        value = getattr(model_module, name, None)
        if value is not None:
            return value
    return None



def esphome_event_type_value(module: Any, *candidates: str) -> Any:
    enum_cls = esphome_module_attr(module, "VoiceAssistantEventType")
    if enum_cls is None:
        return None

    wanted = {_lower(item) for item in candidates if _text(item)}
    for candidate in candidates:
        token = _text(candidate)
        if not token:
            continue
        direct = getattr(enum_cls, token, None)
        if direct is not None:
            return direct

    for attr_name in dir(enum_cls):
        if _lower(attr_name) in wanted:
            return getattr(enum_cls, attr_name, None)

    return None



def esphome_payload_strings(data: Optional[Dict[str, Any]]) -> Dict[str, str]:
    payload = data if isinstance(data, dict) else {}
    out: Dict[str, str] = {}
    for key, value in payload.items():
        token = _text(key)
        if not token or value is None:
            continue
        if isinstance(value, bool):
            out[token] = "1" if value else "0"
        else:
            out[token] = str(value)
    return out


async def esphome_client_call(client: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(client, method_name, None)
    if not callable(method):
        raise RuntimeError(f"ESPHome client missing method: {method_name}")
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def esphome_send_event(client: Any, module: Any, event_candidates: Tuple[str, ...], data: Optional[Dict[str, Any]]) -> bool:
    event_type = esphome_event_type_value(module, *event_candidates)
    if event_type is None:
        _native_debug(f"esphome event unavailable candidates={event_candidates}")
        return False

    payload = esphome_payload_strings(data)
    try:
        await esphome_client_call(client, "send_voice_assistant_event", event_type, payload if payload else None)
        return True
    except Exception as exc:
        _native_debug(f"esphome event send failed candidates={event_candidates} error={exc}")
        return False



def esphome_client_connected(client: Any, fallback: bool = False) -> bool:
    if client is None:
        return False

    marker = getattr(client, "is_connected", None)
    if callable(marker):
        try:
            value = marker()
            if inspect.isawaitable(value):
                return fallback
            return bool(value)
        except Exception:
            return fallback
    if isinstance(marker, bool):
        return marker

    marker2 = getattr(client, "connected", None)
    if isinstance(marker2, bool):
        return marker2

    return fallback



def _esphome_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    with contextlib.suppress(Exception):
        text = str(value)
        if text and text != repr(value):
            return text
    return None



def _esphome_class_token(obj: Any, suffix: str) -> str:
    name = _text(getattr(getattr(obj, "__class__", None), "__name__", ""))
    if not name:
        return ""
    lowered = name.lower()
    wanted = suffix.lower()
    if lowered.endswith(wanted):
        return name[: -len(suffix)]
    return name



def _esphome_format_state_value(value: Any, *, unit: str = "", kind: str = "") -> str:
    if value is None:
        return ""
    kind_token = _lower(kind)
    if isinstance(value, bool):
        if "binary" in kind_token:
            return "On" if value else "Off"
        return "Yes" if value else "No"
    if isinstance(value, float):
        text = f"{value:.2f}".rstrip("0").rstrip(".")
    else:
        text = _text(value)
    if not text:
        return ""
    return f"{text} {unit}".strip() if unit else text


_ESPHOME_STATE_ATTR_NAMES: Tuple[str, ...] = (
    "state",
    "value",
    "position",
    "current_operation",
    "operation",
    "mode",
    "preset",
    "option",
    "brightness",
    "level",
    "volume",
    "current_temperature",
    "target_temperature",
    "temperature",
    "speed",
    "direction",
    "oscillating",
    "muted",
    "active",
    "media_title",
    "media_artist",
    "effect",
    "color_mode",
    "red",
    "green",
    "blue",
    "white",
    "cold_white",
    "warm_white",
    "color_brightness",
)



def _esphome_kind_label(kind: Any) -> str:
    token = _text(kind).strip()
    if not token:
        return "Entity"
    token = token.replace("_", " ")
    token = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", token)
    token = re.sub(r"\s+", " ", token).strip()
    return token.title() if token else "Entity"



def _esphome_entity_meta_label(info_row: Dict[str, Any], state_row: Dict[str, Any]) -> str:
    info = info_row if isinstance(info_row, dict) else {}
    state = state_row if isinstance(state_row, dict) else {}
    parts: List[str] = []
    kind_label = _esphome_kind_label(info.get("kind") or state.get("kind"))
    if kind_label:
        parts.append(kind_label)
    device_class = _text(info.get("device_class")).replace("_", " ").strip()
    if device_class:
        parts.append(device_class.title())
    entity_category = _text(info.get("entity_category")).replace("_", " ").strip()
    if entity_category:
        parts.append(entity_category.title())
    return " • ".join(part for part in parts if part)



def _esphome_entity_state_attrs(state: Any) -> Dict[str, Any]:
    if state is None:
        return {}
    out: Dict[str, Any] = {}
    for attr in _ESPHOME_STATE_ATTR_NAMES:
        if not hasattr(state, attr):
            continue
        candidate = getattr(state, attr, None)
        if callable(candidate):
            continue
        scalar = _esphome_scalar(candidate)
        if scalar in {None, ""}:
            continue
        out[attr] = scalar
    return out



def _esphome_entity_display_value(info_row: Dict[str, Any], state_row: Dict[str, Any]) -> str:
    info = info_row if isinstance(info_row, dict) else {}
    state = state_row if isinstance(state_row, dict) else {}
    unit = _text(info.get("unit"))
    attrs = state.get("attrs") if isinstance(state.get("attrs"), dict) else {}

    preferred_attrs = (
        "state",
        "value",
        "position",
        "mode",
        "option",
        "preset",
        "current_operation",
        "operation",
        "active",
        "brightness",
        "level",
        "volume",
        "current_temperature",
        "target_temperature",
        "temperature",
        "speed",
        "direction",
        "effect",
        "color_mode",
        "media_title",
    )
    for attr in preferred_attrs:
        if attr not in attrs:
            continue
        value = attrs.get(attr)
        if attr in {"brightness", "level", "volume", "position"} and isinstance(value, (int, float)):
            if 0.0 <= float(value) <= 1.0:
                return f"{round(float(value) * 100)}%"
        return _esphome_format_state_value(value, unit=unit, kind=_text(info.get("kind") or state.get("kind")))

    fallback_value = _esphome_format_state_value(
        state.get("raw"),
        unit=unit,
        kind=_text(info.get("kind") or state.get("kind")),
    )
    if fallback_value:
        return fallback_value
    if attrs:
        first_value = next(iter(attrs.values()))
        fallback_value = _esphome_format_state_value(
            first_value,
            unit=unit,
            kind=_text(info.get("kind") or state.get("kind")),
        )
        if fallback_value:
            return fallback_value
    return "Available"



def _esphome_list_values(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            token = _text(_esphome_scalar(item))
            if token:
                out.append(token)
        return out
    token = _text(_esphome_scalar(value))
    return [token] if token else []



def _esphome_entity_is_on(state_row: Dict[str, Any]) -> Optional[bool]:
    state = state_row if isinstance(state_row, dict) else {}
    attrs = state.get("attrs") if isinstance(state.get("attrs"), dict) else {}
    for key in ("state", "active"):
        value = attrs.get(key)
        if isinstance(value, bool):
            return value
    raw = state.get("raw")
    if isinstance(raw, bool):
        return raw
    return None



def _esphome_color_component(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if 0.0 <= numeric <= 1.0:
        numeric *= 255.0
    return max(0, min(255, int(round(numeric))))



def _esphome_light_color_hex(info_row: Dict[str, Any], state_row: Dict[str, Any]) -> str:
    info = info_row if isinstance(info_row, dict) else {}
    state = state_row if isinstance(state_row, dict) else {}
    attrs = state.get("attrs") if isinstance(state.get("attrs"), dict) else {}
    red = _esphome_color_component(attrs.get("red"))
    green = _esphome_color_component(attrs.get("green"))
    blue = _esphome_color_component(attrs.get("blue"))
    if red is None or green is None or blue is None:
        return "#ff9b45" if _esphome_light_supports_color(info, state) else ""
    return f"#{red:02x}{green:02x}{blue:02x}"



def _esphome_light_supports_color(info_row: Dict[str, Any], state_row: Dict[str, Any]) -> bool:
    info = info_row if isinstance(info_row, dict) else {}
    state = state_row if isinstance(state_row, dict) else {}
    attrs = state.get("attrs") if isinstance(state.get("attrs"), dict) else {}
    if all(_esphome_color_component(attrs.get(name)) is not None for name in ("red", "green", "blue")):
        return True
    color_mode = _lower(attrs.get("color_mode"))
    if any(token in color_mode for token in ("rgb", "hs", "xy")):
        return True
    supported = [_lower(item) for item in _esphome_list_values(info.get("supported_color_modes"))]
    return any(any(token in mode for token in ("rgb", "hs", "xy")) for mode in supported)



def _esphome_entity_control_spec(info_row: Dict[str, Any], state_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    info = info_row if isinstance(info_row, dict) else {}
    state = state_row if isinstance(state_row, dict) else {}
    key = _text(info.get("key") or state.get("key"))
    if not key:
        return None
    kind = _lower(info.get("kind") or state.get("kind"))
    attrs = state.get("attrs") if isinstance(state.get("attrs"), dict) else {}

    if "switch" in kind:
        return {
            "type": "toggle",
            "command": "switch_set",
            "checked": bool(_esphome_entity_is_on(state)),
        }

    if "light" in kind:
        return {
            "type": "light",
            "command": "light_set",
            "checked": bool(_esphome_entity_is_on(state)),
            "supports_color": _esphome_light_supports_color(info, state),
            "color": _esphome_light_color_hex(info, state),
        }

    if "button" in kind:
        return {
            "type": "button",
            "command": "button_press",
            "label": "Run",
        }

    if "select" in kind:
        options = _esphome_list_values(info.get("options"))
        if options:
            return {
                "type": "select",
                "command": "select_set",
                "value": _text(attrs.get("option") or attrs.get("state") or state.get("raw")),
                "options": options,
            }

    if "number" in kind:
        current_value = attrs.get("state")
        if current_value is None:
            current_value = state.get("raw")
        return {
            "type": "number",
            "command": "number_set",
            "value": current_value,
            "min": info.get("min_value"),
            "max": info.get("max_value"),
            "step": info.get("step"),
        }

    return None



def _esphome_device_info_snapshot(info: Any) -> Dict[str, Any]:
    if info is None:
        return {}
    names = [
        "name",
        "friendly_name",
        "manufacturer",
        "model",
        "project_name",
        "project_version",
        "esphome_version",
        "compilation_time",
        "mac_address",
        "bluetooth_mac_address",
        "webserver_port",
    ]
    out: Dict[str, Any] = {}
    for name in names:
        value = _esphome_scalar(getattr(info, name, None))
        if value not in {None, ""}:
            out[name] = value
    return out



def _esphome_entity_info_snapshot(info: Any) -> Dict[str, Any]:
    if info is None:
        return {}
    key = getattr(info, "key", None)
    if key is None:
        return {}
    key_text = _text(key)
    if not key_text:
        return {}
    class_name = _text(getattr(getattr(info, "__class__", None), "__name__", ""))
    kind = _esphome_class_token(info, "Info")
    out = {
        "key": key_text,
        "kind": kind,
        "class_name": class_name,
        "name": _text(getattr(info, "name", None)) or _text(getattr(info, "object_id", None)) or f"Entity {key_text}",
        "object_id": _text(getattr(info, "object_id", None)),
        "unit": _text(getattr(info, "unit_of_measurement", None)) or _text(getattr(info, "unit", None)),
        "device_class": _text(getattr(info, "device_class", None)),
        "entity_category": _text(getattr(info, "entity_category", None)),
        "icon": _text(getattr(info, "icon", None)),
        "disabled_by_default": bool(getattr(info, "disabled_by_default", False)),
    }
    for attr in ("min_value", "max_value", "step"):
        scalar = _esphome_scalar(getattr(info, attr, None))
        if scalar not in {None, ""}:
            out[attr] = scalar
    for attr in ("options", "effects", "supported_color_modes"):
        values = _esphome_list_values(getattr(info, attr, None))
        if values:
            out[attr] = values
    return out



def _esphome_entity_state_snapshot(state: Any) -> Dict[str, Any]:
    if state is None:
        return {}
    key = getattr(state, "key", None)
    if key is None:
        return {}
    key_text = _text(key)
    if not key_text:
        return {}
    raw_value = None
    for attr in ("state", "value"):
        if not hasattr(state, attr):
            continue
        candidate = getattr(state, attr, None)
        if callable(candidate):
            continue
        raw_value = candidate
        break
    kind = _esphome_class_token(state, "State")
    attrs = _esphome_entity_state_attrs(state)
    return {
        "key": key_text,
        "kind": kind,
        "raw": _esphome_scalar(raw_value),
        "attrs": attrs,
        "updated_ts": _now(),
    }



def _esphome_entity_rows(entity_infos: Any, entity_states: Any) -> List[Dict[str, Any]]:
    infos = entity_infos if isinstance(entity_infos, dict) else {}
    states = entity_states if isinstance(entity_states, dict) else {}
    rows: List[Dict[str, Any]] = []
    for key, info in infos.items():
        if not isinstance(info, dict):
            continue
        state_row = states.get(key) if isinstance(states.get(key), dict) else {}
        row: Dict[str, Any] = {
            "key": _text(info.get("key") or key),
            "label": _text(info.get("name")) or _text(info.get("object_id")) or f"Entity {key}",
            "value": _esphome_entity_display_value(info, state_row),
            "kind": _text(info.get("kind")),
            "meta": _esphome_entity_meta_label(info, state_row),
        }
        control = _esphome_entity_control_spec(info, state_row)
        if control:
            row["control"] = control
        rows.append(row)
    rows.sort(key=lambda row: (_lower(row.get("label")), _lower(row.get("kind"))))
    return rows


async def list_entity_catalog(client: Any) -> Dict[str, Dict[str, Any]]:
    method = getattr(client, "list_entities_services", None)
    if not callable(method):
        method = getattr(client, "list_entities", None)
    if not callable(method):
        return {}
    result = method()
    if inspect.isawaitable(result):
        result = await result
    parts: List[Any] = []
    if isinstance(result, tuple):
        parts.extend(list(result))
    elif isinstance(result, list):
        parts.append(result)
    else:
        parts.append(result)
    out: Dict[str, Dict[str, Any]] = {}
    for part in parts:
        if not isinstance(part, (list, tuple)):
            continue
        for item in part:
            snap = _esphome_entity_info_snapshot(item)
            key = _text(snap.get("key"))
            if key:
                out[key] = snap
    return out


async def subscribe_states(selector: str, client: Any) -> Optional[Callable[[], None]]:
    method = getattr(client, "subscribe_states", None)
    if not callable(method):
        return None
    token = _text(selector)

    def _on_state(state: Any) -> None:
        snap = _esphome_entity_state_snapshot(state)
        key = _text(snap.get("key"))
        if not key:
            return
        row = _native_clients.get(token)
        if not isinstance(row, dict):
            return
        state_map = row.get("entity_states")
        if not isinstance(state_map, dict):
            state_map = {}
            row["entity_states"] = state_map
        state_map[key] = snap
        row["entity_state_updated_ts"] = _now()

    try:
        result = method(_on_state)
    except TypeError:
        result = method(on_state=_on_state)
    if inspect.isawaitable(result):
        result = await result
    if callable(result):
        return result
    return None



def _esphome_log_enum_value(module: Any, level_name: str) -> Any:
    enum_cls = esphome_module_attr(module, "LogLevel")
    if enum_cls is None:
        return None
    token = _text(level_name).strip().upper()
    if not token:
        return None
    if not token.startswith("LOG_LEVEL_"):
        token = f"LOG_LEVEL_{token}"
    return getattr(enum_cls, token, None)



def _esphome_log_buffer(row: Dict[str, Any]) -> Deque[Dict[str, Any]]:
    buffer = row.get("log_lines")
    if isinstance(buffer, deque):
        if buffer.maxlen != _ESPHOME_LOG_BUFFER_LIMIT:
            buffer = deque(buffer, maxlen=_ESPHOME_LOG_BUFFER_LIMIT)
            row["log_lines"] = buffer
        return buffer
    buffer = deque(maxlen=_ESPHOME_LOG_BUFFER_LIMIT)
    row["log_lines"] = buffer
    return buffer



def _esphome_format_log_level(module: Any, raw_level: Any) -> str:
    with contextlib.suppress(Exception):
        enum_cls = esphome_module_attr(module, "LogLevel")
        if enum_cls is not None and raw_level is not None:
            token = enum_cls(int(raw_level)).name
            if token.startswith("LOG_LEVEL_"):
                token = token[10:]
            return token.lower()
    token = _text(raw_level).strip().lower()
    return token or "info"



def _esphome_log_text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        with contextlib.suppress(Exception):
            return _ANSI_ESCAPE_RE.sub("", bytes(value).decode("utf-8", errors="replace")).strip()
        return ""
    return _ANSI_ESCAPE_RE.sub("", _text(value)).strip()



def _esphome_log_message_text(message: Any) -> str:
    parts: List[str] = []
    tag = _esphome_log_text(getattr(message, "tag", None) or getattr(message, "source", None))
    if tag:
        parts.append(f"[{tag}]")
    body = _esphome_log_text(
        getattr(message, "message", None) or getattr(message, "msg", None) or getattr(message, "text", None)
    )
    if body:
        parts.append(body)
    send_failed = getattr(message, "send_failed", None)
    if parts:
        if bool(send_failed):
            parts.append("(send_failed)")
        return " ".join(part for part in parts if part).strip()
    return _esphome_log_text(message)



def _esphome_append_log_entry(row: Dict[str, Any], *, level: str, message: str, ts_value: Optional[float] = None) -> Dict[str, Any]:
    text = _text(message)
    if not text:
        text = "(empty log line)"
    seq = int(row.get("log_seq") or 0) + 1
    row["log_seq"] = seq
    ts = float(ts_value if ts_value is not None else _now())
    time_label = _format_ts_label(ts)
    entry = {
        "seq": seq,
        "ts": ts,
        "time": time_label,
        "level": _text(level).lower() or "info",
        "message": text,
        "display": f"{time_label} [{_text(level).upper() or 'INFO'}] {text}",
    }
    _esphome_log_buffer(row).append(entry)
    row["log_last_line_ts"] = ts
    return entry



def _esphome_log_entries_after(row: Dict[str, Any], after_seq: int = 0, *, limit: int = 250) -> List[Dict[str, Any]]:
    entries = list(_esphome_log_buffer(row))
    if after_seq > 0:
        entries = [entry for entry in entries if int(entry.get("seq") or 0) > int(after_seq)]
    if limit > 0 and len(entries) > limit:
        entries = entries[-limit:]
    return entries


async def _esphome_disable_device_logs(client: Any, module: Any) -> None:
    method = getattr(client, "subscribe_logs", None)
    if not callable(method):
        return
    level_none = _esphome_log_enum_value(module, "none")
    kwargs: Dict[str, Any] = {"dump_config": False}
    if level_none is not None:
        kwargs["log_level"] = level_none

    def _noop(_: Any) -> None:
        return None

    try:
        result = method(_noop, **kwargs)
    except TypeError:
        result = method(on_log=_noop, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    if callable(result):
        with contextlib.suppress(Exception):
            follow_up = result()
            if inspect.isawaitable(follow_up):
                await follow_up


async def logs_start(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        raise RuntimeError("selector is required")
    module, import_error = esphome_import()
    if module is None:
        raise RuntimeError(f"aioesphomeapi unavailable: {import_error or 'unknown error'}")

    async with _native_lock:
        row = _native_clients.get(token)
        client = row.get("client") if isinstance(row, dict) else None
        if not isinstance(row, dict) or client is None or not bool(row.get("connected")):
            raise RuntimeError("Satellite is not currently connected.")
        row["log_last_access_ts"] = _now()
        row["log_viewers"] = max(1, int(row.get("log_viewers") or 0) + 1)
        existing_unsubscribe = row.get("log_unsubscribe")
        if callable(existing_unsubscribe):
            entries = _esphome_log_entries_after(row, 0)
            return {
                "ok": True,
                "selector": token,
                "active": True,
                "cursor": int(row.get("log_seq") or 0),
                "entries": entries,
                "viewer_count": int(row.get("log_viewers") or 0),
            }

    try:
        method = getattr(client, "subscribe_logs", None)
        if not callable(method):
            raise RuntimeError("ESPHome log subscription is unavailable for this client.")
        log_level = (
            _esphome_log_enum_value(module, "very_verbose")
            or _esphome_log_enum_value(module, "verbose")
            or _esphome_log_enum_value(module, "debug")
            or _esphome_log_enum_value(module, "config")
            or _esphome_log_enum_value(module, "info")
        )

        def _on_log(message: Any) -> None:
            row = _native_clients.get(token)
            if not isinstance(row, dict):
                return
            level = _esphome_format_log_level(module, getattr(message, "level", None))
            line = _esphome_log_message_text(message)
            _esphome_append_log_entry(row, level=level, message=line, ts_value=_now())

        kwargs: Dict[str, Any] = {"dump_config": True}
        if log_level is not None:
            kwargs["log_level"] = log_level
        try:
            result = method(_on_log, **kwargs)
        except TypeError:
            result = method(on_log=_on_log, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        unsubscribe = result if callable(result) else None
    except Exception:
        async with _native_lock:
            row = _native_clients.get(token)
            if isinstance(row, dict):
                row["log_viewers"] = max(0, int(row.get("log_viewers") or 1) - 1)
                row["log_last_access_ts"] = _now()
        raise

    async with _native_lock:
        row = _native_clients.get(token)
        if not isinstance(row, dict):
            raise RuntimeError("Satellite log row is unavailable.")
        row["log_unsubscribe"] = unsubscribe
        row["log_started_ts"] = _now()
        row["log_last_access_ts"] = _now()
        row["log_error"] = ""
        host = _text(row.get("host"))
        device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
        device_label = _text(device_info.get("friendly_name")) or _text(device_info.get("name")) or token
        _esphome_append_log_entry(
            row,
            level="info",
            message=f"Starting log output from {host or token} using ESPHome API.",
            ts_value=_now(),
        )
        _esphome_append_log_entry(
            row,
            level="info",
            message=f"Successful handshake with {device_label} @ {host or token}.",
            ts_value=_now(),
        )
        entries = _esphome_log_entries_after(row, 0)
        return {
            "ok": True,
            "selector": token,
            "active": True,
            "cursor": int(row.get("log_seq") or 0),
            "entries": entries,
            "viewer_count": int(row.get("log_viewers") or 0),
        }


async def logs_poll(selector: str, *, after_seq: int = 0) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        raise RuntimeError("selector is required")
    async with _native_lock:
        row = _native_clients.get(token)
        if not isinstance(row, dict):
            raise RuntimeError("Satellite is unknown.")
        row["log_last_access_ts"] = _now()
        entries = _esphome_log_entries_after(row, after_seq)
        return {
            "ok": True,
            "selector": token,
            "active": callable(row.get("log_unsubscribe")),
            "connected": bool(row.get("connected")),
            "cursor": int(row.get("log_seq") or 0),
            "entries": entries,
            "viewer_count": int(row.get("log_viewers") or 0),
            "error": _text(row.get("log_error")),
        }


async def logs_stop(selector: str, *, force: bool = False, reason: str = "viewer_closed") -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {"ok": True, "selector": token, "stopped": False, "viewer_count": 0}
    async with _native_lock:
        row = _native_clients.get(token)
        client = row.get("client") if isinstance(row, dict) else None
        unsubscribe = row.get("log_unsubscribe") if isinstance(row, dict) else None
        viewers = int(row.get("log_viewers") or 0) if isinstance(row, dict) else 0
        if isinstance(row, dict):
            viewers = 0 if force else max(0, viewers - 1)
            row["log_viewers"] = viewers
            row["log_last_access_ts"] = _now()
            if viewers > 0 and callable(unsubscribe):
                return {"ok": True, "selector": token, "stopped": False, "viewer_count": viewers}
            if callable(unsubscribe):
                row["log_unsubscribe"] = None
            row["log_error"] = _text(reason)
    if callable(unsubscribe):
        with contextlib.suppress(Exception):
            result = unsubscribe()
            if inspect.isawaitable(result):
                await result
    if client is not None:
        with contextlib.suppress(Exception):
            await _esphome_disable_device_logs(client, module=esphome_import()[0])
    return {"ok": True, "selector": token, "stopped": callable(unsubscribe), "viewer_count": 0}


async def logs_cleanup_idle() -> None:
    cutoff = _now() - float(_ESPHOME_LOG_IDLE_SECONDS)
    stale: List[str] = []
    async with _native_lock:
        for selector, row in _native_clients.items():
            if not isinstance(row, dict):
                continue
            if not callable(row.get("log_unsubscribe")):
                continue
            last_access = _as_float(row.get("log_last_access_ts"), 0.0)
            if last_access > 0 and last_access < cutoff:
                stale.append(_text(selector))
    for selector in stale:
        with contextlib.suppress(Exception):
            await logs_stop(selector, force=True, reason="idle_timeout")



def _combine_unsubscribes(*callbacks: Any) -> Optional[Callable[[], None]]:
    valid = [cb for cb in callbacks if callable(cb)]
    if not valid:
        return None

    def _unsubscribe() -> None:
        for cb in valid:
            with contextlib.suppress(Exception):
                result = cb()
                if inspect.isawaitable(result):
                    asyncio.create_task(result)

    return _unsubscribe


async def _call_client_method(client: Any, method_name: str, *, timeout: float) -> Tuple[bool, str]:
    method = getattr(client, method_name, None)
    if not callable(method):
        return False, "unavailable"
    try:
        result = method()
    except TypeError:
        return False, "signature_mismatch"
    except Exception as exc:
        return False, f"error:{exc}"

    try:
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, timeout=timeout)
    except Exception as exc:
        return False, f"error:{exc}"

    return True, "ok"


async def verify_connection(client: Any, *, timeout: float) -> Tuple[bool, str]:
    if client is None:
        return False, "missing_client"

    marker_before = esphome_client_connected(client, fallback=False)
    ping_ok, ping_reason = await _call_client_method(client, "ping", timeout=timeout)
    info_ok, info_reason = await _call_client_method(client, "device_info", timeout=timeout)
    marker_after = esphome_client_connected(client, fallback=False)

    if marker_before or marker_after or ping_ok or info_ok:
        details = f"marker_before={marker_before} marker_after={marker_after} ping={ping_reason} device_info={info_reason}"
        return True, details
    return False, f"marker_before={marker_before} ping={ping_reason} device_info={info_reason}"



def voice_feature_snapshot(info: Any, client: Any, module: Any) -> Dict[str, Any]:
    flags = 0
    api_audio_bit = 0
    speaker_bit = 0
    feature_enum = esphome_module_attr(module, "VoiceAssistantFeature")
    if feature_enum is not None:
        with contextlib.suppress(Exception):
            api_audio_bit = int(getattr(feature_enum, "API_AUDIO"))
        with contextlib.suppress(Exception):
            speaker_bit = int(getattr(feature_enum, "SPEAKER"))

    compat_fn = getattr(info, "voice_assistant_feature_flags_compat", None)
    if callable(compat_fn):
        with contextlib.suppress(Exception):
            api_version = getattr(client, "api_version", None)
            if api_version is not None:
                flags = int(compat_fn(api_version) or 0)
            else:
                flags = int(compat_fn() or 0)

    if not flags:
        for attr in ("voice_assistant_feature_flags", "voice_assistant_feature_flags_compat"):
            value = getattr(info, attr, None)
            if callable(value):
                with contextlib.suppress(Exception):
                    value = value()
            with contextlib.suppress(Exception):
                parsed = int(value or 0)
                if parsed:
                    flags = parsed
                    break

    api_audio_known = bool(api_audio_bit and flags)
    speaker_known = bool(speaker_bit and flags)

    return {
        "flags": int(flags),
        "api_audio_bit": int(api_audio_bit),
        "speaker_bit": int(speaker_bit),
        "api_audio_known": api_audio_known,
        "speaker_known": speaker_known,
        "api_audio_supported": True if not api_audio_known else bool(api_audio_bit and (int(flags) & int(api_audio_bit))),
        "speaker_supported": True if not speaker_known else bool(speaker_bit and (int(flags) & int(speaker_bit))),
    }


async def build_client(module: Any, *, host: str, port: int) -> Any:
    APIClient = getattr(module, "APIClient", None)
    if APIClient is None:
        raise RuntimeError("aioesphomeapi.APIClient is unavailable")

    settings = _voice_settings()
    password = _text(settings.get("VOICE_ESPHOME_PASSWORD"))
    noise_psk = _text(settings.get("VOICE_ESPHOME_NOISE_PSK"))

    if noise_psk:
        try:
            return APIClient(
                address=host,
                port=int(port),
                password=password or None,
                noise_psk=noise_psk,
            )
        except TypeError:
            pass

    try:
        return APIClient(address=host, port=int(port), password=password or None)
    except TypeError:
        return APIClient(host, int(port), password or None)


async def disconnect_selector(selector: str, *, reason: str) -> None:
    token = _text(selector)
    if not token:
        return

    async with _native_lock:
        row = _native_clients.get(token)
        client = row.get("client") if isinstance(row, dict) else None
        unsubscribe = row.get("unsubscribe") if isinstance(row, dict) else None
        log_unsubscribe = row.get("log_unsubscribe") if isinstance(row, dict) else None
        was_connected = bool(row.get("connected", False)) if isinstance(row, dict) else False
        if isinstance(row, dict):
            row["connected"] = False
            row["client"] = None
            row["unsubscribe"] = None
            row["log_unsubscribe"] = None
            row["log_viewers"] = 0
            row["last_disconnect_ts"] = _now()
            row["last_error"] = _text(reason)

    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    sid = ""
    async with lock:
        session = runtime.get("session")
        if _is_voice_session(session):
            sid = _text(getattr(session, "session_id", ""))
        runtime["session"] = None
        _clear_streamed_tts_state(runtime)
        _cancel_announcement_wait(runtime)
        _cancel_audio_stall_watch(runtime)
        runtime["awaiting_announcement"] = False
        runtime["awaiting_session_id"] = ""
        runtime["awaiting_announcement_kind"] = ""
        runtime["announcement_future"] = None

    if sid and client is not None:
        module, _ = esphome_import()
        if module is not None:
            with contextlib.suppress(Exception):
                await _finalize_session(token, client, module, session_id=sid, abort=True, reason=reason or "disconnect")

    if callable(unsubscribe):
        with contextlib.suppress(Exception):
            unsubscribe()
    if callable(log_unsubscribe):
        with contextlib.suppress(Exception):
            result = log_unsubscribe()
            if inspect.isawaitable(result):
                await result

    disconnect_fn = getattr(client, "disconnect", None)
    if callable(disconnect_fn):
        with contextlib.suppress(Exception):
            result = disconnect_fn()
            if inspect.isawaitable(result):
                await result

    if was_connected:
        logger.info("[native-voice] esphome disconnected selector=%s reason=%s", token, _text(reason))
        _voice_metrics_record_connection_event(token, event="disconnect")


async def disconnect_all(reason: str) -> None:
    async with _native_lock:
        selectors = list(_native_clients.keys())
    for selector in selectors:
        await disconnect_selector(selector, reason=reason)


async def connect_selector(selector: str, *, host: str, port: Optional[int] = None, source: str = "reconcile") -> Dict[str, Any]:
    token = _text(selector)
    host_token = _lower(host)
    if not token or not host_token:
        raise RuntimeError("selector and host are required")

    module, import_error = esphome_import()
    if module is None:
        msg = f"aioesphomeapi unavailable: {import_error or 'unknown error'}"
        async with _native_lock:
            row = _native_clients.get(token) or {}
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": int(port or _get_int_setting("VOICE_ESPHOME_API_PORT", _default_port())),
                    "connected": False,
                    "last_attempt_ts": _now(),
                    "last_error": msg,
                    "source": source,
                }
            )
            _native_clients[token] = row
        _voice_metrics_record_connection_event(token, event="error")
        raise RuntimeError(msg)

    timeout = _get_float_setting("VOICE_ESPHOME_CONNECT_TIMEOUT_S", _connect_timeout_s(), minimum=2.0, maximum=60.0)
    connect_port = int(port or _get_int_setting("VOICE_ESPHOME_API_PORT", _default_port()))

    _native_debug(f"esphome connect attempt selector={token} host={host_token} port={connect_port} source={source}")

    async with _native_lock:
        row = _native_clients.get(token) or {}
        row.update(
            {
                "selector": token,
                "host": host_token,
                "port": connect_port,
                "connected": False,
                "last_attempt_ts": _now(),
                "source": source,
            }
        )
        _native_clients[token] = row

    try:
        client = await build_client(module, host=host_token, port=connect_port)
        connect_fn = getattr(client, "connect", None)
        if not callable(connect_fn):
            raise RuntimeError("aioesphomeapi client has no connect()")

        kwargs: Dict[str, Any] = {}
        with contextlib.suppress(Exception):
            sig = inspect.signature(connect_fn)
            if "login" in sig.parameters:
                kwargs["login"] = True
            if "on_stop" in sig.parameters:

                async def _on_stop(expected_disconnect: bool) -> None:
                    await disconnect_selector(token, reason="expected_disconnect" if expected_disconnect else "connection_lost")

                kwargs["on_stop"] = _on_stop

        result = connect_fn(**kwargs) if kwargs else connect_fn()
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, timeout=timeout)

        await asyncio.sleep(0.2)
        verified, verify_reason = await verify_connection(client, timeout=max(1.0, timeout))
        _native_debug(f"esphome connect verification selector={token} verified={verified} details={verify_reason}")
        if not verified:
            raise RuntimeError(f"ESPHome API connection could not be verified. Details: {verify_reason}")

        device_name = token
        device_info_snapshot: Dict[str, Any] = {}
        entity_infos: Dict[str, Dict[str, Any]] = {}
        voice_features = {
            "flags": 0,
            "api_audio_bit": 0,
            "speaker_bit": 0,
            "api_audio_known": False,
            "speaker_known": False,
            "api_audio_supported": True,
            "speaker_supported": True,
        }

        with contextlib.suppress(Exception):
            info = await esphome_client_call(client, "device_info")
            device_info_snapshot = _esphome_device_info_snapshot(info)
            for candidate in (getattr(info, "friendly_name", None), getattr(info, "name", None)):
                label = _text(candidate)
                if label:
                    device_name = label
                    break
            voice_features = voice_feature_snapshot(info, client, module)

        with contextlib.suppress(Exception):
            entity_infos = await list_entity_catalog(client)

        voice_unsubscribe = await _esphome_subscribe_voice_assistant(
            token,
            client,
            module,
            api_audio_supported=bool(voice_features.get("api_audio_supported")),
        )
        state_unsubscribe = await subscribe_states(token, client)
        unsubscribe = _combine_unsubscribes(voice_unsubscribe, state_unsubscribe)

        logger.info(
            "[native-voice] esphome voice features selector=%s flags=%s flags_known=%s api_audio_supported=%s speaker_supported=%s",
            token,
            int(voice_features.get("flags") or 0),
            bool(voice_features.get("api_audio_known")) or bool(voice_features.get("speaker_known")),
            bool(voice_features.get("api_audio_supported")),
            bool(voice_features.get("speaker_supported")),
        )

        _upsert_satellite(
            {
                "selector": token,
                "host": host_token,
                "name": device_name,
                "source": "esphome_native",
                "metadata": {
                    "esphome_selected": True,
                    "esphome_port": connect_port,
                    "voice_feature_flags": int(voice_features.get("flags") or 0),
                    "voice_api_audio_supported": bool(voice_features.get("api_audio_supported")),
                    "voice_speaker_supported": bool(voice_features.get("speaker_supported")),
                },
            }
        )

        async with _native_lock:
            row = _native_clients.get(token) or {}
            reconnect = bool(_as_float(row.get("last_success_ts"), 0.0) > 0.0 or _as_float(row.get("last_disconnect_ts"), 0.0) > 0.0)
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": connect_port,
                    "client": client,
                    "unsubscribe": unsubscribe,
                    "connected": True,
                    "device_info": dict(device_info_snapshot),
                    "entity_infos": dict(entity_infos),
                    "entity_states": row.get("entity_states") if isinstance(row.get("entity_states"), dict) else {},
                    "entity_state_updated_ts": _as_float(row.get("entity_state_updated_ts"), _now()),
                    "voice_feature_flags": int(voice_features.get("flags") or 0),
                    "voice_api_audio_supported": bool(voice_features.get("api_audio_supported")),
                    "voice_speaker_supported": bool(voice_features.get("speaker_supported")),
                    "last_success_ts": _now(),
                    "last_error": "",
                    "source": source,
                }
            )
            _native_clients[token] = row
            if reconnect:
                _voice_metrics_record_connection_event(token, event="reconnect")
            logger.info(
                "[native-voice] esphome connected selector=%s host=%s port=%s source=%s api_audio_supported=%s speaker_supported=%s",
                token,
                host_token,
                connect_port,
                source,
                bool(row.get("voice_api_audio_supported")),
                bool(row.get("voice_speaker_supported")),
            )
            return dict(row)

    except Exception as exc:
        unsubscribe_cb = locals().get("unsubscribe")
        if callable(unsubscribe_cb):
            with contextlib.suppress(Exception):
                unsubscribe_cb()

        client_obj = locals().get("client")
        disconnect_fn = getattr(client_obj, "disconnect", None)
        if callable(disconnect_fn):
            with contextlib.suppress(Exception):
                result = disconnect_fn()
                if inspect.isawaitable(result):
                    await asyncio.wait_for(result, timeout=max(1.0, timeout))

        msg = _text(exc)
        _native_debug(f"esphome connect failed selector={token} host={host_token} error={msg}")

        async with _native_lock:
            row = _native_clients.get(token) or {}
            row.update(
                {
                    "selector": token,
                    "host": host_token,
                    "port": connect_port,
                    "connected": False,
                    "last_error": msg,
                    "source": source,
                }
            )
            _native_clients[token] = row
        _voice_metrics_record_connection_event(token, event="error")
        raise


async def reconcile_once(*, force: bool = False) -> Dict[str, Any]:
    async with _reconcile_lock:
        _native_stats["runs"] = int(_native_stats.get("runs") or 0) + 1
        _native_stats["last_run_ts"] = _now()

        targets = target_map()
        retry_seconds = _get_int_setting(
            "VOICE_ESPHOME_RETRY_SECONDS",
            _retry_seconds(),
            minimum=2,
            maximum=300,
        )

        async with _native_lock:
            snapshot = {k: dict(v) for k, v in _native_clients.items()}

        for selector, row in snapshot.items():
            if selector not in targets:
                await disconnect_selector(selector, reason="not_targeted")
                continue
            client = row.get("client")
            connected_row = bool(row.get("connected", False))
            if connected_row and not esphome_client_connected(client, fallback=connected_row):
                await disconnect_selector(selector, reason="connection_lost")

        for selector, host in targets.items():
            row = snapshot.get(selector) or {}
            if bool(row.get("connected", False)) and esphome_client_connected(row.get("client"), fallback=True):
                continue
            last_attempt = _as_float(row.get("last_attempt_ts"), 0.0)
            if (not force) and ((_now() - last_attempt) < retry_seconds):
                continue
            try:
                await connect_selector(selector, host=host, source="reconcile")
                _native_stats["last_success_ts"] = _now()
                _native_stats["last_error"] = ""
            except Exception as exc:
                _native_stats["last_error"] = _text(exc)

        return status()


async def esphome_loop() -> None:
    while True:
        try:
            await reconcile_once(force=False)
            await logs_cleanup_idle()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _native_stats["last_error"] = _text(exc)
            _native_stats["last_run_ts"] = _now()
            logger.warning("[native-voice] esphome reconcile loop error: %s", exc)
        await asyncio.sleep(float(max(2, _get_int_setting("VOICE_ESPHOME_RETRY_SECONDS", _retry_seconds()))))


async def bootstrap_reconnect() -> None:
    try:
        current = await reconcile_once(force=True)
        logger.info(
            "[native-voice] startup esphome reconcile selected=%s connected=%s",
            len(current.get("targets") or {}),
            len(
                [
                    row
                    for row in (current.get("clients") or {}).values()
                    if isinstance(row, dict) and bool(row.get("connected"))
                ]
            ),
        )
    except Exception as exc:
        logger.warning("[native-voice] startup esphome reconcile failed: %s", exc)

    try:
        await asyncio.sleep(2.0)
        current = await reconcile_once(force=True)
        logger.info(
            "[native-voice] delayed esphome reconcile selected=%s connected=%s",
            len(current.get("targets") or {}),
            len(
                [
                    row
                    for row in (current.get("clients") or {}).values()
                    if isinstance(row, dict) and bool(row.get("connected"))
                ]
            ),
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("[native-voice] delayed esphome reconcile failed: %s", exc)



def status() -> Dict[str, Any]:
    module, import_error = esphome_import()
    targets = target_map()
    metrics_snapshot = _voice_metrics_snapshot()
    device_metrics = metrics_snapshot.get("devices") if isinstance(metrics_snapshot.get("devices"), dict) else {}

    clients: Dict[str, Any] = {}
    for selector, row in _native_clients.items():
        if not isinstance(row, dict):
            continue
        runtime = _selector_runtime(selector)
        session = runtime.get("session")
        device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
        entity_infos = row.get("entity_infos") if isinstance(row.get("entity_infos"), dict) else {}
        entity_states = row.get("entity_states") if isinstance(row.get("entity_states"), dict) else {}
        entity_rows = _esphome_entity_rows(entity_infos, entity_states)
        metrics_row = device_metrics.get(selector) if isinstance(device_metrics.get(selector), dict) else {}
        clients[selector] = {
            "selector": _text(row.get("selector") or selector),
            "host": _text(row.get("host")),
            "port": int(row.get("port") or _get_int_setting("VOICE_ESPHOME_API_PORT", _default_port())),
            "connected": bool(row.get("connected", False)),
            "selected": selector in targets,
            "voice_subscribed": bool(row.get("unsubscribe")),
            "active_session_id": _text(getattr(session, "session_id", "")) if _is_voice_session(session) else "",
            "voice_feature_flags": int(row.get("voice_feature_flags") or 0),
            "voice_api_audio_supported": bool(row.get("voice_api_audio_supported")),
            "voice_speaker_supported": bool(row.get("voice_speaker_supported")),
            "last_attempt_ts": _as_float(row.get("last_attempt_ts"), 0.0),
            "last_success_ts": _as_float(row.get("last_success_ts"), 0.0),
            "last_disconnect_ts": _as_float(row.get("last_disconnect_ts"), 0.0),
            "last_error": _text(row.get("last_error")),
            "source": _text(row.get("source")),
            "device_info": dict(device_info),
            "entity_count": len(entity_infos),
            "entity_row_count": len(entity_rows),
            "entity_rows": entity_rows,
            "entity_state_updated_ts": _as_float(row.get("entity_state_updated_ts"), 0.0),
            "log_active": callable(row.get("log_unsubscribe")),
            "log_viewers": int(row.get("log_viewers") or 0),
            "log_cursor": int(row.get("log_seq") or 0),
            "log_last_line_ts": _as_float(row.get("log_last_line_ts"), 0.0),
            "log_last_access_ts": _as_float(row.get("log_last_access_ts"), 0.0),
            "log_error": _text(row.get("log_error")),
            "voice_metrics": dict(metrics_row),
        }

    return {
        "enabled": True,
        "available": module is not None,
        "import_error": "" if module is not None else _text(import_error),
        "targets": targets,
        "clients": clients,
        "stats": dict(_native_stats),
        "voice_metrics": metrics_snapshot,
    }



def entities_for_selector(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        raise ValueError("selector is required")

    row = _native_clients.get(token)
    if not isinstance(row, dict):
        raise RuntimeError(f"Satellite {token} is unknown")

    entity_infos = row.get("entity_infos") if isinstance(row.get("entity_infos"), dict) else {}
    entity_states = row.get("entity_states") if isinstance(row.get("entity_states"), dict) else {}
    device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
    entity_rows = _esphome_entity_rows(entity_infos, entity_states)

    entities: List[Dict[str, Any]] = []
    for key, info in entity_infos.items():
        if not isinstance(info, dict):
            continue
        state_row = entity_states.get(key) if isinstance(entity_states.get(key), dict) else {}
        entities.append(
            {
                "key": _text(info.get("key") or key),
                "kind": _text(info.get("kind")),
                "name": _text(info.get("name")) or _text(info.get("object_id")) or f"Entity {key}",
                "object_id": _text(info.get("object_id")),
                "unit": _text(info.get("unit")),
                "device_class": _text(info.get("device_class")),
                "entity_category": _text(info.get("entity_category")),
                "icon": _text(info.get("icon")),
                "disabled_by_default": bool(info.get("disabled_by_default")),
                "value": _esphome_entity_display_value(info, state_row),
                "meta": _esphome_entity_meta_label(info, state_row),
                "raw": state_row.get("raw"),
                "attrs": dict(state_row.get("attrs") or {}) if isinstance(state_row, dict) else {},
                "updated_ts": _as_float(state_row.get("updated_ts"), 0.0) if isinstance(state_row, dict) else 0.0,
            }
        )

    entities.sort(key=lambda item: (_lower(item.get("name")), _lower(item.get("kind")), _lower(item.get("object_id"))))
    return {
        "selector": token,
        "connected": bool(row.get("connected")),
        "host": _text(row.get("host")),
        "device_info": dict(device_info),
        "entities": entities,
        "entity_rows": entity_rows,
        "count": len(entities),
    }



def _store_entity_state_override(selector: str, key: str, kind: str, raw: Any, attrs: Dict[str, Any]) -> None:
    token = _text(selector)
    entry_key = _text(key)
    if not token or not entry_key:
        return
    row = _native_clients.get(token)
    if not isinstance(row, dict):
        return
    state_map = row.get("entity_states")
    if not isinstance(state_map, dict):
        state_map = {}
        row["entity_states"] = state_map
    state_map[entry_key] = {
        "key": entry_key,
        "kind": _text(kind),
        "raw": raw,
        "attrs": dict(attrs or {}),
        "updated_ts": _now(),
    }
    row["entity_state_updated_ts"] = _now()



def _hex_to_rgb(color_value: Any) -> Tuple[float, float, float]:
    token = _text(color_value).lstrip("#")
    if len(token) == 3:
        token = "".join(ch * 2 for ch in token)
    if len(token) != 6 or any(ch not in "0123456789abcdefABCDEF" for ch in token):
        raise RuntimeError(f"Invalid color value: {color_value}")
    return (
        int(token[0:2], 16) / 255.0,
        int(token[2:4], 16) / 255.0,
        int(token[4:6], 16) / 255.0,
    )


async def command_entity(
    selector: str,
    *,
    entity_key: Any,
    command: str,
    value: Any = None,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    token = _text(selector)
    key_text = _text(entity_key)
    action = _lower(command)
    if not token:
        raise ValueError("selector is required")
    if not key_text:
        raise ValueError("entity_key is required")
    if not action:
        raise ValueError("command is required")
    payload = options if isinstance(options, dict) else {}

    async with _native_lock:
        client_row = dict(_native_clients.get(token) or {})
    client = client_row.get("client")
    if not bool(client_row.get("connected")) or client is None:
        raise RuntimeError(f"Satellite {token} is not connected")

    row = _native_clients.get(token)
    entity_infos = row.get("entity_infos") if isinstance(row, dict) and isinstance(row.get("entity_infos"), dict) else {}
    info = entity_infos.get(key_text) if isinstance(entity_infos.get(key_text), dict) else {}
    if not info:
        raise RuntimeError(f"Entity {key_text} was not found on satellite {token}")

    kind = _lower(info.get("kind"))
    key_num = _as_int(key_text, 0, minimum=0)
    if key_num <= 0:
        raise RuntimeError(f"Entity {key_text} has an invalid key")

    if action in {"press", "button_press"}:
        if "button" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a button")
        await esphome_client_call(client, "button_command", key_num)
    elif action in {"number_set", "set_number"}:
        if "number" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a number")
        try:
            numeric = float(value)
        except Exception as exc:
            raise RuntimeError(f"number_set requires a numeric value: {exc}") from exc
        await esphome_client_call(client, "number_command", key_num, numeric)
        _store_entity_state_override(token, key_text, _text(info.get("kind")), numeric, {"state": numeric})
    elif action in {"switch_set", "set_switch"}:
        if "switch" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a switch")
        state = _as_bool(value, False)
        await esphome_client_call(client, "switch_command", key_num, state)
        _store_entity_state_override(token, key_text, _text(info.get("kind")), state, {"state": state})
    elif action in {"light_set", "set_light"}:
        if "light" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a light")
        state_value = payload.get("state", value)
        brightness_value = payload.get("brightness")
        color_value = payload.get("color")
        command_kwargs: Dict[str, Any] = {}
        attrs_override: Dict[str, Any] = {}
        raw_override = None
        if state_value not in {None, ""}:
            state_bool = _as_bool(state_value, False)
            command_kwargs["state"] = state_bool
            attrs_override["state"] = state_bool
            raw_override = state_bool
        if brightness_value not in {None, ""}:
            brightness = _as_float(brightness_value, 1.0, minimum=0.0, maximum=1.0)
            command_kwargs["brightness"] = brightness
            attrs_override["brightness"] = brightness
        if color_value not in {None, ""}:
            rgb = _hex_to_rgb(color_value)
            command_kwargs["rgb"] = rgb
            command_kwargs.setdefault("state", True)
            attrs_override.update(
                {
                    "state": True,
                    "red": rgb[0],
                    "green": rgb[1],
                    "blue": rgb[2],
                    "color_mode": "rgb",
                }
            )
            raw_override = True
        if not command_kwargs:
            raise RuntimeError("light_set requires a state, brightness, or color value")
        await esphome_client_call(client, "light_command", key_num, **command_kwargs)
        _store_entity_state_override(
            token,
            key_text,
            _text(info.get("kind")),
            raw_override,
            attrs_override,
        )
    elif action in {"select_set", "set_select"}:
        if "select" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a select")
        state = _text(value)
        if not state:
            raise RuntimeError("select_set requires a state value")
        await esphome_client_call(client, "select_command", key_num, state)
        _store_entity_state_override(token, key_text, _text(info.get("kind")), state, {"state": state})
    elif action in {"text_set", "set_text"}:
        if "text" not in kind:
            raise RuntimeError(f"Entity {key_text} is not a text entity")
        state = _text(value)
        await esphome_client_call(client, "text_command", key_num, state)
        _store_entity_state_override(token, key_text, _text(info.get("kind")), state, {"state": state})
    else:
        raise RuntimeError(f"Unsupported command: {command}")

    return entities_for_selector(token)
