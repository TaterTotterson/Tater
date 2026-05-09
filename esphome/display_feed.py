from __future__ import annotations

import math
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from helpers import redis_client
from integration_runtime import integration_runtime_states
from . import display_bus


_DEFAULT_SLOT_LABELS: Dict[str, str] = {
    "temp_out": "Outdoor Temperature",
    "temp_in": "Indoor Temperature",
    "humidity_out": "Outdoor Humidity",
    "humidity_in": "Indoor Humidity",
    "wind_speed": "Wind Speed",
    "rain_rate": "Rain Rate",
    "lightning_strikes": "Lightning Strikes",
}

_RESERVED_QUERY_KEYS = {
    "format",
    "limit",
    "token",
    "x_tater_token",
}
_ENTITY_LIST_QUERY_KEYS = {"entity", "entities", "entity_id", "entity_ids", "slots"}
_ENTITY_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_]+$")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace").strip()
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_float(value: Any) -> Optional[float]:
    token = _text(value)
    if not token:
        return None
    try:
        out = float(token)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _local_clock() -> Dict[str, Any]:
    now = datetime.now().astimezone()
    return {
        "epoch": time.time(),
        "iso": now.isoformat(),
        "date": now.strftime("%A %b %d"),
        "time": now.strftime("%I:%M").lstrip("0") or "0:00",
        "ampm": now.strftime("%p"),
    }


def _slug(value: Any, fallback: str = "entity") -> str:
    token = _text(value)
    if "." in token:
        token = token.split(".", 1)[1]
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", token).strip("_").lower()
    return clean or fallback


def _iter_query_items(query: Any) -> Iterable[Tuple[str, str]]:
    if query is None:
        return []
    multi_items = getattr(query, "multi_items", None)
    if callable(multi_items):
        return [(_text(key), _text(value)) for key, value in multi_items()]
    if isinstance(query, Mapping):
        rows = []
        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                rows.extend((_text(key), _text(item)) for item in value)
            else:
                rows.append((_text(key), _text(value)))
        return rows
    return []


def _add_entity_spec(slots: Dict[str, str], raw_spec: Any) -> None:
    spec = _text(raw_spec)
    if not spec:
        return
    alias = ""
    entity_id = spec
    if ":" in spec:
        left, right = spec.split(":", 1)
        alias = _slug(left, "slot")
        entity_id = _text(right)
    if not _ENTITY_ID_RE.match(entity_id):
        return
    slots[alias or _slug(entity_id)] = entity_id


def _slot_map_from_query(query: Any) -> Dict[str, str]:
    slots: Dict[str, str] = {}
    for raw_key, raw_value in _iter_query_items(query):
        key = _text(raw_key)
        value = _text(raw_value)
        if not key or not value:
            continue
        key_lower = _lower(key)
        if key_lower in _RESERVED_QUERY_KEYS:
            continue
        if key_lower in _ENTITY_LIST_QUERY_KEYS:
            for part in value.split(","):
                _add_entity_spec(slots, part)
            continue
        if key_lower.startswith("slot_"):
            alias = _slug(key[5:], "slot")
        else:
            alias = _slug(key, "slot")
        if _ENTITY_ID_RE.match(value):
            slots[alias] = value
    return slots


def _target_from_query(query: Any) -> str:
    for raw_key, raw_value in _iter_query_items(query):
        key = _lower(raw_key)
        if key in {"target", "device", "selector"}:
            return _text(raw_value)
    return ""


def _runtime_homeassistant_states(client: Any = None) -> Dict[str, Dict[str, Any]]:
    try:
        snapshot = integration_runtime_states(client or redis_client)
    except Exception:
        return {}
    rows = snapshot.get("states") if isinstance(snapshot, dict) and isinstance(snapshot.get("states"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for record in rows:
        if not isinstance(record, dict) or _lower(record.get("provider")) != "homeassistant":
            continue
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        entity_id = _text(payload.get("entity_id") or record.get("id"))
        if entity_id:
            out[entity_id] = record
    return out


def _display_text(state: str, unit: str) -> str:
    token = _text(state)
    if not token:
        return ""
    if _lower(token) in {"unknown", "unavailable", "none"}:
        return token
    suffix = _text(unit)
    if not suffix:
        return token
    if suffix in {"%", "°", "°C", "°F"}:
        return f"{token}{suffix}" if suffix == "%" else f"{token} {suffix}"
    return f"{token} {suffix}"


def _normalize_homeassistant_slot(
    alias: str,
    entity_id: str,
    *,
    runtime_record: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    record = runtime_record if isinstance(runtime_record, dict) else {}
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    source = "runtime_cache" if payload else ""

    attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    state = _text(payload.get("state"))
    unit = _text(attrs.get("unit_of_measurement"))
    numeric = _as_float(state)
    available = bool(payload) and _lower(state) not in {"", "unknown", "unavailable", "none"}
    updated_at = record.get("updated_at") if record else None
    if updated_at in (None, ""):
        updated_at = payload.get("last_updated") or payload.get("last_changed") or ""

    return {
        "alias": _slug(alias, "slot"),
        "entity_id": _text(entity_id),
        "name": _text(attrs.get("friendly_name")) or _DEFAULT_SLOT_LABELS.get(alias) or _slug(alias, "Sensor"),
        "available": available,
        "state": state,
        "value": numeric if numeric is not None else state,
        "numeric_value": numeric,
        "unit": unit,
        "display": _display_text(state, unit),
        "device_class": _text(attrs.get("device_class")),
        "state_class": _text(attrs.get("state_class")),
        "icon": _text(attrs.get("icon")),
        "updated_at": updated_at,
        "source": source or "missing",
    }


def build_display_feed(query: Any = None, *, client: Any = None, version: str = "") -> Dict[str, Any]:
    redis_obj = client or redis_client
    requested_slots = _slot_map_from_query(query)
    runtime_states = _runtime_homeassistant_states(redis_obj)

    slots: Dict[str, Dict[str, Any]] = {}
    values: Dict[str, Any] = {}
    text_values: Dict[str, str] = {}
    for alias, entity_id in requested_slots.items():
        runtime_record = runtime_states.get(entity_id)
        slot = _normalize_homeassistant_slot(
            alias,
            entity_id,
            runtime_record=runtime_record,
        )
        clean_alias = _text(slot.get("alias")) or alias
        slots[clean_alias] = slot
        values[clean_alias] = slot.get("numeric_value") if slot.get("numeric_value") is not None else slot.get("state")
        text_values[clean_alias] = _text(slot.get("display") or slot.get("state"))

    return {
        "ok": True,
        "service": "tater_display",
        "version": _text(version),
        "ts": time.time(),
        "clock": _local_clock(),
        "slots": slots,
        "values": values,
        "text": text_values,
        "events": display_bus.display_feed_events(_target_from_query(query), client=redis_obj),
        "count": len(slots),
    }
