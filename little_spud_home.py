from __future__ import annotations

import hashlib
import hmac
import math
import re
import time
from typing import Any, Dict, Iterable, List, Tuple


CONTROL_ACTIONS: Dict[str, Tuple[str, ...]] = {
    "light": ("turn_on", "turn_off", "set_brightness"),
    "fan": ("turn_on", "turn_off"),
    "switch": ("turn_on", "turn_off"),
    "plug": ("turn_on", "turn_off"),
    "garage_door": ("open", "close"),
    "cover": ("open", "close"),
    "lock": ("lock", "unlock"),
}

_ON_STATES = {"on", "true", "active", "running", "playing", "home", "present", "detected", "motion", "wet"}
_OFF_STATES = {"off", "false", "inactive", "idle", "paused", "away", "absent", "clear", "dry"}
_OPEN_STATES = {"open", "opening"}
_CLOSED_STATES = {"closed", "closing"}
_LOCKED_STATES = {"locked", "locking"}
_UNLOCKED_STATES = {"unlocked", "unlocking"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _text(value).lower()).strip("_")


def _list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _actions(device: Dict[str, Any]) -> set[str]:
    return {_token(item) for item in _list(device.get("actions")) if _token(item)}


def _category_ids(device: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for item in _list(device.get("category_ids") or device.get("capabilities")):
        value = _token(item)
        if value and value not in out:
            out.append(value)
    device_type = _token(device.get("type"))
    if not out and device_type:
        out.append(device_type)
    return out or ["device"]


def _display_category_ids(device: Dict[str, Any], order: Dict[str, int]) -> List[str]:
    categories = sorted(
        _category_ids(device),
        key=lambda item: (order.get(item, 1000), item),
    )
    device_actions = _actions(device)
    control_categories = [item for item in categories if item in CONTROL_ACTIONS]
    actionable = [
        item
        for item in control_categories
        if device_actions.intersection(CONTROL_ACTIONS.get(item, ()))
    ]
    sensor_categories = [
        item
        for item in categories
        if item not in CONTROL_ACTIONS and item not in {"sensor", "device"}
    ]
    if actionable:
        # A light commonly also reports itself as a generic switch. Show the most
        # specific control once, while retaining any measurements it also reports.
        return [actionable[0], *sensor_categories]
    if control_categories:
        # A state-only cover or garage door still belongs in the room's glance
        # section even when its integration exposes no safe control action.
        return [control_categories[0], *sensor_categories]
    return sensor_categories or categories[:1]


def _room_identity(device: Dict[str, Any]) -> Tuple[str, str]:
    name = _text(device.get("room") or device.get("area")) or "Unassigned"
    room_id = _token(device.get("room_id")) or _token(name) or "unassigned"
    return room_id, name


def _walk_mappings(value: Any, depth: int = 0) -> Iterable[Dict[str, Any]]:
    if not isinstance(value, dict) or depth > 3:
        return
    yield value
    preferred = ("details", "runtime_state", "payload", "new_state", "attributes", "raw", "result", "status")
    visited: set[str] = set()
    for key in preferred:
        child = value.get(key)
        if isinstance(child, dict):
            visited.add(key)
            yield from _walk_mappings(child, depth + 1)
    for key, child in value.items():
        if key in visited or not isinstance(child, dict):
            continue
        yield from _walk_mappings(child, depth + 1)


def _lookup(device: Dict[str, Any], *keys: str) -> Any:
    mappings = list(_walk_mappings(device))
    for wanted in (_token(key) for key in keys):
        for mapping in mappings:
            for key, value in mapping.items():
                if (
                    _token(key) == wanted
                    and value not in (None, "")
                    and not isinstance(value, (dict, list, tuple, set))
                ):
                    return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    match = re.search(r"-?\d+(?:\.\d+)?", _text(value).replace(",", ""))
    if not match:
        return None
    try:
        numeric = float(match.group(0))
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _state(device: Dict[str, Any]) -> str:
    value = _lookup(device, "state", "status")
    return _text(value).lower()


def _power_state(device: Dict[str, Any]) -> str:
    state = _token(_state(device))
    if state in _ON_STATES:
        return "on"
    if state in _OFF_STATES:
        return "off"
    on_value = _lookup(device, "on", "is_on", "ison")
    if isinstance(on_value, bool):
        return "on" if on_value else "off"
    return "unknown"


def _cover_state(device: Dict[str, Any]) -> str:
    state = _token(_state(device))
    if state in _OPEN_STATES:
        return state
    if state in _CLOSED_STATES:
        return state
    open_value = _lookup(device, "open", "is_open")
    if isinstance(open_value, bool):
        return "open" if open_value else "closed"
    return "unknown"


def _lock_state(device: Dict[str, Any]) -> str:
    state = _token(_state(device))
    if state in _LOCKED_STATES:
        return state
    if state in _UNLOCKED_STATES:
        return state
    return "unknown"


def _brightness(device: Dict[str, Any]) -> float | None:
    value = _number(_lookup(device, "brightness_pct", "brightness", "level", "dimming"))
    if value is None:
        return None
    if 100 < value <= 255:
        value = value / 255 * 100
    return max(0.0, min(100.0, value))


def _format_number(value: float, decimals: int = 0) -> str:
    if decimals <= 0 or abs(value - round(value)) < 0.05:
        return str(int(round(value)))
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


def _average(values: Iterable[float]) -> float | None:
    rows = [value for value in values if math.isfinite(value)]
    return sum(rows) / len(rows) if rows else None


def _temperature_unit(device: Dict[str, Any]) -> str:
    unit = _text(_lookup(device, "temperature_unit", "unit_of_measurement", "unit")).upper()
    if not unit:
        unit = _state(device).upper()
    if "C" in unit and "F" not in unit:
        return "C"
    return "F"


def _sensor_summary(category_id: str, devices: List[Dict[str, Any]]) -> str:
    states = [_token(_state(device)) for device in devices]
    if category_id == "temperature":
        values = [
            _number(_lookup(device, "temperature", "current_temperature", "current_temperature_f", "value", "state"))
            for device in devices
        ]
        average = _average(value for value in values if value is not None)
        if average is not None:
            unit = _temperature_unit(devices[0])
            return f"{_format_number(average, 1)}°{unit}"
    if category_id == "humidity":
        values = [
            _number(_lookup(device, "humidity", "current_humidity", "relative_humidity", "value", "state"))
            for device in devices
        ]
        average = _average(value for value in values if value is not None)
        if average is not None:
            return f"{_format_number(average)}%"
    if category_id == "battery":
        values = [
            _number(_lookup(device, "battery", "battery_level", "battery_pct", "percentage", "value", "state"))
            for device in devices
        ]
        average = _average(value for value in values if value is not None)
        if average is not None:
            return f"{_format_number(average)}% average"
    if category_id == "illuminance":
        values = [
            _number(_lookup(device, "illuminance", "lux", "value", "state"))
            for device in devices
        ]
        average = _average(value for value in values if value is not None)
        if average is not None:
            return f"{_format_number(average)} lux"
    if category_id == "energy":
        value = _number(_lookup(devices[0], "power", "power_w", "watts", "energy", "value", "state"))
        if value is not None:
            return f"{_format_number(value, 1)} W"
    if category_id == "entry_sensor":
        known = [
            state
            for state in states
            if state in _OPEN_STATES or state in _CLOSED_STATES or state in {"on", "off", "active", "inactive", "true", "false"}
        ]
        if not known:
            return "Status unavailable"
        open_count = sum(state in _OPEN_STATES or state in {"on", "active"} for state in states)
        return "All closed" if open_count == 0 else f"{open_count} open"
    if category_id == "motion":
        known = [state for state in states if state in {"on", "off", "active", "inactive", "motion", "detected", "clear", "true", "false"}]
        if not known:
            return "Status unavailable"
        active = sum(state in {"on", "active", "motion", "detected", "true"} for state in states)
        return "Motion detected" if active else "Clear"
    if category_id == "leak":
        known = [state for state in states if state in {"on", "off", "active", "inactive", "wet", "dry", "leak", "detected", "clear", "true", "false"}]
        if not known:
            return "Status unavailable"
        wet = sum(state in {"on", "active", "wet", "leak", "detected", "true"} for state in states)
        return "Leak detected" if wet else "Dry"
    if category_id == "presence":
        known = [state for state in states if state in {"on", "off", "home", "away", "present", "absent", "active", "inactive", "true", "false"}]
        if not known:
            return "Status unavailable"
        present = sum(state in {"on", "home", "present", "active", "true"} for state in states)
        return f"{present} present" if present else "Away"
    if category_id == "network_device":
        known = [state for state in states if state in {"on", "off", "online", "offline", "connected", "disconnected", "active", "inactive", "true", "false"}]
        if not known:
            return "Status unavailable"
        online = sum(state in {"on", "online", "connected", "active", "true"} for state in states)
        return f"{online} online" if len(devices) > 1 else ("Online" if online else "Offline")
    if category_id == "climate":
        temperature = _number(_lookup(devices[0], "current_temperature", "temperature", "state"))
        mode = _text(_lookup(devices[0], "hvac_mode", "mode"))
        pieces = []
        if temperature is not None:
            pieces.append(f"{_format_number(temperature, 1)}°{_temperature_unit(devices[0])}")
        if mode:
            pieces.append(mode.replace("_", " ").title())
        if pieces:
            return " · ".join(pieces)
    known_states = [state.replace("_", " ").title() for state in states if state and state not in {"unknown", "unavailable"}]
    if not known_states:
        return "No current reading"
    unique = list(dict.fromkeys(known_states))
    if len(unique) == 1:
        return unique[0]
    return " · ".join(unique[:2])


def _control_type(category_id: str, supports_brightness: bool) -> str:
    if category_id == "light":
        return "light" if supports_brightness else "power"
    if category_id in {"fan", "switch", "plug"}:
        return "power"
    if category_id in {"garage_door", "cover"}:
        return "cover"
    if category_id == "lock":
        return "lock"
    return "read_only"


def camera_preview_ref(device: Dict[str, Any], secret: Any) -> str:
    integration_id = _text(device.get("integration_id"))
    device_id = _text(device.get("id") or device.get("ref"))
    secret_text = _text(secret)
    if not integration_id or not device_id or not secret_text:
        return ""
    digest = hmac.new(
        secret_text.encode("utf-8"),
        f"{integration_id}\0{device_id}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:24]


def _camera_previews(devices: List[Dict[str, Any]], secret: Any) -> List[Dict[str, Any]]:
    compatible = [
        device
        for device in devices
        if _actions(device).intersection({"camera_snapshot", "snapshot"})
    ]
    multiple = len(compatible) > 1
    previews: List[Dict[str, Any]] = []
    for index, device in enumerate(compatible, start=1):
        preview_id = camera_preview_ref(device, secret)
        if not preview_id:
            continue
        previews.append(
            {
                "id": preview_id,
                "label": f"Camera {index}" if multiple else "Camera",
                "snapshot_available": True,
            }
        )
    return previews


def _category_payload(
    category_id: str,
    devices: List[Dict[str, Any]],
    definition: Dict[str, Any],
    *,
    camera_ref_secret: Any = "",
) -> Dict[str, Any]:
    action_sets = [_actions(device) for device in devices]
    available_actions = [
        action
        for action in CONTROL_ACTIONS.get(category_id, ())
        if any(action in actions for actions in action_sets)
    ]
    supports_brightness = category_id == "light" and "set_brightness" in available_actions
    controllable = bool(available_actions)
    count = len(devices)
    state = "unknown"
    summary = ""
    on_count = 0
    off_count = 0

    if category_id in {"light", "fan", "switch", "plug"}:
        states = [_power_state(device) for device in devices]
        on_count = states.count("on")
        off_count = states.count("off")
        if on_count and off_count:
            state = "mixed"
        elif on_count == count:
            state = "on"
        elif off_count == count:
            state = "off"
        summary = (
            ("On" if state == "on" else "Off" if state == "off" else "Mixed" if state == "mixed" else "Status unavailable")
            if count == 1
            else f"{on_count} of {count} on"
        )
    elif category_id in {"garage_door", "cover"}:
        states = [_cover_state(device) for device in devices]
        open_count = sum(item in {"open", "opening"} for item in states)
        closed_count = sum(item in {"closed", "closing"} for item in states)
        if open_count == count:
            state = "open"
        elif closed_count == count:
            state = "closed"
        elif open_count or closed_count:
            state = "mixed"
        summary = (
            state.title()
            if state != "unknown"
            else "Status unavailable"
        )
        if count > 1 and state == "mixed":
            summary = f"{open_count} of {count} open"
    elif category_id == "lock":
        states = [_lock_state(device) for device in devices]
        locked_count = sum(item in {"locked", "locking"} for item in states)
        unlocked_count = sum(item in {"unlocked", "unlocking"} for item in states)
        if locked_count == count:
            state = "locked"
        elif unlocked_count == count:
            state = "unlocked"
        elif locked_count or unlocked_count:
            state = "mixed"
        summary = state.title() if state != "unknown" else "Status unavailable"
    else:
        summary = _sensor_summary(category_id, devices)
        state = _token(summary) or "unknown"

    brightness_values = [_brightness(device) for device in devices]
    brightness = _average(value for value in brightness_values if value is not None)
    name = _text(definition.get("name")) or category_id.replace("_", " ").title()
    payload = {
        "id": category_id,
        "name": name,
        "count": count,
        "order": int(definition.get("order") or 1000),
        "state": state,
        "summary": summary,
        "on_count": on_count,
        "off_count": off_count,
        "controllable": controllable,
        "read_only": not controllable,
        "control_type": _control_type(category_id, supports_brightness),
        "available_actions": available_actions,
        "supports_brightness": supports_brightness,
        "brightness": round(brightness, 1) if brightness is not None else None,
        "reading": summary if not controllable else "",
    }
    if category_id == "camera":
        payload["camera_previews"] = _camera_previews(devices, camera_ref_secret)
    return payload


def _room_summary(categories: List[Dict[str, Any]]) -> List[str]:
    rows: List[str] = []
    for category in categories:
        if category.get("read_only"):
            continue
        name = _text(category.get("name"))
        count = int(category.get("count") or 0)
        state = _text(category.get("state"))
        if category.get("control_type") in {"power", "light"}:
            on_count = int(category.get("on_count") or 0)
            if count == 1:
                rows.append(f"{name.rstrip('s')} {'on' if state == 'on' else 'off' if state == 'off' else state}")
            elif on_count == count:
                rows.append(f"All {count} {name.lower()} on")
            elif on_count:
                rows.append(f"{on_count} of {count} {name.lower()} on")
            else:
                rows.append(f"{name} off")
        elif category.get("control_type") == "cover":
            rows.append(f"{name.rstrip('s')} {category.get('summary', '').lower()}")
        elif category.get("control_type") == "lock":
            rows.append(f"{name} {category.get('summary', '').lower()}")
        if len(rows) >= 3:
            break
    if rows:
        return rows
    sensors = [category for category in categories if category.get("read_only") and _text(category.get("summary"))]
    return [f"{category.get('name')}: {category.get('summary')}" for category in sensors[:2]]


def _category_definitions(registry: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    definitions: Dict[str, Dict[str, Any]] = {}
    order: Dict[str, int] = {}
    for index, item in enumerate(registry.get("category_definitions") or []):
        if not isinstance(item, dict):
            continue
        category_id = _token(item.get("id"))
        if not category_id:
            continue
        definition = dict(item)
        definition["order"] = int(item.get("order") or (index + 1) * 10)
        definitions[category_id] = definition
        order[category_id] = int(definition["order"])
    return definitions, order


def categorized_devices(registry: Dict[str, Any]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    _definitions, order = _category_definitions(registry)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    seen: set[Tuple[str, str]] = set()
    for raw_device in registry.get("devices") or []:
        if not isinstance(raw_device, dict):
            continue
        device = dict(raw_device)
        integration_id = _text(device.get("integration_id"))
        device_id = _text(device.get("id") or device.get("ref"))
        identity = (integration_id, device_id)
        if not integration_id or not device_id or identity in seen:
            continue
        seen.add(identity)
        room_id, _room_name = _room_identity(device)
        for category_id in _display_category_ids(device, order):
            grouped.setdefault((room_id, category_id), []).append(device)
    return grouped


def build_home_snapshot(
    registry: Dict[str, Any],
    *,
    camera_ref_secret: Any = "",
) -> Dict[str, Any]:
    definitions, order = _category_definitions(registry)
    grouped = categorized_devices(registry)
    room_devices: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = {}
    room_names: Dict[str, str] = {}
    for raw_device in registry.get("devices") or []:
        if not isinstance(raw_device, dict):
            continue
        device = dict(raw_device)
        integration_id = _text(device.get("integration_id"))
        device_id = _text(device.get("id") or device.get("ref"))
        if not integration_id or not device_id:
            continue
        room_id, room_name = _room_identity(device)
        room_names.setdefault(room_id, room_name)
        room_devices.setdefault(room_id, {})[(integration_id, device_id)] = device

    rooms: List[Dict[str, Any]] = []
    for room_id, unique_devices in room_devices.items():
        category_rows: List[Dict[str, Any]] = []
        category_ids = {
            category_id
            for grouped_room_id, category_id in grouped
            if grouped_room_id == room_id
        }
        for category_id in sorted(category_ids, key=lambda item: (order.get(item, 1000), item)):
            devices = grouped.get((room_id, category_id), [])
            if not devices:
                continue
            definition = definitions.get(
                category_id,
                {"id": category_id, "name": category_id.replace("_", " ").title(), "order": order.get(category_id, 1000)},
            )
            category_rows.append(
                _category_payload(
                    category_id,
                    devices,
                    definition,
                    camera_ref_secret=camera_ref_secret,
                )
            )
        read_only = [row for row in category_rows if row.get("read_only")]
        controls = [row for row in category_rows if not row.get("read_only")]
        rooms.append(
            {
                "id": room_id,
                "name": room_names.get(room_id, "Unassigned"),
                "device_count": len(unique_devices),
                "category_count": len(category_rows),
                "summary": _room_summary(category_rows),
                "categories": category_rows,
                "sensors": read_only,
                "controls": controls,
            }
        )

    rooms.sort(key=lambda item: (_text(item.get("name")).casefold() == "unassigned", _text(item.get("name")).casefold()))
    cache = registry.get("cache") if isinstance(registry.get("cache"), dict) else {}
    return {
        "ok": True,
        "rooms": rooms,
        "room_count": len(rooms),
        "device_count": sum(int(room.get("device_count") or 0) for room in rooms),
        "generated_at": time.time(),
        "cache": {
            "cached": bool(cache.get("cached")),
            "age_seconds": float(cache.get("age_seconds") or 0.0),
        },
    }


def resolve_home_camera_target(
    registry: Dict[str, Any],
    *,
    room_id: Any,
    camera_id: Any,
    camera_ref_secret: Any,
) -> Tuple[Dict[str, Any], str]:
    clean_room = _token(room_id)
    clean_camera = _text(camera_id).lower()
    secret_text = _text(camera_ref_secret)
    if not clean_room or not clean_camera or not secret_text:
        raise ValueError("Room and camera are required.")
    if not re.fullmatch(r"[a-f0-9]{24}", clean_camera):
        raise LookupError("That camera is no longer available.")

    devices = categorized_devices(registry).get((clean_room, "camera"), [])
    for device in devices:
        preview_id = camera_preview_ref(device, secret_text)
        if not preview_id or not hmac.compare_digest(preview_id, clean_camera):
            continue
        actions = _actions(device)
        if "camera_snapshot" in actions:
            return device, "camera_snapshot"
        if "snapshot" in actions:
            return device, "snapshot"
        break
    raise LookupError("That camera is no longer available.")


def resolve_home_action_targets(
    registry: Dict[str, Any],
    *,
    room_id: Any,
    category_id: Any,
    action: Any,
) -> List[Dict[str, Any]]:
    clean_room = _token(room_id)
    clean_category = _token(category_id)
    clean_action = _token(action)
    if not clean_room or not clean_category or not clean_action:
        raise ValueError("Room, category, and action are required.")
    allowed = set(CONTROL_ACTIONS.get(clean_category, ()))
    if clean_action not in allowed:
        raise ValueError("That category does not support this control.")
    devices = categorized_devices(registry).get((clean_room, clean_category), [])
    if not devices:
        raise LookupError("That room category is no longer available.")
    targets = [device for device in devices if clean_action in _actions(device)]
    if not targets:
        raise ValueError("The devices in this category do not support this control.")
    return targets


def home_action_payload(action: Any, value: Any = None) -> Dict[str, Any]:
    clean_action = _token(action)
    if clean_action != "set_brightness":
        return {}
    brightness = _number(value)
    if brightness is None:
        raise ValueError("Brightness must be a percentage from 0 to 100.")
    brightness = max(0.0, min(100.0, brightness))
    return {
        "brightness": brightness,
        "brightness_pct": brightness,
        "level": brightness,
        "percent": brightness,
    }
