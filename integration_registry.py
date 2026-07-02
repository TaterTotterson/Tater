from __future__ import annotations

import copy
import importlib
import json
import logging
import os
import pkgutil
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

from tateros import integration_store as integration_store_module


INTEGRATION_PACKAGE = "integrations"
INTEGRATION_DIR = Path(os.getenv("TATER_INTEGRATION_DIR", "integrations"))
logger = logging.getLogger("integration_registry")
integration_registry_errors: List[str] = []
INTEGRATION_DEVICE_REGISTRY_CACHE_KEY = "tater:integration_runtime:device_registry"
INTEGRATION_ROOM_OVERRIDES_KEY = "tater:integration_runtime:room_overrides"
INTEGRATION_RUNTIME_STATES_KEY = "tater:integration_runtime:states"
_DEVICE_REGISTRY_CACHE_VERSION = 3


CAPABILITY_CATEGORIES: List[Dict[str, Any]] = [
    {
        "id": "light",
        "name": "Lights",
        "singular": "Light",
        "description": "Light bulbs, lamps, dimmers, and light-capable relays.",
        "aliases": ["lights", "lamp", "lamps", "bulb", "bulbs", "dimmer", "dimmers"],
        "order": 10,
    },
    {
        "id": "switch",
        "name": "Switches",
        "singular": "Switch",
        "description": "On/off relays, wall switches, and controllable switch entities.",
        "aliases": ["switches", "relay", "relays"],
        "order": 20,
    },
    {
        "id": "plug",
        "name": "Plugs",
        "singular": "Plug",
        "description": "Smart plugs, outlets, and plug-style switch devices.",
        "aliases": ["plugs", "outlet", "outlets", "smart_plug", "smart_plugs"],
        "order": 30,
    },
    {
        "id": "fan",
        "name": "Fans",
        "singular": "Fan",
        "description": "Fans and fan-speed controllable air movers.",
        "aliases": ["fans", "fan_speed"],
        "order": 35,
    },
    {
        "id": "garage_door",
        "name": "Garage Doors",
        "singular": "Garage Door",
        "description": "Garage doors, garage door openers, and garage entry devices.",
        "aliases": [
            "garage",
            "garages",
            "garage_door",
            "garage_doors",
            "garage_opener",
            "garage_openers",
            "garage_door_opener",
            "garage_door_openers",
        ],
        "order": 40,
    },
    {
        "id": "cover",
        "name": "Covers",
        "singular": "Cover",
        "description": "Garage doors, shades, blinds, curtains, and other open/close devices.",
        "aliases": ["covers", "shade", "shades", "blind", "blinds", "curtain", "curtains"],
        "order": 45,
    },
    {
        "id": "entry_sensor",
        "name": "Door & Window Sensors",
        "singular": "Door Sensor",
        "description": "Door, window, garage, contact, and opening sensors.",
        "aliases": [
            "entry_sensors",
            "door_sensor",
            "door_sensors",
            "window_sensor",
            "window_sensors",
            "contact",
            "contacts",
            "opening",
            "open_close",
            "door",
            "window",
        ],
        "order": 50,
    },
    {
        "id": "lock",
        "name": "Locks",
        "singular": "Lock",
        "description": "Door locks and smart locks.",
        "aliases": ["locks", "door_lock", "door_locks", "smart_lock", "smart_locks"],
        "order": 55,
    },
    {
        "id": "motion",
        "name": "Motion",
        "singular": "Motion Sensor",
        "description": "Motion and occupancy sensors.",
        "aliases": ["motion_sensor", "motion_sensors", "occupancy", "occupancy_sensor"],
        "order": 60,
    },
    {
        "id": "camera",
        "name": "Cameras",
        "singular": "Camera",
        "description": "Cameras, snapshots, doorbells, and video devices.",
        "aliases": ["cameras", "snapshot", "snapshots", "doorbell", "doorbells", "video"],
        "order": 70,
    },
    {
        "id": "leak",
        "name": "Leak Sensors",
        "singular": "Leak Sensor",
        "description": "Water, leak, flood, and moisture sensors.",
        "aliases": ["leaks", "water", "water_sensor", "water_sensors", "flood", "flood_sensor", "moisture"],
        "order": 75,
    },
    {
        "id": "climate",
        "name": "Climate",
        "singular": "Climate Device",
        "description": "Thermostats, HVAC, and climate controllers.",
        "aliases": ["thermostat", "thermostats", "hvac", "heater", "air_conditioner"],
        "order": 80,
    },
    {
        "id": "temperature",
        "name": "Temperature",
        "singular": "Temperature Sensor",
        "description": "Temperature readings and probes.",
        "aliases": ["temp", "temperature_sensor", "temperature_sensors"],
        "order": 90,
    },
    {
        "id": "humidity",
        "name": "Humidity",
        "singular": "Humidity Sensor",
        "description": "Humidity and relative-humidity sensors.",
        "aliases": ["relative_humidity", "humidity_sensor", "humidity_sensors"],
        "order": 100,
    },
    {
        "id": "illuminance",
        "name": "Light Sensors",
        "singular": "Light Sensor",
        "description": "Illuminance, lux, and ambient light sensors.",
        "aliases": ["light_sensor", "light_sensors", "lux"],
        "order": 110,
    },
    {
        "id": "energy",
        "name": "Energy",
        "singular": "Energy Meter",
        "description": "Power, energy, voltage, and current meters.",
        "aliases": ["power", "power_meter", "power_meters", "voltage", "current", "meter", "meters"],
        "order": 120,
    },
    {
        "id": "battery",
        "name": "Battery",
        "singular": "Battery Sensor",
        "description": "Battery level and low-battery status sensors.",
        "aliases": ["battery_sensor", "battery_sensors", "low_battery", "battery_level"],
        "order": 125,
    },
    {
        "id": "media_player",
        "name": "Speakers & Media",
        "singular": "Speaker",
        "description": "Speakers, zones, players, and audio outputs.",
        "aliases": [
            "speaker",
            "speakers",
            "audio",
            "audio_output",
            "audio_player",
            "music",
            "roon_zone",
            "announcement_target",
            "play_media",
        ],
        "order": 130,
    },
    {
        "id": "presence",
        "name": "Presence",
        "singular": "Presence Device",
        "description": "Occupancy, connected-client, and presence-style devices.",
        "aliases": ["presence_sensor", "presence_sensors", "occupancy_device", "connected_client"],
        "order": 135,
    },
    {
        "id": "network_device",
        "name": "Network Devices",
        "singular": "Network Device",
        "description": "Network clients, access points, hosts, and connectivity devices.",
        "aliases": [
            "network",
            "connectivity",
            "client",
            "clients",
            "host",
            "hosts",
            "access_point",
            "gateway",
            "network_switch",
            "network_switches",
        ],
        "order": 140,
    },
    {
        "id": "remote",
        "name": "Remotes",
        "singular": "Remote",
        "description": "Remote controls and button-command targets.",
        "aliases": ["remotes", "remote_control", "remote_controls"],
        "order": 150,
    },
    {
        "id": "scene",
        "name": "Scenes",
        "singular": "Scene",
        "description": "Scenes and named automation states that can be activated.",
        "aliases": ["scenes"],
        "order": 160,
    },
    {
        "id": "script",
        "name": "Scripts",
        "singular": "Script",
        "description": "Runnable scripts and reusable automation commands.",
        "aliases": ["scripts"],
        "order": 170,
    },
    {
        "id": "sensor",
        "name": "Other Sensors",
        "singular": "Sensor",
        "description": "Sensors that do not fit a more specific category.",
        "aliases": ["sensors", "binary_sensor", "binary_sensors"],
        "order": 900,
    },
    {
        "id": "device",
        "name": "Other Devices",
        "singular": "Device",
        "description": "Devices that do not fit a more specific category yet.",
        "aliases": ["devices", "entity", "entities"],
        "order": 1000,
    },
]

_CATEGORY_BY_ID = {str(item["id"]): item for item in CAPABILITY_CATEGORIES}
_CAPABILITY_ALIASES: Dict[str, str] = {}
for _category in CAPABILITY_CATEGORIES:
    _category_id = str(_category.get("id") or "")
    if _category_id:
        _CAPABILITY_ALIASES[_category_id] = _category_id
    for _alias in _category.get("aliases") or []:
        token = str(_alias or "").strip().lower().replace(" ", "_").replace("-", "_")
        if token:
            _CAPABILITY_ALIASES[token] = _category_id
_CAPABILITY_ALIASES.update(
    {
        "contact_sensor": "entry_sensor",
        "garage_door_opener": "garage_door",
        "garage_door_sensor": "entry_sensor",
        "garage_sensor": "entry_sensor",
        "relative_humidity_sensor": "humidity",
        "light_level": "illuminance",
        "media": "media_player",
        "player": "media_player",
        "switchable": "switch",
    }
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value in (None, ""):
        return []
    return [value]


def _normalize_token(value: Any) -> str:
    return _text(value).lower().replace(" ", "_").replace("-", "_")


def _canonical_capability(value: Any) -> str:
    token = _normalize_token(value)
    return _CAPABILITY_ALIASES.get(token, token)


def _normalize_capabilities(value: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in _as_list(value):
        token = _canonical_capability(item)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _normalize_features(value: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    def add(item: Any) -> None:
        token = _normalize_token(item)
        if not token or token in seen:
            return
        seen.add(token)
        out.append(token)

    if isinstance(value, dict):
        for key, raw in value.items():
            add(key)
            for item in _as_list(raw):
                add(item)
    else:
        for item in _as_list(value):
            add(item)
    return out


def _normalize_actions(value: Any) -> List[str]:
    return _normalize_features(value)


def _room_slug(value: Any) -> str:
    token = _normalize_token(value)
    return token or "unassigned"


def _room_name(value: Any) -> str:
    text = _text(value)
    return text if text else "Unassigned"


def _device_sort_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        _text(item.get("room")).casefold(),
        _text(item.get("name")).casefold(),
        _text(item.get("integration_name")).casefold(),
        _text(item.get("id")).casefold(),
    )


def _cache_client(client: Any = None) -> Any:
    if client is not None:
        return client
    try:
        from helpers import redis_client as shared_redis_client

        return shared_redis_client
    except Exception:
        return None


def _json_dict_loads(raw: Any) -> Optional[Dict[str, Any]]:
    text = _text(raw)
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _copy_dict(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return copy.deepcopy(value)


def _device_room_key_from_parts(integration_id: Any, device_id: Any) -> str:
    provider = _text(integration_id).lower()
    ident = _text(device_id)
    return f"{provider}::{ident}" if provider and ident else ""


def _device_room_keys(device: Dict[str, Any]) -> List[str]:
    integration_id = _text(device.get("integration_id")).lower()
    keys: List[str] = []
    seen: set[str] = set()
    for ident in (device.get("id"), device.get("ref"), device.get("device_id")):
        key = _device_room_key_from_parts(integration_id, ident)
        if key and key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def integration_device_room_key(integration_id: Any, device_id: Any) -> str:
    return _device_room_key_from_parts(integration_id, device_id)


def _default_room_store() -> Dict[str, Any]:
    return {
        "rooms": {},
        "room_aliases": {},
        "device_rooms": {},
        "device_names": {},
        "room_media_players": {},
        "updated_at": 0.0,
    }


def _normalize_room_media_player_target(value: Any) -> str:
    try:
        from announcement_targets import normalize_announcement_targets

        targets = normalize_announcement_targets(value)
        target = _text(targets[0]) if targets else ""
    except Exception:
        token = _text(value)
        if not token:
            return ""
        target = token
    if target.lower().startswith("ha:") or target.lower().startswith("media_player."):
        return ""
    return target


def _coerce_room_store(value: Any) -> Dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    out = _default_room_store()
    rooms = source.get("rooms") if isinstance(source.get("rooms"), dict) else {}
    for raw_id, raw_room in rooms.items():
        if not isinstance(raw_room, dict):
            continue
        name = _room_name(raw_room.get("name") or raw_id)
        room_id = _room_slug(raw_room.get("id") or name)
        if not room_id or room_id == "unassigned":
            continue
        out["rooms"][room_id] = {
            "id": room_id,
            "name": name,
            "created_at": float(raw_room.get("created_at") or 0.0),
            "updated_at": float(raw_room.get("updated_at") or 0.0),
        }

    aliases = source.get("room_aliases") if isinstance(source.get("room_aliases"), dict) else {}
    for raw_source, raw_target in aliases.items():
        source_id = _room_slug(raw_source)
        target_id = _room_slug(raw_target)
        if source_id and target_id and source_id != target_id:
            out["room_aliases"][source_id] = target_id

    device_rooms = source.get("device_rooms") if isinstance(source.get("device_rooms"), dict) else {}
    for raw_key, raw_room_id in device_rooms.items():
        key = _text(raw_key)
        room_id = _room_slug(raw_room_id)
        if key and room_id:
            out["device_rooms"][key] = room_id

    device_names = source.get("device_names") if isinstance(source.get("device_names"), dict) else {}
    for raw_key, raw_value in device_names.items():
        key = _text(raw_key)
        raw_name = raw_value.get("name") if isinstance(raw_value, dict) else raw_value
        name = _text(raw_name)
        if not key or not name:
            continue
        updated_at = 0.0
        if isinstance(raw_value, dict):
            try:
                updated_at = float(raw_value.get("updated_at") or 0.0)
            except Exception:
                updated_at = 0.0
        out["device_names"][key] = {"name": name, "updated_at": updated_at}

    room_media_players = source.get("room_media_players") if isinstance(source.get("room_media_players"), dict) else {}
    for raw_room_id, raw_value in room_media_players.items():
        room_id = _room_slug(raw_room_id)
        raw_target = raw_value.get("target") if isinstance(raw_value, dict) else raw_value
        target = _normalize_room_media_player_target(raw_target)
        if not room_id or room_id == "unassigned" or not target:
            continue
        updated_at = 0.0
        if isinstance(raw_value, dict):
            try:
                updated_at = float(raw_value.get("updated_at") or 0.0)
            except Exception:
                updated_at = 0.0
        out["room_media_players"][room_id] = {"target": target, "updated_at": updated_at}

    try:
        out["updated_at"] = float(source.get("updated_at") or 0.0)
    except Exception:
        out["updated_at"] = 0.0
    return out


def _load_room_store(client: Any = None) -> Dict[str, Any]:
    redis_obj = _cache_client(client)
    if not redis_obj:
        return _default_room_store()
    try:
        raw = redis_obj.get(INTEGRATION_ROOM_OVERRIDES_KEY)
    except Exception:
        return _default_room_store()
    return _coerce_room_store(_json_dict_loads(raw) or {})


def _save_room_store(store: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    redis_obj = _cache_client(client)
    payload = _coerce_room_store(store)
    now = time.time()
    payload["updated_at"] = now
    for room in payload.get("rooms", {}).values():
        if isinstance(room, dict):
            room["updated_at"] = float(room.get("updated_at") or now)
            if not float(room.get("created_at") or 0.0):
                room["created_at"] = now
    for device_name in payload.get("device_names", {}).values():
        if isinstance(device_name, dict) and not float(device_name.get("updated_at") or 0.0):
            device_name["updated_at"] = now
    for room_media_player in payload.get("room_media_players", {}).values():
        if isinstance(room_media_player, dict) and not float(room_media_player.get("updated_at") or 0.0):
            room_media_player["updated_at"] = now
    if redis_obj:
        redis_obj.set(INTEGRATION_ROOM_OVERRIDES_KEY, json.dumps(payload, separators=(",", ":"), default=str))
    return payload


def _ensure_room(store: Dict[str, Any], *, room_id: Any = "", name: Any = "") -> str:
    rooms = store.setdefault("rooms", {})
    clean_name = _room_name(name or room_id)
    clean_id = _room_slug(room_id or clean_name)
    if not clean_id or clean_id == "unassigned":
        return "unassigned"
    now = time.time()
    existing = rooms.get(clean_id) if isinstance(rooms.get(clean_id), dict) else {}
    rooms[clean_id] = {
        "id": clean_id,
        "name": clean_name,
        "created_at": float(existing.get("created_at") or now),
        "updated_at": now,
    }
    return clean_id


def _room_display_name(room_id: Any, store: Dict[str, Any], fallback: Any = "") -> str:
    clean_id = _room_slug(room_id)
    room = store.get("rooms", {}).get(clean_id) if isinstance(store.get("rooms"), dict) else None
    if isinstance(room, dict) and _text(room.get("name")):
        return _room_name(room.get("name"))
    if _text(fallback):
        return _room_name(fallback)
    return _room_name(clean_id.replace("_", " "))


def _resolve_room_alias(room_id: Any, store: Dict[str, Any]) -> str:
    aliases = store.get("room_aliases") if isinstance(store.get("room_aliases"), dict) else {}
    current = _room_slug(room_id)
    seen: set[str] = set()
    for _idx in range(8):
        raw_target = aliases.get(current)
        if raw_target in (None, ""):
            return current
        target = _room_slug(raw_target)
        if not target or target == current or target in seen:
            return current
        seen.add(current)
        current = target
    return current


def _room_media_player_preference(room_id: Any, store: Dict[str, Any]) -> Dict[str, Any]:
    clean_id = _resolve_room_alias(room_id, store)
    prefs = store.get("room_media_players") if isinstance(store.get("room_media_players"), dict) else {}
    raw_value = prefs.get(clean_id) or prefs.get(_room_slug(room_id))
    if not raw_value:
        return {}
    target = _normalize_room_media_player_target(raw_value.get("target") if isinstance(raw_value, dict) else raw_value)
    if not target:
        return {}
    updated_at = 0.0
    if isinstance(raw_value, dict):
        try:
            updated_at = float(raw_value.get("updated_at") or 0.0)
        except Exception:
            updated_at = 0.0
    return {
        "room_id": clean_id,
        "target": target,
        "updated_at": updated_at,
    }


def _room_ids_for_media_player_lookup(value: Any, store: Dict[str, Any]) -> List[str]:
    raw_items = list(value) if isinstance(value, (list, tuple, set)) else [value]
    rows: List[str] = []
    seen: set[str] = set()

    def add(room_id: Any) -> None:
        clean_id = _resolve_room_alias(room_id, store)
        if not clean_id or clean_id == "unassigned" or clean_id in seen:
            return
        seen.add(clean_id)
        rows.append(clean_id)

    for item in raw_items:
        token = _room_slug(item)
        if token:
            add(token)
        for stored_id, room in (store.get("rooms") or {}).items():
            if not isinstance(room, dict):
                continue
            if token and token == _room_slug(room.get("name")):
                add(stored_id)
    return rows


def _device_assigned_room_id(device: Dict[str, Any], store: Dict[str, Any]) -> str:
    device_rooms = store.get("device_rooms") if isinstance(store.get("device_rooms"), dict) else {}
    for key in _device_room_keys(device):
        if key not in device_rooms:
            continue
        raw_room_id = device_rooms.get(key)
        if raw_room_id in (None, ""):
            continue
        room_id = _room_slug(raw_room_id)
        if room_id:
            return _resolve_room_alias(room_id, store)
    return ""


def _device_name_override(device: Dict[str, Any], store: Dict[str, Any]) -> Tuple[str, str]:
    device_names = store.get("device_names") if isinstance(store.get("device_names"), dict) else {}
    for key in _device_room_keys(device):
        if key not in device_names:
            continue
        raw_value = device_names.get(key)
        name = _text(raw_value.get("name") if isinstance(raw_value, dict) else raw_value)
        if name:
            return key, name
    return "", ""


def _apply_device_name_store_to_device(device: Dict[str, Any], store: Dict[str, Any]) -> None:
    reported_name = _text(device.get("reported_name") or device.get("name") or device.get("display_name") or device.get("label") or device.get("id") or device.get("ref"))
    if reported_name:
        device["reported_name"] = reported_name
        device["reported_device_name"] = reported_name
    override_key, override_name = _device_name_override(device, store)
    if override_name:
        device["name"] = override_name
        device["display_name"] = override_name
        device["device_name_source"] = "tater_override"
        device["device_name_override"] = override_name
        device["device_name_override_key"] = override_key
    else:
        if reported_name and not _text(device.get("name")):
            device["name"] = reported_name
        device["device_name_source"] = "integration"


def _apply_room_store_to_device(device: Dict[str, Any], store: Dict[str, Any]) -> None:
    reported_room = _room_name(device.get("room") or device.get("area"))
    reported_room_id = _room_slug(reported_room)
    device["reported_room"] = reported_room
    device["reported_room_id"] = reported_room_id
    assigned_room_id = _device_assigned_room_id(device, store)
    alias_room_id = _resolve_room_alias(reported_room_id, store)
    room_id = assigned_room_id or alias_room_id or reported_room_id
    source = "integration"
    if assigned_room_id:
        source = "device_override"
    elif alias_room_id and alias_room_id != reported_room_id:
        source = "room_alias"
        device["room_alias_from_id"] = reported_room_id
    room_name = _room_display_name(room_id, store, reported_room if room_id == reported_room_id else "")
    device["room"] = room_name
    device["area"] = room_name
    device["room_id"] = _room_slug(room_id)
    device["room_source"] = source
    if assigned_room_id:
        device["room_override_id"] = assigned_room_id
    device_keys = _device_room_keys(device)
    if device_keys:
        device["room_assignment_key"] = device_keys[0]
        device["device_name_assignment_key"] = device.get("device_name_override_key") or device_keys[0]


def _enabled_integration_ids() -> List[str]:
    try:
        ids = integration_store_module.get_enabled_integration_ids()
    except Exception:
        return []
    return sorted(_text(item).lower() for item in ids if _text(item))


def _state_payload_id_tokens(payload: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []
    for key in (
        "id",
        "entity_id",
        "device_id",
        "ref",
        "resource_ref",
        "resource_id",
        "device_ref",
    ):
        token = _text(payload.get(key))
        if token:
            tokens.append(token)
    return tokens


def _device_state_tokens(device: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []
    details = device.get("details") if isinstance(device.get("details"), dict) else {}
    for source in (device, details):
        for key in (
            "id",
            "entity_id",
            "device_id",
            "ref",
            "resource_ref",
            "resource_id",
            "device_ref",
            "name",
            "reported_name",
            "reported_device_name",
            "display_name",
        ):
            token = _text(source.get(key))
            if token:
                tokens.append(token)
    device_type = _text(device.get("type"))
    device_id = _text(device.get("id"))
    if device_type and device_id:
        tokens.append(f"{device_type}:{device_id}")
    return tokens


def _token_variants(value: Any) -> List[str]:
    token = _text(value).lower()
    if not token:
        return []
    out = [token]
    if ":" in token:
        out.append(token.split(":", 1)[1])
    return list(dict.fromkeys(out))


def _runtime_state_provider_candidates(integration_id: Any) -> List[str]:
    token = _text(integration_id).lower()
    candidates = [token] if token else []
    if token == "homekit":
        candidates.append("ecobee_homekit")
    return list(dict.fromkeys(item for item in candidates if item))


def _runtime_state_index(client: Any = None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    redis_obj = _cache_client(client)
    if not redis_obj:
        return {}
    try:
        raw = redis_obj.hgetall(INTEGRATION_RUNTIME_STATES_KEY) or {}
    except Exception:
        return {}
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return index
    for key, value in raw.items():
        record = _json_dict_loads(value)
        if not record:
            continue
        provider = _text(record.get("provider") or _text(key).split(":", 1)[0]).lower()
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        candidate_tokens = [_text(record.get("id"))]
        candidate_tokens.extend(_state_payload_id_tokens(payload))
        for token in candidate_tokens:
            for variant in _token_variants(token):
                index[(provider, variant)] = record
    return index


def _runtime_state_for_device(device: Dict[str, Any], state_index: Dict[Tuple[str, str], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    providers = _runtime_state_provider_candidates(device.get("integration_id"))
    tokens: List[str] = []
    for token in _device_state_tokens(device):
        tokens.extend(_token_variants(token))
    for provider in providers:
        for token in tokens:
            record = state_index.get((provider, token))
            if record:
                return record
    return None


def _state_text_from_payload(payload: Dict[str, Any]) -> str:
    for key in ("state", "status", "value"):
        token = _text(payload.get(key))
        if token:
            return token
    new_state = payload.get("new_state") if isinstance(payload.get("new_state"), dict) else {}
    token = _text(new_state.get("state"))
    if token:
        return token
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for key in ("state", "status"):
        token = _text(raw.get(key))
        if token:
            return token
    return ""


def _online_from_state_text(value: str) -> Optional[bool]:
    token = _text(value).lower()
    if token in {"on", "open", "motion", "wet", "online", "connected", "playing", "active", "true"}:
        return True
    if token in {"off", "closed", "clear", "dry", "offline", "disconnected", "paused", "idle", "false"}:
        return False
    return None


def _overlay_runtime_state_on_device(device: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    state_text = _state_text_from_payload(payload)
    device["runtime_state"] = {
        "provider": _text(record.get("provider")),
        "id": _text(record.get("id")),
        "updated_at": record.get("updated_at"),
        "payload": payload,
    }
    if state_text:
        device["state"] = state_text
        device["status"] = state_text
    online = _online_from_state_text(state_text)
    if online is not None:
        device["online"] = online
    return device


def _apply_runtime_state_overlay_to_devices(devices: List[Dict[str, Any]], client: Any = None) -> None:
    state_index = _runtime_state_index(client)
    if not state_index:
        return
    for device in devices:
        if not isinstance(device, dict):
            continue
        record = _runtime_state_for_device(device, state_index)
        if record:
            _overlay_runtime_state_on_device(device, record)


def _apply_runtime_state_overlay_to_registry(registry: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    state_index = _runtime_state_index(client)
    if not state_index:
        return registry

    def overlay_list(rows: Any) -> None:
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            record = _runtime_state_for_device(row, state_index)
            if record:
                _overlay_runtime_state_on_device(row, record)

    overlay_list(registry.get("devices"))
    for group in registry.get("groups") or []:
        if isinstance(group, dict):
            overlay_list(group.get("devices"))
    for category in registry.get("categories") or []:
        if not isinstance(category, dict):
            continue
        overlay_list(category.get("devices"))
        for room in category.get("rooms") or []:
            if isinstance(room, dict):
                overlay_list(room.get("devices"))
    for room in registry.get("rooms") or []:
        if isinstance(room, dict):
            overlay_list(room.get("devices"))
    return registry


def _infer_device_capabilities(device_type: str, source: Dict[str, Any], details: Dict[str, Any]) -> List[str]:
    explicit_capabilities = _normalize_capabilities(source.get("category_ids") or source.get("capabilities"))
    if explicit_capabilities:
        return explicit_capabilities

    raw_capability_tokens = {_normalize_token(item) for item in _as_list(source.get("capabilities"))}
    actions = set(_normalize_actions(source.get("actions")))
    capabilities = []
    detail_tokens = {
        _normalize_token(details.get("device_class")),
        _normalize_token(details.get("resource_type")),
        _normalize_token(details.get("sensor_type")),
    }
    switch_control_actions = {"turn_on", "turn_off", "toggle", "set_brightness", "set_color"}
    has_switch_control = bool(actions.intersection(switch_control_actions))
    cover_control_actions = {"open", "close", "stop", "set_position"}
    has_garage_control = bool(
        actions.intersection(cover_control_actions)
        or raw_capability_tokens.intersection(
            {
                "cover",
                "covers",
                "garage_door",
                "garage_doors",
                "garage_opener",
                "garage_openers",
                "garage_door_opener",
                "garage_door_openers",
            }
        )
        or device_type == "cover"
    )
    if "garage_door" in capabilities and not has_garage_control and raw_capability_tokens.intersection(
        {"garage", "garage_sensor", "garage_door_sensor"}
    ):
        capabilities = [item for item in capabilities if item != "garage_door"]
        if "entry_sensor" not in capabilities:
            capabilities.append("entry_sensor")
    networkish = (
        "network_device" in capabilities
        or device_type in {"network_device", "client"}
        or bool(detail_tokens.intersection({"network_device", "client", "gateway", "access_point", "network_switch"}))
    )

    for token in (device_type, details.get("device_class"), details.get("resource_type"), details.get("sensor_type")):
        normalized = _normalize_token(token)
        category_id = _canonical_capability(normalized)
        if category_id == "switch" and networkish and not has_switch_control:
            continue
        if category_id == "garage_door" and not has_garage_control:
            continue
        if normalized and normalized not in capabilities:
            capabilities.append(normalized)

    haystack = " ".join(
        _text(value).lower()
        for value in (
            source.get("id"),
            source.get("name"),
            source.get("type"),
            details.get("device_class"),
            details.get("resource_type"),
            details.get("sensor_type"),
        )
    )

    def add(token: str) -> None:
        if token not in capabilities:
            capabilities.append(token)

    sensorish = device_type in {
        "sensor",
        "binary_sensor",
        "contact",
        "entry_sensor",
        "garage",
        "garage_door",
        "thermostat",
        "temperature",
        "humidity",
        "device",
    }
    if device_type == "camera" or "camera" in haystack:
        add("camera")
        add("snapshot")
    if "doorbell" in haystack or "ring" in haystack:
        add("doorbell")
    if device_type in {"light"}:
        add("switch")
    if device_type in {"switch", "outlet"} and (not networkish or has_switch_control):
        add("switch")
    if device_type == "fan":
        add("fan")
    if ("plug" in haystack or "outlet" in haystack) and not networkish:
        add("plug")
        add("switch")
    if (device_type in {"garage", "garage_door"} and has_garage_control) or (
        device_type == "cover" and ("garage" in haystack or detail_tokens.intersection({"garage", "garage_door"}))
    ):
        add("garage_door")
        add("cover")
    if (
        device_type in {"contact", "entry_sensor", "garage_door"}
        or detail_tokens.intersection({"door", "window", "garage", "garage_door", "contact", "opening"})
        or (
            sensorish
            and device_type != "camera"
            and any(token in haystack for token in ("door", "window", "garage", "contact", "opening"))
        )
    ):
        add("entry_sensor")
    if "motion" in haystack:
        add("motion")
    if device_type in {"temperature", "thermostat"} or (
        sensorish and any(token in haystack for token in ("temperature", "temp"))
    ):
        add("temperature")
    if device_type in {"humidity"} or (sensorish and "humidity" in haystack):
        add("humidity")
    if device_type in {"illuminance"} or (sensorish and any(token in haystack for token in ("illuminance", "lux"))):
        add("illuminance")
    if device_type in {"climate", "thermostat"}:
        add("climate")
    if device_type == "lock":
        add("lock")
    if device_type in {"media_player", "speaker", "roon_zone"}:
        add("media_player")
    if any(token in haystack for token in ("speaker", "sonos", "roon", "audio", "media")):
        add("media_player")
    if device_type in {"network_device", "client"} or any(token in haystack for token in ("network", "gateway", "access point", "client")):
        add("network_device")
    if "presence" in haystack or "occupancy" in haystack:
        add("presence")
    if any(token in haystack for token in ("leak", "water", "flood", "moisture")):
        add("leak")
    if "battery" in haystack:
        add("battery")
    if device_type == "remote":
        add("remote")
    if device_type == "scene":
        add("scene")
    if device_type == "script":
        add("script")
    return capabilities


def _ensure_import_context() -> None:
    parent = str(INTEGRATION_DIR.resolve().parent)
    if parent and parent not in sys.path:
        sys.path.insert(0, parent)

    package = sys.modules.get(INTEGRATION_PACKAGE)
    if package is not None and not isinstance(package, ModuleType):
        sys.modules.pop(INTEGRATION_PACKAGE, None)
        package = None

    importlib.invalidate_caches()
    if package is None:
        package = importlib.import_module(INTEGRATION_PACKAGE)

    package_paths = getattr(package, "__path__", None)
    if package_paths is not None:
        expected = str(INTEGRATION_DIR.resolve())
        normalized = {str(Path(path).resolve()) for path in package_paths}
        if expected not in normalized:
            package_paths.append(expected)


def _integration_modules() -> List[Any]:
    _ensure_import_context()
    package = importlib.import_module(INTEGRATION_PACKAGE)
    modules: List[Any] = []
    errors: List[str] = []
    package_paths = getattr(package, "__path__", None)
    if package_paths is None:
        return modules

    for item in pkgutil.iter_modules(package_paths):
        name = _text(item.name)
        if not name or name.startswith("_"):
            continue
        if not integration_store_module.get_integration_enabled(name):
            continue
        module_name = f"{INTEGRATION_PACKAGE}.{name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            errors.append(f"{module_name}: {exc}")
            continue
        definition = getattr(module, "INTEGRATION", None)
        if isinstance(definition, dict) and _text(definition.get("id")):
            modules.append(module)
    integration_registry_errors.clear()
    integration_registry_errors.extend(errors)
    if errors:
        logger.warning("Integration registry load issues: %s", "; ".join(errors))
    return modules


def _module_for_integration(integration_id: str) -> Any:
    target = _text(integration_id)
    if not target:
        raise KeyError("Integration id is required.")
    for module in _integration_modules():
        definition = getattr(module, "INTEGRATION", None)
        if isinstance(definition, dict) and _text(definition.get("id")) == target:
            return module
    raise KeyError(f"Unknown integration: {target}")


def _coerce_definition(module: Any) -> Dict[str, Any]:
    definition = dict(getattr(module, "INTEGRATION", {}) or {})
    integration_id = _text(definition.get("id"))
    if not integration_id:
        return {}

    fields = definition.get("fields")
    if not isinstance(fields, list):
        fields = []
    actions = definition.get("actions")
    if not isinstance(actions, list):
        actions = []
    capabilities = _normalize_capabilities(definition.get("capabilities"))

    return {
        "id": integration_id,
        "name": _text(definition.get("name")) or integration_id.replace("_", " ").title(),
        "description": _text(definition.get("description")),
        "badge": _text(definition.get("badge")) or integration_id[:3].upper(),
        "order": int(definition.get("order") or 1000),
        "capabilities": capabilities,
        "fields": [dict(field) for field in fields if isinstance(field, dict)],
        "actions": [dict(action) for action in actions if isinstance(action, dict)],
    }


def _read_values(module: Any) -> Dict[str, Any]:
    reader = getattr(module, "read_integration_settings", None)
    if not callable(reader):
        return {}
    try:
        values = reader()
    except Exception as exc:
        return {"_error": str(exc)}
    return values if isinstance(values, dict) else {}


def _read_status(module: Any) -> Dict[str, Any]:
    reader = getattr(module, "integration_status", None)
    if not callable(reader):
        return {}
    try:
        status = reader()
    except Exception as exc:
        return {"error": str(exc), "message": str(exc)}
    return status if isinstance(status, dict) else {}


def _coerce_device_row(integration_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
    source = dict(row or {})
    details = source.get("details") if isinstance(source.get("details"), dict) else {}
    if not details:
        details = {
            key: value
            for key, value in source.items()
            if key not in {"id", "name", "type", "kind", "status", "state", "room", "area", "details"}
        }
    device_id = _text(source.get("id") or source.get("device_id") or source.get("entity_id") or source.get("mac"))
    name = _text(source.get("name") or source.get("label") or source.get("friendly_name") or device_id)
    device_type = _normalize_token(source.get("type") or source.get("kind") or source.get("domain") or "device")
    ref = _text(source.get("ref") or source.get("resource_ref"))
    if not ref and device_id:
        ref = f"{device_type or 'device'}:{device_id}"
    capabilities = _infer_device_capabilities(device_type, source, details)
    room = _text(
        source.get("room")
        or source.get("area")
        or source.get("area_name")
        or source.get("room_name")
        or source.get("location")
        or source.get("zone")
        or details.get("room")
        or details.get("area")
        or details.get("area_name")
        or details.get("room_name")
        or details.get("location")
        or details.get("zone")
    )
    actions = _normalize_actions(source.get("actions"))
    features = _normalize_features(source.get("features"))
    for action in actions:
        if action not in features:
            features.append(action)
    return {
        "integration_id": integration_id,
        "id": device_id or name,
        "name": name or device_id or "Device",
        "type": device_type or "device",
        "ref": ref or device_id or name,
        "capabilities": capabilities,
        "category_ids": _device_category_ids(capabilities, device_type),
        "features": features,
        "actions": actions,
        "event_sources": [dict(item) for item in _as_list(source.get("event_sources")) if isinstance(item, dict)],
        "status": _text(source.get("status")),
        "state": _text(source.get("state")),
        "room": room,
        "area": room,
        "room_id": _room_slug(room),
        "details": dict(details),
    }


def _device_category_ids(capabilities: List[str], device_type: str = "") -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    normalized_capabilities = _normalize_capabilities(capabilities)
    explicit_categories = set(normalized_capabilities)
    networkish = "network_device" in explicit_categories
    for raw in [device_type, *normalized_capabilities]:
        category_id = _canonical_capability(raw)
        if raw == device_type and category_id == "switch" and networkish and "switch" not in explicit_categories:
            continue
        if raw == device_type and category_id == "garage_door" and "garage_door" not in explicit_categories and "cover" not in explicit_categories:
            continue
        if category_id not in _CATEGORY_BY_ID or category_id in seen:
            continue
        seen.add(category_id)
        out.append(category_id)

    if not out:
        out.append("device")
    elif out == ["sensor"]:
        out = ["sensor"]
    elif "sensor" in out and len(out) > 1:
        out = [item for item in out if item != "sensor"]
    elif "device" in out and len(out) > 1:
        out = [item for item in out if item != "device"]
    return sorted(out, key=lambda token: int(_CATEGORY_BY_ID.get(token, {}).get("order") or 1000))


def _read_devices(module: Any, integration_id: str) -> Dict[str, Any]:
    reader = getattr(module, "integration_devices", None)
    if not callable(reader):
        return {"devices": []}
    try:
        rows = reader()
    except Exception as exc:
        return {"devices": [], "error": str(exc), "message": str(exc)}
    if isinstance(rows, dict):
        raw_rows = rows.get("devices") if isinstance(rows.get("devices"), list) else []
        out = {key: value for key, value in rows.items() if key != "devices"}
    else:
        raw_rows = rows if isinstance(rows, list) else []
        out = {}
    devices = [
        _coerce_device_row(integration_id, row)
        for row in raw_rows
        if isinstance(row, dict)
    ]
    devices.sort(key=lambda item: (_text(item.get("name")).casefold(), _text(item.get("type")).casefold(), _text(item.get("id")).casefold()))
    out["devices"] = devices
    return out


def _device_group(module: Any, definition: Dict[str, Any]) -> Dict[str, Any]:
    integration_id = _text(definition.get("id"))
    status = _read_status(module)
    device_result = _read_devices(module, integration_id)
    devices = device_result.get("devices") if isinstance(device_result.get("devices"), list) else []
    error = _text(device_result.get("error"))
    return {
        "id": integration_id,
        "name": _text(definition.get("name")) or integration_id,
        "badge": _text(definition.get("badge")),
        "order": int(definition.get("order") or 1000),
        "status": status,
        "devices": devices,
        "device_count": len(devices),
        "error": error,
        "message": _text(device_result.get("message")),
    }


def get_integration_catalog() -> List[Dict[str, Any]]:
    catalog: List[Dict[str, Any]] = []
    for module in _integration_modules():
        definition = _coerce_definition(module)
        if not definition:
            continue
        row = dict(definition)
        values = _read_values(module)
        status = _read_status(module)
        if values.get("_error") and not status.get("message"):
            status = {"error": values.get("_error"), "message": values.get("_error")}
        row["values"] = values
        row["status"] = status
        catalog.append(row)
    catalog.sort(key=lambda item: (int(item.get("order") or 1000), _text(item.get("name")).casefold(), _text(item.get("id"))))
    return catalog


def _load_integration_devices_live() -> Dict[str, Any]:
    groups: List[Dict[str, Any]] = []
    total = 0
    errors: List[Dict[str, str]] = []
    for module in _integration_modules():
        definition = _coerce_definition(module)
        if not definition:
            continue
        group = _device_group(module, definition)
        devices = group.get("devices") if isinstance(group.get("devices"), list) else []
        total += len(devices)
        error = _text(group.get("error"))
        if error:
            errors.append({"integration_id": _text(group.get("id")), "name": _text(group.get("name")), "error": error})
        groups.append(group)
    groups.sort(key=lambda item: (int(item.get("order") or 1000), _text(item.get("name")).casefold(), _text(item.get("id"))))
    return {"groups": groups, "total": total, "errors": errors}


def get_integration_devices(client: Any = None, *, refresh: bool = False, use_cache: bool = True) -> Dict[str, Any]:
    if use_cache and not refresh:
        cached = get_cached_integration_device_registry(client)
        if cached:
            return {
                "groups": cached.get("groups") if isinstance(cached.get("groups"), list) else [],
                "total": int(cached.get("total") or 0),
                "errors": cached.get("errors") if isinstance(cached.get("errors"), list) else [],
                "cache": cached.get("cache") if isinstance(cached.get("cache"), dict) else {},
            }
    return _load_integration_devices_live()


def _build_integration_device_registry(snapshot: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    groups = snapshot.get("groups") if isinstance(snapshot.get("groups"), list) else []
    room_store = _load_room_store(client)
    devices: List[Dict[str, Any]] = []
    integration_counts: Dict[str, int] = {}
    integration_names: Dict[str, str] = {}

    for group in groups:
        if not isinstance(group, dict):
            continue
        integration_id = _text(group.get("id"))
        integration_name = _text(group.get("name")) or integration_id
        integration_names[integration_id] = integration_name
        for raw_device in group.get("devices") or []:
            if not isinstance(raw_device, dict):
                continue
            device = dict(raw_device)
            device["integration_id"] = _text(device.get("integration_id")) or integration_id
            device["integration_name"] = integration_name
            device["integration_badge"] = _text(group.get("badge"))
            device["room"] = _room_name(device.get("room") or device.get("area"))
            device["area"] = device["room"]
            device["room_id"] = _room_slug(device.get("room"))
            _apply_device_name_store_to_device(device, room_store)
            _apply_room_store_to_device(device, room_store)
            device["category_ids"] = _device_category_ids(
                _normalize_capabilities(device.get("category_ids") or device.get("capabilities")),
                _text(device.get("type")),
            )
            devices.append(device)
            integration_counts[integration_id] = integration_counts.get(integration_id, 0) + 1

    _apply_runtime_state_overlay_to_devices(devices, client)
    devices.sort(key=_device_sort_key)

    categories: List[Dict[str, Any]] = []
    for category in CAPABILITY_CATEGORIES:
        category_id = _text(category.get("id"))
        if not category_id:
            continue
        category_devices = [
            dict(device)
            for device in devices
            if category_id in set(_normalize_capabilities(device.get("category_ids") or device.get("capabilities")))
        ]
        if not category_devices and category_id != "device":
            continue

        rooms_by_id: Dict[str, Dict[str, Any]] = {}
        category_integrations: Dict[str, int] = {}
        for device in category_devices:
            room_id = _room_slug(device.get("room"))
            room = rooms_by_id.setdefault(
                room_id,
                {
                    "id": room_id,
                    "name": _room_name(device.get("room")),
                    "source": _text(device.get("room_source")) or "integration",
                    "devices": [],
                    "device_count": 0,
                },
            )
            room["devices"].append(device)
            room["device_count"] = len(room["devices"])
            integration_id = _text(device.get("integration_id"))
            if integration_id:
                category_integrations[integration_id] = category_integrations.get(integration_id, 0) + 1

        rooms = list(rooms_by_id.values())
        rooms.sort(key=lambda item: (_text(item.get("name")).casefold() == "unassigned", _text(item.get("name")).casefold()))
        for room in rooms:
            room["devices"].sort(key=_device_sort_key)

        integrations = [
            {
                "id": integration_id,
                "name": integration_names.get(integration_id, integration_id),
                "device_count": count,
            }
            for integration_id, count in sorted(category_integrations.items(), key=lambda item: integration_names.get(item[0], item[0]).casefold())
        ]

        categories.append(
            {
                "id": category_id,
                "name": _text(category.get("name")) or category_id.replace("_", " ").title(),
                "singular": _text(category.get("singular")) or _text(category.get("name")) or category_id,
                "description": _text(category.get("description")),
                "aliases": list(category.get("aliases") or []),
                "order": int(category.get("order") or 1000),
                "devices": category_devices,
                "device_count": len(category_devices),
                "rooms": rooms,
                "room_count": len(rooms),
                "integrations": integrations,
            }
        )

    categories.sort(key=lambda item: (int(item.get("order") or 1000), _text(item.get("name")).casefold()))

    rooms_by_id: Dict[str, Dict[str, Any]] = {}
    for device in devices:
        room_id = _room_slug(device.get("room"))
        room = rooms_by_id.setdefault(
            room_id,
            {
                "id": room_id,
                "name": _room_name(device.get("room")),
                "source": _text(device.get("room_source")) or "integration",
                "devices": [],
                "device_count": 0,
                "categories": {},
            },
        )
        room["devices"].append(device)
        room["device_count"] = len(room["devices"])
        for category_id in device.get("category_ids") or []:
            if category_id in _CATEGORY_BY_ID:
                room["categories"][category_id] = room["categories"].get(category_id, 0) + 1

    rooms: List[Dict[str, Any]] = []
    for room in rooms_by_id.values():
        category_rows = [
            {
                "id": category_id,
                "name": _text(_CATEGORY_BY_ID.get(category_id, {}).get("name")) or category_id,
                "device_count": count,
            }
            for category_id, count in sorted(
                room.get("categories", {}).items(),
                key=lambda item: int(_CATEGORY_BY_ID.get(item[0], {}).get("order") or 1000),
            )
        ]
        room["categories"] = category_rows
        room["devices"].sort(key=_device_sort_key)
        rooms.append(room)

    for room_id, custom_room in (room_store.get("rooms") or {}).items():
        if not isinstance(custom_room, dict):
            continue
        clean_id = _room_slug(room_id)
        if not clean_id or clean_id in rooms_by_id:
            continue
        rooms.append(
            {
                "id": clean_id,
                "name": _room_name(custom_room.get("name") or clean_id),
                "source": "tater",
                "devices": [],
                "device_count": 0,
                "categories": [],
            }
        )
    rooms.sort(key=lambda item: (_text(item.get("name")).casefold() == "unassigned", _text(item.get("name")).casefold()))
    for room in rooms:
        preference = _room_media_player_preference(room.get("id"), room_store)
        room["preferred_media_player"] = _text(preference.get("target"))
        if preference:
            room["preferred_media_player_updated_at"] = float(preference.get("updated_at") or 0.0)

    return {
        "devices": devices,
        "categories": categories,
        "rooms": rooms,
        "groups": groups,
        "total": len(devices),
        "errors": snapshot.get("errors") if isinstance(snapshot.get("errors"), list) else [],
        "category_definitions": CAPABILITY_CATEGORIES,
        "integration_counts": integration_counts,
        "room_overrides": {
            "rooms": sorted(
                [dict(room) for room in (room_store.get("rooms") or {}).values() if isinstance(room, dict)],
                key=lambda item: _text(item.get("name")).casefold(),
            ),
            "room_aliases": dict(room_store.get("room_aliases") or {}),
            "device_rooms": dict(room_store.get("device_rooms") or {}),
            "device_names": dict(room_store.get("device_names") or {}),
            "room_media_players": dict(room_store.get("room_media_players") or {}),
            "updated_at": float(room_store.get("updated_at") or 0.0),
        },
    }


def _cache_metadata(*, generated_at: float, duration_ms: float = 0.0, source: str = "runtime") -> Dict[str, Any]:
    now = time.time()
    return {
        "version": _DEVICE_REGISTRY_CACHE_VERSION,
        "cached": True,
        "source": source,
        "enabled_integrations": _enabled_integration_ids(),
        "generated_at": generated_at,
        "updated_at": generated_at,
        "age_seconds": max(0.0, now - generated_at),
        "duration_ms": max(0.0, duration_ms),
    }


def get_cached_integration_device_registry(client: Any = None) -> Dict[str, Any]:
    redis_obj = _cache_client(client)
    if not redis_obj:
        return {}
    try:
        raw = redis_obj.get(INTEGRATION_DEVICE_REGISTRY_CACHE_KEY)
    except Exception:
        return {}
    registry = _json_dict_loads(raw)
    if not registry:
        return {}
    registry = _copy_dict(registry)
    generated_at = 0.0
    cache = registry.get("cache") if isinstance(registry.get("cache"), dict) else {}
    try:
        cached_version = int(cache.get("version") or 0)
    except Exception:
        cached_version = 0
    if cached_version != _DEVICE_REGISTRY_CACHE_VERSION:
        return {}
    cached_enabled = [_text(item).lower() for item in cache.get("enabled_integrations") or [] if _text(item)]
    current_enabled = _enabled_integration_ids()
    if cached_enabled and sorted(cached_enabled) != current_enabled:
        return {}
    try:
        generated_at = float(cache.get("generated_at") or cache.get("updated_at") or 0.0)
    except Exception:
        generated_at = 0.0
    registry["cache"] = {
        **cache,
        "version": _DEVICE_REGISTRY_CACHE_VERSION,
        "cached": True,
        "enabled_integrations": current_enabled,
        "age_seconds": max(0.0, time.time() - generated_at) if generated_at else 0.0,
    }
    return _apply_runtime_state_overlay_to_registry(registry, redis_obj)


def save_integration_device_registry_cache(registry: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    redis_obj = _cache_client(client)
    payload = _copy_dict(registry)
    generated_at = time.time()
    cache = payload.get("cache") if isinstance(payload.get("cache"), dict) else {}
    payload["cache"] = {
        **cache,
        "version": _DEVICE_REGISTRY_CACHE_VERSION,
        "cached": True,
        "enabled_integrations": _enabled_integration_ids(),
        "generated_at": float(cache.get("generated_at") or generated_at),
        "updated_at": generated_at,
    }
    if redis_obj:
        redis_obj.set(INTEGRATION_DEVICE_REGISTRY_CACHE_KEY, json.dumps(payload, separators=(",", ":"), default=str))
    return payload


def refresh_integration_device_registry_cache(client: Any = None, *, source: str = "runtime") -> Dict[str, Any]:
    started = time.time()
    snapshot = _load_integration_devices_live()
    registry = _build_integration_device_registry(snapshot, client)
    duration_ms = (time.time() - started) * 1000.0
    generated_at = time.time()
    registry["cache"] = _cache_metadata(generated_at=generated_at, duration_ms=duration_ms, source=source)
    return save_integration_device_registry_cache(registry, client)


def get_integration_device_registry(client: Any = None, *, refresh: bool = False, use_cache: bool = True) -> Dict[str, Any]:
    if use_cache and not refresh:
        cached = get_cached_integration_device_registry(client)
        if cached:
            return cached
    if refresh:
        return refresh_integration_device_registry_cache(client, source="manual")
    snapshot = _load_integration_devices_live()
    registry = _build_integration_device_registry(snapshot, client)
    registry["cache"] = _cache_metadata(generated_at=time.time(), source="live")
    registry["cache"]["cached"] = False
    return registry


def _rebuild_integration_device_registry_cache(client: Any = None, *, source: str = "rooms") -> Dict[str, Any]:
    cached = get_cached_integration_device_registry(client)
    groups = cached.get("groups") if isinstance(cached.get("groups"), list) else []
    if groups:
        snapshot = {
            "groups": groups,
            "total": int(cached.get("total") or 0),
            "errors": cached.get("errors") if isinstance(cached.get("errors"), list) else [],
        }
        started = time.time()
        registry = _build_integration_device_registry(snapshot, client)
        registry["cache"] = _cache_metadata(
            generated_at=time.time(),
            duration_ms=(time.time() - started) * 1000.0,
            source=source,
        )
        return save_integration_device_registry_cache(registry, client)
    return refresh_integration_device_registry_cache(client, source=source)


def get_integration_room_overrides(client: Any = None) -> Dict[str, Any]:
    store = _load_room_store(client)
    return {
        "rooms": sorted(
            [dict(room) for room in (store.get("rooms") or {}).values() if isinstance(room, dict)],
            key=lambda item: _text(item.get("name")).casefold(),
        ),
        "room_aliases": dict(store.get("room_aliases") or {}),
        "device_rooms": dict(store.get("device_rooms") or {}),
        "device_names": dict(store.get("device_names") or {}),
        "room_media_players": dict(store.get("room_media_players") or {}),
        "updated_at": float(store.get("updated_at") or 0.0),
    }


def create_integration_room(name: Any, client: Any = None) -> Dict[str, Any]:
    room_name = _text(name)
    if not room_name:
        raise ValueError("Room name is required.")
    store = _load_room_store(client)
    room_id = _ensure_room(store, name=room_name)
    if room_id == "unassigned":
        raise ValueError("Choose a named room.")
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="room_create")
    return {"ok": True, "room_id": room_id, "room_overrides": get_integration_room_overrides(client), "registry": registry}


def rename_integration_room(room_id: Any, name: Any, client: Any = None) -> Dict[str, Any]:
    old_id = _room_slug(room_id)
    new_name = _text(name)
    if not old_id:
        raise ValueError("Room id is required.")
    if not new_name:
        raise ValueError("Room name is required.")
    store = _load_room_store(client)
    old_room = store.get("rooms", {}).get(old_id) if isinstance(store.get("rooms"), dict) else {}
    new_id = _ensure_room(store, name=new_name)
    if new_id == "unassigned":
        raise ValueError("Choose a named room.")
    if old_id != new_id:
        store.setdefault("room_aliases", {})[old_id] = new_id
        for source_id, target_id in list((store.get("room_aliases") or {}).items()):
            if _room_slug(target_id) == old_id:
                store["room_aliases"][source_id] = new_id
        for key, assigned_room_id in list((store.get("device_rooms") or {}).items()):
            if _room_slug(assigned_room_id) == old_id:
                store["device_rooms"][key] = new_id
        if old_id in (store.get("room_media_players") or {}) and old_id != new_id:
            store.setdefault("room_media_players", {})[new_id] = store["room_media_players"].pop(old_id)
        if old_id in (store.get("rooms") or {}) and old_id != new_id:
            del store["rooms"][old_id]
    elif isinstance(old_room, dict):
        store["rooms"][new_id]["created_at"] = float(old_room.get("created_at") or store["rooms"][new_id].get("created_at") or time.time())
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="room_rename")
    return {"ok": True, "room_id": new_id, "room_overrides": get_integration_room_overrides(client), "registry": registry}


def get_integration_room_preferred_media_player(room: Any, client: Any = None) -> Dict[str, Any]:
    store = _load_room_store(client)
    for room_id in _room_ids_for_media_player_lookup(room, store):
        preference = _room_media_player_preference(room_id, store)
        if preference:
            return preference
    return {}


def set_integration_room_preferred_media_player(
    room_id: Any,
    target: Any,
    client: Any = None,
    *,
    room_name: Any = "",
) -> Dict[str, Any]:
    clean_id = _room_slug(room_id)
    if not clean_id or clean_id == "unassigned":
        raise ValueError("Choose a named room.")
    normalized_target = _normalize_room_media_player_target(target)
    if not normalized_target:
        return clear_integration_room_preferred_media_player(clean_id, client)

    store = _load_room_store(client)
    resolved_room_id = _resolve_room_alias(clean_id, store)
    _ensure_room(store, room_id=resolved_room_id, name=room_name or resolved_room_id.replace("_", " "))
    store.setdefault("room_media_players", {})[resolved_room_id] = {
        "target": normalized_target,
        "updated_at": time.time(),
    }
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="room_media_player_set")
    return {
        "ok": True,
        "room_id": resolved_room_id,
        "target": normalized_target,
        "room_overrides": get_integration_room_overrides(client),
        "registry": registry,
    }


def clear_integration_room_preferred_media_player(room_id: Any, client: Any = None) -> Dict[str, Any]:
    clean_id = _room_slug(room_id)
    if not clean_id or clean_id == "unassigned":
        raise ValueError("Choose a named room.")
    store = _load_room_store(client)
    resolved_room_id = _resolve_room_alias(clean_id, store)
    store.setdefault("room_media_players", {}).pop(resolved_room_id, None)
    store.setdefault("room_media_players", {}).pop(clean_id, None)
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="room_media_player_clear")
    return {
        "ok": True,
        "room_id": resolved_room_id,
        "room_overrides": get_integration_room_overrides(client),
        "registry": registry,
    }


def assign_integration_device_room(
    integration_id: Any,
    device_id: Any,
    *,
    room_id: Any = "",
    room_name: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    key = _device_room_key_from_parts(integration_id, device_id)
    if not key:
        raise ValueError("Integration id and device id are required.")
    store = _load_room_store(client)
    selected_room_id = _room_slug(room_id)
    if selected_room_id and selected_room_id != "unassigned":
        if selected_room_id not in (store.get("rooms") or {}):
            _ensure_room(store, room_id=selected_room_id, name=room_name or selected_room_id.replace("_", " "))
    else:
        selected_room_id = _ensure_room(store, name=room_name)
    if not selected_room_id or selected_room_id == "unassigned":
        raise ValueError("Choose a named room.")
    store.setdefault("device_rooms", {})[key] = selected_room_id
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="device_room_assign")
    return {
        "ok": True,
        "device_key": key,
        "room_id": selected_room_id,
        "room_overrides": get_integration_room_overrides(client),
        "registry": registry,
    }


def clear_integration_device_room(integration_id: Any, device_id: Any, client: Any = None) -> Dict[str, Any]:
    key = _device_room_key_from_parts(integration_id, device_id)
    if not key:
        raise ValueError("Integration id and device id are required.")
    store = _load_room_store(client)
    store.setdefault("device_rooms", {}).pop(key, None)
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="device_room_clear")
    return {"ok": True, "device_key": key, "room_overrides": get_integration_room_overrides(client), "registry": registry}


def rename_integration_device(integration_id: Any, device_id: Any, name: Any, client: Any = None) -> Dict[str, Any]:
    key = _device_room_key_from_parts(integration_id, device_id)
    clean_name = _text(name)
    if not key:
        raise ValueError("Integration id and device id are required.")
    if not clean_name:
        raise ValueError("Device name is required.")
    store = _load_room_store(client)
    store.setdefault("device_names", {})[key] = {"name": clean_name, "updated_at": time.time()}
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="device_rename")
    return {
        "ok": True,
        "device_key": key,
        "name": clean_name,
        "room_overrides": get_integration_room_overrides(client),
        "registry": registry,
    }


def clear_integration_device_name(integration_id: Any, device_id: Any, client: Any = None) -> Dict[str, Any]:
    key = _device_room_key_from_parts(integration_id, device_id)
    if not key:
        raise ValueError("Integration id and device id are required.")
    store = _load_room_store(client)
    store.setdefault("device_names", {}).pop(key, None)
    store = _save_room_store(store, client)
    registry = _rebuild_integration_device_registry_cache(client, source="device_name_clear")
    return {"ok": True, "device_key": key, "room_overrides": get_integration_room_overrides(client), "registry": registry}


def get_integration_device_group(integration_id: str) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    definition = _coerce_definition(module)
    if not definition:
        raise KeyError(f"Unknown integration: {_text(integration_id)}")
    group = _device_group(module, definition)
    return {"group": group}


def get_integration_devices_by_capability(capability: str, client: Any = None, *, refresh: bool = False) -> List[Dict[str, Any]]:
    token = _canonical_capability(capability)
    out: List[Dict[str, Any]] = []
    if not token:
        return out
    registry = get_integration_device_registry(client, refresh=refresh)
    for category in registry.get("categories") or []:
        if isinstance(category, dict) and _text(category.get("id")) == token:
            return [dict(item) for item in category.get("devices") or [] if isinstance(item, dict)]
    for device in registry.get("devices") or []:
        if not isinstance(device, dict):
            continue
        caps = set(_normalize_capabilities(device.get("category_ids") or device.get("capabilities")))
        if token in caps:
            out.append(dict(device))
    return out


def _action_implied_state(action: str) -> str:
    token = _normalize_token(action)
    if token in {"turn_on", "on", "light_on", "switch_on", "open", "cover_open", "garage_open", "open_garage"}:
        return "open" if "open" in token else "on"
    if token in {"turn_off", "off", "light_off", "switch_off", "close", "cover_close", "garage_close", "close_garage"}:
        return "closed" if "close" in token else "off"
    if token in {"lock"}:
        return "locked"
    if token in {"unlock"}:
        return "unlocked"
    return ""


def _device_matches_action_target(device: Dict[str, Any], integration_id: str, device_id: str) -> bool:
    if _text(device.get("integration_id")) != _text(integration_id):
        return False
    target_variants = set(_token_variants(device_id))
    if not target_variants:
        return False
    for token in _device_state_tokens(device):
        if target_variants.intersection(_token_variants(token)):
            return True
    return False


def _update_cached_device_after_action(
    integration_id: str,
    action_id: str,
    device_id: str,
    result: Any,
    client: Any = None,
) -> None:
    state = _action_implied_state(action_id)
    if not state:
        return
    redis_obj = _cache_client(client)
    if not redis_obj:
        return
    registry = get_cached_integration_device_registry(redis_obj)
    if not registry:
        return
    now = time.time()
    result_payload = result if isinstance(result, dict) else {"result": _text(result)}
    updated = False

    def update_list(rows: Any) -> None:
        nonlocal updated
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict) or not _device_matches_action_target(row, integration_id, device_id):
                continue
            row["state"] = state
            row["status"] = state
            row["last_action"] = _normalize_token(action_id)
            row["last_action_at"] = now
            row["last_action_result"] = result_payload
            online = _online_from_state_text(state)
            if online is not None:
                row["online"] = online
            updated = True

    update_list(registry.get("devices"))
    for group in registry.get("groups") or []:
        if isinstance(group, dict):
            update_list(group.get("devices"))
    for category in registry.get("categories") or []:
        if not isinstance(category, dict):
            continue
        update_list(category.get("devices"))
        for room in category.get("rooms") or []:
            if isinstance(room, dict):
                update_list(room.get("devices"))
    for room in registry.get("rooms") or []:
        if isinstance(room, dict):
            update_list(room.get("devices"))
    if updated:
        save_integration_device_registry_cache(registry, redis_obj)


def save_integration_settings(integration_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    saver = getattr(module, "save_integration_settings", None)
    if not callable(saver):
        raise KeyError(f"{integration_id} does not support settings saves.")
    values = saver(dict(payload or {}))
    return {
        "ok": True,
        "id": _text(integration_id),
        "values": values if isinstance(values, dict) else _read_values(module),
        "status": _read_status(module),
    }


def run_integration_action(integration_id: str, action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    module = _module_for_integration(integration_id)
    runner = getattr(module, "run_integration_action", None)
    if not callable(runner):
        raise KeyError(f"{integration_id} does not support actions.")
    result = runner(_text(action_id), dict(payload or {}))
    if result is None:
        result = {}
    if not isinstance(result, dict):
        result = {"result": result}
    result.setdefault("ok", True)
    result.setdefault("id", _text(integration_id))
    result.setdefault("action", _text(action_id))
    result["status"] = _read_status(module)
    return result


def run_integration_device_action(integration_id: str, action_id: str, device_id: str, payload: Dict[str, Any] | None = None) -> Any:
    module = _module_for_integration(integration_id)
    action = _text(action_id)
    device = _text(device_id)
    if not action:
        raise KeyError("Device action is required.")
    if not device:
        raise KeyError("Device id is required.")

    runner = getattr(module, "run_integration_device_action", None)
    if callable(runner):
        result = runner(action, device, dict(payload or {}))
        _update_cached_device_after_action(integration_id, action, device, result)
        return result

    runner = getattr(module, "integration_device_action", None)
    if callable(runner):
        result = runner(action, device, dict(payload or {}))
        _update_cached_device_after_action(integration_id, action, device, result)
        return result

    if action in {"camera_snapshot", "snapshot"}:
        snapshotter = getattr(module, "get_camera_snapshot", None)
        if callable(snapshotter):
            return snapshotter(device)

    raise KeyError(f"{integration_id} does not support device action {action}.")


def get_integration_registry_errors() -> List[str]:
    return list(integration_registry_errors)
