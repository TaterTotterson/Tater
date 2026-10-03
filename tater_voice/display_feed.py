from __future__ import annotations

import importlib.util
import json
import math
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from helpers import redis_client
from integration_runtime import integration_runtime_states
from . import display_bus


_DISPLAY_PROFILE_HASH_KEY = "tater:display:profiles:v1"
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
    "compact",
    "device",
    "display_target",
    "format",
    "limit",
    "mode",
    "selector",
    "target",
    "token",
    "x_tater_token",
}
_ENTITY_LIST_QUERY_KEYS = {"entity", "entities", "entity_id", "entity_ids", "slots"}
_ENTITY_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_]+$")
_INTEGRATION_SOURCE_LABELS: Dict[str, str] = {
    "environment": "Environment Core",
    "homeassistant": "Home Assistant",
    "unifi_protect": "UniFi Protect",
    "unifi_network": "UniFi Network",
    "hue": "Philips Hue",
    "ecobee_homekit": "Ecobee HomeKit",
    "weather_api": "WeatherAPI.com",
}
_ENVIRONMENT_PROVIDER_LATEST_KEYS: Dict[str, str] = {
    "ecowitt": "environment:latest:ecowitt",
    "unifi_protect": "environment:latest:unifi_protect",
    "ecobee_homekit": "environment:latest:ecobee_homekit",
    "hue": "environment:latest:hue",
    "homeassistant": "environment:latest:homeassistant",
    "weather_api": "environment:latest:weather_api",
}
_ENVIRONMENT_SETTINGS_KEY = "environment_core_settings"
_DEFAULT_TEMPERATURE_UNIT = "F"
_DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES = 20


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace").strip()
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _assistant_first_name(client: Any = None) -> str:
    try:
        value = (client or redis_client).get("tater:first_name")
    except Exception:
        value = ""
    return _text(value) or "Tater"


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


def _value_label(value: Any, unit: str = "") -> str:
    number = _as_float(value)
    if number is not None:
        rounded = round(number, 2)
        text = str(int(rounded)) if float(rounded).is_integer() else str(rounded).rstrip("0").rstrip(".")
    else:
        text = _text(value)
    return f"{text} {_text(unit)}".strip()


def _temperature_unit(value: Any, default: str = _DEFAULT_TEMPERATURE_UNIT) -> str:
    token = _text(value).strip().lower().replace("°", "")
    if token in {"c", "celcius", "celsius", "centigrade", "metric"}:
        return "C"
    if token in {"f", "fahrenheit", "imperial", "us"}:
        return "F"
    default_token = _text(default).strip().upper()
    if default_token in {"C", "F"}:
        return default_token
    return ""


def _environment_temperature_unit(client: Any) -> str:
    try:
        raw = (client or redis_client).hget(_ENVIRONMENT_SETTINGS_KEY, "ENVIRONMENT_TEMPERATURE_UNIT")
    except Exception:
        raw = ""
    return _temperature_unit(raw, _DEFAULT_TEMPERATURE_UNIT)


def _temperature_row_unit(row: Dict[str, Any]) -> str:
    for value in (row.get("native_unit"), row.get("source_unit"), row.get("unit")):
        unit = _temperature_unit(value, "")
        if unit in {"C", "F"}:
            return unit
    return ""


def _temperature_value(value: Any, from_unit: Any, to_unit: Any) -> Optional[float]:
    number = _as_float(value)
    if number is None:
        return None
    source = _temperature_unit(from_unit, "")
    target = _temperature_unit(to_unit, _DEFAULT_TEMPERATURE_UNIT)
    if source == target:
        return number
    if source == "C" and target == "F":
        return (number * 9.0 / 5.0) + 32.0
    if source == "F" and target == "C":
        return (number - 32.0) * 5.0 / 9.0
    return number


def _normalize_environment_temperature_row(row: Dict[str, Any], preferred_unit: str) -> Dict[str, Any]:
    next_row = dict(row)
    unit = _temperature_row_unit(next_row)
    if unit not in {"C", "F"}:
        return next_row
    category = _lower(next_row.get("category"))
    haystack = f"{next_row.get('key') or ''} {next_row.get('label') or ''}".lower()
    if category != "temperature" and "temp" not in haystack and "temperature" not in haystack:
        return next_row
    native_value = next_row.get("native_value", next_row.get("source_value", next_row.get("value")))
    converted = _temperature_value(native_value, unit, preferred_unit)
    if converted is None:
        return next_row
    target = _temperature_unit(preferred_unit, _DEFAULT_TEMPERATURE_UNIT)
    next_row.setdefault("native_unit", unit)
    next_row.setdefault("native_value", _as_float(native_value))
    if _text(next_row.get("display")):
        next_row.setdefault("native_display", _text(next_row.get("display")))
    next_row["unit"] = target
    next_row["value"] = round(converted, 2)
    next_row["display"] = _value_label(converted, target)
    next_row["normalized_unit"] = target
    return next_row


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


def _display_target_key(value: Any) -> str:
    token = _lower(value)
    if not token:
        return ""
    clean = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return clean or token


def _integration_source_label(provider: Any) -> str:
    token = _lower(provider)
    if not token:
        return "Tater"
    label = _INTEGRATION_SOURCE_LABELS.get(token)
    if label:
        return label
    return " ".join(part.capitalize() for part in re.split(r"[_\-\s]+", token) if part) or token


def _split_slot_spec(spec: Any) -> Tuple[str, str]:
    token = _text(spec)
    if not token:
        return "", ""
    if _ENTITY_ID_RE.match(token):
        return "homeassistant", token
    if ":" not in token:
        return "", ""
    provider, state_id = token.split(":", 1)
    provider = _lower(provider)
    state_id = _text(state_id)
    if provider not in _INTEGRATION_SOURCE_LABELS or not state_id:
        return "", ""
    return provider, state_id


def _valid_slot_spec(spec: Any) -> bool:
    provider, state_id = _split_slot_spec(spec)
    return bool(provider and state_id)


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
    if _valid_slot_spec(spec):
        slots[_slug(spec)] = spec
        return
    alias = ""
    entity_id = spec
    if ":" in spec:
        left, right = spec.split(":", 1)
        alias = _slug(left, "slot")
        entity_id = _text(right)
    if not _valid_slot_spec(entity_id):
        return
    slots[alias or _slug(entity_id)] = entity_id


def _json_from_hash(client: Any, hash_key: str, field: str) -> Dict[str, Any]:
    try:
        raw = client.hget(hash_key, field)
    except Exception:
        return {}
    if raw in (None, ""):
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _display_identity_items(query: Any) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    for raw_key, raw_value in _iter_query_items(query):
        key = _lower(raw_key)
        if key not in {"target", "display_target", "device", "selector"}:
            continue
        value = _display_target_key(raw_value)
        if value:
            items.append((key, value))
    return items


def _display_identity_candidates(query: Any) -> List[str]:
    items = _display_identity_items(query)
    candidates: List[str] = []
    for wanted_key in ("target", "display_target", "device", "selector"):
        for key, value in items:
            if key == wanted_key and value not in candidates:
                candidates.append(value)
    return candidates


def _display_profile_slots(profile: Mapping[str, Any]) -> Dict[str, str]:
    slots = profile.get("slots") if isinstance(profile.get("slots"), dict) else {}
    return {alias: _text(value) for alias, value in slots.items() if _valid_slot_spec(value)}


def _display_profile_matches_candidates(profile: Mapping[str, Any], fallback_key: Any, candidates: Iterable[str]) -> bool:
    candidate_set = {value for value in candidates if value}
    if not candidate_set:
        return False
    values = [
        fallback_key,
        profile.get("target"),
        profile.get("display_target"),
        profile.get("selector"),
        profile.get("id"),
        profile.get("device"),
        profile.get("device_name"),
    ]
    return any(_display_target_key(value) in candidate_set for value in values if _text(value))


def _display_profile_matches_selector(profile: Mapping[str, Any], fallback_key: Any, candidates: Iterable[str]) -> bool:
    candidate_set = {value for value in candidates if value}
    if not candidate_set:
        return False
    values = [
        fallback_key,
        profile.get("selector"),
        profile.get("id"),
        profile.get("device"),
        profile.get("device_name"),
    ]
    return any(_display_target_key(value) in candidate_set for value in values if _text(value))


def _iter_saved_display_profiles(client: Any) -> Iterable[Tuple[str, Dict[str, Any]]]:
    try:
        raw_rows = client.hgetall(_DISPLAY_PROFILE_HASH_KEY)
    except Exception:
        return []
    if not isinstance(raw_rows, dict):
        return []
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for raw_key, raw_value in raw_rows.items():
        key = _text(raw_key)
        try:
            parsed = json.loads(_text(raw_value))
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        template = _lower(parsed.get("template"))
        if template and template not in {"s3box_display", "native_screen"}:
            continue
        rows.append((key, parsed))
    return rows


def _saved_display_profile_from_query(query: Any, client: Any) -> Tuple[Dict[str, Any], bool]:
    identity_items = _display_identity_items(query)
    candidates = _display_identity_candidates(query)
    selector_candidates: List[str] = []
    for key, value in identity_items:
        if key == "selector" and value not in selector_candidates:
            selector_candidates.append(value)
    if not candidates:
        return {}, False
    target_fallback: Dict[str, Any] = {}
    for candidate in candidates:
        profile = _json_from_hash(client, _DISPLAY_PROFILE_HASH_KEY, candidate)
        if profile:
            if selector_candidates and not _display_profile_matches_selector(profile, candidate, selector_candidates):
                if not target_fallback:
                    target_fallback = profile
                continue
            return profile, True
    saved_profiles = list(_iter_saved_display_profiles(client))
    if selector_candidates:
        for fallback_key, profile in saved_profiles:
            if _display_profile_matches_selector(profile, fallback_key, selector_candidates):
                return profile, True
    for fallback_key, profile in saved_profiles:
        if _display_profile_matches_candidates(profile, fallback_key, candidates):
            return profile, True
    if target_fallback:
        return target_fallback, True
    return {}, False


def _slot_map_from_saved_display_profile(query: Any, client: Any) -> Tuple[Dict[str, str], bool]:
    profile, found = _saved_display_profile_from_query(query, client)
    if not found:
        return {}, False
    return _display_profile_slots(profile), True


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
        if _valid_slot_spec(value):
            slots[alias] = value
    return slots


def _target_from_query(query: Any) -> str:
    candidates = _display_identity_candidates(query)
    return candidates[0] if candidates else ""


def _compact_requested(query: Any) -> bool:
    for raw_key, raw_value in _iter_query_items(query):
        key = _lower(raw_key)
        value = _lower(raw_value)
        if key in {"format", "mode"} and value in {"compact", "firmware", "lite"}:
            return True
        if key == "compact":
            return value not in {"", "0", "false", "no", "off"}
    return False


def _runtime_integration_states(client: Any = None) -> Dict[str, Dict[str, Any]]:
    try:
        snapshot = integration_runtime_states(client or redis_client)
    except Exception:
        snapshot = {}
    rows = snapshot.get("states") if isinstance(snapshot, dict) and isinstance(snapshot.get("states"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for record in rows:
        if not isinstance(record, dict):
            continue
        provider = _lower(record.get("provider"))
        if not provider:
            continue
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        state_id = _text(payload.get("entity_id") if provider == "homeassistant" else record.get("id"))
        if not state_id:
            state_id = _text(record.get("id"))
        if not state_id:
            continue
        out[f"{provider}:{state_id}"] = record
        if provider == "homeassistant":
            out[state_id] = record
    out.update(_environment_runtime_states(client or redis_client))
    return out


def _json_from_redis_key(client: Any, key: str, default: Any) -> Any:
    try:
        raw = client.get(key)
    except Exception:
        return default
    if raw in (None, ""):
        return default
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _environment_provider_snapshots(client: Any) -> Dict[str, Dict[str, Any]]:
    snapshots: Dict[str, Dict[str, Any]] = {}
    for provider, key in _ENVIRONMENT_PROVIDER_LATEST_KEYS.items():
        snapshot = _json_from_redis_key(client, key, {})
        if isinstance(snapshot, dict) and snapshot:
            snapshots[provider] = snapshot
    latest = _json_from_redis_key(client, "environment:latest", {})
    if isinstance(latest, dict) and latest:
        provider = _lower(latest.get("provider")) or "environment"
        snapshots.setdefault(provider, latest)
    return snapshots


def _environment_reading_state_id(row: Dict[str, Any]) -> str:
    provider = _lower(row.get("provider")) or "environment"
    source_id = _text(row.get("source_id")) or provider
    key = _text(row.get("key"))
    return f"{provider}:{source_id}:{key}" if key else ""


def _environment_runtime_states(client: Any) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    preferred_temperature_unit = _environment_temperature_unit(client)
    for provider, snapshot in _environment_provider_snapshots(client).items():
        if not isinstance(snapshot, dict):
            continue
        provider_token = _lower(snapshot.get("provider")) or _lower(provider)
        source_id = _text(snapshot.get("source_id")) or provider_token
        source_name = _text(snapshot.get("model") or snapshot.get("stationtype")) or _integration_source_label(provider_token)
        for row in snapshot.get("readings") or []:
            if not isinstance(row, dict):
                continue
            reading_key = _text(row.get("key"))
            if not reading_key:
                continue
            enriched = dict(row)
            enriched.setdefault("provider", provider_token)
            enriched.setdefault("provider_label", _integration_source_label(provider_token))
            enriched.setdefault("source_id", source_id)
            enriched.setdefault("source_name", source_name)
            enriched = _normalize_environment_temperature_row(enriched, preferred_temperature_unit)
            state_id = _environment_reading_state_id(enriched)
            if not state_id:
                continue
            label = _text(enriched.get("label")) or reading_key
            unit = _text(enriched.get("unit"))
            payload = {
                "entity_id": f"environment:{state_id}",
                "name": label,
                "display_name": label,
                "state": enriched.get("value"),
                "display": _text(enriched.get("display")),
                "unit": unit,
                "unit_of_measurement": unit,
                "attributes": {
                    "friendly_name": label,
                    "unit_of_measurement": unit,
                    "device_class": _text(enriched.get("category")),
                    "area": _text(enriched.get("area")),
                    "source_provider": provider_token,
                    "source_name": source_name,
                },
            }
            out[f"environment:{state_id}"] = {
                "provider": "environment",
                "id": state_id,
                "payload": payload,
                "updated_at": snapshot.get("received_at"),
                "environment": enriched,
            }
    return out


def _environment_core_installed() -> bool:
    """Return whether Environment Core is actually installed, not merely cached."""
    try:
        import core_registry

        core_dir = getattr(core_registry, "CORE_DIR", None)
        if core_dir is not None and (Path(core_dir) / "environment_core.py").exists():
            return True
    except Exception:
        pass
    try:
        return importlib.util.find_spec("cores.environment_core") is not None
    except Exception:
        return False


def _environment_setting(client: Any, key: str, default: str = "") -> str:
    try:
        value = client.hget(_ENVIRONMENT_SETTINGS_KEY, key)
    except Exception:
        value = ""
    return _text(value) or default


def _environment_source_provider(value: Any) -> str:
    token = _lower(value)
    if token.startswith("provider:"):
        return token.split(":", 1)[1]
    return token if token in _ENVIRONMENT_PROVIDER_LATEST_KEYS else ""


def _environment_reading(snapshot: Dict[str, Any], *keys: str) -> Optional[Dict[str, Any]]:
    wanted = {_lower(key) for key in keys if _text(key)}
    for row in snapshot.get("readings") or []:
        if isinstance(row, dict) and _lower(row.get("key")) in wanted:
            return row
    return None


def _environment_category_reading(
    snapshot: Dict[str, Any],
    category: str,
    *,
    prefer_outdoor: bool = False,
) -> Optional[Dict[str, Any]]:
    rows = [
        row
        for row in (snapshot.get("readings") or [])
        if isinstance(row, dict) and _lower(row.get("category")) == _lower(category)
    ]
    if prefer_outdoor:
        for row in rows:
            area = _lower(row.get("area"))
            if area in {"outside", "outdoor", "forecast"}:
                return row
    return rows[0] if rows else None


def _environment_temperature_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _environment_reading(
        snapshot,
        "tempf",
        "tempc",
        "weather_api_temperature",
        "outdoor_temperature",
        "temperature",
    ) or _environment_category_reading(snapshot, "temperature", prefer_outdoor=True)


def _environment_indoor_temperature_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    row = _environment_reading(
        snapshot,
        "temp_in",
        "tempinf",
        "tempinc",
        "indoor_temperature",
        "temperature_indoor",
    )
    if row:
        return row
    rows = [
        candidate
        for candidate in (snapshot.get("readings") or [])
        if isinstance(candidate, dict) and _lower(candidate.get("category")) == "temperature"
    ]
    for candidate in rows:
        area = _lower(candidate.get("area"))
        if area and area not in {"outside", "outdoor", "forecast"}:
            return candidate
    return None


def _environment_indoor_humidity_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    row = _environment_reading(
        snapshot,
        "humidity_in",
        "indoor_humidity",
        "humidity_indoor",
    )
    if row:
        return row
    rows = [
        candidate
        for candidate in (snapshot.get("readings") or [])
        if isinstance(candidate, dict) and _lower(candidate.get("category")) == "humidity"
    ]
    for candidate in rows:
        area = _lower(candidate.get("area"))
        if area and area not in {"outside", "outdoor", "forecast"}:
            return candidate
    return None


def _environment_condition_reading(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    row = _environment_reading(snapshot, "weather_api_condition", "current_condition", "condition")
    if row:
        return row
    for candidate in snapshot.get("readings") or []:
        if not isinstance(candidate, dict) or _lower(candidate.get("category")) != "condition":
            continue
        if _lower(candidate.get("key")) in {"cloud", "weather_api_cloud"}:
            continue
        value = _text(candidate.get("display") or candidate.get("value"))
        if value and _as_float(value) is None:
            return candidate
    return None


def _environment_reading_text(row: Optional[Dict[str, Any]]) -> str:
    if not isinstance(row, dict):
        return ""
    return _text(row.get("display") or row.get("value"))


def _environment_wind_direction_text(row: Optional[Dict[str, Any]]) -> str:
    if not isinstance(row, dict):
        return ""
    degrees = _as_float(row.get("value"))
    if degrees is None:
        return _environment_reading_text(row)
    compass = (
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    )
    return compass[int((degrees % 360.0 + 11.25) // 22.5) % len(compass)]


def _environment_weather_kind(condition: Any) -> str:
    token = _lower(condition)
    if "partly" in token:
        return "partly"
    if any(word in token for word in ("thunder", "storm", "lightning")):
        return "storm"
    if any(word in token for word in ("rain", "drizzle", "shower", "sleet")):
        return "rain"
    if any(word in token for word in ("snow", "ice", "blizzard", "flurr")):
        return "snow"
    if any(word in token for word in ("fog", "mist", "haze", "smoke")):
        return "fog"
    if any(word in token for word in ("cloud", "overcast")):
        return "cloud"
    if any(word in token for word in ("wind", "breez")):
        return "wind"
    if any(word in token for word in ("sun", "clear")):
        return "sun"
    return "partly"


def _environment_number_text(value: Any, *, digits: int = 0) -> str:
    number = _as_float(value)
    if number is None:
        return ""
    if digits <= 0:
        return str(int(round(number)))
    return f"{number:.{digits}f}".rstrip("0").rstrip(".")


def _environment_weather_source(snapshot: Dict[str, Any], row: Optional[Dict[str, Any]]) -> str:
    if isinstance(row, dict):
        source = _text(row.get("source_name"))
        if source:
            return source
    provider = _lower(snapshot.get("provider"))
    return (
        _text(snapshot.get("model") or snapshot.get("stationtype"))
        or _integration_source_label(provider)
        or "Environment Core"
    )


def _environment_received_at(*snapshots: Dict[str, Any]) -> float:
    values: List[float] = []
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            continue
        value = _as_float(snapshot.get("received_at") or snapshot.get("sample_time"))
        if value is not None:
            values.append(value)
    return max(values) if values else 0.0


def _tater_display_clock() -> Dict[str, Any]:
    now = datetime.now().astimezone()
    offset = now.utcoffset()
    return {
        "clock_unix_ms": int(time.time() * 1000),
        "utc_offset_seconds": int(offset.total_seconds()) if offset is not None else 0,
        "timezone": _text(now.tzname()) or "Tater",
    }


def build_weather_summary(
    *,
    client: Any = None,
    core_installed: Optional[bool] = None,
    selector: str = "",
) -> Dict[str, Any]:
    """Build the small, secret-free weather payload sent to screen satellites."""
    result = _tater_display_clock()
    if core_installed is None:
        core_installed = _environment_core_installed()
    if not core_installed:
        return {**result, "available": False, "environment_installed": False}

    redis_obj = client or redis_client
    snapshots = _environment_provider_snapshots(redis_obj)
    if not snapshots:
        return {**result, "available": False, "environment_installed": True}

    live_provider = _environment_source_provider(
        _environment_setting(
            redis_obj,
            "ENVIRONMENT_CURRENT_CONDITION_LIVE_SOURCE",
            "provider:ecowitt",
        )
    )
    condition_provider = _environment_source_provider(
        _environment_setting(
            redis_obj,
            "ENVIRONMENT_CURRENT_CONDITION_CONDITION_SOURCE",
            "provider:weather_api",
        )
    )
    live_snapshot = (
        snapshots.get(live_provider)
        or snapshots.get("ecowitt")
        or snapshots.get("weather_api")
        or next(iter(snapshots.values()))
    )
    condition_snapshot = (
        snapshots.get(condition_provider)
        or snapshots.get("weather_api")
        or live_snapshot
    )

    preferred_unit = _environment_temperature_unit(redis_obj)
    selected_slots: Dict[str, Dict[str, Any]] = {}
    slot_specs: Dict[str, str] = {}
    profile_found = False
    if _text(selector):
        slot_specs, profile_found = _slot_map_from_saved_display_profile(
            {"selector": _text(selector), "target": _text(selector)},
            redis_obj,
        )
        runtime_states = _runtime_integration_states(redis_obj) if slot_specs else {}
        for alias, spec in slot_specs.items():
            provider, state_id = _split_slot_spec(spec)
            selected_slots[alias] = _normalize_integration_slot(
                alias,
                spec,
                runtime_record=runtime_states.get(spec) or runtime_states.get(f"{provider}:{state_id}"),
            )

    def selected_number(alias: str) -> Optional[float]:
        slot = selected_slots.get(alias)
        if not isinstance(slot, dict) or not bool(slot.get("available")):
            return None
        return _as_float(slot.get("numeric_value", slot.get("value")))

    def selected_temperature(alias: str) -> str:
        slot = selected_slots.get(alias)
        if not isinstance(slot, dict) or not bool(slot.get("available")):
            return ""
        converted = _temperature_value(
            slot.get("numeric_value", slot.get("value")),
            slot.get("unit"),
            preferred_unit,
        )
        return _environment_number_text(converted)

    def selected_text(alias: str) -> str:
        slot = selected_slots.get(alias)
        if not isinstance(slot, dict) or not bool(slot.get("available")):
            return ""
        return _text(slot.get("display") or slot.get("state") or slot.get("value"))

    def slot_is_visible(alias: str) -> bool:
        return not profile_found or alias in slot_specs

    temperature_row = _environment_temperature_reading(live_snapshot)
    if temperature_row is None:
        temperature_row = _environment_temperature_reading(condition_snapshot)
    temperature_row = (
        _normalize_environment_temperature_row(temperature_row, preferred_unit)
        if isinstance(temperature_row, dict)
        else None
    )
    condition_row = _environment_condition_reading(condition_snapshot)
    if condition_row is None:
        condition_row = _environment_condition_reading(live_snapshot)
    condition = _environment_reading_text(condition_row)

    temperature = selected_temperature("temp_out") if slot_is_visible("temp_out") else ""
    if not temperature and not profile_found:
        temperature = _environment_number_text(
            temperature_row.get("value") if isinstance(temperature_row, dict) else None
        )

    feels_row = _environment_reading(
        condition_snapshot,
        "weather_api_feelslike",
        "feelslike",
        "feels_like",
        "apparent_temperature",
    ) or _environment_reading(
        live_snapshot,
        "weather_api_feelslike",
        "feelslike",
        "feels_like",
        "apparent_temperature",
    )
    if isinstance(feels_row, dict):
        feels_row = _normalize_environment_temperature_row(feels_row, preferred_unit)
    humidity_row = _environment_reading(
        live_snapshot,
        "humidity",
        "humidity_out",
        "outdoor_humidity",
        "weather_api_humidity",
    ) or _environment_category_reading(live_snapshot, "humidity", prefer_outdoor=True)
    if humidity_row is None:
        humidity_row = _environment_reading(condition_snapshot, "weather_api_humidity", "humidity")
    wind_row = _environment_reading(
        live_snapshot,
        "windspeedmph",
        "weather_api_wind_speed",
        "wind_speed",
        "windspeed",
    ) or _environment_category_reading(live_snapshot, "wind_speed")
    if wind_row is None:
        wind_row = _environment_reading(condition_snapshot, "windspeedmph", "weather_api_wind_speed", "wind_speed")
    wind_direction_row = _environment_reading(
        live_snapshot,
        "weather_api_wind_dir",
        "wind_direction",
        "winddir",
    ) or _environment_reading(
        condition_snapshot,
        "weather_api_wind_dir",
        "wind_direction",
        "winddir",
    )
    lightning_row = _environment_reading(
        live_snapshot,
        "lightning_num",
        "lightning_strikes",
        "lightning",
    ) or _environment_category_reading(live_snapshot, "lightning")
    rain_row = _environment_reading(
        live_snapshot,
        "rain_rate",
        "rainratein",
        "rainfall_rate",
        "precipitation_rate",
    ) or _environment_category_reading(live_snapshot, "rain_rate") \
        or _environment_category_reading(live_snapshot, "rain")

    indoor_temperature = selected_temperature("temp_in") if slot_is_visible("temp_in") else ""
    if not indoor_temperature and not profile_found:
        indoor_row = _environment_indoor_temperature_reading(live_snapshot)
        if indoor_row is None:
            for candidate_snapshot in snapshots.values():
                indoor_row = _environment_indoor_temperature_reading(candidate_snapshot)
                if indoor_row is not None:
                    break
        if isinstance(indoor_row, dict):
            indoor_row = _normalize_environment_temperature_row(indoor_row, preferred_unit)
            indoor_temperature = _environment_number_text(indoor_row.get("value"))

    indoor_humidity = selected_text("humidity_in") if slot_is_visible("humidity_in") else ""
    if not indoor_humidity and not profile_found:
        indoor_humidity_row = _environment_indoor_humidity_reading(live_snapshot)
        if indoor_humidity_row is None:
            for candidate_snapshot in snapshots.values():
                indoor_humidity_row = _environment_indoor_humidity_reading(candidate_snapshot)
                if indoor_humidity_row is not None:
                    break
        indoor_humidity = _environment_reading_text(indoor_humidity_row)

    feels = _environment_number_text(
        feels_row.get("value") if temperature and isinstance(feels_row, dict) else None
    )
    feels_like_relation = ""
    shown_temperature = _as_float(temperature)
    shown_feels = _as_float(feels)
    if shown_temperature is not None and shown_feels is not None:
        if shown_feels < shown_temperature:
            feels_like_relation = "cooler"
        elif shown_feels > shown_temperature:
            feels_like_relation = "warmer"
        else:
            feels_like_relation = "same"
    humidity = selected_text("humidity_out") if slot_is_visible("humidity_out") else ""
    if not humidity and not profile_found:
        humidity_number = _environment_number_text(
            humidity_row.get("value") if isinstance(humidity_row, dict) else None
        )
        humidity = f"{humidity_number}%" if humidity_number else ""
    selected_wind = selected_number("wind_speed")
    wind_speed = _environment_number_text(
        selected_wind if selected_wind is not None else (
            wind_row.get("value") if isinstance(wind_row, dict) else None
        )
    )
    selected_wind_slot = selected_slots.get("wind_speed")
    wind_unit = (
        _text(selected_wind_slot.get("unit"))
        if selected_wind is not None and isinstance(selected_wind_slot, dict)
        else (_text(wind_row.get("unit")) if isinstance(wind_row, dict) else "")
    )
    wind_direction = _environment_wind_direction_text(wind_direction_row)
    if profile_found:
        wind_text = selected_text("wind_speed") if slot_is_visible("wind_speed") else ""
    else:
        wind_text = " ".join(part for part in (wind_direction, wind_speed, wind_unit) if part)
    lightning_text = selected_text("lightning_strikes") if slot_is_visible("lightning_strikes") else ""
    if not lightning_text and not profile_found:
        lightning_text = _environment_reading_text(lightning_row)
    rain_text = selected_text("rain_rate") if slot_is_visible("rain_rate") else ""
    if not rain_text and not profile_found:
        rain_text = _environment_reading_text(rain_row)

    received_at = _environment_received_at(live_snapshot, condition_snapshot)
    try:
        stale_minutes = int(float(_environment_setting(
            redis_obj,
            "ENVIRONMENT_STALE_AFTER_MINUTES",
            str(_DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES),
        )))
    except Exception:
        stale_minutes = _DEFAULT_ENVIRONMENT_STALE_AFTER_MINUTES
    stale_minutes = max(1, min(10080, stale_minutes))
    stale = bool(received_at and time.time() - received_at > stale_minutes * 60)

    source = _environment_weather_source(condition_snapshot, condition_row)
    if not condition:
        condition = "Current conditions"
        source = _environment_weather_source(live_snapshot, temperature_row)
    return {
        **result,
        "available": True,
        "environment_installed": True,
        "temperature_text": f"{temperature}°" if temperature else "",
        "temperature_unit": preferred_unit,
        "indoor_temperature_text": f"{indoor_temperature}°" if indoor_temperature else "",
        "indoor_humidity_text": indoor_humidity,
        "condition": condition,
        "condition_kind": _environment_weather_kind(condition),
        "feels_like_text": f"Feels like {feels}°" if feels else "",
        "feels_like_relation": feels_like_relation,
        "humidity_text": humidity,
        "wind_text": wind_text,
        "rain_text": rain_text,
        "lightning_text": lightning_text,
        "source": source,
        "stale": stale,
        "updated_at": received_at,
    }


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


def _payload_name(payload: Dict[str, Any], attrs: Dict[str, Any], provider: str, state_id: str, alias: str) -> str:
    for value in (
        attrs.get("friendly_name"),
        payload.get("name"),
        payload.get("display_name"),
        payload.get("displayName"),
        payload.get("friendlyName"),
        payload.get("id"),
    ):
        token = _text(value)
        if token:
            return token
    return _DEFAULT_SLOT_LABELS.get(alias) or _slug(state_id, "Sensor")


def _payload_state(payload: Dict[str, Any]) -> str:
    for key in ("state", "status", "value", "event_type", "current_hvac_state"):
        token = _text(payload.get(key))
        if token:
            return token
    for key in ("current_temperature_f", "current_temperature_c", "current_humidity"):
        value = payload.get(key)
        if value not in (None, ""):
            return _text(value)
    return ""


def _payload_unit(payload: Dict[str, Any], attrs: Dict[str, Any], state_key: str = "") -> str:
    for value in (
        attrs.get("unit_of_measurement"),
        payload.get("unit_of_measurement"),
        payload.get("unit"),
    ):
        token = _text(value)
        if token:
            return token
    if any(_text(payload.get(key)) for key in ("state", "status", "value", "event_type")):
        return ""
    if state_key == "current_temperature_f" or payload.get("current_temperature_f") not in (None, ""):
        return "°F"
    if state_key == "current_temperature_c" or payload.get("current_temperature_c") not in (None, ""):
        return "°C"
    if state_key == "current_humidity" or payload.get("current_humidity") not in (None, ""):
        return "%"
    return ""


def _normalize_integration_slot(
    alias: str,
    spec: str,
    *,
    runtime_record: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    record = runtime_record if isinstance(runtime_record, dict) else {}
    parsed_provider, parsed_state_id = _split_slot_spec(spec)
    provider = _lower(record.get("provider")) or parsed_provider
    state_id = _text(record.get("id")) or parsed_state_id
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    attrs = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    state = _payload_state(payload)
    unit = _payload_unit(payload, attrs)
    numeric = _as_float(state)
    available = bool(payload) and _lower(state) not in {"", "unknown", "unavailable", "none"}
    updated_at = record.get("updated_at") if record else None
    if updated_at in (None, ""):
        updated_at = payload.get("last_updated") or payload.get("last_changed") or ""

    return {
        "alias": _slug(alias, "slot"),
        "entity_id": _text(spec),
        "integration_id": f"{provider}:{state_id}" if provider and state_id else _text(spec),
        "source_id": state_id,
        "source_provider": provider,
        "source_label": _integration_source_label(provider),
        "name": _payload_name(payload, attrs, provider, state_id, alias),
        "available": available,
        "state": state,
        "value": numeric if numeric is not None else state,
        "numeric_value": numeric,
        "unit": unit,
        "display": _text(payload.get("display")) or _display_text(state, unit),
        "device_class": _text(attrs.get("device_class")),
        "state_class": _text(attrs.get("state_class")),
        "icon": _text(attrs.get("icon")),
        "updated_at": updated_at,
        "source": provider or "missing",
    }


def build_display_feed(query: Any = None, *, client: Any = None, version: str = "") -> Dict[str, Any]:
    redis_obj = client or redis_client
    identity_present = bool(_display_identity_candidates(query))
    requested_slots, saved_profile_found = _slot_map_from_saved_display_profile(query, redis_obj)
    if not requested_slots and not saved_profile_found and not identity_present:
        requested_slots = _slot_map_from_query(query)
    runtime_states = _runtime_integration_states(redis_obj)
    clock = _local_clock()
    assistant = {
        "first_name": _assistant_first_name(redis_obj),
    }

    slots: Dict[str, Dict[str, Any]] = {}
    values: Dict[str, Any] = {}
    text_values: Dict[str, str] = {}
    for alias, spec in requested_slots.items():
        provider, state_id = _split_slot_spec(spec)
        runtime_record = runtime_states.get(spec) or runtime_states.get(f"{provider}:{state_id}")
        slot = _normalize_integration_slot(
            alias,
            spec,
            runtime_record=runtime_record,
        )
        clean_alias = _text(slot.get("alias")) or alias
        slots[clean_alias] = slot
        values[clean_alias] = slot.get("numeric_value") if slot.get("numeric_value") is not None else slot.get("state")
        text_values[clean_alias] = _text(slot.get("display") or slot.get("state"))

    if _compact_requested(query):
        return {
            "ok": True,
            "service": "tater_display",
            "version": _text(version),
            "ts": time.time(),
            "assistant": assistant,
            "assistant_name": assistant["first_name"],
            "clock": {
                "date": clock.get("date", ""),
                "time": clock.get("time", ""),
                "ampm": clock.get("ampm", ""),
            },
            "values": values,
            "text": text_values,
            "count": len(slots),
        }

    return {
        "ok": True,
        "service": "tater_display",
        "version": _text(version),
        "ts": time.time(),
        "assistant": assistant,
        "assistant_name": assistant["first_name"],
        "clock": clock,
        "slots": slots,
        "values": values,
        "text": text_values,
        "events": display_bus.display_feed_events(_target_from_query(query), client=redis_obj),
        "count": len(slots),
    }
