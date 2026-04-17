from __future__ import annotations

import json
from typing import Any, Dict, List

from helpers import redis_client

REDIS_VOICE_SATELLITE_REGISTRY_KEY = "tater:voice:satellites:registry:v1"
VOICE_CORE_TARGET_PREFIX = "voice_core:"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_voice_target(raw: Any) -> str:
    token = _text(raw)
    if not token:
        return ""
    lower = token.lower()
    if lower.startswith(VOICE_CORE_TARGET_PREFIX):
        selector = _text(token[len(VOICE_CORE_TARGET_PREFIX):])
        return f"{VOICE_CORE_TARGET_PREFIX}{selector}" if selector else ""
    if lower.startswith("ha:") or lower.startswith("media_player."):
        return ""
    return f"{VOICE_CORE_TARGET_PREFIX}{token}"


def normalize_announcement_targets(value: Any) -> List[str]:
    raw_items: List[Any] = []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = _text(value)
        if text:
            parsed = None
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
            if isinstance(parsed, list):
                raw_items = parsed
            else:
                raw_items = [part.strip() for part in text.replace("\n", ",").split(",")]

    rows: List[str] = []
    seen = set()
    for item in raw_items:
        target = _normalize_voice_target(item)
        if not target or target in seen:
            continue
        seen.add(target)
        rows.append(target)
    return rows


def split_announcement_targets(value: Any) -> Dict[str, List[str]]:
    voice_core_selectors: List[str] = []

    for target in normalize_announcement_targets(value):
        lower = target.lower()
        selector = target
        if lower.startswith(VOICE_CORE_TARGET_PREFIX):
            selector = _text(target[len(VOICE_CORE_TARGET_PREFIX):])
        if selector:
            voice_core_selectors.append(selector)

    return {
        "homeassistant_media_players": [],
        "voice_core_selectors": voice_core_selectors,
    }


def _voice_core_satellite_label(row: Dict[str, Any], selector: str) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    name = _text(row.get("name"))
    area = ""
    for key in ("area_name", "room_name", "room", "area"):
        area = _text(metadata.get(key))
        if area:
            break
    host = _text(row.get("host"))
    title = name or area or selector
    details = []
    if area and area.lower() != title.lower():
        details.append(area)
    if host and host.lower() != title.lower():
        details.append(host)
    details.append(selector)
    suffix = " • ".join(part for part in details if part)
    return f"Voice Core: {title} ({suffix})" if suffix else f"Voice Core: {title}"


def get_voice_core_satellite_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()

    try:
        raw = redis_client.get(REDIS_VOICE_SATELLITE_REGISTRY_KEY)
        parsed = json.loads(raw) if raw else []
    except Exception:
        parsed = []

    if isinstance(parsed, list):
        for item in parsed:
            row = item if isinstance(item, dict) else {}
            selector = _text(row.get("selector"))
            if not selector:
                host = _text(row.get("host"))
                if host:
                    selector = f"host:{host.lower()}"
            if not selector:
                continue
            value = f"{VOICE_CORE_TARGET_PREFIX}{selector}"
            if value in seen:
                continue
            seen.add(value)
            rows.append({"value": value, "label": _voice_core_satellite_label(row, selector)})

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(VOICE_CORE_TARGET_PREFIX) or value in seen:
            continue
        selector = _text(value[len(VOICE_CORE_TARGET_PREFIX):])
        if not selector:
            continue
        rows.append({"value": value, "label": f"Voice Core: {selector} (saved)"})
        seen.add(value)

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def fetch_homeassistant_media_player_target_options(
    base_url: Any,
    token: Any,
    *,
    current_values: Any = None,
) -> List[Dict[str, str]]:
    return []


def build_announcement_target_options(
    *,
    homeassistant_base_url: Any,
    homeassistant_token: Any,
    current_values: Any = None,
) -> List[Dict[str, str]]:
    return get_voice_core_satellite_target_options(current_values=current_values)
