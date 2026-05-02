from __future__ import annotations

import json
from typing import Any, Dict, List

from helpers import redis_client
from integrations.homeassistant import entity_registry_list_sync
from integrations.sonos import SONOS_TARGET_PREFIX, discover_sonos_speakers, sonos_target_id
from integrations.unifi_protect import (
    list_unifi_cameras,
    unifi_camera_entity,
    unifi_camera_has_speaker_hint,
    unifi_camera_id_from_target,
    unifi_camera_name,
    unifi_protect_configured,
)

REDIS_VOICE_SATELLITE_REGISTRY_KEY = "tater:voice:satellites:registry:v1"
HOMEASSISTANT_TARGET_PREFIX = "ha:"
VOICE_CORE_TARGET_PREFIX = "voice_core:"
UNIFI_PROTECT_TARGET_PREFIX = "unifi:"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_voice_target(raw: Any) -> str:
    token = _text(raw)
    if not token:
        return ""
    lower = token.lower()
    if lower.startswith(HOMEASSISTANT_TARGET_PREFIX):
        entity_id = _text(token[len(HOMEASSISTANT_TARGET_PREFIX):])
        return f"{HOMEASSISTANT_TARGET_PREFIX}{entity_id}" if entity_id else ""
    if lower.startswith(VOICE_CORE_TARGET_PREFIX):
        selector = _text(token[len(VOICE_CORE_TARGET_PREFIX):])
        return f"{VOICE_CORE_TARGET_PREFIX}{selector}" if selector else ""
    if lower.startswith(UNIFI_PROTECT_TARGET_PREFIX):
        camera_ref = _text(token[len(UNIFI_PROTECT_TARGET_PREFIX):])
        return f"{UNIFI_PROTECT_TARGET_PREFIX}{camera_ref}" if camera_ref else ""
    if lower.startswith(SONOS_TARGET_PREFIX):
        speaker_ref = sonos_target_id(token)
        return f"{SONOS_TARGET_PREFIX}{speaker_ref}" if speaker_ref else ""
    if lower.startswith("media_player."):
        return f"{HOMEASSISTANT_TARGET_PREFIX}{token}"
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
    homeassistant_media_players: List[str] = []
    voice_core_selectors: List[str] = []
    unifi_protect_cameras: List[str] = []
    sonos_speakers: List[str] = []

    for target in normalize_announcement_targets(value):
        lower = target.lower()
        if lower.startswith(HOMEASSISTANT_TARGET_PREFIX):
            entity_id = _text(target[len(HOMEASSISTANT_TARGET_PREFIX):])
            if entity_id:
                homeassistant_media_players.append(entity_id)
            continue
        if lower.startswith(UNIFI_PROTECT_TARGET_PREFIX):
            camera_ref = _text(target[len(UNIFI_PROTECT_TARGET_PREFIX):])
            if camera_ref:
                unifi_protect_cameras.append(camera_ref)
            continue
        if lower.startswith(SONOS_TARGET_PREFIX):
            speaker_ref = sonos_target_id(target)
            if speaker_ref:
                sonos_speakers.append(speaker_ref)
            continue
        selector = target
        if lower.startswith(VOICE_CORE_TARGET_PREFIX):
            selector = _text(target[len(VOICE_CORE_TARGET_PREFIX):])
        if selector:
            voice_core_selectors.append(selector)

    return {
        "homeassistant_media_players": homeassistant_media_players,
        "voice_core_selectors": voice_core_selectors,
        "unifi_protect_cameras": unifi_protect_cameras,
        "sonos_speakers": sonos_speakers,
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


def _voice_core_selector_from_row(row: Dict[str, Any]) -> str:
    selector = _text(row.get("selector"))
    if selector:
        return selector
    host = _text(row.get("host")).lower()
    return f"host:{host}" if host else ""


def _voice_core_connected_clients() -> Dict[str, Dict[str, Any]]:
    try:
        from esphome import runtime as esphome_runtime

        status = esphome_runtime.status()
    except Exception:
        return {}
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for selector, row in clients.items():
        if not isinstance(row, dict) or not bool(row.get("connected")):
            continue
        token = _text(selector) or _voice_core_selector_from_row(row)
        if not token:
            continue
        out[token] = dict(row)
    return out


def get_voice_core_satellite_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    connected_clients = _voice_core_connected_clients()

    try:
        raw = redis_client.get(REDIS_VOICE_SATELLITE_REGISTRY_KEY)
        parsed = json.loads(raw) if raw else []
    except Exception:
        parsed = []

    registry_by_selector: Dict[str, Dict[str, Any]] = {}
    if isinstance(parsed, list):
        for item in parsed:
            row = item if isinstance(item, dict) else {}
            selector = _voice_core_selector_from_row(row)
            if selector:
                registry_by_selector[selector] = dict(row)

    for selector, client_row in connected_clients.items():
        selector = _text(selector)
        if not selector:
            continue
        label_row = dict(registry_by_selector.get(selector) or {})
        for key, value in client_row.items():
            if key == "metadata" and isinstance(value, dict):
                label_row["metadata"] = {**(label_row.get("metadata") if isinstance(label_row.get("metadata"), dict) else {}), **value}
                continue
            if value not in ("", None):
                label_row[key] = value
        if not _text(label_row.get("selector")):
            label_row["selector"] = selector
        value = f"{VOICE_CORE_TARGET_PREFIX}{selector}"
        if value in seen:
            continue
        seen.add(value)
        rows.append({"value": value, "label": _voice_core_satellite_label(label_row, selector)})

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
    platforms: Any = None,
    current_values: Any = None,
) -> List[Dict[str, str]]:
    base = _text(base_url).rstrip("/")
    bearer = _text(token)
    rows: List[Dict[str, str]] = []
    seen = set()

    allowed_platforms = {
        _text(item).lower()
        for item in (
            list(platforms)
            if isinstance(platforms, (list, tuple, set))
            else [platforms]
        )
        if _text(item)
    }

    def add_row(value: Any, label: Any = "") -> None:
        entity_id = _text(value)
        if not entity_id:
            return
        prefixed = f"{HOMEASSISTANT_TARGET_PREFIX}{entity_id}"
        if prefixed in seen:
            return
        seen.add(prefixed)
        rows.append({"value": prefixed, "label": _text(label) or entity_id})

    if base and bearer:
        try:
            payload = entity_registry_list_sync(base, bearer, timeout_s=30.0)
        except Exception:
            payload = []
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                entity_id = _text(item.get("entity_id"))
                if not entity_id.lower().startswith("media_player."):
                    continue
                if item.get("disabled_by") not in (None, ""):
                    continue
                platform = _text(item.get("platform")).lower()
                if allowed_platforms and platform not in allowed_platforms:
                    continue
                name = _text(item.get("name")) or _text(item.get("original_name")) or entity_id
                label = f"Home Assistant: {name} ({entity_id})" if name != entity_id else f"Home Assistant: {entity_id}"
                add_row(entity_id, label)

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(HOMEASSISTANT_TARGET_PREFIX) or value in seen:
            continue
        entity_id = _text(value[len(HOMEASSISTANT_TARGET_PREFIX):])
        if not entity_id:
            continue
        add_row(entity_id, f"Home Assistant: {entity_id} (saved)")

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def _sonos_speaker_label(row: Dict[str, Any], speaker_id: str) -> str:
    name = _text(row.get("name")) or speaker_id
    details = []
    host = _text(row.get("host"))
    model = _text(row.get("model"))
    if model and model.lower() != name.lower():
        details.append(model)
    if host and host.lower() != name.lower():
        details.append(host)
    suffix = " • ".join(part for part in details if part)
    return f"Sonos: {name} ({suffix})" if suffix else f"Sonos: {name}"


def fetch_sonos_speaker_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()

    for item in discover_sonos_speakers():
        if not isinstance(item, dict):
            continue
        speaker_id = sonos_target_id(item.get("id") or item.get("udn") or item.get("root_url"))
        if not speaker_id:
            continue
        value = f"{SONOS_TARGET_PREFIX}{speaker_id}"
        if value in seen:
            continue
        seen.add(value)
        rows.append({"value": value, "label": _sonos_speaker_label(item, speaker_id)})

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(SONOS_TARGET_PREFIX) or value in seen:
            continue
        speaker_ref = sonos_target_id(value)
        if not speaker_ref:
            continue
        rows.append({"value": value, "label": f"Sonos: {speaker_ref} (saved)"})
        seen.add(value)

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def fetch_unifi_protect_camera_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()

    def add_row(camera_ref: Any, label: Any = "") -> None:
        camera_id = unifi_camera_id_from_target(camera_ref)
        if not camera_id:
            return
        value = f"{UNIFI_PROTECT_TARGET_PREFIX}{unifi_camera_entity(camera_id)}"
        if value in seen:
            return
        seen.add(value)
        rows.append({"value": value, "label": _text(label) or f"UniFi Protect: {camera_id}"})

    if unifi_protect_configured():
        try:
            payload = list_unifi_cameras()
        except Exception:
            payload = []
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                camera_id = _text(item.get("id"))
                if not camera_id:
                    continue
                if not unifi_camera_has_speaker_hint(item):
                    continue
                name = unifi_camera_name(item, camera_id)
                add_row(camera_id, f"UniFi Protect: {name} (speaker, {camera_id})")

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(UNIFI_PROTECT_TARGET_PREFIX):
            continue
        camera_ref = _text(value[len(UNIFI_PROTECT_TARGET_PREFIX):])
        if not camera_ref:
            continue
        add_row(camera_ref, f"UniFi Protect: {camera_ref} (saved)")

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def build_announcement_target_options(
    *,
    homeassistant_base_url: Any,
    homeassistant_token: Any,
    include_homeassistant: bool = False,
    homeassistant_platforms: Any = None,
    include_sonos: bool = True,
    include_unifi_protect: bool = False,
    include_voice_core: bool = True,
    current_values: Any = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if include_homeassistant:
        rows.extend(
            fetch_homeassistant_media_player_target_options(
                homeassistant_base_url,
                homeassistant_token,
                platforms=homeassistant_platforms,
                current_values=current_values,
            )
        )
    if include_voice_core:
        rows.extend(get_voice_core_satellite_target_options(current_values=current_values))
    if include_sonos:
        rows.extend(fetch_sonos_speaker_target_options(current_values=current_values))
    if include_unifi_protect:
        rows.extend(fetch_unifi_protect_camera_target_options(current_values=current_values))
    return rows
