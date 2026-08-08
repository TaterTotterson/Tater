from __future__ import annotations

import json
import re
from typing import Any, Dict, List
from urllib.parse import quote, unquote, urlparse

from helpers import redis_client
from tateros import integration_store as integration_store_module

REDIS_VOICE_SATELLITE_REGISTRY_KEY = "tater:voice:satellites:registry:v1"
HOMEASSISTANT_TARGET_PREFIX = "ha:"
VOICE_CORE_TARGET_PREFIX = "voice_core:"
UNIFI_PROTECT_TARGET_PREFIX = "unifi:"
SONOS_TARGET_PREFIX = "sonos:"
AIRPLAY_TARGET_PREFIX = "airplay:"
INTEGRATION_TARGET_PREFIX = "integration:"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _integration_function(integration_id: str, function_name: str):
    return integration_store_module.integration_function(integration_id, function_name)


def _integration_registry_devices(capability: str, *, integration_id: str = "") -> List[Dict[str, Any]]:
    try:
        from integration_registry import get_integration_devices_by_capability

        devices = get_integration_devices_by_capability(capability, redis_client)
    except Exception:
        return []
    wanted = _text(integration_id).lower()
    rows: List[Dict[str, Any]] = []
    for row in devices:
        if not isinstance(row, dict):
            continue
        if wanted and _text(row.get("integration_id")).lower() != wanted:
            continue
        rows.append(dict(row))
    return rows


def _device_details(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("details") if isinstance(row.get("details"), dict) else {}


def _device_tokens(row: Dict[str, Any], *keys: str) -> str:
    details = _device_details(row)
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return _text(value)
        value = details.get(key)
        if value not in (None, ""):
            return _text(value)
    return ""


def integration_target_value(integration_id: Any, device_id: Any) -> str:
    integration = _text(integration_id).lower()
    device = _text(device_id)
    if not integration or not device:
        return ""
    return f"{INTEGRATION_TARGET_PREFIX}{integration}:{quote(device, safe='')}"


def parse_integration_target(value: Any) -> Dict[str, str]:
    token = _text(value)
    if not token.lower().startswith(INTEGRATION_TARGET_PREFIX):
        return {}
    body = token[len(INTEGRATION_TARGET_PREFIX) :]
    integration, sep, encoded_device = body.partition(":")
    integration = _text(integration).lower()
    device = _text(unquote(encoded_device)) if sep else ""
    if not integration or not device:
        return {}
    return {"integration_id": integration, "device_id": device}


def entity_registry_list_sync(*args, **kwargs):
    fn = _integration_function("homeassistant", "entity_registry_list_sync")
    return fn(*args, **kwargs) if fn else []


def discover_sonos_speakers(*args, **kwargs):
    fn = _integration_function("sonos", "discover_sonos_speakers")
    return fn(*args, **kwargs) if fn else []


def sonos_target_id(value: Any) -> str:
    fn = _integration_function("sonos", "sonos_target_id")
    if fn:
        return fn(value)
    token = _text(value)
    if token.lower().startswith(SONOS_TARGET_PREFIX):
        token = _text(token[len(SONOS_TARGET_PREFIX) :])
    if token.lower().startswith("uuid:"):
        token = _text(token[5:])
    return token


def resolve_sonos_target(value: Any) -> Dict[str, Any]:
    fn = _integration_function("sonos", "resolve_sonos_target")
    if fn:
        row = fn(value)
        return dict(row) if isinstance(row, dict) else {}
    return {}


def airplay_target_id(value: Any) -> str:
    try:
        from airplay_bridge import normalize_airplay_id

        return normalize_airplay_id(value)
    except Exception:
        token = _text(value)
        if token.lower().startswith(AIRPLAY_TARGET_PREFIX):
            token = token[len(AIRPLAY_TARGET_PREFIX) :]
        return "".join(character for character in token.lower() if character.isalnum())


def airplay_target_value(value: Any) -> str:
    device_id = airplay_target_id(value)
    return f"{AIRPLAY_TARGET_PREFIX}{device_id}" if device_id else ""


def list_unifi_cameras(*args, **kwargs):
    fn = _integration_function("unifi_protect", "list_unifi_cameras")
    return fn(*args, **kwargs) if fn else []


def unifi_camera_entity(camera_id: Any) -> str:
    fn = _integration_function("unifi_protect", "unifi_camera_entity")
    if fn:
        return fn(camera_id)
    token = _text(camera_id).lower()
    return f"camera.unifi_{token}" if token else ""


def unifi_camera_id_from_target(target: Any) -> str:
    fn = _integration_function("unifi_protect", "unifi_camera_id_from_target")
    if fn:
        return fn(target)
    token = _text(target)
    lower = token.lower()
    if lower.startswith(UNIFI_PROTECT_TARGET_PREFIX):
        token = _text(token.split(":", 1)[1])
        lower = token.lower()
    if lower.startswith("camera."):
        object_id = lower.split(".", 1)[1]
        return object_id[len("unifi_") :] if object_id.startswith("unifi_") else object_id
    return lower[len("unifi_") :] if lower.startswith("unifi_") else lower


def unifi_camera_name(row: Dict[str, Any], camera_id: str) -> str:
    fn = _integration_function("unifi_protect", "unifi_camera_name")
    if fn:
        return fn(row, camera_id)
    for key in ("name", "displayName", "display_name", "friendlyName", "friendly_name"):
        value = _text((row or {}).get(key))
        if value:
            return value
    return camera_id


def unifi_camera_has_speaker_hint(row: Dict[str, Any]) -> bool:
    fn = _integration_function("unifi_protect", "unifi_camera_has_speaker_hint")
    return bool(fn(row)) if fn else False


def unifi_protect_configured(*args, **kwargs) -> bool:
    fn = _integration_function("unifi_protect", "unifi_protect_configured")
    return bool(fn(*args, **kwargs)) if fn else False


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
    if lower.startswith(AIRPLAY_TARGET_PREFIX):
        return airplay_target_value(token)
    if lower.startswith(INTEGRATION_TARGET_PREFIX):
        parsed = parse_integration_target(token)
        return integration_target_value(parsed.get("integration_id"), parsed.get("device_id")) if parsed else ""
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
    airplay_players: List[str] = []
    integration_devices: List[Dict[str, str]] = []

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
        if lower.startswith(AIRPLAY_TARGET_PREFIX):
            player_ref = airplay_target_id(target)
            if player_ref:
                airplay_players.append(player_ref)
            continue
        if lower.startswith(INTEGRATION_TARGET_PREFIX):
            parsed = parse_integration_target(target)
            if parsed:
                integration_devices.append(parsed)
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
        "airplay_players": airplay_players,
        "integration_devices": integration_devices,
    }


def _voice_core_satellite_label(row: Dict[str, Any], selector: str) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
    name = (
        _text(row.get("name"))
        or _text(row.get("friendly_name"))
        or _text(row.get("device_name"))
        or _text(device_info.get("friendly_name"))
        or _text(device_info.get("name"))
    )
    area = ""
    for key in ("area_name", "room_name", "room", "area"):
        area = _text(metadata.get(key))
        if area:
            break
        area = _text(row.get(key))
        if area:
            break
        area = _text(device_info.get(key))
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
    return f"Tater Satellite: {title} ({suffix})" if suffix else f"Tater Satellite: {title}"


def _voice_core_selector_from_row(row: Dict[str, Any]) -> str:
    selector = _text(row.get("selector"))
    if selector:
        return selector
    host = _text(row.get("host")).lower()
    return f"host:{host}" if host else ""


def _voice_core_registry_row_is_native(row: Dict[str, Any]) -> bool:
    selector = _voice_core_selector_from_row(row)
    source = _text(row.get("source")).lower()
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return (
        selector.startswith("native:")
        or source in {"tater_native", "native_satellite"}
        or bool(metadata.get("native_selected"))
        or bool(metadata.get("native_protocol"))
    )


def _voice_core_connected_clients() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    try:
        from tater_voice import native_satellite

        status = native_satellite.status_snapshot_sync()
        clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
        for selector, row in clients.items():
            if not isinstance(row, dict) or not bool(row.get("connected")):
                continue
            token = _text(selector) or _voice_core_selector_from_row(row)
            if token:
                out[token] = dict(row)
    except Exception:
        pass

    try:
        from tater_voice import runtime as esphome_runtime

        status = esphome_runtime.status()
    except Exception:
        return out
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    for selector, row in clients.items():
        if not isinstance(row, dict) or not bool(row.get("connected")):
            continue
        token = _text(selector) or _voice_core_selector_from_row(row)
        if not token or token in out:
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

    for selector, registry_row in registry_by_selector.items():
        if selector in connected_clients or not _voice_core_registry_row_is_native(registry_row):
            continue
        value = f"{VOICE_CORE_TARGET_PREFIX}{selector}"
        if value in seen:
            continue
        seen.add(value)
        rows.append(
            {
                "value": value,
                "label": f"{_voice_core_satellite_label(registry_row, selector)} • offline",
            }
        )

    try:
        from tater_voice import stereo_pairs

        required_capabilities = {
            "synchronized_media_sessions",
            "stereo_channel_selection",
            "media_playhead_telemetry",
            "media_drift_correction",
        }
        for pair in stereo_pairs.list_pairs():
            pair_selector = _text(pair.get("selector"))
            left_selector = _text(pair.get("left_selector"))
            right_selector = _text(pair.get("right_selector"))
            if not pair_selector or not left_selector or not right_selector:
                continue
            member_rows = [connected_clients.get(left_selector), connected_clients.get(right_selector)]
            ready = all(isinstance(member, dict) for member in member_rows)
            if ready:
                for member in member_rows:
                    capabilities = (
                        member.get("capabilities")
                        if isinstance(member.get("capabilities"), dict)
                        else {}
                    )
                    try:
                        session_version = int(float(capabilities.get("audio_session_version") or 0))
                    except Exception:
                        session_version = 0
                    if session_version < 2 or any(
                        not bool(capabilities.get(capability))
                        for capability in required_capabilities
                    ):
                        ready = False
                        break
            value = f"{VOICE_CORE_TARGET_PREFIX}{pair_selector}"
            if value in seen:
                continue
            seen.add(value)
            left_name = _text((connected_clients.get(left_selector) or {}).get("device_name")) or left_selector
            right_name = _text((connected_clients.get(right_selector) or {}).get("device_name")) or right_selector
            status = "ready" if ready else "offline or firmware update required"
            rows.append(
                {
                    "value": value,
                    "label": (
                        f"Tater Stereo: {_text(pair.get('name')) or pair_selector} "
                        f"({left_name} L + {right_name} R • {status})"
                    ),
                }
            )
    except Exception:
        pass

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(VOICE_CORE_TARGET_PREFIX) or value in seen:
            continue
        selector = _text(value[len(VOICE_CORE_TARGET_PREFIX):])
        if not selector:
            continue
        rows.append({"value": value, "label": f"Tater Satellite: {selector} (saved)"})
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

    registry_rows = _integration_registry_devices("media_player", integration_id="homeassistant")
    for item in registry_rows:
        entity_id = _text(item.get("ref") or item.get("id"))
        if not entity_id.lower().startswith("media_player."):
            continue
        name = _text(item.get("name")) or entity_id
        label = f"Home Assistant: {name} ({entity_id})" if name != entity_id else f"Home Assistant: {entity_id}"
        add_row(entity_id, label)

    if not registry_rows and base and bearer:
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
    explicit = _text(row.get("label"))
    if explicit:
        return explicit if explicit.lower().startswith("sonos:") else f"Sonos: {explicit}"
    name = _text(row.get("display_name")) or _text(row.get("name")) or speaker_id
    details = []
    try:
        member_count = int(float(row.get("member_count") or 0))
    except Exception:
        member_count = 0
    model = _text(row.get("model"))
    if member_count > 1:
        details.append(f"{member_count} speakers")
    elif model and model.lower() != name.lower():
        details.append(model)
    suffix = " • ".join(part for part in details if part)
    return f"Sonos: {name} ({suffix})" if suffix else f"Sonos: {name}"


def _row_values(row: Dict[str, Any], key: str) -> List[str]:
    details = _device_details(row)
    raw = row.get(key)
    if raw in (None, ""):
        raw = details.get(key)
    values = list(raw) if isinstance(raw, (list, tuple, set)) else [raw]
    return [_text(value) for value in values if _text(value)]


def _host_token(value: Any) -> str:
    token = _text(value).lower()
    if not token:
        return ""
    parsed = urlparse(token if "://" in token else f"//{token}")
    return _text(parsed.hostname).lower()


def _sonos_bridge_match_tokens(row: Dict[str, Any], speaker_id: str) -> Dict[str, List[str]]:
    ids = set()
    hosts = set()
    for value in [speaker_id, *_row_values(row, "member_ids"), *_row_values(row, "aliases")]:
        match = re.search(r"rincon[_:-]?([0-9a-f]{12})", _text(value), flags=re.IGNORECASE)
        if match:
            ids.add(match.group(1).lower())
    for key in ("host", "root_url", "location", "member_hosts", "member_root_urls"):
        for value in _row_values(row, key):
            host = _host_token(value)
            if host:
                hosts.add(host)
    return {"ids": sorted(ids), "hosts": sorted(hosts)}


def _airplay_bridge_match_tokens(row: Dict[str, Any]) -> Dict[str, List[str]]:
    device_id = airplay_target_id(row.get("id") or row.get("target"))
    host = _host_token(row.get("host"))
    return {
        "ids": [device_id] if device_id else [],
        "hosts": [host] if host else [],
    }


def fetch_sonos_speaker_target_options(*, current_values: Any = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()

    registry_rows = _integration_registry_devices("media_player", integration_id="sonos")
    source_rows = registry_rows if registry_rows else discover_sonos_speakers()
    for item in source_rows:
        if not isinstance(item, dict):
            continue
        speaker_id = sonos_target_id(item.get("id") or _device_tokens(item, "udn", "root_url"))
        if not speaker_id:
            continue
        value = f"{SONOS_TARGET_PREFIX}{speaker_id}"
        if value in seen:
            continue
        seen.add(value)
        match_tokens = _sonos_bridge_match_tokens(item, speaker_id)
        rows.append(
            {
                "value": value,
                "label": _sonos_speaker_label(item, speaker_id),
                "sonos_device_id": speaker_id,
                "bridge_match_ids": match_tokens["ids"],
                "bridge_match_hosts": match_tokens["hosts"],
            }
        )

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(SONOS_TARGET_PREFIX) or value in seen:
            continue
        speaker_ref = sonos_target_id(value)
        if not speaker_ref:
            continue
        resolved = resolve_sonos_target(speaker_ref)
        if resolved:
            resolved_id = sonos_target_id(resolved.get("id") or resolved.get("udn") or resolved.get("root_url"))
            resolved_value = f"{SONOS_TARGET_PREFIX}{resolved_id}" if resolved_id else ""
            label = _sonos_speaker_label(resolved, resolved_id or speaker_ref)
            if resolved_value == value:
                match_tokens = _sonos_bridge_match_tokens(resolved, resolved_id or speaker_ref)
                rows.append(
                    {
                        "value": value,
                        "label": f"{label} (saved)",
                        "sonos_device_id": resolved_id or speaker_ref,
                        "bridge_match_ids": match_tokens["ids"],
                        "bridge_match_hosts": match_tokens["hosts"],
                    }
                )
            else:
                rows.append({"value": value, "label": f"{label} (saved paired member)"})
        else:
            rows.append({"value": value, "label": f"Sonos: {speaker_ref} (saved)"})
        seen.add(value)

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def fetch_airplay_target_options(*, current_values: Any = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    try:
        from airplay_bridge import discover_airplay_devices, resolve_airplay_target

        discovered = discover_airplay_devices()
    except Exception:
        discovered = []
        resolve_airplay_target = None

    def add_row(item: Dict[str, Any], *, saved: bool = False) -> None:
        device_id = airplay_target_id(item.get("id") or item.get("target"))
        value = airplay_target_value(device_id)
        if not value or value in seen:
            return
        seen.add(value)
        name = _text(item.get("name")) or device_id
        details = []
        manufacturer = _text(item.get("manufacturer"))
        model = _text(item.get("model"))
        host = _text(item.get("host"))
        if manufacturer and manufacturer.casefold() not in name.casefold():
            details.append(manufacturer)
        if model and model.casefold() not in name.casefold():
            details.append(model)
        if host:
            details.append(host)
        if saved and not bool(item.get("available", False)):
            details.append("offline")
        suffix = f" ({' • '.join(details)})" if details else ""
        match_tokens = _airplay_bridge_match_tokens(item)
        rows.append(
            {
                "value": value,
                "label": f"AirPlay Bridge: {name}{suffix}",
                "description": "Wall-clock scheduled through Tater AirPlay Bridge",
                "airplay_device_id": device_id,
                "bridge_match_ids": match_tokens["ids"],
                "bridge_match_hosts": match_tokens["hosts"],
            }
        )

    for item in discovered:
        if isinstance(item, dict):
            add_row(item)

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(AIRPLAY_TARGET_PREFIX) or value in seen:
            continue
        resolved: Dict[str, Any] = {}
        if callable(resolve_airplay_target):
            try:
                resolved = resolve_airplay_target(value)
            except Exception:
                resolved = {}
        if resolved:
            add_row(resolved, saved=True)
        else:
            device_id = airplay_target_id(value)
            rows.append(
                {
                    "value": value,
                    "label": f"AirPlay Bridge: {device_id} (saved • offline)",
                    "description": "Wall-clock scheduled through Tater AirPlay Bridge",
                }
            )
            seen.add(value)

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def merge_sonos_airplay_target_options(
    sonos_rows: List[Dict[str, Any]],
    airplay_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Collapse a Sonos receiver and its AirPlay endpoint into one player option."""
    used_airplay_values = set()
    merged: List[Dict[str, Any]] = []
    for raw_sonos in sonos_rows:
        sonos = dict(raw_sonos)
        sonos_ids = set(_row_values(sonos, "bridge_match_ids"))
        sonos_hosts = set(_row_values(sonos, "bridge_match_hosts"))
        match: Dict[str, Any] = {}
        for airplay in airplay_rows:
            value = _text(airplay.get("value"))
            if not value or value in used_airplay_values:
                continue
            airplay_ids = set(_row_values(airplay, "bridge_match_ids"))
            airplay_hosts = set(_row_values(airplay, "bridge_match_hosts"))
            if (sonos_ids and airplay_ids and sonos_ids & airplay_ids) or (
                sonos_hosts and airplay_hosts and sonos_hosts & airplay_hosts
            ):
                match = dict(airplay)
                break
        if match:
            bridge_target = _text(match.get("value"))
            used_airplay_values.add(bridge_target)
            sonos.update(
                {
                    "airplay_bridge_target": bridge_target,
                    "target_aliases": [bridge_target],
                    "transport_options": [
                        {"value": "auto", "label": "Automatic"},
                        {"value": "native", "label": "Native Sonos"},
                        {"value": "airplay", "label": "AirPlay Bridge"},
                    ],
                    "description": (
                        "Automatic uses AirPlay Bridge with Tater sats and native Sonos "
                        "for Sonos-only playback."
                    ),
                }
            )
        sonos.pop("bridge_match_ids", None)
        sonos.pop("bridge_match_hosts", None)
        merged.append(sonos)

    for raw_airplay in airplay_rows:
        if _text(raw_airplay.get("value")) in used_airplay_values:
            continue
        airplay = dict(raw_airplay)
        airplay.pop("bridge_match_ids", None)
        airplay.pop("bridge_match_hosts", None)
        merged.append(airplay)
    return merged


def resolve_sonos_airplay_target(value: Any) -> str:
    """Return the matching AirPlay target for a Sonos target, if one is available."""
    speaker_id = sonos_target_id(value)
    if not speaker_id:
        return ""
    speaker = resolve_sonos_target(speaker_id)
    sonos_tokens = _sonos_bridge_match_tokens(
        speaker if isinstance(speaker, dict) else {},
        speaker_id,
    )
    if not sonos_tokens["ids"] and not sonos_tokens["hosts"]:
        return ""
    sonos_ids = set(sonos_tokens["ids"])
    sonos_hosts = set(sonos_tokens["hosts"])
    try:
        from airplay_bridge import discover_airplay_devices

        devices = discover_airplay_devices()
    except Exception:
        devices = []
    for device in devices:
        if not isinstance(device, dict):
            continue
        airplay_tokens = _airplay_bridge_match_tokens(device)
        if (sonos_ids and set(airplay_tokens["ids"]) & sonos_ids) or (
            sonos_hosts and set(airplay_tokens["hosts"]) & sonos_hosts
        ):
            return airplay_target_value(device.get("id") or device.get("target"))
    return ""


def fetch_unifi_protect_camera_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, Any]] = []
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

    registry_rows = _integration_registry_devices("camera", integration_id="unifi_protect")
    for item in registry_rows:
        features = {_text(value).lower() for value in item.get("features") or [] if _text(value)}
        capabilities = {_text(value).lower() for value in item.get("capabilities") or [] if _text(value)}
        details_text = json.dumps(_device_details(item), default=str).lower()
        if not ({"speaker", "announcement"} & (features | capabilities) or "speaker" in details_text):
            continue
        camera_ref = _text(item.get("ref") or item.get("id"))
        name = _text(item.get("name")) or camera_ref
        add_row(camera_ref, f"UniFi Protect: {name} (speaker, {camera_ref})")

    if not registry_rows and unifi_protect_configured():
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


def _integration_device_playback_action(row: Dict[str, Any]) -> str:
    actions = {_text(value).lower() for value in row.get("actions") or [] if _text(value)}
    features = {_text(value).lower() for value in row.get("features") or [] if _text(value)}
    supported = actions | features
    if "announce" in supported:
        return "announce"
    if "play_url" in supported:
        return "play_url"
    if "play_media" in supported:
        return "play_media"
    return ""


def fetch_integration_playback_target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    handled_integrations = {"homeassistant", "sonos", "unifi_protect"}

    for item in _integration_registry_devices("media_player"):
        if not isinstance(item, dict):
            continue
        integration_id = _text(item.get("integration_id")).lower()
        if not integration_id or integration_id in handled_integrations:
            continue
        if not _integration_device_playback_action(item):
            continue
        device_id = _text(item.get("id") or item.get("ref"))
        value = integration_target_value(integration_id, device_id)
        if not value or value in seen:
            continue
        seen.add(value)
        integration_name = _text(item.get("integration_name")) or integration_id.replace("_", " ").title()
        name = _text(item.get("name")) or device_id
        room = _text(item.get("room") or item.get("area"))
        suffix = f" • {room}" if room and room.lower() != name.lower() else ""
        rows.append({"value": value, "label": f"{integration_name}: {name}{suffix}"})

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(INTEGRATION_TARGET_PREFIX) or value in seen:
            continue
        parsed = parse_integration_target(value)
        if not parsed:
            continue
        rows.append(
            {
                "value": value,
                "label": f"{parsed.get('integration_id')}: {parsed.get('device_id')} (saved)",
            }
        )
        seen.add(value)

    rows.sort(key=lambda row: _text(row.get("label")).lower())
    return rows


def build_announcement_target_options(
    *,
    homeassistant_base_url: Any,
    homeassistant_token: Any,
    include_homeassistant: bool = False,
    homeassistant_platforms: Any = None,
    include_sonos: bool = True,
    include_airplay: bool = False,
    include_unifi_protect: bool = False,
    include_voice_core: bool = True,
    include_integrations: bool = False,
    current_values: Any = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
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
    sonos_rows = (
        fetch_sonos_speaker_target_options(current_values=current_values)
        if include_sonos
        else []
    )
    airplay_rows = (
        fetch_airplay_target_options(current_values=current_values)
        if include_airplay
        else []
    )
    if include_sonos and include_airplay:
        rows.extend(merge_sonos_airplay_target_options(sonos_rows, airplay_rows))
    else:
        rows.extend(sonos_rows)
        rows.extend(airplay_rows)
    if include_unifi_protect:
        rows.extend(fetch_unifi_protect_camera_target_options(current_values=current_values))
    if include_integrations:
        rows.extend(fetch_integration_playback_target_options(current_values=current_values))
    return rows
