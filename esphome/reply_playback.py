from __future__ import annotations

from typing import Any, Dict, List

from announcement_targets import build_announcement_target_options, normalize_announcement_targets
from tateros import integration_store as integration_store_module

REPLY_PLAYBACK_DEVICE = "device"
REPLY_PLAYBACK_SILENT = "silent"
HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"


def _text(value: Any) -> str:
    return str(value or "").strip()


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    fn = integration_store_module.integration_function("homeassistant", "load_homeassistant_config")
    if fn:
        return fn(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": HOMEASSISTANT_DEFAULT_BASE_URL, "token": ""}


def _lower(value: Any) -> str:
    return _text(value).lower()


def normalize_reply_playback_target(value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    lower = token.lower().replace("-", "_").replace(" ", "_")
    if lower in {"device", "this_device", "on_device", "local", "local_device", "box_speaker"}:
        return REPLY_PLAYBACK_DEVICE
    if lower in {"silent", "none", "off", "muted", "display_only", "display"}:
        return REPLY_PLAYBACK_SILENT
    targets = normalize_announcement_targets([token])
    return targets[0] if targets else ""


def is_external_reply_playback_target(value: Any) -> bool:
    token = normalize_reply_playback_target(value)
    return bool(token and token not in {REPLY_PLAYBACK_DEVICE, REPLY_PLAYBACK_SILENT})


def _looks_like_s3box_display(row: Any, client_row: Any = None) -> bool:
    data = row if isinstance(row, dict) else {}
    client = client_row if isinstance(client_row, dict) else {}
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    device_info = client.get("device_info") if isinstance(client.get("device_info"), dict) else {}
    pieces = [
        data.get("selector"),
        data.get("host"),
        data.get("name"),
        data.get("source"),
        meta.get("firmware_template"),
        meta.get("firmware_template_key"),
        meta.get("device_name"),
        device_info.get("name"),
        device_info.get("friendly_name"),
        device_info.get("model"),
        device_info.get("project_name"),
    ]
    haystack = " ".join(_lower(part) for part in pieces if _text(part))
    if not haystack:
        return False
    display_hints = {"taters3box", "tater s3box", "tater-s3box", "s3box display", "s3box-display"}
    box_hints = {"esp32-s3-box-3", "esp32 s3 box 3", "s3-box-3", "s3 box 3"}
    return any(hint in haystack for hint in display_hints) or any(hint in haystack for hint in box_hints)


def default_reply_playback_target(row: Any, *, client_row: Any = None) -> str:
    return REPLY_PLAYBACK_SILENT if _looks_like_s3box_display(row, client_row) else REPLY_PLAYBACK_DEVICE


def resolve_reply_playback_target(row: Any, *, client_row: Any = None) -> str:
    data = row if isinstance(row, dict) else {}
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    configured = normalize_reply_playback_target(meta.get("reply_playback_target"))
    return configured or default_reply_playback_target(data, client_row=client_row)


def _current_external_values(current_value: Any) -> List[str]:
    target = normalize_reply_playback_target(current_value)
    if is_external_reply_playback_target(target):
        return [target]
    return []


def build_reply_playback_options(current_value: Any = None) -> List[Dict[str, str]]:
    current_external = _current_external_values(current_value)
    options: List[Dict[str, str]] = [
        {"value": REPLY_PLAYBACK_DEVICE, "label": "This device speaker"},
        {"value": REPLY_PLAYBACK_SILENT, "label": "Silent / display only"},
    ]
    try:
        ha_config = load_homeassistant_config(required=False)
    except Exception:
        ha_config = {"base": "", "token": ""}
    try:
        options.extend(
            build_announcement_target_options(
                homeassistant_base_url=ha_config.get("base", ""),
                homeassistant_token=ha_config.get("token", ""),
                include_homeassistant=True,
                include_sonos=True,
                include_unifi_protect=True,
                include_voice_core=True,
                current_values=current_external,
            )
        )
    except Exception:
        for value in current_external:
            options.append({"value": value, "label": f"{value} (saved)"})

    seen = set()
    rows: List[Dict[str, str]] = []
    for option in options:
        value = normalize_reply_playback_target(option.get("value"))
        if not value or value in seen:
            continue
        seen.add(value)
        rows.append({"value": value, "label": _text(option.get("label")) or value})
    return rows
