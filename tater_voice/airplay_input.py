from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import announcement_targets
import external_audio
from helpers import redis_client


logger = logging.getLogger("tater_voice.airplay_input")

REDIS_AIRPLAY_INPUT_SETTINGS_KEY = "tater:voice:airplay_input:v1"
DEFAULT_RECEIVER_NAME = "Tater Audio"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _default_settings() -> Dict[str, Any]:
    return {
        "enabled": False,
        "receiver_name": DEFAULT_RECEIVER_NAME,
        "receiver_pin": "",
        "targets": [],
    }


def load_settings() -> Dict[str, Any]:
    document: Dict[str, Any] = {}
    try:
        raw = redis_client.get(REDIS_AIRPLAY_INPUT_SETTINGS_KEY)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(str(raw)) if raw else {}
        if isinstance(parsed, dict):
            document = parsed
    except Exception:
        logger.exception("Could not load AirPlay Input settings.")

    settings = _default_settings()
    settings.update(
        {
            "enabled": _as_bool(document.get("enabled"), False),
            "receiver_name": _text(document.get("receiver_name"))[:80]
            or DEFAULT_RECEIVER_NAME,
            "receiver_pin": _text(document.get("receiver_pin")),
            "targets": announcement_targets.normalize_announcement_targets(
                document.get("targets")
            ),
        }
    )
    return settings


def _target_kind(value: Any) -> str:
    token = _text(value).lower()
    if token.startswith(("voice_core:stereo:", "stereo:")):
        return "stereo"
    if token.startswith(("voice_core:native:", "native:")):
        return "satellite"
    if token.startswith("sonos:"):
        return "sonos"
    if token.startswith("airplay:"):
        return "airplay"
    return ""


def _is_supported_option(option: Dict[str, Any]) -> bool:
    kind = _target_kind(option.get("value"))
    if kind in {"satellite", "stereo", "airplay"}:
        return True
    if kind == "sonos":
        return _text(option.get("airplay_bridge_target")).lower().startswith("airplay:")
    return False


def _clean_label(label: Any) -> str:
    value = _text(label)
    for prefix in (
        "Tater Satellite:",
        "Tater Sat:",
        "Tater Stereo:",
        "AirPlay Bridge:",
        "AirPlay:",
        "Sonos:",
    ):
        if value.lower().startswith(prefix.lower()):
            return value[len(prefix) :].strip()
    return value


def destination_options(settings: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    current = settings if isinstance(settings, dict) else load_settings()
    selected = set(announcement_targets.normalize_announcement_targets(current.get("targets")))
    try:
        discovered = announcement_targets.build_announcement_target_options(
            homeassistant_base_url="",
            homeassistant_token="",
            include_homeassistant=False,
            include_sonos=True,
            include_airplay=True,
            include_voice_core=True,
            current_values=list(selected),
        )
    except Exception:
        logger.exception("Could not discover AirPlay Input destinations.")
        discovered = []

    rows: List[Dict[str, Any]] = []
    seen = set()
    for raw in discovered:
        option = dict(raw) if isinstance(raw, dict) else {}
        value = _text(option.get("value"))
        if not value or value in seen or not _is_supported_option(option):
            continue
        seen.add(value)
        kind = _target_kind(value)
        description = _text(option.get("description"))
        if not description:
            description = {
                "satellite": "Tater Native satellite",
                "stereo": "Synchronized Tater stereo pair",
                "sonos": "Sonos player with a matching AirPlay endpoint",
                "airplay": "Discovered AirPlay speaker",
            }.get(kind, "Audio destination")
        label = _clean_label(option.get("label")) or value
        offline = "offline" in label.lower()
        rows.append(
            {
                "value": value,
                "label": label,
                "kind": kind,
                "description": description,
                "selected": value in selected,
                "available": not offline,
            }
        )

    rows.sort(key=lambda row: (_text(row.get("kind")), _text(row.get("label")).lower()))
    return rows


def _validate_targets(values: Any, options: List[Dict[str, Any]]) -> List[str]:
    targets = announcement_targets.normalize_announcement_targets(values)
    allowed = {_text(option.get("value")) for option in options}
    unsupported = [target for target in targets if target not in allowed]
    if unsupported:
        raise ValueError(
            "Choose currently available Tater satellites, stereo pairs, or AirPlay-capable speakers."
        )
    return targets


def _runtime_config(settings: Dict[str, Any]) -> Dict[str, Any]:
    targets = list(settings.get("targets") or [])
    return {
        "enabled": bool(settings.get("enabled")),
        "receiver_name": _text(settings.get("receiver_name")) or DEFAULT_RECEIVER_NAME,
        "receiver_pin": _text(settings.get("receiver_pin")),
        "targets": targets,
        "volume_percent": 100,
        "target_volume_percent": {target: 100 for target in targets},
        "target_sync_offset_ms": {},
        "target_transport_mode": {
            target: "airplay"
            for target in targets
            if _target_kind(target) == "sonos"
        },
    }


def configure_runtime(settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    current = settings if isinstance(settings, dict) else load_settings()
    result = external_audio.configure_external_audio_runtime(_runtime_config(current))
    return result if isinstance(result, dict) else {}


def startup() -> Dict[str, Any]:
    try:
        return configure_runtime()
    except Exception as exc:
        logger.exception("Could not start AirPlay Input.")
        return {"enabled": False, "status": "error", "receiver_error": _text(exc)}


def save_settings(values: Dict[str, Any]) -> Dict[str, Any]:
    data = values if isinstance(values, dict) else {}
    current = load_settings()
    next_settings = dict(current)

    if "enabled" in data:
        next_settings["enabled"] = _as_bool(data.get("enabled"), False)
    if "receiver_name" in data:
        next_settings["receiver_name"] = (
            _text(data.get("receiver_name"))[:80] or DEFAULT_RECEIVER_NAME
        )
    if "receiver_pin" in data:
        pin = _text(data.get("receiver_pin"))
        if pin and (len(pin) != 4 or not pin.isdigit()):
            raise ValueError("The AirPlay pairing PIN must be exactly four digits, or left blank.")
        next_settings["receiver_pin"] = pin
    if "targets" in data:
        next_settings["targets"] = _validate_targets(
            data.get("targets"), destination_options(next_settings)
        )

    if next_settings["enabled"] and not next_settings["targets"]:
        raise ValueError("Choose at least one playback destination before enabling AirPlay Input.")

    redis_client.set(
        REDIS_AIRPLAY_INPUT_SETTINGS_KEY,
        json.dumps(next_settings, ensure_ascii=False),
    )
    status = configure_runtime(next_settings)
    return {"settings": next_settings, "status": status}


def stop_input() -> Dict[str, Any]:
    result = external_audio.stop_external_audio_input()
    return result if isinstance(result, dict) else {}


def panel_payload() -> Dict[str, Any]:
    settings = load_settings()
    try:
        status = external_audio.get_external_audio_status()
        if not isinstance(status, dict):
            status = {}
    except Exception as exc:
        status = {
            "enabled": bool(settings.get("enabled")),
            "status": "error",
            "receiver_error": _text(exc),
            "input_active": False,
        }
    return {
        "settings": settings,
        "status": status,
        "options": destination_options(settings),
    }
