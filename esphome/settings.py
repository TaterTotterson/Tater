from __future__ import annotations

import contextlib
from typing import Any, Dict, List

from helpers import redis_client
from . import ui_helpers as esphome_ui_helpers


def _vp():
    from . import voice_pipeline as vp

    return vp


def settings_hash_key() -> str:
    vp = _vp()
    return str(vp.VOICE_CORE_SETTINGS_HASH_KEY or "voice_core_settings")


def voice_ui_setting_specs() -> List[Dict[str, Any]]:
    vp = _vp()
    return [
        {
            "key": "VOICE_NATIVE_DEBUG",
            "label": "Native Voice Debug Logs",
            "type": "checkbox",
            "default": False,
            "description": "Enable verbose voice pipeline logs.",
        },
        {
            "key": "VOICE_CONTINUED_CHAT_ENABLED",
            "label": "Continued Chat (Auto Reopen Mic)",
            "type": "checkbox",
            "default": vp.DEFAULT_CONTINUED_CHAT_ENABLED,
            "description": "If enabled, Tater uses a small AI check to decide whether to reopen the mic for a follow-up reply.",
        },
        {
            "key": "VOICE_VAD_BACKEND",
            "label": "VAD Backend",
            "type": "select",
            "default": vp.DEFAULT_VAD_BACKEND,
            "options": [
                {"value": "silero", "label": "Silero"},
                {"value": "webrtc", "label": "WebRTC (lightweight)"},
                {"value": "auto", "label": "Auto"},
            ],
            "description": "Silero is the best default. WebRTC is much lighter for low-power PCs and Pi-class hosts.",
        },
        {
            "key": "VOICE_WEBRTC_VAD_AGGRESSIVENESS",
            "label": "WebRTC VAD Aggressiveness",
            "type": "number",
            "default": vp.DEFAULT_WEBRTC_VAD_AGGRESSIVENESS,
            "min": 0,
            "max": 3,
            "step": 1,
            "description": "WebRTC only. 0 is least aggressive; 3 filters non-speech most aggressively.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED",
            "label": "Experimental Live Tool Progress Speech",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED,
            "description": "If enabled, Tater can briefly speak Hydra tool-progress updates before the final reply. Updated Tater firmware on VoicePE or Satellite1 can also show the tool-call LED animation during those spoken updates. If disabled, Tater stays in thinking until the final response.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_PARTIAL_STT_ENABLED",
            "label": "Experimental Partial STT",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_ENABLED,
            "description": "If enabled, Tater will try to build live partial transcripts during capture for local and Wyoming STT backends. This can improve turn-end decisions, but may use more CPU/GPU.",
        },
        {
            "key": "VOICE_EXPERIMENTAL_TTS_EARLY_START_ENABLED",
            "label": "Experimental Early-Start TTS",
            "type": "checkbox",
            "default": vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_ENABLED,
            "description": "If enabled, Tater may start speaking long replies sooner by splitting playback into early chunks. This is experimental and may introduce slightly more audible sentence gaps.",
        },
        {
            "key": "VOICE_DISCOVERY_ENABLED",
            "label": "Enable mDNS Discovery",
            "type": "checkbox",
            "default": True,
            "description": "Discover ESPHome satellites via mDNS.",
        },
        {
            "key": "VOICE_DISCOVERY_SCAN_SECONDS",
            "label": "Discovery Scan Interval (sec)",
            "type": "number",
            "default": vp.DEFAULT_DISCOVERY_SCAN_SECONDS,
            "min": 5,
            "max": 600,
        },
        {
            "key": "VOICE_DISCOVERY_MDNS_TIMEOUT_S",
            "label": "mDNS Listen Window (sec)",
            "type": "number",
            "default": vp.DEFAULT_DISCOVERY_MDNS_TIMEOUT_S,
            "min": 0.5,
            "max": 20.0,
            "step": 0.1,
        },
        {
            "key": "VOICE_ESPHOME_API_PORT",
            "label": "ESPHome API Port",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_API_PORT,
            "min": 1,
            "max": 65535,
        },
        {
            "key": "VOICE_ESPHOME_PASSWORD",
            "label": "ESPHome API Password",
            "type": "password",
            "default": "",
        },
        {
            "key": "VOICE_ESPHOME_NOISE_PSK",
            "label": "ESPHome Noise PSK",
            "type": "password",
            "default": "",
        },
        {
            "key": "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
            "label": "ESPHome Connect Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_CONNECT_TIMEOUT_S,
            "min": 2.0,
            "max": 60.0,
            "step": 0.1,
        },
        {
            "key": "VOICE_ESPHOME_RETRY_SECONDS",
            "label": "ESPHome Retry Seconds",
            "type": "number",
            "default": vp.DEFAULT_ESPHOME_RETRY_SECONDS,
            "min": 2,
            "max": 300,
        },
        {
            "key": "VOICE_NATIVE_WYOMING_TIMEOUT_S",
            "label": "Wyoming Timeout (sec)",
            "type": "number",
            "default": vp.DEFAULT_WYOMING_TIMEOUT_SECONDS,
            "min": 5.0,
            "max": 300.0,
            "step": 0.5,
        },
    ]


def _voice_ui_spec_map() -> Dict[str, Dict[str, Any]]:
    vp = _vp()
    out: Dict[str, Dict[str, Any]] = {}
    for spec in voice_ui_setting_specs():
        if not isinstance(spec, dict):
            continue
        key = vp._text(spec.get("key"))
        if not key:
            continue
        out[key] = dict(spec)
    return out


def _voice_ui_field_value(spec: Dict[str, Any], raw_value: Any) -> Any:
    vp = _vp()
    field_type = vp._lower(spec.get("type") or "text")
    default = spec.get("default")
    if field_type == "checkbox":
        return vp._as_bool(raw_value, vp._as_bool(default, False))

    if field_type == "number":
        minimum = spec.get("min") if isinstance(spec.get("min"), (int, float)) else None
        maximum = spec.get("max") if isinstance(spec.get("max"), (int, float)) else None
        default_num = default if isinstance(default, (int, float)) and not isinstance(default, bool) else 0
        step = spec.get("step")
        wants_int = isinstance(default, int) and not isinstance(default, bool)
        if wants_int:
            with contextlib.suppress(Exception):
                if step is not None and not float(step).is_integer():
                    wants_int = False
        if wants_int:
            min_int = int(minimum) if isinstance(minimum, (int, float)) else None
            max_int = int(maximum) if isinstance(maximum, (int, float)) else None
            return vp._as_int(raw_value, int(default_num), minimum=min_int, maximum=max_int)
        return vp._as_float(raw_value, float(default_num), minimum=minimum, maximum=maximum)

    return vp._text(raw_value if raw_value is not None else default)


def settings_fields() -> List[Dict[str, Any]]:
    vp = _vp()
    stored = vp._voice_settings()
    rows: List[Dict[str, Any]] = []
    for spec in voice_ui_setting_specs():
        if not isinstance(spec, dict):
            continue
        key = vp._text(spec.get("key"))
        if not key:
            continue
        row = dict(spec)
        row["key"] = key
        field_type = vp._lower(row.get("type") or "text")
        raw_value = stored.get(key, row.get("default"))

        if field_type in {"select", "multiselect"}:
            row["options"] = list(spec.get("options") or [])

        if field_type == "password":
            has_saved = bool(vp._text(stored.get(key)))
            row["value"] = ""
            if has_saved:
                existing_desc = vp._text(row.get("description"))
                keep_desc = "Leave blank to keep current saved value."
                row["description"] = f"{existing_desc} {keep_desc}".strip() if existing_desc else keep_desc
                row["placeholder"] = "Leave blank to keep current value"
        else:
            row["value"] = _voice_ui_field_value(spec, raw_value)

        rows.append(row)
    return rows


def settings_sections() -> List[Dict[str, Any]]:
    vp = _vp()
    ordered_fields = settings_fields()
    by_key = {vp._text(field.get("key")): field for field in ordered_fields if isinstance(field, dict)}
    groups = [
        ("Debugging", ["VOICE_NATIVE_DEBUG"]),
        ("Conversation Flow", ["VOICE_CONTINUED_CHAT_ENABLED"]),
        ("Voice Activity Detection", ["VOICE_VAD_BACKEND", "VOICE_WEBRTC_VAD_AGGRESSIVENESS"]),
        (
            "Experimental",
            [
                "VOICE_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED",
                "VOICE_EXPERIMENTAL_PARTIAL_STT_ENABLED",
                "VOICE_EXPERIMENTAL_TTS_EARLY_START_ENABLED",
            ],
        ),
        (
            "Satellite Discovery",
            [
                "VOICE_DISCOVERY_ENABLED",
                "VOICE_DISCOVERY_SCAN_SECONDS",
                "VOICE_DISCOVERY_MDNS_TIMEOUT_S",
            ],
        ),
        (
            "ESPHome Connection",
            [
                "VOICE_ESPHOME_API_PORT",
                "VOICE_ESPHOME_PASSWORD",
                "VOICE_ESPHOME_NOISE_PSK",
                "VOICE_ESPHOME_CONNECT_TIMEOUT_S",
                "VOICE_ESPHOME_RETRY_SECONDS",
            ],
        ),
        ("External Voice Services", ["VOICE_NATIVE_WYOMING_TIMEOUT_S"]),
    ]

    sections: List[Dict[str, Any]] = []
    used = set()
    for label, keys in groups:
        fields = []
        for key in keys:
            field = by_key.get(key)
            if not isinstance(field, dict):
                continue
            fields.append(field)
            used.add(key)
        if fields:
            sections.append({"label": label, "fields": fields})

    remaining = [field for field in ordered_fields if vp._text(field.get("key")) not in used]
    if remaining:
        sections.append({"label": "Advanced", "fields": remaining})
    return sections


def settings_item_form() -> Dict[str, Any]:
    return {
        "id": "voice_settings",
        "group": "settings",
        "title": "Voice Pipeline Settings",
        "subtitle": "Tune ESPHome and runtime behavior here. Shared STT/TTS model choices now live in Tater Settings under Models.",
        "sections": list(settings_sections()),
        "save_action": "voice_settings_save",
        "save_label": "Save Settings",
        "settings_title": "Voice Pipeline Settings",
        "fields_dropdown": False,
        "sections_in_dropdown": False,
        "remove_action": "",
    }


def satellite_item_forms(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = esphome_ui_helpers.satellite_item_forms(status)
    return rows if isinstance(rows, list) else []


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    vp = _vp()
    incoming = values if isinstance(values, dict) else {}
    specs = _voice_ui_spec_map()
    current = vp._voice_settings()
    mapping: Dict[str, str] = {}
    changed_keys: List[str] = []

    for key, spec in specs.items():
        if key not in incoming:
            continue
        field_type = vp._lower(spec.get("type") or "text")
        raw_value = incoming.get(key)

        if field_type == "password":
            token = vp._text(raw_value)
            if not token:
                continue
            normalized = token
        elif field_type == "checkbox":
            normalized = "true" if vp._as_bool(raw_value, False) else "false"
        elif field_type == "number":
            coerced = _voice_ui_field_value(spec, raw_value)
            if isinstance(coerced, float) and coerced.is_integer():
                normalized = str(int(coerced))
            else:
                normalized = str(coerced)
        elif field_type == "select":
            normalized = vp._text(raw_value)
            allowed = []
            for option in list(spec.get("options") or []):
                if isinstance(option, dict):
                    allowed.append(vp._text(option.get("value") or option.get("id") or option.get("key")))
                else:
                    allowed.append(vp._text(option))
            allowed = [item for item in allowed if item]
            if allowed and normalized not in allowed:
                normalized = vp._text(current.get(key)) or vp._text(spec.get("default"))
        else:
            normalized = vp._text(raw_value)

        old = vp._text(current.get(key))
        if normalized != old:
            mapping[key] = normalized
            changed_keys.append(key)

    if mapping:
        redis_client.hset(settings_hash_key(), mapping=mapping)

    return {
        "updated_count": len(changed_keys),
        "changed_keys": changed_keys,
        "restart_required": False,
    }
