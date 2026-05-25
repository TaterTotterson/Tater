from __future__ import annotations

import contextlib
import copy
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import parse as urllib_parse, request as urllib_request

import yaml

from helpers import redis_client

from . import runtime as esphome_runtime
from . import ui_helpers as esphome_ui_helpers

FIRMWARE_PROFILE_HASH_KEY = "tater:esphome:firmware:profiles:v1"
FIRMWARE_INSTALLED_VERSION_HASH_KEY = "tater:esphome:firmware:installed_versions:v1"
DISPLAY_PROFILE_HASH_KEY = "tater:display:profiles:v1"
FIRMWARE_AGENT_LABS_ROOT = Path(__file__).resolve().parents[1] / "agent_lab" / "esphome"
FIRMWARE_CONFIG_ROOT = FIRMWARE_AGENT_LABS_ROOT / "firmware_configs"
FIRMWARE_BUILD_ROOT = FIRMWARE_AGENT_LABS_ROOT / "firmware_builds"
FIRMWARE_WEB_FLASH_ROOT = FIRMWARE_AGENT_LABS_ROOT / "web_flash"
FIRMWARE_RUNNER_ROOT = FIRMWARE_AGENT_LABS_ROOT / "runner"
FIRMWARE_PLATFORMIO_ROOT = FIRMWARE_AGENT_LABS_ROOT / "platformio"
FIRMWARE_HOME_ROOT = FIRMWARE_AGENT_LABS_ROOT / "home"
FIRMWARE_CACHE_ROOT = FIRMWARE_AGENT_LABS_ROOT / "cache"
FIRMWARE_BUILD_TIMEOUT_SECONDS = 60 * 60
_CLI_STATUS_CACHE_TTL_SECONDS = 30.0
_CLI_STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "status": {}}
_CLI_STATUS_LOCK = threading.Lock()
_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS = 3.0
_REMOTE_TEMPLATE_CACHE_TTL_SECONDS = 60.0
_REMOTE_TEMPLATE_CACHE: Dict[str, Dict[str, Any]] = {}
_REMOTE_TEMPLATE_LOCK = threading.Lock()
_REMOTE_JSON_CACHE_TTL_SECONDS = 15 * 60.0
_REMOTE_JSON_CACHE: Dict[str, Dict[str, Any]] = {}
_REMOTE_JSON_LOCK = threading.Lock()
_TATER_SENSOR_OPTIONS_CACHE_TTL_SECONDS = 60.0
_TATER_SENSOR_OPTIONS_CACHE: Dict[str, Any] = {"ts": 0.0, "options": []}
_TATER_SENSOR_OPTIONS_LOCK = threading.Lock()
_FIRMWARE_SESSION_MAX_ENTRIES = 4000
_FIRMWARE_SESSION_TTL_SECONDS = 45 * 60.0
_FIRMWARE_DEVICE_LOG_RETRY_SECONDS = 2.5
_FIRMWARE_USB_RECOVERY_SELECTOR = "__usb_recovery__"
_FIRMWARE_SESSIONS: Dict[str, Dict[str, Any]] = {}
_FIRMWARE_SESSION_LOCK = threading.Lock()
_ANSI_ESCAPE_RE = re.compile(r"\x1B(?:\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")
_WAKE_WORD_GITHUB_OWNER = "TaterTotterson"
_WAKE_WORD_GITHUB_REPO = "microWakeWords"
_WAKE_WORD_GITHUB_REF = "main"
_WAKE_WORD_MANIFEST_URLS: tuple[str, ...] = (
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake_word_manifest.json",
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake-word-manifest.json",
)
_WAKE_SOUND_MANIFEST_URLS: tuple[str, ...] = (
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake_sound_manifest.json",
    f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/wake-sound-manifest.json",
)
_WAKE_WORD_SOURCE_SPECS: tuple[Dict[str, str], ...] = (
    {"key": "microWakeWords", "label": "microWakeWords"},
    {"key": "microWakeWordsV2", "label": "microWakeWordsV2"},
    {"key": "microWakeWordsV3", "label": "microWakeWordsV3"},
)
_WAKE_SOUND_SOURCE_SPECS: tuple[Dict[str, str], ...] = (
    {"key": "wakeSounds", "label": "wakeSounds"},
)

_S3BOX_SENSOR_FIELD_LABELS: Dict[str, str] = {
    "sensor_temp_out": "Outdoor Temperature",
    "sensor_temp_in": "Indoor Temperature",
    "sensor_humidity_out": "Outdoor Humidity",
    "sensor_humidity_in": "Indoor Humidity",
    "sensor_wind_speed": "Wind Speed",
    "sensor_rain_rate": "Rain Rate",
    "sensor_lightning_strikes": "Lightning Strikes",
}
_S3BOX_DISPLAY_SLOT_KEYS: Dict[str, str] = {
    "temp_out": "sensor_temp_out",
    "temp_in": "sensor_temp_in",
    "humidity_out": "sensor_humidity_out",
    "humidity_in": "sensor_humidity_in",
    "wind_speed": "sensor_wind_speed",
    "rain_rate": "sensor_rain_rate",
    "lightning_strikes": "sensor_lightning_strikes",
}
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
_ENVIRONMENT_SELECTED_SENSORS_KEY = "environment:selected_sensors"
_ENVIRONMENT_DISPLAY_SENSOR_CATEGORIES = {
    "air",
    "condition",
    "forecast",
    "humidity",
    "lightning",
    "pressure",
    "rain",
    "solar",
    "temperature",
    "wind",
}
_WAKE_SOUND_AUDIO_EXTS = {".flac", ".mp3", ".ogg", ".wav"}
_WAKE_WORD_CATALOG_CACHE_TTL_SECONDS = 10 * 60.0
_WAKE_WORD_CATALOG_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": {}}
_WAKE_WORD_CATALOG_LOCK = threading.Lock()
_TRAINER_WAKE_WORD_CATALOG_CACHE_TTL_SECONDS = 30.0
_TRAINER_WAKE_WORD_CATALOG_CACHE: Dict[str, Dict[str, Any]] = {}
_TRAINER_WAKE_WORD_CATALOG_LOCK = threading.Lock()
_WAKE_SOUND_CATALOG_CACHE_TTL_SECONDS = 10 * 60.0
_WAKE_SOUND_CATALOG_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": {}}
_WAKE_SOUND_CATALOG_LOCK = threading.Lock()
_WAKE_SOUND_DISABLED_PICKER_VALUE = "__none__"
_WAKE_SOUND_ENABLED_PROFILE_KEY = "wake_sound_enabled"
_WAKE_SOUND_DEFAULT_ENABLED = False

_TEMPLATE_SPECS: tuple[Dict[str, Any], ...] = (
    {
        "key": "voicepe",
        "label": "VoicePE",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/voicePE-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "voicePE-TaterTimer.yaml"),
            ("VoicePE-ESPHome", "voicePE-TaterTimer.yaml"),
        ],
        "fixed_keys": {"device_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "voicepe",
            "voice pe",
            "tatervpe",
            "vpe",
        },
    },
    {
        "key": "satellite1",
        "label": "Satellite1",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/satellite1-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "satellite1-TaterTimer.yaml"),
            ("Satellite1-ESPHome", "satellite1-TaterTimer.yaml"),
        ],
        "fixed_keys": {"node_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "satellite1",
            "sat 1",
            "sat1",
            "tatersat1",
            "tater_sat1",
            "tater sat1",
            "core board",
        },
    },
    {
        "key": "respeaker_lite",
        "label": "ReSpeaker Lite",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/respeakerLite-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "respeakerLite-TaterTimer.yaml"),
        ],
        "fixed_keys": {"device_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "respeaker lite",
            "respeaker_lite",
            "respeakerlite",
            "tater-respeaker-lite",
            "tater respeaker lite",
            "tater.respeaker_lite",
        },
    },
    {
        "key": "koala",
        "label": "Koala Satellite",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/koala-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "koala-TaterTimer.yaml"),
        ],
        "fixed_keys": {"device_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "koala",
            "koala satellite",
            "tater-koala",
            "tater koala",
            "tater.koala",
        },
    },
    {
        "key": "respeaker_xvf3800",
        "label": "ReSpeaker XVF3800",
        "source_urls": [
            "https://github.com/TaterTotterson/microWakeWords/raw/refs/heads/main/respeakerXVF3800-TaterTimer.yaml",
        ],
        "candidates": [
            ("microWakeWords", "respeakerXVF3800-TaterTimer.yaml"),
        ],
        "fixed_keys": {"device_name"},
        "auto_keys": {"ha_voice_ip"},
        "match_tokens": {
            "respeaker xvf3800",
            "respeaker_xvf3800",
            "respeakerxvf3800",
            "xvf3800",
            "tater-respeaker-xvf3800",
            "tater respeaker xvf3800",
            "tater.respeaker_xvf3800",
        },
    },
    {
        "key": "s3box_display",
        "label": "Tater ESP32-S3-BOX-3 Display",
        "source_urls": [
            "https://github.com/TaterTotterson/Tater-S3Box-Display/raw/refs/heads/main/esp32-s3-box-3.yaml",
        ],
        "candidates": [
            ("Tater-S3Box-Display", "esp32-s3-box-3.yaml"),
        ],
        "fixed_keys": {"device_name"},
        "auto_keys": {"device_ip"},
        "match_tokens": {
            "s3box",
            "s3 box",
            "s3-box",
            "esp32-s3-box",
            "esp32-s3-box-3",
            "esp32 s3 box",
            "esp32 s3 box 3",
            "box-3",
            "box 3",
            "taters3box",
            "tater-s3box",
            "tater s3box",
            "tater-s3box-display",
        },
    },
)


class _FirmwareYamlLoader(yaml.SafeLoader):
    pass


class _FirmwareYamlDumper(yaml.SafeDumper):
    pass


class _TaggedYamlValue:
    __slots__ = ("tag", "value")

    def __init__(self, tag: str, value: Any) -> None:
        self.tag = _text(tag)
        self.value = value


def _construct_secret(loader: yaml.SafeLoader, node: yaml.Node) -> Dict[str, str]:
    return {"__secret__": loader.construct_scalar(node)}


_FirmwareYamlLoader.add_constructor("!secret", _construct_secret)


def _construct_tagged_yaml(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node) -> _TaggedYamlValue:
    tag = f"!{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    else:
        value = loader.construct_object(node, deep=True)
    return _TaggedYamlValue(tag, value)


def _represent_tagged_yaml(dumper: yaml.SafeDumper, value: _TaggedYamlValue) -> yaml.Node:
    payload = value.value
    if isinstance(payload, dict):
        return dumper.represent_mapping(value.tag, payload)
    if isinstance(payload, list):
        return dumper.represent_sequence(value.tag, payload)
    return dumper.represent_scalar(value.tag, "" if payload is None else str(payload))


_FirmwareYamlLoader.add_multi_constructor("!", _construct_tagged_yaml)
_FirmwareYamlDumper.add_representer(_TaggedYamlValue, _represent_tagged_yaml)


def _text(value: Any) -> str:
    return esphome_runtime.text(value)


def _lower(value: Any) -> str:
    return esphome_runtime.lower(value)


def _as_bool(value: Any, default: bool = False) -> bool:
    return esphome_runtime.as_bool(value, default)


def _as_int(value: Any, default: int = 0, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    return esphome_runtime.as_int(value, default, minimum=minimum, maximum=maximum)


def _repo_siblings_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sanitize_token(value: Any) -> str:
    token = _text(value)
    if not token:
        return "device"
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", token)
    return clean.strip("._-") or "device"


def _humanize_key(key: str) -> str:
    token = _text(key)
    if not token:
        return "Value"
    label = token.replace("_", " ").strip()
    special = {
        "ha": "HA",
        "ip": "IP",
        "id": "ID",
        "ssid": "SSID",
        "wifi": "Wi-Fi",
        "xmos": "XMOS",
        "fw": "FW",
    }
    parts = []
    for raw in label.split():
        lower = raw.lower()
        parts.append(special.get(lower, raw.capitalize()))
    return " ".join(parts) or token


def _firmware_field_label(key: str) -> str:
    if key == "ha_voice_ip":
        return "Satellite IP"
    return _S3BOX_SENSOR_FIELD_LABELS.get(key, _humanize_key(key))


def _current_tater_first_name() -> str:
    try:
        return _text(redis_client.get("tater:first_name")) or "Tater"
    except Exception:
        return "Tater"


def _normalize_http_base_url(value: Any, *, default_scheme: str = "http") -> str:
    token = _text(value)
    if not token:
        return ""
    token = re.sub(r"\s+", "", token)
    lower = token.lower()
    if lower.startswith("http:") and not lower.startswith("http://"):
        token = f"http://{token[5:].lstrip('/')}"
    elif lower.startswith("https:") and not lower.startswith("https://"):
        token = f"https://{token[6:].lstrip('/')}"
    elif "://" not in token:
        scheme = _lower(default_scheme) or "http"
        token = f"{scheme}://{token.lstrip('/')}"
    return token.rstrip("/")


def _integration_source_label(provider: Any) -> str:
    token = _text(provider).strip()
    if not token:
        return "Tater"
    return _INTEGRATION_SOURCE_LABELS.get(token, _humanize_key(token))


def _json_from_redis_key(key: str, default: Any) -> Any:
    try:
        raw = redis_client.get(key)
    except Exception:
        return copy.deepcopy(default)
    if raw in (None, ""):
        return copy.deepcopy(default)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(str(raw))
    except Exception:
        return copy.deepcopy(default)


def _environment_provider_label(provider: Any) -> str:
    return _INTEGRATION_SOURCE_LABELS.get(_lower(provider), _integration_source_label(provider))


def _environment_core_installed() -> bool:
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


def _environment_provider_snapshots() -> Dict[str, Dict[str, Any]]:
    snapshots: Dict[str, Dict[str, Any]] = {}
    for provider, key in _ENVIRONMENT_PROVIDER_LATEST_KEYS.items():
        snapshot = _json_from_redis_key(key, {})
        if isinstance(snapshot, dict) and snapshot:
            snapshots[provider] = snapshot
    latest = _json_from_redis_key("environment:latest", {})
    if isinstance(latest, dict) and latest:
        provider = _lower(latest.get("provider")) or "environment"
        snapshots.setdefault(provider, latest)
    return snapshots


def _environment_combined_readings(provider_snapshots: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    readings: List[Dict[str, Any]] = []
    for provider, snapshot in provider_snapshots.items():
        if not isinstance(snapshot, dict):
            continue
        provider_token = _lower(snapshot.get("provider")) or _lower(provider)
        source_id = _text(snapshot.get("source_id")) or provider_token
        source_name = _text(snapshot.get("model") or snapshot.get("stationtype")) or _environment_provider_label(provider_token)
        for row in snapshot.get("readings") or []:
            if not isinstance(row, dict):
                continue
            key = _text(row.get("key"))
            if not key:
                continue
            next_row = dict(row)
            next_row.setdefault("provider", provider_token)
            next_row.setdefault("provider_label", _environment_provider_label(provider_token))
            next_row.setdefault("source_id", source_id)
            next_row.setdefault("source_name", source_name)
            readings.append(next_row)
    return readings


def _environment_selected_sensor_labels() -> Dict[str, Dict[str, str]]:
    raw = _json_from_redis_key(_ENVIRONMENT_SELECTED_SENSORS_KEY, [])
    rows = raw if isinstance(raw, list) else []
    selected: Dict[str, Dict[str, str]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        key = _text(item.get("key"))
        if not key:
            continue
        selected[key] = {
            "label": _text(item.get("label")),
            "area": _text(item.get("area")),
            "provider": _lower(item.get("provider")),
            "category": _lower(item.get("category")),
        }
    return selected


def _environment_reading_state_id(row: Dict[str, Any]) -> str:
    provider = _lower(row.get("provider")) or "environment"
    source_id = _text(row.get("source_id")) or provider
    key = _text(row.get("key"))
    return f"{provider}:{source_id}:{key}" if key else ""


def _environment_sensor_options_from_core() -> Dict[str, Dict[str, str]]:
    options: Dict[str, Dict[str, str]] = {}
    provider_snapshots = _environment_provider_snapshots()
    selected = _environment_selected_sensor_labels()
    for row in _environment_combined_readings(provider_snapshots):
        category = _lower(row.get("category")) or "other"
        if category not in _ENVIRONMENT_DISPLAY_SENSOR_CATEGORIES:
            continue
        state_id = _environment_reading_state_id(row)
        if not state_id:
            continue
        value = f"environment:{state_id}"
        source_key = _text(row.get("source_id"))
        selection = selected.get(source_key) or selected.get(_text(row.get("key"))) or {}
        label = _text(selection.get("label")) or _text(row.get("label")) or _text(row.get("key")) or state_id
        area = _text(selection.get("area")) or _text(row.get("area"))
        provider_label = _text(row.get("provider_label")) or _environment_provider_label(row.get("provider"))
        display = _text(row.get("display"))
        label_parts = []
        if area:
            label_parts.append(area)
        label_parts.append(label)
        if display:
            label_parts.append(display)
        label_text = " - ".join(part for part in label_parts if part)
        if provider_label:
            label_text = f"{label_text} ({provider_label})"
        options[value] = {
            "value": value,
            "label": label_text,
            "source": "environment",
            "source_label": "Environment Core",
            "category": category,
            "area": area,
        }
    return options


def _environment_sensor_picker_state() -> Dict[str, Any]:
    if not _environment_core_installed():
        return {
            "ready": False,
            "options": [],
            "message": "Install Environment Core to choose display sensors.",
        }
    options = _environment_sensor_options_from_core()
    if not options:
        return {
            "ready": False,
            "options": [],
            "message": "Environment Core is installed, but no readings are available yet. Open Environment Core, run discovery or add a source, then refresh this firmware tab.",
        }
    return {"ready": True, "options": options, "message": ""}


def _tater_sensor_options() -> List[Dict[str, str]]:
    now = time.time()
    with _TATER_SENSOR_OPTIONS_LOCK:
        cached_ts = float(_TATER_SENSOR_OPTIONS_CACHE.get("ts") or 0.0)
        cached_options = _TATER_SENSOR_OPTIONS_CACHE.get("options")
        if isinstance(cached_options, list) and (now - cached_ts) < _TATER_SENSOR_OPTIONS_CACHE_TTL_SECONDS:
            return copy.deepcopy(cached_options)

    picker = _environment_sensor_picker_state()
    options_by_value = picker.get("options") if isinstance(picker.get("options"), dict) else {}

    options = sorted(
        options_by_value.values(),
        key=lambda row: (
            _lower(row.get("source_label")),
            _lower(row.get("area")),
            _lower(row.get("category")),
            _lower(row.get("label")),
            _lower(row.get("value")),
        ),
    )
    with _TATER_SENSOR_OPTIONS_LOCK:
        _TATER_SENSOR_OPTIONS_CACHE["ts"] = now
        _TATER_SENSOR_OPTIONS_CACHE["options"] = copy.deepcopy(options)
    return options


def _tater_sensor_select_state(current_value: Any) -> Dict[str, Any]:
    current = _text(current_value)
    sensor_options = _tater_sensor_options()
    known_values = {_text(row.get("value")) for row in sensor_options if isinstance(row, dict)}
    options: List[Dict[str, Any]] = [{"value": "", "label": "None"}]
    if current and current not in known_values:
        options.append({"value": current, "label": f"{current} (current)"})
    if sensor_options:
        grouped: Dict[str, List[Dict[str, str]]] = {}
        source_labels: Dict[str, str] = {}
        for row in sensor_options:
            if not isinstance(row, dict):
                continue
            source = _text(row.get("source")) or "tater"
            grouped.setdefault(source, []).append(row)
            source_labels[source] = _text(row.get("source_label")) or _integration_source_label(source)
        for source in sorted(grouped, key=lambda item: _lower(source_labels.get(item) or item)):
            options.append({"label": source_labels.get(source) or _integration_source_label(source), "options": grouped[source]})
    picker = _environment_sensor_picker_state()
    return {
        "ready": bool(sensor_options) and bool(picker.get("ready")),
        "options": options,
        "message": _text(picker.get("message")),
    }


def _clean_terminal_text(value: Any) -> str:
    text_value = _text(value)
    if not text_value:
        return ""
    clean = _ANSI_ESCAPE_RE.sub("", text_value).replace("\r", "")
    clean = "".join(ch for ch in clean if ch == "\t" or ord(ch) >= 32)
    return clean.strip()


def _ensure_agent_labs_dirs() -> None:
    for path in (
        FIRMWARE_AGENT_LABS_ROOT,
        FIRMWARE_CONFIG_ROOT,
        FIRMWARE_BUILD_ROOT,
        FIRMWARE_WEB_FLASH_ROOT,
        FIRMWARE_RUNNER_ROOT,
        FIRMWARE_PLATFORMIO_ROOT,
        FIRMWARE_HOME_ROOT,
        FIRMWARE_CACHE_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def _runner_env_overrides() -> Dict[str, str]:
    _ensure_agent_labs_dirs()
    return {
        "HOME": str(FIRMWARE_HOME_ROOT),
        "XDG_CACHE_HOME": str(FIRMWARE_CACHE_ROOT),
        "PLATFORMIO_CORE_DIR": str(FIRMWARE_PLATFORMIO_ROOT),
        "PLATFORMIO_CACHE_DIR": str(FIRMWARE_PLATFORMIO_ROOT / "cache"),
    }


def _active_flash_session_summaries() -> List[str]:
    summaries: List[str] = []
    with _FIRMWARE_SESSION_LOCK:
        for session in list(_FIRMWARE_SESSIONS.values()):
            if not isinstance(session, dict) or not bool(session.get("active")):
                continue
            label = _text(session.get("display_name")) or _text(session.get("selector")) or "firmware session"
            summaries.append(label)
    return summaries


def _clean_firmware_workspace() -> Dict[str, Any]:
    active = _active_flash_session_summaries()
    if active:
        joined = ", ".join(active[:3])
        more = "" if len(active) <= 3 else f" and {len(active) - 3} more"
        raise RuntimeError(f"Stop the active firmware session(s) first: {joined}{more}.")

    removed: List[str] = []
    for path in (FIRMWARE_CONFIG_ROOT, FIRMWARE_BUILD_ROOT, FIRMWARE_RUNNER_ROOT):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            removed.append(path.name)
    _ensure_agent_labs_dirs()
    return {
        "ok": True,
        "removed": removed,
        "message": (
            "Cleaned firmware build files: "
            + (", ".join(removed) if removed else "nothing to remove")
            + "."
        ),
    }


def _template_default_string(raw_value: Any) -> str:
    if isinstance(raw_value, dict) and raw_value.get("__secret__"):
        return ""
    if isinstance(raw_value, bool):
        return "true" if raw_value else "false"
    if raw_value is None:
        return ""
    return _text(raw_value)


def _secret_name(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        return _text(raw_value.get("__secret__"))
    return ""


def _semver_tuple(value: Any) -> tuple[int, int, int]:
    token = _lower(value)
    if not token:
        return (0, 0, 0)
    if token.startswith("v"):
        token = token[1:].strip()
    match = re.match(r"^([0-9]+(\.[0-9]+){0,2})", token)
    core = match.group(1) if match else "0.0.0"
    parts = (core.split(".") + ["0", "0", "0"])[:3]
    try:
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        return (0, 0, 0)


def _known_firmware_version(value: Any) -> str:
    token = _text(value)
    if _lower(token) in {"unknown", "unavailable", "none", "null"}:
        return ""
    return token


def _resolve_template_refs(value: Any, substitutions: Dict[str, Any]) -> str:
    token = _template_default_string(value)
    if not token:
        return ""
    for _idx in range(4):
        previous = token
        for key, raw_value in (substitutions or {}).items():
            key_token = _text(key)
            if key_token:
                token = token.replace("${" + key_token + "}", _template_default_string(raw_value))
        if token == previous:
            break
    return _text(token)


def _template_firmware_metadata(template_doc: Dict[str, Any], substitutions: Dict[str, Any]) -> Dict[str, str]:
    esphome_block = template_doc.get("esphome") if isinstance(template_doc.get("esphome"), dict) else {}
    project_block = esphome_block.get("project") if isinstance(esphome_block.get("project"), dict) else {}
    version = ""
    for candidate in (
        substitutions.get("firmware_version"),
        project_block.get("version"),
        substitutions.get("esp32_fw_version"),
        substitutions.get("version"),
    ):
        resolved = _resolve_template_refs(candidate, substitutions)
        if resolved:
            version = resolved
            break
    project_name = _resolve_template_refs(project_block.get("name"), substitutions)
    return {
        "version": version,
        "project_name": project_name,
    }


def _installed_version_key(selector: Any, template_key: Any) -> str:
    selector_token = _text(selector)
    template_token = _text(template_key)
    if not selector_token or not template_token:
        return ""
    return f"{selector_token}|{template_token}"


def _load_recorded_firmware_version(selector: Any, template_key: Any) -> Dict[str, str]:
    key = _installed_version_key(selector, template_key)
    if not key:
        return {}
    with contextlib.suppress(Exception):
        raw = redis_client.hget(FIRMWARE_INSTALLED_VERSION_HASH_KEY, key)
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return {str(k): _text(v) for k, v in parsed.items() if _text(k)}
    return {}


def _save_recorded_firmware_version(
    selector: Any,
    template_key: Any,
    version: Any,
    *,
    display_name: Any = "",
    source: str = "",
) -> None:
    key = _installed_version_key(selector, template_key)
    version_token = _text(version)
    if not key or not version_token:
        return
    payload = {
        "selector": _text(selector),
        "template_key": _text(template_key),
        "version": version_token,
        "display_name": _text(display_name),
        "source": _text(source),
        "updated_at": str(time.time()),
    }
    redis_client.hset(FIRMWARE_INSTALLED_VERSION_HASH_KEY, key, json.dumps(payload, ensure_ascii=False))


def _firmware_version_snapshot(
    selector: str,
    template_key: str,
    device_info: Dict[str, Any],
    latest_version: str,
    *,
    update_if_missing_installed: bool = False,
) -> Dict[str, Any]:
    device_version = _known_firmware_version(device_info.get("project_version"))
    recorded = _load_recorded_firmware_version(selector, template_key)
    recorded_version = _known_firmware_version(recorded.get("version"))
    installed_version = device_version or recorded_version
    source = "device" if device_version else ("recorded" if recorded_version else "")
    versioned_update = bool(
        latest_version
        and installed_version
        and _semver_tuple(latest_version) > _semver_tuple(installed_version)
    )
    missing_installed_update = bool(latest_version and not installed_version and update_if_missing_installed)
    update_available = versioned_update
    return {
        "latest": latest_version,
        "installed": installed_version,
        "source": source,
        "recorded": recorded_version,
        "device": device_version,
        "update_available": update_available,
        "missing_installed_update": missing_installed_update,
    }


def _remote_json(url: str, *, force_refresh: bool = False) -> Any:
    target = _text(url)
    if not target:
        raise RuntimeError("Remote JSON URL is missing.")

    now = time.time()
    if not force_refresh:
        with _REMOTE_JSON_LOCK:
            cached = _REMOTE_JSON_CACHE.get(target)
            cached_ts = float(cached.get("ts") or 0.0) if isinstance(cached, dict) else 0.0
            if isinstance(cached, dict) and (now - cached_ts) < _REMOTE_JSON_CACHE_TTL_SECONDS:
                if "data" in cached:
                    return copy.deepcopy(cached.get("data"))
                error_value = _text(cached.get("error"))
                if error_value:
                    raise RuntimeError(error_value)

    req = urllib_request.Request(
        target,
        headers={
            "User-Agent": "Tater/1.0",
            "Accept": "application/vnd.github+json, application/json, text/plain, */*",
        },
    )
    try:
        with urllib_request.urlopen(req, timeout=_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = json.loads(response.read().decode(charset, errors="replace"))
    except urllib_request.HTTPError as exc:
        message = f"Failed to fetch remote JSON from {target}: HTTP {int(exc.code or 0)}."
        with _REMOTE_JSON_LOCK:
            _REMOTE_JSON_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc
    except Exception as exc:
        message = f"Failed to fetch remote JSON from {target}: {_text(exc) or exc.__class__.__name__}."
        with _REMOTE_JSON_LOCK:
            _REMOTE_JSON_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc

    with _REMOTE_JSON_LOCK:
        _REMOTE_JSON_CACHE[target] = {"ts": now, "data": copy.deepcopy(payload)}
    return payload


def _wake_word_label_from_slug(slug: str) -> str:
    token = _text(slug).strip()
    if not token:
        return "Wake Word"
    parts = [part for part in re.split(r"[_\-\s]+", token) if part]
    if not parts:
        return token
    return " ".join(part.capitalize() for part in parts)


def _wake_word_source_version_tag(source_key: Any) -> str:
    token = _lower(source_key)
    if token == "microwakewordsv2":
        return "V2"
    if token == "microwakewordsv3":
        return "V3"
    if token == "microwakewords":
        return "V1"
    return ""


def _wake_word_source_display_label(source_key: Any, source_label: Any = "") -> str:
    tag = _wake_word_source_version_tag(source_key)
    label = _text(source_label).strip() or _text(source_key).strip() or "Wake Words"
    if not tag:
        return label
    return f"{tag} - {label}"


def _wake_word_option_label(label: Any, slug: Any, source_key: Any) -> str:
    base = _text(label).strip() or _text(slug).strip() or "Wake Word"
    tag = _wake_word_source_version_tag(source_key)
    if not tag:
        return base
    return f"{base} [{tag}]"


def _wake_word_slug_from_url(url: str) -> str:
    token = _text(url).strip()
    if not token:
        return ""
    name = Path(token.split("?", 1)[0]).name
    if name.lower().endswith(".json"):
        name = name[:-5]
    return _sanitize_token(name).lower()


def _wake_word_source_value(value: Any) -> str:
    token = _lower(value)
    if token in {"prebuilt", "trainer", "custom"}:
        return token
    return ""


def _wake_word_source_from_profile(profile: Dict[str, Any], wake_word_catalog: Dict[str, Any]) -> str:
    explicit = _wake_word_source_value(profile.get("wake_word_source"))
    if explicit:
        return explicit

    current_url = _text(profile.get("wake_word_model_url"))
    if not current_url:
        return "prebuilt"
    if "/api/trained_wake_words/" in current_url:
        return "trainer"

    entries = wake_word_catalog.get("entries") if isinstance(wake_word_catalog.get("entries"), list) else []
    prebuilt_urls = {_text(row.get("url")) for row in entries if isinstance(row, dict)}
    if current_url in prebuilt_urls or f"/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/" in current_url:
        return "prebuilt"
    return "custom"


def _trainer_base_url_from_model_url(value: Any) -> str:
    token = _text(value)
    if not token or "/api/trained_wake_words/" not in token:
        return ""
    try:
        parsed = urllib_parse.urlparse(token)
    except Exception:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _wake_word_trainer_url_from_profile(profile: Dict[str, Any]) -> str:
    explicit = _normalize_http_base_url(profile.get("wake_word_trainer_url"))
    if explicit:
        return explicit
    return _trainer_base_url_from_model_url(profile.get("wake_word_model_url"))


def _trainer_catalog_url(trainer_base_url: Any) -> str:
    base = _normalize_http_base_url(trainer_base_url)
    if not base:
        return ""
    if base.lower().endswith("/api/trained_wake_words/catalog"):
        return base
    return f"{base}/api/trained_wake_words/catalog"


def _trainer_absolute_url(trainer_base_url: str, value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    parsed = urllib_parse.urlparse(token)
    if parsed.scheme and parsed.netloc:
        return token
    base = _normalize_http_base_url(trainer_base_url)
    if not base:
        return token
    if token.startswith("/"):
        return f"{base}{token}"
    return f"{base}/{token}"


def _wake_sound_slug_from_url(url: str) -> str:
    token = _text(url).strip()
    if not token:
        return ""
    name = Path(token.split("?", 1)[0]).name
    suffix = Path(name).suffix
    if suffix:
        name = name[: -len(suffix)]
    return _sanitize_token(name).lower()


def _wake_sound_label_from_slug(slug: str) -> str:
    token = _text(slug).strip()
    if not token:
        return "Wake Sound"
    parts = [part for part in re.split(r"[_\-\.\s]+", token) if part]
    if not parts:
        return token
    rendered: List[str] = []
    for part in parts:
        if len(part) <= 3 and part.isascii():
            rendered.append(part.upper())
        else:
            rendered.append(part.capitalize())
    return " ".join(rendered)


def _wake_word_raw_url(path: str) -> str:
    clean = _text(path).strip().lstrip("/")
    if not clean:
        return ""
    quoted = "/".join(urllib_parse.quote(part) for part in clean.split("/") if part)
    return f"https://raw.githubusercontent.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/{_WAKE_WORD_GITHUB_REF}/{quoted}"


def _wake_word_contents_api_url(path: str) -> str:
    clean = _text(path).strip().lstrip("/")
    quoted = urllib_parse.quote(clean, safe="/")
    return (
        f"https://api.github.com/repos/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}/contents/{quoted}"
        f"?ref={urllib_parse.quote(_WAKE_WORD_GITHUB_REF)}"
    )


def _wake_word_entry(
    *,
    source_key: str,
    source_label: str,
    slug: str,
    url: str,
    label: str = "",
    path: str = "",
) -> Optional[Dict[str, str]]:
    slug_token = _text(slug).strip()
    url_token = _text(url).strip()
    if not slug_token or not url_token:
        return None
    return {
        "id": f"{_text(source_key)}:{slug_token}",
        "slug": slug_token,
        "label": _text(label).strip() or _wake_word_label_from_slug(slug_token),
        "url": url_token,
        "path": _text(path).strip(),
        "source_key": _text(source_key).strip(),
        "source_label": _text(source_label).strip() or _text(source_key).strip(),
    }


def _wake_sound_entry(
    *,
    source_key: str,
    source_label: str,
    slug: str,
    url: str,
    label: str = "",
    path: str = "",
) -> Optional[Dict[str, str]]:
    slug_token = _text(slug).strip()
    url_token = _text(url).strip()
    if not slug_token or not url_token:
        return None
    return {
        "id": f"{_text(source_key)}:{slug_token}",
        "slug": slug_token,
        "label": _text(label).strip() or _wake_sound_label_from_slug(slug_token),
        "url": url_token,
        "path": _text(path).strip(),
        "source_key": _text(source_key).strip(),
        "source_label": _text(source_label).strip() or _text(source_key).strip(),
    }


def _wake_word_entries_from_manifest(payload: Any) -> List[Dict[str, str]]:
    rows: List[Any] = []
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        for key in ("entries", "wake_words", "words", "models", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                rows = list(candidate)
                break
        if not rows:
            for source_key, candidate in payload.items():
                if isinstance(candidate, list):
                    for item in candidate:
                        if isinstance(item, dict):
                            enriched = dict(item)
                            enriched.setdefault("source", source_key)
                            rows.append(enriched)

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_key = _text(row.get("source") or row.get("folder") or row.get("group"))
        source_spec = next((spec for spec in _WAKE_WORD_SOURCE_SPECS if _lower(spec.get("key")) == _lower(source_key)), None)
        source_key = _text((source_spec or {}).get("key")) or source_key or "custom"
        source_label = _text((source_spec or {}).get("label")) or source_key or "Custom"
        url = (
            _text(row.get("url"))
            or _text(row.get("json_url"))
            or _text(row.get("download_url"))
            or _text(row.get("model_url"))
            or _text(row.get("wake_word_model_url"))
        )
        path = _text(row.get("path"))
        if not url and path:
            url = _wake_word_raw_url(path)
        slug = _text(row.get("slug") or row.get("name") or row.get("key")) or _wake_word_slug_from_url(url)
        entry = _wake_word_entry(
            source_key=source_key,
            source_label=source_label,
            slug=slug,
            url=url,
            label=_text(row.get("label") or row.get("title")),
            path=path,
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _wake_sound_entries_from_manifest(payload: Any) -> List[Dict[str, str]]:
    rows: List[Any] = []
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        for key in ("entries", "wake_sounds", "sounds", "audio", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                rows = list(candidate)
                break
        if not rows:
            for source_key, candidate in payload.items():
                if isinstance(candidate, list):
                    for item in candidate:
                        if isinstance(item, dict):
                            enriched = dict(item)
                            enriched.setdefault("source", source_key)
                            rows.append(enriched)

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_key = _text(row.get("source") or row.get("folder") or row.get("group")) or "wakeSounds"
        source_spec = next((spec for spec in _WAKE_SOUND_SOURCE_SPECS if _lower(spec.get("key")) == _lower(source_key)), None)
        source_key = _text((source_spec or {}).get("key")) or source_key
        source_label = _text((source_spec or {}).get("label")) or source_key
        url = (
            _text(row.get("url"))
            or _text(row.get("audio_url"))
            or _text(row.get("sound_url"))
            or _text(row.get("download_url"))
            or _text(row.get("wake_sound_url"))
            or _text(row.get("wake_word_triggered_sound_file"))
        )
        path = _text(row.get("path"))
        if not url and path:
            url = _wake_word_raw_url(path)
        slug = _text(row.get("slug") or row.get("name") or row.get("key")) or _wake_sound_slug_from_url(url)
        entry = _wake_sound_entry(
            source_key=source_key,
            source_label=source_label,
            slug=slug,
            url=url,
            label=_text(row.get("label") or row.get("title")),
            path=path,
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _wake_word_entries_from_source_folder(source_spec: Dict[str, str], *, force_refresh: bool = False) -> List[Dict[str, str]]:
    path = _text(source_spec.get("key"))
    if not path:
        return []
    try:
        payload = _remote_json(_wake_word_contents_api_url(path), force_refresh=force_refresh)
    except RuntimeError as exc:
        if "HTTP 404" in _text(exc):
            return []
        raise

    rows = payload if isinstance(payload, list) else payload.get("entries")
    if not isinstance(rows, list):
        return []

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _lower(row.get("type")) != "file":
            continue
        name = _text(row.get("name")).strip()
        if not name.lower().endswith(".json"):
            continue
        slug = name[:-5]
        entry = _wake_word_entry(
            source_key=path,
            source_label=_text(source_spec.get("label")) or path,
            slug=slug,
            url=_wake_word_raw_url(f"{path}/{name}"),
            path=f"{path}/{name}",
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _wake_sound_entries_from_source_folder(source_spec: Dict[str, str], *, force_refresh: bool = False) -> List[Dict[str, str]]:
    path = _text(source_spec.get("key"))
    if not path:
        return []
    try:
        payload = _remote_json(_wake_word_contents_api_url(path), force_refresh=force_refresh)
    except RuntimeError as exc:
        if "HTTP 404" in _text(exc):
            return []
        raise

    rows = payload if isinstance(payload, list) else payload.get("entries")
    if not isinstance(rows, list):
        return []

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _lower(row.get("type")) != "file":
            continue
        name = _text(row.get("name")).strip()
        if Path(name).suffix.lower() not in _WAKE_SOUND_AUDIO_EXTS:
            continue
        slug = _wake_sound_slug_from_url(name)
        entry = _wake_sound_entry(
            source_key=path,
            source_label=_text(source_spec.get("label")) or path,
            slug=slug,
            url=_wake_word_raw_url(f"{path}/{name}"),
            path=f"{path}/{name}",
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _sorted_wake_word_entries(entries: List[Dict[str, str]]) -> List[Dict[str, str]]:
    source_order = {_text(spec.get("key")): index for index, spec in enumerate(_WAKE_WORD_SOURCE_SPECS)}
    unique: Dict[str, Dict[str, str]] = {}
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = _text(row.get("url"))
        if not url:
            continue
        unique[url] = dict(row)
    return sorted(
        unique.values(),
        key=lambda row: (
            source_order.get(_text(row.get("source_key")), 999),
            _lower(row.get("label")),
            _lower(row.get("slug")),
        ),
    )


def _sorted_wake_sound_entries(entries: List[Dict[str, str]]) -> List[Dict[str, str]]:
    source_order = {_text(spec.get("key")): index for index, spec in enumerate(_WAKE_SOUND_SOURCE_SPECS)}
    unique: Dict[str, Dict[str, str]] = {}
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = _text(row.get("url"))
        if not url:
            continue
        unique[url] = dict(row)
    return sorted(
        unique.values(),
        key=lambda row: (
            source_order.get(_text(row.get("source_key")), 999),
            _lower(row.get("label")),
            _lower(row.get("slug")),
        ),
    )


def _load_wake_word_catalog(*, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force_refresh:
        with _WAKE_WORD_CATALOG_LOCK:
            cached_ts = float(_WAKE_WORD_CATALOG_CACHE.get("ts") or 0.0)
            cached_payload = _WAKE_WORD_CATALOG_CACHE.get("payload")
            if isinstance(cached_payload, dict) and (now - cached_ts) < _WAKE_WORD_CATALOG_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached_payload)

    warnings: List[str] = []

    for manifest_url in _WAKE_WORD_MANIFEST_URLS:
        try:
            manifest_payload = _remote_json(manifest_url, force_refresh=force_refresh)
            entries = _sorted_wake_word_entries(_wake_word_entries_from_manifest(manifest_payload))
            if entries:
                payload = {
                    "entries": entries,
                    "source_kind": "manifest",
                    "source_label": manifest_url,
                    "warning": "",
                }
                with _WAKE_WORD_CATALOG_LOCK:
                    _WAKE_WORD_CATALOG_CACHE["ts"] = now
                    _WAKE_WORD_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
                return payload
        except RuntimeError as exc:
            if "HTTP 404" not in _text(exc):
                warnings.append(_text(exc))

    entries: List[Dict[str, str]] = []
    for source_spec in _WAKE_WORD_SOURCE_SPECS:
        try:
            entries.extend(_wake_word_entries_from_source_folder(source_spec, force_refresh=force_refresh))
        except RuntimeError as exc:
            warnings.append(_text(exc))

    payload = {
        "entries": _sorted_wake_word_entries(entries),
        "source_kind": "repo_contents",
        "source_label": _text(_WAKE_WORD_GITHUB_REPO),
        "warning": _text(warnings[0] if warnings else ""),
    }
    with _WAKE_WORD_CATALOG_LOCK:
        _WAKE_WORD_CATALOG_CACHE["ts"] = now
        _WAKE_WORD_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
    return payload


def _trainer_wake_word_entries_from_payload(payload: Any, trainer_base_url: str) -> List[Dict[str, str]]:
    rows: List[Any] = []
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        for key in ("wake_words", "entries", "models", "items", "words"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                rows = list(candidate)
                break

    entries: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        url = (
            _text(row.get("json_url"))
            or _text(row.get("url"))
            or _text(row.get("wake_word_model_url"))
            or _text(row.get("download_url"))
            or _text(row.get("model_json_url"))
            or _text(row.get("model_url"))
        )
        url = _trainer_absolute_url(trainer_base_url, url)
        slug = (
            _text(row.get("wake_word_name"))
            or _text(row.get("slug"))
            or _text(row.get("key"))
            or _wake_word_slug_from_url(url)
        )
        label = _text(row.get("label") or row.get("wake_word") or row.get("title"))
        entry = _wake_word_entry(
            source_key="trainer",
            source_label="Trainer",
            slug=slug,
            url=url,
            label=label,
            path=_text(row.get("json_file") or row.get("path")),
        )
        if isinstance(entry, dict):
            entries.append(entry)
    return sorted(entries, key=lambda row: (_lower(row.get("label")), _text(row.get("url"))))


def _load_trainer_wake_word_catalog(
    trainer_base_url: Any,
    *,
    force_refresh: bool = False,
    strict: bool = False,
) -> Dict[str, Any]:
    base = _normalize_http_base_url(trainer_base_url)
    catalog_url = _trainer_catalog_url(base)
    if not catalog_url:
        if strict:
            raise RuntimeError("Trainer URL is required.")
        return {"entries": [], "source_kind": "trainer", "source_label": "", "warning": "Enter a trainer URL."}

    now = time.time()
    if not force_refresh:
        with _TRAINER_WAKE_WORD_CATALOG_LOCK:
            cached = _TRAINER_WAKE_WORD_CATALOG_CACHE.get(catalog_url)
            cached_ts = float(cached.get("ts") or 0.0) if isinstance(cached, dict) else 0.0
            if isinstance(cached, dict) and (now - cached_ts) < _TRAINER_WAKE_WORD_CATALOG_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached.get("payload") or {})

    try:
        payload = _remote_json(catalog_url, force_refresh=True)
        catalog = {
            "entries": _trainer_wake_word_entries_from_payload(payload, base),
            "source_kind": "trainer",
            "source_label": base,
            "warning": "",
        }
    except RuntimeError as exc:
        if strict:
            raise
        catalog = {
            "entries": [],
            "source_kind": "trainer",
            "source_label": base,
            "warning": _text(exc),
        }

    with _TRAINER_WAKE_WORD_CATALOG_LOCK:
        _TRAINER_WAKE_WORD_CATALOG_CACHE[catalog_url] = {"ts": now, "payload": copy.deepcopy(catalog)}
    return catalog


def _load_wake_sound_catalog(*, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    if not force_refresh:
        with _WAKE_SOUND_CATALOG_LOCK:
            cached_ts = float(_WAKE_SOUND_CATALOG_CACHE.get("ts") or 0.0)
            cached_payload = _WAKE_SOUND_CATALOG_CACHE.get("payload")
            if isinstance(cached_payload, dict) and (now - cached_ts) < _WAKE_SOUND_CATALOG_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached_payload)

    warnings: List[str] = []

    for manifest_url in _WAKE_SOUND_MANIFEST_URLS:
        try:
            manifest_payload = _remote_json(manifest_url, force_refresh=force_refresh)
            entries = _sorted_wake_sound_entries(_wake_sound_entries_from_manifest(manifest_payload))
            if entries:
                payload = {
                    "entries": entries,
                    "source_kind": "manifest",
                    "source_label": manifest_url,
                    "warning": "",
                }
                with _WAKE_SOUND_CATALOG_LOCK:
                    _WAKE_SOUND_CATALOG_CACHE["ts"] = now
                    _WAKE_SOUND_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
                return payload
        except RuntimeError as exc:
            if "HTTP 404" not in _text(exc):
                warnings.append(_text(exc))

    entries: List[Dict[str, str]] = []
    for source_spec in _WAKE_SOUND_SOURCE_SPECS:
        try:
            entries.extend(_wake_sound_entries_from_source_folder(source_spec, force_refresh=force_refresh))
        except RuntimeError as exc:
            warnings.append(_text(exc))

    payload = {
        "entries": _sorted_wake_sound_entries(entries),
        "source_kind": "repo_contents",
        "source_label": _text(_WAKE_WORD_GITHUB_REPO),
        "warning": _text(warnings[0] if warnings else ""),
    }
    with _WAKE_SOUND_CATALOG_LOCK:
        _WAKE_SOUND_CATALOG_CACHE["ts"] = now
        _WAKE_SOUND_CATALOG_CACHE["payload"] = copy.deepcopy(payload)
    return payload


def _wake_word_picker_options(
    catalog: Dict[str, Any],
    *,
    include_custom: bool = True,
    blank_label: str = "Custom URL",
) -> List[Dict[str, Any]]:
    entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
    rows: List[Dict[str, str]] = []
    for row in entries:
        if not isinstance(row, dict):
            continue
        source_key = _text(row.get("source_key"))
        url = _text(row.get("url"))
        if not url:
            continue
        rows.append(
            {
                "value": url,
                "label": _wake_word_option_label(row.get("label"), row.get("slug"), source_key),
            }
        )
    rows.sort(key=lambda option: (_lower(option.get("label")), _text(option.get("value"))))
    if include_custom:
        return [{"value": "__custom__", "label": blank_label or "Custom URL"}, *rows]
    return [{"value": "", "label": blank_label or "Choose wake word"}, *rows]


def _trainer_wake_word_picker_options(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
    options = _wake_word_picker_options(catalog, include_custom=False, blank_label="Choose trained wake word")
    if len(options) > 1:
        return options
    warning = _text(catalog.get("warning"))
    label = "Trainer unavailable" if warning else "No trained wake words found"
    return [{"value": "", "label": label}]


def _wake_sound_picker_options(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
    rows: List[Dict[str, str]] = []
    for row in entries:
        if not isinstance(row, dict):
            continue
        url = _text(row.get("url"))
        if not url:
            continue
        rows.append(
            {
                "value": url,
                "label": _text(row.get("label")) or _wake_sound_label_from_slug(_text(row.get("slug"))),
            }
        )
    rows.sort(key=lambda option: (_lower(option.get("label")), _text(option.get("value"))))
    return [
        {"value": _WAKE_SOUND_DISABLED_PICKER_VALUE, "label": "No wake sound"},
        {"value": "__custom__", "label": "Custom URL"},
        *rows,
    ]


def _extract_substitution_sections(raw_text: str) -> Dict[str, str]:
    section_map: Dict[str, str] = {}
    in_substitutions = False
    current_section = "Firmware"

    for line in raw_text.splitlines():
        if not in_substitutions:
            if re.match(r"^\s*substitutions:\s*$", line):
                in_substitutions = True
            continue

        if line and not line.startswith((" ", "\t")):
            break

        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            comment = stripped[1:].strip()
            if not comment or set(comment) <= {"-"}:
                continue
            if len(comment) <= 40 and re.search(r"[A-Za-z]", comment):
                current_section = comment.title() if comment.isupper() else comment
            continue

        match = re.match(r"^([A-Za-z0-9_]+)\s*:", stripped)
        if match:
            section_map[match.group(1)] = current_section

    return section_map


def _resolve_template_source(spec: Dict[str, Any], *, force_remote_refresh: bool = False) -> Optional[Dict[str, Any]]:
    last_error = ""
    for url in list(spec.get("source_urls") or []):
        try:
            return {
                "repo_root": None,
                "template_path": None,
                "raw_text": _remote_template_text(_text(url), force_refresh=force_remote_refresh),
                "source_kind": "remote",
                "source_label": _text(url),
            }
        except Exception as exc:
            last_error = _text(exc) or f"Failed to load template from {_text(url)}."
    if last_error:
        raise RuntimeError(last_error)
    return None


def _template_spec_by_key(template_key: str) -> Optional[Dict[str, Any]]:
    token = _lower(template_key)
    for spec in _TEMPLATE_SPECS:
        if _lower(spec.get("key")) == token:
            return dict(spec)
    return None


def _is_usb_recovery_selector(selector: Any) -> bool:
    return _text(selector) == _FIRMWARE_USB_RECOVERY_SELECTOR


def _usb_recovery_client_row(template_spec: Dict[str, Any]) -> Dict[str, Any]:
    template_label = _text(template_spec.get("label")) or _text(template_spec.get("key")) or "Firmware"
    return {
        "selector": _FIRMWARE_USB_RECOVERY_SELECTOR,
        "host": "",
        "port": 0,
        "connected": False,
        "selected": True,
        "firmware_usb_recovery": True,
        "source": "usb_recovery",
        "device_info": {
            "name": _lower(template_spec.get("key")) or "usb_recovery",
            "friendly_name": f"{template_label} Browser USB Recovery",
            "model": "USB Serial",
            "project_name": _text(template_spec.get("label")),
        },
    }


def _firmware_action_client_row(selector: str, template_spec: Dict[str, Any]) -> Dict[str, Any]:
    selector_token = _text(selector)
    if _is_usb_recovery_selector(selector_token):
        return _usb_recovery_client_row(template_spec)

    client_row = esphome_runtime.client_row_snapshot_sync(selector_token)
    if not isinstance(client_row, dict):
        client_row = {}
    else:
        client_row = dict(client_row)

    registry_row = esphome_runtime.satellite_lookup(selector_token)
    registry_meta = registry_row.get("metadata") if isinstance(registry_row.get("metadata"), dict) else {}
    registry_selected = bool(registry_meta.get("esphome_selected"))

    if not client_row and isinstance(registry_row, dict) and registry_row:
        client_row = {
            "selector": selector_token,
            "host": _text(registry_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token),
            "port": _as_int(registry_meta.get("esphome_port") or registry_row.get("port"), 0),
            "connected": False,
            "selected": registry_selected,
            "source": _text(registry_row.get("source")) or "satellite_registry",
            "device_info": {},
        }

    if client_row:
        client_row.setdefault("selector", selector_token)
        if not _text(client_row.get("host")):
            client_row["host"] = _text(registry_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token)
        client_row["selected"] = bool(client_row.get("selected")) or registry_selected
        device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
        if not device_info and isinstance(registry_row, dict):
            display_name = _text(registry_row.get("name")) or selector_token
            client_row["device_info"] = {
                "name": display_name,
                "friendly_name": display_name,
            }
    return client_row


def _load_template_context(spec: Dict[str, Any], *, force_remote_refresh: bool = False) -> Dict[str, Any]:
    resolved = _resolve_template_source(spec, force_remote_refresh=force_remote_refresh)
    if not isinstance(resolved, dict):
        raise RuntimeError(f"Firmware template for {spec.get('label') or spec.get('key')} is unavailable.")

    template_path = Path(resolved["template_path"]) if resolved.get("template_path") else None
    raw_text = _text(resolved.get("raw_text"))
    if not raw_text:
        if not isinstance(template_path, Path):
            raise RuntimeError(f"Firmware template for {spec.get('label') or spec.get('key')} is unavailable.")
        raw_text = template_path.read_text(encoding="utf-8")
    parsed = yaml.load(raw_text, Loader=_FirmwareYamlLoader)
    if not isinstance(parsed, dict):
        template_name = template_path.name if isinstance(template_path, Path) else _text(resolved.get("source_label")) or "template"
        raise RuntimeError(f"Firmware template {template_name} did not parse into a YAML mapping.")

    substitutions = parsed.get("substitutions") if isinstance(parsed.get("substitutions"), dict) else {}
    sections = _extract_substitution_sections(raw_text)
    firmware_meta = _template_firmware_metadata(parsed, substitutions)
    return {
        "spec": dict(spec),
        "repo_root": Path(resolved["repo_root"]) if resolved.get("repo_root") else None,
        "template_path": template_path,
        "template_doc": parsed,
        "substitutions": dict(substitutions),
        "firmware_version": _text(firmware_meta.get("version")),
        "firmware_project": _text(firmware_meta.get("project_name")),
        "sections": sections,
        "source_kind": _text(resolved.get("source_kind")),
        "source_label": _text(resolved.get("source_label")),
    }


def _profile_storage_key(template_key: str, selector: str = "") -> str:
    token = _lower(template_key)
    if not token:
        return ""
    selector_token = _lower(selector)
    if selector_token:
        return f"template:{token}:target:{selector_token}"
    return f"template:{token}"


def _profile_load(template_key: str, selector: str = "") -> Dict[str, str]:
    tokens = [_profile_storage_key(template_key, selector), _profile_storage_key(template_key)]
    legacy_selector = _text(selector)
    if legacy_selector:
        tokens.append(legacy_selector)

    seen_tokens: set[str] = set()
    for token in [item for item in tokens if _text(item)]:
        if token in seen_tokens:
            continue
        seen_tokens.add(token)
        with contextlib.suppress(Exception):
            raw = redis_client.hget(FIRMWARE_PROFILE_HASH_KEY, token)
            if raw:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return {str(key): _text(value) for key, value in parsed.items() if _text(key)}
    return {}


def _profile_save(template_key: str, selector: str, values: Dict[str, Any]) -> None:
    token = _profile_storage_key(template_key, selector)
    if not token:
        return
    clean = {str(key): _text(value) for key, value in (values or {}).items() if _text(key)}
    redis_client.hset(FIRMWARE_PROFILE_HASH_KEY, token, json.dumps(clean, ensure_ascii=False))
    _display_profile_save(template_key, selector, clean)


def _display_profile_save(template_key: str, selector: str, values: Dict[str, str]) -> None:
    if _lower(template_key) != "s3box_display":
        return
    target = _text(values.get("display_target")) or _text(values.get("selector")) or _text(values.get("device_name"))
    if not target:
        return
    slots = {
        alias: _text(values.get(key))
        for alias, key in _S3BOX_DISPLAY_SLOT_KEYS.items()
        if _text(values.get(key))
    }
    payload = {
        "target": target,
        "template": "s3box_display",
        "selector": _text(selector),
        "profile_key": _profile_storage_key(template_key, selector),
        "updated_at": time.time(),
        "slots": slots,
    }
    redis_client.hset(DISPLAY_PROFILE_HASH_KEY, target, json.dumps(payload, ensure_ascii=False))


def _match_template_spec(selector: str, client_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    haystack = " ".join(
        part
        for part in [
            selector,
            client_row.get("selector"),
            client_row.get("host"),
            client_row.get("source"),
            device_info.get("name"),
            device_info.get("friendly_name"),
            device_info.get("manufacturer"),
            device_info.get("model"),
            device_info.get("project_name"),
        ]
        if _text(part)
    ).lower()

    for spec in _TEMPLATE_SPECS:
        tokens = {_lower(token) for token in set(spec.get("match_tokens") or set()) if _text(token)}
        if any(token and token in haystack for token in tokens):
            return dict(spec)
    return None


def _matched_template_key(selector: str, client_row: Dict[str, Any]) -> str:
    matched = _match_template_spec(selector, client_row)
    return _text(matched.get("key")) if isinstance(matched, dict) else ""


def _checkbox_like_key(key: str, raw_value: Any) -> bool:
    token = _lower(key)
    if token in {"hidden_ssid"}:
        return True
    raw_text = _lower(_template_default_string(raw_value))
    return raw_text in {"true", "false"}


def _build_device_context(
    selector: str,
    client_row: Dict[str, Any],
    template_spec: Dict[str, Any],
    *,
    force_remote_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    if not isinstance(client_row, dict):
        return None

    if not isinstance(template_spec, dict):
        return None

    connected = bool(client_row.get("connected"))
    selected = bool(client_row.get("selected"))
    usb_recovery = bool(client_row.get("firmware_usb_recovery")) or _is_usb_recovery_selector(selector)
    if not (connected or selected or usb_recovery):
        return None

    template_ctx = _load_template_context(template_spec, force_remote_refresh=force_remote_refresh)
    substitutions = template_ctx["substitutions"]
    field_order = [key for key in substitutions.keys() if _text(key)]
    if not field_order:
        return None

    selector_token = _text(selector)
    host = _text(client_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token)
    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    template_key = _text(template_spec.get("key"))
    latest_firmware_version = _text(template_ctx.get("firmware_version"))
    firmware_project = _text(template_ctx.get("firmware_project"))
    matched_template_key = _matched_template_key(selector_token, client_row)
    update_if_missing_installed = bool(
        connected
        and not usb_recovery
        and matched_template_key
        and _lower(matched_template_key) == _lower(template_key)
    )
    version_snapshot = _firmware_version_snapshot(
        selector_token,
        template_key,
        device_info,
        latest_firmware_version,
        update_if_missing_installed=update_if_missing_installed,
    )
    profile = _profile_load(template_key, selector_token)
    fixed_keys = set(template_spec.get("fixed_keys") or set())
    auto_keys = set(template_spec.get("auto_keys") or set())
    wake_word_catalog = _load_wake_word_catalog()
    wake_word_source = _wake_word_source_from_profile(profile, wake_word_catalog)
    wake_word_trainer_url = _wake_word_trainer_url_from_profile(profile)
    trainer_wake_word_catalog = (
        _load_trainer_wake_word_catalog(wake_word_trainer_url)
        if wake_word_source == "trainer" and wake_word_trainer_url
        else {"entries": [], "source_kind": "trainer", "source_label": wake_word_trainer_url, "warning": ""}
    )
    wake_sound_catalog = _load_wake_sound_catalog()

    if usb_recovery:
        display_name = f"{_text(template_spec.get('label')) or 'Firmware'} Browser USB Recovery"
    else:
        display_name = (
            _text(device_info.get("friendly_name"))
            or _text(device_info.get("name"))
            or _text(client_row.get("selector"))
            or selector_token
        )

    sections_ui: List[Dict[str, Any]] = []
    fields_meta: Dict[str, Dict[str, Any]] = {}
    section_lookup: Dict[str, List[Dict[str, Any]]] = {}

    for key in field_order:
        raw_value = substitutions.get(key)
        template_default = _template_default_string(raw_value)
        secret_hint = _secret_name(raw_value)
        saved_value = _text(profile.get(key))
        version_key = key in {"firmware_version", "esp32_fw_version"}
        section_title = _text(template_ctx["sections"].get(key)) or "Firmware"
        if key in {
            "wake_engine",
            "wake_word_name",
            "wake_word_model_url",
            "wake_model_stop_url",
            "openwakeword_server_url",
            "nanowakeword_server_url",
            "openwakeword_http_timeout_ms",
            "openwakeword_max_failures",
        } or key.startswith("wake_cutoff_"):
            section_title = "Wake Word"
        if key == "wake_word_triggered_sound_file":
            section_title = "Wake Sound"
        if section_title not in section_lookup:
            fields: List[Dict[str, Any]] = []
            section_lookup[section_title] = fields
            sections_ui.append({"title": section_title, "fields": fields})
        fields = section_lookup[section_title]

        resolved_value = saved_value or template_default
        if version_key:
            resolved_value = template_default
        if key == "friendly_name":
            resolved_value = saved_value or _text(device_info.get("friendly_name")) or display_name or template_default
        if key == "tater_first_name":
            resolved_value = saved_value or _current_tater_first_name() or template_default
        if key == "tater_base_url":
            resolved_value = _normalize_http_base_url(resolved_value)
        if key == "openwakeword_server_url":
            resolved_value = _normalize_http_base_url(resolved_value, default_scheme="ws")
        if key == "nanowakeword_server_url":
            resolved_value = _normalize_http_base_url(resolved_value, default_scheme="ws")
        if key in auto_keys and host:
            resolved_value = host
        if key in fixed_keys:
            resolved_value = template_default or resolved_value

        field_type = "checkbox" if _checkbox_like_key(key, raw_value) else "text"
        field_value: Any = resolved_value
        field_options: Optional[List[Dict[str, Any]]] = None
        field_disabled = False
        field_min: Optional[Any] = None
        field_max: Optional[Any] = None
        field_step: Optional[Any] = None
        description_parts: List[str] = []
        placeholder = ""
        read_only = key in fixed_keys or key in auto_keys or version_key

        if field_type == "checkbox":
            field_value = _as_bool(resolved_value, _as_bool(template_default, False))
        elif key == "wifi_password":
            field_type = "password"
            field_value = ""
            placeholder = "Leave blank to keep saved Wi-Fi password" if saved_value else "Enter Wi-Fi password"
            if saved_value:
                description_parts.append("Leave blank to keep the saved Wi-Fi password in Tater.")
            else:
                description_parts.append("Required before build or flash.")
        elif key == "wifi_ssid" and secret_hint:
            placeholder = secret_hint
            if not saved_value:
                description_parts.append("Required before build or flash.")
        elif key == "wake_word_name":
            placeholder = placeholder or "hey_tater"
            description_parts.append("Auto-filled when you choose a prebuilt or trainer wake word, but you can still edit it.")
        elif key == "wake_engine":
            field_type = "select"
            engine_value = _lower(resolved_value) or "microwakeword"
            if engine_value not in {"microwakeword", "openwakeword", "nanowakeword"}:
                engine_value = "microwakeword"
            field_value = engine_value
            field_options = [
                {"value": "microwakeword", "label": "microWakeWord (device)"},
                {"value": "openwakeword", "label": "openWakeWord (remote URL)"},
                {"value": "nanowakeword", "label": "NanoWakeWord (remote URL)"},
            ]
            description_parts.append("Choose whether the satellite listens locally or streams wake-word audio to a configured remote wake URL.")
        elif key == "openwakeword_server_url":
            placeholder = placeholder or "ws://tater.local:8501"
            description_parts.append("Base openWakeWord URL for the streaming detector. http:// and https:// values are accepted, but firmware converts them to ws:// or wss:// and streams audio only. If this endpoint fails, the device falls back to microWakeWord.")
        elif key == "nanowakeword_server_url":
            placeholder = placeholder or "ws://tater.local:8501"
            description_parts.append("Base NanoWakeWord URL for the streaming detector. http:// and https:// values are accepted, but firmware converts them to ws:// or wss:// and streams audio only. If this endpoint fails, the device falls back to microWakeWord.")
        elif key == "openwakeword_http_timeout_ms":
            field_type = "number"
            label = "Remote Wake Transport Timeout"
            field_value = _as_int(resolved_value, 3000, minimum=250, maximum=10000)
            field_min = 250
            field_max = 10000
            field_step = 50
            description_parts.append("Remote openWakeWord transport timeout before the device counts a failed request.")
        elif key == "openwakeword_max_failures":
            field_type = "number"
            label = "Remote Wake Max Failures"
            field_value = _as_int(resolved_value, 3, minimum=1, maximum=20)
            field_min = 1
            field_max = 20
            field_step = 1
            description_parts.append("Consecutive remote openWakeWord request failures before the device falls back to microWakeWord.")
        elif key == "wake_word_model_url":
            description_parts.append("Used when microWakeWord Model Source is Custom URL.")
        elif key == "wake_word_triggered_sound_file":
            description_parts.append("Pick a prebuilt wake sound above or paste any custom audio URL.")
        elif key == "tater_base_url":
            placeholder = placeholder or "http://tater.local:8501"
            description_parts.append("Base URL for the Tater display feed and event API.")
        elif key == "tater_token":
            field_type = "password"
            description_parts.append("Use the same token as Tater's ESPHome voice/display API, if auth is enabled.")
        elif key == "tater_first_name":
            placeholder = placeholder or _current_tater_first_name()
            description_parts.append("Defaults to Tater First Name from Settings and controls the display header label.")
        elif key == "display_target":
            description_parts.append("Display target name used for Tater events, for example livingroom or kitchen.")
        elif key == "timezone":
            placeholder = placeholder or "America/Chicago"
            description_parts.append("IANA timezone used by the display clock.")
        elif key == "device_ip":
            placeholder = placeholder or host or "192.168.1.50"
            description_parts.append("OTA target address for this ESPHome display.")
        elif version_key:
            description_parts.append("Managed by the firmware template and used for update checks.")
        elif key in _S3BOX_SENSOR_FIELD_LABELS:
            field_type = "select"
            sensor_select = _tater_sensor_select_state(resolved_value)
            field_options = sensor_select.get("options") if isinstance(sensor_select.get("options"), list) else []
            sensor_ready = bool(sensor_select.get("ready"))
            field_disabled = not sensor_ready
            if sensor_ready:
                description_parts.append("Choose an Environment Core reading to show in this display slot.")
            else:
                description_parts.append(_text(sensor_select.get("message")) or "Install Environment Core to choose display sensors.")

        if key in fixed_keys:
            description_parts.append("Locked to the firmware template for this device family.")
        elif key in auto_keys:
            description_parts.append("Auto-filled from the currently connected satellite IP.")
        elif secret_hint and key not in {"wifi_password", "wifi_ssid"}:
            placeholder = placeholder or secret_hint

        effective_read_only = read_only or field_disabled
        field_row = {
            "key": key,
            "label": _firmware_field_label(key),
            "type": field_type,
            "value": field_value,
            "read_only": effective_read_only,
        }
        if isinstance(field_options, list):
            field_row["options"] = field_options
        if field_min is not None:
            field_row["min"] = field_min
        if field_max is not None:
            field_row["max"] = field_max
        if field_step is not None:
            field_row["step"] = field_step
        if key == "wake_word_model_url":
            field_row["show_when"] = {"source_key": "wake_word_source", "equals": "custom"}
        if key == "wake_word_triggered_sound_file":
            field_row["disable_when"] = {"source_key": "wake_sound_catalog", "equals": _WAKE_SOUND_DISABLED_PICKER_VALUE}
            field_row["disabled_note"] = "Wake sound is disabled for this build."
        if field_disabled:
            field_row["disabled"] = True
        if placeholder and not effective_read_only:
            field_row["placeholder"] = placeholder
        if description_parts:
            field_row["description"] = " ".join(part for part in description_parts if part)
        fields.append(field_row)

        fields_meta[key] = {
            "type": field_type,
            "template_default": template_default,
            "secret_hint": secret_hint,
            "read_only": effective_read_only,
            "resolved_value": resolved_value,
            "required": key in {"wifi_ssid", "wifi_password"},
        }

    wake_word_section = section_lookup.get("Wake Word") if isinstance(section_lookup.get("Wake Word"), list) else None
    if isinstance(wake_word_section, list) and "wake_word_model_url" in fields_meta:
        wake_word_entries = wake_word_catalog.get("entries") if isinstance(wake_word_catalog.get("entries"), list) else []
        trainer_wake_word_entries = (
            trainer_wake_word_catalog.get("entries")
            if isinstance(trainer_wake_word_catalog.get("entries"), list)
            else []
        )
        current_wake_word_url = _text(
            (
                fields_meta.get("wake_word_model_url", {}).get("resolved_value")
                if isinstance(fields_meta.get("wake_word_model_url"), dict)
                else ""
            )
        )
        available_urls = {_text(row.get("url")) for row in wake_word_entries if isinstance(row, dict)}
        trainer_available_urls = {_text(row.get("url")) for row in trainer_wake_word_entries if isinstance(row, dict)}
        picker_value = current_wake_word_url if current_wake_word_url in available_urls else ""
        trainer_picker_value = current_wake_word_url if current_wake_word_url in trainer_available_urls else ""
        catalog_description = (
            f"Choose from {len(wake_word_entries)} prebuilt microWakeWord models. If you need a new shared wake word, request it from the "
            "microWakeWords repo link below and this list will update after it is added."
            if wake_word_entries
            else "Prebuilt microWakeWord catalog is unavailable right now. "
            "If you need a new wake word, request it from the microWakeWords repo link below and this list will update after it is added."
        )
        catalog_warning = _text(wake_word_catalog.get("warning"))
        if catalog_warning and not wake_word_entries:
            catalog_description = f"{catalog_description} {_text(catalog_warning)}".strip()
        trainer_description = (
            f"Loaded {len(trainer_wake_word_entries)} trained microWakeWord models from the trainer app."
            if trainer_wake_word_entries
            else "Enter a trainer URL; Tater loads this list when the URL changes or when the tab refreshes."
        )
        trainer_warning = _text(trainer_wake_word_catalog.get("warning"))
        if trainer_warning and not trainer_wake_word_entries:
            trainer_description = f"{trainer_description} {trainer_warning}".strip()
        micro_wakeword_picker_fields = [
            {
                "key": "wake_word_source",
                "label": "microWakeWord Model Source",
                "type": "select",
                "value": wake_word_source,
                "options": [
                    {"value": "prebuilt", "label": "Prebuilt"},
                    {"value": "trainer", "label": "Trainer App"},
                    {"value": "custom", "label": "Custom URL"},
                ],
                "description": "Choose the local microWakeWord model flashed onto the device. openWakeWord uses the separate Wake Engine and openWakeWord URL settings.",
            },
            {
                "key": "wake_word_catalog",
                "label": "Prebuilt microWakeWord",
                "type": "select",
                "value": picker_value,
                "options": _wake_word_picker_options(
                    wake_word_catalog,
                    include_custom=False,
                    blank_label="Choose prebuilt microWakeWord",
                ),
                "description": catalog_description,
                "show_when": {"source_key": "wake_word_source", "equals": "prebuilt"},
            },
            {
                "key": "wake_word_trainer_url",
                "label": "microWakeWord Trainer URL",
                "type": "text",
                "value": wake_word_trainer_url,
                "placeholder": "http://trainer.local:8789",
                "description": "Tater will read /api/trained_wake_words/catalog from this microWakeWord trainer app.",
                "show_when": {"source_key": "wake_word_source", "equals": "trainer"},
            },
            {
                "key": "wake_word_trainer_catalog",
                "label": "Trainer microWakeWord",
                "type": "select",
                "value": trainer_picker_value,
                "options": _trainer_wake_word_picker_options(trainer_wake_word_catalog),
                "description": trainer_description,
                "show_when": {"source_key": "wake_word_source", "equals": "trainer"},
            },
        ]
        wake_engine_index = next(
            (
                idx
                for idx, field in enumerate(wake_word_section)
                if isinstance(field, dict) and _text(field.get("key")) == "wake_engine"
            ),
            -1,
        )
        insert_at = wake_engine_index + 1 if wake_engine_index >= 0 else 0
        wake_word_section[insert_at:insert_at] = micro_wakeword_picker_fields

    wake_sound_section = section_lookup.get("Wake Sound") if isinstance(section_lookup.get("Wake Sound"), list) else None
    if isinstance(wake_sound_section, list) and "wake_word_triggered_sound_file" in fields_meta:
        wake_sound_entries = wake_sound_catalog.get("entries") if isinstance(wake_sound_catalog.get("entries"), list) else []
        wake_sound_enabled = _as_bool(profile.get(_WAKE_SOUND_ENABLED_PROFILE_KEY), _WAKE_SOUND_DEFAULT_ENABLED)
        current_wake_sound_url = _text(
            (
                fields_meta.get("wake_word_triggered_sound_file", {}).get("resolved_value")
                if isinstance(fields_meta.get("wake_word_triggered_sound_file"), dict)
                else ""
            )
        )
        available_urls = {_text(row.get("url")) for row in wake_sound_entries if isinstance(row, dict)}
        picker_value = (
            _WAKE_SOUND_DISABLED_PICKER_VALUE
            if not wake_sound_enabled
            else current_wake_sound_url
            if current_wake_sound_url in available_urls
            else "__custom__"
        )
        catalog_description = (
            f"Choose from {len(wake_sound_entries)} prebuilt wake sounds, "
            "select No wake sound, or leave this on Custom URL and paste your own audio URL below. No wake sound gives the fastest first-word capture."
            if wake_sound_entries
            else "Prebuilt wake-sound catalog is unavailable right now. You can still select No wake sound for fastest first-word capture or paste any custom audio URL below."
        )
        catalog_warning = _text(wake_sound_catalog.get("warning"))
        if catalog_warning and not wake_sound_entries:
            catalog_description = f"{catalog_description} {_text(catalog_warning)}".strip()
        wake_sound_section.insert(
            0,
            {
                "key": "wake_sound_catalog",
                "label": "Prebuilt Wake Sound",
                "type": "select",
                "value": picker_value,
                "options": _wake_sound_picker_options(wake_sound_catalog),
                "description": catalog_description,
            },
        )

    cli_status = esphome_cli_status()
    links = [
        {"label": "Template YAML", "href": _text((template_spec.get("source_urls") or [""])[0])},
        {"label": "Wake Word Requests", "href": f"https://github.com/{_WAKE_WORD_GITHUB_OWNER}/{_WAKE_WORD_GITHUB_REPO}"},
    ]

    model = _text(device_info.get("model"))
    project_name = _text(device_info.get("project_name"))
    detail_parts = [part for part in [host, model or project_name] if part]
    installed_firmware_version = _text(version_snapshot.get("installed"))
    firmware_badges: List[Dict[str, str]] = []
    if bool(version_snapshot.get("update_available")):
        firmware_badges.append({"label": "Firmware update available", "tone": "accent"})
    elif latest_firmware_version and installed_firmware_version:
        firmware_badges.append({"label": "Firmware current", "tone": "success"})
    elif latest_firmware_version:
        firmware_badges.append({"label": "Firmware version unknown", "tone": "muted"})

    item = {
        "id": selector_token,
        "selector": selector_token,
        "template_key": _text(template_spec.get("key")),
        "title": display_name,
        "subtitle": " • ".join(part for part in [host, _text(template_spec.get("label"))] if part),
        "detail": " • ".join(detail_parts),
        "template_label": _text(template_spec.get("label")),
        "template_url": _text((template_spec.get("source_urls") or [""])[0]),
        "firmware_version": latest_firmware_version,
        "firmware_project": firmware_project,
        "installed_firmware_version": installed_firmware_version,
        "installed_firmware_version_source": _text(version_snapshot.get("source")),
        "firmware_update_available": bool(version_snapshot.get("update_available")),
        "firmware_update_status": (
            f"Installed {installed_firmware_version} • latest {latest_firmware_version}"
            if installed_firmware_version and latest_firmware_version
            else (
                f"Latest {latest_firmware_version} • flash once to enable version checks"
                if latest_firmware_version
                else "Firmware version metadata unavailable"
            )
        ),
        "hero_badges": firmware_badges,
        "hero_image_src": esphome_ui_helpers.device_image_src(
            display_name,
            device_info.get("name"),
            device_info.get("friendly_name"),
            device_info.get("model"),
            device_info.get("project_name"),
            template_spec.get("key"),
            template_spec.get("label"),
        ),
        "hero_image_alt": f"{display_name} firmware target",
        "connected": connected,
        "sections": sections_ui,
        "links": [row for row in links if _text(row.get("href"))],
        "cli_available": bool(cli_status.get("available")),
        "cli_reason": _text(cli_status.get("detail")),
        "host": host,
    }

    return {
        "selector": selector_token,
        "host": host,
        "display_name": display_name,
        "template_key": template_key,
        "template_label": _text(template_spec.get("label")),
        "firmware_version": latest_firmware_version,
        "firmware_project": firmware_project,
        "template_spec": template_spec,
        "template_ctx": template_ctx,
        "profile": profile,
        "field_order": field_order,
        "fields_meta": fields_meta,
        "item": item,
    }


def _summarize_process_output(stdout: str, stderr: str, *, max_lines: int = 16) -> str:
    joined = "\n".join(part for part in [_text(stdout), _text(stderr)] if _text(part))
    if not joined:
        return ""
    lines = [line.rstrip() for line in joined.splitlines() if line.strip()]
    return "\n".join(lines[-max_lines:])


def _remote_template_text(url: str, *, force_refresh: bool = False) -> str:
    target = _text(url)
    if not target:
        raise RuntimeError("Firmware template URL is missing.")

    now = time.time()
    if not force_refresh:
        with _REMOTE_TEMPLATE_LOCK:
            cached = _REMOTE_TEMPLATE_CACHE.get(target)
            cached_ts = float(cached.get("ts") or 0.0) if isinstance(cached, dict) else 0.0
            if isinstance(cached, dict) and (now - cached_ts) < _REMOTE_TEMPLATE_CACHE_TTL_SECONDS:
                text_value = _text(cached.get("text"))
                if text_value:
                    return text_value
                error_value = _text(cached.get("error"))
                if error_value:
                    raise RuntimeError(error_value)

    req = urllib_request.Request(
        target,
        headers={
            "User-Agent": "Tater/1.0",
            "Accept": "text/plain, text/yaml, application/yaml, */*",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    try:
        with urllib_request.urlopen(req, timeout=_REMOTE_TEMPLATE_FETCH_TIMEOUT_SECONDS) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            text_value = response.read().decode(charset, errors="replace")
    except Exception as exc:
        message = f"Failed to fetch firmware template from {target}: {_text(exc) or exc.__class__.__name__}."
        with _REMOTE_TEMPLATE_LOCK:
            _REMOTE_TEMPLATE_CACHE[target] = {"ts": now, "error": message}
        raise RuntimeError(message) from exc

    with _REMOTE_TEMPLATE_LOCK:
        _REMOTE_TEMPLATE_CACHE[target] = {"ts": now, "text": text_value}
    return text_value


def _probe_cli_executable(path_token: str) -> Dict[str, Any]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    proc = subprocess.run(
        [path_token, "version"],
        cwd=str(FIRMWARE_RUNNER_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if proc.returncode == 0:
        return {
            "available": True,
            "label": path_token,
            "detail": "Using ESPHome from PATH.",
            "argv": [path_token],
            "cwd": str(FIRMWARE_RUNNER_ROOT),
            "env": _runner_env_overrides(),
        }
    return {
        "available": False,
        "label": path_token,
        "detail": _summarize_process_output(proc.stdout, proc.stderr) or f"`{path_token} version` failed.",
    }


def _probe_source_checkout() -> Dict[str, Any]:
    source_root = _repo_siblings_root() / "esphome"
    if not source_root.is_dir():
        return {"available": False, "label": "Source checkout", "detail": "No ESPHome source checkout was found."}

    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    existing_pythonpath = _text(env.get("PYTHONPATH"))
    env["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(source_root)
    )
    argv = [sys.executable, "-m", "esphome"]
    proc = subprocess.run(
        [*argv, "version"],
        cwd=str(FIRMWARE_RUNNER_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if proc.returncode == 0:
        return {
            "available": True,
            "label": f"{Path(sys.executable).name} -m esphome",
            "detail": f"Using ESPHome source checkout in {source_root} with isolated runner workspace.",
            "argv": argv,
            "cwd": str(FIRMWARE_RUNNER_ROOT),
            "env": {
                **_runner_env_overrides(),
                "PYTHONPATH": env["PYTHONPATH"],
            },
        }
    return {
        "available": False,
        "label": "Source checkout",
        "detail": _summarize_process_output(proc.stdout, proc.stderr)
        or f"ESPHome source checkout in {source_root} is not runnable in the current Python environment.",
    }


def esphome_cli_status(*, force: bool = False) -> Dict[str, Any]:
    now = time.time()
    with _CLI_STATUS_LOCK:
        cached = _CLI_STATUS_CACHE.get("status")
        cached_ts = float(_CLI_STATUS_CACHE.get("ts") or 0.0)
        if not force and isinstance(cached, dict) and (now - cached_ts) < _CLI_STATUS_CACHE_TTL_SECONDS:
            return dict(cached)

    status = {"available": False, "label": "Unavailable", "detail": "ESPHome CLI is not available."}
    path_cli = shutil.which("esphome")
    if path_cli:
        status = _probe_cli_executable(path_cli)
    if not bool(status.get("available")):
        source_status = _probe_source_checkout()
        if bool(source_status.get("available")):
            status = source_status
        elif not path_cli:
            status = source_status
        else:
            status["detail"] = _text(status.get("detail")) or _text(source_status.get("detail")) or status["detail"]

    with _CLI_STATUS_LOCK:
        _CLI_STATUS_CACHE["ts"] = now
        _CLI_STATUS_CACHE["status"] = dict(status)
    return dict(status)


def _firmware_device_option(selector: str, client_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(client_row, dict):
        return None

    selector_token = _text(selector)
    connected = bool(client_row.get("connected"))
    selected = bool(client_row.get("selected"))
    usb_recovery = bool(client_row.get("firmware_usb_recovery")) or _is_usb_recovery_selector(selector_token)
    if not (connected or selected or usb_recovery):
        return None

    if usb_recovery:
        return {
            "value": _FIRMWARE_USB_RECOVERY_SELECTOR,
            "label": "Browser USB Recovery",
            "title": "Browser USB Recovery",
            "host": "",
            "detail": "Build firmware in Tater, then flash from this browser over Web Serial.",
            "connected": False,
        }

    device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
    host = _text(client_row.get("host")) or esphome_runtime.satellite_host_from_selector(selector_token)
    title = (
        _text(device_info.get("friendly_name"))
        or _text(device_info.get("name"))
        or _text(client_row.get("selector"))
        or selector_token
    )
    model = _text(device_info.get("model")) or _text(device_info.get("project_name"))
    status_label = "" if connected else "offline"
    label_parts = [part for part in [title, host, status_label] if part]
    label = " • ".join(label_parts) or selector_token
    detail = " • ".join(part for part in [host, model, "" if connected else "USB flash/logs available"] if part)
    return {
        "value": selector_token,
        "label": label,
        "title": title,
        "host": host,
        "detail": detail,
        "connected": connected,
    }


def firmware_panel_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    cli_status = esphome_cli_status()

    template_options = [
        {"value": _text(spec.get("key")), "label": _text(spec.get("label")) or _text(spec.get("key"))}
        for spec in _TEMPLATE_SPECS
        if _text(spec.get("key"))
    ]
    devices: List[Dict[str, Any]] = []
    devices_by_template: Dict[str, List[Dict[str, Any]]] = {
        row["value"]: [] for row in template_options if _text(row.get("value"))
    }
    seen_devices: set[str] = set()
    seen_devices_by_template: Dict[str, set[str]] = {
        row["value"]: set() for row in template_options if _text(row.get("value"))
    }
    variants: Dict[str, Dict[str, Dict[str, Any]]] = {row["value"]: {} for row in template_options if _text(row.get("value"))}
    warnings: List[str] = []
    seen_warnings: set[str] = set()

    def append_device_option(device_option: Dict[str, Any], *, template_key: str = "") -> None:
        value = _text(device_option.get("value"))
        if not value:
            return
        if value not in seen_devices:
            seen_devices.add(value)
            devices.append(device_option)
        template_token = _text(template_key)
        if not template_token:
            return
        seen_for_template = seen_devices_by_template.setdefault(template_token, set())
        if value in seen_for_template:
            return
        seen_for_template.add(value)
        devices_by_template.setdefault(template_token, []).append(
            {
                **device_option,
                "template_key": template_token,
            }
        )

    for selector, client_row in sorted(clients.items(), key=lambda item: _lower(item[0])):
        selector_token = _text(selector)
        row = client_row if isinstance(client_row, dict) else {}
        matched_template_key = _matched_template_key(selector_token, row)
        device_option = _firmware_device_option(selector_token, row)
        if not isinstance(device_option, dict):
            continue
        candidate_specs: List[Dict[str, Any]] = []
        if matched_template_key:
            spec = _template_spec_by_key(matched_template_key)
            if isinstance(spec, dict):
                candidate_specs.append(spec)
        else:
            candidate_specs = [dict(spec) for spec in _TEMPLATE_SPECS]

        for spec in candidate_specs:
            template_key = _text(spec.get("key"))
            if not template_key:
                continue
            try:
                context = _build_device_context(selector_token, row, dict(spec))
            except Exception as exc:
                message = f"{_text(spec.get('label')) or template_key}: {_text(exc) or 'Firmware template is unavailable.'}"
                if message not in seen_warnings:
                    seen_warnings.add(message)
                    warnings.append(message)
                continue
            if isinstance(context, dict):
                context_item = context["item"]
                if isinstance(context_item, dict):
                    context_item["unmatched_template"] = not bool(matched_template_key)
                append_device_option(
                    {
                        **device_option,
                        "unmatched_template": not bool(matched_template_key),
                    },
                    template_key=template_key,
                )
                variants.setdefault(template_key, {})[selector_token] = context["item"]

    usb_recovery_option = _firmware_device_option(_FIRMWARE_USB_RECOVERY_SELECTOR, {"firmware_usb_recovery": True})
    if isinstance(usb_recovery_option, dict):
        for spec in _TEMPLATE_SPECS:
            template_key = _text(spec.get("key"))
            if not template_key:
                continue
            try:
                context = _build_device_context(
                    _FIRMWARE_USB_RECOVERY_SELECTOR,
                    _usb_recovery_client_row(dict(spec)),
                    dict(spec),
                )
            except Exception as exc:
                message = f"{_text(spec.get('label')) or template_key}: {_text(exc) or 'Firmware template is unavailable.'}"
                if message not in seen_warnings:
                    seen_warnings.add(message)
                    warnings.append(message)
                continue
            if isinstance(context, dict):
                append_device_option(usb_recovery_option, template_key=template_key)
                variants.setdefault(template_key, {})[_FIRMWARE_USB_RECOVERY_SELECTOR] = context["item"]

    active_template_key = ""
    active_selector = ""
    for template_option in template_options:
        candidate_key = _text(template_option.get("value"))
        candidate_devices = devices_by_template.get(candidate_key) if candidate_key else []
        first_real_device = next(
            (
                row
                for row in (candidate_devices or [])
                if isinstance(row, dict) and not _is_usb_recovery_selector(row.get("value"))
            ),
            None,
        )
        if candidate_key and isinstance(first_real_device, dict):
            active_template_key = candidate_key
            active_selector = _text(first_real_device.get("value"))
            break
    if not active_template_key:
        for template_option in template_options:
            candidate_key = _text(template_option.get("value"))
            if candidate_key and devices_by_template.get(candidate_key):
                active_template_key = candidate_key
                active_selector = _text((devices_by_template[candidate_key][0] or {}).get("value"))
                break
    if not active_template_key:
        active_template_key = _text((template_options[0] or {}).get("value")) if template_options else ""
    if not active_selector:
        active_selector = _text((devices[0] or {}).get("value")) if devices else ""

    empty_message = "No ESPHome firmware targets are available."
    if warnings and not any(bool(rows) for rows in variants.values()):
        empty_message = warnings[0]
    elif not devices:
        empty_message = "No ESPHome firmware targets are available."

    firmware_updates: List[Dict[str, Any]] = []
    firmware_flash_targets: List[Dict[str, Any]] = []
    for template_key, rows in variants.items():
        if not isinstance(rows, dict):
            continue
        for selector_token, item in rows.items():
            if not isinstance(item, dict):
                continue
            if _is_usb_recovery_selector(selector_token):
                continue
            row_payload = {
                "selector": selector_token,
                "template_key": template_key,
                "title": _text(item.get("title")) or selector_token,
                "template_label": _text(item.get("template_label")),
                "installed": _text(item.get("installed_firmware_version")) or "unknown",
                "latest": _text(item.get("firmware_version")),
            }
            if bool(item.get("connected")) and not bool(item.get("unmatched_template")):
                firmware_flash_targets.append(dict(row_payload))
            if not bool(item.get("firmware_update_available")):
                continue
            if bool(item.get("unmatched_template")):
                continue
            if not _text(item.get("installed_firmware_version")):
                continue
            firmware_updates.append(
                row_payload
            )

    payload = {
        "cli": cli_status,
        "devices": devices,
        "devices_by_template": devices_by_template,
        "templates": template_options,
        "variants": variants,
        "firmware_updates": firmware_updates,
        "firmware_update_count": len(firmware_updates),
        "firmware_flash_targets": firmware_flash_targets,
        "firmware_flash_target_count": len(firmware_flash_targets),
        "active_selector": active_selector,
        "active_template_key": active_template_key,
        "empty_message": empty_message,
        "wifi_note": (
            "Wi-Fi SSID is stored per device in Tater. "
            "Leave the Wi-Fi password blank to keep the saved password for that device."
        ),
        "browser_flash_note": (
            "Browser USB flash builds firmware in Tater, then uses Web Serial from this browser. "
            "Plug the device into the computer running the browser, and use Chrome or Edge on a secure context."
        ),
    }
    if warnings:
        payload["warnings"] = warnings[:6]
    return payload


def _normalize_profile_values(context: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, str]:
    incoming = values if isinstance(values, dict) else {}
    existing = context.get("profile") if isinstance(context.get("profile"), dict) else {}
    normalized: Dict[str, str] = dict(existing)
    wake_word_source = (
        _wake_word_source_value(incoming.get("wake_word_source"))
        or _wake_word_source_value(existing.get("wake_word_source"))
        or "prebuilt"
    )
    normalized["wake_word_source"] = wake_word_source

    if "wake_word_trainer_url" in incoming:
        trainer_url = _normalize_http_base_url(incoming.get("wake_word_trainer_url"))
    else:
        trainer_url = _normalize_http_base_url(existing.get("wake_word_trainer_url"))
    if trainer_url:
        normalized["wake_word_trainer_url"] = trainer_url
    else:
        normalized.pop("wake_word_trainer_url", None)

    for key in list(context.get("field_order") or []):
        meta = context.get("fields_meta", {}).get(key) if isinstance(context.get("fields_meta"), dict) else {}
        field_type = _text(meta.get("type"))
        current_value = _text(meta.get("resolved_value"))
        if bool(meta.get("read_only")):
            normalized[key] = current_value
            continue

        has_incoming_value = key in incoming
        raw_value = incoming.get(key)
        if not has_incoming_value:
            normalized[key] = _text(existing.get(key)) or current_value
            continue

        if field_type == "checkbox":
            normalized[key] = "true" if _as_bool(raw_value, False) else "false"
            continue

        if key == "wake_engine":
            engine_value = _lower(raw_value)
            if engine_value in {"openwakeword", "open_wake_word", "open wakeword"}:
                normalized[key] = "openwakeword"
            elif engine_value in {"nanowakeword", "nano_wake_word", "nano wakeword"}:
                normalized[key] = "nanowakeword"
            else:
                normalized[key] = "microwakeword"
            continue

        if key == "wifi_password":
            token = _text(raw_value)
            normalized[key] = token or _text(existing.get(key))
            continue

        if key == "tater_base_url":
            normalized[key] = _normalize_http_base_url(raw_value)
            continue

        if key == "openwakeword_server_url":
            normalized[key] = _normalize_http_base_url(raw_value, default_scheme="ws")
            continue

        if key == "nanowakeword_server_url":
            normalized[key] = _normalize_http_base_url(raw_value, default_scheme="ws")
            continue

        if key == "openwakeword_http_timeout_ms":
            normalized[key] = str(_as_int(raw_value, 3000, minimum=250, maximum=10000))
            continue

        if key == "openwakeword_max_failures":
            normalized[key] = str(_as_int(raw_value, 3, minimum=1, maximum=20))
            continue

        normalized[key] = _text(raw_value)

    wake_word_catalog_value = _text(incoming.get("wake_word_catalog"))
    if wake_word_source == "prebuilt" and wake_word_catalog_value and "wake_word_model_url" in normalized:
        normalized["wake_word_model_url"] = wake_word_catalog_value
        if "wake_word_name" in normalized:
            normalized["wake_word_name"] = _wake_word_slug_from_url(wake_word_catalog_value) or _text(normalized.get("wake_word_name"))

    trainer_wake_word_value = _text(incoming.get("wake_word_trainer_catalog"))
    if wake_word_source == "trainer" and trainer_wake_word_value and "wake_word_model_url" in normalized:
        normalized["wake_word_model_url"] = trainer_wake_word_value
        if "wake_word_name" in normalized:
            normalized["wake_word_name"] = _wake_word_slug_from_url(trainer_wake_word_value) or _text(normalized.get("wake_word_name"))

    wake_sound_catalog_value = _text(incoming.get("wake_sound_catalog"))
    if wake_sound_catalog_value == _WAKE_SOUND_DISABLED_PICKER_VALUE:
        normalized[_WAKE_SOUND_ENABLED_PROFILE_KEY] = "false"
    elif wake_sound_catalog_value:
        normalized[_WAKE_SOUND_ENABLED_PROFILE_KEY] = "true"
    if (
        wake_sound_catalog_value
        and wake_sound_catalog_value not in {"__custom__", _WAKE_SOUND_DISABLED_PICKER_VALUE}
        and "wake_word_triggered_sound_file" in normalized
    ):
        normalized["wake_word_triggered_sound_file"] = wake_sound_catalog_value

    if _text(context.get("host")) and "ha_voice_ip" in normalized:
        normalized["ha_voice_ip"] = _text(context.get("host"))

    return {key: _text(value) for key, value in normalized.items() if _text(key)}


def _validate_profile_values(context: Dict[str, Any], values: Dict[str, str]) -> None:
    required: List[str] = []
    if "wifi_ssid" in values and not _text(values.get("wifi_ssid")):
        required.append("Wi-Fi SSID")
    if "wifi_password" in values and not _text(values.get("wifi_password")):
        required.append("Wi-Fi password")
    if "ha_voice_ip" in values and not _text(values.get("ha_voice_ip")):
        required.append("satellite IP")
    if required:
        raise RuntimeError(f"Missing required firmware values: {', '.join(required)}.")


def _rewrite_local_packages(config: Dict[str, Any], repo_root: Optional[Path]) -> None:
    if not isinstance(repo_root, Path):
        return
    packages = config.get("packages")
    if not isinstance(packages, dict):
        return

    new_packages: Dict[str, Any] = {}
    changed = False
    for package_name, package_value in packages.items():
        if not isinstance(package_value, dict):
            new_packages[_text(package_name)] = package_value
            continue
        files = package_value.get("files")
        if not isinstance(files, list):
            new_packages[_text(package_name)] = package_value
            continue

        changed = True
        for index, entry in enumerate(files, start=1):
            file_path = ""
            file_vars: Dict[str, Any] = {}
            if isinstance(entry, dict):
                file_path = _text(entry.get("path"))
                file_vars = entry.get("vars") if isinstance(entry.get("vars"), dict) else {}
            else:
                file_path = _text(entry)
            if not file_path:
                continue
            absolute_path = repo_root / file_path
            package_row: Dict[str, Any] = {"file": str(absolute_path)}
            if file_vars:
                package_row["vars"] = dict(file_vars)
            new_packages[f"{_text(package_name) or 'package'}.{index}"] = package_row

    if changed and new_packages:
        config["packages"] = new_packages


def _append_esphome_on_boot(config: Dict[str, Any], automation: Dict[str, Any]) -> None:
    if not isinstance(config, dict) or not isinstance(automation, dict):
        return
    esphome_block = config.get("esphome") if isinstance(config.get("esphome"), dict) else {}
    existing = esphome_block.get("on_boot")
    if isinstance(existing, list):
        existing.append(automation)
    elif existing:
        esphome_block["on_boot"] = [existing, automation]
    else:
        esphome_block["on_boot"] = [automation]
    config["esphome"] = esphome_block


def _apply_wake_sound_profile(config: Dict[str, Any], values: Dict[str, str]) -> None:
    if _as_bool((values or {}).get(_WAKE_SOUND_ENABLED_PROFILE_KEY), _WAKE_SOUND_DEFAULT_ENABLED):
        return
    substitutions = config.get("substitutions") if isinstance(config.get("substitutions"), dict) else {}
    if "wake_sound_restore_mode" in substitutions:
        substitutions["wake_sound_restore_mode"] = "ALWAYS_OFF"
        config["substitutions"] = substitutions
    _append_esphome_on_boot(
        config,
        {
            "priority": -100,
            "then": [
                {"switch.turn_off": "wake_sound"},
            ],
        },
    )


def _render_config_text(context: Dict[str, Any], values: Dict[str, str]) -> str:
    config = copy.deepcopy(context["template_ctx"]["template_doc"])
    substitutions = config.get("substitutions") if isinstance(config.get("substitutions"), dict) else {}
    for key in list(context.get("field_order") or []):
        substitutions[key] = _text(values.get(key))
    config["substitutions"] = substitutions
    esphome_block = config.get("esphome") if isinstance(config.get("esphome"), dict) else {}
    esphome_block["build_path"] = str(
        FIRMWARE_BUILD_ROOT / _sanitize_token(context.get("selector")) / _sanitize_token(context.get("template_key"))
    )
    config["esphome"] = esphome_block
    _apply_wake_sound_profile(config, values)
    _rewrite_local_packages(config, context["template_ctx"].get("repo_root"))
    return yaml.dump(config, Dumper=_FirmwareYamlDumper, sort_keys=False, allow_unicode=True)


def _prepare_config_path(context: Dict[str, Any], values: Dict[str, str]) -> Path:
    _ensure_agent_labs_dirs()
    _reset_build_path_for_context(context)
    selector_dir = FIRMWARE_CONFIG_ROOT / _sanitize_token(context.get("selector"))
    selector_dir.mkdir(parents=True, exist_ok=True)
    config_path = selector_dir / f"{_sanitize_token(context.get('template_key'))}.yaml"
    config_path.write_text(_render_config_text(context, values), encoding="utf-8")
    return config_path


def _build_path_for_context(context: Dict[str, Any]) -> Path:
    return FIRMWARE_BUILD_ROOT / _sanitize_token(context.get("selector")) / _sanitize_token(context.get("template_key"))


def _reset_build_path_for_context(context: Dict[str, Any]) -> Optional[Path]:
    build_path = _build_path_for_context(context)
    if not build_path.exists():
        return None
    shutil.rmtree(build_path, ignore_errors=True)
    if build_path.exists():
        raise RuntimeError(f"Could not clear stale ESPHome build output at {build_path}.")
    return build_path


def _find_browser_flash_binary(context: Dict[str, Any]) -> Path:
    build_path = _build_path_for_context(context)
    if not build_path.exists():
        raise RuntimeError(f"ESPHome build output was not found at {build_path}.")

    bins = [path for path in build_path.rglob("*.bin") if path.is_file()]
    if not bins:
        raise RuntimeError(f"ESPHome did not produce a browser-flashable .bin under {build_path}.")

    preferred_names = (
        "firmware.factory.bin",
        "firmware-factory.bin",
        "factory.bin",
        "merged-firmware.bin",
        "merged.bin",
    )
    for wanted in preferred_names:
        matches = [path for path in bins if path.name == wanted]
        if matches:
            return max(matches, key=lambda path: path.stat().st_size)

    raise RuntimeError(
        "ESPHome built firmware, but no factory/merged .bin was found for browser flashing. "
        "ESP Web Tools needs a single factory image for ESP32-S3 browser installs."
    )


def _browser_flash_artifact_id(context: Dict[str, Any]) -> str:
    base = "_".join(
        part
        for part in [
            _sanitize_token(context.get("selector")),
            _sanitize_token(context.get("template_key")),
            str(int(time.time())),
            uuid.uuid4().hex[:8],
        ]
        if part
    )
    return base or f"artifact_{uuid.uuid4().hex[:12]}"


def _create_browser_flash_artifact(context: Dict[str, Any], binary_path: Path) -> Dict[str, Any]:
    artifact_id = _browser_flash_artifact_id(context)
    artifact_dir = FIRMWARE_WEB_FLASH_ROOT / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    target_binary_name = "firmware.bin"
    target_binary_path = artifact_dir / target_binary_name
    shutil.copy2(binary_path, target_binary_path)

    display_name = _text(context.get("display_name")) or _text(context.get("template_label")) or "Tater Firmware"
    manifest = {
        "name": display_name,
        "version": time.strftime("%Y.%m.%d.%H%M%S", time.localtime()),
        "new_install_prompt_erase": True,
        "builds": [
            {
                "chipFamily": "ESP32-S3",
                "parts": [
                    {
                        "path": target_binary_name,
                        "offset": 0,
                    }
                ],
            }
        ],
    }
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    template_ctx = context.get("template_ctx") if isinstance(context.get("template_ctx"), dict) else {}
    template_doc = template_ctx.get("template_doc") if isinstance(template_ctx.get("template_doc"), dict) else {}
    esp32_block = template_doc.get("esp32") if isinstance(template_doc.get("esp32"), dict) else {}
    base_url = f"/api/settings/esphome/firmware-web/{artifact_id}"
    return {
        "artifact_id": artifact_id,
        "manifest_url": f"{base_url}/manifest.json",
        "binary_url": f"{base_url}/{target_binary_name}",
        "binary_name": target_binary_name,
        "selector": _text(context.get("selector")),
        "template_key": _text(context.get("template_key")),
        "firmware_version": _text(context.get("firmware_version")),
        "source_binary": str(binary_path),
        "binary_size": int(target_binary_path.stat().st_size),
        "erase_all": True,
        "flash_size": _text(esp32_block.get("flash_size")) or "4MB",
        "flash_mode": "dio",
        "flash_freq": "40m",
    }


def _prepare_browser_flash_build(
    context: Dict[str, Any],
    profile_values: Dict[str, str],
    cli_status: Dict[str, Any],
) -> Dict[str, Any]:
    config_path = _prepare_config_path(context, profile_values)
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")

    command = [*argv, "compile", str(config_path)]
    proc = _run_esphome_command(
        command,
        cwd=_text(cli_status.get("cwd")),
        env=_runner_env(cli_status),
    )
    summary = _summarize_process_output(proc.stdout, proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError(
            f"ESPHome browser flash build failed for {context.get('display_name') or context.get('selector')}.\n\n"
            f"{summary or 'No CLI output was captured.'}"
        )

    binary_path = _find_browser_flash_binary(context)
    artifact = _create_browser_flash_artifact(context, binary_path)
    return {
        "ok": True,
        "selector": context.get("selector"),
        "template_key": context.get("template_key"),
        "config_path": str(config_path),
        "command": command,
        "message": f"Built browser flash firmware for {context.get('display_name') or context.get('selector')}.",
        "output_tail": summary,
        **artifact,
    }


def browser_flash_artifact_path(artifact_id: str, relative_path: str) -> Path:
    artifact = _sanitize_token(artifact_id)
    if not artifact:
        raise KeyError("Browser flash artifact is missing.")
    rel = Path(_text(relative_path))
    if rel.is_absolute() or any(part in {"", ".", ".."} for part in rel.parts):
        raise KeyError("Browser flash artifact path is invalid.")
    root = (FIRMWARE_WEB_FLASH_ROOT / artifact).resolve()
    target = (root / rel).resolve()
    if root not in target.parents and target != root:
        raise KeyError("Browser flash artifact path is invalid.")
    if not target.is_file():
        raise KeyError("Browser flash artifact file was not found.")
    return target


def _runner_env(status: Dict[str, Any]) -> Dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update(_runner_env_overrides())
    runner_env = status.get("env") if isinstance(status.get("env"), dict) else {}
    for key, value in runner_env.items():
        env[str(key)] = _text(value)
    return env


def _run_esphome_command(argv: List[str], *, cwd: str = "", env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd or None,
        env=env or None,
        capture_output=True,
        text=True,
        timeout=FIRMWARE_BUILD_TIMEOUT_SECONDS,
        check=False,
    )


def _entry_time_text(ts_value: Optional[float] = None) -> str:
    stamp = float(ts_value or time.time())
    return time.strftime("%H:%M:%S", time.localtime(stamp))


def _session_entries_after_locked(session: Dict[str, Any], after_seq: int = 0) -> List[Dict[str, Any]]:
    rows = session.get("entries") if isinstance(session.get("entries"), list) else []
    threshold = max(0, int(after_seq or 0))
    return [dict(row) for row in rows if int(row.get("seq") or 0) > threshold]


def _append_session_entry_locked(
    session: Dict[str, Any],
    *,
    level: str = "info",
    message: Any = "",
    ts_value: Optional[float] = None,
    time_text: str = "",
    source: str = "cli",
    display: str = "",
) -> Optional[Dict[str, Any]]:
    text_value = _clean_terminal_text(display) or _clean_terminal_text(message)
    if not text_value:
        return None
    entries = session.get("entries")
    if not isinstance(entries, list):
        entries = []
        session["entries"] = entries
    seq = int(session.get("cursor") or 0) + 1
    row = {
        "seq": seq,
        "time": _text(time_text) or _entry_time_text(ts_value),
        "level": _lower(level) or "info",
        "message": _clean_terminal_text(message) or text_value,
        "display": text_value,
        "source": _text(source) or "cli",
    }
    entries.append(row)
    overflow = len(entries) - _FIRMWARE_SESSION_MAX_ENTRIES
    if overflow > 0:
        del entries[:overflow]
    session["cursor"] = seq
    session["updated_ts"] = time.time()
    return dict(row)


def _append_session_passthrough_locked(session: Dict[str, Any], entry: Dict[str, Any], *, source: str) -> Optional[Dict[str, Any]]:
    return _append_session_entry_locked(
        session,
        level=_text(entry.get("level")) or "info",
        message=_text(entry.get("message") or entry.get("display")),
        time_text=_text(entry.get("time")),
        source=source,
        display=_text(entry.get("display") or entry.get("message")),
    )


def _phase_status_text(phase: str, display_name: str = "") -> str:
    name = _text(display_name) or "device"
    token = _lower(phase)
    if token == "starting":
        return f"Preparing firmware flash for {name}..."
    if token == "building":
        return f"Building firmware for {name}..."
    if token == "uploading":
        return f"Uploading firmware to {name}..."
    if token == "awaiting_device_logs":
        return f"Upload finished. Waiting for {name} to reconnect for live logs..."
    if token == "live_logs":
        return f"Streaming live logs from {name}."
    if token == "failed":
        return f"Firmware flash failed for {name}."
    if token == "cancelled":
        return f"Firmware flash stopped for {name}."
    if token == "completed":
        return f"Firmware flash completed for {name}."
    return f"Firmware session active for {name}."


def _set_session_phase_locked(session: Dict[str, Any], phase: str) -> None:
    token = _text(phase)
    if token:
        session["phase"] = token
    session["status_text"] = _phase_status_text(_text(session.get("phase")), _text(session.get("display_name")))
    session["updated_ts"] = time.time()


def _cli_line_level(line: str) -> str:
    text_value = _clean_terminal_text(line)
    if not text_value:
        return "info"
    upper = text_value.upper()
    if re.search(r"\[[^\]]+\]\[E\]", text_value):
        return "error"
    if re.search(r"\[[^\]]+\]\[W\]", text_value):
        return "warn"
    if re.search(r"\[[^\]]+\]\[[DV]\]", text_value):
        return "debug"
    if any(token in upper for token in ["ERROR", "FAILED", "EXCEPTION", "TRACEBACK"]):
        return "error"
    if "WARN" in upper:
        return "warn"
    if "DEBUG" in upper or "VERBOSE" in upper:
        return "debug"
    return "info"


def _cli_line_phase(current_phase: str, line: str) -> str:
    token = _lower(current_phase)
    text_value = _lower(_clean_terminal_text(line))
    upload_markers = (
        "uploading",
        "espota.py",
        "sending invitation to",
        "ota",
        "writing at",
        "hard resetting via",
    )
    build_markers = (
        "dependency graph",
        "compiling ",
        "linking ",
        "archiving ",
        "building ",
        ".pioenvs",
    )
    if any(marker in text_value for marker in upload_markers):
        return "uploading"
    if token in {"starting", ""} and any(marker in text_value for marker in build_markers):
        return "building"
    return token or "building"


def _final_session_phase(session: Dict[str, Any]) -> str:
    phase = _lower(session.get("phase"))
    if phase in {"failed", "cancelled"}:
        return phase
    if phase == "live_logs":
        return "live_logs" if bool(session.get("active")) else "completed"
    if int(session.get("returncode") or 0) == 0:
        return "completed"
    if bool(session.get("stop_requested")):
        return "cancelled"
    return "failed"


def _session_payload_locked(session: Dict[str, Any], *, after_seq: int = 0) -> Dict[str, Any]:
    phase = _text(session.get("phase"))
    final_phase = _final_session_phase(session)
    active = bool(session.get("active"))
    return {
        "ok": True,
        "session_id": _text(session.get("id")),
        "selector": _text(session.get("selector")),
        "template_key": _text(session.get("template_key")),
        "firmware_version": _text(session.get("firmware_version")),
        "display_name": _text(session.get("display_name")),
        "host": _text(session.get("host")),
        "operation": _text(session.get("operation")),
        "phase": phase,
        "status_text": _text(session.get("status_text")) or _phase_status_text(phase, _text(session.get("display_name"))),
        "active": active,
        "completed": not active and final_phase in {"completed", "failed", "cancelled"},
        "cursor": int(session.get("cursor") or 0),
        "entries": _session_entries_after_locked(session, after_seq),
        "error": _text(session.get("error")),
        "message": _text(session.get("message")),
        "command": list(session.get("command") or []),
        "config_path": _text(session.get("config_path")),
        "artifact_id": _text(session.get("artifact_id")),
        "manifest_url": _text(session.get("manifest_url")),
        "binary_url": _text(session.get("binary_url")),
        "binary_name": _text(session.get("binary_name")),
        "binary_size": int(session.get("binary_size") or 0),
        "source_binary": _text(session.get("source_binary")),
        "erase_all": _as_bool(session.get("erase_all"), False),
        "flash_size": _text(session.get("flash_size")),
        "flash_mode": _text(session.get("flash_mode")),
        "flash_freq": _text(session.get("flash_freq")),
    }


def _stop_device_logs_if_needed(session: Dict[str, Any]) -> None:
    if not bool(session.get("device_logs_started")):
        return
    selector = _text(session.get("selector"))
    if not selector:
        return
    with contextlib.suppress(Exception):
        esphome_runtime.logs_stop(selector, force=False, timeout=20.0)


def _prune_firmware_sessions() -> None:
    now = time.time()
    stale_sessions: List[Dict[str, Any]] = []
    with _FIRMWARE_SESSION_LOCK:
        for session_id, session in list(_FIRMWARE_SESSIONS.items()):
            if not isinstance(session, dict):
                _FIRMWARE_SESSIONS.pop(session_id, None)
                continue
            proc = session.get("proc")
            running = isinstance(proc, subprocess.Popen) and proc.poll() is None
            updated_ts = float(session.get("updated_ts") or session.get("created_ts") or 0.0)
            if running:
                continue
            if updated_ts <= 0 or (now - updated_ts) < _FIRMWARE_SESSION_TTL_SECONDS:
                continue
            stale_sessions.append(session)
            _FIRMWARE_SESSIONS.pop(session_id, None)
    for session in stale_sessions:
        _stop_device_logs_if_needed(session)


def _active_flash_for_selector(selector: str) -> Optional[Dict[str, Any]]:
    token = _text(selector)
    if not token:
        return None
    with _FIRMWARE_SESSION_LOCK:
        for session in _FIRMWARE_SESSIONS.values():
            if not isinstance(session, dict):
                continue
            if _text(session.get("selector")) != token:
                continue
            if bool(session.get("active")):
                return dict(session)
    return None


def _firmware_session_worker(session_id: str) -> None:
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        command = list(session.get("command") or [])
        cwd = _text(session.get("cwd"))
        env = session.get("env") if isinstance(session.get("env"), dict) else None
        _set_session_phase_locked(session, "building")

    proc: Optional[subprocess.Popen[str]] = None
    try:
        proc = subprocess.Popen(
            command,
            cwd=cwd or None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except Exception as exc:
        with _FIRMWARE_SESSION_LOCK:
            session = _FIRMWARE_SESSIONS.get(session_id)
            if isinstance(session, dict):
                session["active"] = False
                session["error"] = _text(exc) or exc.__class__.__name__
                _set_session_phase_locked(session, "failed")
                _append_session_entry_locked(
                    session,
                    level="error",
                    message=f"Failed to start ESPHome CLI: {_text(exc) or exc.__class__.__name__}.",
                    source="cli",
                )
        return

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            with contextlib.suppress(Exception):
                proc.terminate()
            return
        session["proc"] = proc
        session["pid"] = int(proc.pid or 0)
        _append_session_entry_locked(
            session,
            level="debug",
            message=f"ESPHome process started (pid {int(proc.pid or 0)}).",
            source="cli",
        )

    try:
        if proc.stdout is not None:
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                with _FIRMWARE_SESSION_LOCK:
                    session = _FIRMWARE_SESSIONS.get(session_id)
                    if not isinstance(session, dict):
                        continue
                    next_phase = _cli_line_phase(_text(session.get("phase")), line)
                    if next_phase != _text(session.get("phase")):
                        _set_session_phase_locked(session, next_phase)
                    _append_session_entry_locked(
                        session,
                        level=_cli_line_level(line),
                        message=line,
                        source="cli",
                    )
    finally:
        with contextlib.suppress(Exception):
            if proc.stdout is not None:
                proc.stdout.close()
        returncode = proc.wait()

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        session["proc"] = None
        session["returncode"] = int(returncode)
        stop_requested = bool(session.get("stop_requested"))
        if stop_requested:
            operation = _lower(session.get("operation"))
            session["active"] = False
            session["message"] = (
                "Browser flash build stopped."
                if operation == "browser_build"
                else "Firmware flash stopped."
            )
            _set_session_phase_locked(session, "cancelled")
            _append_session_entry_locked(
                session,
                level="warn",
                message=(
                    "Browser flash build cancelled."
                    if operation == "browser_build"
                    else "Firmware flash cancelled."
                ),
                source="cli",
            )
            return
        if returncode != 0:
            session["active"] = False
            session["error"] = f"ESPHome exited with code {int(returncode)}."
            session["message"] = (
                "Browser flash build failed."
                if _lower(session.get("operation")) == "browser_build"
                else "Firmware flash failed."
            )
            _set_session_phase_locked(session, "failed")
            _append_session_entry_locked(
                session,
                level="error",
                message=f"ESPHome CLI exited with code {int(returncode)}.",
                source="cli",
            )
            return
        if _lower(session.get("operation")) == "browser_build":
            try:
                context = session.get("context") if isinstance(session.get("context"), dict) else {}
                binary_path = _find_browser_flash_binary(context)
                artifact = _create_browser_flash_artifact(context, binary_path)
            except Exception as exc:
                session["active"] = False
                session["error"] = _text(exc) or exc.__class__.__name__
                session["message"] = "Browser flash build failed."
                _set_session_phase_locked(session, "failed")
                _append_session_entry_locked(
                    session,
                    level="error",
                    message=f"Browser flash artifact failed: {_text(exc) or exc.__class__.__name__}.",
                    source="session",
                )
                return
            session.update(artifact)
            session["active"] = False
            session["returncode"] = 0
            session["message"] = "Browser flash firmware is ready."
            _set_session_phase_locked(session, "completed")
            _append_session_entry_locked(
                session,
                level="info",
                message=f"Browser flash firmware ready: {artifact.get('binary_name')} ({artifact.get('binary_size')} bytes).",
                source="session",
            )
            return
        _save_recorded_firmware_version(
            session.get("selector"),
            session.get("template_key"),
            session.get("firmware_version"),
            display_name=session.get("display_name"),
            source="ota_flash",
        )
        if not bool(session.get("follow_logs", True)):
            session["active"] = False
            session["returncode"] = 0
            session["message"] = "Firmware uploaded successfully."
            _set_session_phase_locked(session, "completed")
            _append_session_entry_locked(
                session,
                level="info",
                message="Build and upload finished.",
                source="session",
            )
            return
        session["returncode"] = 0
        session["message"] = "Firmware uploaded successfully. Waiting for live device logs."
        session["device_log_next_retry_ts"] = time.time()
        session["device_log_retry_count"] = 0
        _set_session_phase_locked(session, "awaiting_device_logs")
        _append_session_entry_locked(
            session,
            level="info",
            message="Build and upload finished. Waiting for the device to reconnect so live logs can continue here.",
            source="session",
        )


def _pump_session_device_logs(session_id: str) -> None:
    start_selector = ""
    start_after_seq = 0
    should_start = False
    should_poll = False
    retry_ts = 0.0
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        if not bool(session.get("active")):
            return
        phase = _lower(session.get("phase"))
        if phase not in {"awaiting_device_logs", "live_logs"}:
            return
        start_selector = _text(session.get("selector"))
        start_after_seq = int(session.get("device_log_cursor") or 0)
        retry_ts = float(session.get("device_log_next_retry_ts") or 0.0)
        should_poll = bool(session.get("device_logs_started"))
        should_start = not should_poll and time.time() >= retry_ts

    if not start_selector:
        return

    if should_start:
        try:
            result = esphome_runtime.logs_start(start_selector, timeout=20.0)
        except Exception as exc:
            with _FIRMWARE_SESSION_LOCK:
                session = _FIRMWARE_SESSIONS.get(session_id)
                if isinstance(session, dict):
                    attempts = int(session.get("device_log_retry_count") or 0) + 1
                    session["device_log_retry_count"] = attempts
                    session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
                    session["device_log_error"] = _text(exc) or exc.__class__.__name__
                    session["status_text"] = _phase_status_text("awaiting_device_logs", _text(session.get("display_name")))
        else:
            with _FIRMWARE_SESSION_LOCK:
                session = _FIRMWARE_SESSIONS.get(session_id)
                if isinstance(session, dict):
                    session["device_logs_started"] = True
                    session["device_log_cursor"] = int(result.get("cursor") or 0)
                    session["device_log_error"] = ""
                    session["message"] = "Firmware uploaded successfully. Streaming live device logs."
                    _set_session_phase_locked(session, "live_logs")
                    _append_session_entry_locked(
                        session,
                        level="info",
                        message="Connected to device logs. Streaming live output below.",
                        source="session",
                    )
                    for entry in list(result.get("entries") or []):
                        if isinstance(entry, dict):
                            _append_session_passthrough_locked(session, entry, source="device")
            return

    if not should_poll:
        return

    try:
        result = esphome_runtime.logs_poll(start_selector, after_seq=start_after_seq, timeout=5.0)
    except Exception as exc:
        with _FIRMWARE_SESSION_LOCK:
            session = _FIRMWARE_SESSIONS.get(session_id)
            if isinstance(session, dict):
                session["device_logs_started"] = False
                session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
                session["device_log_error"] = _text(exc) or exc.__class__.__name__
                _set_session_phase_locked(session, "awaiting_device_logs")
        return

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(session, dict):
            return
        session["device_log_cursor"] = int(result.get("cursor") or session.get("device_log_cursor") or 0)
        error_text = _text(result.get("error"))
        if error_text:
            session["device_log_error"] = error_text
        if not bool(result.get("active")):
            session["device_logs_started"] = False
            session["device_log_next_retry_ts"] = time.time() + _FIRMWARE_DEVICE_LOG_RETRY_SECONDS
            _set_session_phase_locked(session, "awaiting_device_logs")
            return
        _set_session_phase_locked(session, "live_logs")
        session["device_log_error"] = ""
        for entry in list(result.get("entries") or []):
            if isinstance(entry, dict):
                _append_session_passthrough_locked(session, entry, source="device")


def _start_flash_session(
    context: Dict[str, Any],
    profile_values: Dict[str, str],
    cli_status: Dict[str, Any],
    *,
    follow_logs: bool = True,
) -> Dict[str, Any]:
    _prune_firmware_sessions()
    selector = _text(context.get("selector"))
    active_session = _active_flash_for_selector(selector)
    if isinstance(active_session, dict):
        raise RuntimeError(
            f"A firmware flash session is already active for {_text(context.get('display_name')) or selector}."
        )

    config_path = _prepare_config_path(context, profile_values)
    host = _text(context.get("host"))
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")
    command = [*argv, "run", str(config_path), "--no-logs", "--device", host or "OTA"]
    session_id = f"fw_{uuid.uuid4().hex}"
    target_label = _text(context.get("display_name")) or selector
    session = {
        "id": session_id,
        "selector": selector,
        "template_key": _text(context.get("template_key")),
        "firmware_version": _text(context.get("firmware_version")),
        "display_name": target_label,
        "host": host,
        "context": context,
        "config_path": str(config_path),
        "command": command,
        "cwd": _text(cli_status.get("cwd")),
        "env": _runner_env(cli_status),
        "created_ts": time.time(),
        "updated_ts": time.time(),
        "cursor": 0,
        "entries": [],
        "phase": "starting",
        "status_text": _phase_status_text("starting", target_label),
        "active": True,
        "error": "",
        "message": (
            f"Streaming build, upload, and live device logs for {target_label}."
            if follow_logs
            else f"Streaming build and upload logs for {target_label}."
        ),
        "returncode": None,
        "proc": None,
        "stop_requested": False,
        "follow_logs": bool(follow_logs),
        "device_logs_started": False,
        "device_log_cursor": 0,
        "device_log_next_retry_ts": 0.0,
        "device_log_retry_count": 0,
        "device_log_error": "",
    }
    with _FIRMWARE_SESSION_LOCK:
        _FIRMWARE_SESSIONS[session_id] = session
        _append_session_entry_locked(
            session,
            level="info",
            message=(
                f"Preparing {_text(context.get('template_label')) or 'firmware'} for "
                f"{target_label} via OTA."
            ),
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message=f"Config written to {str(config_path)}",
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message="Command: " + " ".join(command),
            source="session",
        )

    worker = threading.Thread(target=_firmware_session_worker, args=(session_id,), daemon=True)
    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if isinstance(live_session, dict):
            live_session["worker"] = worker
    worker.start()

    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(live_session, dict):
            raise RuntimeError("Firmware session was not created.")
        return _session_payload_locked(live_session, after_seq=0)


def _start_browser_build_session(
    context: Dict[str, Any],
    profile_values: Dict[str, str],
    cli_status: Dict[str, Any],
) -> Dict[str, Any]:
    _prune_firmware_sessions()
    selector = _text(context.get("selector"))
    active_session = _active_flash_for_selector(selector)
    if isinstance(active_session, dict):
        raise RuntimeError(
            f"A firmware session is already active for {_text(context.get('display_name')) or selector}."
        )

    config_path = _prepare_config_path(context, profile_values)
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")
    command = [*argv, "compile", str(config_path)]
    session_id = f"fw_{uuid.uuid4().hex}"
    target_label = _text(context.get("display_name")) or selector
    session = {
        "id": session_id,
        "selector": selector,
        "template_key": _text(context.get("template_key")),
        "firmware_version": _text(context.get("firmware_version")),
        "display_name": target_label,
        "host": _text(context.get("host")),
        "operation": "browser_build",
        "context": context,
        "config_path": str(config_path),
        "command": command,
        "cwd": _text(cli_status.get("cwd")),
        "env": _runner_env(cli_status),
        "created_ts": time.time(),
        "updated_ts": time.time(),
        "cursor": 0,
        "entries": [],
        "phase": "starting",
        "status_text": _phase_status_text("starting", target_label),
        "active": True,
        "error": "",
        "message": f"Building browser flash firmware for {target_label}.",
        "returncode": None,
        "proc": None,
        "stop_requested": False,
        "device_logs_started": False,
        "device_log_cursor": 0,
        "device_log_next_retry_ts": 0.0,
        "device_log_retry_count": 0,
        "device_log_error": "",
    }
    with _FIRMWARE_SESSION_LOCK:
        _FIRMWARE_SESSIONS[session_id] = session
        _append_session_entry_locked(
            session,
            level="info",
            message=f"Preparing browser flash firmware for {target_label}.",
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message=f"Config written to {str(config_path)}",
            source="session",
        )
        _append_session_entry_locked(
            session,
            level="debug",
            message="Command: " + " ".join(command),
            source="session",
        )

    worker = threading.Thread(target=_firmware_session_worker, args=(session_id,), daemon=True)
    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if isinstance(live_session, dict):
            live_session["worker"] = worker
    worker.start()

    with _FIRMWARE_SESSION_LOCK:
        live_session = _FIRMWARE_SESSIONS.get(session_id)
        if not isinstance(live_session, dict):
            raise RuntimeError("Firmware session was not created.")
        return _session_payload_locked(live_session, after_seq=0)


def _poll_flash_session(session_id: str, *, after_seq: int = 0) -> Dict[str, Any]:
    _prune_firmware_sessions()
    _pump_session_device_logs(session_id)
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(_text(session_id))
        if not isinstance(session, dict):
            raise RuntimeError("Firmware log session is no longer available.")
        return _session_payload_locked(session, after_seq=after_seq)


def _stop_flash_session(session_id: str) -> Dict[str, Any]:
    _prune_firmware_sessions()
    session_token = _text(session_id)
    proc: Optional[subprocess.Popen[str]] = None
    selector = ""
    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_token)
        if not isinstance(session, dict):
            return {"ok": True, "session_id": session_token, "stopped": True}
        session["stop_requested"] = True
        session["active"] = False
        selector = _text(session.get("selector"))
        proc = session.get("proc") if isinstance(session.get("proc"), subprocess.Popen) else None
        if proc is None:
            _set_session_phase_locked(session, _final_session_phase(session))
            _append_session_entry_locked(
                session,
                level="info",
                message="Firmware log viewer closed.",
                source="session",
            )

    if isinstance(proc, subprocess.Popen) and proc.poll() is None:
        with contextlib.suppress(Exception):
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            with contextlib.suppress(Exception):
                proc.kill()

    if selector:
        with contextlib.suppress(Exception):
            esphome_runtime.logs_stop(selector, force=False, timeout=20.0)

    with _FIRMWARE_SESSION_LOCK:
        session = _FIRMWARE_SESSIONS.get(session_token)
        if not isinstance(session, dict):
            return {"ok": True, "session_id": session_token, "stopped": True}
        session["device_logs_started"] = False
        operation = _lower(session.get("operation"))
        if _lower(session.get("phase")) not in {"failed", "cancelled"}:
            _set_session_phase_locked(session, _final_session_phase(session))
        if _lower(session.get("phase")) == "completed":
            session["message"] = (
                "Browser flash firmware is ready."
                if operation == "browser_build"
                else "Firmware flash completed."
            )
        elif _lower(session.get("phase")) == "cancelled":
            session["message"] = (
                "Browser flash build stopped."
                if operation == "browser_build"
                else "Firmware flash stopped."
            )
        return {
            "ok": True,
            "session_id": session_token,
            "selector": selector,
            "stopped": True,
            "phase": _text(session.get("phase")),
            "message": _text(session.get("message")) or "Firmware log viewer closed.",
        }


def handle_runtime_action(action_name: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if action_name == "voice_firmware_trainer_wake_words":
        body = payload if isinstance(payload, dict) else {}
        trainer_url = _normalize_http_base_url(body.get("trainer_url"))
        catalog = _load_trainer_wake_word_catalog(trainer_url, force_refresh=True, strict=True)
        entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
        return {
            "ok": True,
            "action": action_name,
            "trainer_url": trainer_url,
            "entries": entries,
            "options": _trainer_wake_word_picker_options(catalog),
            "count": len(entries),
            "message": f"Loaded {len(entries)} trained wake word(s) from {trainer_url}.",
        }

    if action_name == "voice_firmware_flash_poll":
        body = payload if isinstance(payload, dict) else {}
        session_id = _text(body.get("session_id") or body.get("id"))
        after_seq = esphome_runtime.as_int(body.get("after_seq"), 0, minimum=0)
        if not session_id:
            raise ValueError("session_id is required")
        result = _poll_flash_session(session_id, after_seq=after_seq)
        result["action"] = action_name
        return result

    if action_name == "voice_firmware_flash_stop":
        body = payload if isinstance(payload, dict) else {}
        session_id = _text(body.get("session_id") or body.get("id"))
        if not session_id:
            raise ValueError("session_id is required")
        result = _stop_flash_session(session_id)
        result["action"] = action_name
        return result

    if action_name == "voice_firmware_clean":
        result = _clean_firmware_workspace()
        result["action"] = action_name
        return result

    if action_name == "voice_firmware_mark_installed":
        body = payload if isinstance(payload, dict) else {}
        selector = esphome_runtime.payload_selector(body)
        template_key = _text(body.get("template_key"))
        firmware_version = _text(body.get("firmware_version") or body.get("version"))
        if not selector:
            raise ValueError("selector is required")
        if not template_key:
            raise ValueError("template_key is required")
        if not firmware_version:
            raise ValueError("firmware_version is required")
        _save_recorded_firmware_version(
            selector,
            template_key,
            firmware_version,
            display_name=body.get("display_name"),
            source=_text(body.get("source")) or "browser_usb_flash",
        )
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "template_key": template_key,
            "firmware_version": firmware_version,
            "message": f"Recorded firmware {firmware_version} for {_text(body.get('display_name')) or selector}.",
        }

    if action_name not in {
        "voice_firmware_save",
        "voice_firmware_build",
        "voice_firmware_browser_build",
        "voice_firmware_flash",
        "voice_firmware_flash_start",
    }:
        return None

    body = payload if isinstance(payload, dict) else {}
    selector = esphome_runtime.payload_selector(body)
    template_key = _text(body.get("template_key"))
    if not selector:
        raise ValueError("selector is required")
    if not template_key:
        raise ValueError("template_key is required")

    template_spec = _template_spec_by_key(template_key)
    if not isinstance(template_spec, dict):
        raise RuntimeError(f"Firmware template {template_key} is not supported.")

    client_row = _firmware_action_client_row(selector, template_spec)
    if not isinstance(client_row, dict):
        raise RuntimeError(f"ESPHome device {selector} is not available for firmware actions.")
    if not _is_usb_recovery_selector(selector):
        matched_template_key = _matched_template_key(selector, client_row)
        if matched_template_key and _lower(matched_template_key) != _lower(template_key):
            matched_spec = _template_spec_by_key(matched_template_key) or {}
            matched_label = _text(matched_spec.get("label")) or matched_template_key
            selected_label = _text(template_spec.get("label")) or template_key
            raise RuntimeError(
                f"{selector} looks like a {matched_label} target, not {selected_label}. "
                f"Select the {matched_label} firmware template before building or flashing."
            )
    if (
        action_name in {"voice_firmware_flash", "voice_firmware_flash_start"}
        and not bool(client_row.get("connected"))
    ):
        raise RuntimeError(f"ESPHome device {selector} is offline. Use Browser USB Flash to recover it from this browser.")

    force_remote_refresh = action_name in {
        "voice_firmware_build",
        "voice_firmware_browser_build",
        "voice_firmware_flash",
        "voice_firmware_flash_start",
    }
    context = _build_device_context(
        selector,
        client_row,
        template_spec,
        force_remote_refresh=force_remote_refresh,
    )
    if not isinstance(context, dict):
        raise RuntimeError(f"Connected ESPHome device {selector} is not available for firmware actions.")

    values = esphome_runtime.payload_values(body)
    profile_values = _normalize_profile_values(context, values)

    if action_name == "voice_firmware_save":
        _profile_save(_text(context.get("template_key")), _text(context.get("selector")) or selector, profile_values)
        return {
            "ok": True,
            "action": action_name,
            "selector": selector,
            "template_key": context.get("template_key"),
            "message": f"Saved firmware substitutions for {context.get('display_name') or selector}.",
        }

    _validate_profile_values(context, profile_values)
    _profile_save(_text(context.get("template_key")), _text(context.get("selector")) or selector, profile_values)
    cli_status = esphome_cli_status(force=True)
    if not bool(cli_status.get("available")):
        raise RuntimeError(_text(cli_status.get("detail")) or "ESPHome CLI is unavailable.")

    if action_name == "voice_firmware_flash_start":
        result = _start_flash_session(
            context,
            profile_values,
            cli_status,
            follow_logs=_as_bool(body.get("follow_logs"), True),
        )
        result["action"] = action_name
        return result

    if action_name == "voice_firmware_browser_build":
        result = _start_browser_build_session(context, profile_values, cli_status)
        result["action"] = action_name
        return result

    config_path = _prepare_config_path(context, profile_values)
    host = _text(context.get("host"))
    argv = list(cli_status.get("argv") or [])
    if not argv:
        raise RuntimeError("ESPHome CLI runner is not configured.")

    if action_name == "voice_firmware_build":
        command = [*argv, "compile", str(config_path)]
    else:
        command = [*argv, "run", str(config_path), "--no-logs", "--device", host or "OTA"]

    proc = _run_esphome_command(
        command,
        cwd=_text(cli_status.get("cwd")),
        env=_runner_env(cli_status),
    )
    summary = _summarize_process_output(proc.stdout, proc.stderr)
    if proc.returncode != 0:
        verb = "flash" if action_name == "voice_firmware_flash" else "build"
        raise RuntimeError(
            f"ESPHome {verb} failed for {context.get('display_name') or selector}.\n\n{summary or 'No CLI output was captured.'}"
        )

    if action_name == "voice_firmware_flash":
        _save_recorded_firmware_version(
            selector,
            context.get("template_key"),
            context.get("firmware_version"),
            display_name=context.get("display_name"),
            source="ota_flash",
        )
        message = f"Built and flashed {context.get('display_name') or selector}."
    else:
        message = f"Built firmware for {context.get('display_name') or selector}."
    return {
        "ok": True,
        "action": action_name,
        "selector": selector,
        "template_key": context.get("template_key"),
        "config_path": str(config_path),
        "command": command,
        "message": message,
        "output_tail": summary,
    }
