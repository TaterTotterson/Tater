from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import math
import re
import secrets
from typing import Any, Dict, List
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from helpers import redis_client

from . import wake_word_catalog

SETTINGS_HASH_KEY = "voice_native_satellite_live_settings"
DEVICE_SETTINGS_HASH_PREFIX = f"{SETTINGS_HASH_KEY}:device:"
WAKE_FAMILY_SETTINGS_HASH_PREFIX = f"{SETTINGS_HASH_KEY}:wake-family:"
GLOBAL_VOICE_SETTINGS_HASH_KEY = "voice_core_settings"
GLOBAL_WAKE_VERIFIER_MODE_KEY = "VOICE_WAKE_VERIFIER_MODE"
GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY = "_global_satellite_voice_settings_v1"
WAKE_FAMILY_SETTINGS_MIGRATION_KEY = "_wake_family_settings_v1"
UNIFIED_WAKE_SETTINGS_MIGRATION_KEY = "_unified_wake_settings_v1"
WAKE_PROFILE_FETCH_TIMEOUT_S = 4.0

DEFAULTS: Dict[str, Any] = {
    "wake_engine": "micro_wake_word",
    "wake_mww_enabled": True,
    "wake_oww_enabled": True,
    "wake_nww_enabled": False,
    "wake_word": "hey_tater",
    "wake_word_url": "",
    "wake_model_revision": "",
    "wake_profile_key": "hey_tater",
    "wake_profile_name": "Hey Tater",
    "wake_profile_source_url": "",
    "wake_profile_model_url": "",
    "wake_profile_threshold": 0.98,
    "wake_profile_sliding_window": 5,
    "wake_profile_close_miss_threshold": 0.81,
    "wake_profile_error": "",
    "oww_wake_word": "hey_tater",
    "oww_wake_word_url": "",
    "oww_model_revision": "",
    "oww_profile_name": "Hey Tater",
    "oww_profile_threshold": 0.99,
    "oww_profile_patience": 3,
    "oww_profile_confirmation_threshold": 0.92,
    "oww_profile_confirmation_patience": 3,
    "oww_profile_error": "",
    "nww_architecture": "cnn",
    "nww_model_revision": "",
    "nww_available_architectures": "",
    "nww_profile_threshold": 0.99,
    "nww_profile_patience": 2,
    "nww_cnn_profile_threshold": 0.99,
    "nww_cnn_profile_patience": 2,
    "nww_gru_profile_threshold": 0.98,
    "nww_gru_profile_patience": 2,
    "wake_sensitivity": "normal",
    "wake_environment": "balanced",
    "mww_tuning_override_enabled": False,
    "mww_threshold_override": 0.98,
    "mww_sliding_window_override": 5,
    "oww_tuning_override_enabled": False,
    "oww_threshold_override": 0.74,
    "oww_patience_override": 3,
    "nww_tuning_override_enabled": False,
    "nww_threshold_override": 0.99,
    "nww_patience_override": 2,
    "wake_tuning_override_enabled": False,
    "wake_threshold_override": 0.98,
    "wake_sliding_window_override": 5,
    "wake_close_miss_threshold_override": 0.81,
    "wake_tuning_overrides": "{}",
    "wake_threshold": 0.98,
    "wake_sliding_window": 5,
    "capture_wake_audio": False,
    "capture_close_misses": False,
    "close_miss_threshold": 0.81,
    "trainer_app_url": "http://trainer.local:8789",
    "wake_verifier_mode": "off",
    "wake_verifier_phrase": "",
    "wake_verifier_threshold": 0.85,
    "wake_verifier_window_ms": 1000,
    "wake_verifier_timeout_ms": 500,
    "wake_sound_enabled": False,
    "wake_sound": "no_sound",
    "wake_sound_url": "",
    "aec_enabled": False,
    "aec_strength_percent": 70,
    "aec_delay_ms": 85,
    "continued_chat": True,
    "barge_in_enabled": False,
    "volume_percent": 80,
    "eq_bands": [0.0] * 8,
    "eq_loudness": False,
    "bass_guard_enabled": True,
    "bass_guard_db": -30.0,
    "limiter_enabled": True,
    "limiter_threshold_db": -1.0,
    "limiter_release_ms": 150.0,
    "audio_output_mode": "auto",
    "screen_brightness": 80,
    "screen_auto_brightness_enabled": False,
    "screen_auto_brightness_minimum": 12,
    "screen_night_mode_enabled": False,
    "screen_night_brightness": 10,
    "screen_night_start": "22:00",
    "screen_night_end": "07:00",
    "screen_night_display": "red_clock",
    "display_theme": "tater",
    "display_wallpaper_interval_minutes": 1440,
    "display_edge_glow": True,
    "led_brightness": 80,
    "led_color": "#ff5a1f",
    "led_listening_animation": "directional",
    "led_thinking_animation": "sparkle",
    "led_tool_call_animation": "ping_pong",
    "led_replying_animation": "audio_glow",
    "led_music_animation": "music_bars",
    "logging_level": "info",
    "ssh_enabled": False,
}
FIRMWARE_SETTING_KEYS = (
    "wake_engine",
    "wake_mww_enabled",
    "wake_oww_enabled",
    "wake_nww_enabled",
    "wake_word",
    "wake_word_url",
    "wake_model_revision",
    "oww_wake_word",
    "oww_wake_word_url",
    "oww_model_revision",
    "nww_wake_word",
    "nww_wake_word_url",
    "nww_model_revision",
    "nww_architecture",
    "wake_sensitivity",
    "wake_environment",
    "wake_threshold",
    "wake_sliding_window",
    "oww_threshold",
    "oww_patience",
    "nww_threshold",
    "nww_patience",
    "capture_wake_audio",
    "capture_close_misses",
    "close_miss_threshold",
    "trainer_app_url",
    "wake_verifier_mode",
    "wake_verifier_window_ms",
    "wake_verifier_timeout_ms",
    "wake_sound_enabled",
    "wake_sound",
    "wake_sound_url",
    "continued_chat",
    "barge_in_enabled",
    "volume_percent",
    "audio_output_mode",
    "screen_brightness",
    "screen_auto_brightness_enabled",
    "screen_auto_brightness_minimum",
    "screen_night_mode_enabled",
    "screen_night_brightness",
    "screen_night_start",
    "screen_night_end",
    "screen_night_display",
    "display_theme",
    "display_wallpaper_interval_minutes",
    "display_edge_glow",
    "led_brightness",
    "led_color",
    "led_listening_animation",
    "led_thinking_animation",
    "led_tool_call_animation",
    "led_replying_animation",
    "led_music_animation",
    "logging_level",
    "ssh_enabled",
)

WAKE_ENGINES = {"off", "button", "micro_wake_word", "server"}
WAKE_DETECTOR_MODES = {"mww", "oww", "nww", "mww_oww", "mww_nww"}
WAKE_FAMILIES = {"mww", "echo"}
WAKE_VERIFIER_MODES = {"off", "observe", "enforce"}
LOGGING_LEVELS = {"error", "warning", "info", "debug"}
SSH_PASSWORD_PATTERN = re.compile(r"^[A-Za-z0-9._@%+=-]{12,64}$")
SSH_PASSWORD_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"
DISPLAY_THEMES = (
    {
        "value": "tater",
        "label": "Tater Harvest",
        "description": "The original warm Tater palette.",
        "colors": ["#ff8430", "#34e2b7", "#9a70ff", "#ffd05c"],
    },
    {
        "value": "ocean",
        "label": "Ocean Current",
        "description": "Cool cyan, seafoam, and deep blue.",
        "colors": ["#3ecbff", "#49e6c2", "#6686ff", "#9cf4ff"],
    },
    {
        "value": "violet",
        "label": "Violet Bloom",
        "description": "Rich violet with lavender and rose accents.",
        "colors": ["#b07cff", "#ef8dff", "#758cff", "#ffb5dc"],
    },
    {
        "value": "forest",
        "label": "Forest Moss",
        "description": "Natural greens with mint and golden light.",
        "colors": ["#75d66e", "#55e6b7", "#d4b95f", "#c9f27c"],
    },
    {
        "value": "sunset",
        "label": "Sunset Rose",
        "description": "Coral, amber, and dusky magenta.",
        "colors": ["#ff6f61", "#ffad55", "#c674ff", "#ffd06a"],
    },
)
DISPLAY_THEME_VALUES = {str(row["value"]) for row in DISPLAY_THEMES}
DISPLAY_WALLPAPER_INTERVALS = (
    {"value": 0, "label": "Fixed (first photo)"},
    {"value": 60, "label": "Every hour"},
    {"value": 360, "label": "Every 6 hours"},
    {"value": 720, "label": "Every 12 hours"},
    {"value": 1440, "label": "Once a day"},
)
DISPLAY_WALLPAPER_INTERVAL_VALUES = {
    int(row["value"])
    for row in DISPLAY_WALLPAPER_INTERVALS
}
GLOBAL_SATELLITE_CONTROL_KEYS = (
    "wake_engine",
    "wake_mww_enabled",
    "wake_oww_enabled",
    "wake_nww_enabled",
    "wake_word",
    "wake_word_url",
    "oww_wake_word",
    "oww_wake_word_url",
    "capture_wake_audio",
    "capture_close_misses",
    "trainer_app_url",
    "wake_sound_enabled",
    "wake_sound",
    "wake_sound_url",
    "continued_chat",
    "barge_in_enabled",
)
GLOBAL_WAKE_PROFILE_KEYS = (
    "wake_profile_key",
    "wake_profile_name",
    "wake_profile_source_url",
    "wake_profile_model_url",
    "wake_profile_threshold",
    "wake_profile_sliding_window",
    "wake_profile_close_miss_threshold",
    "wake_profile_error",
    "wake_model_revision",
    "oww_model_revision",
    "nww_model_revision",
    "nww_architecture",
    "nww_available_architectures",
    "nww_profile_threshold",
    "nww_profile_patience",
    "nww_cnn_profile_threshold",
    "nww_cnn_profile_patience",
    "nww_gru_profile_threshold",
    "nww_gru_profile_patience",
    "bundle_has_mww",
    "bundle_has_oww",
    "wake_bundle_embedded_micro",
    "oww_profile_name",
    "oww_profile_threshold",
    "oww_profile_patience",
    "oww_profile_confirmation_threshold",
    "oww_profile_confirmation_patience",
    "oww_profile_error",
)
GLOBAL_WAKE_TUNING_KEYS = {
    "mww_tuning_override_enabled",
    "mww_threshold_override",
    "mww_sliding_window_override",
    "oww_tuning_override_enabled",
    "oww_threshold_override",
    "oww_patience_override",
    "nww_tuning_override_enabled",
    "nww_threshold_override",
    "nww_patience_override",
}
LEGACY_DEVICE_WAKE_OVERRIDE_KEYS = {
    "wake_tuning_override_enabled",
    "wake_threshold_override",
    "wake_sliding_window_override",
    "wake_close_miss_threshold_override",
    "wake_tuning_overrides",
}
GLOBAL_SATELLITE_PERSISTED_KEYS = (
    set(GLOBAL_SATELLITE_CONTROL_KEYS)
    | set(GLOBAL_WAKE_PROFILE_KEYS)
    | GLOBAL_WAKE_TUNING_KEYS
    | LEGACY_DEVICE_WAKE_OVERRIDE_KEYS
)
WAKE_FAMILY_PERSISTED_KEYS = {
    "wake_engine",
    "wake_detector_mode",
    "wake_mww_enabled",
    "wake_oww_enabled",
    "wake_nww_enabled",
    "wake_word",
    "wake_word_url",
    "oww_wake_word",
    "oww_wake_word_url",
    "nww_architecture",
} | set(GLOBAL_WAKE_PROFILE_KEYS) | GLOBAL_WAKE_TUNING_KEYS
SHARED_SATELLITE_CONTROL_KEYS = {
    "capture_wake_audio",
    "capture_close_misses",
    "trainer_app_url",
    "wake_sound_enabled",
    "wake_sound",
    "wake_sound_url",
    "continued_chat",
    "barge_in_enabled",
}
GLOBAL_SATELLITE_DEVICE_FIELD_KEYS = set(GLOBAL_SATELLITE_CONTROL_KEYS) | {
    "wake_section",
    "wake_detector_mode",
    "wake_word_catalog_url",
    "oww_wake_word_catalog_url",
    "wake_profile_name",
    "oww_profile_name",
    "training_section",
    "playback_section",
    "model_override_section",
    *GLOBAL_WAKE_TUNING_KEYS,
    *LEGACY_DEVICE_WAKE_OVERRIDE_KEYS,
}
WAKE_SENSITIVITY_ROWS = [
    ("conservative", "Conservative"),
    ("normal", "Normal"),
    ("high", "High"),
]
WAKE_SENSITIVITY_ADJUSTMENTS = {
    "conservative": 0.02,
    "normal": 0.0,
    "high": -0.05,
}
WAKE_SENSITIVITY_ALIASES = {
    "slightly sensitive": "conservative",
    "moderately sensitive": "normal",
    "very sensitive": "high",
    "low": "conservative",
    "medium": "normal",
    "normal": "normal",
    "high": "high",
    "conservative": "conservative",
    "default": "normal",
    **{value: value for value, _label in WAKE_SENSITIVITY_ROWS},
    **{label.lower(): value for value, label in WAKE_SENSITIVITY_ROWS},
}
WAKE_ENVIRONMENT_ROWS = [
    ("balanced", "Balanced"),
    ("tv_nearby", "TV Nearby"),
    ("strict", "Strict"),
    ("far_field", "Far Field / Quiet Room"),
]
WAKE_ENVIRONMENT_ALIASES = {
    "": "balanced",
    "default": "balanced",
    "balanced": "balanced",
    "strict": "strict",
    "tv": "tv_nearby",
    "near tv": "tv_nearby",
    "next to tv": "tv_nearby",
    "tv nearby": "tv_nearby",
    "tv_nearby": "tv_nearby",
    "very sensitive": "far_field",
    "sensitive": "far_field",
    "quiet": "far_field",
    "quiet room": "far_field",
    "far": "far_field",
    "far field": "far_field",
    "far_field": "far_field",
    **{value: value for value, _label in WAKE_ENVIRONMENT_ROWS},
    **{label.lower(): value for value, label in WAKE_ENVIRONMENT_ROWS},
}
WAKE_WORD_ROWS = [
    ("hey_tater", "Hey Tater"),
    ("custom_url", "Custom URL"),
]
WAKE_WORD_SOURCE_ROWS = [
    ("hey_tater", "Built-in Hey Tater"),
    ("catalog", "Tater Wake Word Catalog"),
    ("custom_url", "Custom URL"),
]
BUILTIN_WAKE_PROFILES: Dict[str, Dict[str, Any]] = {
    "hey_tater": {
        "wake_profile_key": "hey_tater",
        "wake_profile_name": "Hey Tater",
        "wake_profile_source_url": "",
        "wake_profile_model_url": "",
        "wake_profile_threshold": 0.99,
        "wake_profile_sliding_window": 5,
        "wake_profile_close_miss_threshold": 0.82,
        "wake_profile_error": "",
    }
}
BUILTIN_OWW_PROFILES: Dict[str, Dict[str, Any]] = {
    "hey_tater": {
        "oww_profile_name": "Hey Tater",
        "oww_profile_threshold": 0.74,
        "oww_profile_patience": 3,
        "oww_profile_confirmation_threshold": 0.96,
        "oww_profile_confirmation_patience": 3,
        "oww_profile_error": "",
        "oww_model_revision": "",
        "nww_model_revision": "",
        "nww_available_architectures": "cnn,gru",
        "nww_profile_threshold": 0.99,
        "nww_profile_patience": 2,
        "nww_cnn_profile_threshold": 0.99,
        "nww_cnn_profile_patience": 2,
        "nww_gru_profile_threshold": 0.98,
        "nww_gru_profile_patience": 2,
        "bundle_has_mww": True,
        "bundle_has_oww": True,
        "wake_bundle_embedded_micro": True,
    }
}
WAKE_WORDS = {row[0]: {"label": row[1]} for row in WAKE_WORD_ROWS}
WAKE_WORD_ALIASES = {
    "": "hey_tater",
    "default": "hey_tater",
    "custom": "custom_url",
    "url": "custom_url",
    **{value: value for value, _label in WAKE_WORD_ROWS},
    **{value.replace("_", " "): value for value, _label in WAKE_WORD_ROWS},
    **{label.lower(): value for value, label in WAKE_WORD_ROWS},
}
WAKE_SOUND_ROWS = [
    ("default", "Default", "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/wake_word_triggered.wav"),
    ("no_sound", "No Sound", ""),
    ("blip2", "Blip2", "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/blip2.wav"),
    (
        "message-notification-4",
        "Message Notification 4",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/message-notification-4.wav",
    ),
    (
        "notification-ding",
        "Notification Ding",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/notification-ding.wav",
    ),
    (
        "notification-squeak",
        "Notification Squeak",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/notification-squeak.wav",
    ),
    ("phone-chime", "Phone Chime", "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/phone-chime.wav"),
    ("pop-up-sound", "Pop Up Sound", "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/pop-up-sound.wav"),
    (
        "short-definite-fart",
        "Short Definite Fart",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/short-definite-fart.wav",
    ),
    (
        "star_treck_communications_start_transmission",
        "Star Treck Communications Start Transmission",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/star_treck_communications_start_transmission.wav",
    ),
    (
        "star_treck_computer_work_beep",
        "Star Treck Computer Work Beep",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/star_treck_computer_work_beep.wav",
    ),
    (
        "tater_notify_digital_blip",
        "Tater Notify Digital Blip",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/tater_notify_digital_blip.wav",
    ),
    (
        "turning-off-microphone-percussion-1",
        "Turning Off Microphone Percussion 1",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/turning-off-microphone-percussion-1.wav",
    ),
    (
        "wake_word_triggered",
        "Wake Word Triggered",
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/wake_word_triggered.wav",
    ),
    ("waterdrop", "Waterdrop", "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wakeSounds/waterdrop.wav"),
    ("custom", "Custom URL", ""),
]
WAKE_SOUNDS = {row[0]: {"label": row[1], "url": row[2]} for row in WAKE_SOUND_ROWS}
WAKE_SOUND_ALIASES = {
    "": "no_sound",
    "no sound": "no_sound",
    "none": "no_sound",
    "off": "no_sound",
    **{value: value for value, _label, _url in WAKE_SOUND_ROWS},
    **{label.lower(): value for value, label, _url in WAKE_SOUND_ROWS},
}
LED_ANIMATION_ROWS = [
    ("directional", "Directional Listening"),
    ("sparkle", "Sparkle"),
    ("ping_pong", "Ping Pong"),
    ("audio_glow", "Audio Glow"),
    ("voice_ring", "Voice Ring"),
    ("spinner", "Spinner"),
    ("orbit", "Orbit"),
    ("pulse", "Pulse"),
    ("breathe", "Breathe"),
    ("comet", "Comet"),
    ("dual_comet", "Dual Comet"),
    ("scanner", "Scanner"),
    ("ripple", "Ripple"),
    ("heartbeat", "Heartbeat"),
    ("theater", "Theater Chase"),
    ("wave", "Wave"),
    ("shimmer", "Shimmer"),
    ("twinkle", "Twinkle"),
    ("equalizer", "Equalizer"),
    ("solid", "Solid"),
]


def _animation_rows(*preferred: str) -> List[tuple[str, str]]:
    labels = {value: label for value, label in LED_ANIMATION_ROWS}
    used = set()
    rows: List[tuple[str, str]] = []
    for value in preferred:
        if value in labels and value not in used:
            rows.append((value, labels[value]))
            used.add(value)
    for value, label in LED_ANIMATION_ROWS:
        if value not in used:
            rows.append((value, label))
            used.add(value)
    return rows


def _with_no_animation(rows: List[tuple[str, str]]) -> List[tuple[str, str]]:
    return [("off", "No Animation"), *rows]


LED_LISTENING_ANIMATIONS = _with_no_animation([
    row for row in _animation_rows("directional", "pulse", "spinner", "breathe")
    if row[0] != "audio_glow"
])
LED_THINKING_ANIMATIONS = _with_no_animation([
    row for row in _animation_rows("sparkle", "shimmer", "twinkle", "breathe")
    if row[0] != "audio_glow"
])
LED_TOOL_CALL_ANIMATIONS = _with_no_animation([
    row for row in _animation_rows("ping_pong", "scanner", "orbit", "comet")
    if row[0] != "audio_glow"
])
LED_REPLYING_ANIMATIONS = _with_no_animation(
    _animation_rows("audio_glow", "voice_ring", "wave", "ripple", "equalizer")
)
LED_MUSIC_ANIMATIONS = [
    ("off", "No Animation"),
    ("audio_glow", "Audio Glow"),
    ("music_pulse", "Beat Pulse"),
    ("music_bars", "Level Bars"),
    ("music_orbit", "Reactive Orbit"),
    ("music_wave", "Reactive Wave"),
]
LED_MUSIC_ANIMATION_VALUES = {value for value, _label in LED_MUSIC_ANIMATIONS}
LED_ANIMATION_VALUES = {
    value
    for rows in (
        LED_LISTENING_ANIMATIONS,
        LED_THINKING_ANIMATIONS,
        LED_TOOL_CALL_ANIMATIONS,
        LED_REPLYING_ANIMATIONS,
    )
    for value, _label in rows
}
S420_LED_DEFAULTS = {
    "led_listening_animation": "pulse",
    "led_thinking_animation": "breathe",
    "led_tool_call_animation": "heartbeat",
    "led_replying_animation": "audio_glow",
}
S420_LED_ANIMATIONS = [
    ("off", "No Animation"),
    ("audio_glow", "Audio Glow"),
    ("pulse", "Tater Pulse"),
    ("breathe", "Tater Breathe"),
    ("heartbeat", "Tater Heartbeat"),
    ("solid", "Steady Tater Glow"),
]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _is_url(value: Any) -> bool:
    token = _lower(value)
    return token.startswith("http://") or token.startswith("https://")


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except Exception:
        return "{}"


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return default


def _wake_overrides(value: Any) -> Dict[str, Dict[str, Any]]:
    parsed = _json_loads(value, {})
    if not isinstance(parsed, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, row in parsed.items():
        token = _text(key)
        if token and isinstance(row, dict):
            out[token] = dict(row)
    return out


def _wake_profile_key(wake_word: str, wake_word_url: str = "") -> str:
    if wake_word == "custom_url":
        return f"custom:{wake_word_url}" if wake_word_url else "custom_url"
    return wake_word or str(DEFAULTS["wake_word"])


def _wake_profile_from_source(source: Dict[str, Any], wake_word: str, wake_word_url: str) -> Dict[str, Any]:
    builtin = BUILTIN_WAKE_PROFILES.get(wake_word)
    if builtin:
        return {**builtin, "wake_model_revision": ""}

    key = _wake_profile_key(wake_word, wake_word_url)
    if wake_word == "custom_url" and wake_word_url and _text(source.get("wake_profile_source_url")) == wake_word_url:
        return {
            "wake_profile_key": key,
            "wake_profile_name": _text(source.get("wake_profile_name")) or "Custom Wake Word",
            "wake_profile_source_url": wake_word_url,
            "wake_profile_model_url": _text(source.get("wake_profile_model_url")),
            "wake_profile_threshold": round(
                _as_float(source.get("wake_profile_threshold"), float(DEFAULTS["wake_profile_threshold"]), minimum=0.01, maximum=0.99),
                3,
            ),
            "wake_profile_sliding_window": _as_int(
                source.get("wake_profile_sliding_window"),
                int(DEFAULTS["wake_profile_sliding_window"]),
                minimum=1,
                maximum=10,
            ),
            "wake_profile_close_miss_threshold": round(
                _as_float(
                    source.get("wake_profile_close_miss_threshold"),
                    float(DEFAULTS["wake_profile_close_miss_threshold"]),
                    minimum=0.01,
                    maximum=0.99,
                ),
                3,
            ),
            "wake_profile_error": _text(source.get("wake_profile_error")),
            "wake_model_revision": _text(source.get("wake_model_revision")),
        }

    return {
        "wake_profile_key": key,
        "wake_profile_name": "Custom Wake Word" if wake_word == "custom_url" else _text(wake_word).replace("_", " ").title(),
        "wake_profile_source_url": wake_word_url if wake_word == "custom_url" else "",
        "wake_profile_model_url": "",
        "wake_profile_threshold": float(DEFAULTS["wake_profile_threshold"]),
        "wake_profile_sliding_window": int(DEFAULTS["wake_profile_sliding_window"]),
        "wake_profile_close_miss_threshold": float(DEFAULTS["wake_profile_close_miss_threshold"]),
        "wake_profile_error": "",
        "wake_model_revision": "",
    }


def _fetch_wake_profile_json(wake_word_url: str) -> Dict[str, Any]:
    url = _text(wake_word_url)
    if not _is_url(url):
        return {
            "wake_profile_key": _wake_profile_key("custom_url", url),
            "wake_profile_name": "Custom Wake Word",
            "wake_profile_source_url": url,
            "wake_profile_model_url": "",
            "wake_profile_threshold": float(DEFAULTS["wake_profile_threshold"]),
            "wake_profile_sliding_window": int(DEFAULTS["wake_profile_sliding_window"]),
            "wake_profile_close_miss_threshold": float(DEFAULTS["wake_profile_close_miss_threshold"]),
            "wake_profile_error": "Wake word JSON URL is missing or invalid.",
        }

    try:
        request = Request(url, headers={"User-Agent": "Tater-Native-Satellite/0.1"})
        with urlopen(request, timeout=WAKE_PROFILE_FETCH_TIMEOUT_S) as response:
            raw = response.read(64 * 1024)
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        return {
            "wake_profile_key": _wake_profile_key("custom_url", url),
            "wake_profile_name": "Custom Wake Word",
            "wake_profile_source_url": url,
            "wake_profile_model_url": "",
            "wake_profile_threshold": float(DEFAULTS["wake_profile_threshold"]),
            "wake_profile_sliding_window": int(DEFAULTS["wake_profile_sliding_window"]),
            "wake_profile_close_miss_threshold": float(DEFAULTS["wake_profile_close_miss_threshold"]),
            "wake_profile_error": f"Could not read wake JSON: {exc}",
        }

    if not isinstance(payload, dict):
        return {
            "wake_profile_key": _wake_profile_key("custom_url", url),
            "wake_profile_name": "Custom Wake Word",
            "wake_profile_source_url": url,
            "wake_profile_model_url": "",
            "wake_profile_threshold": float(DEFAULTS["wake_profile_threshold"]),
            "wake_profile_sliding_window": int(DEFAULTS["wake_profile_sliding_window"]),
            "wake_profile_close_miss_threshold": float(DEFAULTS["wake_profile_close_miss_threshold"]),
            "wake_profile_error": "Wake JSON did not contain an object.",
        }

    wake_name = _text(payload.get("wake_word") or payload.get("label") or payload.get("name")) or "Custom Wake Word"
    model_ref = _text(payload.get("model") or payload.get("model_url"))
    micro = payload.get("micro") if isinstance(payload.get("micro"), dict) else {}
    native = payload.get("tater_native") if isinstance(payload.get("tater_native"), dict) else {}
    threshold = round(
        _as_float(
            native.get("wake_threshold", micro.get("probability_cutoff")),
            float(DEFAULTS["wake_profile_threshold"]),
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    sliding_window = _as_int(
        native.get("wake_sliding_window", micro.get("sliding_window_size")),
        int(DEFAULTS["wake_profile_sliding_window"]),
        minimum=1,
        maximum=10,
    )
    close_miss_threshold = round(
        _as_float(
            native.get("close_miss_threshold"),
            float(DEFAULTS["wake_profile_close_miss_threshold"]),
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    return {
        "wake_profile_key": _wake_profile_key("custom_url", url),
        "wake_profile_name": wake_name,
        "wake_profile_source_url": url,
        "wake_profile_model_url": urljoin(url, model_ref) if model_ref else "",
        "wake_profile_threshold": threshold,
        "wake_profile_sliding_window": sliding_window,
        "wake_profile_close_miss_threshold": close_miss_threshold,
        "wake_profile_error": "" if model_ref else "Wake JSON did not include a model field.",
        "wake_model_revision": hashlib.sha256(raw).hexdigest(),
    }


def _oww_profile_from_source(source: Dict[str, Any], wake_word: str, bundle_url: str) -> Dict[str, Any]:
    builtin = BUILTIN_OWW_PROFILES.get(wake_word)
    if builtin:
        return dict(builtin)
    if wake_word == "custom_url" and bundle_url and _text(source.get("oww_wake_word_url")) == bundle_url:
        return {
            "oww_profile_name": _text(source.get("oww_profile_name")) or "Custom Wake Word",
            "oww_profile_threshold": round(
                _as_float(source.get("oww_profile_threshold"), float(DEFAULTS["oww_profile_threshold"]), minimum=0.01, maximum=1.0),
                3,
            ),
            "oww_profile_patience": _as_int(
                source.get("oww_profile_patience"),
                int(DEFAULTS["oww_profile_patience"]),
                minimum=1,
                maximum=20,
            ),
            "oww_profile_confirmation_threshold": round(
                _as_float(
                    source.get("oww_profile_confirmation_threshold"),
                    float(DEFAULTS["oww_profile_confirmation_threshold"]),
                    minimum=0.01,
                    maximum=1.0,
                ),
                3,
            ),
            "oww_profile_confirmation_patience": _as_int(
                source.get("oww_profile_confirmation_patience"),
                int(DEFAULTS["oww_profile_confirmation_patience"]),
                minimum=1,
                maximum=20,
            ),
            "oww_profile_error": _text(source.get("oww_profile_error")),
            "oww_model_revision": _text(source.get("oww_model_revision")),
            "nww_model_revision": _text(source.get("nww_model_revision")),
            "nww_available_architectures": _text(source.get("nww_available_architectures")),
            "nww_profile_threshold": round(
                _as_float(source.get("nww_profile_threshold"), float(DEFAULTS["nww_profile_threshold"]), minimum=0.01, maximum=1.0),
                3,
            ),
            "nww_profile_patience": _as_int(
                source.get("nww_profile_patience"), int(DEFAULTS["nww_profile_patience"]), minimum=1, maximum=20,
            ),
            "nww_cnn_profile_threshold": round(
                _as_float(source.get("nww_cnn_profile_threshold"), float(DEFAULTS["nww_cnn_profile_threshold"]), minimum=0.01, maximum=1.0),
                3,
            ),
            "nww_cnn_profile_patience": _as_int(
                source.get("nww_cnn_profile_patience"), int(DEFAULTS["nww_cnn_profile_patience"]), minimum=1, maximum=20,
            ),
            "nww_gru_profile_threshold": round(
                _as_float(source.get("nww_gru_profile_threshold"), float(DEFAULTS["nww_gru_profile_threshold"]), minimum=0.01, maximum=1.0),
                3,
            ),
            "nww_gru_profile_patience": _as_int(
                source.get("nww_gru_profile_patience"), int(DEFAULTS["nww_gru_profile_patience"]), minimum=1, maximum=20,
            ),
            "bundle_has_mww": _as_bool(source.get("bundle_has_mww"), False),
            "bundle_has_oww": _as_bool(source.get("bundle_has_oww"), False),
            "wake_bundle_embedded_micro": _as_bool(source.get("wake_bundle_embedded_micro"), False),
        }
    return {
        "oww_profile_name": "Custom Wake Word" if wake_word == "custom_url" else "Hey Tater",
        "oww_profile_threshold": float(DEFAULTS["oww_profile_threshold"]),
        "oww_profile_patience": int(DEFAULTS["oww_profile_patience"]),
        "oww_profile_confirmation_threshold": float(DEFAULTS["oww_profile_confirmation_threshold"]),
        "oww_profile_confirmation_patience": int(DEFAULTS["oww_profile_confirmation_patience"]),
        "oww_profile_error": "",
        "oww_model_revision": "",
        "nww_model_revision": "",
        "nww_available_architectures": "",
        "nww_profile_threshold": float(DEFAULTS["nww_profile_threshold"]),
        "nww_profile_patience": int(DEFAULTS["nww_profile_patience"]),
        "nww_cnn_profile_threshold": float(DEFAULTS["nww_cnn_profile_threshold"]),
        "nww_cnn_profile_patience": int(DEFAULTS["nww_cnn_profile_patience"]),
        "nww_gru_profile_threshold": float(DEFAULTS["nww_gru_profile_threshold"]),
        "nww_gru_profile_patience": int(DEFAULTS["nww_gru_profile_patience"]),
        "bundle_has_mww": wake_word == "hey_tater",
        "bundle_has_oww": wake_word == "hey_tater",
        "wake_bundle_embedded_micro": False,
    }


def _fetch_oww_profile_json(bundle_url: str) -> Dict[str, Any]:
    url = _text(bundle_url)
    fallback = {
        "oww_profile_name": "Custom Wake Word",
        "oww_profile_threshold": float(DEFAULTS["oww_profile_threshold"]),
        "oww_profile_patience": int(DEFAULTS["oww_profile_patience"]),
        "oww_profile_confirmation_threshold": float(DEFAULTS["oww_profile_confirmation_threshold"]),
        "oww_profile_confirmation_patience": int(DEFAULTS["oww_profile_confirmation_patience"]),
        "oww_model_revision": "",
        "nww_model_revision": "",
        "nww_available_architectures": "",
        "nww_profile_threshold": float(DEFAULTS["nww_profile_threshold"]),
        "nww_profile_patience": int(DEFAULTS["nww_profile_patience"]),
        "nww_cnn_profile_threshold": float(DEFAULTS["nww_cnn_profile_threshold"]),
        "nww_cnn_profile_patience": int(DEFAULTS["nww_cnn_profile_patience"]),
        "nww_gru_profile_threshold": float(DEFAULTS["nww_gru_profile_threshold"]),
        "nww_gru_profile_patience": int(DEFAULTS["nww_gru_profile_patience"]),
        "bundle_has_mww": False,
        "bundle_has_oww": False,
        "wake_bundle_embedded_micro": False,
    }
    if not _is_url(url):
        return {**fallback, "oww_profile_error": "Wake bundle URL is missing or invalid."}
    try:
        request = Request(url, headers={"User-Agent": "Tater-Native-Satellite/0.1"})
        with urlopen(request, timeout=WAKE_PROFILE_FETCH_TIMEOUT_S) as response:
            raw = response.read(512 * 1024)
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        return {**fallback, "oww_profile_error": f"Could not read wake bundle: {exc}"}
    if not isinstance(payload, dict):
        return {**fallback, "oww_profile_error": "Wake bundle did not contain an object."}
    micro = payload.get("micro_wake_word") if isinstance(payload.get("micro_wake_word"), dict) else {}
    oww = payload.get("open_wake_word") if isinstance(payload.get("open_wake_word"), dict) else {}
    artifacts = oww.get("artifacts") if isinstance(oww.get("artifacts"), dict) else {}
    onnx_artifacts = [
        row
        for row in artifacts.values()
        if isinstance(row, dict) and _text(row.get("file")).lower().endswith(".onnx")
    ]
    has_onnx = len(onnx_artifacts) == 1
    onnx_sha = _text(onnx_artifacts[0].get("sha256")).lower() if has_onnx else ""
    threshold = round(
        _as_float(oww.get("recommended_threshold"), float(DEFAULTS["oww_profile_threshold"]), minimum=0.01, maximum=1.0),
        3,
    )
    patience = _as_int(
        oww.get("recommended_patience"),
        int(DEFAULTS["oww_profile_patience"]),
        minimum=1,
        maximum=20,
    )
    confirmation_threshold = round(
        _as_float(
            oww.get("recommended_confirmation_threshold"),
            threshold,
            minimum=0.01,
            maximum=1.0,
        ),
        3,
    )
    confirmation_patience = _as_int(
        oww.get("recommended_confirmation_patience"),
        patience,
        minimum=1,
        maximum=20,
    )
    bundle_type = _text(payload.get("type"))
    schema_version = _as_int(payload.get("schema_version"), 0, minimum=0, maximum=99)
    manifest_ref = _text(micro.get("manifest"))
    mww_model_ref = _text(micro.get("model"))
    manifest_sha = _text(micro.get("manifest_sha256")).lower()
    mww_model_sha = _text(micro.get("model_sha256")).lower()
    metadata_sha = _text(oww.get("metadata_sha256")).lower()

    def valid_digest(value: str) -> bool:
        return len(value) == 64 and all(character in "0123456789abcdef" for character in value)

    embedded_micro = schema_version >= 2 and not manifest_ref
    has_micro = (
        mww_model_ref.lower().endswith(".tflite")
        and valid_digest(mww_model_sha)
        and (
            embedded_micro
            or (manifest_ref.lower().endswith(".json") and valid_digest(manifest_sha))
        )
    )
    has_oww = has_onnx and valid_digest(onnx_sha) and (
        not _text(oww.get("metadata")) or valid_digest(metadata_sha)
    )
    nano = payload.get("nano_wake_word") if isinstance(payload.get("nano_wake_word"), dict) else {}
    variants = nano.get("variants") if isinstance(nano.get("variants"), dict) else {}
    nano_architectures: List[str] = []
    nano_profiles: Dict[str, tuple[float, int]] = {}
    for architecture in ("cnn", "gru"):
        variant = variants.get(architecture)
        if not isinstance(variant, dict):
            continue
        model_ref = _text(variant.get("model"))
        model_sha = _text(variant.get("model_sha256")).lower()
        size_value = variant.get("model_size_bytes")
        if not model_ref:
            variant_artifacts = variant.get("artifacts") if isinstance(variant.get("artifacts"), dict) else {}
            variant_model = variant_artifacts.get("onnx")
            if isinstance(variant_model, dict):
                model_ref = _text(variant_model.get("file"))
                model_sha = _text(variant_model.get("sha256")).lower()
                size_value = variant_model.get("size_bytes")
        size = _as_int(size_value, 0, minimum=0, maximum=16 * 1024 * 1024)
        if model_ref.lower().endswith(".onnx") and valid_digest(model_sha) and size > 0:
            nano_architectures.append(architecture)
            nano_profiles[architecture] = (
                round(
                    _as_float(
                        variant.get("recommended_threshold"),
                        float(DEFAULTS[f"nww_{architecture}_profile_threshold"]),
                        minimum=0.01,
                        maximum=1.0,
                    ),
                    3,
                ),
                _as_int(
                    variant.get("recommended_patience"),
                    int(DEFAULTS[f"nww_{architecture}_profile_patience"]),
                    minimum=1,
                    maximum=20,
                ),
            )

    default_nano_architecture = _lower(nano.get("default_architecture"))
    if default_nano_architecture not in nano_profiles:
        default_nano_architecture = nano_architectures[0] if nano_architectures else "cnn"
    default_nano_profile = nano_profiles.get(
        default_nano_architecture,
        (
            float(DEFAULTS["nww_profile_threshold"]),
            int(DEFAULTS["nww_profile_patience"]),
        ),
    )

    native = micro.get("tater_native") if isinstance(micro.get("tater_native"), dict) else {}
    micro_settings = micro.get("micro") if isinstance(micro.get("micro"), dict) else {}
    profile_threshold = round(
        _as_float(
            native.get("wake_threshold", micro_settings.get("probability_cutoff")),
            float(DEFAULTS["wake_profile_threshold"]),
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    profile_window = _as_int(
        native.get("wake_sliding_window", micro_settings.get("sliding_window_size")),
        int(DEFAULTS["wake_profile_sliding_window"]),
        minimum=1,
        maximum=10,
    )
    profile_close_miss = round(
        _as_float(
            native.get("close_miss_threshold"),
            float(DEFAULTS["wake_profile_close_miss_threshold"]),
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    error = ""
    if schema_version not in {1, 2, 3} or bundle_type != "tater_wake_word_bundle" or not has_micro:
        error = "URL is not a complete Tater wake bundle with a verified microWakeWord fallback."
    revision = hashlib.sha256(raw).hexdigest()
    profile_name = _text(payload.get("wake_word") or payload.get("label") or payload.get("key")) or "Custom Wake Word"
    return {
        "oww_profile_name": profile_name,
        "oww_profile_threshold": threshold,
        "oww_profile_patience": patience,
        "oww_profile_confirmation_threshold": confirmation_threshold,
        "oww_profile_confirmation_patience": confirmation_patience,
        "oww_profile_error": error,
        "oww_model_revision": revision,
        "nww_model_revision": revision,
        "nww_available_architectures": ",".join(nano_architectures),
        "nww_profile_threshold": default_nano_profile[0],
        "nww_profile_patience": default_nano_profile[1],
        "nww_cnn_profile_threshold": nano_profiles.get("cnn", (float(DEFAULTS["nww_cnn_profile_threshold"]), 0))[0],
        "nww_cnn_profile_patience": nano_profiles.get("cnn", (0.0, int(DEFAULTS["nww_cnn_profile_patience"])))[1],
        "nww_gru_profile_threshold": nano_profiles.get("gru", (float(DEFAULTS["nww_gru_profile_threshold"]), 0))[0],
        "nww_gru_profile_patience": nano_profiles.get("gru", (0.0, int(DEFAULTS["nww_gru_profile_patience"])))[1],
        "bundle_has_mww": has_micro,
        "bundle_has_oww": has_oww,
        "wake_bundle_embedded_micro": embedded_micro,
        "wake_bundle_url": url,
        "wake_bundle_manifest_url": urljoin(url, manifest_ref) if manifest_ref else "",
        "wake_bundle_model_url": urljoin(url, mww_model_ref) if mww_model_ref else "",
        "wake_bundle_phrase": _text(payload.get("wake_word")),
        "wake_bundle_manifest_sha256": manifest_sha,
        "wake_bundle_model_sha256": mww_model_sha,
        "wake_profile_key": _wake_profile_key("custom_url", url),
        "wake_profile_name": profile_name,
        "wake_profile_source_url": urljoin(url, manifest_ref) if manifest_ref else url,
        "wake_profile_model_url": urljoin(url, mww_model_ref) if mww_model_ref else "",
        "wake_profile_threshold": profile_threshold,
        "wake_profile_sliding_window": profile_window,
        "wake_profile_close_miss_threshold": profile_close_miss,
        "wake_profile_error": error,
        "wake_model_revision": revision,
    }


def _wake_sound_value(value: Any) -> str:
    token = _lower(value).replace("_", "-")
    if token in WAKE_SOUND_ALIASES:
        return WAKE_SOUND_ALIASES[token]
    token = _lower(value)
    return WAKE_SOUND_ALIASES.get(token, str(DEFAULTS["wake_sound"]))


def _wake_word_value(value: Any) -> str:
    if _is_url(value):
        return "custom_url"
    token = _lower(value)
    if token in WAKE_WORD_ALIASES:
        return WAKE_WORD_ALIASES[token]
    token = token.replace("-", "_")
    if token in WAKE_WORD_ALIASES:
        return WAKE_WORD_ALIASES[token]
    return str(DEFAULTS["wake_word"])


def _wake_sensitivity_value(value: Any) -> str:
    token = _lower(value).replace("-", " ").replace("_", " ")
    return WAKE_SENSITIVITY_ALIASES.get(token, str(DEFAULTS["wake_sensitivity"]))


def _wake_environment_value(value: Any) -> str:
    token = _lower(value).replace("-", " ").replace("_", " ")
    return WAKE_ENVIRONMENT_ALIASES.get(token, str(DEFAULTS["wake_environment"]))


def wake_sensitivity_label(value: Any) -> str:
    token = _wake_sensitivity_value(value)
    labels = {key: label for key, label in WAKE_SENSITIVITY_ROWS}
    return labels.get(token, labels[str(DEFAULTS["wake_sensitivity"])])


def wake_environment_label(value: Any) -> str:
    token = _wake_environment_value(value)
    labels = {key: label for key, label in WAKE_ENVIRONMENT_ROWS}
    return labels.get(token, labels[str(DEFAULTS["wake_environment"])])


def _threshold_for_sensitivity(base_threshold: float, wake_sensitivity: str) -> float:
    adjustment = WAKE_SENSITIVITY_ADJUSTMENTS.get(wake_sensitivity, 0.0)
    return round(_as_float(base_threshold + adjustment, base_threshold, minimum=0.01, maximum=0.99), 3)


def _led_color_value(value: Any) -> str:
    token = _text(value).lower()
    if not token:
        token = str(DEFAULTS["led_color"])
    if token.startswith("#"):
        token = token[1:]
    if len(token) == 3 and all(ch in "0123456789abcdef" for ch in token):
        token = "".join(ch + ch for ch in token)
    if len(token) != 6 or any(ch not in "0123456789abcdef" for ch in token):
        token = str(DEFAULTS["led_color"]).lower().lstrip("#")
    return f"#{token}"


def _led_animation_value(value: Any, default_key: str) -> str:
    token = _lower(value).replace("-", "_")
    fallback = str(DEFAULTS.get(default_key) or "pulse")
    return token if token in LED_ANIMATION_VALUES else fallback


def _led_music_animation_value(value: Any) -> str:
    token = _lower(value).replace("-", "_")
    fallback = str(DEFAULTS["led_music_animation"])
    return token if token in LED_MUSIC_ANIMATION_VALUES else fallback


def _time_value(value: Any, fallback: str) -> str:
    token = _text(value)
    parts = token.split(":")
    if len(parts) != 2:
        return fallback
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        return fallback
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return fallback
    return f"{hour:02d}:{minute:02d}"


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _lower(value)
    if not token:
        return bool(default)
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _supports_ssh_management(capabilities: Dict[str, Any] | None = None) -> bool:
    values = capabilities if isinstance(capabilities, dict) else {}
    return _as_bool(values.get("ssh_management"), False) and _as_int(
        values.get("ssh_management_version"),
        0,
        minimum=0,
        maximum=100,
    ) >= 1


def _valid_ssh_password(value: Any) -> bool:
    return bool(SSH_PASSWORD_PATTERN.fullmatch(_text(value)))


def _new_ssh_password() -> str:
    groups = ["".join(secrets.choice(SSH_PASSWORD_ALPHABET) for _ in range(4)) for _ in range(3)]
    return "tater-" + "-".join(groups)


def _ensure_ssh_password(selector: Any) -> str:
    token = _selector_token(selector)
    if not token:
        return ""
    current = _text(_raw_settings(token).get("ssh_password"))
    if _valid_ssh_password(current):
        return current
    password = _new_ssh_password()
    redis_client.hset(settings_hash_key(token), mapping={"ssh_password": password})
    return password


def _as_float(value: Any, default: float, *, minimum: float, maximum: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if out < minimum:
        out = minimum
    if out > maximum:
        out = maximum
    return out


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        out = int(round(float(value)))
    except Exception:
        out = int(default)
    if out < minimum:
        out = minimum
    if out > maximum:
        out = maximum
    return out


def _selector_token(selector: Any = "") -> str:
    return _text(selector)


def settings_hash_key(selector: Any = "") -> str:
    token = _selector_token(selector)
    return f"{DEVICE_SETTINGS_HASH_PREFIX}{token}" if token else SETTINGS_HASH_KEY


def _raw_settings(selector: Any = "") -> Dict[str, Any]:
    try:
        return dict(redis_client.hgetall(settings_hash_key(selector)) or {})
    except Exception:
        return {}


def wake_family_hash_key(family: Any) -> str:
    token = _lower(family)
    if token not in WAKE_FAMILIES:
        raise ValueError(f"Unsupported wake family: {family}")
    return f"{WAKE_FAMILY_SETTINGS_HASH_PREFIX}{token}"


def _raw_wake_family_settings(family: Any) -> Dict[str, Any]:
    try:
        return dict(redis_client.hgetall(wake_family_hash_key(family)) or {})
    except Exception:
        return {}


def _wake_detector_mode(value: Any, source: Dict[str, Any] | None = None) -> str:
    token = _lower(value).replace("+", "_").replace("-", "_")
    if token == "dual":
        token = "mww_oww"
    if token in WAKE_DETECTOR_MODES:
        return token
    row = source or {}
    mww_enabled = _as_bool(row.get("wake_mww_enabled"), bool(DEFAULTS["wake_mww_enabled"]))
    oww_enabled = _as_bool(row.get("wake_oww_enabled"), bool(DEFAULTS["wake_oww_enabled"]))
    nww_enabled = _as_bool(row.get("wake_nww_enabled"), bool(DEFAULTS["wake_nww_enabled"]))
    if mww_enabled and nww_enabled:
        return "mww_nww"
    if mww_enabled and oww_enabled:
        return "mww_oww"
    if nww_enabled:
        return "nww"
    if oww_enabled:
        return "oww"
    return "mww"


def _wake_detector_flags(mode: Any) -> tuple[bool, bool, bool]:
    token = _wake_detector_mode(mode)
    return (
        token in {"mww", "mww_oww", "mww_nww"},
        token in {"oww", "mww_oww"},
        token in {"nww", "mww_nww"},
    )


def wake_family_for(*, capabilities: Dict[str, Any] | None = None, board: Any = "") -> str:
    caps = capabilities if isinstance(capabilities, dict) else {}
    if any(
        _as_bool(caps.get(key), False)
        for key in ("openwakeword", "nanowakeword", "wake_detector_selection", "dual_wake_confirmation")
    ):
        return "echo"
    # Board fallback keeps settings correct while an older Echo build is connecting
    # before it has advertised the newer detector capability flags.
    if _lower(board).replace("_", "-") in {"biscuit", "radar", "checkers", "crown", "athena", "rook"}:
        return "echo"
    return "mww"


def migrate_selector(old_selector: Any, new_selector: Any) -> bool:
    """Move device-scoped settings when firmware corrects a device selector."""
    old_token = _selector_token(old_selector)
    new_token = _selector_token(new_selector)
    if not old_token or not new_token or old_token == new_token:
        return False
    try:
        old_key = settings_hash_key(old_token)
        new_key = settings_hash_key(new_token)
        old_values = dict(redis_client.hgetall(old_key) or {})
        if not old_values:
            return False
        new_values = dict(redis_client.hgetall(new_key) or {})
        redis_client.hset(new_key, mapping={**old_values, **new_values})
        redis_client.delete(old_key)
        return True
    except Exception:
        return False


def _global_settings_with_migration() -> Dict[str, Any]:
    existing = _raw_settings()
    if _as_bool(existing.get(GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY), False):
        return existing

    device_rows: List[Dict[str, Any]] = []
    try:
        for raw_key in redis_client.scan_iter(match=f"{DEVICE_SETTINGS_HASH_PREFIX}*"):
            key = _text(raw_key)
            if not key:
                continue
            row = dict(redis_client.hgetall(key) or {})
            if row:
                device_rows.append(row)
    except Exception:
        device_rows = []

    migrated: Dict[str, Any] = {}
    for key in GLOBAL_SATELLITE_CONTROL_KEYS:
        values = [str(row.get(key) if row.get(key) is not None else "") for row in device_rows if key in row]
        if not values:
            continue
        counts = Counter(values)
        highest = max(counts.values())
        winners = sorted(value for value, count in counts.items() if count == highest)
        existing_value = str(existing.get(key) if existing.get(key) is not None else "")
        migrated[key] = existing_value if existing_value in winners else winners[0]

    selected_wake_word = _wake_word_value(migrated.get("wake_word", existing.get("wake_word")))
    selected_wake_url = _text(migrated.get("wake_word_url", existing.get("wake_word_url")))
    representative = next(
        (
            row
            for row in device_rows
            if _wake_word_value(row.get("wake_word")) == selected_wake_word
            and _text(row.get("wake_word_url")) == selected_wake_url
        ),
        {},
    )
    for key in GLOBAL_WAKE_PROFILE_KEYS:
        if key in representative:
            migrated[key] = representative.get(key)

    migrated[GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY] = "true"
    try:
        redis_client.hset(SETTINGS_HASH_KEY, mapping=migrated)
    except Exception:
        return {**existing, **migrated}
    return {**existing, **migrated}


def _ensure_wake_family_settings() -> None:
    global_raw = _global_settings_with_migration()
    if _as_bool(global_raw.get(WAKE_FAMILY_SETTINGS_MIGRATION_KEY), False):
        return

    legacy = normalize_settings(global_raw)
    legacy_mode = _wake_detector_mode(global_raw.get("wake_detector_mode"), legacy)
    mww_values = {
        key: legacy.get(key)
        for key in WAKE_FAMILY_PERSISTED_KEYS
        if key in legacy
    }
    mww_values.update(
        {
            "wake_detector_mode": "mww",
            "wake_mww_enabled": True,
            "wake_oww_enabled": False,
            "wake_nww_enabled": False,
        }
    )
    legacy_mww, legacy_oww, legacy_nww = _wake_detector_flags(legacy_mode)
    echo_values = {
        key: legacy.get(key)
        for key in WAKE_FAMILY_PERSISTED_KEYS
        if key in legacy
    }
    echo_values.update(
        {
            "wake_detector_mode": legacy_mode,
            "wake_mww_enabled": legacy_mww,
            "wake_oww_enabled": legacy_oww,
            "wake_nww_enabled": legacy_nww,
        }
    )
    try:
        if not _raw_wake_family_settings("mww"):
            redis_client.hset(
                wake_family_hash_key("mww"),
                mapping={key: str(value) for key, value in mww_values.items()},
            )
        if not _raw_wake_family_settings("echo"):
            redis_client.hset(
                wake_family_hash_key("echo"),
                mapping={key: str(value) for key, value in echo_values.items()},
            )
        redis_client.hset(
            SETTINGS_HASH_KEY,
            mapping={WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true"},
        )
    except Exception:
        # A temporary Redis outage must not prevent settings from being rendered;
        # both family snapshots can still fall back to the legacy global values.
        return


def _wake_row_uses_custom_model(row: Dict[str, Any]) -> bool:
    return bool(
        (_wake_word_value(row.get("wake_word")) == "custom_url" and _text(row.get("wake_word_url")))
        or (
            _wake_word_value(row.get("oww_wake_word")) == "custom_url"
            and _text(row.get("oww_wake_word_url"))
        )
    )


def _shared_wake_raw_settings() -> Dict[str, Any]:
    """Choose and persist the one profile when upgrading from split families."""
    _ensure_wake_family_settings()
    global_raw = _global_settings_with_migration()
    echo_raw = _raw_wake_family_settings("echo")
    mww_raw = _raw_wake_family_settings("mww")
    if _as_bool(global_raw.get(UNIFIED_WAKE_SETTINGS_MIGRATION_KEY), False):
        return echo_raw or mww_raw

    # Preserve an explicitly selected custom model. Echo wins when both sides
    # were customized because it is the only former row that can represent a
    # validated dual bundle; otherwise retain the user's MWW custom model
    # instead of silently replacing it with the default Hey Tater Echo row.
    selected = echo_raw or mww_raw
    if not _wake_row_uses_custom_model(echo_raw) and _wake_row_uses_custom_model(mww_raw):
        selected = dict(mww_raw)
        if (
            _wake_word_value(selected.get("wake_word")) == "custom_url"
            and _text(selected.get("wake_word_url"))
            and not _text(selected.get("wake_profile_source_url"))
        ):
            selected["wake_profile_source_url"] = _text(selected.get("wake_word_url"))
        selected.update(
            {
                "wake_detector_mode": "mww",
                "wake_mww_enabled": True,
                "wake_oww_enabled": False,
            }
        )

    try:
        if selected:
            redis_client.hset(
                wake_family_hash_key("echo"),
                mapping={key: str(value) for key, value in selected.items()},
            )
        redis_client.hset(
            SETTINGS_HASH_KEY,
            mapping={UNIFIED_WAKE_SETTINGS_MIGRATION_KEY: "true"},
        )
    except Exception:
        return dict(selected)
    return _raw_wake_family_settings("echo") or dict(selected)


def _global_wake_verifier_mode() -> str:
    try:
        mode = _lower(redis_client.hget(GLOBAL_VOICE_SETTINGS_HASH_KEY, GLOBAL_WAKE_VERIFIER_MODE_KEY))
    except Exception:
        mode = ""
    return mode if mode in WAKE_VERIFIER_MODES else "off"


def _is_sat1_board(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "satellite1",
        "satellite-1",
        "sat1",
        "sat-1",
        "satellite1-beta-rev41",
        "sat1-beta-rev41",
    } or compact in {
        "satellite1",
        "sat1",
        "satellite1betarev41",
        "sat1betarev41",
    }


def _board_default_overrides(board: Any = "") -> Dict[str, Any]:
    if _is_sat1_board(board):
        return {
            "wake_sensitivity": "high",
            "wake_environment": "far_field",
            "mww_tuning_override_enabled": True,
            "mww_threshold_override": 0.88,
            "wake_tuning_override_enabled": True,
            "wake_threshold_override": 0.88,
        }
    return {}


_LED_FIELD_KEYS = {
    "led_section",
    "led_brightness",
    "led_color",
    "led_listening_animation",
    "led_thinking_animation",
    "led_tool_call_animation",
    "led_replying_animation",
    "led_music_animation",
    "led_preview",
}

_SCREEN_SETTING_KEYS = {
    "screen_brightness",
    "screen_auto_brightness_enabled",
    "screen_auto_brightness_minimum",
    "screen_night_mode_enabled",
    "screen_night_brightness",
    "screen_night_start",
    "screen_night_end",
    "screen_night_display",
}

_ECHO_SCREEN_DEFAULTS = {
    "screen_brightness": 35,
    "screen_auto_brightness_enabled": False,
    "screen_auto_brightness_minimum": 12,
    "screen_night_mode_enabled": False,
    "screen_night_brightness": 3,
    "screen_night_start": "22:00",
    "screen_night_end": "07:00",
    "screen_night_display": "red_clock",
}

_SCREEN_FIELD_KEYS = _SCREEN_SETTING_KEYS | {"screen_section"}

_DISPLAY_THEME_SETTING_KEYS = {"display_theme", "display_wallpaper_interval_minutes"}
_DISPLAY_THEME_FIELD_KEYS = _DISPLAY_THEME_SETTING_KEYS | {
    "display_theme_section",
    "display_theme_preview",
}
_DISPLAY_EDGE_SETTING_KEYS = {"display_edge_glow"}
_DISPLAY_EDGE_FIELD_KEYS = _DISPLAY_EDGE_SETTING_KEYS

_AUDIO_OUTPUT_SETTING_KEYS = {"audio_output_mode"}
_AUDIO_OUTPUT_FIELD_KEYS = _AUDIO_OUTPUT_SETTING_KEYS | {"audio_output_section"}

_OUTPUT_CHAIN_SETTING_KEYS = {
    "eq_bands",
    "eq_loudness",
    "bass_guard_enabled",
    "bass_guard_db",
    "limiter_enabled",
    "limiter_threshold_db",
    "limiter_release_ms",
}
_OUTPUT_CHAIN_FIELD_KEYS = _OUTPUT_CHAIN_SETTING_KEYS | {
    "output_chain_section",
}
_OUTPUT_CHAIN_WIRE_KEYS = {
    "eq_bands": "eqBands",
    "eq_loudness": "eqLoudness",
    "bass_guard_enabled": "bassGuardEnabled",
    "bass_guard_db": "bassGuardDb",
    "limiter_enabled": "limiterEnabled",
    "limiter_threshold_db": "limiterThreshold",
    "limiter_release_ms": "limiterRelease",
}
_EQ_BAND_LABELS = (
    "125 Hz",
    "250 Hz",
    "500 Hz",
    "1 kHz",
    "2 kHz",
    "3.5 kHz",
    "5.5 kHz",
    "8 kHz",
)


def _supports_output_chain(capabilities: Dict[str, Any] | None = None) -> bool:
    values = capabilities if isinstance(capabilities, dict) else {}
    return _as_bool(values.get("output_chain"), False)


def _eq_bands_value(value: Any) -> List[float]:
    raw = _json_loads(value, [])
    if not isinstance(raw, list):
        raw = []
    bands: List[float] = []
    for index in range(len(_EQ_BAND_LABELS)):
        fallback = float(DEFAULTS["eq_bands"][index])
        candidate = raw[index] if index < len(raw) else fallback
        level = _as_float(candidate, fallback, minimum=-12.0, maximum=12.0)
        bands.append(round(level * 2.0) / 2.0 if math.isfinite(level) else fallback)
    return bands


def _audio_output_mode_value(value: Any) -> str:
    token = _lower(value).replace("-", "_")
    aliases = {"speaker": "internal", "line_out": "aux", "lineout": "aux"}
    token = aliases.get(token, token)
    return token if token in {"auto", "internal", "aux", "both"} else str(DEFAULTS["audio_output_mode"])


def _board_supports_display_theme(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "checkers",
        "echo-show-5",
        "echo-show-5-1st-gen",
        "crown",
        "echo-show-8",
        "echo-show-8-1st-gen",
        "athena",
        "echo-show-8-2nd-gen",
        "rook",
        "echo-spot",
    } or compact in {
        "checkers",
        "echoshow5",
        "echoshow51stgen",
        "crown",
        "echoshow8",
        "echoshow81stgen",
        "athena",
        "echoshow82ndgen",
        "rook",
        "echospot",
    }


def _board_supports_display_photos(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "checkers",
        "echo-show-5",
        "echo-show-5-1st-gen",
        "crown",
        "echo-show-8",
        "echo-show-8-1st-gen",
        "athena",
        "echo-show-8-2nd-gen",
        "rook",
        "echo-spot",
    } or compact in {
        "checkers",
        "echoshow5",
        "echoshow51stgen",
        "crown",
        "echoshow8",
        "echoshow81stgen",
        "athena",
        "echoshow82ndgen",
        "rook",
        "echospot",
    }


def _display_theme_value(value: Any) -> str:
    token = _lower(value).replace("-", "_")
    return token if token in DISPLAY_THEME_VALUES else str(DEFAULTS["display_theme"])


def _display_wallpaper_interval_value(value: Any) -> int:
    minutes = _as_int(
        value,
        int(DEFAULTS["display_wallpaper_interval_minutes"]),
        minimum=0,
        maximum=1440,
    )
    if minutes not in DISPLAY_WALLPAPER_INTERVAL_VALUES:
        return int(DEFAULTS["display_wallpaper_interval_minutes"])
    return minutes


def _board_supports_led_settings(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    if _board_supports_display_theme(board):
        return False
    if token in {"s3-box", "s3-box-3", "esp32-s3-box", "esp32-s3-box-3"}:
        return False
    if compact in {"s3box", "s3box3", "esp32s3box", "esp32s3box3"}:
        return False
    return True


def _is_echo_ring_board(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "biscuit",
        "echo-dot-2",
        "echo-dot-2nd-gen",
        "radar",
        "echo-2",
        "echo-2-2017",
        "echo-2nd-gen",
    } or compact in {
        "biscuit",
        "echodot2",
        "echodot2ndgen",
        "radar",
        "echo2",
        "echo22017",
        "echo2ndgen",
    }


def _board_supports_music_led_animation(board: Any = "") -> bool:
    if _is_echo_ring_board(board):
        return True
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "voice-pe",
        "satellite1",
        "satellite-1",
        "sat1",
        "sat-1",
        "respeaker-xvf3800",
    } or compact in {
        "voicepe",
        "satellite1",
        "sat1",
        "respeakerxvf3800",
    }


def _is_s420_board(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "s420",
        "tater-s420",
        "thirdreality-s420",
        "third-reality-s420",
    } or compact == "s420" or compact.startswith(("taters420", "thirdrealitys420"))


def _s420_led_animation(value: Any, default_key: str) -> str:
    token = _lower(value).replace("-", "_")
    aliases = {
        "directional": "pulse",
        "sparkle": "breathe",
        "ping_pong": "heartbeat",
        "voice_ring": "pulse",
    }
    token = aliases.get(token, token)
    supported = {value for value, _label in S420_LED_ANIMATIONS}
    return token if token in supported else S420_LED_DEFAULTS[default_key]


def _board_supports_screen_settings(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {
        "s3-box", "s3-box-3", "esp32-s3-box", "esp32-s3-box-3",
        "checkers", "echo-show-5", "echo-show-5-1st-gen",
        "crown", "echo-show-8", "echo-show-8-1st-gen",
        "athena", "echo-show-8-2nd-gen",
        "rook", "echo-spot",
    } or compact in {
        "s3box",
        "s3box3",
        "esp32s3box",
        "esp32s3box3",
        "checkers",
        "echoshow5",
        "echoshow51stgen",
        "crown",
        "echoshow8",
        "echoshow81stgen",
        "athena",
        "echoshow82ndgen",
        "rook",
        "echospot",
    }


def _board_is_s3_screen(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    return token in {"s3-box", "s3-box-3", "esp32-s3-box", "esp32-s3-box-3"} or compact in {
        "s3box", "s3box3", "esp32s3box", "esp32s3box3",
    }


def _board_is_echo_screen(board: Any = "") -> bool:
    return _board_supports_screen_settings(board) and not _board_is_s3_screen(board)


def _supports_screen_power_management(
    board: Any = "",
    capabilities: Dict[str, Any] | None = None,
) -> bool:
    if _board_is_s3_screen(board):
        return True
    values = capabilities if isinstance(capabilities, dict) else {}
    return _as_int(values.get("screen_power_management_version"), 0, minimum=0, maximum=100) >= 1


def _screen_night_display_value(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    aliases = {"clock": "red_clock", "red": "red_clock", "screen_off": "off"}
    token = aliases.get(token, token)
    return token if token in {"dim", "red_clock", "off"} else str(DEFAULTS["screen_night_display"])


def normalize_settings(values: Dict[str, Any], *, base: Dict[str, Any] | None = None) -> Dict[str, Any]:
    incoming = values or {}
    source = {**DEFAULTS, **(base or {}), **incoming}
    wake_engine = _lower(source.get("wake_engine")) or str(DEFAULTS["wake_engine"])
    if wake_engine not in WAKE_ENGINES:
        wake_engine = str(DEFAULTS["wake_engine"])
    logging_level = _lower(source.get("logging_level")) or str(DEFAULTS["logging_level"])
    if logging_level not in LOGGING_LEVELS:
        logging_level = str(DEFAULTS["logging_level"])
    wake_verifier_mode = _lower(source.get("wake_verifier_mode")) or str(DEFAULTS["wake_verifier_mode"])
    if wake_verifier_mode not in WAKE_VERIFIER_MODES:
        wake_verifier_mode = str(DEFAULTS["wake_verifier_mode"])
    wake_mww_enabled = _as_bool(source.get("wake_mww_enabled"), bool(DEFAULTS["wake_mww_enabled"]))
    wake_oww_enabled = _as_bool(source.get("wake_oww_enabled"), bool(DEFAULTS["wake_oww_enabled"]))
    wake_nww_enabled = _as_bool(source.get("wake_nww_enabled"), bool(DEFAULTS["wake_nww_enabled"]))
    nww_architecture = _lower(source.get("nww_architecture"))
    if nww_architecture not in {"cnn", "gru"}:
        nww_architecture = str(DEFAULTS["nww_architecture"])
    wake_word = _wake_word_value(source.get("wake_word"))
    wake_word_url = _text(source.get("wake_word_url"))
    if wake_word == "custom_url" and not wake_word_url and _is_url(source.get("wake_word")):
        wake_word_url = _text(source.get("wake_word"))
    if wake_word != "custom_url":
        wake_word_url = ""
    oww_wake_word = _wake_word_value(source.get("oww_wake_word"))
    oww_wake_word_url = _text(source.get("oww_wake_word_url"))
    if oww_wake_word == "custom_url" and not oww_wake_word_url and _is_url(source.get("oww_wake_word")):
        oww_wake_word_url = _text(source.get("oww_wake_word"))
    if oww_wake_word != "custom_url":
        oww_wake_word_url = ""
    if wake_mww_enabled and wake_oww_enabled and oww_wake_word == "hey_tater":
        wake_word = "hey_tater"
        wake_word_url = ""
    wake_sensitivity = _wake_sensitivity_value(source.get("wake_sensitivity"))
    wake_environment = _wake_environment_value(source.get("wake_environment"))
    profile = _wake_profile_from_source(source, wake_word, wake_word_url)
    oww_profile = _oww_profile_from_source(source, oww_wake_word, oww_wake_word_url)
    detector_mode = _wake_detector_mode(source.get("wake_detector_mode"), source)
    profile_key = _text(profile.get("wake_profile_key")) or _wake_profile_key(wake_word, wake_word_url)
    profile_threshold = round(
        _as_float(profile.get("wake_profile_threshold"), float(DEFAULTS["wake_profile_threshold"]), minimum=0.01, maximum=0.99),
        3,
    )
    profile_window = _as_int(
        profile.get("wake_profile_sliding_window"),
        int(DEFAULTS["wake_profile_sliding_window"]),
        minimum=1,
        maximum=10,
    )
    profile_close_miss_threshold = round(
        _as_float(
            profile.get("wake_profile_close_miss_threshold"),
            float(DEFAULTS["wake_profile_close_miss_threshold"]),
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    overrides = _wake_overrides(source.get("wake_tuning_overrides"))
    existing_override = overrides.get(profile_key, {})
    if base is not None and "mww_tuning_override_enabled" in incoming:
        override_enabled_source = incoming.get("mww_tuning_override_enabled")
    elif base is not None and "wake_tuning_override_enabled" in incoming:
        override_enabled_source = incoming.get("wake_tuning_override_enabled")
    else:
        override_enabled_source = source.get(
            "mww_tuning_override_enabled",
            existing_override.get("enabled", source.get("wake_tuning_override_enabled")),
        )
    override_enabled = _as_bool(override_enabled_source, False)
    if base is not None and "mww_threshold_override" in incoming:
        threshold_override_source = incoming.get("mww_threshold_override")
    elif base is not None and "wake_threshold_override" in incoming:
        threshold_override_source = incoming.get("wake_threshold_override")
    else:
        threshold_override_source = source.get(
            "mww_threshold_override",
            existing_override.get("threshold", profile_threshold),
        )
    threshold_override = round(
        _as_float(
            threshold_override_source,
            profile_threshold,
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    if base is not None and "mww_sliding_window_override" in incoming:
        window_override_source = incoming.get("mww_sliding_window_override")
    elif base is not None and "wake_sliding_window_override" in incoming:
        window_override_source = incoming.get("wake_sliding_window_override")
    else:
        window_override_source = source.get(
            "mww_sliding_window_override",
            existing_override.get("sliding_window", profile_window),
        )
    window_override = _as_int(
        window_override_source,
        profile_window,
        minimum=1,
        maximum=10,
    )
    if base is not None and "wake_close_miss_threshold_override" in incoming:
        close_miss_override_source = incoming.get("wake_close_miss_threshold_override")
    elif base is not None and "close_miss_threshold" in incoming:
        close_miss_override_source = incoming.get("close_miss_threshold")
    else:
        close_miss_override_source = existing_override.get(
            "close_miss_threshold",
            profile_close_miss_threshold,
        )
    close_miss_override = round(
        _as_float(
            close_miss_override_source,
            profile_close_miss_threshold,
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    overrides[profile_key] = {
        "enabled": bool(override_enabled),
        "threshold": threshold_override,
        "sliding_window": window_override,
        "close_miss_threshold": close_miss_override,
    }
    sensitivity_threshold = _threshold_for_sensitivity(profile_threshold, wake_sensitivity)
    effective_threshold = threshold_override if override_enabled else sensitivity_threshold
    effective_window = window_override if override_enabled else profile_window
    # Close-miss capture remains package-owned. The shared model override UI
    # intentionally exposes only the values that decide a wake; it must not
    # silently alter trainer-feedback capture behavior.
    effective_close_miss_threshold = profile_close_miss_threshold

    oww_profile_threshold = round(
        _as_float(oww_profile.get("oww_profile_threshold"), float(DEFAULTS["oww_profile_threshold"]), minimum=0.01, maximum=1.0),
        3,
    )
    oww_profile_patience = _as_int(
        oww_profile.get("oww_profile_patience"), int(DEFAULTS["oww_profile_patience"]), minimum=1, maximum=20,
    )
    oww_confirmation_threshold = round(
        _as_float(
            oww_profile.get("oww_profile_confirmation_threshold"),
            float(DEFAULTS["oww_profile_confirmation_threshold"]),
            minimum=0.01,
            maximum=1.0,
        ),
        3,
    )
    oww_confirmation_patience = _as_int(
        oww_profile.get("oww_profile_confirmation_patience"),
        int(DEFAULTS["oww_profile_confirmation_patience"]),
        minimum=1,
        maximum=20,
    )
    active_oww_profile_threshold = oww_confirmation_threshold if detector_mode == "mww_oww" else oww_profile_threshold
    active_oww_profile_patience = oww_confirmation_patience if detector_mode == "mww_oww" else oww_profile_patience
    oww_override_enabled = _as_bool(source.get("oww_tuning_override_enabled"), False)
    oww_threshold_override = round(
        _as_float(source.get("oww_threshold_override"), active_oww_profile_threshold, minimum=0.01, maximum=1.0),
        3,
    )
    oww_patience_override = _as_int(
        source.get("oww_patience_override"), active_oww_profile_patience, minimum=1, maximum=20,
    )

    architecture_prefix = "nww_gru" if nww_architecture == "gru" else "nww_cnn"
    nww_profile_threshold = round(
        _as_float(
            oww_profile.get(f"{architecture_prefix}_profile_threshold", oww_profile.get("nww_profile_threshold")),
            float(DEFAULTS[f"{architecture_prefix}_profile_threshold"]),
            minimum=0.01,
            maximum=1.0,
        ),
        3,
    )
    nww_profile_patience = _as_int(
        oww_profile.get(f"{architecture_prefix}_profile_patience", oww_profile.get("nww_profile_patience")),
        int(DEFAULTS[f"{architecture_prefix}_profile_patience"]),
        minimum=1,
        maximum=20,
    )
    nww_override_enabled = _as_bool(source.get("nww_tuning_override_enabled"), False)
    nww_threshold_override = round(
        _as_float(source.get("nww_threshold_override"), nww_profile_threshold, minimum=0.01, maximum=1.0),
        3,
    )
    nww_patience_override = _as_int(
        source.get("nww_patience_override"), nww_profile_patience, minimum=1, maximum=20,
    )
    wake_sound = _wake_sound_value(source.get("wake_sound"))
    wake_sound_url = _text(source.get("wake_sound_url"))
    if wake_sound != "custom":
        wake_sound_url = ""
    return {
        "wake_engine": wake_engine,
        "wake_mww_enabled": wake_mww_enabled,
        "wake_oww_enabled": wake_oww_enabled,
        "wake_nww_enabled": wake_nww_enabled,
        "wake_word": wake_word,
        "wake_word_url": wake_word_url,
        "wake_model_revision": _text(profile.get("wake_model_revision")),
        "wake_profile_key": profile_key,
        "wake_profile_name": _text(profile.get("wake_profile_name")) or str(DEFAULTS["wake_profile_name"]),
        "wake_profile_source_url": _text(profile.get("wake_profile_source_url")),
        "wake_profile_model_url": _text(profile.get("wake_profile_model_url")),
        "wake_profile_threshold": profile_threshold,
        "wake_profile_sliding_window": profile_window,
        "wake_profile_close_miss_threshold": profile_close_miss_threshold,
        "wake_profile_error": _text(profile.get("wake_profile_error")),
        "oww_wake_word": oww_wake_word,
        "oww_wake_word_url": oww_wake_word_url,
        "oww_model_revision": _text(oww_profile.get("oww_model_revision")),
        "nww_model_revision": _text(oww_profile.get("nww_model_revision")),
        "nww_architecture": nww_architecture,
        "nww_available_architectures": _text(oww_profile.get("nww_available_architectures")),
        "oww_profile_name": _text(oww_profile.get("oww_profile_name")) or str(DEFAULTS["oww_profile_name"]),
        "oww_profile_threshold": oww_profile_threshold,
        "oww_profile_patience": oww_profile_patience,
        "oww_profile_confirmation_threshold": oww_confirmation_threshold,
        "oww_profile_confirmation_patience": oww_confirmation_patience,
        "oww_active_profile_threshold": active_oww_profile_threshold,
        "oww_active_profile_patience": active_oww_profile_patience,
        "oww_profile_error": _text(oww_profile.get("oww_profile_error")),
        "nww_profile_threshold": nww_profile_threshold,
        "nww_profile_patience": nww_profile_patience,
        "nww_cnn_profile_threshold": round(
            _as_float(oww_profile.get("nww_cnn_profile_threshold"), float(DEFAULTS["nww_cnn_profile_threshold"]), minimum=0.01, maximum=1.0),
            3,
        ),
        "nww_cnn_profile_patience": _as_int(
            oww_profile.get("nww_cnn_profile_patience"), int(DEFAULTS["nww_cnn_profile_patience"]), minimum=1, maximum=20,
        ),
        "nww_gru_profile_threshold": round(
            _as_float(oww_profile.get("nww_gru_profile_threshold"), float(DEFAULTS["nww_gru_profile_threshold"]), minimum=0.01, maximum=1.0),
            3,
        ),
        "nww_gru_profile_patience": _as_int(
            oww_profile.get("nww_gru_profile_patience"), int(DEFAULTS["nww_gru_profile_patience"]), minimum=1, maximum=20,
        ),
        "wake_sensitivity": wake_sensitivity,
        "wake_environment": wake_environment,
        "mww_tuning_override_enabled": bool(override_enabled),
        "mww_threshold_override": threshold_override,
        "mww_sliding_window_override": window_override,
        "oww_tuning_override_enabled": bool(oww_override_enabled),
        "oww_threshold_override": oww_threshold_override,
        "oww_patience_override": oww_patience_override,
        "nww_tuning_override_enabled": bool(nww_override_enabled),
        "nww_threshold_override": nww_threshold_override,
        "nww_patience_override": nww_patience_override,
        "wake_tuning_override_enabled": bool(override_enabled),
        "wake_threshold_override": threshold_override,
        "wake_sliding_window_override": window_override,
        "wake_close_miss_threshold_override": close_miss_override,
        "wake_tuning_overrides": _json_dumps(overrides),
        "wake_threshold": effective_threshold,
        "wake_sliding_window": effective_window,
        "oww_threshold": oww_threshold_override if oww_override_enabled else active_oww_profile_threshold,
        "oww_patience": oww_patience_override if oww_override_enabled else active_oww_profile_patience,
        "nww_threshold": nww_threshold_override if nww_override_enabled else nww_profile_threshold,
        "nww_patience": nww_patience_override if nww_override_enabled else nww_profile_patience,
        "capture_wake_audio": _as_bool(source.get("capture_wake_audio"), bool(DEFAULTS["capture_wake_audio"])),
        "capture_close_misses": _as_bool(source.get("capture_close_misses"), bool(DEFAULTS["capture_close_misses"])),
        "close_miss_threshold": effective_close_miss_threshold,
        "trainer_app_url": _text(source.get("trainer_app_url")) or str(DEFAULTS["trainer_app_url"]),
        "wake_verifier_mode": wake_verifier_mode,
        "wake_verifier_phrase": _text(source.get("wake_verifier_phrase")),
        "wake_verifier_threshold": round(
            _as_float(
                source.get("wake_verifier_threshold"),
                float(DEFAULTS["wake_verifier_threshold"]),
                minimum=0.5,
                maximum=1.0,
            ),
            3,
        ),
        "wake_verifier_window_ms": _as_int(
            source.get("wake_verifier_window_ms"),
            int(DEFAULTS["wake_verifier_window_ms"]),
            minimum=500,
            maximum=2000,
        ),
        "wake_verifier_timeout_ms": _as_int(
            source.get("wake_verifier_timeout_ms"),
            int(DEFAULTS["wake_verifier_timeout_ms"]),
            minimum=100,
            maximum=2000,
        ),
        "wake_sound_enabled": _as_bool(source.get("wake_sound_enabled"), bool(DEFAULTS["wake_sound_enabled"])),
        "wake_sound": wake_sound,
        "wake_sound_url": wake_sound_url,
        "aec_enabled": _as_bool(source.get("aec_enabled"), bool(DEFAULTS["aec_enabled"])),
        "aec_strength_percent": _as_int(
            source.get("aec_strength_percent"),
            int(DEFAULTS["aec_strength_percent"]),
            minimum=0,
            maximum=100,
        ),
        "aec_delay_ms": _as_int(source.get("aec_delay_ms"), int(DEFAULTS["aec_delay_ms"]), minimum=0, maximum=220),
        "continued_chat": _as_bool(source.get("continued_chat"), bool(DEFAULTS["continued_chat"])),
        "barge_in_enabled": _as_bool(source.get("barge_in_enabled"), bool(DEFAULTS["barge_in_enabled"])),
        "volume_percent": _as_int(source.get("volume_percent"), int(DEFAULTS["volume_percent"]), minimum=0, maximum=100),
        "eq_bands": _eq_bands_value(source.get("eq_bands")),
        "eq_loudness": _as_bool(source.get("eq_loudness"), bool(DEFAULTS["eq_loudness"])),
        "bass_guard_enabled": _as_bool(
            source.get("bass_guard_enabled"),
            bool(DEFAULTS["bass_guard_enabled"]),
        ),
        "bass_guard_db": round(
            _as_float(
                source.get("bass_guard_db"),
                float(DEFAULTS["bass_guard_db"]),
                minimum=-60.0,
                maximum=0.0,
            ),
            1,
        ),
        "limiter_enabled": _as_bool(
            source.get("limiter_enabled"),
            bool(DEFAULTS["limiter_enabled"]),
        ),
        "limiter_threshold_db": round(
            _as_float(
                source.get("limiter_threshold_db"),
                float(DEFAULTS["limiter_threshold_db"]),
                minimum=-12.0,
                maximum=0.0,
            ),
            1,
        ),
        "limiter_release_ms": round(
            _as_float(
                source.get("limiter_release_ms"),
                float(DEFAULTS["limiter_release_ms"]),
                minimum=20.0,
                maximum=1000.0,
            ),
            1,
        ),
        "audio_output_mode": _audio_output_mode_value(source.get("audio_output_mode")),
        "screen_brightness": _as_int(
            source.get("screen_brightness"),
            int(DEFAULTS["screen_brightness"]),
            minimum=0,
            maximum=100,
        ),
        "screen_auto_brightness_enabled": _as_bool(
            source.get("screen_auto_brightness_enabled"),
            bool(DEFAULTS["screen_auto_brightness_enabled"]),
        ),
        "screen_auto_brightness_minimum": _as_int(
            source.get("screen_auto_brightness_minimum"),
            int(DEFAULTS["screen_auto_brightness_minimum"]),
            minimum=1,
            maximum=50,
        ),
        "screen_night_mode_enabled": _as_bool(
            source.get("screen_night_mode_enabled"),
            bool(DEFAULTS["screen_night_mode_enabled"]),
        ),
        "screen_night_brightness": _as_int(
            source.get("screen_night_brightness"),
            int(DEFAULTS["screen_night_brightness"]),
            minimum=0,
            maximum=100,
        ),
        "screen_night_start": _time_value(
            source.get("screen_night_start"),
            str(DEFAULTS["screen_night_start"]),
        ),
        "screen_night_end": _time_value(
            source.get("screen_night_end"),
            str(DEFAULTS["screen_night_end"]),
        ),
        "screen_night_display": _screen_night_display_value(source.get("screen_night_display")),
        "display_theme": _display_theme_value(source.get("display_theme")),
        "display_wallpaper_interval_minutes": _display_wallpaper_interval_value(
            source.get("display_wallpaper_interval_minutes")
        ),
        "display_edge_glow": _as_bool(source.get("display_edge_glow"), bool(DEFAULTS["display_edge_glow"])),
        "led_brightness": _as_int(source.get("led_brightness"), int(DEFAULTS["led_brightness"]), minimum=0, maximum=100),
        "led_color": _led_color_value(source.get("led_color")),
        "led_listening_animation": _led_animation_value(source.get("led_listening_animation"), "led_listening_animation"),
        "led_thinking_animation": _led_animation_value(source.get("led_thinking_animation"), "led_thinking_animation"),
        "led_tool_call_animation": _led_animation_value(source.get("led_tool_call_animation"), "led_tool_call_animation"),
        "led_replying_animation": _led_animation_value(source.get("led_replying_animation"), "led_replying_animation"),
        "led_music_animation": _led_music_animation_value(source.get("led_music_animation")),
        "logging_level": logging_level,
        "ssh_enabled": _as_bool(source.get("ssh_enabled"), bool(DEFAULTS["ssh_enabled"])),
    }


def settings_snapshot(selector: Any = "", *, board: Any = "") -> Dict[str, Any]:
    token = _selector_token(selector)
    board_defaults = _board_default_overrides(board)
    base = normalize_settings(board_defaults) if board_defaults else None
    global_raw = _global_settings_with_migration()
    global_settings = normalize_settings(global_raw, base=base)
    if not token:
        current = global_settings
    else:
        device_raw = {
            key: value
            for key, value in _raw_settings(token).items()
            if key not in GLOBAL_SATELLITE_PERSISTED_KEYS
        }
        current = normalize_settings(device_raw, base=global_settings)
        if _board_is_echo_screen(board):
            for key, value in _ECHO_SCREEN_DEFAULTS.items():
                if key not in device_raw:
                    current[key] = value
    current["wake_verifier_mode"] = _global_wake_verifier_mode()
    if _is_s420_board(board):
        for key in S420_LED_DEFAULTS:
            current[key] = _s420_led_animation(current.get(key), key)
    return current


def shared_wake_settings_snapshot() -> Dict[str, Any]:
    """Return the one wake profile shared by every satellite family.

    The former Echo-family row is the canonical row because it can describe
    all supported modes while a matched MWW+OWW bundle keeps the phrase
    consistent across detector families. MWW-only satellites receive a projection from
    ``wake_family_settings_snapshot`` below.
    """
    global_settings = normalize_settings(_global_settings_with_migration())
    raw = _shared_wake_raw_settings()
    current = normalize_settings(raw, base=global_settings)
    legacy_oww_only = _lower(raw.get("wake_detector_mode")) == "oww" or (
        not _as_bool(raw.get("wake_mww_enabled"), True)
        and _as_bool(raw.get("wake_oww_enabled"), False)
    )
    if legacy_oww_only and _text(current.get("oww_profile_name")):
        # The old standalone-OWW row may not contain the bundle's MWW profile
        # metadata even though the bundle itself does. Both models name the
        # same validated phrase, so carry that identity into the unified view.
        current["wake_profile_name"] = _text(current.get("oww_profile_name"))
    mode = _wake_detector_mode(raw.get("wake_detector_mode"), current)
    mww_enabled, oww_enabled, nww_enabled = _wake_detector_flags(mode)
    current["wake_detector_mode"] = mode
    current["wake_mww_enabled"] = mww_enabled
    current["wake_oww_enabled"] = oww_enabled
    current["wake_nww_enabled"] = nww_enabled
    return current


def wake_family_settings_snapshot(family: Any) -> Dict[str, Any]:
    token = _lower(family)
    if token not in WAKE_FAMILIES:
        raise ValueError(f"Unsupported wake family: {family}")
    current = shared_wake_settings_snapshot()
    if token == "mww":
        # ESP and other MWW-only satellites use the exact microWakeWord half
        # of the shared profile. OWW fields remain in this internal snapshot
        # only long enough for the firmware payload builder to project them
        # away; they are never sent to an MWW-only device.
        current["wake_detector_mode"] = "mww"
        current["wake_mww_enabled"] = True
        current["wake_oww_enabled"] = False
        current["wake_nww_enabled"] = False
    return current


def effective_settings_snapshot(
    selector: Any = "",
    *,
    board: Any = "",
    capabilities: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return the effective settings Tater projects to this satellite family.

    ``settings_snapshot`` contains room/device controls, while the selected wake
    profile is shared across compatible satellite families.  Status and UI
    callers need the same merged view as the firmware payload builder or they
    can display stale detector flags and model tuning.
    """
    current = settings_snapshot(selector, board=board)
    # An untargeted snapshot represents the shared wake-word form itself.
    # Targeted snapshots are projected to the receiving satellite family.
    untargeted = not _selector_token(selector) and not _text(board) and capabilities is None
    family = "echo" if untargeted else wake_family_for(capabilities=capabilities, board=board)
    family_settings = wake_family_settings_snapshot(family)
    current = normalize_settings(
        {
            key: family_settings.get(key)
            for key in WAKE_FAMILY_PERSISTED_KEYS
            if key in family_settings
        },
        base=current,
    )
    mode = "mww" if family == "mww" else _wake_detector_mode(
        family_settings.get("wake_detector_mode"),
        family_settings,
    )
    if mode in {"nww", "mww_nww"} and capabilities is not None and not _as_bool(
        (capabilities or {}).get("nanowakeword"),
        False,
    ):
        mode = "mww"
    mww_enabled, oww_enabled, nww_enabled = _wake_detector_flags(mode)
    current["wake_detector_mode"] = mode
    current["wake_mww_enabled"] = mww_enabled
    current["wake_oww_enabled"] = oww_enabled
    current["wake_nww_enabled"] = nww_enabled
    return current


def firmware_settings_snapshot(
    selector: Any = "",
    *,
    board: Any = "",
    capabilities: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    current = settings_snapshot(selector, board=board)
    family = wake_family_for(capabilities=capabilities, board=board)
    family_settings = wake_family_settings_snapshot(family)
    current = normalize_settings(
        {
            key: family_settings.get(key)
            for key in WAKE_FAMILY_PERSISTED_KEYS
            if key in family_settings
        },
        base=current,
    )
    # Some firmware fields (notably the projected NWW model source) are
    # derived below rather than persisted in the shared settings document.
    output = {key: current[key] for key in FIRMWARE_SETTING_KEYS if key in current}
    if family == "mww":
        output["wake_mww_enabled"] = True
        output["wake_oww_enabled"] = False
        output["wake_nww_enabled"] = False
        for key in (
            "wake_oww_enabled", "wake_nww_enabled", "oww_wake_word", "oww_wake_word_url",
            "oww_model_revision", "nww_wake_word", "nww_wake_word_url", "nww_model_revision",
            "nww_architecture", "oww_threshold", "oww_patience", "nww_threshold", "nww_patience",
        ):
            output.pop(key, None)
    else:
        mode = _wake_detector_mode(family_settings.get("wake_detector_mode"), family_settings)
        if mode in {"nww", "mww_nww"} and not _as_bool((capabilities or {}).get("nanowakeword"), False):
            # Older Echo/ThirdReality builds do not understand the NWW flags.
            # Keep them safely on the shared bundle's MWW fallback until their
            # firmware advertises NanoWakeWord support.
            mode = "mww"
        mww_enabled, oww_enabled, nww_enabled = _wake_detector_flags(mode)
        output["wake_detector_mode"] = mode
        output["wake_mww_enabled"] = mww_enabled
        output["wake_oww_enabled"] = oww_enabled
        output["wake_nww_enabled"] = nww_enabled
        if mode == "mww":
            for key in (
                "oww_wake_word", "oww_wake_word_url", "oww_model_revision",
                "nww_wake_word", "nww_wake_word_url", "nww_model_revision", "nww_architecture",
                "oww_threshold", "oww_patience", "nww_threshold", "nww_patience",
            ):
                output.pop(key, None)
        elif mode in {"oww", "nww"}:
            # Echo firmware installs every model source named in a settings
            # frame, even when that detector is disabled. Do not make an
            # OWW-only selection download or validate the MWW half of a
            # matched catalog bundle.
            for key in ("wake_word", "wake_word_url", "wake_model_revision"):
                output.pop(key, None)
        if nww_enabled:
            if _wake_word_value(family_settings.get("oww_wake_word")) == "custom_url":
                output["nww_wake_word"] = "custom_url"
                output["nww_wake_word_url"] = output.get("oww_wake_word_url", "")
                output["nww_model_revision"] = output.get("nww_model_revision") or output.get("oww_model_revision", "")
            else:
                output["nww_wake_word"] = "hey_tater"
                output.pop("nww_wake_word_url", None)
                output.pop("nww_model_revision", None)
            for key in ("oww_wake_word", "oww_wake_word_url", "oww_model_revision"):
                output.pop(key, None)
            output.pop("oww_threshold", None)
            output.pop("oww_patience", None)
        elif oww_enabled:
            output.pop("nww_threshold", None)
            output.pop("nww_patience", None)
    if not _board_supports_led_settings(board):
        for key in _LED_FIELD_KEYS:
            output.pop(key, None)
    if not _board_supports_music_led_animation(board):
        output.pop("led_music_animation", None)
    if not _board_supports_display_theme(board):
        for key in _DISPLAY_THEME_SETTING_KEYS:
            output.pop(key, None)
    if not _board_supports_display_photos(board):
        for key in _DISPLAY_EDGE_SETTING_KEYS:
            output.pop(key, None)
    if not _is_sat1_board(board):
        for key in _AUDIO_OUTPUT_SETTING_KEYS:
            output.pop(key, None)
    if _supports_output_chain(capabilities):
        output.update(
            {
                wire_key: current[setting_key]
                for setting_key, wire_key in _OUTPUT_CHAIN_WIRE_KEYS.items()
            }
        )
    if _supports_ssh_management(capabilities):
        output["ssh_enabled"] = bool(current.get("ssh_enabled"))
        password = _text(_raw_settings(selector).get("ssh_password"))
        if output["ssh_enabled"] and _valid_ssh_password(password):
            output["ssh_password"] = password
        else:
            output.pop("ssh_password", None)
    else:
        output.pop("ssh_enabled", None)
        output.pop("ssh_password", None)
    if _board_supports_screen_settings(board) and _supports_screen_power_management(board, capabilities):
        if _board_is_s3_screen(board):
            now = datetime.now().astimezone()
            output["screen_local_time_seconds"] = (now.hour * 60 * 60) + (now.minute * 60) + now.second
            for key in (
                "screen_auto_brightness_enabled",
                "screen_auto_brightness_minimum",
                "screen_night_display",
            ):
                output.pop(key, None)
    else:
        for key in _SCREEN_SETTING_KEYS:
            output.pop(key, None)
    return output


def _profile_for_save(values: Dict[str, Any], current_raw: Dict[str, Any]) -> Dict[str, Any]:
    source = {**DEFAULTS, **(current_raw or {}), **(values or {})}
    wake_word = _wake_word_value(source.get("wake_word"))
    wake_word_url = _text(source.get("wake_word_url"))
    if wake_word == "custom_url" and not wake_word_url and _is_url(source.get("wake_word")):
        wake_word_url = _text(source.get("wake_word"))
    if wake_word != "custom_url":
        return dict(BUILTIN_WAKE_PROFILES.get(wake_word) or BUILTIN_WAKE_PROFILES["hey_tater"])
    if wake_word_url.lower().split("?", 1)[0].endswith(".wake-bundle.json"):
        bundled = _fetch_oww_profile_json(wake_word_url)
        if _as_bool(bundled.get("bundle_has_mww"), False) and not _text(bundled.get("wake_profile_error")):
            return bundled
    fetched = _fetch_wake_profile_json(wake_word_url)
    fetch_error = _text(fetched.get("wake_profile_error"))
    if not fetch_error:
        return fetched
    if (
        _text(current_raw.get("wake_profile_source_url")) == wake_word_url and
        _text(current_raw.get("wake_profile_name")) and
        _text(current_raw.get("wake_profile_close_miss_threshold"))
    ):
        existing = _wake_profile_from_source(current_raw, wake_word, wake_word_url)
        existing["wake_profile_error"] = fetch_error
        return existing
    return fetched


def _oww_profile_for_save(values: Dict[str, Any], current_raw: Dict[str, Any]) -> Dict[str, Any]:
    source = {**DEFAULTS, **(current_raw or {}), **(values or {})}
    wake_word = _wake_word_value(source.get("oww_wake_word"))
    bundle_url = _text(source.get("oww_wake_word_url"))
    if wake_word == "custom_url" and not bundle_url and _is_url(source.get("oww_wake_word")):
        bundle_url = _text(source.get("oww_wake_word"))
    if wake_word != "custom_url":
        return dict(BUILTIN_OWW_PROFILES.get(wake_word) or BUILTIN_OWW_PROFILES["hey_tater"])
    fetched = _fetch_oww_profile_json(bundle_url)
    fetch_error = _text(fetched.get("oww_profile_error"))
    if not fetch_error:
        return fetched
    if (
        _text(current_raw.get("oww_wake_word_url")) == bundle_url
        and _text(current_raw.get("oww_profile_name"))
        and _text(current_raw.get("oww_model_revision"))
    ):
        existing = _oww_profile_from_source(current_raw, wake_word, bundle_url)
        existing["oww_profile_error"] = fetch_error
        return existing
    return fetched


def _wake_phrase_identity(value: Any) -> str:
    return "".join(character for character in _lower(value) if character.isalnum())


def _dual_profile_for_save(
    values: Dict[str, Any],
    current_raw: Dict[str, Any],
    *,
    required_engine: str = "oww",
    nww_architecture: str = "cnn",
) -> Dict[str, Any]:
    source = {**DEFAULTS, **(current_raw or {}), **(values or {})}
    bundle_source = _wake_word_value(source.get("oww_wake_word"))
    bundle_url = _text(source.get("oww_wake_word_url"))
    if bundle_source != "custom_url":
        builtin = {
            **dict(BUILTIN_WAKE_PROFILES["hey_tater"]),
            **dict(BUILTIN_OWW_PROFILES["hey_tater"]),
        }
        if required_engine == "nww" and nww_architecture not in {
            item.strip().lower()
            for item in _text(builtin.get("nww_available_architectures")).split(",")
            if item.strip()
        }:
            raise ValueError(
                f"The built-in wake package does not contain a NanoWakeWord {nww_architecture.upper()} model."
            )
        return builtin

    fetched = _fetch_oww_profile_json(bundle_url)
    bundle_error = _text(fetched.get("oww_profile_error"))
    if bundle_error:
        unchanged = _text(current_raw.get("oww_wake_word_url")) == bundle_url
        if unchanged and _text(current_raw.get("wake_profile_source_url")):
            return {
                **_wake_profile_from_source(current_raw, "custom_url", _text(current_raw.get("wake_word_url"))),
                **_oww_profile_from_source(current_raw, "custom_url", bundle_url),
                "oww_profile_error": bundle_error,
            }
        raise ValueError(bundle_error)

    if required_engine == "oww" and not _as_bool(fetched.get("bundle_has_oww"), False):
        raise ValueError("The selected wake bundle does not contain an openWakeWord model.")
    if required_engine == "nww":
        architectures = {
            item.strip().lower()
            for item in _text(fetched.get("nww_available_architectures")).split(",")
            if item.strip()
        }
        if nww_architecture not in architectures:
            raise ValueError(
                f"The selected wake bundle does not contain a NanoWakeWord {nww_architecture.upper()} model."
            )

    if _as_bool(fetched.get("wake_bundle_embedded_micro"), False):
        return fetched

    manifest_url = _text(fetched.get("wake_bundle_manifest_url"))
    mww_profile = _fetch_wake_profile_json(manifest_url)
    mww_error = _text(mww_profile.get("wake_profile_error"))
    if mww_error:
        raise ValueError(f"Dual-model bundle microWakeWord manifest is invalid: {mww_error}")
    expected_manifest_sha = _text(fetched.get("wake_bundle_manifest_sha256")).lower()
    if expected_manifest_sha != _text(mww_profile.get("wake_model_revision")).lower():
        raise ValueError("Dual-model bundle microWakeWord manifest checksum does not match.")
    expected_model_url = _text(fetched.get("wake_bundle_model_url"))
    actual_model_url = _text(mww_profile.get("wake_profile_model_url"))
    if expected_model_url != actual_model_url:
        raise ValueError("Dual-model bundle microWakeWord manifest names a different model.")
    bundle_phrase = _wake_phrase_identity(fetched.get("wake_bundle_phrase"))
    mww_phrase = _wake_phrase_identity(mww_profile.get("wake_profile_name"))
    if bundle_phrase and mww_phrase and bundle_phrase != mww_phrase:
        raise ValueError("Dual-model bundle contains different MWW and OWW wake phrases.")
    return {**mww_profile, **fetched}


def settings_fields(
    selector: Any = "",
    *,
    board: Any = "",
    wake_family: Any = "",
    capabilities: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    family = _lower(wake_family)
    token = _selector_token(selector)
    ssh_password = ""
    if token and _supports_ssh_management(capabilities):
        ssh_password = _ensure_ssh_password(token)
    current = (
        wake_family_settings_snapshot(family)
        if family in WAKE_FAMILIES
        else settings_snapshot(selector, board=board)
    )
    screen_defaults = _ECHO_SCREEN_DEFAULTS if _board_is_echo_screen(board) else DEFAULTS
    catalog_selected = current["wake_word"] == "custom_url" and wake_word_catalog.is_catalog_url(
        current["wake_word_url"]
    )
    catalog_field = (
        wake_word_catalog.field_payload(
            current_url=current["wake_word_url"],
            current_label=current["wake_profile_name"],
        )
        if not _selector_token(selector)
        else {"options": [], "selected_url": "", "description": ""}
    )
    wake_word_source = "catalog" if catalog_selected else current["wake_word"]
    detector_mode = _wake_detector_mode(current.get("wake_detector_mode"), current)
    oww_catalog_type = "nww" if detector_mode in {"nww", "mww_nww"} else "dual"
    oww_catalog_selected = (
        current["oww_wake_word"] == "custom_url"
        and wake_word_catalog.is_catalog_url(
            current["oww_wake_word_url"],
            model_type=oww_catalog_type,
        )
    )
    oww_catalog_field = (
        wake_word_catalog.field_payload(
            current_url=current["oww_wake_word_url"],
            current_label=current["oww_profile_name"],
            model_type=oww_catalog_type,
        )
        if not _selector_token(selector)
        else {"options": [], "selected_url": "", "description": ""}
    )
    oww_wake_word_source = "catalog" if oww_catalog_selected else current["oww_wake_word"]
    oww_custom_source_label = "Custom Wake Bundle"
    oww_catalog_source_label = "Wake Bundle Catalog"
    oww_source_description = (
        "Choose the bundled Hey Tater models or one trainer-produced wake bundle. "
        "MWW-only satellites use its microWakeWord model; capable satellites use the "
        "selected MWW, OWW, or NWW policy."
    )
    oww_url_label = "Wake Bundle URL"
    oww_url_description = (
        "Use the .wake-bundle.json from a Tater trainer. Tater validates the selected models, "
        "sends microWakeWord to MWW-only satellites, and sends the requested detector mode to capable firmware."
    )
    fields = [
        {
            "key": "wake_section",
            "label": "Wake Word",
            "type": "section",
            "description": "Select the active on-device wake model and its source.",
        },
        {
            "key": "wake_engine",
            "label": "Wake Engine",
            "type": "select",
            "value": current["wake_engine"],
            "default": DEFAULTS["wake_engine"],
            "options": [
                {"value": "micro_wake_word", "label": "On-device Wake"},
                {"value": "button", "label": "Button Only"},
                {"value": "server", "label": "Server Wake Stream"},
                {"value": "off", "label": "Off"},
            ],
        },
        {
            "key": "wake_detector_mode",
            "label": "Wake Detection Mode",
            "type": "select",
            "value": current.get("wake_detector_mode", "mww_oww"),
            "default": "mww_oww",
            "options": [
                {"value": "mww", "label": "MWW", "group": "single"},
                {"value": "oww", "label": "OWW", "group": "single"},
                {"value": "nww", "label": "NWW", "group": "single"},
                {"value": "mww_oww", "label": "MWW + OWW", "group": "dual"},
                {"value": "mww_nww", "label": "MWW + NWW", "group": "dual"},
            ],
            "description": (
                "Single modes use one detector on capable satellites. Dual modes require MWW plus the selected "
                "companion. ESP32 and other MWW-only devices always keep the bundle's MWW fallback."
            ),
        },
        {
            "key": "wake_mww_enabled",
            "label": "Enable microWakeWord",
            "type": "checkbox",
            "value": current["wake_mww_enabled"],
            "default": DEFAULTS["wake_mww_enabled"],
            "description": "Use microWakeWord as the first on-device detector. With openWakeWord also enabled, both models must agree.",
        },
        {
            "key": "wake_oww_enabled",
            "label": "Enable openWakeWord",
            "type": "checkbox",
            "value": current["wake_oww_enabled"],
            "default": DEFAULTS["wake_oww_enabled"],
            "description": "Use openWakeWord as a second opinion, or as the primary detector when microWakeWord is disabled.",
        },
        {
            "key": "wake_nww_enabled",
            "label": "Enable NanoWakeWord",
            "type": "checkbox",
            "value": current["wake_nww_enabled"],
            "default": DEFAULTS["wake_nww_enabled"],
            "description": "Use a NanoWakeWord CNN or GRU model alone, or alongside microWakeWord.",
        },
        {
            "key": "wake_word",
            "label": "Wake Word",
            "type": "select",
            "value": wake_word_source,
            "default": DEFAULTS["wake_word"],
            "options": [{"value": value, "label": label} for value, label in WAKE_WORD_SOURCE_ROWS],
            "description": "Use Tater's built-in model, choose a versioned model from the official catalog, or provide a custom microWakeWord JSON package.",
        },
        {
            "key": "wake_word_catalog_url",
            "label": "Wake Word Catalog",
            "type": "select",
            "value": catalog_field.get("selected_url") or "",
            "default": "",
            "options": catalog_field.get("options") if isinstance(catalog_field.get("options"), list) else [],
            "show_when": {"source_key": "wake_word", "equals": "catalog"},
            "description": _text(catalog_field.get("description")),
        },
        {
            "key": "wake_word_url",
            "label": "Wake Bundle or MWW JSON URL",
            "type": "text",
            "value": current["wake_word_url"] if current["wake_word"] == "custom_url" else "",
            "default": DEFAULTS["wake_word_url"],
            "placeholder": "https://example.local/wake_word.wake-bundle.json",
            "show_when": {"source_key": "wake_word", "equals": "custom_url"},
            "description": (
                "Use the trainer's .wake-bundle.json so the same package can serve every detector mode, "
                "or provide a simple ESPHome microWakeWord JSON package."
            ),
        },
        {
            "key": "wake_profile_name",
            "label": "Loaded Profile",
            "type": "readonly",
            "value": current["wake_profile_name"],
            "description": current["wake_profile_error"] or "Name from the selected built-in profile or custom wake JSON.",
        },
        {
            "key": "oww_wake_word",
            "label": "Wake Word Bundle",
            "type": "select",
            "value": oww_wake_word_source,
            "default": DEFAULTS["oww_wake_word"],
            "options": [
                {"value": "hey_tater", "label": "Built-in Hey Tater"},
                {"value": "catalog", "label": oww_catalog_source_label},
                {"value": "custom_url", "label": oww_custom_source_label},
            ],
            "description": oww_source_description,
        },
        {
            "key": "oww_wake_word_catalog_url",
            "label": oww_catalog_source_label,
            "type": "select",
            "value": oww_catalog_field.get("selected_url") or "",
            "default": "",
            "options": (
                oww_catalog_field.get("options")
                if isinstance(oww_catalog_field.get("options"), list)
                else []
            ),
            "show_when": {"source_key": "oww_wake_word", "equals": "catalog"},
            "description": _text(oww_catalog_field.get("description")),
        },
        {
            "key": "oww_wake_word_url",
            "label": oww_url_label,
            "type": "text",
            "value": current["oww_wake_word_url"] if current["oww_wake_word"] == "custom_url" else "",
            "default": DEFAULTS["oww_wake_word_url"],
            "placeholder": "https://example.local/hey_tater.wake-bundle.json",
            "show_when": {"source_key": "oww_wake_word", "equals": "custom_url"},
            "description": oww_url_description,
        },
        {
            "key": "oww_profile_name",
            "label": "Loaded Wake Bundle",
            "type": "readonly",
            "value": current["oww_profile_name"],
            "description": current["oww_profile_error"] or (
                (
                    f"Standalone threshold {current['oww_profile_threshold']} · "
                    f"dual confirmation {current['oww_profile_confirmation_threshold']} · "
                    f"{current['oww_profile_confirmation_patience']} consecutive frames."
                )
                if current["wake_mww_enabled"] and current["wake_oww_enabled"]
                else f"Threshold {current['oww_profile_threshold']} · {current['oww_profile_patience']} consecutive frames."
            ),
        },
        {
            "key": "nww_architecture",
            "label": "NanoWakeWord Architecture",
            "type": "select",
            "value": current["nww_architecture"],
            "default": DEFAULTS["nww_architecture"],
            "options": [
                {
                    "value": architecture,
                    "label": architecture.upper(),
                    "disabled": architecture not in {
                        item.strip().lower()
                        for item in _text(current.get("nww_available_architectures")).split(",")
                        if item.strip()
                    },
                }
                for architecture in ("cnn", "gru")
            ],
            "description": "Only architectures present in the selected wake bundle are available.",
        },
        {
            "key": "wake_tuning_section",
            "label": "Wake Tuning",
            "type": "section",
            "description": "Trainer JSON values are the base. Sensitivity and environment adjust how the satellite accepts wakes in this room.",
        },
        {
            "key": "wake_profile_threshold",
            "label": "JSON Threshold",
            "type": "readonly",
            "value": current["wake_profile_threshold"],
            "description": "From micro.probability_cutoff in the wake JSON, or the built-in profile default.",
        },
        {
            "key": "wake_profile_sliding_window",
            "label": "JSON Sliding Window",
            "type": "readonly",
            "value": current["wake_profile_sliding_window"],
            "description": "From micro.sliding_window_size in the wake JSON, or the built-in profile default.",
        },
        {
            "key": "wake_profile_close_miss_threshold",
            "label": "JSON Close Miss Threshold",
            "type": "readonly",
            "value": current["wake_profile_close_miss_threshold"],
            "description": "From tater_native.close_miss_threshold in the wake JSON, or the built-in profile default.",
        },
        {
            "key": "wake_sensitivity",
            "label": "Wake Sensitivity",
            "type": "select",
            "value": current["wake_sensitivity"],
            "default": DEFAULTS["wake_sensitivity"],
            "options": [{"value": value, "label": label} for value, label in WAKE_SENSITIVITY_ROWS],
            "description": f"Effective threshold now: {current['wake_threshold']}. Conservative reduces false wakes; High helps far or soft voices.",
        },
        {
            "key": "wake_environment",
            "label": "Wake Environment",
            "type": "select",
            "value": current["wake_environment"],
            "default": DEFAULTS["wake_environment"],
            "options": [{"value": value, "label": label} for value, label in WAKE_ENVIRONMENT_ROWS],
            "description": "Balanced is the default. TV Nearby and Strict require stronger wake shape before the firmware opens the mic.",
        },
        {
            "key": "wake_tuning_override_enabled",
            "label": "Override JSON Tuning",
            "type": "checkbox",
            "value": current["wake_tuning_override_enabled"],
            "default": DEFAULTS["wake_tuning_override_enabled"],
            "description": "Stores threshold, window, and close-miss overrides for this selected wake profile. Threshold override replaces the sensitivity-adjusted value.",
        },
        {
            "key": "wake_threshold_override",
            "label": "Threshold Override",
            "type": "number",
            "value": current["wake_threshold_override"],
            "default": DEFAULTS["wake_threshold_override"],
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "show_when": {"source_key": "wake_tuning_override_enabled", "equals": True},
            "description": f"Effective threshold now: {current['wake_threshold']}. Higher values reduce false wakes.",
        },
        {
            "key": "wake_sliding_window_override",
            "label": "Sliding Window Override",
            "type": "number",
            "value": current["wake_sliding_window_override"],
            "default": DEFAULTS["wake_sliding_window_override"],
            "min": 1,
            "max": 10,
            "step": 1,
            "show_when": {"source_key": "wake_tuning_override_enabled", "equals": True},
            "description": f"Effective window now: {current['wake_sliding_window']}. Larger values are more conservative.",
        },
        {
            "key": "wake_close_miss_threshold_override",
            "label": "Close Miss Threshold Override",
            "type": "number",
            "value": current["wake_close_miss_threshold_override"],
            "default": DEFAULTS["wake_close_miss_threshold_override"],
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "show_when": {"source_key": "wake_tuning_override_enabled", "equals": True},
            "description": f"Effective close-miss threshold now: {current['close_miss_threshold']}. Lower values capture more near misses.",
        },
        {
            "key": "model_override_section",
            "label": "Model Overrides",
            "type": "section",
            "description": "Optional expert overrides for the active wake engines. With overrides off, every model uses the values validated by its wake bundle.",
        },
        {
            "key": "mww_tuning_override_enabled",
            "label": "Override MWW Bundle Tuning",
            "type": "checkbox",
            "value": current["mww_tuning_override_enabled"],
            "default": DEFAULTS["mww_tuning_override_enabled"],
            "description": "Replace the selected microWakeWord package threshold and sliding window.",
        },
        {
            "key": "mww_threshold_override",
            "label": "MWW Threshold",
            "type": "number",
            "value": current["mww_threshold_override"],
            "default": current["wake_profile_threshold"],
            "min": 0.01,
            "max": 0.99,
            "step": 0.01,
            "description": f"Bundle value: {current['wake_profile_threshold']}. Higher values reduce false wakes.",
        },
        {
            "key": "mww_sliding_window_override",
            "label": "MWW Sliding Window",
            "type": "number",
            "value": current["mww_sliding_window_override"],
            "default": current["wake_profile_sliding_window"],
            "min": 1,
            "max": 10,
            "step": 1,
            "description": f"Bundle value: {current['wake_profile_sliding_window']}. Larger values require a longer stable detection.",
        },
        {
            "key": "oww_tuning_override_enabled",
            "label": "Override OWW Bundle Tuning",
            "type": "checkbox",
            "value": current["oww_tuning_override_enabled"],
            "default": DEFAULTS["oww_tuning_override_enabled"],
            "description": "Replace the active openWakeWord threshold and consecutive-frame patience.",
        },
        {
            "key": "oww_threshold_override",
            "label": "OWW Threshold",
            "type": "number",
            "value": current["oww_threshold_override"],
            "default": current["oww_active_profile_threshold"],
            "standalone_default": current["oww_profile_threshold"],
            "confirmation_default": current["oww_profile_confirmation_threshold"],
            "min": 0.01,
            "max": 1.0,
            "step": 0.01,
            "description": f"Active bundle value: {current['oww_active_profile_threshold']}.",
        },
        {
            "key": "oww_patience_override",
            "label": "OWW Patience",
            "type": "number",
            "value": current["oww_patience_override"],
            "default": current["oww_active_profile_patience"],
            "standalone_default": current["oww_profile_patience"],
            "confirmation_default": current["oww_profile_confirmation_patience"],
            "min": 1,
            "max": 20,
            "step": 1,
            "description": f"Active bundle value: {current['oww_active_profile_patience']} consecutive 80 ms scores.",
        },
        {
            "key": "nww_tuning_override_enabled",
            "label": "Override NWW Bundle Tuning",
            "type": "checkbox",
            "value": current["nww_tuning_override_enabled"],
            "default": DEFAULTS["nww_tuning_override_enabled"],
            "description": "Replace the selected NanoWakeWord architecture's threshold and consecutive-frame patience.",
        },
        {
            "key": "nww_threshold_override",
            "label": "NWW Threshold",
            "type": "number",
            "value": current["nww_threshold_override"],
            "default": current["nww_profile_threshold"],
            "cnn_default": current["nww_cnn_profile_threshold"],
            "gru_default": current["nww_gru_profile_threshold"],
            "min": 0.01,
            "max": 1.0,
            "step": 0.01,
            "description": f"{current['nww_architecture'].upper()} bundle value: {current['nww_profile_threshold']}.",
        },
        {
            "key": "nww_patience_override",
            "label": "NWW Patience",
            "type": "number",
            "value": current["nww_patience_override"],
            "default": current["nww_profile_patience"],
            "cnn_default": current["nww_cnn_profile_patience"],
            "gru_default": current["nww_gru_profile_patience"],
            "min": 1,
            "max": 20,
            "step": 1,
            "description": f"{current['nww_architecture'].upper()} bundle value: {current['nww_profile_patience']} consecutive 80 ms scores.",
        },
        {
            "key": "training_section",
            "label": "Trainer Feedback",
            "type": "section",
            "description": "Optional capture settings for improving wake models. Close-miss tuning comes from the selected wake profile unless overrides are enabled.",
        },
        {
            "key": "capture_wake_audio",
            "label": "Send Good Wakes To Trainer",
            "type": "checkbox",
            "value": current["capture_wake_audio"],
            "default": DEFAULTS["capture_wake_audio"],
            "description": "Capture confirmed wake clips for the trainer when the native capture hook is enabled in firmware.",
        },
        {
            "key": "capture_close_misses",
            "label": "Send Close Misses To Trainer",
            "type": "checkbox",
            "value": current["capture_close_misses"],
            "default": DEFAULTS["capture_close_misses"],
            "description": "Capture near-wake clips for wake-word tuning when the native capture hook is enabled in firmware.",
        },
        {
            "key": "trainer_app_url",
            "label": "Trainer App URL",
            "type": "text",
            "value": current["trainer_app_url"],
            "default": DEFAULTS["trainer_app_url"],
            "placeholder": "http://trainer.local:8789",
        },
        {
            "key": "playback_section",
            "label": "Audio Feedback",
            "type": "section",
            "description": "Wake acknowledgement and local playback controls.",
        },
        {
            "key": "wake_sound_enabled",
            "label": "Wake Sound",
            "type": "checkbox",
            "value": current["wake_sound_enabled"],
            "default": DEFAULTS["wake_sound_enabled"],
        },
        {
            "key": "wake_sound",
            "label": "Wake Sound Choice",
            "type": "select",
            "value": current["wake_sound"],
            "default": DEFAULTS["wake_sound"],
            "options": [{"value": value, "label": row["label"]} for value, row in WAKE_SOUNDS.items()],
            "description": "Built-in choices play embedded on-device WAV assets from the native firmware image.",
        },
        {
            "key": "wake_sound_url",
            "label": "Custom Wake Sound URL",
            "type": "text",
            "value": current["wake_sound_url"] if current["wake_sound"] == "custom" else "",
            "default": DEFAULTS["wake_sound_url"],
            "placeholder": "https://example.local/wake.wav",
            "show_when": {"source_key": "wake_sound", "equals": "custom"},
            "description": "The satellite downloads and caches this WAV locally, then plays it on future wakes.",
        },
        {
            "key": "device_section",
            "label": "Device Behavior",
            "type": "section",
            "description": "Satellite behavior and diagnostics.",
        },
        {
            "key": "continued_chat",
            "label": "Continued Chat Reopen",
            "type": "checkbox",
            "value": current["continued_chat"],
            "default": DEFAULTS["continued_chat"],
        },
        {
            "key": "barge_in_enabled",
            "label": "Barge-In During Replies",
            "type": "checkbox",
            "value": current["barge_in_enabled"],
            "default": DEFAULTS["barge_in_enabled"],
            "description": "When enabled, the local wake engine can interrupt current satellite playback and open the mic.",
        },
        {
            "key": "volume_percent",
            "label": "Volume",
            "type": "number",
            "value": current["volume_percent"],
            "default": DEFAULTS["volume_percent"],
            "min": 0,
            "max": 100,
            "step": 1,
        },
        {
            "key": "output_chain_section",
            "label": "Speaker Equalizer",
            "type": "section",
            "description": "Tune this satellite's speaker response. Its hardware speaker filter remains active underneath these controls.",
        },
        {
            "key": "eq_bands",
            "label": "Eight-Band Equalizer",
            "type": "equalizer",
            "value": current["eq_bands"],
            "default": DEFAULTS["eq_bands"],
            "bands": [
                {"label": label, "index": index}
                for index, label in enumerate(_EQ_BAND_LABELS)
            ],
            "min": -12,
            "max": 12,
            "step": 0.5,
            "full_width": True,
            "description": "Changes apply on the satellite without a firmware rebuild. Reset returns every band to 0 dB.",
        },
        {
            "key": "eq_loudness",
            "label": "Presence Boost",
            "type": "checkbox",
            "value": current["eq_loudness"],
            "default": DEFAULTS["eq_loudness"],
            "description": "Adds a broad +5 dB presence lift centered near 2.5 kHz for clearer speech.",
        },
        {
            "key": "bass_guard_enabled",
            "label": "Bass Protection",
            "type": "checkbox",
            "value": current["bass_guard_enabled"],
            "default": DEFAULTS["bass_guard_enabled"],
            "description": "Protects the small driver from low-frequency over-excursion. Keep this enabled for normal use.",
        },
        {
            "key": "bass_guard_db",
            "label": "Bass Protection Floor (dB)",
            "type": "number",
            "value": current["bass_guard_db"],
            "default": DEFAULTS["bass_guard_db"],
            "min": -60,
            "max": 0,
            "step": 1,
            "show_when": {"source_key": "bass_guard_enabled", "equals": True},
            "description": "Maximum low-bass reduction when protection is active. The safe default is -30 dB.",
        },
        {
            "key": "limiter_enabled",
            "label": "Peak Limiter",
            "type": "checkbox",
            "value": current["limiter_enabled"],
            "default": DEFAULTS["limiter_enabled"],
            "description": "Prevents boosted EQ bands from clipping the speaker output.",
        },
        {
            "key": "limiter_threshold_db",
            "label": "Limiter Ceiling (dBFS)",
            "type": "number",
            "value": current["limiter_threshold_db"],
            "default": DEFAULTS["limiter_threshold_db"],
            "min": -12,
            "max": 0,
            "step": 0.5,
            "show_when": {"source_key": "limiter_enabled", "equals": True},
        },
        {
            "key": "limiter_release_ms",
            "label": "Limiter Release (ms)",
            "type": "number",
            "value": current["limiter_release_ms"],
            "default": DEFAULTS["limiter_release_ms"],
            "min": 20,
            "max": 1000,
            "step": 10,
            "show_when": {"source_key": "limiter_enabled", "equals": True},
        },
        {
            "key": "audio_output_section",
            "label": "Sat1 Audio Output",
            "type": "section",
            "description": "Choose the Sat1 internal speaker, 3.5 mm line-out, or both outputs.",
        },
        {
            "key": "audio_output_mode",
            "label": "Audio Output",
            "type": "select",
            "value": current["audio_output_mode"],
            "default": DEFAULTS["audio_output_mode"],
            "options": [
                {"value": "auto", "label": "Automatic (Jack Detection)"},
                {"value": "internal", "label": "Internal Speaker"},
                {"value": "aux", "label": "AUX / Line-Out"},
                {"value": "both", "label": "Internal + AUX"},
            ],
            "description": "Automatic switches to the 3.5 mm output when a plug is detected. AUX requires powered speakers or an external amplifier.",
        },
        {
            "key": "screen_section",
            "label": "Display & Night",
            "type": "section",
            "description": "Control this satellite's screen, automatic room-light response, and overnight behavior.",
        },
        {
            "key": "screen_brightness",
            "label": "Screen Brightness (%)",
            "type": "number",
            "value": current["screen_brightness"],
            "default": screen_defaults["screen_brightness"],
            "min": 0,
            "max": 100,
            "step": 1,
            "description": "Normal display brightness. Setting this to 0 turns the backlight off.",
        },
        {
            "key": "screen_auto_brightness_enabled",
            "label": "Automatic Brightness",
            "type": "checkbox",
            "value": current["screen_auto_brightness_enabled"],
            "default": screen_defaults["screen_auto_brightness_enabled"],
            "description": "Follow the room's ambient-light sensor smoothly. Screen Brightness remains the maximum.",
        },
        {
            "key": "screen_auto_brightness_minimum",
            "label": "Dimmest Automatic Level (%)",
            "type": "number",
            "value": current["screen_auto_brightness_minimum"],
            "default": screen_defaults["screen_auto_brightness_minimum"],
            "min": 1,
            "max": 50,
            "step": 1,
            "show_when": {"source_key": "screen_auto_brightness_enabled", "equals": True},
            "description": "Lowest fraction of the selected brightness used in a dark room.",
        },
        {
            "key": "screen_night_mode_enabled",
            "label": "Scheduled Night Mode",
            "type": "checkbox",
            "value": current["screen_night_mode_enabled"],
            "default": screen_defaults["screen_night_mode_enabled"],
            "description": "Use a dim display, red bedside clock, or turn the screen off during a daily local-time window.",
        },
        {
            "key": "screen_night_start",
            "label": "Dim At",
            "type": "time",
            "value": current["screen_night_start"],
            "default": screen_defaults["screen_night_start"],
            "show_when": {"source_key": "screen_night_mode_enabled", "equals": True},
        },
        {
            "key": "screen_night_end",
            "label": "Restore At",
            "type": "time",
            "value": current["screen_night_end"],
            "default": screen_defaults["screen_night_end"],
            "show_when": {"source_key": "screen_night_mode_enabled", "equals": True},
            "description": "The normal brightness returns at this local time. Overnight schedules are supported.",
        },
        {
            "key": "screen_night_display",
            "label": "Night Display",
            "type": "select",
            "value": current["screen_night_display"],
            "default": screen_defaults["screen_night_display"],
            "options": [
                {"value": "red_clock", "label": "Dim Red Clock"},
                {"value": "dim", "label": "Dim Current Display"},
                {"value": "off", "label": "Screen Off"},
            ],
            "show_when": {"source_key": "screen_night_mode_enabled", "equals": True},
            "description": "The red clock appears only while the satellite is idle; calls, timers, and voice activity remain visible at night brightness.",
        },
        {
            "key": "screen_night_brightness",
            "label": "Night Brightness (%)",
            "type": "number",
            "value": current["screen_night_brightness"],
            "default": screen_defaults["screen_night_brightness"],
            "min": 0,
            "max": 100,
            "step": 1,
            "show_when": {"source_key": "screen_night_mode_enabled", "equals": True},
            "description": "Brightness used between Dim At and Restore At. Zero turns the backlight off regardless of the selected night display.",
        },
        {
            "key": "display_theme_section",
            "label": "Appearance & Photos",
            "type": "section",
            "description": "Choose this satellite's color personality, photo rotation, and voice-state edge effects. Screen-satellite uploads are managed below.",
        },
        {
            "key": "display_theme",
            "label": "Color Theme",
            "type": "select",
            "value": current["display_theme"],
            "default": DEFAULTS["display_theme"],
            "options": [
                {"value": row["value"], "label": row["label"]}
                for row in DISPLAY_THEMES
            ],
            "description": "Changes the screen accents live. Listening, thinking, working, and reply states follow the selected theme.",
        },
        {
            "key": "display_wallpaper_interval_minutes",
            "label": "Photo Rotation",
            "type": "select",
            "value": current["display_wallpaper_interval_minutes"],
            "default": DEFAULTS["display_wallpaper_interval_minutes"],
            "options": [dict(row) for row in DISPLAY_WALLPAPER_INTERVALS],
            "description": "Choose how often this satellite advances to the next bundled or uploaded photo. A single uploaded photo always remains fixed.",
        },
        {
            "key": "display_edge_glow",
            "label": "Voice State Edge Glow",
            "type": "checkbox",
            "value": current["display_edge_glow"],
            "default": DEFAULTS["display_edge_glow"],
            "description": "Show the selected theme's state colors while the assistant listens, thinks, works, and replies. Reply glow reacts to the assistant's voice.",
        },
        {
            "key": "display_theme_preview",
            "label": "Theme Preview",
            "type": "display_theme_preview",
            "themes": [dict(row) for row in DISPLAY_THEMES],
        },
        {
            "key": "led_section",
            "label": "LED Settings",
            "type": "section",
            "description": "Choose the shared LED color and the animations used for voice and music.",
        },
        {
            "key": "led_brightness",
            "label": "LED Brightness (%)",
            "type": "number",
            "value": current["led_brightness"],
            "default": DEFAULTS["led_brightness"],
            "min": 0,
            "max": 100,
            "step": 1,
            "description": "Brightness percentage for this satellite's LEDs.",
        },
        {
            "key": "led_color",
            "label": "LED Color",
            "type": "color",
            "value": current["led_color"],
            "default": DEFAULTS["led_color"],
            "description": "Applies to listening, thinking, tool call, reply, and music animations. Setup, error, OTA, and connection colors stay reserved.",
        },
        {
            "key": "led_listening_animation",
            "label": "Listening Animation",
            "type": "select",
            "value": current["led_listening_animation"],
            "default": DEFAULTS["led_listening_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_LISTENING_ANIMATIONS],
            "description": "Choose No Animation to keep the LEDs dark while listening.",
        },
        {
            "key": "led_thinking_animation",
            "label": "Thinking Animation",
            "type": "select",
            "value": current["led_thinking_animation"],
            "default": DEFAULTS["led_thinking_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_THINKING_ANIMATIONS],
            "description": "Choose No Animation to keep the LEDs dark while thinking.",
        },
        {
            "key": "led_tool_call_animation",
            "label": "Tool Call Animation",
            "type": "select",
            "value": current["led_tool_call_animation"],
            "default": DEFAULTS["led_tool_call_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_TOOL_CALL_ANIMATIONS],
            "description": "Choose No Animation to keep the LEDs dark during tool calls.",
        },
        {
            "key": "led_replying_animation",
            "label": "Replying Animation",
            "type": "select",
            "value": current["led_replying_animation"],
            "default": DEFAULTS["led_replying_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_REPLYING_ANIMATIONS],
            "description": "Choose No Animation to keep the LEDs dark while Tater replies.",
        },
        {
            "key": "led_music_animation",
            "label": "Music Animation",
            "type": "select",
            "value": current["led_music_animation"],
            "default": DEFAULTS["led_music_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_MUSIC_ANIMATIONS],
            "description": "Audio-reactive ring animation for music playback, including Sendspin. Track colors are used when the player provides them. Choose No Animation to keep the ring dark.",
        },
        {
            "key": "led_preview",
            "label": "LED Preview",
            "type": "led_preview",
            "states": [
                {"label": "Listening", "animation_key": "led_listening_animation"},
                {"label": "Thinking", "animation_key": "led_thinking_animation"},
                {"label": "Tool Call", "animation_key": "led_tool_call_animation"},
                {"label": "Replying", "animation_key": "led_replying_animation"},
                {"label": "Music", "animation_key": "led_music_animation"},
            ],
        },
        {
            "key": "logging_level",
            "label": "Logging Level",
            "type": "select",
            "value": current["logging_level"],
            "default": DEFAULTS["logging_level"],
            "options": [
                {"value": "error", "label": "Error"},
                {"value": "warning", "label": "Warning"},
                {"value": "info", "label": "Info"},
                {"value": "debug", "label": "Debug"},
            ],
        },
        {
            "key": "ssh_section",
            "label": "Remote Maintenance",
            "type": "section",
            "description": "Turn on an SSH shell for this satellite over its active Wi-Fi or Ethernet connection.",
        },
        {
            "key": "ssh_enabled",
            "label": "SSH Access",
            "type": "checkbox",
            "value": current["ssh_enabled"],
            "default": DEFAULTS["ssh_enabled"],
            "description": "Off by default. Turn this on only while you want remote shell access to this satellite.",
            "full_width": True,
        },
        {
            "key": "ssh_login",
            "label": "SSH Login",
            "type": "readonly",
            "value": "root",
            "description": "Use this login from Terminal after saving SSH Access.",
            "show_when": {"source_key": "ssh_enabled", "equals": True},
            "copyable": True,
        },
        {
            "key": "ssh_password",
            "label": "SSH Password",
            "type": "password",
            "value": ssh_password,
            "description": "Unique to this satellite. Use Show or Copy, or generate a new readable password before saving.",
            "show_when": {"source_key": "ssh_enabled", "equals": True},
            "copyable": True,
            "revealable": True,
            "generatable": True,
            "generator": "tater_password",
        },
    ]
    if _selector_token(selector):
        fields = [
            field
            for field in fields
            if _text(field.get("key")) not in GLOBAL_SATELLITE_DEVICE_FIELD_KEYS
        ]
    if not _board_supports_led_settings(board):
        fields = [field for field in fields if _text(field.get("key")) not in _LED_FIELD_KEYS]
    elif _is_s420_board(board):
        fields = [
            field
            for field in fields
            if _text(field.get("key")) != "led_tool_call_animation"
        ]
        for field in fields:
            key = _text(field.get("key"))
            if key == "led_section":
                field.update(
                    {
                        "label": "Tater S420 Status Light",
                        "description": "Customize the S420's single top status light. Changes apply live during the voice cycle.",
                    }
                )
            elif key == "led_brightness":
                field.update(
                    {
                        "label": "Status Light Brightness (%)",
                        "description": "Brightness for the single visible S420 status light.",
                    }
                )
            elif key == "led_color":
                field.update(
                    {
                        "label": "Tater Status Color",
                        "description": "Shared listening, thinking, and reply color. Setup, mute, error, and OTA colors stay reserved.",
                    }
                )
            elif key in S420_LED_DEFAULTS:
                animations = S420_LED_ANIMATIONS
                if key != "led_replying_animation":
                    animations = [row for row in animations if row[0] != "audio_glow"]
                field["options"] = [
                    {"value": value, "label": label}
                    for value, label in animations
                ]
                field["default"] = S420_LED_DEFAULTS[key]
            elif key == "led_preview":
                field["single_light"] = True
                field["states"] = [
                    {"label": "Listening", "animation_key": "led_listening_animation"},
                    {"label": "Thinking", "animation_key": "led_thinking_animation"},
                    {"label": "Replying", "animation_key": "led_replying_animation"},
                ]
    if not (_selector_token(selector) and _board_supports_music_led_animation(board)):
        fields = [field for field in fields if _text(field.get("key")) != "led_music_animation"]
        for field in fields:
            if _text(field.get("key")) == "led_preview":
                field["states"] = [
                    state
                    for state in field.get("states", [])
                    if _text(state.get("animation_key")) != "led_music_animation"
                ]
    if not (
        _selector_token(selector)
        and _board_supports_screen_settings(board)
        and _supports_screen_power_management(board, capabilities)
    ):
        fields = [field for field in fields if _text(field.get("key")) not in _SCREEN_FIELD_KEYS]
    elif _board_is_s3_screen(board):
        # Preserve the established S3 Box wire contract until its firmware
        # advertises the Echo-style ambient-light and night-display features.
        echo_only = {
            "screen_auto_brightness_enabled",
            "screen_auto_brightness_minimum",
            "screen_night_display",
        }
        fields = [field for field in fields if _text(field.get("key")) not in echo_only]
    if not (_selector_token(selector) and _board_supports_display_theme(board)):
        fields = [field for field in fields if _text(field.get("key")) not in _DISPLAY_THEME_FIELD_KEYS]
    if not (_selector_token(selector) and _board_supports_display_photos(board)):
        fields = [field for field in fields if _text(field.get("key")) not in _DISPLAY_EDGE_FIELD_KEYS]
    if not (_selector_token(selector) and _is_sat1_board(board)):
        fields = [field for field in fields if _text(field.get("key")) not in _AUDIO_OUTPUT_FIELD_KEYS]
    if not (_selector_token(selector) and _supports_output_chain(capabilities)):
        fields = [field for field in fields if _text(field.get("key")) not in _OUTPUT_CHAIN_FIELD_KEYS]
    if not (_selector_token(selector) and _supports_ssh_management(capabilities)):
        fields = [
            field
            for field in fields
            if _text(field.get("key")) not in {"ssh_section", "ssh_enabled", "ssh_login", "ssh_password"}
        ]
    return fields


def _global_settings_sections(
    groups: List[tuple[str, str, tuple[str, ...]]],
    *,
    wake_family: Any = "",
) -> List[Dict[str, Any]]:
    fields = {
        _text(field.get("key")): field
        for field in settings_fields(wake_family=wake_family)
        if isinstance(field, dict) and _text(field.get("key"))
    }
    return [
        {
            "label": label,
            "description": description,
            "fields": [dict(fields[key]) for key in keys if key in fields],
        }
        for label, description, keys in groups
    ]


def mww_model_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "microWakeWord Satellites",
                "Used by ESP satellites and any Tater satellite that does not advertise openWakeWord support.",
                (
                    "wake_engine", "wake_word", "wake_word_catalog_url", "wake_word_url",
                    "wake_profile_name",
                ),
            ),
        ],
        wake_family="mww",
    )


def shared_wake_model_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "All Satellites",
                (
                    "Choose one wake phrase for every satellite. Use MWW, OWW, or NWW alone on capable "
                    "firmware, or require MWW plus OWW/NWW. MWW-only satellites keep the bundle fallback."
                ),
                (
                    "wake_engine", "wake_detector_mode",
                    "wake_word", "wake_word_catalog_url", "wake_word_url", "wake_profile_name",
                    "oww_wake_word", "oww_wake_word_catalog_url", "oww_wake_word_url",
                    "oww_profile_name", "nww_architecture",
                    "mww_tuning_override_enabled", "mww_threshold_override", "mww_sliding_window_override",
                    "oww_tuning_override_enabled", "oww_threshold_override", "oww_patience_override",
                    "nww_tuning_override_enabled", "nww_threshold_override", "nww_patience_override",
                ),
            ),
        ],
        wake_family="echo",
    )


def echo_model_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "Echo Satellites",
                "Choose MWW, OWW, NWW, MWW + OWW, or MWW + NWW for capable Echo firmware.",
                (
                    "wake_engine", "wake_detector_mode",
                    "wake_word", "wake_word_catalog_url", "wake_word_url", "wake_profile_name",
                    "oww_wake_word", "oww_wake_word_catalog_url", "oww_wake_word_url",
                    "oww_profile_name", "nww_architecture",
                ),
            ),
        ],
        wake_family="echo",
    )


def wake_trainer_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "Trainer Feedback",
                "Choose which wake clips Tater sends for model improvement, and manage the secure trainer connection that receives them.",
                ("capture_wake_audio", "capture_close_misses", "trainer_app_url"),
            ),
        ]
    )


def global_model_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "Wake Word",
                "",
                (
                    "wake_engine", "wake_mww_enabled", "wake_oww_enabled", "wake_nww_enabled",
                    "wake_word", "wake_word_catalog_url", "wake_word_url",
                    "wake_profile_name", "oww_wake_word", "oww_wake_word_catalog_url",
                    "oww_wake_word_url", "oww_profile_name", "nww_architecture",
                ),
            ),
            (
                "Trainer Feedback",
                "Choose which wake clips Tater sends for model improvement, and manage the secure trainer connection that receives them.",
                ("capture_wake_audio", "capture_close_misses", "trainer_app_url"),
            ),
        ]
    )


def global_voice_runtime_settings_sections() -> List[Dict[str, Any]]:
    return _global_settings_sections(
        [
            (
                "Wake Sound & Conversation",
                "",
                ("wake_sound_enabled", "wake_sound", "wake_sound_url", "continued_chat", "barge_in_enabled"),
            ),
        ]
    )


def global_settings_sections() -> List[Dict[str, Any]]:
    """Return every shared satellite setting section for compatibility callers."""
    return _global_settings_sections(
        [
            (
                "Wake Word",
                "",
                (
                    "wake_engine", "wake_mww_enabled", "wake_oww_enabled", "wake_nww_enabled",
                    "wake_word", "wake_word_catalog_url", "wake_word_url",
                    "wake_profile_name", "oww_wake_word", "oww_wake_word_catalog_url",
                    "oww_wake_word_url", "oww_profile_name", "nww_architecture",
                ),
            ),
            (
                "Trainer Feedback",
                "Choose which wake clips Tater sends for model improvement, and manage the secure trainer connection that receives them.",
                ("capture_wake_audio", "capture_close_misses", "trainer_app_url"),
            ),
            (
                "Wake Sound & Conversation",
                "",
                ("wake_sound_enabled", "wake_sound", "wake_sound_url", "continued_chat", "barge_in_enabled"),
            ),
        ]
    )


def resolve_wake_word_source_values(values: Dict[str, Any]) -> Dict[str, Any]:
    resolved = dict(values or {})
    source = _lower(resolved.get("wake_word"))
    if source == "catalog":
        resolved["wake_word"] = "custom_url"
        resolved["wake_word_url"] = wake_word_catalog.require_catalog_url(
            resolved.get("wake_word_catalog_url")
        )
    oww_source = _lower(resolved.get("oww_wake_word"))
    if oww_source == "catalog":
        detector_mode = _lower(resolved.get("wake_detector_mode"))
        if detector_mode == "dual":
            detector_mode = "mww_oww"
        nww_catalog_requested = detector_mode in {"nww", "mww_nww"}
        if not detector_mode:
            nww_catalog_requested = _as_bool(
                resolved.get("wake_nww_enabled"),
                False,
            )
        catalog_type = (
            "nww"
            if nww_catalog_requested
            else "dual"
        )
        resolved["oww_wake_word"] = "custom_url"
        resolved["oww_wake_word_url"] = wake_word_catalog.require_catalog_url(
            resolved.get("oww_wake_word_catalog_url"),
            model_type=catalog_type,
        )
    resolved.pop("wake_word_catalog_url", None)
    resolved.pop("oww_wake_word_catalog_url", None)
    return resolved


def save_wake_family_settings(family: Any, values: Dict[str, Any]) -> Dict[str, Any]:
    token = _lower(family)
    if token not in WAKE_FAMILIES:
        raise ValueError(f"Unsupported wake family: {family}")
    _ensure_wake_family_settings()
    incoming = resolve_wake_word_source_values(dict(values or {}))
    incoming = {
        key: value
        for key, value in incoming.items()
        if key in WAKE_FAMILY_PERSISTED_KEYS
    }
    if not incoming:
        raise ValueError("No wake-family settings were provided.")

    current_raw = _raw_wake_family_settings(token)
    current = wake_family_settings_snapshot(token)
    if token == "mww":
        mode = "mww"
    else:
        mode = _wake_detector_mode(incoming.get("wake_detector_mode"), {**current, **incoming})
    mww_enabled, oww_enabled, nww_enabled = _wake_detector_flags(mode)
    incoming.update(
        {
            "wake_detector_mode": mode,
            "wake_mww_enabled": mww_enabled,
            "wake_oww_enabled": oww_enabled,
            "wake_nww_enabled": nww_enabled,
        }
    )

    combined_source = {**DEFAULTS, **current_raw, **incoming}
    if oww_enabled or nww_enabled:
        nww_architecture = _lower(combined_source.get("nww_architecture"))
        if nww_architecture not in {"cnn", "gru"}:
            nww_architecture = str(DEFAULTS["nww_architecture"])
        incoming["nww_architecture"] = nww_architecture
        dual_profile = _dual_profile_for_save(
            incoming,
            current_raw,
            required_engine="nww" if nww_enabled else "oww",
            nww_architecture=nww_architecture,
        )
        profile = dual_profile
        oww_profile = dual_profile
        if _wake_word_value(combined_source.get("oww_wake_word")) == "custom_url":
            incoming["wake_word"] = "custom_url"
            incoming["wake_word_url"] = _text(dual_profile.get("wake_profile_source_url"))
        else:
            incoming.update(
                {
                    "wake_word": "hey_tater",
                    "wake_word_url": "",
                    "oww_wake_word": "hey_tater",
                    "oww_wake_word_url": "",
                }
            )
    elif mode == "mww":
        profile = _profile_for_save(incoming, current_raw)
        profile_error = _text(profile.get("wake_profile_error"))
        if profile_error:
            raise ValueError(profile_error)
        oww_profile = _oww_profile_from_source(
            current,
            _wake_word_value(current.get("oww_wake_word")),
            _text(current.get("oww_wake_word_url")),
        )
    normalized = normalize_settings(incoming, base={**current, **profile, **oww_profile})
    normalized["wake_detector_mode"] = mode
    normalized["wake_mww_enabled"] = mww_enabled
    normalized["wake_oww_enabled"] = oww_enabled
    normalized["wake_nww_enabled"] = nww_enabled
    changed = [
        key
        for key in WAKE_FAMILY_PERSISTED_KEYS
        if current.get(key) != normalized.get(key)
    ]
    persisted = {
        key: normalized.get(key)
        for key in WAKE_FAMILY_PERSISTED_KEYS
        if key in normalized
    }
    if changed:
        redis_client.hset(
            wake_family_hash_key(token),
            mapping={key: str(value) for key, value in persisted.items()},
        )
    return {
        "ok": True,
        "scope": "wake_family",
        "family": token,
        "settings": normalized,
        "changed_keys": sorted(changed),
        "updated": len(changed),
    }


def save_shared_wake_settings(values: Dict[str, Any]) -> Dict[str, Any]:
    """Save one wake choice and retain an MWW-only compatibility mirror.

    The Echo-capable row is canonical because a dual bundle carries both
    models. The legacy MWW-family row is mirrored for downgrade compatibility;
    current firmware payloads derive both satellite families from the
    canonical row.
    """
    result = save_wake_family_settings("echo", values)
    settings = dict(result.get("settings") or {})
    projection = {
        key: settings.get(key)
        for key in WAKE_FAMILY_PERSISTED_KEYS
        if key in settings
    }
    projection.update(
        {
            "wake_detector_mode": "mww",
            "wake_mww_enabled": True,
            "wake_oww_enabled": False,
            "wake_nww_enabled": False,
        }
    )
    try:
        redis_client.hset(
            wake_family_hash_key("mww"),
            mapping={key: str(value) for key, value in projection.items()},
        )
        redis_client.hset(
            SETTINGS_HASH_KEY,
            mapping={UNIFIED_WAKE_SETTINGS_MIGRATION_KEY: "true"},
        )
    except Exception:
        # The canonical save already succeeded. A temporary failure updating
        # the downgrade-only mirror must not make the live change look failed.
        pass
    result["scope"] = "shared_wake"
    result["family"] = "shared"
    return result


def save_settings(values: Dict[str, Any], *, selector: Any = "", board: Any = "") -> Dict[str, Any]:
    token = _selector_token(selector)
    incoming = dict(values or {})
    ssh_password_changed = False
    if token and _as_bool(incoming.get("ssh_enabled"), False):
        password = _text(incoming.get("ssh_password"))
        if not password:
            password = _ensure_ssh_password(token)
            incoming["ssh_password"] = password
        if not _valid_ssh_password(password):
            raise ValueError("SSH password must be 12-64 characters using letters, numbers, or ._@%+=-.")
    elif "ssh_password" in incoming and _text(incoming.get("ssh_password")) and not _valid_ssh_password(incoming.get("ssh_password")):
        raise ValueError("SSH password must be 12-64 characters using letters, numbers, or ._@%+=-.")
    if token and _valid_ssh_password(incoming.get("ssh_password")):
        password = _text(incoming.get("ssh_password"))
        ssh_password_changed = _text(_raw_settings(token).get("ssh_password")) != password
        if ssh_password_changed:
            redis_client.hset(settings_hash_key(token), mapping={"ssh_password": password})
    incoming.pop("ssh_password", None)
    if token:
        incoming = {
            key: value
            for key, value in incoming.items()
            if key not in GLOBAL_SATELLITE_PERSISTED_KEYS
        }
    board_defaults = _board_default_overrides(board)
    board_base = normalize_settings(board_defaults) if board_defaults else None
    global_raw = _global_settings_with_migration()
    device_raw = _raw_settings(token) if token else {}
    if token:
        device_raw = {
            key: value
            for key, value in device_raw.items()
            if key not in GLOBAL_SATELLITE_PERSISTED_KEYS
        }
    current_raw = {**global_raw, **device_raw}
    combined_source = {**DEFAULTS, **current_raw, **incoming}
    dual_enabled = (
        _as_bool(combined_source.get("wake_mww_enabled"), bool(DEFAULTS["wake_mww_enabled"]))
        and _as_bool(combined_source.get("wake_oww_enabled"), bool(DEFAULTS["wake_oww_enabled"]))
    )
    current = settings_snapshot(token, board=board)
    if token:
        global_wake_word = _wake_word_value(global_raw.get("wake_word"))
        global_wake_url = _text(global_raw.get("wake_word_url"))
        profile = _wake_profile_from_source(global_raw, global_wake_word, global_wake_url)
        global_oww_word = _wake_word_value(global_raw.get("oww_wake_word"))
        global_oww_url = _text(global_raw.get("oww_wake_word_url"))
        oww_profile = _oww_profile_from_source(global_raw, global_oww_word, global_oww_url)
    elif dual_enabled:
        dual_profile = _dual_profile_for_save(incoming, current_raw, required_engine="oww")
        profile = dual_profile
        oww_profile = dual_profile
        selected_bundle = _wake_word_value(combined_source.get("oww_wake_word"))
        if selected_bundle == "custom_url":
            incoming["wake_word"] = "custom_url"
            incoming["wake_word_url"] = _text(dual_profile.get("wake_profile_source_url"))
        else:
            incoming["wake_word"] = "hey_tater"
            incoming["wake_word_url"] = ""
            incoming["oww_wake_word"] = "hey_tater"
            incoming["oww_wake_word_url"] = ""
    else:
        profile = _profile_for_save(incoming, current_raw)
        oww_profile = _oww_profile_for_save(incoming, current_raw)
    current_base = normalize_settings(current_raw, base=board_base)
    normalized = normalize_settings(incoming, base={**current_base, **profile, **oww_profile})
    changed = [key for key, value in normalized.items() if current.get(key) != value]
    if ssh_password_changed:
        changed.append("ssh_password")
    persisted = normalized
    if token:
        persisted = {
            key: value
            for key, value in normalized.items()
            if key not in GLOBAL_SATELLITE_PERSISTED_KEYS
        }
        try:
            redis_client.hdel(settings_hash_key(token), *sorted(GLOBAL_SATELLITE_PERSISTED_KEYS))
        except Exception:
            pass
    if changed:
        redis_client.hset(settings_hash_key(token), mapping={key: str(value) for key, value in persisted.items()})
    return {
        "ok": True,
        "selector": token,
        "scope": "device" if token else "global",
        "settings": normalized,
        "changed_keys": changed,
        "updated": len(changed),
    }


def adopt_reported_device_volume(
    value: Any,
    *,
    selector: Any = "",
    board: Any = "",
) -> bool:
    """Persist a physical satellite volume change as its device override."""
    token = _selector_token(selector)
    if not token:
        return False
    volume = _as_int(
        value,
        int(DEFAULTS["volume_percent"]),
        minimum=0,
        maximum=100,
    )
    if settings_snapshot(token, board=board).get("volume_percent") == volume:
        return False
    try:
        redis_client.hset(
            settings_hash_key(token),
            mapping={"volume_percent": str(volume)},
        )
    except Exception:
        return False
    return True
