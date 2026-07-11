from __future__ import annotations

import json
from typing import Any, Dict, List
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from helpers import redis_client

SETTINGS_HASH_KEY = "voice_native_satellite_live_settings"
DEVICE_SETTINGS_HASH_PREFIX = f"{SETTINGS_HASH_KEY}:device:"
WAKE_PROFILE_FETCH_TIMEOUT_S = 4.0

DEFAULTS: Dict[str, Any] = {
    "wake_engine": "micro_wake_word",
    "wake_word": "hey_tater",
    "wake_word_url": "",
    "wake_profile_key": "hey_tater",
    "wake_profile_name": "Hey Tater",
    "wake_profile_source_url": "",
    "wake_profile_model_url": "",
    "wake_profile_threshold": 0.97,
    "wake_profile_sliding_window": 5,
    "wake_profile_close_miss_threshold": 0.78,
    "wake_profile_error": "",
    "wake_sensitivity": "normal",
    "wake_environment": "balanced",
    "wake_tuning_override_enabled": False,
    "wake_threshold_override": 0.97,
    "wake_sliding_window_override": 5,
    "wake_close_miss_threshold_override": 0.78,
    "wake_tuning_overrides": "{}",
    "wake_threshold": 0.97,
    "wake_sliding_window": 5,
    "capture_wake_audio": False,
    "capture_close_misses": False,
    "close_miss_threshold": 0.78,
    "trainer_app_url": "http://trainer.local:8789",
    "wake_sound_enabled": False,
    "wake_sound": "no_sound",
    "wake_sound_url": "",
    "aec_enabled": False,
    "aec_strength_percent": 70,
    "aec_delay_ms": 85,
    "continued_chat": True,
    "barge_in_enabled": False,
    "volume_percent": 80,
    "led_brightness": 64,
    "led_color": "#ff5a1f",
    "led_listening_animation": "directional",
    "led_thinking_animation": "sparkle",
    "led_tool_call_animation": "ping_pong",
    "led_replying_animation": "voice_ring",
    "logging_level": "info",
}
FIRMWARE_SETTING_KEYS = (
    "wake_engine",
    "wake_word",
    "wake_word_url",
    "wake_sensitivity",
    "wake_environment",
    "wake_threshold",
    "wake_sliding_window",
    "capture_wake_audio",
    "capture_close_misses",
    "close_miss_threshold",
    "trainer_app_url",
    "wake_sound_enabled",
    "wake_sound",
    "wake_sound_url",
    "aec_enabled",
    "aec_strength_percent",
    "aec_delay_ms",
    "continued_chat",
    "barge_in_enabled",
    "volume_percent",
    "led_brightness",
    "led_color",
    "led_listening_animation",
    "led_thinking_animation",
    "led_tool_call_animation",
    "led_replying_animation",
    "logging_level",
)

WAKE_ENGINES = {"off", "button", "micro_wake_word", "server"}
LOGGING_LEVELS = {"error", "warning", "info", "debug"}
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
BUILTIN_WAKE_PROFILES: Dict[str, Dict[str, Any]] = {
    "hey_tater": {
        "wake_profile_key": "hey_tater",
        "wake_profile_name": "Hey Tater",
        "wake_profile_source_url": "",
        "wake_profile_model_url": "",
        "wake_profile_threshold": 0.97,
        "wake_profile_sliding_window": 5,
        "wake_profile_close_miss_threshold": 0.78,
        "wake_profile_error": "",
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


LED_LISTENING_ANIMATIONS = _animation_rows("directional", "pulse", "spinner", "breathe")
LED_THINKING_ANIMATIONS = _animation_rows("sparkle", "shimmer", "twinkle", "breathe")
LED_TOOL_CALL_ANIMATIONS = _animation_rows("ping_pong", "scanner", "orbit", "comet")
LED_REPLYING_ANIMATIONS = _animation_rows("voice_ring", "wave", "ripple", "equalizer")
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
        return dict(builtin)

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


def _board_default_overrides(board: Any = "") -> Dict[str, Any]:
    token = _lower(board).replace("_", "-")
    compact = token.replace("-", "").replace(" ", "")
    if token in {"satellite1", "satellite-1", "sat1", "sat-1"} or compact in {"satellite1", "sat1"}:
        return {
            "wake_sensitivity": "high",
            "wake_environment": "far_field",
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
    "led_preview",
}


def _board_supports_led_settings(board: Any = "") -> bool:
    token = _lower(board).replace("_", "-").replace(" ", "-")
    compact = token.replace("-", "")
    if token in {"s3-box", "s3-box-3", "esp32-s3-box", "esp32-s3-box-3"}:
        return False
    if compact in {"s3box", "s3box3", "esp32s3box", "esp32s3box3"}:
        return False
    return True


def normalize_settings(values: Dict[str, Any], *, base: Dict[str, Any] | None = None) -> Dict[str, Any]:
    incoming = values or {}
    source = {**DEFAULTS, **(base or {}), **incoming}
    wake_engine = _lower(source.get("wake_engine")) or str(DEFAULTS["wake_engine"])
    if wake_engine not in WAKE_ENGINES:
        wake_engine = str(DEFAULTS["wake_engine"])
    logging_level = _lower(source.get("logging_level")) or str(DEFAULTS["logging_level"])
    if logging_level not in LOGGING_LEVELS:
        logging_level = str(DEFAULTS["logging_level"])
    wake_word = _wake_word_value(source.get("wake_word"))
    wake_word_url = _text(source.get("wake_word_url"))
    if wake_word == "custom_url" and not wake_word_url and _is_url(source.get("wake_word")):
        wake_word_url = _text(source.get("wake_word"))
    if wake_word != "custom_url":
        wake_word_url = ""
    wake_sensitivity = _wake_sensitivity_value(source.get("wake_sensitivity"))
    wake_environment = _wake_environment_value(source.get("wake_environment"))
    profile = _wake_profile_from_source(source, wake_word, wake_word_url)
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
    if base is not None and "wake_tuning_override_enabled" in incoming:
        override_enabled_source = incoming.get("wake_tuning_override_enabled")
    else:
        override_enabled_source = existing_override.get("enabled", source.get("wake_tuning_override_enabled"))
    override_enabled = _as_bool(override_enabled_source, False)
    if base is not None and "wake_threshold_override" in incoming:
        threshold_override_source = incoming.get("wake_threshold_override")
    else:
        threshold_override_source = existing_override.get("threshold", profile_threshold)
    threshold_override = round(
        _as_float(
            threshold_override_source,
            profile_threshold,
            minimum=0.01,
            maximum=0.99,
        ),
        3,
    )
    if base is not None and "wake_sliding_window_override" in incoming:
        window_override_source = incoming.get("wake_sliding_window_override")
    else:
        window_override_source = existing_override.get("sliding_window", profile_window)
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
    effective_close_miss_threshold = close_miss_override if override_enabled else profile_close_miss_threshold
    wake_sound = _wake_sound_value(source.get("wake_sound"))
    wake_sound_url = _text(source.get("wake_sound_url"))
    if wake_sound != "custom":
        wake_sound_url = ""
    return {
        "wake_engine": wake_engine,
        "wake_word": wake_word,
        "wake_word_url": wake_word_url,
        "wake_profile_key": profile_key,
        "wake_profile_name": _text(profile.get("wake_profile_name")) or str(DEFAULTS["wake_profile_name"]),
        "wake_profile_source_url": _text(profile.get("wake_profile_source_url")),
        "wake_profile_model_url": _text(profile.get("wake_profile_model_url")),
        "wake_profile_threshold": profile_threshold,
        "wake_profile_sliding_window": profile_window,
        "wake_profile_close_miss_threshold": profile_close_miss_threshold,
        "wake_profile_error": _text(profile.get("wake_profile_error")),
        "wake_sensitivity": wake_sensitivity,
        "wake_environment": wake_environment,
        "wake_tuning_override_enabled": bool(override_enabled),
        "wake_threshold_override": threshold_override,
        "wake_sliding_window_override": window_override,
        "wake_close_miss_threshold_override": close_miss_override,
        "wake_tuning_overrides": _json_dumps(overrides),
        "wake_threshold": effective_threshold,
        "wake_sliding_window": effective_window,
        "capture_wake_audio": _as_bool(source.get("capture_wake_audio"), bool(DEFAULTS["capture_wake_audio"])),
        "capture_close_misses": _as_bool(source.get("capture_close_misses"), bool(DEFAULTS["capture_close_misses"])),
        "close_miss_threshold": effective_close_miss_threshold,
        "trainer_app_url": _text(source.get("trainer_app_url")) or str(DEFAULTS["trainer_app_url"]),
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
        "led_brightness": _as_int(source.get("led_brightness"), int(DEFAULTS["led_brightness"]), minimum=0, maximum=255),
        "led_color": _led_color_value(source.get("led_color")),
        "led_listening_animation": _led_animation_value(source.get("led_listening_animation"), "led_listening_animation"),
        "led_thinking_animation": _led_animation_value(source.get("led_thinking_animation"), "led_thinking_animation"),
        "led_tool_call_animation": _led_animation_value(source.get("led_tool_call_animation"), "led_tool_call_animation"),
        "led_replying_animation": _led_animation_value(source.get("led_replying_animation"), "led_replying_animation"),
        "logging_level": logging_level,
    }


def settings_snapshot(selector: Any = "", *, board: Any = "") -> Dict[str, Any]:
    token = _selector_token(selector)
    board_defaults = _board_default_overrides(board)
    base = normalize_settings(board_defaults) if board_defaults else None
    global_raw = _raw_settings()
    global_settings = normalize_settings(global_raw, base=base)
    if not token:
        return global_settings
    return normalize_settings(_raw_settings(token), base=global_settings)


def firmware_settings_snapshot(selector: Any = "", *, board: Any = "") -> Dict[str, Any]:
    current = settings_snapshot(selector, board=board)
    return {key: current[key] for key in FIRMWARE_SETTING_KEYS}


def _profile_for_save(values: Dict[str, Any], current_raw: Dict[str, Any]) -> Dict[str, Any]:
    source = {**DEFAULTS, **(current_raw or {}), **(values or {})}
    wake_word = _wake_word_value(source.get("wake_word"))
    wake_word_url = _text(source.get("wake_word_url"))
    if wake_word == "custom_url" and not wake_word_url and _is_url(source.get("wake_word")):
        wake_word_url = _text(source.get("wake_word"))
    if wake_word != "custom_url":
        return dict(BUILTIN_WAKE_PROFILES.get(wake_word) or BUILTIN_WAKE_PROFILES["hey_tater"])
    if (
        _text(current_raw.get("wake_profile_source_url")) == wake_word_url and
        _text(current_raw.get("wake_profile_name")) and
        _text(current_raw.get("wake_profile_close_miss_threshold"))
    ):
        return _wake_profile_from_source(current_raw, wake_word, wake_word_url)
    return _fetch_wake_profile_json(wake_word_url)


def settings_fields(selector: Any = "", *, board: Any = "") -> List[Dict[str, Any]]:
    current = settings_snapshot(selector, board=board)
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
                {"value": "micro_wake_word", "label": "microWakeWord"},
                {"value": "button", "label": "Button Only"},
                {"value": "server", "label": "Server Wake Stream"},
                {"value": "off", "label": "Off"},
            ],
        },
        {
            "key": "wake_word",
            "label": "Wake Word",
            "type": "select",
            "value": current["wake_word"],
            "default": DEFAULTS["wake_word"],
            "options": [{"value": value, "label": row["label"]} for value, row in WAKE_WORDS.items()],
            "description": "Use the built-in Tater model or load a trainer/GitHub microWakeWord JSON package.",
        },
        {
            "key": "wake_word_url",
            "label": "Wake Word JSON URL",
            "type": "text",
            "value": current["wake_word_url"] if current["wake_word"] == "custom_url" else "",
            "default": DEFAULTS["wake_word_url"],
            "placeholder": "https://example.local/wake_word.json",
            "show_when": {"source_key": "wake_word", "equals": "custom_url"},
            "description": "Tater reads tuning from this JSON, and the satellite downloads the JSON/model to swap on-device.",
        },
        {
            "key": "wake_profile_name",
            "label": "Loaded Profile",
            "type": "readonly",
            "value": current["wake_profile_name"],
            "description": current["wake_profile_error"] or "Name from the selected built-in profile or custom wake JSON.",
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
            "key": "aec_enabled",
            "label": "Acoustic Echo Cancellation",
            "type": "checkbox",
            "value": current["aec_enabled"],
            "default": DEFAULTS["aec_enabled"],
            "description": "Experimental. Leave off unless testing echo cancellation tuning for wake sounds, replies, timers, or intercom audio.",
        },
        {
            "key": "aec_strength_percent",
            "label": "AEC Strength",
            "type": "number",
            "value": current["aec_strength_percent"],
            "default": DEFAULTS["aec_strength_percent"],
            "min": 0,
            "max": 100,
            "step": 1,
            "show_when": {"source_key": "aec_enabled", "equals": True},
            "description": "Higher values suppress more speaker echo. Lower values preserve more near-end speech for barge-in.",
        },
        {
            "key": "aec_delay_ms",
            "label": "AEC Delay",
            "type": "number",
            "value": current["aec_delay_ms"],
            "default": DEFAULTS["aec_delay_ms"],
            "min": 0,
            "max": 220,
            "step": 5,
            "show_when": {"source_key": "aec_enabled", "equals": True},
            "description": "Reference delay between speaker output and mic pickup. 85 ms is the default starting point.",
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
            "key": "led_section",
            "label": "LED Settings",
            "type": "section",
            "description": "Choose the shared voice LED color and the animation used for each voice cycle.",
        },
        {
            "key": "led_brightness",
            "label": "LED Brightness",
            "type": "number",
            "value": current["led_brightness"],
            "default": DEFAULTS["led_brightness"],
            "min": 0,
            "max": 255,
            "step": 1,
        },
        {
            "key": "led_color",
            "label": "Voice LED Color",
            "type": "color",
            "value": current["led_color"],
            "default": DEFAULTS["led_color"],
            "description": "Applies to listening, thinking, tool call, and reply animations. Setup, error, OTA, and connection colors stay reserved.",
        },
        {
            "key": "led_listening_animation",
            "label": "Listening Animation",
            "type": "select",
            "value": current["led_listening_animation"],
            "default": DEFAULTS["led_listening_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_LISTENING_ANIMATIONS],
        },
        {
            "key": "led_thinking_animation",
            "label": "Thinking Animation",
            "type": "select",
            "value": current["led_thinking_animation"],
            "default": DEFAULTS["led_thinking_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_THINKING_ANIMATIONS],
        },
        {
            "key": "led_tool_call_animation",
            "label": "Tool Call Animation",
            "type": "select",
            "value": current["led_tool_call_animation"],
            "default": DEFAULTS["led_tool_call_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_TOOL_CALL_ANIMATIONS],
        },
        {
            "key": "led_replying_animation",
            "label": "Replying Animation",
            "type": "select",
            "value": current["led_replying_animation"],
            "default": DEFAULTS["led_replying_animation"],
            "options": [{"value": value, "label": label} for value, label in LED_REPLYING_ANIMATIONS],
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
    ]
    if not _board_supports_led_settings(board):
        fields = [field for field in fields if _text(field.get("key")) not in _LED_FIELD_KEYS]
    return fields


def save_settings(values: Dict[str, Any], *, selector: Any = "", board: Any = "") -> Dict[str, Any]:
    token = _selector_token(selector)
    board_defaults = _board_default_overrides(board)
    board_base = normalize_settings(board_defaults) if board_defaults else None
    global_raw = _raw_settings()
    device_raw = _raw_settings(token) if token else {}
    current_raw = {**global_raw, **device_raw}
    current = settings_snapshot(token, board=board)
    profile = _profile_for_save(values or {}, current_raw)
    current_base = normalize_settings(current_raw, base=board_base)
    normalized = normalize_settings(values or {}, base={**current_base, **profile})
    changed = [key for key, value in normalized.items() if current.get(key) != value]
    if changed:
        redis_client.hset(settings_hash_key(token), mapping={key: str(value) for key, value in normalized.items()})
    return {
        "ok": True,
        "selector": token,
        "scope": "device" if token else "global",
        "settings": normalized,
        "changed_keys": changed,
        "updated": len(changed),
    }
