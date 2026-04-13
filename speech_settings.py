from __future__ import annotations

import contextlib
import importlib.util
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as urllib_request

from helpers import redis_client

SPEECH_SETTINGS_KEY = "verba_settings:Speech"
REDIS_PIPER_TTS_MODELS_KEY = "tater:voice:piper:tts_models:v1"
REDIS_PIPER_TTS_MODELS_META_KEY = "tater:voice:piper:tts_models:meta:v1"

DEFAULT_STT_BACKEND = "faster_whisper"
DEFAULT_TTS_BACKEND = "wyoming"
DEFAULT_WYOMING_STT_HOST = "127.0.0.1"
DEFAULT_WYOMING_STT_PORT = 10300
DEFAULT_WYOMING_TTS_HOST = "127.0.0.1"
DEFAULT_WYOMING_TTS_PORT = 10200
DEFAULT_WYOMING_TTS_VOICE = ""
DEFAULT_TTS_PUBLIC_BASE_URL = ""
DEFAULT_KOKORO_MODEL = "v1.0:q8"
DEFAULT_KOKORO_VOICE = "af_bella"
DEFAULT_POCKET_TTS_MODEL = "b6369a24"
DEFAULT_POCKET_TTS_VOICE = "alba"
DEFAULT_PIPER_MODEL = "en_US-lessac-medium"
DEFAULT_ANNOUNCEMENT_TTS_BACKEND = "homeassistant_api"
DEFAULT_ANNOUNCEMENT_TTS_ENTITY = "tts.piper"

KOKORO_MODEL_SPECS: Dict[str, Dict[str, str]] = {
    "v1.0:q8": {
        "label": "Kokoro English v1.0 (q8)",
        "variant": "v1.0",
        "quality": "q8",
    },
    "v1.0:fp32": {
        "label": "Kokoro English v1.0 (fp32)",
        "variant": "v1.0",
        "quality": "fp32",
    },
    "v1.1-zh:q8": {
        "label": "Kokoro Chinese v1.1 (q8)",
        "variant": "v1.1-zh",
        "quality": "q8",
    },
}

try:
    from pykokoro.onnx_backend import VOICE_NAMES_BY_VARIANT as KOKORO_VOICE_NAMES_BY_VARIANT
except Exception:  # pragma: no cover - optional dependency
    KOKORO_VOICE_NAMES_BY_VARIANT = {}

try:
    from pocket_tts.utils.utils import PREDEFINED_VOICES as POCKET_TTS_PREDEFINED_VOICES
except Exception:  # pragma: no cover - optional dependency
    POCKET_TTS_PREDEFINED_VOICES = {}

try:
    from piper.download_voices import VOICES_JSON as PIPER_VOICES_CATALOG_URL
except Exception:  # pragma: no cover - optional dependency
    PIPER_VOICES_CATALOG_URL = ""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_int(value: Any, default: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < 1 or parsed > 65535:
        return int(default)
    return int(parsed)


def _normalize_stt_backend(value: Any) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_STT_BACKEND
    if token in {"faster_whisper", "fasterwhisper", "whisper"}:
        return "faster_whisper"
    if token == "vosk":
        return "vosk"
    if token == "wyoming":
        return "wyoming"
    return DEFAULT_STT_BACKEND


def _normalize_tts_backend(value: Any) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_TTS_BACKEND
    if token == "wyoming":
        return "wyoming"
    if token == "kokoro":
        return "kokoro"
    if token in {"pocket_tts", "pockettts", "pocket"}:
        return "pocket_tts"
    if token == "piper":
        return "piper"
    return DEFAULT_TTS_BACKEND


def normalize_announcement_tts_backend(value: Any, *, default: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return default
    if token in {"homeassistant_api", "homeassistant", "ha_api", "ha_tts"}:
        return "homeassistant_api"
    if token == "wyoming":
        return "wyoming"
    if token == "kokoro":
        return "kokoro"
    if token in {"pocket_tts", "pockettts", "pocket"}:
        return "pocket_tts"
    if token == "piper":
        return "piper"
    return default


def _option_rows_from_values(
    values: List[str],
    *,
    current_value: Any = "",
    labels: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    for raw in values:
        value = _clean(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        label = _clean((labels or {}).get(value)) or value
        rows.append({"value": value, "label": label})
    current = _clean(current_value)
    if current and current not in seen:
        rows.insert(0, {"value": current, "label": _clean((labels or {}).get(current)) or current})
    return rows


def _prefer_value_first(values: List[str], preferred_value: Any) -> List[str]:
    preferred = _clean(preferred_value)
    ordered = [_clean(value) for value in values if _clean(value)]
    if preferred and preferred in ordered:
        ordered = [value for value in ordered if value != preferred]
        ordered.insert(0, preferred)
    return ordered


def _module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


def _supported_kokoro_language_prefixes() -> set[str]:
    prefixes = {"a", "b", "d", "f"}
    if _module_available("pypinyin"):
        prefixes.add("z")
    if _module_available("pyopenjtalk"):
        prefixes.add("j")
    return prefixes


def _kokoro_voice_supported(value: Any) -> bool:
    token = _clean(value).lower()
    if not token:
        return False
    return token[0] in _supported_kokoro_language_prefixes()


def _kokoro_voice_label(value: Any) -> str:
    token = _clean(value)
    if not token:
        return ""
    prefix, _, remainder = token.partition("_")
    prefix_labels = {
        "af": "American Female",
        "am": "American Male",
        "bf": "British Female",
        "bm": "British Male",
        "ef": "Spanish Female",
        "em": "Spanish Male",
        "ff": "French Female",
        "hf": "Hindi Female",
        "hm": "Hindi Male",
        "if": "Italian Female",
        "im": "Italian Male",
        "jf": "Japanese Female",
        "jm": "Japanese Male",
        "pf": "Portuguese Female",
        "pm": "Portuguese Male",
        "zf": "Chinese Female",
        "zm": "Chinese Male",
        "df": "German Female",
        "dm": "German Male",
    }
    prefix_label = prefix_labels.get(prefix, prefix.upper())
    if not remainder:
        return prefix_label
    pretty_name = remainder.replace("-", " ").replace("_", " ").title()
    return f"{pretty_name} ({prefix_label})"


def _kokoro_model_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    labels = {key: _clean(spec.get("label")) or key for key, spec in KOKORO_MODEL_SPECS.items()}
    values = []
    for model_key, spec in KOKORO_MODEL_SPECS.items():
        variant = _clean((spec or {}).get("variant")) or "v1.0"
        voices = list(KOKORO_VOICE_NAMES_BY_VARIANT.get(variant) or KOKORO_VOICE_NAMES_BY_VARIANT.get("v1.0") or [])
        if any(_kokoro_voice_supported(voice) for voice in voices):
            values.append(model_key)
    current_model = _clean(current_value)
    if current_model not in values:
        current_model = DEFAULT_KOKORO_MODEL if DEFAULT_KOKORO_MODEL in values else ""
    return _option_rows_from_values(values, current_value=current_model, labels=labels)


def _kokoro_voice_option_rows(*, model_id: Any, current_value: Any = "") -> List[Dict[str, str]]:
    model_token = _clean(model_id)
    spec = dict(KOKORO_MODEL_SPECS.get(model_token) or KOKORO_MODEL_SPECS[DEFAULT_KOKORO_MODEL])
    variant = _clean(spec.get("variant")) or "v1.0"
    voices = [
        voice
        for voice in list(KOKORO_VOICE_NAMES_BY_VARIANT.get(variant) or KOKORO_VOICE_NAMES_BY_VARIANT.get("v1.0") or [])
        if _kokoro_voice_supported(voice)
    ]
    labels = {voice: _kokoro_voice_label(voice) for voice in voices}
    preferred_voice = _clean(current_value)
    if preferred_voice not in voices:
        preferred_voice = DEFAULT_KOKORO_VOICE if DEFAULT_KOKORO_VOICE in voices else _clean(voices[0] if voices else "")
    voices = _prefer_value_first(voices, preferred_voice)
    return _option_rows_from_values(voices, current_value=preferred_voice, labels=labels)


def _pocket_tts_model_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    labels = {DEFAULT_POCKET_TTS_MODEL: f"Pocket TTS {DEFAULT_POCKET_TTS_MODEL}"}
    return _option_rows_from_values([DEFAULT_POCKET_TTS_MODEL], current_value=current_value, labels=labels)


def _pocket_tts_voice_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    values = sorted(POCKET_TTS_PREDEFINED_VOICES.keys())
    labels = {value: value.replace("-", " ").replace("_", " ").title() for value in values}
    preferred_voice = _clean(current_value) or (DEFAULT_POCKET_TTS_VOICE if DEFAULT_POCKET_TTS_VOICE in values else "")
    values = _prefer_value_first(values, preferred_voice)
    return _option_rows_from_values(values, current_value=preferred_voice, labels=labels)


def _load_piper_tts_model_catalog() -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    rows: List[Dict[str, str]] = []
    meta: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        rows_raw = redis_client.get(REDIS_PIPER_TTS_MODELS_KEY)
        meta_raw = redis_client.get(REDIS_PIPER_TTS_MODELS_META_KEY)
        parsed_rows = json.loads(rows_raw) if rows_raw else []
        parsed_meta = json.loads(meta_raw) if meta_raw else {}
        if isinstance(parsed_rows, list):
            rows = [row for row in parsed_rows if isinstance(row, dict)]
        if isinstance(parsed_meta, dict):
            meta = dict(parsed_meta)
    return rows, meta


def _save_piper_tts_model_catalog(rows: List[Dict[str, str]], *, source: str, error: str = "") -> None:
    clean: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        value = _clean((row or {}).get("value"))
        label = _clean((row or {}).get("label")) or value
        if not value or value in seen:
            continue
        seen.add(value)
        clean.append({"value": value, "label": label})
    meta = {
        "updated_ts": float(__import__("time").time()),
        "source": _clean(source),
        "error": _clean(error),
    }
    with contextlib.suppress(Exception):
        redis_client.set(REDIS_PIPER_TTS_MODELS_KEY, json.dumps(clean, ensure_ascii=False))
        redis_client.set(REDIS_PIPER_TTS_MODELS_META_KEY, json.dumps(meta, ensure_ascii=False))


def refresh_piper_tts_model_catalog(force: bool = False) -> Dict[str, Any]:
    rows, meta = _load_piper_tts_model_catalog()
    if rows and not force:
        return {"models": rows, "meta": meta, "count": len(rows)}
    if not PIPER_VOICES_CATALOG_URL:
        return {"models": [], "meta": meta, "count": 0}
    catalog_rows: List[Dict[str, str]] = []
    with urllib_request.urlopen(PIPER_VOICES_CATALOG_URL, timeout=20) as response:
        payload = json.load(response)
    if isinstance(payload, dict):
        for voice_code in sorted(payload.keys()):
            value = _clean(voice_code)
            if value:
                catalog_rows.append({"value": value, "label": value})
    _save_piper_tts_model_catalog(catalog_rows, source=PIPER_VOICES_CATALOG_URL, error="")
    rows, meta = _load_piper_tts_model_catalog()
    return {"models": rows, "meta": meta, "count": len(rows)}


def _piper_tts_model_option_rows(*, current_value: Any = "", ensure_catalog: bool = False) -> List[Dict[str, str]]:
    rows, _meta = _load_piper_tts_model_catalog()
    if ensure_catalog and not rows:
        with contextlib.suppress(Exception):
            rows = list((refresh_piper_tts_model_catalog(force=False) or {}).get("models") or [])
    options = sorted(rows, key=lambda row: _clean(row.get("label")).lower())
    current = _clean(current_value) or DEFAULT_PIPER_MODEL
    if current and current not in {row.get("value") for row in options}:
        options.insert(0, {"value": current, "label": current})
    return options


def get_speech_settings() -> Dict[str, Any]:
    shared = redis_client.hgetall(SPEECH_SETTINGS_KEY) or {}
    return {
        "stt_backend": _normalize_stt_backend(shared.get("stt_backend")),
        "wyoming_stt_host": _clean(shared.get("wyoming_stt_host")) or DEFAULT_WYOMING_STT_HOST,
        "wyoming_stt_port": _as_int(shared.get("wyoming_stt_port"), DEFAULT_WYOMING_STT_PORT),
        "tts_backend": _normalize_tts_backend(shared.get("tts_backend")),
        "tts_model": _clean(shared.get("tts_model")),
        "tts_voice": _clean(shared.get("tts_voice")),
        "wyoming_tts_host": _clean(shared.get("wyoming_tts_host")) or DEFAULT_WYOMING_TTS_HOST,
        "wyoming_tts_port": _as_int(shared.get("wyoming_tts_port"), DEFAULT_WYOMING_TTS_PORT),
        "wyoming_tts_voice": _clean(shared.get("wyoming_tts_voice")) or DEFAULT_WYOMING_TTS_VOICE,
        "announcement_tts_backend": normalize_announcement_tts_backend(
            shared.get("announcement_tts_backend"),
            default=DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
        ),
        "announcement_tts_model": _clean(shared.get("announcement_tts_model")),
        "announcement_tts_voice": _clean(shared.get("announcement_tts_voice")),
        "announcement_tts_entity": _clean(shared.get("announcement_tts_entity")) or DEFAULT_ANNOUNCEMENT_TTS_ENTITY,
        "tts_public_base_url": _clean(shared.get("tts_public_base_url")) or DEFAULT_TTS_PUBLIC_BASE_URL,
    }


def save_speech_settings(
    *,
    stt_backend: Any,
    wyoming_stt_host: Any,
    wyoming_stt_port: Any,
    tts_backend: Any,
    tts_model: Any,
    tts_voice: Any,
    wyoming_tts_host: Any,
    wyoming_tts_port: Any,
    wyoming_tts_voice: Any,
    announcement_tts_backend: Any,
    announcement_tts_model: Any,
    announcement_tts_voice: Any,
    announcement_tts_entity: Any,
    tts_public_base_url: Any,
) -> None:
    redis_client.hset(
        SPEECH_SETTINGS_KEY,
        mapping={
            "stt_backend": _normalize_stt_backend(stt_backend),
            "wyoming_stt_host": _clean(wyoming_stt_host) or DEFAULT_WYOMING_STT_HOST,
            "wyoming_stt_port": str(_as_int(wyoming_stt_port, DEFAULT_WYOMING_STT_PORT)),
            "tts_backend": _normalize_tts_backend(tts_backend),
            "tts_model": _clean(tts_model),
            "tts_voice": _clean(tts_voice),
            "wyoming_tts_host": _clean(wyoming_tts_host) or DEFAULT_WYOMING_TTS_HOST,
            "wyoming_tts_port": str(_as_int(wyoming_tts_port, DEFAULT_WYOMING_TTS_PORT)),
            "wyoming_tts_voice": _clean(wyoming_tts_voice),
            "announcement_tts_backend": normalize_announcement_tts_backend(
                announcement_tts_backend,
                default=DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
            ),
            "announcement_tts_model": _clean(announcement_tts_model),
            "announcement_tts_voice": _clean(announcement_tts_voice),
            "announcement_tts_entity": _clean(announcement_tts_entity) or DEFAULT_ANNOUNCEMENT_TTS_ENTITY,
            "tts_public_base_url": _clean(tts_public_base_url).rstrip("/"),
        },
    )


def get_speech_ui_payload(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    current = dict(settings or get_speech_settings())
    current_tts_backend = _normalize_tts_backend(current.get("tts_backend"))
    current_tts_model = _clean(current.get("tts_model"))
    current_tts_voice = _clean(current.get("tts_voice"))

    kokoro_model_rows = _kokoro_model_option_rows(
        current_value=current_tts_model if current_tts_backend == "kokoro" else ""
    )
    pocket_model_rows = _pocket_tts_model_option_rows(
        current_value=current_tts_model if current_tts_backend == "pocket_tts" else ""
    )
    piper_model_rows = _piper_tts_model_option_rows(
        current_value=current_tts_model if current_tts_backend == "piper" else "",
        ensure_catalog=True,
    )

    voice_options_by_model: Dict[str, List[Dict[str, str]]] = {}
    for row in kokoro_model_rows:
        model_value = _clean((row or {}).get("value"))
        if model_value:
            voice_options_by_model[model_value] = _kokoro_voice_option_rows(
                model_id=model_value,
                current_value=current_tts_voice if current_tts_backend == "kokoro" and model_value == current_tts_model else "",
            )
    for row in pocket_model_rows:
        model_value = _clean((row or {}).get("value"))
        if model_value:
            voice_options_by_model[model_value] = _pocket_tts_voice_option_rows(
                current_value=current_tts_voice if current_tts_backend == "pocket_tts" and model_value == current_tts_model else ""
            )

    return {
        "stt_backend_options": [
            {"value": "faster_whisper", "label": "Faster Whisper"},
            {"value": "wyoming", "label": "Wyoming"},
            {"value": "vosk", "label": "Vosk"},
        ],
        "tts_backend_options": [
            {"value": "wyoming", "label": "Wyoming"},
            {"value": "kokoro", "label": "Kokoro"},
            {"value": "pocket_tts", "label": "Pocket TTS"},
            {"value": "piper", "label": "Piper"},
        ],
        "tts_model_options_by_backend": {
            "kokoro": kokoro_model_rows,
            "pocket_tts": pocket_model_rows,
            "piper": piper_model_rows,
        },
        "tts_voice_options_by_model": voice_options_by_model,
    }


def get_announcement_tts_ui_payload(
    *,
    backend: Any = "",
    model: Any = "",
    voice: Any = "",
    homeassistant_tts_entity: Any = "",
    homeassistant_tts_options: Optional[List[Dict[str, str]]] = None,
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
) -> Dict[str, Any]:
    current_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    current_model = _clean(model)
    current_voice = _clean(voice)
    speech_ui = get_speech_ui_payload(
        {
            "tts_backend": current_backend if current_backend != "homeassistant_api" else DEFAULT_TTS_BACKEND,
            "tts_model": current_model,
            "tts_voice": current_voice,
        }
    )
    ha_rows = [row for row in list(homeassistant_tts_options or []) if isinstance(row, dict)]
    current_entity = _clean(homeassistant_tts_entity)
    if current_entity and current_entity not in {_clean(row.get("value")) for row in ha_rows}:
        ha_rows.insert(0, {"value": current_entity, "label": f"{current_entity} (saved)"})
    backend_options = [{"value": "homeassistant_api", "label": "Home Assistant API TTS"}]
    backend_options.extend(list(speech_ui.get("tts_backend_options") or []))
    return {
        "tts_backend_options": backend_options,
        "tts_model_options_by_backend": dict(speech_ui.get("tts_model_options_by_backend") or {}),
        "tts_voice_options_by_model": dict(speech_ui.get("tts_voice_options_by_model") or {}),
        "homeassistant_tts_entity_options": ha_rows,
    }


def _selected_option_value(rows: List[Dict[str, str]], current_value: Any = "") -> str:
    current = _clean(current_value)
    values = [_clean((row or {}).get("value")) for row in list(rows or [])]
    values = [value for value in values if value]
    if current and current in values:
        return current
    return values[0] if values else ""


def build_announcement_tts_fields(
    *,
    backend: Any = "",
    model: Any = "",
    voice: Any = "",
    homeassistant_tts_entity: Any = "",
    homeassistant_tts_options: Optional[List[Dict[str, str]]] = None,
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
    backend_key: str = "tts_backend",
    model_key: str = "tts_model",
    voice_key: str = "tts_voice",
    homeassistant_tts_entity_key: str = "tts_entity",
    backend_label: str = "TTS Backend",
    model_label: str = "TTS Model",
    voice_label: str = "TTS Voice",
    homeassistant_tts_entity_label: str = "Home Assistant TTS Entity",
    backend_description: str = "",
    model_description: str = "",
    voice_description: str = "",
    homeassistant_tts_entity_description: str = "",
    homeassistant_tts_placeholder: str = "(Select Home Assistant TTS entity)",
) -> List[Dict[str, Any]]:
    payload = get_announcement_tts_ui_payload(
        backend=backend,
        model=model,
        voice=voice,
        homeassistant_tts_entity=homeassistant_tts_entity,
        homeassistant_tts_options=homeassistant_tts_options,
        default_backend=default_backend,
    )
    current_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    backend_options = [row for row in list(payload.get("tts_backend_options") or []) if isinstance(row, dict)]
    model_options_by_backend = dict(payload.get("tts_model_options_by_backend") or {})
    voice_options_by_model = dict(payload.get("tts_voice_options_by_model") or {})
    ha_entity_options = [row for row in list(payload.get("homeassistant_tts_entity_options") or []) if isinstance(row, dict)]

    current_model_options = [row for row in list(model_options_by_backend.get(current_backend) or []) if isinstance(row, dict)]
    current_model = _selected_option_value(current_model_options, model)
    current_voice_options = [row for row in list(voice_options_by_model.get(current_model) or []) if isinstance(row, dict)]
    current_voice = _selected_option_value(current_voice_options, voice)

    ha_fields_options = [{"value": "", "label": _clean(homeassistant_tts_placeholder) or "(Select Home Assistant TTS entity)"}]
    ha_fields_options.extend(ha_entity_options)
    current_ha_entity = _selected_option_value(ha_fields_options, homeassistant_tts_entity)

    backend_keys_with_models = [key for key, rows in model_options_by_backend.items() if isinstance(rows, list) and rows]
    backend_keys_with_voices = []
    for backend_key_value in backend_keys_with_models:
        model_rows = list(model_options_by_backend.get(backend_key_value) or [])
        if any(list(voice_options_by_model.get(_clean((row or {}).get("value"))) or []) for row in model_rows):
            backend_keys_with_voices.append(backend_key_value)

    return [
        {
            "key": backend_key,
            "label": _clean(backend_label) or "TTS Backend",
            "type": "select",
            "options": backend_options,
            "value": current_backend,
            "description": _clean(backend_description),
        },
        {
            "key": model_key,
            "label": _clean(model_label) or "TTS Model",
            "type": "select",
            "options": current_model_options,
            "value": current_model,
            "description": _clean(model_description),
            "show_when": {"source_key": backend_key, "any_of": backend_keys_with_models},
            "dependent_options": {
                "source_key": backend_key,
                "options_by_source": model_options_by_backend,
                "default_options": current_model_options,
            },
        },
        {
            "key": voice_key,
            "label": _clean(voice_label) or "TTS Voice",
            "type": "select",
            "options": current_voice_options,
            "value": current_voice,
            "description": _clean(voice_description),
            "show_when": {"source_key": backend_key, "any_of": backend_keys_with_voices},
            "dependent_options": {
                "source_key": model_key,
                "options_by_source": voice_options_by_model,
                "default_options": current_voice_options,
            },
        },
        {
            "key": homeassistant_tts_entity_key,
            "label": _clean(homeassistant_tts_entity_label) or "Home Assistant TTS Entity",
            "type": "select",
            "options": ha_fields_options,
            "value": current_ha_entity,
            "description": _clean(homeassistant_tts_entity_description),
            "show_when": {"source_key": backend_key, "equals": "homeassistant_api"},
        },
    ]
