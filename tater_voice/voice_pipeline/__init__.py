"""
Tater Native Voice Pipeline

Native satellite backend pipeline:
- Tater Native voice session handling
- Server-side EOU strategy
- Silero VAD backend
- Wyoming STT/TTS orchestration
- Hydra turn execution
- URL-based TTS playback lifecycle (announcement_finished aware)
- Native WebSocket satellite transport

This package is intentionally split across focused modules:
- conversation.py: session history and Hydra turn orchestration
- satellites.py: satellite registry and per-selector runtime state
- backends.py: STT/TTS backend selection and synthesis/transcription work
- routes.py: FastAPI router plus startup/shutdown lifecycle

This file now keeps the remaining shared constants, config helpers, audio/VAD
logic, native bridge helpers, and session finalization flow while
re-exporting package-level compatibility symbols.
"""

from __future__ import annotations

import asyncio
try:
    import audioop as _audioop
except Exception:  # Python 3.13 removed audioop
    _audioop = None
import contextlib
import html
import importlib
import inspect
import io
import json
import logging
import math
import os
import re
import shutil
import socket
import tempfile
import threading
import time
import uuid
import wave
import zipfile
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib import request as urllib_request

from dotenv import load_dotenv
from fastapi import HTTPException

from helpers import extract_json, get_llm_client_from_env, redis_client
from runtime_executors import run_background, run_speech
from tater_paths import agent_lab_path
from tateros import integration_store as integration_store_module
import verba_registry
from verba_settings import get_verba_enabled
from hydra import run_hydra_turn, resolve_agent_limits
from speech_settings import (
    get_speech_settings as get_shared_speech_settings,
    normalize_speech_acceleration,
)
from .. import reply_playback
# Compatibility re-exports for package-level callers while the implementation
# lives in smaller modules.
from .conversation import (
    VoiceSessionRuntime,
    _history_ctx_key,
    _history_key,
    _load_context,
    _load_history,
    _run_hydra_turn_for_voice,
    _save_context,
    _save_history_message,
    _to_template_msg,
)
from .backends import (
    _build_experimental_tts_chunks,
    clear_tts_model_caches,
    _chatterbox_tts_request,
    _iter_chatterbox_tts_stream_response,
    _load_faster_whisper_model,
    _load_kokoro_pipeline,
    _load_piper_voice_model,
    _load_pocket_tts_model,
    _load_vosk_model,
    _native_local_partial_stt_task,
    _open_chatterbox_tts_stream_response,
    _native_synthesize_text,
    _native_transcribe_local_audio_bytes,
    _native_transcribe_session_audio,
    _native_wyoming_refresh_tts_voices,
    _native_wyoming_stream_stt_task,
    _native_wyoming_synthesize,
    _normalized_audio_format,
    _resolve_stt_backend,
    _resolve_tts_backend,
    _selected_tts_backend,
    _split_piper_sentences,
    _stitch_pcm_playback_segments,
    _synthesize_kokoro_sync,
    _synthesize_piper_segment_sync,
    _synthesize_piper_sync,
    _synthesize_pocket_tts_sync,
    _synthesize_spoken_response_audio,
    _transcribe_faster_whisper_sync,
    _transcribe_mlx_whisper_sync,
    _transcribe_vosk_sync,
    _trim_pcm_for_playback,
    _tts_backend_available,
    _tts_config_snapshot,
    _tts_selection_from_values,
    _vosk_result_text,
    _wyoming_stt_endpoint,
    _wyoming_timeout_s,
    _wyoming_tts_endpoint,
    _float_audio_to_pcm16_bytes,
    _temporary_env,
    _piper_model_paths,
    _build_piper_segment_plan,
)
from .routes import router, shutdown, startup

HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    fn = integration_store_module.integration_function("homeassistant", "load_homeassistant_config")
    if fn:
        return fn(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": HOMEASSISTANT_DEFAULT_BASE_URL, "token": ""}
from .satellites import (
    _arm_pending_followup,
    _cancel_announcement_wait,
    _cancel_audio_stall_watch,
    _cancel_streamed_tts_dispatch,
    _claim_pending_followup,
    _clear_pending_followup,
    _clear_streamed_tts_state,
    _load_satellite_registry,
    _normalize_satellite_row,
    _remove_satellite,
    _satellite_area_name,
    _satellite_lookup,
    _save_satellite_registry,
    _schedule_audio_stall_watch,
    _selector_runtime,
    _set_satellite_selected,
    _upsert_satellite,
)

try:
    from wyoming.client import AsyncTcpClient
    from wyoming.asr import Transcribe, Transcript
    from wyoming.tts import Synthesize
    from wyoming.audio import AudioStart as WyomingAudioStart, AudioChunk as WyomingAudioChunk, AudioStop as WyomingAudioStop
    from wyoming.error import Error as WyomingError
    try:
        from wyoming.tts import SynthesizeVoice
    except Exception:
        SynthesizeVoice = None
    try:
        from wyoming.info import Describe, Info
    except Exception:
        Describe = None
        Info = None
    WYOMING_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    AsyncTcpClient = None
    Transcribe = None
    Transcript = None
    Synthesize = None
    SynthesizeVoice = None
    Describe = None
    Info = None
    WyomingAudioStart = None
    WyomingAudioChunk = None
    WyomingAudioStop = None
    WyomingError = None
    WYOMING_IMPORT_ERROR = str(exc)

try:
    from faster_whisper import WhisperModel
    FASTER_WHISPER_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    WhisperModel = None
    FASTER_WHISPER_IMPORT_ERROR = str(exc)

try:
    import mlx_whisper as MLXWhisper
    MLX_WHISPER_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    MLXWhisper = None
    MLX_WHISPER_IMPORT_ERROR = str(exc)

try:
    from vosk import Model as VoskModel, KaldiRecognizer, SetLogLevel as VoskSetLogLevel
    with contextlib.suppress(Exception):
        VoskSetLogLevel(-1)
    VOSK_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    VoskModel = None
    KaldiRecognizer = None
    VoskSetLogLevel = None
    VOSK_IMPORT_ERROR = str(exc)

try:
    from pykokoro import build_pipeline as build_kokoro_pipeline, PipelineConfig as KokoroPipelineConfig
    from pykokoro.tokenizer import TokenizerConfig as KokoroTokenizerConfig
    from pykokoro.onnx_backend import VOICE_NAMES_BY_VARIANT as KOKORO_VOICE_NAMES_BY_VARIANT
    KOKORO_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    build_kokoro_pipeline = None
    KokoroPipelineConfig = None
    KokoroTokenizerConfig = None
    KOKORO_VOICE_NAMES_BY_VARIANT = {}
    KOKORO_IMPORT_ERROR = str(exc)

try:
    from kokoro import KPipeline as KokoroTorchPipeline
    KOKORO_TORCH_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    KokoroTorchPipeline = None
    KOKORO_TORCH_IMPORT_ERROR = str(exc)

try:
    from pocket_tts import TTSModel as PocketTTSModel
    from pocket_tts.utils import utils as pocket_tts_utils

    POCKET_TTS_PREDEFINED_VOICES = (
        getattr(pocket_tts_utils, "PREDEFINED_VOICES", None)
        or getattr(pocket_tts_utils, "_ORIGINS_OF_PREDEFINED_VOICES", None)
        or {}
    )
    POCKET_TTS_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    PocketTTSModel = None
    POCKET_TTS_PREDEFINED_VOICES = {}
    POCKET_TTS_IMPORT_ERROR = str(exc)

try:
    from piper import PiperVoice
    from piper.config import SynthesisConfig as PiperSynthesisConfig
    from piper.download_voices import VOICES_JSON as PIPER_VOICES_CATALOG_URL, download_voice as piper_download_voice
    PIPER_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    PiperVoice = None
    PiperSynthesisConfig = None
    PIPER_VOICES_CATALOG_URL = ""
    piper_download_voice = None
    PIPER_IMPORT_ERROR = str(exc)

load_dotenv()

__version__ = "3.0.7"

logger = logging.getLogger("voice_core")
logger.setLevel(logging.INFO)

# -------------------- Constants + Defaults --------------------
VOICE_CORE_SETTINGS_HASH_KEY = "voice_core_settings"


def _main_app_port() -> int:
    raw = str(os.getenv("HTMLUI_PORT", "8501") or "8501").strip()
    try:
        port = int(raw)
    except Exception:
        port = 8501
    if port < 1 or port > 65535:
        port = 8501
    return int(port)

REDIS_VOICE_SATELLITE_REGISTRY_KEY = "tater:voice:satellites:registry:v1"
REDIS_WYOMING_TTS_VOICES_KEY = "tater:voice:wyoming:tts_voices:v1"
REDIS_WYOMING_TTS_VOICES_META_KEY = "tater:voice:wyoming:tts_voices:meta:v1"
REDIS_PIPER_TTS_MODELS_KEY = "tater:voice:piper:tts_models:v1"
REDIS_PIPER_TTS_MODELS_META_KEY = "tater:voice:piper:tts_models:meta:v1"
REDIS_VOICE_METRICS_KEY = "tater:voice:metrics:v1"

DEFAULT_WYOMING_STT_HOST = "127.0.0.1"
DEFAULT_WYOMING_STT_PORT = 10300
DEFAULT_WYOMING_TTS_HOST = "127.0.0.1"
DEFAULT_WYOMING_TTS_PORT = 10200
DEFAULT_WYOMING_TTS_VOICE = ""
DEFAULT_WYOMING_TIMEOUT_SECONDS = 45.0
DEFAULT_WYOMING_STT_QUEUE_CHUNKS = 64
DEFAULT_WYOMING_STT_QUEUE_DROP_LIMIT = 3
DEFAULT_LOCAL_STT_TIMEOUT_SECONDS = 35.0
DEFAULT_VOICE_TURN_PROCESS_TIMEOUT_S = 90.0
DEFAULT_OPENAI_COMPATIBLE_TTS_TIMEOUT_SECONDS = 90.0
DEFAULT_CHATTERBOX_TTS_TIMEOUT_SECONDS = 90.0
DEFAULT_STT_BACKEND = "faster_whisper"
DEFAULT_TTS_BACKEND = "wyoming"
DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS = 0.24
DEFAULT_PIPER_PARAGRAPH_PAUSE_SECONDS = 0.46
DEFAULT_PIPER_TAIL_PAD_SECONDS = 0.18
DEFAULT_FASTER_WHISPER_MODEL = "base.en"
DEFAULT_FASTER_WHISPER_DEVICE = "cpu"
DEFAULT_FASTER_WHISPER_COMPUTE_TYPE = "int8"
DEFAULT_FASTER_WHISPER_CUDA_COMPUTE_TYPE = "float16"
DEFAULT_FASTER_WHISPER_COMPUTE_TYPE_SETTING = "auto"
FASTER_WHISPER_COMPUTE_TYPES = {
    "auto",
    "float16",
    "float32",
    "int8",
    "int8_float16",
    "int8_float32",
}
DEFAULT_FASTER_WHISPER_BEAM_SIZE = 5
DEFAULT_FASTER_WHISPER_PARTIAL_BEAM_SIZE = 1
DEFAULT_FASTER_WHISPER_INITIAL_PROMPT = (
    "Tater is a home voice assistant. Common words include Tater, timer, lights, weather, "
    "music, pause, resume, stop, cancel, volume, kitchen, living room, office, bedroom, "
    "garage, Home Assistant, UniFi, Ecobee, Ecowitt, Sonos, Discord, and Kodi."
)
DEFAULT_MLX_WHISPER_MODEL = "mlx-community/whisper-base.en-mlx"
DEFAULT_VOSK_MODEL_NAME = "vosk-model-small-en-us-0.15"
DEFAULT_VOSK_MODEL_URL = "https://alphacephei.com/vosk/models/vosk-model-small-en-us-0.15.zip"
DEFAULT_STT_MODEL_ROOT = str(agent_lab_path("models", "stt"))
DEFAULT_TTS_MODEL_ROOT = str(agent_lab_path("models", "tts"))
DEFAULT_KOKORO_MODEL = "v1.0:q8"
DEFAULT_KOKORO_VOICE = "af_bella"
DEFAULT_KOKORO_ENGINE = "auto"
DEFAULT_KOKORO_PROVIDER = "cpu"
DEFAULT_KOKORO_OUTPUT_GAIN = 1.5
DEFAULT_POCKET_TTS_OUTPUT_GAIN = 1.5
DEFAULT_KOKORO_TORCH_REPO_ID = "hexgrad/Kokoro-82M"
DEFAULT_KOKORO_TORCH_ZH_REPO_ID = "hexgrad/Kokoro-82M-v1.1-zh"
DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL = ""
DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY = ""
DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL = "tts-1"
DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE = "alloy"
DEFAULT_CHATTERBOX_TTS_BASE_URL = ""
DEFAULT_CHATTERBOX_TTS_VOICE_MODE = "predefined"
DEFAULT_CHATTERBOX_TTS_CHUNK_SIZE = 120
DEFAULT_POCKET_TTS_MODEL = "b6369a24"
DEFAULT_POCKET_TTS_VOICE = "alba"
DEFAULT_PIPER_MODEL = "en_US-lessac-medium"
_PIPER_ABBREVIATIONS = {"dr", "mr", "mrs", "ms", "prof", "sr", "jr", "st", "vs", "etc", "e.g", "i.e"}

DEFAULT_VOICE_SAMPLE_RATE_HZ = 16000
DEFAULT_VOICE_SAMPLE_WIDTH = 2
DEFAULT_VOICE_CHANNELS = 1
DEFAULT_MAX_AUDIO_BYTES = 4 * 1024 * 1024

DEFAULT_DISCOVERY_ENABLED = False
DEFAULT_DISCOVERY_SCAN_SECONDS = 45
DEFAULT_DISCOVERY_MDNS_TIMEOUT_S = 3.0
DEFAULT_CONTINUED_CHAT_ENABLED = False
DEFAULT_CONTINUED_CHAT_REUSE_SECONDS = 30.0
DEFAULT_CONTINUED_CHAT_CLASSIFY_TIMEOUT_S = 4.0
DEFAULT_CONTINUED_CHAT_REPLY_TO_CUE_PAUSE_S = 0.60
DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S = 0.45
DEFAULT_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS = 0.96
DEFAULT_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS = 11.50
DEFAULT_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S = 5.00
DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES = 4
DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S = 0.66
DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S = 0.82
DEFAULT_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S = 0.40
DEFAULT_CONTINUED_CHAT_REOPEN_VAD_STARTUP_IGNORE_S = 0.55
DEFAULT_CONTINUED_CHAT_REOPEN_PREROLL_S = 0.30
DEFAULT_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED = False
DEFAULT_EXPERIMENTAL_PARTIAL_STT_ENABLED = False
DEFAULT_EXPERIMENTAL_TTS_EARLY_START_ENABLED = False
DEFAULT_EXPERIMENTAL_PARTIAL_STT_INTERVAL_S = 0.50
DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_AUDIO_S = 0.45
DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_NEW_AUDIO_S = 0.28
DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_CHARS = 90
DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_FIRST_CHARS = 28
DEFAULT_CHATTERBOX_TTS_STREAM_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_WAKE_ARBITRATION_ENABLED = True
DEFAULT_WAKE_ARBITRATION_WINDOW_MS = 900
DEFAULT_WAKE_ARBITRATION_SCOPE = "same_area"
DEFAULT_WAKE_ARBITRATION_BUSY_TIMEOUT_S = 120.0
DEFAULT_WAKE_ARBITRATION_RELEASE_GRACE_S = 1.5
DEFAULT_SPEECHBRAIN_ACCELERATION = "auto"
DEFAULT_STARTUP_GATE_S = 0.0
DEFAULT_WAKE_VAD_STARTUP_IGNORE_S = 0.30
DEFAULT_TTS_URL_TTL_S = 180
DEFAULT_TTS_ANNOUNCEMENT_TIMEOUT_MAX_S = 170.0
DEFAULT_TTS_DEVICE_FETCH_BYTES_PER_S = 25000.0

DEFAULT_EOU_MODE = "server"
DEFAULT_VAD_BACKEND = "silero"
DEFAULT_VAD_SILENCE_SECONDS = 0.78
DEFAULT_VAD_TIMEOUT_SECONDS = 8.50
DEFAULT_VAD_NO_SPEECH_TIMEOUT_S = 3.50
DEFAULT_SILERO_THRESHOLD = 0.24
DEFAULT_SILERO_NEG_THRESHOLD = 0.18
DEFAULT_SILERO_FRAME_SAMPLES = 512
DEFAULT_SILERO_MIN_SPEECH_FRAMES = 2
DEFAULT_SILERO_MIN_SILENCE_FRAMES = 4
DEFAULT_WEBRTC_VAD_AGGRESSIVENESS = 2
DEFAULT_WEBRTC_VAD_FRAME_MS = 30
DEFAULT_VAD_MIN_SILENCE_SHORT_S = 0.50
DEFAULT_VAD_MIN_SILENCE_LONG_S = 0.62
DEFAULT_AUDIO_INPUT_GAIN = 1.6
DEFAULT_NOISE_SUPPRESSION_ENABLED = True
DEFAULT_NOISE_SUPPRESSION_HIGH_PASS_ENABLED = True
DEFAULT_NOISE_SUPPRESSION_HIGH_PASS_ALPHA = 0.995
DEFAULT_NOISE_SUPPRESSION_TAIL_SILENCE_S = 0.24
DEFAULT_NOISE_SUPPRESSION_SILENCE_ATTENUATION = 0.18
DEFAULT_AUDIO_STALL_TIMEOUT_S = 1.20
DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S = 6.00
DEFAULT_BLANK_WAKE_TIMEOUT_S = 3.00
DEFAULT_AUDIO_STALL_POLL_S = 0.15
DEFAULT_TRANSCRIPT_COMPLETENESS_EXTENSION_S = 0.60
DEFAULT_STT_TRAILING_SILENCE_KEEP_S = 0.35

DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60
DEFAULT_HISTORY_MAX_STORE = 20
DEFAULT_HISTORY_MAX_LLM = 8

VOICE_STATE_LISTENING = "listening"
VOICE_STATE_THINKING = "thinking"
VOICE_STATE_SPEAKING = "speaking"
VOICE_STATE_ERROR = "error"

CORE_SETTINGS = {
    "category": "Voice Core Settings",
    # Intentionally empty. Voice settings are managed in the Voice tab UI.
    "required": {},
}

CORE_WEBUI_TAB = {
    "label": "Voice",
    "order": 40,
    "requires_running": True,
}

PLATFORM_SETTINGS = CORE_SETTINGS
PLATFORM_WEBUI_TAB = CORE_WEBUI_TAB
# ESPHome settings/schema now live in tater_voice.settings so the native ESPHome
# surface can grow beyond voice-only devices without piling more UI metadata
# into the live voice runtime.

# -------------------- Global Runtime State --------------------
_voice_runtime_lock = asyncio.Lock()
_voice_selector_runtime: Dict[str, Dict[str, Any]] = {}
_wake_arbitration_lock = asyncio.Lock()
_wake_arbitration_groups: Dict[str, Dict[str, Any]] = {}
_wake_arbitration_room_claims: Dict[str, Dict[str, Any]] = {}
_CUDA_RUNTIME_AVAILABLE_CACHE: Optional[bool] = None
_CUDA_RUNTIME_AVAILABLE_LOCK = threading.RLock()

_background_tasks: Dict[str, asyncio.Task] = {}

_tts_url_store: Dict[str, Dict[str, Any]] = {}
_tts_url_store_lock = threading.Lock()

_wyoming_tts_voice_catalog_mem: List[Dict[str, str]] = []
_wyoming_tts_voice_catalog_meta_mem: Dict[str, Any] = {
    "updated_ts": 0.0,
    "host": "",
    "port": 0,
    "error": "",
}
_piper_tts_model_catalog_mem: List[Dict[str, str]] = []
_piper_tts_model_catalog_meta_mem: Dict[str, Any] = {
    "updated_ts": 0.0,
    "source": "",
    "error": "",
}

_faster_whisper_model_cache: Dict[Tuple[str, str, str], Any] = {}
_faster_whisper_model_lock = threading.Lock()
_vosk_model_cache: Dict[str, Any] = {}
_vosk_model_lock = threading.Lock()
_vosk_bootstrap_lock = threading.Lock()
_kokoro_pipeline_cache: Dict[Tuple[str, ...], Any] = {}
_kokoro_pipeline_lock = threading.Lock()
_pocket_tts_model_cache: Dict[str, Any] = {}
_pocket_tts_model_lock = threading.Lock()
_piper_voice_cache: Dict[str, Any] = {}
_piper_voice_lock = threading.Lock()
_kokoro_ssmd_patch_applied = False
_TRANSCRIPT_COMPLETE_SHORT_COMMANDS = {
    "yes",
    "no",
    "stop",
    "cancel",
    "play",
    "pause",
    "next",
    "back",
    "resume",
    "thanks",
    "thank you",
}
_TRANSCRIPT_INCOMPLETE_TRAILING_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "but",
    "for",
    "from",
    "if",
    "in",
    "into",
    "my",
    "of",
    "or",
    "some",
    "than",
    "that",
    "the",
    "these",
    "this",
    "those",
    "to",
    "with",
    "your",
}
_TRANSCRIPT_INCOMPLETE_TRAILING_PHRASES = (
    "tell me",
    "show me",
    "play me",
    "send me",
    "call the",
    "open the",
    "close the",
    "turn on",
    "turn on the",
    "turn off",
    "turn off the",
    "switch on",
    "switch on the",
    "switch off",
    "switch off the",
    "set a timer for",
    "set timer for",
    "what is the",
    "what's the",
)
_VOICE_OUTCOME_VALID = "valid_turn"
_VOICE_OUTCOME_FALSE_WAKE = "false_wake"
_VOICE_OUTCOME_WAKE_NO_SPEECH = "wake_no_speech"
_VOICE_OUTCOME_LOW_SIGNAL = "low_signal_speech"
_VOICE_OUTCOME_CLIPPED = "clipped_ambiguous_speech"
_VOICE_METRICS_TEMPLATE: Dict[str, Any] = {
    "sessions_started": 0,
    "valid_turns": 0,
    "no_op_turns": 0,
    "false_wake_count": 0,
    "wake_no_speech_count": 0,
    "low_signal_count": 0,
    "clipped_ambiguous_count": 0,
    "blank_wake_count": 0,
    "continued_chat_attempts": 0,
    "continued_chat_reopens": 0,
    "stt_fallback_count": 0,
    "tts_fallback_count": 0,
    "speech_duration": {"count": 0, "total": 0.0},
    "silence_duration": {"count": 0, "total": 0.0},
    "turn_latency_ms": {"count": 0, "total": 0.0},
    "stt_latency_ms": {"count": 0, "total": 0.0, "by_backend": {}},
    "tts_latency_ms": {"count": 0, "total": 0.0, "by_backend": {}},
    "devices": {},
}
_VOICE_METRICS: Dict[str, Any] = json.loads(json.dumps(_VOICE_METRICS_TEMPLATE))
_VOICE_METRICS_LOCK = threading.Lock()
_VOICE_METRICS_LOADED = False

# -------------------- Shared Utility Helpers --------------------
def _now() -> float:
    return float(time.time())


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _lower(value)
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def _as_float(value: Any, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def _pcm_rms(data: bytes, sample_width: int) -> float:
    if _audioop is not None:
        with contextlib.suppress(Exception):
            return float(_audioop.rms(data, sample_width))
    if not data:
        return 0.0
    if sample_width == 2:
        usable = len(data) - (len(data) % 2)
        if usable <= 0:
            return 0.0
        view = memoryview(data[:usable]).cast("h")
        if len(view) <= 0:
            return 0.0
        total = 0.0
        for sample in view:
            value = float(sample)
            total += value * value
        return math.sqrt(total / float(len(view)))
    if sample_width == 1:
        total = 0.0
        for b in data:
            value = float(int(b) - 128)
            total += value * value
        return math.sqrt(total / float(len(data)))
    return 0.0


def _native_debug_enabled() -> bool:
    return _as_bool(_voice_settings().get("VOICE_NATIVE_DEBUG"), False)


def _native_debug(message: str) -> None:
    if _native_debug_enabled():
        logger.info("[native-voice] %s", message)


def _continued_chat_enabled() -> bool:
    return _get_bool_setting("VOICE_CONTINUED_CHAT_ENABLED", DEFAULT_CONTINUED_CHAT_ENABLED)


def _wake_arbitration_enabled() -> bool:
    return _get_bool_setting("VOICE_WAKE_ARBITRATION_ENABLED", DEFAULT_WAKE_ARBITRATION_ENABLED)


def _wake_arbitration_window_s() -> float:
    ms = _get_int_setting(
        "VOICE_WAKE_ARBITRATION_WINDOW_MS",
        DEFAULT_WAKE_ARBITRATION_WINDOW_MS,
        minimum=100,
        maximum=3000,
    )
    return float(ms) / 1000.0


def _wake_arbitration_busy_timeout_s() -> float:
    return _get_float_setting(
        "VOICE_WAKE_ARBITRATION_BUSY_TIMEOUT_S",
        DEFAULT_WAKE_ARBITRATION_BUSY_TIMEOUT_S,
        minimum=5.0,
        maximum=300.0,
    )


def _wake_arbitration_scope() -> str:
    token = _lower(_voice_settings().get("VOICE_WAKE_ARBITRATION_SCOPE") or DEFAULT_WAKE_ARBITRATION_SCOPE)
    token = token.replace("-", "_").replace(" ", "_")
    return token if token in {"same_area", "global"} else DEFAULT_WAKE_ARBITRATION_SCOPE


def _speechbrain_acceleration_setting() -> str:
    token = normalize_speech_acceleration(
        _voice_settings().get("VOICE_SPEECHBRAIN_ACCELERATION"),
        default=DEFAULT_SPEECHBRAIN_ACCELERATION,
    )
    return token if token in {"auto", "cpu", "cuda", "rocm", "mps"} else DEFAULT_SPEECHBRAIN_ACCELERATION


def _speechbrain_device() -> str:
    selected = _speechbrain_acceleration_setting()
    if selected == "cuda":
        return "cuda" if _torch_cuda_available() else "cpu"
    if selected == "rocm":
        return "cuda" if _torch_rocm_available() else "cpu"
    if selected == "mps":
        return "mps" if _mps_runtime_available() else "cpu"
    if selected == "cpu":
        return "cpu"
    effective = _effective_speech_acceleration()
    if effective == "cuda":
        return "cuda" if _torch_cuda_available() else "cpu"
    if effective == "rocm":
        return "cuda" if _torch_rocm_available() else "cpu"
    if effective == "mps":
        return "mps" if _mps_runtime_available() else "cpu"
    return "cpu"


def _wyoming_stt_queue_max_chunks() -> int:
    return _get_int_setting(
        "VOICE_WYOMING_STT_QUEUE_CHUNKS",
        DEFAULT_WYOMING_STT_QUEUE_CHUNKS,
        minimum=8,
        maximum=512,
    )


def _wyoming_stt_queue_drop_limit() -> int:
    return _get_int_setting(
        "VOICE_WYOMING_STT_QUEUE_DROP_LIMIT",
        DEFAULT_WYOMING_STT_QUEUE_DROP_LIMIT,
        minimum=1,
        maximum=64,
    )


def _experimental_live_tool_progress_enabled() -> bool:
    return _get_bool_setting(
        "VOICE_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED",
        DEFAULT_EXPERIMENTAL_LIVE_TOOL_PROGRESS_ENABLED,
    )

def _experimental_partial_stt_enabled() -> bool:
    return _get_bool_setting(
        "VOICE_EXPERIMENTAL_PARTIAL_STT_ENABLED",
        DEFAULT_EXPERIMENTAL_PARTIAL_STT_ENABLED,
    )


def _experimental_tts_early_start_enabled() -> bool:
    return _get_bool_setting(
        "VOICE_EXPERIMENTAL_TTS_EARLY_START_ENABLED",
        DEFAULT_EXPERIMENTAL_TTS_EARLY_START_ENABLED,
    )


def _chatterbox_tts_streaming_enabled(selection: Optional[Dict[str, Any]] = None) -> bool:
    row = selection if isinstance(selection, dict) else _tts_selection_from_values()
    return _as_bool(row.get("chatterbox_streaming_enabled"), False)


def _continued_chat_followup_cue(response_text: str) -> str:
    cues = (
        "I'm listening.",
        "Go ahead.",
        "Tell me.",
        "Say it.",
    )
    tail = _text(response_text).strip().lower()[-240:]
    if not tail:
        return "I'm listening."
    idx = sum(ord(ch) for ch in tail) % len(cues)
    return cues[idx]


def _sanitize_followup_cue_text(raw_text: str) -> str:
    cue = _text(raw_text).replace("\n", " ").strip()
    cue = re.sub(r"^[\s'\"`*#>-]+", "", cue)
    cue = re.sub(r"[\s'\"`]+$", "", cue)
    cue = cue.replace("?", "").strip()
    if not cue:
        return ""

    words = cue.split()
    if len(words) > 8:
        cue = " ".join(words[:8]).strip()
    if len(cue) > 80:
        cue = cue[:80].rsplit(" ", 1)[0].strip() or cue[:80].strip()
    if not cue:
        return ""
    if cue[-1:] not in ".!":
        cue = f"{cue}."
    return cue


def _sanitize_tool_progress_spoken_text(raw_text: str) -> str:
    cue = _text(raw_text).replace("\n", " ").strip()
    cue = re.sub(r"\s+", " ", cue)
    cue = re.sub(r"^[\s'\"`*#>-]+", "", cue)
    cue = re.sub(r"[\s'\"`]+$", "", cue)
    if not cue:
        return ""
    words = cue.split()
    if len(words) > 18:
        cue = " ".join(words[:18]).strip()
    if len(cue) > 140:
        cue = cue[:140].rsplit(" ", 1)[0].strip() or cue[:140].strip()
    if not cue:
        return ""
    if cue[-1:] not in ".!?":
        cue = f"{cue}."
    return cue


def _sanitize_spoken_response_text(raw_text: str) -> str:
    original = _text(raw_text)
    text = html.unescape(original)
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", lambda m: _text(m.group(1)).strip(), text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"`{1,3}([^`]+?)`{1,3}", r"\1", text)
    text = re.sub(r"```+", " ", text)
    text = re.sub(r"(?m)^\s*(?:[-*+]+|\d+[.)]+|>+|#{1,6})\s*", "", text)

    previous = None
    while text != previous:
        previous = text
        text = re.sub(r"(?<!\w)(\*{1,3}|_{1,3}|~{2})([^*_~\n]+?)\1(?!\w)", r"\2", text)

    text = re.sub(r"(?<!\w)[*_`~#>]+(?!\w)", " ", text)
    text = re.sub(r"\s*\n+\s*", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[\s'\"`]+", "", text)
    text = re.sub(r"[\s'\"`]+$", "", text)

    if text:
        return text

    fallback = re.sub(r"\s+", " ", original.replace("\n", " ")).strip()
    return fallback


async def _generate_followup_cue(user_text: str, assistant_text: str) -> str:
    transcript = _text(user_text).strip()
    reply = _text(assistant_text).strip()
    fallback = _continued_chat_followup_cue(reply)
    if not reply:
        return fallback

    prompt = (
        "You write the tiny spoken cue that plays right after an assistant asks a real follow-up question and just before the microphone reopens.\n"
        "Write one short, natural cue that invites the user to continue.\n"
        "Requirements:\n"
        "- plain text only\n"
        "- 2 to 6 words\n"
        "- not a question\n"
        "- do not repeat the assistant's question\n"
        "- do not mention microphones, wake words, buttons, or devices\n"
        "- sound warm and conversational, like 'Go ahead.' or 'Tell me.'\n"
    )
    user_prompt = (
        "User's last spoken request:\n"
        f"{transcript or '(not available)'}\n\n"
        "Assistant reply that triggered continued chat:\n"
        f"{reply}\n\n"
        "Return only the short spoken cue."
    )

    try:
        async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
            result = await llm_client.chat(
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.4,
                max_tokens=20,
                timeout=DEFAULT_CONTINUED_CHAT_CLASSIFY_TIMEOUT_S,
                activity="voice_followup_cue",
            )
        content = _text(((result or {}).get("message") or {}).get("content"))
        cue = _sanitize_followup_cue_text(content)
        if cue:
            _native_debug(
                f"continued chat cue generated cue={cue!r} transcript_tail={transcript[-80:]!r} reply_tail={reply[-80:]!r}"
            )
            return cue
        _native_debug(
            f"continued chat cue empty raw={content[:120]!r} fallback={fallback!r}"
        )
    except Exception as exc:
        _native_debug(f"continued chat cue generation failed error={exc}")

    return fallback


def _continued_chat_spoken_reply_text(
    response_text: str,
    *,
    continue_conversation: bool,
    followup_cue: str = "",
) -> str:
    reply = _sanitize_spoken_response_text(response_text)
    if not continue_conversation:
        return reply

    cue = _sanitize_followup_cue_text(followup_cue) or _continued_chat_followup_cue(reply)
    if not reply:
        return cue
    if reply[-1:] in ".!?":
        return f"{reply} {cue}".strip()
    return f"{reply}. {cue}".strip()


def _session_reply_playback_target(selector: str, session: "VoiceSessionRuntime") -> str:
    context = session.context if isinstance(session.context, dict) else {}
    configured = reply_playback.normalize_reply_playback_target(context.get("reply_playback_target"))
    if configured:
        return configured
    row = _satellite_lookup(selector)
    client_row: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        client_row = _esphome_client_row_snapshot_sync(selector)
    return reply_playback.resolve_reply_playback_target(row, client_row=client_row)


async def _play_reply_on_external_target(
    *,
    selector: str,
    session: "VoiceSessionRuntime",
    spoken_text: str,
    target: str,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    backend_used: str,
) -> Dict[str, Any]:
    wav_bytes, _normalized_format = _pcm_to_wav(audio_bytes, audio_format)
    if not wav_bytes:
        return {"ok": False, "sent_count": 0, "error": "Reply audio is empty."}

    try:
        ha_config = load_homeassistant_config(required=False)
    except Exception:
        ha_config = {"base": "", "token": ""}

    from speech_tts import play_announcement_audio_targets

    result = await play_announcement_audio_targets(
        text=spoken_text,
        wav_bytes=wav_bytes,
        ha_base=_text(ha_config.get("base")),
        token=_text(ha_config.get("token")),
        targets=[target],
        public_base_url=_text(os.getenv("VOICE_CORE_PUBLIC_BASE_URL")),
        backend=backend_used or session.tts_backend_effective or session.tts_backend,
    )
    return result if isinstance(result, dict) else {"ok": False, "sent_count": 0, "error": "External playback failed."}


def _wake_arbitration_phrase_key(wake_phrase: Any) -> str:
    phrase = _lower(wake_phrase)
    phrase = re.sub(r"[^a-z0-9]+", "_", phrase).strip("_")
    return phrase or "wake"


def _wake_arbitration_area_key(area_name: Any) -> str:
    area = _lower(area_name)
    area = re.sub(r"[^a-z0-9]+", "_", area).strip("_")
    return area or "unknown"


def _wake_arbitration_group_key(*, wake_phrase: Any, area_name: Any) -> str:
    phrase = _wake_arbitration_phrase_key(wake_phrase)
    if _wake_arbitration_scope() == "global":
        return f"global:{phrase}"
    return f"area:{phrase}:{_wake_arbitration_area_key(area_name)}"


def _wake_arbitration_room_key(area_name: Any) -> str:
    if _wake_arbitration_scope() == "global":
        return "global"
    return f"area:{_wake_arbitration_area_key(area_name)}"


def _wake_arbitration_cleanup_room_claims_locked(now_ts: float) -> None:
    for key, row in list(_wake_arbitration_room_claims.items()):
        if not isinstance(row, dict):
            _wake_arbitration_room_claims.pop(key, None)
            continue
        if float(row.get("expires_at") or 0.0) <= now_ts:
            _wake_arbitration_room_claims.pop(key, None)


async def _wake_arbitration_active_room_claim(*, selector: str, area_name: str) -> Optional[Dict[str, Any]]:
    if not _wake_arbitration_enabled():
        return None
    token = _text(selector)
    room_key = _wake_arbitration_room_key(area_name)
    now_ts = _now()
    async with _wake_arbitration_lock:
        _wake_arbitration_cleanup_room_claims_locked(now_ts)
        claim = _wake_arbitration_room_claims.get(room_key)
        if not isinstance(claim, dict):
            return None
        if _text(claim.get("selector")) == token:
            return None
        return dict(claim)


async def _wake_arbitration_claim_room(
    *,
    selector: str,
    session: VoiceSessionRuntime,
    area_name: str,
    reason: str,
    expires_at: float = 0.0,
) -> None:
    if not _wake_arbitration_enabled() or not isinstance(session, VoiceSessionRuntime):
        return
    token = _text(selector)
    if not token:
        return
    now_ts = _now()
    room_key = _wake_arbitration_room_key(area_name)
    expiry = float(expires_at or 0.0)
    if expiry <= now_ts:
        expiry = now_ts + _wake_arbitration_busy_timeout_s()
    row = {
        "key": room_key,
        "selector": token,
        "session_id": _text(session.session_id),
        "conversation_id": _text(session.conversation_id),
        "area_name": _text(area_name) or "unknown",
        "claimed_ts": now_ts,
        "updated_ts": now_ts,
        "expires_at": expiry,
        "reason": _text(reason) or "active_session",
    }
    async with _wake_arbitration_lock:
        _wake_arbitration_cleanup_room_claims_locked(now_ts)
        _wake_arbitration_room_claims[room_key] = row
    session.wake_arbitration_room_key = room_key
    session.wake_arbitration_room_claimed = True
    if isinstance(session.context, dict):
        session.context["wake_arbitration_room_key"] = room_key
        session.context["wake_arbitration_room_claimed"] = True
    _native_debug(
        f"wake arbitration room claim selector={token} session_id={session.session_id} "
        f"room={room_key} reason={_text(reason) or 'active_session'} expires_in_s={max(0.0, expiry - now_ts):.1f}"
    )


async def _wake_arbitration_extend_room_claim_for_session(
    *,
    selector: str,
    session_id: str,
    until_ts: float,
    reason: str,
    replace: bool = False,
) -> None:
    if not _wake_arbitration_enabled():
        return
    token = _text(selector)
    sid = _text(session_id)
    expiry = float(until_ts or 0.0)
    now_ts = _now()
    if not token or not sid or expiry <= now_ts:
        return
    touched = False
    async with _wake_arbitration_lock:
        _wake_arbitration_cleanup_room_claims_locked(now_ts)
        for row in _wake_arbitration_room_claims.values():
            if not isinstance(row, dict):
                continue
            if _text(row.get("selector")) != token or _text(row.get("session_id")) != sid:
                continue
            row["expires_at"] = expiry if replace else max(float(row.get("expires_at") or 0.0), expiry)
            row["updated_ts"] = now_ts
            row["reason"] = _text(reason) or "extended"
            touched = True
    if touched:
        _native_debug(
            f"wake arbitration room claim extended selector={token} session_id={sid} "
            f"reason={_text(reason) or 'extended'} expires_in_s={max(0.0, expiry - now_ts):.1f}"
        )


async def _wake_arbitration_release_room_claim_for_session(
    *,
    selector: str,
    session_id: str,
    reason: str,
    delay_s: float = 0.0,
) -> None:
    if not _wake_arbitration_enabled():
        return
    token = _text(selector)
    sid = _text(session_id)
    if not token or not sid:
        return
    now_ts = _now()
    delay = max(0.0, float(delay_s or 0.0))
    release_at = now_ts + delay
    released: List[str] = []
    async with _wake_arbitration_lock:
        _wake_arbitration_cleanup_room_claims_locked(now_ts)
        for key, row in list(_wake_arbitration_room_claims.items()):
            if not isinstance(row, dict):
                continue
            if _text(row.get("selector")) != token or _text(row.get("session_id")) != sid:
                continue
            if delay > 0.0:
                row["expires_at"] = min(float(row.get("expires_at") or release_at), release_at)
                row["updated_ts"] = now_ts
                row["reason"] = _text(reason) or "release_grace"
            else:
                _wake_arbitration_room_claims.pop(key, None)
            released.append(key)
    if released:
        _native_debug(
            f"wake arbitration room claim release selector={token} session_id={sid} "
            f"rooms={','.join(released)} reason={_text(reason) or 'release'} delay_s={delay:.1f}"
        )


def _wake_arbitration_candidate_score(candidate: Dict[str, Any]) -> float:
    selector = _text(candidate.get("selector"))
    session_id = _text(candidate.get("session_id"))
    runtime = _selector_runtime(selector)
    session = runtime.get("session")
    if not isinstance(session, VoiceSessionRuntime) or _text(session.session_id) != session_id:
        return -100000.0
    if bool(session.processing):
        return -100000.0

    peak_dbfs = float(session.wake_arbitration_peak_dbfs or -120.0)
    audio_chunks = int(session.wake_arbitration_audio_chunks or 0)
    max_prob = float(session.max_probability or 0.0)
    started_delta_ms = max(0.0, (float(candidate.get("started_ts") or _now()) - float(candidate.get("group_started_ts") or _now())) * 1000.0)

    audio_score = max(0.0, min(60.0, peak_dbfs + 60.0))
    chunk_score = min(12.0, float(audio_chunks) * 2.0)
    vad_score = max(0.0, min(20.0, max_prob * 20.0))
    first_bias = max(0.0, 8.0 - (started_delta_ms / 100.0))
    return audio_score + chunk_score + vad_score + first_bias


async def _resolve_wake_arbitration_group(group_id: str, *, delay_s: float) -> None:
    try:
        await asyncio.sleep(max(0.05, float(delay_s)))
        losers: List[Dict[str, Any]] = []
        winner_selector = ""
        winner_session_id = ""
        winner_score = 0.0
        candidate_count = 0

        async with _wake_arbitration_lock:
            group = _wake_arbitration_groups.get(group_id)
            if not isinstance(group, dict):
                return
            candidates = group.get("candidates") if isinstance(group.get("candidates"), dict) else {}
            candidate_rows = [dict(row) for row in candidates.values() if isinstance(row, dict)]
            candidate_count = len(candidate_rows)
            if not candidate_rows:
                _wake_arbitration_groups.pop(group_id, None)
                return

            scored = []
            for row in candidate_rows:
                row["group_started_ts"] = group.get("started_ts")
                score = _wake_arbitration_candidate_score(row)
                scored.append((score, float(row.get("started_ts") or 0.0), row))
            scored = [item for item in scored if item[0] > -99999.0]
            if not scored:
                _wake_arbitration_groups.pop(group_id, None)
                return
            scored.sort(key=lambda item: (-item[0], item[1]))
            winner_score, _winner_started, winner = scored[0]
            winner_selector = _text(winner.get("selector"))
            winner_session_id = _text(winner.get("session_id"))
            losers = [row for _score, _started, row in scored[1:]]
            _wake_arbitration_groups.pop(group_id, None)

        if winner_selector and winner_session_id:
            winner_runtime = _selector_runtime(winner_selector)
            winner_lock = winner_runtime.get("lock")
            winner_session: Optional[VoiceSessionRuntime] = None
            winner_area = _text(winner.get("area_name"))
            if winner_lock is not None:
                async with winner_lock:
                    session = winner_runtime.get("session")
                    if isinstance(session, VoiceSessionRuntime) and _text(session.session_id) == winner_session_id:
                        session.wake_arbitration_winner = True
                        winner_session = session
                        if isinstance(session.context, dict):
                            session.context["wake_arbitration_winner"] = True
                            session.context["wake_arbitration_candidate_count"] = candidate_count
                            session.context["wake_arbitration_score"] = round(float(winner_score), 2)
                            winner_area = (
                                _text(session.context.get("area_name") or session.context.get("room_name"))
                                or winner_area
                            )
            if winner_session is not None:
                await _wake_arbitration_claim_room(
                    selector=winner_selector,
                    session=winner_session,
                    area_name=winner_area,
                    reason="wake_arbitration_winner",
                )

        for row in losers:
            selector = _text(row.get("selector"))
            session_id = _text(row.get("session_id"))
            client = row.get("client")
            module = row.get("module")
            if not selector or not session_id or client is None or module is None:
                continue
            with contextlib.suppress(Exception):
                await _esphome_send_event(
                    client,
                    module,
                    ("VOICE_ASSISTANT_ERROR", "ERROR"),
                    {
                        "code": "duplicate_wake_up_detected",
                        "message": "Another nearby satellite is handling this wake.",
                    },
                )
            with contextlib.suppress(Exception):
                await _finalize_session(
                    selector,
                    client,
                    module,
                    session_id=session_id,
                    abort=True,
                    reason="wake_arbitration_lost",
                )

        if candidate_count > 1:
            logger.info(
                "[native-voice] wake arbitration winner selector=%s session_id=%s candidates=%s losers=%s score=%.2f",
                winner_selector,
                winner_session_id,
                candidate_count,
                len(losers),
                float(winner_score),
            )
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning("[native-voice] wake arbitration failed group=%s error=%s", group_id, exc)


async def _register_wake_arbitration_candidate(
    *,
    selector: str,
    session: VoiceSessionRuntime,
    client: Any,
    module: Any,
    wake_phrase: str,
    area_name: str,
) -> None:
    if not _wake_arbitration_enabled() or not _text(wake_phrase):
        return

    now_ts = _now()
    window_s = _wake_arbitration_window_s()
    key = _wake_arbitration_group_key(wake_phrase=wake_phrase, area_name=area_name)
    group_id = ""
    created = False

    async with _wake_arbitration_lock:
        for existing_id, group in list(_wake_arbitration_groups.items()):
            if not isinstance(group, dict):
                _wake_arbitration_groups.pop(existing_id, None)
                continue
            if float(group.get("resolve_at") or 0.0) + 2.0 < now_ts:
                _wake_arbitration_groups.pop(existing_id, None)

        for existing_id, group in _wake_arbitration_groups.items():
            if _text(group.get("key")) == key and now_ts <= float(group.get("resolve_at") or 0.0):
                group_id = existing_id
                break

        if not group_id:
            group_id = uuid.uuid4().hex
            created = True
            _wake_arbitration_groups[group_id] = {
                "id": group_id,
                "key": key,
                "started_ts": now_ts,
                "resolve_at": now_ts + window_s,
                "candidates": {},
            }

        group = _wake_arbitration_groups.get(group_id)
        candidates = group.get("candidates") if isinstance(group, dict) and isinstance(group.get("candidates"), dict) else {}
        if isinstance(group, dict):
            group["candidates"] = candidates
            candidates[_text(selector)] = {
                "selector": _text(selector),
                "session_id": _text(session.session_id),
                "wake_phrase": _text(wake_phrase),
                "area_name": _text(area_name),
                "started_ts": now_ts,
                "client": client,
                "module": module,
            }

    session.wake_arbitration_group_id = group_id
    session.wake_arbitration_candidate = True
    if isinstance(session.context, dict):
        session.context["wake_arbitration_group_id"] = group_id
        session.context["wake_arbitration_scope"] = _wake_arbitration_scope()

    if created:
        asyncio.create_task(_resolve_wake_arbitration_group(group_id, delay_s=window_s))
    _native_debug(
        f"wake arbitration candidate selector={selector} session_id={session.session_id} group={group_id} key={key} window_s={window_s:.2f}"
    )


def _record_wake_arbitration_audio(session: VoiceSessionRuntime, audio_bytes: bytes, audio_format: Dict[str, Any]) -> None:
    if not isinstance(session, VoiceSessionRuntime) or not session.wake_arbitration_candidate:
        return
    if not _text(session.wake_arbitration_group_id):
        return
    width = int((audio_format or {}).get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    dbfs = _pcm_dbfs(audio_bytes, sample_width=width)
    if dbfs is None:
        return
    session.wake_arbitration_audio_chunks += 1
    if float(dbfs) > float(session.wake_arbitration_peak_dbfs or -120.0):
        session.wake_arbitration_peak_dbfs = float(dbfs)


def _merge_text_notes(*parts: str) -> str:
    seen = set()
    out: List[str] = []
    for part in parts:
        text = _text(part)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return " ".join(out).strip()


def _voice_metric_avg(bucket: Dict[str, Any]) -> float:
    count = int(bucket.get("count") or 0) if isinstance(bucket, dict) else 0
    total = float(bucket.get("total") or 0.0) if isinstance(bucket, dict) else 0.0
    if count <= 0:
        return 0.0
    return total / float(count)


def _voice_metric_backend_avgs(group_bucket: Dict[str, Any]) -> Dict[str, float]:
    by_backend = group_bucket.get("by_backend") if isinstance(group_bucket, dict) else {}
    if not isinstance(by_backend, dict):
        return {}
    out: Dict[str, float] = {}
    for backend, bucket in by_backend.items():
        if not isinstance(bucket, dict):
            continue
        avg = round(_voice_metric_avg(bucket), 1)
        if avg > 0.0:
            out[_text(backend) or "unknown"] = avg
    return out


def _voice_metric_add(bucket: Dict[str, Any], value: Any) -> None:
    if not isinstance(bucket, dict):
        return
    bucket["count"] = int(bucket.get("count") or 0) + 1
    bucket["total"] = float(bucket.get("total") or 0.0) + max(0.0, float(value or 0.0))


def _voice_metric_merge_counter(target: Dict[str, Any], source: Dict[str, Any], key: str) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    try:
        target[key] = max(int(target.get(key) or 0), int(source.get(key) or 0))
    except Exception:
        return


def _voice_metric_merge_bucket(target: Dict[str, Any], source: Any) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    _voice_metric_merge_counter(target, source, "count")
    try:
        target["total"] = max(float(target.get("total") or 0.0), float(source.get("total") or 0.0))
    except Exception:
        pass
    source_by_backend = source.get("by_backend") if isinstance(source.get("by_backend"), dict) else {}
    if not source_by_backend:
        return
    target_by_backend = target.get("by_backend")
    if not isinstance(target_by_backend, dict):
        target_by_backend = {}
        target["by_backend"] = target_by_backend
    for backend, saved_bucket in source_by_backend.items():
        if not isinstance(saved_bucket, dict):
            continue
        token = _text(backend) or "unknown"
        bucket = target_by_backend.get(token)
        if not isinstance(bucket, dict):
            bucket = {"count": 0, "total": 0.0}
            target_by_backend[token] = bucket
        _voice_metric_merge_bucket(bucket, saved_bucket)


def _voice_metrics_merge_row(target: Dict[str, Any], source: Any) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    for key in (
        "sessions_started",
        "valid_turns",
        "no_op_turns",
        "false_wake_count",
        "wake_no_speech_count",
        "low_signal_count",
        "clipped_ambiguous_count",
        "blank_wake_count",
        "continued_chat_attempts",
        "continued_chat_reopens",
        "stt_fallback_count",
        "tts_fallback_count",
        "error_count",
        "disconnect_count",
        "reconnect_count",
    ):
        if key in source:
            _voice_metric_merge_counter(target, source, key)
    for key in ("speech_duration", "silence_duration", "turn_latency_ms", "stt_latency_ms", "tts_latency_ms"):
        if isinstance(target.get(key), dict):
            _voice_metric_merge_bucket(target[key], source.get(key))
    source_seen = _as_float(source.get("last_seen_ts"), 0.0)
    target_seen = _as_float(target.get("last_seen_ts"), 0.0)
    if source_seen >= target_seen:
        for key in ("last_outcome", "last_reason"):
            if key in source:
                target[key] = _text(source.get(key))
        if source_seen > 0.0:
            target["last_seen_ts"] = source_seen


def _voice_metrics_load_persisted_locked() -> None:
    global _VOICE_METRICS_LOADED
    if _VOICE_METRICS_LOADED:
        return
    try:
        raw = redis_client.get(REDIS_VOICE_METRICS_KEY)
    except Exception as exc:
        logger.debug("[native-voice] voice metrics load skipped: %s", exc)
        return
    _VOICE_METRICS_LOADED = True
    if raw in (None, ""):
        return
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        saved = json.loads(str(raw))
    except Exception as exc:
        logger.debug("[native-voice] voice metrics payload invalid: %s", exc)
        return
    if not isinstance(saved, dict):
        return
    _voice_metrics_merge_row(_VOICE_METRICS, saved)
    saved_devices = saved.get("devices") if isinstance(saved.get("devices"), dict) else {}
    for selector, saved_row in saved_devices.items():
        if not isinstance(saved_row, dict):
            continue
        row = _voice_device_metrics(_text(selector) or "unknown")
        _voice_metrics_merge_row(row, saved_row)


def _voice_metrics_persist_locked() -> None:
    try:
        redis_client.set(REDIS_VOICE_METRICS_KEY, json.dumps(_VOICE_METRICS, ensure_ascii=False))
    except Exception as exc:
        logger.debug("[native-voice] voice metrics persist skipped: %s", exc)


def _voice_backend_bucket(group_name: str, backend: str) -> Dict[str, Any]:
    metrics = _VOICE_METRICS.get(group_name)
    if not isinstance(metrics, dict):
        metrics = {"count": 0, "total": 0.0, "by_backend": {}}
        _VOICE_METRICS[group_name] = metrics
    by_backend = metrics.get("by_backend")
    if not isinstance(by_backend, dict):
        by_backend = {}
        metrics["by_backend"] = by_backend
    token = _text(backend) or "unknown"
    bucket = by_backend.get(token)
    if not isinstance(bucket, dict):
        bucket = {"count": 0, "total": 0.0}
        by_backend[token] = bucket
    return bucket


def _voice_device_metrics(selector: str) -> Dict[str, Any]:
    devices = _VOICE_METRICS.get("devices")
    if not isinstance(devices, dict):
        devices = {}
        _VOICE_METRICS["devices"] = devices
    token = _text(selector) or "unknown"
    row = devices.get(token)
    if not isinstance(row, dict):
        row = {
            "sessions_started": 0,
            "valid_turns": 0,
            "no_op_turns": 0,
            "false_wake_count": 0,
            "wake_no_speech_count": 0,
            "low_signal_count": 0,
            "clipped_ambiguous_count": 0,
            "error_count": 0,
            "disconnect_count": 0,
            "reconnect_count": 0,
            "continued_chat_reopens": 0,
            "speech_duration": {"count": 0, "total": 0.0},
            "silence_duration": {"count": 0, "total": 0.0},
            "turn_latency_ms": {"count": 0, "total": 0.0},
            "stt_latency_ms": {"count": 0, "total": 0.0},
            "tts_latency_ms": {"count": 0, "total": 0.0},
            "last_outcome": "",
            "last_reason": "",
            "last_seen_ts": 0.0,
        }
        devices[token] = row
    return row


def _voice_metrics_record_session_start(
    *,
    selector: str,
    continued_chat_reopen: bool,
    stt_fallback_used: bool,
    tts_fallback_used: bool,
) -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        _VOICE_METRICS["sessions_started"] = int(_VOICE_METRICS.get("sessions_started") or 0) + 1
        if continued_chat_reopen:
            _VOICE_METRICS["continued_chat_reopens"] = int(_VOICE_METRICS.get("continued_chat_reopens") or 0) + 1
        if stt_fallback_used:
            _VOICE_METRICS["stt_fallback_count"] = int(_VOICE_METRICS.get("stt_fallback_count") or 0) + 1
        if tts_fallback_used:
            _VOICE_METRICS["tts_fallback_count"] = int(_VOICE_METRICS.get("tts_fallback_count") or 0) + 1
        device = _voice_device_metrics(selector)
        device["sessions_started"] = int(device.get("sessions_started") or 0) + 1
        if continued_chat_reopen:
            device["continued_chat_reopens"] = int(device.get("continued_chat_reopens") or 0) + 1
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _drain_task_result(task: asyncio.Task[Any]) -> None:
    with contextlib.suppress(BaseException):
        task.result()


async def _voice_metrics_record_session_start_async(
    *,
    selector: str,
    continued_chat_reopen: bool,
    stt_fallback_used: bool,
    tts_fallback_used: bool,
) -> None:
    try:
        await run_background(
            _voice_metrics_record_session_start,
            selector=selector,
            continued_chat_reopen=continued_chat_reopen,
            stt_fallback_used=stt_fallback_used,
            tts_fallback_used=tts_fallback_used,
        )
    except Exception as exc:
        logger.debug("[native-voice] voice metrics session-start persist skipped: %s", exc)


def _voice_metrics_record_connection_event(selector: str, *, event: str) -> None:
    token = _text(event)
    if token not in {"disconnect", "reconnect", "error"}:
        return
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        device = _voice_device_metrics(selector)
        if token == "disconnect":
            device["disconnect_count"] = int(device.get("disconnect_count") or 0) + 1
        elif token == "reconnect":
            device["reconnect_count"] = int(device.get("reconnect_count") or 0) + 1
        else:
            device["error_count"] = int(device.get("error_count") or 0) + 1
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_record_stt(selector: str, backend: str, latency_ms: float) -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        _voice_metric_add(_VOICE_METRICS.setdefault("stt_latency_ms", {"count": 0, "total": 0.0, "by_backend": {}}), latency_ms)
        _voice_metric_add(_voice_backend_bucket("stt_latency_ms", backend), latency_ms)
        device = _voice_device_metrics(selector)
        _voice_metric_add(device.setdefault("stt_latency_ms", {"count": 0, "total": 0.0}), latency_ms)
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_record_stt_fallback(selector: str, reason: str = "") -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        _VOICE_METRICS["stt_fallback_count"] = int(_VOICE_METRICS.get("stt_fallback_count") or 0) + 1
        device = _voice_device_metrics(selector)
        device["last_reason"] = _text(reason) or "stt_fallback"
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_record_tts(selector: str, backend: str, latency_ms: float) -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        _voice_metric_add(_VOICE_METRICS.setdefault("tts_latency_ms", {"count": 0, "total": 0.0, "by_backend": {}}), latency_ms)
        _voice_metric_add(_voice_backend_bucket("tts_latency_ms", backend), latency_ms)
        device = _voice_device_metrics(selector)
        _voice_metric_add(device.setdefault("tts_latency_ms", {"count": 0, "total": 0.0}), latency_ms)
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_record_turn(
    *,
    selector: str,
    outcome: str,
    reason: str,
    speech_s: float,
    silence_s: float,
    turn_latency_ms: float,
) -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        valid = outcome == _VOICE_OUTCOME_VALID
        if valid:
            _VOICE_METRICS["valid_turns"] = int(_VOICE_METRICS.get("valid_turns") or 0) + 1
        else:
            _VOICE_METRICS["no_op_turns"] = int(_VOICE_METRICS.get("no_op_turns") or 0) + 1
        if outcome == _VOICE_OUTCOME_FALSE_WAKE:
            _VOICE_METRICS["false_wake_count"] = int(_VOICE_METRICS.get("false_wake_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_WAKE_NO_SPEECH:
            _VOICE_METRICS["wake_no_speech_count"] = int(_VOICE_METRICS.get("wake_no_speech_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_LOW_SIGNAL:
            _VOICE_METRICS["low_signal_count"] = int(_VOICE_METRICS.get("low_signal_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_CLIPPED:
            _VOICE_METRICS["clipped_ambiguous_count"] = int(_VOICE_METRICS.get("clipped_ambiguous_count") or 0) + 1
        if _text(reason) == "blank_wake_timeout":
            _VOICE_METRICS["blank_wake_count"] = int(_VOICE_METRICS.get("blank_wake_count") or 0) + 1
        _voice_metric_add(_VOICE_METRICS.setdefault("speech_duration", {"count": 0, "total": 0.0}), speech_s)
        _voice_metric_add(_VOICE_METRICS.setdefault("silence_duration", {"count": 0, "total": 0.0}), silence_s)
        _voice_metric_add(_VOICE_METRICS.setdefault("turn_latency_ms", {"count": 0, "total": 0.0}), turn_latency_ms)

        device = _voice_device_metrics(selector)
        if valid:
            device["valid_turns"] = int(device.get("valid_turns") or 0) + 1
        else:
            device["no_op_turns"] = int(device.get("no_op_turns") or 0) + 1
        if outcome == _VOICE_OUTCOME_FALSE_WAKE:
            device["false_wake_count"] = int(device.get("false_wake_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_WAKE_NO_SPEECH:
            device["wake_no_speech_count"] = int(device.get("wake_no_speech_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_LOW_SIGNAL:
            device["low_signal_count"] = int(device.get("low_signal_count") or 0) + 1
        elif outcome == _VOICE_OUTCOME_CLIPPED:
            device["clipped_ambiguous_count"] = int(device.get("clipped_ambiguous_count") or 0) + 1
        _voice_metric_add(device.setdefault("speech_duration", {"count": 0, "total": 0.0}), speech_s)
        _voice_metric_add(device.setdefault("silence_duration", {"count": 0, "total": 0.0}), silence_s)
        _voice_metric_add(device.setdefault("turn_latency_ms", {"count": 0, "total": 0.0}), turn_latency_ms)
        device["last_outcome"] = _text(outcome)
        device["last_reason"] = _text(reason)
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_record_continued_chat_attempt(selector: str) -> None:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        _VOICE_METRICS["continued_chat_attempts"] = int(_VOICE_METRICS.get("continued_chat_attempts") or 0) + 1
        device = _voice_device_metrics(selector)
        device["last_seen_ts"] = _now()
        _voice_metrics_persist_locked()


def _voice_metrics_snapshot() -> Dict[str, Any]:
    with _VOICE_METRICS_LOCK:
        _voice_metrics_load_persisted_locked()
        metrics = json.loads(json.dumps(_VOICE_METRICS))
    devices = metrics.get("devices") if isinstance(metrics.get("devices"), dict) else {}
    for row in devices.values():
        if not isinstance(row, dict):
            continue
        row["avg_turn_latency_ms"] = round(_voice_metric_avg(row.get("turn_latency_ms") if isinstance(row.get("turn_latency_ms"), dict) else {}), 1)
        row["avg_stt_latency_ms"] = round(_voice_metric_avg(row.get("stt_latency_ms") if isinstance(row.get("stt_latency_ms"), dict) else {}), 1)
        row["avg_tts_latency_ms"] = round(_voice_metric_avg(row.get("tts_latency_ms") if isinstance(row.get("tts_latency_ms"), dict) else {}), 1)
        row["avg_speech_s"] = round(_voice_metric_avg(row.get("speech_duration") if isinstance(row.get("speech_duration"), dict) else {}), 2)
        row["avg_silence_s"] = round(_voice_metric_avg(row.get("silence_duration") if isinstance(row.get("silence_duration"), dict) else {}), 2)
    metrics["avg_turn_latency_ms"] = round(_voice_metric_avg(metrics.get("turn_latency_ms") if isinstance(metrics.get("turn_latency_ms"), dict) else {}), 1)
    metrics["avg_stt_latency_ms"] = round(_voice_metric_avg(metrics.get("stt_latency_ms") if isinstance(metrics.get("stt_latency_ms"), dict) else {}), 1)
    metrics["avg_tts_latency_ms"] = round(_voice_metric_avg(metrics.get("tts_latency_ms") if isinstance(metrics.get("tts_latency_ms"), dict) else {}), 1)
    metrics["avg_speech_s"] = round(_voice_metric_avg(metrics.get("speech_duration") if isinstance(metrics.get("speech_duration"), dict) else {}), 2)
    metrics["avg_silence_s"] = round(_voice_metric_avg(metrics.get("silence_duration") if isinstance(metrics.get("silence_duration"), dict) else {}), 2)
    metrics["avg_stt_latency_by_backend_ms"] = _voice_metric_backend_avgs(
        metrics.get("stt_latency_ms") if isinstance(metrics.get("stt_latency_ms"), dict) else {}
    )
    metrics["avg_tts_latency_by_backend_ms"] = _voice_metric_backend_avgs(
        metrics.get("tts_latency_ms") if isinstance(metrics.get("tts_latency_ms"), dict) else {}
    )
    attempts = int(metrics.get("continued_chat_attempts") or 0)
    successes = int(metrics.get("continued_chat_reopens") or 0)
    metrics["continued_chat_reopen_rate"] = round((float(successes) / float(attempts) * 100.0), 1) if attempts > 0 else 0.0
    return metrics


def _transcript_completeness_assessment(text: str) -> Dict[str, Any]:
    raw = _text(text)
    if not raw:
        return {"complete": True, "reason": "empty"}
    compact = re.sub(r"\s+", " ", raw).strip()
    lower = compact.lower()
    words = re.findall(r"[a-z0-9']+", lower)
    if not words:
        return {"complete": True, "reason": "no_words"}
    if lower in _TRANSCRIPT_COMPLETE_SHORT_COMMANDS:
        return {"complete": True, "reason": "short_command"}
    if compact.endswith(("...", "…", "-", "—", ":", "/", ",")):
        return {"complete": False, "reason": "trailing_punctuation"}
    if words[-1] in _TRANSCRIPT_INCOMPLETE_TRAILING_WORDS:
        return {"complete": False, "reason": f"trailing_word:{words[-1]}"}
    for phrase in _TRANSCRIPT_INCOMPLETE_TRAILING_PHRASES:
        if lower.endswith(phrase):
            return {"complete": False, "reason": f"trailing_phrase:{phrase}"}
    return {"complete": True, "reason": "looks_complete"}


def _classify_no_op_outcome(
    session: "VoiceSessionRuntime",
    *,
    reason: str,
    transcript: str = "",
) -> str:
    transcript_text = _text(transcript)
    reason_token = _text(reason)
    speech_s = max(0.0, float(getattr(session, "speech_duration_s", 0.0) or 0.0))
    wake_word_session = bool(_text(getattr(session, "wake_word", "")))
    if reason_token == "low_signal_transcript":
        return _VOICE_OUTCOME_LOW_SIGNAL
    if reason_token == "clipped_ambiguous_transcript":
        return _VOICE_OUTCOME_CLIPPED
    if reason_token in {"blank_wake_timeout", "audio_stall_no_audio"}:
        return _VOICE_OUTCOME_FALSE_WAKE if wake_word_session else _VOICE_OUTCOME_WAKE_NO_SPEECH
    if reason_token in {"audio_stall_no_speech", "server_vad", "empty_transcript"}:
        if wake_word_session and speech_s < 0.12 and not transcript_text:
            return _VOICE_OUTCOME_FALSE_WAKE
        if speech_s < 0.30 and not transcript_text:
            return _VOICE_OUTCOME_WAKE_NO_SPEECH
        if transcript_text:
            assessment = _transcript_completeness_assessment(transcript_text)
            if not bool(assessment.get("complete")):
                return _VOICE_OUTCOME_CLIPPED
        return _VOICE_OUTCOME_WAKE_NO_SPEECH if wake_word_session else _VOICE_OUTCOME_CLIPPED
    if transcript_text:
        assessment = _transcript_completeness_assessment(transcript_text)
        if not bool(assessment.get("complete")):
            return _VOICE_OUTCOME_CLIPPED
    return _VOICE_OUTCOME_WAKE_NO_SPEECH if wake_word_session else _VOICE_OUTCOME_CLIPPED


async def _response_is_followup_question(text: str) -> bool:
    reply = _text(text).strip()
    if not reply:
        return False

    prompt = (
        "You classify whether an assistant reply is genuinely asking the user for another spoken response right now.\n"
        "Return strict JSON only with exactly this shape: {\"follow_up\": true}\n"
        "or {\"follow_up\": false}\n\n"
        "Mark true only when the assistant is explicitly asking a direct follow-up question or inviting an immediate answer.\n"
        "Mark false for statements, confirmations, explanations, rhetorical questions, quoted questions, or replies that do not need the mic reopened.\n"
    )
    user_text = (
        "Assistant reply:\n"
        f"{reply}\n\n"
        "Return JSON only."
    )

    try:
        async with get_llm_client_from_env(redis_conn=redis_client) as llm_client:
            result = await llm_client.chat(
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_text},
                ],
                temperature=0,
                max_tokens=40,
                timeout=DEFAULT_CONTINUED_CHAT_CLASSIFY_TIMEOUT_S,
                activity="voice_followup_classifier",
            )
        content = _text(((result or {}).get("message") or {}).get("content"))
        parsed_text = extract_json(content)
        if parsed_text:
            parsed = json.loads(parsed_text)
            if isinstance(parsed, dict) and isinstance(parsed.get("follow_up"), bool):
                decision = bool(parsed.get("follow_up"))
                _native_debug(
                    f"continued chat classifier follow_up={decision} reply_tail={reply[-120:]!r}"
                )
                return decision
        _native_debug(
            f"continued chat classifier invalid_json raw={content[:200]!r}"
        )
    except Exception as exc:
        _native_debug(f"continued chat classifier failed error={exc}")

    return False


def _require_api_auth(x_tater_token: Optional[str]) -> None:
    settings = _voice_settings()
    if not _as_bool(settings.get("API_AUTH_ENABLED"), False):
        return
    expected = _text(settings.get("API_AUTH_KEY"))
    if not expected:
        raise HTTPException(status_code=503, detail="API auth enabled but API_AUTH_KEY is not configured")
    got = _text(x_tater_token)
    if got != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Tater-Token")


def _voice_settings() -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        row = redis_client.hgetall(VOICE_CORE_SETTINGS_HASH_KEY) or {}
        if isinstance(row, dict):
            return row
    return {}


def _shared_speech_voice_settings() -> Dict[str, Any]:
    shared = get_shared_speech_settings() or {}
    return {
        "VOICE_STT_BACKEND": shared.get("stt_backend"),
        "VOICE_ACCELERATION": shared.get("acceleration"),
        "VOICE_WYOMING_STT_HOST": shared.get("wyoming_stt_host"),
        "VOICE_WYOMING_STT_PORT": shared.get("wyoming_stt_port"),
        "VOICE_TTS_BACKEND": shared.get("tts_backend"),
        "VOICE_TTS_MODEL": shared.get("tts_model"),
        "VOICE_TTS_VOICE": shared.get("tts_voice"),
        "VOICE_KOKORO_OUTPUT_GAIN": shared.get("kokoro_output_gain"),
        "VOICE_POCKET_TTS_OUTPUT_GAIN": shared.get("pocket_tts_output_gain"),
        "VOICE_WYOMING_TTS_HOST": shared.get("wyoming_tts_host"),
        "VOICE_WYOMING_TTS_PORT": shared.get("wyoming_tts_port"),
        "VOICE_WYOMING_TTS_VOICE": shared.get("wyoming_tts_voice"),
        "VOICE_OPENAI_TTS_BASE_URL": shared.get("openai_tts_base_url"),
        "VOICE_OPENAI_TTS_API_KEY": shared.get("openai_tts_api_key"),
        "VOICE_CHATTERBOX_TTS_BASE_URL": shared.get("chatterbox_tts_base_url"),
        "VOICE_CHATTERBOX_TTS_VOICE_MODE": shared.get("chatterbox_tts_voice_mode"),
        "VOICE_CHATTERBOX_TTS_CHUNK_SIZE": shared.get("chatterbox_tts_chunk_size"),
        "VOICE_CHATTERBOX_TTS_TEMPERATURE": shared.get("chatterbox_tts_temperature"),
        "VOICE_CHATTERBOX_TTS_EXAGGERATION": shared.get("chatterbox_tts_exaggeration"),
        "VOICE_CHATTERBOX_TTS_CFG_WEIGHT": shared.get("chatterbox_tts_cfg_weight"),
        "VOICE_CHATTERBOX_TTS_SEED": shared.get("chatterbox_tts_seed"),
        "VOICE_CHATTERBOX_TTS_SPEED_FACTOR": shared.get("chatterbox_tts_speed_factor"),
        "VOICE_CHATTERBOX_TTS_LANGUAGE": shared.get("chatterbox_tts_language"),
        "VOICE_CHATTERBOX_TTS_STREAMING_ENABLED": shared.get("chatterbox_tts_streaming_enabled"),
    }


def _voice_settings_with_shared_speech(extra_values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    merged = dict(_voice_settings())
    merged.update({key: value for key, value in _shared_speech_voice_settings().items() if value is not None})
    if isinstance(extra_values, dict):
        merged.update({key: value for key, value in extra_values.items() if value is not None})
    return merged


def _cuda_runtime_available() -> bool:
    global _CUDA_RUNTIME_AVAILABLE_CACHE
    cached = _CUDA_RUNTIME_AVAILABLE_CACHE
    if cached is not None:
        return bool(cached)
    with _CUDA_RUNTIME_AVAILABLE_LOCK:
        cached = _CUDA_RUNTIME_AVAILABLE_CACHE
        if cached is not None:
            return bool(cached)
        available = False
        with contextlib.suppress(Exception):
            ctranslate2_mod = importlib.import_module("ctranslate2")
            if int(getattr(ctranslate2_mod, "get_cuda_device_count")()) > 0:
                available = True
        if not available:
            with contextlib.suppress(Exception):
                torch_mod = importlib.import_module("torch")
                cuda_mod = getattr(torch_mod, "cuda", None)
                if cuda_mod is not None and bool(cuda_mod.is_available()) and not _torch_has_rocm(torch_mod):
                    available = True
        _CUDA_RUNTIME_AVAILABLE_CACHE = bool(available)
        return bool(available)


def _torch_has_rocm(torch_mod: Any) -> bool:
    version = getattr(torch_mod, "version", None)
    return bool(getattr(version, "hip", None))


def _ctranslate2_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        ctranslate2_mod = importlib.import_module("ctranslate2")
        return int(getattr(ctranslate2_mod, "get_cuda_device_count")()) > 0
    return False


def _onnx_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        ort_mod = importlib.import_module("onnxruntime")
        providers = set(getattr(ort_mod, "get_available_providers")() or [])
        return "CUDAExecutionProvider" in providers
    return False


def _onnx_rocm_available() -> bool:
    with contextlib.suppress(Exception):
        ort_mod = importlib.import_module("onnxruntime")
        providers = set(getattr(ort_mod, "get_available_providers")() or [])
        return "ROCMExecutionProvider" in providers or "MIGraphXExecutionProvider" in providers
    return False


def _torch_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        return cuda_mod is not None and bool(cuda_mod.is_available()) and not _torch_has_rocm(torch_mod)
    return False


def _torch_rocm_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        return cuda_mod is not None and bool(cuda_mod.is_available()) and _torch_has_rocm(torch_mod)
    return False


def _mps_runtime_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        backends = getattr(torch_mod, "backends", None)
        mps = getattr(backends, "mps", None)
        if mps is not None and bool(mps.is_available()):
            return True
    return False


def _effective_speech_acceleration() -> str:
    selected = normalize_speech_acceleration(_voice_settings_with_shared_speech().get("VOICE_ACCELERATION"))
    if selected != "auto":
        if selected == "cuda":
            return "cuda" if _cuda_runtime_available() else "cpu"
        if selected == "rocm":
            return "rocm" if _torch_rocm_available() else "cpu"
        if selected == "mps":
            return "mps" if _mps_runtime_available() else "cpu"
        return selected
    env_selected = normalize_speech_acceleration(
        os.getenv("TATER_SPEECH_ACCELERATION") or os.getenv("TATER_VOICE_ACCELERATION")
    )
    if env_selected != "auto":
        if env_selected == "cuda":
            return "cuda" if _cuda_runtime_available() else "cpu"
        if env_selected == "rocm":
            return "rocm" if _torch_rocm_available() else "cpu"
        if env_selected == "mps":
            return "mps" if _mps_runtime_available() else "cpu"
        return env_selected
    if _cuda_runtime_available():
        return "cuda"
    if _torch_rocm_available():
        return "rocm"
    if _mps_runtime_available():
        return "mps"
    return "cpu"


def _faster_whisper_device() -> str:
    return "cuda" if _effective_speech_acceleration() == "cuda" and _ctranslate2_cuda_available() else DEFAULT_FASTER_WHISPER_DEVICE


def _mlx_whisper_model() -> str:
    return _text(os.getenv("TATER_MLX_WHISPER_MODEL")) or DEFAULT_MLX_WHISPER_MODEL


def _normalize_faster_whisper_compute_type(value: Any) -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_FASTER_WHISPER_COMPUTE_TYPE_SETTING
    return token if token in FASTER_WHISPER_COMPUTE_TYPES else DEFAULT_FASTER_WHISPER_COMPUTE_TYPE_SETTING


def _cuda_device_capability() -> Optional[Tuple[int, int]]:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        if cuda_mod is None or not bool(cuda_mod.is_available()):
            return None
        major, minor = cuda_mod.get_device_capability(0)
        return int(major), int(minor)
    return None


def _faster_whisper_compute_type() -> str:
    env_compute_type = _normalize_faster_whisper_compute_type(os.getenv("TATER_FASTER_WHISPER_COMPUTE_TYPE"))
    selected = env_compute_type
    if not _text(os.getenv("TATER_FASTER_WHISPER_COMPUTE_TYPE")):
        selected = _normalize_faster_whisper_compute_type(_voice_settings().get("VOICE_FASTER_WHISPER_COMPUTE_TYPE"))
    if selected != "auto":
        return selected
    if _faster_whisper_device() != "cuda":
        return DEFAULT_FASTER_WHISPER_COMPUTE_TYPE
    capability = _cuda_device_capability()
    if capability is not None and capability[0] < 7:
        return DEFAULT_FASTER_WHISPER_COMPUTE_TYPE
    return DEFAULT_FASTER_WHISPER_CUDA_COMPUTE_TYPE


def _faster_whisper_compute_type_label() -> str:
    env_raw = os.getenv("TATER_FASTER_WHISPER_COMPUTE_TYPE")
    selected = _normalize_faster_whisper_compute_type(env_raw if _text(env_raw) else _voice_settings().get("VOICE_FASTER_WHISPER_COMPUTE_TYPE"))
    effective = _faster_whisper_compute_type()
    if selected == "auto":
        capability = _cuda_device_capability()
        cap = f" cc={capability[0]}.{capability[1]}" if capability else ""
        return f"auto->{effective}{cap}"
    source = "env" if _text(env_raw) else "settings"
    return f"{source}->{effective}"


def _faster_whisper_beam_size(*, partial: bool = False) -> int:
    if partial:
        return DEFAULT_FASTER_WHISPER_PARTIAL_BEAM_SIZE
    return _get_int_setting(
        "VOICE_FASTER_WHISPER_BEAM_SIZE",
        DEFAULT_FASTER_WHISPER_BEAM_SIZE,
        minimum=1,
        maximum=8,
    )


def _faster_whisper_initial_prompt() -> str:
    raw = _voice_settings().get("VOICE_FASTER_WHISPER_INITIAL_PROMPT")
    prompt = _text(raw)
    if raw is None or prompt == "":
        return DEFAULT_FASTER_WHISPER_INITIAL_PROMPT
    if prompt.lower() in {"none", "off", "disabled"}:
        return ""
    return prompt[:1000]


def _kokoro_engine(acceleration: Any = None) -> str:
    env_engine = _text(os.getenv("TATER_KOKORO_ENGINE") or os.getenv("TATER_KOKORO_RUNTIME")).lower().replace("-", "_")
    env_provider = _text(os.getenv("TATER_KOKORO_PROVIDER")).lower().replace("-", "_")
    token = env_engine or env_provider
    if token in {"torch", "pytorch"}:
        return "torch"
    if token in {"rocm", "amd", "amd_rocm"}:
        return "torch"
    if token in {"onnx", "pykokoro", "onnxruntime", "cpu", "cuda", "gpu", "nvidia", "nvidia_cuda"}:
        return "onnx"

    selected = normalize_speech_acceleration(acceleration)
    effective = _effective_speech_acceleration() if selected == "auto" else selected
    if effective in {"mps", "rocm"} and KokoroTorchPipeline is not None:
        return "torch"
    return "onnx"


def _kokoro_torch_device(acceleration: Any = None) -> str:
    selected = normalize_speech_acceleration(acceleration)
    effective = _effective_speech_acceleration() if selected == "auto" else selected
    if effective == "cuda" and _torch_cuda_available():
        return "cuda"
    if effective == "rocm" and _torch_rocm_available():
        return "cuda"
    if effective == "mps" and _mps_runtime_available():
        return "mps"
    return "cpu"


def _kokoro_provider() -> str:
    if _kokoro_engine() == "torch":
        return f"torch:{_kokoro_torch_device()}"
    env_provider = _text(os.getenv("TATER_KOKORO_PROVIDER")).lower().replace("-", "_")
    if env_provider in {"cuda", "gpu", "nvidia", "nvidia_cuda"}:
        return "cuda"
    if env_provider in {"cpu", "none", "off"}:
        return "cpu"
    return "cuda" if _effective_speech_acceleration() == "cuda" and _onnx_cuda_available() else DEFAULT_KOKORO_PROVIDER


def _get_bool_setting(name: str, default: bool) -> bool:
    return _as_bool(_voice_settings().get(name), default)


def _get_int_setting(name: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    return _as_int(_voice_settings().get(name), default, minimum=minimum, maximum=maximum)


def _get_float_setting(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    return _as_float(_voice_settings().get(name), default, minimum=minimum, maximum=maximum)


def _normalize_stt_backend(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_STT_BACKEND
    if token in {"faster_whisper", "fasterwhisper", "whisper"}:
        return "faster_whisper"
    if token in {"mlx_whisper", "mlxwhisper", "mlx"}:
        return "mlx_whisper"
    if token == "vosk":
        return "vosk"
    if token == "wyoming":
        return "wyoming"
    return DEFAULT_STT_BACKEND


def _normalize_vad_backend(value: Any, *, default: str = DEFAULT_VAD_BACKEND) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    fallback = _lower(default).replace("-", "_").replace(" ", "_")
    if fallback not in {"silero", "webrtc", "auto"}:
        fallback = DEFAULT_VAD_BACKEND
    if token in {"", "default"}:
        return fallback
    if token in {"silero", "silero_vad"}:
        return "silero"
    if token in {"webrtc", "webrtcvad", "webrtc_vad", "web_rtc"}:
        return "webrtc"
    if token == "auto":
        return "auto"
    return fallback


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


def _normalize_tts_backend(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_TTS_BACKEND
    if token == "wyoming":
        return "wyoming"
    if token in {"openai_compatible", "openai", "openai_api"}:
        return "openai_compatible"
    if token in {"chatterbox", "chatterbox_tts"}:
        return "chatterbox"
    if token == "kokoro":
        return "kokoro"
    if token in {"pocket_tts", "pockettts", "pocket"}:
        return "pocket_tts"
    if token == "piper":
        return "piper"
    return DEFAULT_TTS_BACKEND


def _normalize_chatterbox_voice_mode(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    if token in {"clone", "cloned", "reference", "reference_audio"}:
        return "clone"
    return DEFAULT_CHATTERBOX_TTS_VOICE_MODE


def _normalize_chatterbox_chunk_size(value: Any) -> int:
    return _as_int(value, DEFAULT_CHATTERBOX_TTS_CHUNK_SIZE, minimum=50, maximum=500)


def _tts_model_root() -> str:
    return os.path.expanduser(DEFAULT_TTS_MODEL_ROOT)


def _ensure_tts_model_root() -> str:
    root = _tts_model_root()
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _tts_backend_model_root(backend: str) -> str:
    base = _ensure_tts_model_root()
    token = _normalize_tts_backend(backend)
    dirname_map = {
        "kokoro": "kokoro",
        "pocket_tts": "pocket-tts",
        "piper": "piper",
        "openai_compatible": "openai-compatible",
        "chatterbox": "chatterbox",
        "wyoming": "wyoming",
    }
    dirname = dirname_map.get(token, token or DEFAULT_TTS_BACKEND)
    return os.path.join(base, dirname)


def _ensure_tts_backend_model_root(backend: str) -> str:
    root = _tts_backend_model_root(backend)
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _patch_kokoro_ssmd_parser() -> None:
    global _kokoro_ssmd_patch_applied
    if _kokoro_ssmd_patch_applied:
        return
    try:
        ssmd_parser_mod = importlib.import_module("pykokoro.ssmd_parser")
        ssmd_doc_parser_mod = importlib.import_module("pykokoro.stages.doc_parsers.ssmd")
    except Exception:
        return

    original = getattr(ssmd_parser_mod, "parse_ssmd_to_segments", None)
    if not callable(original):
        return
    if getattr(original, "_tater_forces_no_spacy", False):
        _kokoro_ssmd_patch_applied = True
        return

    def _wrapped_parse_ssmd_to_segments(*args, **kwargs):
        wrapped_kwargs = dict(kwargs or {})
        wrapped_kwargs["use_spacy"] = False
        wrapped_kwargs.setdefault("model_size", "sm")
        return original(*args, **wrapped_kwargs)

    setattr(_wrapped_parse_ssmd_to_segments, "_tater_forces_no_spacy", True)
    setattr(ssmd_parser_mod, "parse_ssmd_to_segments", _wrapped_parse_ssmd_to_segments)
    setattr(ssmd_doc_parser_mod, "parse_ssmd_to_segments", _wrapped_parse_ssmd_to_segments)
    _kokoro_ssmd_patch_applied = True


def _option_rows_from_values(values: List[str], *, current_value: Any = "", labels: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    for raw in values:
        value = _text(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        label = _text((labels or {}).get(value)) or value
        rows.append({"value": value, "label": label})

    current = _text(current_value)
    if current and current not in seen:
        rows.insert(0, {"value": current, "label": _text((labels or {}).get(current)) or current})
    return rows


def _prefer_value_first(values: List[str], preferred_value: Any) -> List[str]:
    preferred = _text(preferred_value)
    ordered = [_text(value) for value in values if _text(value)]
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
    token = _text(value).lower()
    if not token:
        return False
    return token[0] in _supported_kokoro_language_prefixes()


def _kokoro_voice_label(value: Any) -> str:
    token = _text(value)
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
    labels = {key: _text(spec.get("label")) or key for key, spec in KOKORO_MODEL_SPECS.items()}
    values = []
    for model_key, spec in KOKORO_MODEL_SPECS.items():
        variant = _text((spec or {}).get("variant")) or "v1.0"
        voices = list(KOKORO_VOICE_NAMES_BY_VARIANT.get(variant) or KOKORO_VOICE_NAMES_BY_VARIANT.get("v1.0") or [])
        if any(_kokoro_voice_supported(voice) for voice in voices):
            values.append(model_key)
    current_model = _text(current_value)
    if current_model not in values:
        current_model = DEFAULT_KOKORO_MODEL if DEFAULT_KOKORO_MODEL in values else ""
    return _option_rows_from_values(values, current_value=current_model, labels=labels)


def _kokoro_model_spec(model_id: Any) -> Dict[str, str]:
    token = _text(model_id)
    if token in KOKORO_MODEL_SPECS:
        return dict(KOKORO_MODEL_SPECS[token])
    return dict(KOKORO_MODEL_SPECS[DEFAULT_KOKORO_MODEL])


def _kokoro_voice_option_rows(*, model_id: Any, current_value: Any = "") -> List[Dict[str, str]]:
    spec = _kokoro_model_spec(model_id)
    variant = _text(spec.get("variant")) or "v1.0"
    voices = [
        voice
        for voice in list(KOKORO_VOICE_NAMES_BY_VARIANT.get(variant) or KOKORO_VOICE_NAMES_BY_VARIANT.get("v1.0") or [])
        if _kokoro_voice_supported(voice)
    ]
    labels = {voice: _kokoro_voice_label(voice) for voice in voices}
    preferred_voice = _text(current_value)
    if preferred_voice not in voices:
        preferred_voice = DEFAULT_KOKORO_VOICE if DEFAULT_KOKORO_VOICE in voices else _text(voices[0] if voices else "")
    voices = _prefer_value_first(voices, preferred_voice)
    return _option_rows_from_values(voices, current_value=preferred_voice, labels=labels)


def _pocket_tts_model_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    labels = {DEFAULT_POCKET_TTS_MODEL: f"Pocket TTS {DEFAULT_POCKET_TTS_MODEL}"}
    return _option_rows_from_values([DEFAULT_POCKET_TTS_MODEL], current_value=current_value, labels=labels)


def _pocket_tts_voice_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    return _option_rows_from_values(sorted(POCKET_TTS_PREDEFINED_VOICES.keys()), current_value=current_value)


def _stt_model_root() -> str:
    return os.path.expanduser(DEFAULT_STT_MODEL_ROOT)


def _ensure_stt_model_root() -> str:
    root = _stt_model_root()
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _stt_backend_model_root(backend: str) -> str:
    base = _ensure_stt_model_root()
    token = _normalize_stt_backend(backend)
    dirname = "faster-whisper" if token == "faster_whisper" else token
    return os.path.join(base, dirname)


def _ensure_stt_backend_model_root(backend: str) -> str:
    root = _stt_backend_model_root(backend)
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _looks_like_faster_whisper_model_dir(path: str) -> bool:
    token = os.path.expanduser(_text(path))
    if not token or not os.path.isdir(token):
        return False
    required_files = ("config.json", "model.bin", "tokenizer.json")
    return all(os.path.isfile(os.path.join(token, name)) for name in required_files)


def _resolve_faster_whisper_model_source() -> str:
    root = _ensure_stt_backend_model_root("faster_whisper")
    direct_candidates = [root]
    for candidate in direct_candidates:
        if _looks_like_faster_whisper_model_dir(candidate):
            return candidate

    repo_dir = os.path.join(root, f"models--Systran--faster-whisper-{DEFAULT_FASTER_WHISPER_MODEL}")
    refs_main = os.path.join(repo_dir, "refs", "main")
    with contextlib.suppress(Exception):
        snapshot_ref = _text(open(refs_main, "r", encoding="utf-8").read()).strip()
        if snapshot_ref:
            snapshot_dir = os.path.join(repo_dir, "snapshots", snapshot_ref)
            if _looks_like_faster_whisper_model_dir(snapshot_dir):
                return snapshot_dir

    snapshot_root = os.path.join(repo_dir, "snapshots")
    with contextlib.suppress(Exception):
        for name in sorted(os.listdir(snapshot_root), reverse=True):
            candidate = os.path.join(snapshot_root, name)
            if _looks_like_faster_whisper_model_dir(candidate):
                return candidate

    with contextlib.suppress(Exception):
        for current_root, dirs, _files in os.walk(root):
            dirs.sort(reverse=True)
            if _looks_like_faster_whisper_model_dir(current_root):
                return current_root

    return DEFAULT_FASTER_WHISPER_MODEL


def _looks_like_vosk_model_dir(path: str) -> bool:
    token = os.path.expanduser(_text(path))
    if not token or not os.path.isdir(token):
        return False
    return (
        os.path.isfile(os.path.join(token, "am", "final.mdl"))
        and os.path.isdir(os.path.join(token, "conf"))
    )


def _find_vosk_model_path(search_roots: Optional[List[str]] = None) -> str:
    roots = search_roots or [_ensure_stt_backend_model_root("vosk"), _ensure_stt_model_root()]
    for root in roots:
        if _looks_like_vosk_model_dir(root):
            return root

        with contextlib.suppress(Exception):
            for name in sorted(os.listdir(root)):
                candidate = os.path.join(root, name)
                if _looks_like_vosk_model_dir(candidate):
                    return candidate

        with contextlib.suppress(Exception):
            for current_root, dirs, _files in os.walk(root):
                dirs.sort()
                if _looks_like_vosk_model_dir(current_root):
                    return current_root
    return ""


def _safe_extract_zip(archive_path: str, extract_root: str) -> None:
    abs_root = os.path.abspath(extract_root)
    with zipfile.ZipFile(archive_path) as zf:
        for member in zf.infolist():
            target = os.path.abspath(os.path.join(abs_root, member.filename))
            if os.path.commonpath([abs_root, target]) != abs_root:
                raise RuntimeError(f"Refusing to extract unexpected path from archive: {member.filename}")
        zf.extractall(abs_root)


def _bootstrap_vosk_model() -> str:
    backend_root = _ensure_stt_backend_model_root("vosk")
    existing = _find_vosk_model_path([backend_root, _ensure_stt_model_root()])
    if existing:
        return existing

    with _vosk_bootstrap_lock:
        existing = _find_vosk_model_path([backend_root, _ensure_stt_model_root()])
        if existing:
            return existing

        logger.info(
            "[native-voice] vosk model missing; downloading url=%s target_root=%s",
            DEFAULT_VOSK_MODEL_URL,
            backend_root,
        )
        with tempfile.TemporaryDirectory(prefix="tater_vosk_") as temp_dir:
            archive_path = os.path.join(temp_dir, "vosk_model.zip")
            extract_root = os.path.join(temp_dir, "extract")
            os.makedirs(extract_root, exist_ok=True)
            urllib_request.urlretrieve(DEFAULT_VOSK_MODEL_URL, archive_path)
            _safe_extract_zip(archive_path, extract_root)
            extracted_model = _find_vosk_model_path([extract_root])
            if not extracted_model:
                raise RuntimeError(
                    f"Downloaded Vosk archive did not contain a valid model from {DEFAULT_VOSK_MODEL_URL}"
                )

            final_dir = os.path.join(backend_root, os.path.basename(extracted_model.rstrip(os.sep)))
            with contextlib.suppress(Exception):
                if os.path.isdir(final_dir) and not _looks_like_vosk_model_dir(final_dir):
                    shutil.rmtree(final_dir)
            shutil.copytree(extracted_model, final_dir, dirs_exist_ok=True)
            if not _looks_like_vosk_model_dir(final_dir):
                raise RuntimeError(f"Vosk model download completed but final model dir is invalid: {final_dir}")
            logger.info("[native-voice] vosk model downloaded target=%s", final_dir)
            return final_dir


def _resolve_vosk_model_path() -> str:
    resolved = _find_vosk_model_path()
    if resolved:
        return resolved
    return _bootstrap_vosk_model()


def _voice_config_snapshot() -> Dict[str, Any]:
    settings = _voice_settings_with_shared_speech()
    tts_backend = _normalize_tts_backend(settings.get("VOICE_TTS_BACKEND"))
    tts_model = _text(settings.get("VOICE_TTS_MODEL"))
    tts_voice = _text(settings.get("VOICE_TTS_VOICE"))
    vad_backend = _normalize_vad_backend(settings.get("VOICE_VAD_BACKEND"))
    selected_acceleration = normalize_speech_acceleration(settings.get("VOICE_ACCELERATION"))
    effective_acceleration = _effective_speech_acceleration()
    return {
        "native_debug": _native_debug_enabled(),
        "wyoming_timeout_s": _get_float_setting("VOICE_NATIVE_WYOMING_TIMEOUT_S", DEFAULT_WYOMING_TIMEOUT_SECONDS, minimum=5.0, maximum=180.0),
        "wyoming_stt_queue_chunks": _wyoming_stt_queue_max_chunks(),
        "wyoming_stt_queue_drop_limit": _wyoming_stt_queue_drop_limit(),
        "acceleration": {
            "selected": selected_acceleration,
            "effective": effective_acceleration,
            "cuda_available": _cuda_runtime_available(),
            "ctranslate2_cuda_available": _ctranslate2_cuda_available(),
            "onnx_cuda_available": _onnx_cuda_available(),
            "onnx_rocm_available": _onnx_rocm_available(),
            "torch_cuda_available": _torch_cuda_available(),
            "torch_rocm_available": _torch_rocm_available(),
            "mps_available": _mps_runtime_available(),
        },
        "wyoming_stt": {
            "host": _text(settings.get("VOICE_WYOMING_STT_HOST")) or DEFAULT_WYOMING_STT_HOST,
            "port": _get_int_setting("VOICE_WYOMING_STT_PORT", DEFAULT_WYOMING_STT_PORT, minimum=1, maximum=65535),
        },
        "stt": {
            "backend": _normalize_stt_backend(settings.get("VOICE_STT_BACKEND")),
            "model_root": DEFAULT_STT_MODEL_ROOT,
            "faster_whisper": {
                "model": DEFAULT_FASTER_WHISPER_MODEL,
                "device": _faster_whisper_device(),
                "compute_type": _faster_whisper_compute_type(),
                "beam_size": _faster_whisper_beam_size(partial=False),
                "partial_beam_size": DEFAULT_FASTER_WHISPER_PARTIAL_BEAM_SIZE,
                "model_root": _stt_backend_model_root("faster_whisper"),
            },
            "mlx_whisper": {
                "model": _mlx_whisper_model(),
                "model_root": _stt_backend_model_root("mlx_whisper"),
            },
            "vosk": {
                "model_root": _stt_backend_model_root("vosk"),
            },
        },
        "tts": {
            "backend": tts_backend,
            "model_root": DEFAULT_TTS_MODEL_ROOT,
            "model": (
                tts_model
                or (
                    DEFAULT_KOKORO_MODEL
                    if tts_backend == "kokoro"
                    else DEFAULT_POCKET_TTS_MODEL
                    if tts_backend == "pocket_tts"
                    else DEFAULT_PIPER_MODEL
                    if tts_backend == "piper"
                    else DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL
                    if tts_backend == "openai_compatible"
                    else ""
                )
            ),
            "voice": (
                tts_voice
                or (
                    DEFAULT_KOKORO_VOICE
                    if tts_backend == "kokoro"
                    else DEFAULT_POCKET_TTS_VOICE
                    if tts_backend == "pocket_tts"
                    else DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE
                    if tts_backend == "openai_compatible"
                    else _text(settings.get("VOICE_TTS_VOICE"))
                    if tts_backend == "chatterbox"
                    else ""
                )
            ),
            "openai_compatible": {
                "base_url": _text(settings.get("VOICE_OPENAI_TTS_BASE_URL")) or DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL,
                "api_key_set": bool(_text(settings.get("VOICE_OPENAI_TTS_API_KEY"))),
                "model": _text(settings.get("VOICE_TTS_MODEL")) or DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL,
                "voice": _text(settings.get("VOICE_TTS_VOICE")) or DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE,
            },
            "chatterbox": {
                "base_url": _text(settings.get("VOICE_CHATTERBOX_TTS_BASE_URL")) or DEFAULT_CHATTERBOX_TTS_BASE_URL,
                "voice_mode": _normalize_chatterbox_voice_mode(settings.get("VOICE_CHATTERBOX_TTS_VOICE_MODE")),
                "voice": _text(settings.get("VOICE_TTS_VOICE")),
                "chunk_size": _normalize_chatterbox_chunk_size(settings.get("VOICE_CHATTERBOX_TTS_CHUNK_SIZE")),
                "temperature": _text(settings.get("VOICE_CHATTERBOX_TTS_TEMPERATURE")),
                "exaggeration": _text(settings.get("VOICE_CHATTERBOX_TTS_EXAGGERATION")),
                "cfg_weight": _text(settings.get("VOICE_CHATTERBOX_TTS_CFG_WEIGHT")),
                "seed": _text(settings.get("VOICE_CHATTERBOX_TTS_SEED")),
                "speed_factor": _text(settings.get("VOICE_CHATTERBOX_TTS_SPEED_FACTOR")),
                "language": _text(settings.get("VOICE_CHATTERBOX_TTS_LANGUAGE")),
                "streaming_enabled": _as_bool(settings.get("VOICE_CHATTERBOX_TTS_STREAMING_ENABLED"), False),
            },
            "kokoro": {
                "model": _text(settings.get("VOICE_TTS_MODEL")) or DEFAULT_KOKORO_MODEL,
                "voice": _text(settings.get("VOICE_TTS_VOICE")) or DEFAULT_KOKORO_VOICE,
                "output_gain": _as_float(
                    settings.get("VOICE_KOKORO_OUTPUT_GAIN"),
                    DEFAULT_KOKORO_OUTPUT_GAIN,
                    minimum=0.1,
                    maximum=4.0,
                ),
                "provider": _kokoro_provider(),
                "model_root": _tts_backend_model_root("kokoro"),
            },
            "pocket_tts": {
                "model": _text(settings.get("VOICE_TTS_MODEL")) or DEFAULT_POCKET_TTS_MODEL,
                "voice": _text(settings.get("VOICE_TTS_VOICE")) or DEFAULT_POCKET_TTS_VOICE,
                "output_gain": _as_float(
                    settings.get("VOICE_POCKET_TTS_OUTPUT_GAIN"),
                    DEFAULT_POCKET_TTS_OUTPUT_GAIN,
                    minimum=0.1,
                    maximum=4.0,
                ),
                "model_root": _tts_backend_model_root("pocket_tts"),
            },
            "piper": {
                "model": _text(settings.get("VOICE_TTS_MODEL")) or DEFAULT_PIPER_MODEL,
                "model_root": _tts_backend_model_root("piper"),
            },
        },
        "wyoming_tts": {
            "host": _text(settings.get("VOICE_WYOMING_TTS_HOST")) or DEFAULT_WYOMING_TTS_HOST,
            "port": _get_int_setting("VOICE_WYOMING_TTS_PORT", DEFAULT_WYOMING_TTS_PORT, minimum=1, maximum=65535),
            "voice": _text(settings.get("VOICE_WYOMING_TTS_VOICE")) or DEFAULT_WYOMING_TTS_VOICE,
        },
        "native_satellite": {
            "transport": "websocket",
            "legacy_satellite_api": False,
        },
        "discovery": {
            "enabled": _get_bool_setting("VOICE_DISCOVERY_ENABLED", DEFAULT_DISCOVERY_ENABLED),
            "scan_seconds": _get_int_setting("VOICE_DISCOVERY_SCAN_SECONDS", DEFAULT_DISCOVERY_SCAN_SECONDS, minimum=5, maximum=600),
            "mdns_timeout_s": _get_float_setting("VOICE_DISCOVERY_MDNS_TIMEOUT_S", DEFAULT_DISCOVERY_MDNS_TIMEOUT_S, minimum=0.5, maximum=20.0),
        },
        "eou": {
            "mode": DEFAULT_EOU_MODE,
            "input_gain": _get_float_setting("VOICE_INPUT_GAIN", DEFAULT_AUDIO_INPUT_GAIN, minimum=0.5, maximum=16.0),
            "noise_suppression_enabled": _get_bool_setting("VOICE_NOISE_SUPPRESSION_ENABLED", DEFAULT_NOISE_SUPPRESSION_ENABLED),
            "noise_suppression_high_pass_enabled": _get_bool_setting("VOICE_NOISE_SUPPRESSION_HIGH_PASS_ENABLED", DEFAULT_NOISE_SUPPRESSION_HIGH_PASS_ENABLED),
            "noise_suppression_tail_silence_s": _get_float_setting(
                "VOICE_NOISE_SUPPRESSION_TAIL_SILENCE_S",
                DEFAULT_NOISE_SUPPRESSION_TAIL_SILENCE_S,
                minimum=0.0,
                maximum=2.0,
            ),
            "noise_suppression_silence_attenuation": _get_float_setting(
                "VOICE_NOISE_SUPPRESSION_SILENCE_ATTENUATION",
                DEFAULT_NOISE_SUPPRESSION_SILENCE_ATTENUATION,
                minimum=0.0,
                maximum=1.0,
            ),
            "backend": vad_backend,
            "silence_s": _get_float_setting("VOICE_VAD_SILENCE_SECONDS", DEFAULT_VAD_SILENCE_SECONDS, minimum=0.2, maximum=5.0),
            "timeout_s": _get_float_setting("VOICE_VAD_TIMEOUT_SECONDS", DEFAULT_VAD_TIMEOUT_SECONDS, minimum=2.0, maximum=60.0),
            "startup_gate_s": _get_float_setting("VOICE_STARTUP_GATE_S", DEFAULT_STARTUP_GATE_S, minimum=0.0, maximum=2.0),
            "wake_vad_startup_ignore_s": _get_float_setting("VOICE_WAKE_VAD_STARTUP_IGNORE_S", DEFAULT_WAKE_VAD_STARTUP_IGNORE_S, minimum=0.0, maximum=2.0),
            "no_speech_timeout_s": _get_float_setting("VOICE_VAD_NO_SPEECH_TIMEOUT_S", DEFAULT_VAD_NO_SPEECH_TIMEOUT_S, minimum=1.0, maximum=20.0),
            "silero_threshold": _get_float_setting("VOICE_SILERO_THRESHOLD", DEFAULT_SILERO_THRESHOLD, minimum=0.01, maximum=0.99),
            "silero_neg_threshold": _get_float_setting("VOICE_SILERO_NEG_THRESHOLD", DEFAULT_SILERO_NEG_THRESHOLD, minimum=0.0, maximum=0.99),
            "min_speech_frames": _get_int_setting("VOICE_SILERO_MIN_SPEECH_FRAMES", DEFAULT_SILERO_MIN_SPEECH_FRAMES, minimum=1, maximum=30),
            "min_silence_frames": _get_int_setting("VOICE_SILERO_MIN_SILENCE_FRAMES", DEFAULT_SILERO_MIN_SILENCE_FRAMES, minimum=3, maximum=60),
            "min_silence_short_s": _get_float_setting("VOICE_VAD_MIN_SILENCE_SHORT_S", DEFAULT_VAD_MIN_SILENCE_SHORT_S, minimum=0.1, maximum=5.0),
            "min_silence_long_s": _get_float_setting("VOICE_VAD_MIN_SILENCE_LONG_S", DEFAULT_VAD_MIN_SILENCE_LONG_S, minimum=0.1, maximum=5.0),
            "continued_chat_reopen_startup_gate_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S", DEFAULT_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S, minimum=0.0, maximum=2.0),
            "continued_chat_reopen_vad_startup_ignore_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_VAD_STARTUP_IGNORE_S", DEFAULT_CONTINUED_CHAT_REOPEN_VAD_STARTUP_IGNORE_S, minimum=0.0, maximum=2.0),
            "continued_chat_reopen_preroll_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_PREROLL_S", DEFAULT_CONTINUED_CHAT_REOPEN_PREROLL_S, minimum=0.0, maximum=1.0),
            "continued_chat_reopen_silence_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS", DEFAULT_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS, minimum=0.2, maximum=5.0),
            "continued_chat_reopen_timeout_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS", DEFAULT_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS, minimum=2.0, maximum=60.0),
            "continued_chat_reopen_no_speech_timeout_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S", DEFAULT_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S, minimum=1.0, maximum=20.0),
            "continued_chat_reopen_min_silence_frames": _get_int_setting("VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES", DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES, minimum=3, maximum=60),
            "continued_chat_reopen_min_silence_short_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S", DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S, minimum=0.1, maximum=5.0),
            "continued_chat_reopen_min_silence_long_s": _get_float_setting("VOICE_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S", DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S, minimum=0.1, maximum=5.0),
            "audio_stall_timeout_s": _get_float_setting("VOICE_AUDIO_STALL_TIMEOUT_S", DEFAULT_AUDIO_STALL_TIMEOUT_S, minimum=0.4, maximum=20.0),
            "audio_stall_no_speech_timeout_s": _get_float_setting("VOICE_AUDIO_STALL_NO_SPEECH_TIMEOUT_S", DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S, minimum=1.0, maximum=30.0),
            "blank_wake_timeout_s": _get_float_setting("VOICE_BLANK_WAKE_TIMEOUT_S", DEFAULT_BLANK_WAKE_TIMEOUT_S, minimum=1.0, maximum=20.0),
            "webrtc_aggressiveness": _get_int_setting(
                "VOICE_WEBRTC_VAD_AGGRESSIVENESS",
                DEFAULT_WEBRTC_VAD_AGGRESSIVENESS,
                minimum=0,
                maximum=3,
            ),
            "webrtc_frame_ms": int(DEFAULT_WEBRTC_VAD_FRAME_MS),
        },
        "limits": {
            "max_audio_bytes": _get_int_setting("VOICE_NATIVE_MAX_AUDIO_BYTES", DEFAULT_MAX_AUDIO_BYTES, minimum=4096, maximum=16 * 1024 * 1024),
            "tts_url_ttl_s": _get_float_setting("VOICE_NATIVE_TTS_URL_TTL_S", DEFAULT_TTS_URL_TTL_S, minimum=30.0, maximum=900.0),
            "session_ttl_s": _get_int_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS, minimum=300, maximum=24 * 60 * 60),
            "history_store": _get_int_setting("MAX_STORE", DEFAULT_HISTORY_MAX_STORE, minimum=4, maximum=200),
            "history_llm": _get_int_setting("MAX_LLM", DEFAULT_HISTORY_MAX_LLM, minimum=2, maximum=80),
        },
    }


# -------------------- Voice Catalog Helpers --------------------
def _load_wyoming_tts_voice_catalog() -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    global _wyoming_tts_voice_catalog_mem, _wyoming_tts_voice_catalog_meta_mem
    try:
        rows_raw = redis_client.get(REDIS_WYOMING_TTS_VOICES_KEY)
        meta_raw = redis_client.get(REDIS_WYOMING_TTS_VOICES_META_KEY)
        rows = json.loads(rows_raw) if rows_raw else []
        meta = json.loads(meta_raw) if meta_raw else {}
        if isinstance(rows, list):
            _wyoming_tts_voice_catalog_mem = [r for r in rows if isinstance(r, dict)]
        if isinstance(meta, dict):
            _wyoming_tts_voice_catalog_meta_mem = dict(meta)
    except Exception:
        pass
    return list(_wyoming_tts_voice_catalog_mem), dict(_wyoming_tts_voice_catalog_meta_mem)


def _save_wyoming_tts_voice_catalog(rows: List[Dict[str, str]], *, host: str, port: int, error: str = "") -> None:
    global _wyoming_tts_voice_catalog_mem, _wyoming_tts_voice_catalog_meta_mem
    clean: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        value = _text((row or {}).get("value"))
        label = _text((row or {}).get("label"))
        if not value or value in seen:
            continue
        seen.add(value)
        clean.append({"value": value, "label": label or value})
    meta = {
        "updated_ts": _now(),
        "host": _text(host),
        "port": int(port or 0),
        "error": _text(error),
    }
    _wyoming_tts_voice_catalog_mem = list(clean)
    _wyoming_tts_voice_catalog_meta_mem = dict(meta)
    with contextlib.suppress(Exception):
        redis_client.set(REDIS_WYOMING_TTS_VOICES_KEY, json.dumps(clean, ensure_ascii=False))
        redis_client.set(REDIS_WYOMING_TTS_VOICES_META_KEY, json.dumps(meta, ensure_ascii=False))


def _wyoming_tts_voice_option_rows(*, current_value: Any) -> List[Dict[str, str]]:
    rows, _meta = _load_wyoming_tts_voice_catalog()
    options = [{"value": "", "label": "Default"}]
    options.extend(sorted(rows, key=lambda r: _lower(r.get("label"))))
    current = _text(current_value)
    if current and current not in {row.get("value") for row in options}:
        options.append({"value": current, "label": f"{current} (saved)"})
    return options


def _load_piper_tts_model_catalog() -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    global _piper_tts_model_catalog_mem, _piper_tts_model_catalog_meta_mem
    try:
        rows_raw = redis_client.get(REDIS_PIPER_TTS_MODELS_KEY)
        meta_raw = redis_client.get(REDIS_PIPER_TTS_MODELS_META_KEY)
        rows = json.loads(rows_raw) if rows_raw else []
        meta = json.loads(meta_raw) if meta_raw else {}
        if isinstance(rows, list):
            _piper_tts_model_catalog_mem = [r for r in rows if isinstance(r, dict)]
        if isinstance(meta, dict):
            _piper_tts_model_catalog_meta_mem = dict(meta)
    except Exception:
        pass
    return list(_piper_tts_model_catalog_mem), dict(_piper_tts_model_catalog_meta_mem)


def _save_piper_tts_model_catalog(rows: List[Dict[str, str]], *, source: str, error: str = "") -> None:
    global _piper_tts_model_catalog_mem, _piper_tts_model_catalog_meta_mem
    clean: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        value = _text((row or {}).get("value"))
        label = _text((row or {}).get("label")) or value
        if not value or value in seen:
            continue
        seen.add(value)
        clean.append({"value": value, "label": label})
    meta = {
        "updated_ts": _now(),
        "source": _text(source),
        "error": _text(error),
    }
    _piper_tts_model_catalog_mem = list(clean)
    _piper_tts_model_catalog_meta_mem = dict(meta)
    with contextlib.suppress(Exception):
        redis_client.set(REDIS_PIPER_TTS_MODELS_KEY, json.dumps(clean, ensure_ascii=False))
        redis_client.set(REDIS_PIPER_TTS_MODELS_META_KEY, json.dumps(meta, ensure_ascii=False))


def _refresh_piper_tts_model_catalog(force: bool = False) -> Dict[str, Any]:
    rows, meta = _load_piper_tts_model_catalog()
    if rows and not force:
        return {"models": rows, "meta": meta, "count": len(rows)}

    if not PIPER_VOICES_CATALOG_URL:
        raise RuntimeError(f"piper dependency unavailable: {PIPER_IMPORT_ERROR or 'unknown import error'}")

    catalog_rows: List[Dict[str, str]] = []
    with urllib_request.urlopen(PIPER_VOICES_CATALOG_URL, timeout=20) as response:
        payload = json.load(response)
    if isinstance(payload, dict):
        for voice_code in sorted(payload.keys()):
            value = _text(voice_code)
            if value:
                catalog_rows.append({"value": value, "label": value})

    _save_piper_tts_model_catalog(catalog_rows, source=PIPER_VOICES_CATALOG_URL, error="")
    return {"models": catalog_rows, "meta": dict(_piper_tts_model_catalog_meta_mem), "count": len(catalog_rows)}


def _piper_tts_model_option_rows(*, current_value: Any, ensure_catalog: bool = False) -> List[Dict[str, str]]:
    rows, _meta = _load_piper_tts_model_catalog()
    if ensure_catalog and not rows:
        with contextlib.suppress(Exception):
            rows = list((_refresh_piper_tts_model_catalog(force=False) or {}).get("models") or [])
    options = sorted(rows, key=lambda r: _lower(r.get("label")))
    current = _text(current_value) or DEFAULT_PIPER_MODEL
    if current and current not in {row.get("value") for row in options}:
        options.insert(0, {"value": current, "label": current})
    return options


def _voice_selection_from_string(raw: Any) -> Dict[str, str]:
    token = _text(raw)
    if not token:
        return {}
    with contextlib.suppress(Exception):
        parsed = json.loads(token)
        if isinstance(parsed, dict):
            out = {
                "name": _text(parsed.get("name")),
                "language": _text(parsed.get("language")),
                "speaker": _text(parsed.get("speaker")),
            }
            return {k: v for k, v in out.items() if v}
    # backward-compat plain voice name
    return {"name": token}


def _voice_selection_to_value(selection: Dict[str, Any]) -> str:
    payload = {
        "name": _text(selection.get("name")),
        "language": _text(selection.get("language")),
        "speaker": _text(selection.get("speaker")),
    }
    clean = {k: v for k, v in payload.items() if v}
    if not clean:
        return ""
    return json.dumps(clean, separators=(",", ":"), sort_keys=True)


def _voice_selection_label(selection: Dict[str, Any]) -> str:
    name = _text(selection.get("name"))
    language = _text(selection.get("language"))
    speaker = _text(selection.get("speaker"))
    parts = [part for part in [name, language, speaker] if part]
    return " / ".join(parts) if parts else "Default"


# -------------------- Audio + VAD --------------------
def _pcm_dbfs(audio_bytes: bytes, *, sample_width: int) -> Optional[float]:
    data = bytes(audio_bytes or b"")
    width = int(sample_width or DEFAULT_VOICE_SAMPLE_WIDTH)
    if not data or width < 1 or width > 4:
        return None

    frame_size = max(1, width)
    usable = len(data) - (len(data) % frame_size)
    if usable <= 0:
        return None
    if usable != len(data):
        data = data[:usable]

    with contextlib.suppress(Exception):
        rms = float(_pcm_rms(data, width))
        if rms <= 0.0:
            return -120.0
        full_scale = float((1 << ((8 * width) - 1)) - 1)
        if full_scale <= 0.0:
            return None
        normalized = min(1.0, max(rms / full_scale, 1e-9))
        return 20.0 * math.log10(normalized)
    return None


def _pcm_apply_gain(audio_bytes: bytes, *, sample_width: int, gain: float) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data:
        return b""
    factor = float(gain or 1.0)
    if abs(factor - 1.0) < 0.0001:
        return data
    if factor <= 0.0:
        return b"\x00" * len(data)
    width = int(sample_width or DEFAULT_VOICE_SAMPLE_WIDTH)
    if _audioop is not None:
        with contextlib.suppress(Exception):
            return _audioop.mul(data, width, factor)
    if width != 2:
        return data
    usable = len(data) - (len(data) % 2)
    if usable <= 0:
        return data
    out = bytearray(usable)
    src = memoryview(data[:usable]).cast("h")
    dst = memoryview(out).cast("h")
    for idx, sample in enumerate(src):
        scaled = int(round(float(sample) * factor))
        if scaled > 32767:
            scaled = 32767
        elif scaled < -32768:
            scaled = -32768
        dst[idx] = scaled
    if usable < len(data):
        out.extend(data[usable:])
    return bytes(out)


def _pcm_high_pass_filter_session(session: "VoiceSessionRuntime", audio_bytes: bytes, *, sample_width: int) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data or int(sample_width or DEFAULT_VOICE_SAMPLE_WIDTH) != 2:
        return data
    usable = len(data) - (len(data) % 2)
    if usable <= 0:
        return data

    alpha = _get_float_setting(
        "VOICE_NOISE_SUPPRESSION_HIGH_PASS_ALPHA",
        DEFAULT_NOISE_SUPPRESSION_HIGH_PASS_ALPHA,
        minimum=0.90,
        maximum=0.9999,
    )
    out = bytearray(usable)
    src = memoryview(data[:usable]).cast("h")
    dst = memoryview(out).cast("h")
    prev_input = float(getattr(session, "noise_highpass_prev_input", 0.0) or 0.0)
    prev_output = float(getattr(session, "noise_highpass_prev_output", 0.0) or 0.0)
    for idx, sample in enumerate(src):
        current = float(sample)
        filtered = current - prev_input + (float(alpha) * prev_output)
        prev_input = current
        prev_output = filtered
        if filtered > 32767.0:
            dst[idx] = 32767
        elif filtered < -32768.0:
            dst[idx] = -32768
        else:
            dst[idx] = int(round(filtered))
    session.noise_highpass_prev_input = prev_input
    session.noise_highpass_prev_output = prev_output
    if usable < len(data):
        out.extend(data[usable:])
    return bytes(out)


def _apply_stt_noise_suppression(
    session: "VoiceSessionRuntime",
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    metrics: Dict[str, Any],
) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data or not _get_bool_setting("VOICE_NOISE_SUPPRESSION_ENABLED", DEFAULT_NOISE_SUPPRESSION_ENABLED):
        return data

    width = _as_int((audio_format or {}).get("width"), DEFAULT_VOICE_SAMPLE_WIDTH, minimum=1, maximum=4)
    processed = data
    if _get_bool_setting("VOICE_NOISE_SUPPRESSION_HIGH_PASS_ENABLED", DEFAULT_NOISE_SUPPRESSION_HIGH_PASS_ENABLED):
        processed = _pcm_high_pass_filter_session(session, processed, sample_width=width)

    silence_s = max(0.0, float((metrics or {}).get("silence_s") or 0.0))
    tail_s = _get_float_setting(
        "VOICE_NOISE_SUPPRESSION_TAIL_SILENCE_S",
        DEFAULT_NOISE_SUPPRESSION_TAIL_SILENCE_S,
        minimum=0.0,
        maximum=2.0,
    )
    should_attenuate = bool((metrics or {}).get("voice_seen")) and silence_s >= float(tail_s)
    if should_attenuate:
        attenuation = _get_float_setting(
            "VOICE_NOISE_SUPPRESSION_SILENCE_ATTENUATION",
            DEFAULT_NOISE_SUPPRESSION_SILENCE_ATTENUATION,
            minimum=0.0,
            maximum=1.0,
        )
        processed = _pcm_apply_gain(processed, sample_width=width, gain=attenuation)
        session.noise_suppression_chunks += 1
        now_ts = _now()
        if session.noise_suppression_chunks == 1 or now_ts - float(session.noise_suppression_last_log_ts or 0.0) >= 5.0:
            session.noise_suppression_last_log_ts = now_ts
            _native_debug(
                f"noise suppression tail attenuate selector={session.selector} session_id={session.session_id} "
                f"silence_s={silence_s:.2f} attenuation={float(attenuation):.2f}"
            )

    return processed


def _audio_format_from_settings(audio_settings: Any) -> Dict[str, int]:
    source = audio_settings if audio_settings is not None else {}

    def _read_int(candidates: List[str], default: int) -> int:
        for key in candidates:
            raw = source.get(key) if isinstance(source, dict) else getattr(source, key, None)
            with contextlib.suppress(Exception):
                value = int(raw)
                if value > 0:
                    return value
        return int(default)

    return {
        "rate": _read_int(["rate", "sample_rate", "sample_rate_hz"], DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "width": _read_int(["width", "sample_width", "sample_width_bytes"], DEFAULT_VOICE_SAMPLE_WIDTH),
        "channels": _read_int(["channels", "num_channels"], DEFAULT_VOICE_CHANNELS),
    }


def _audio_bytes_for_seconds(audio_format: Dict[str, Any], seconds: float) -> int:
    rate = _as_int((audio_format or {}).get("rate"), DEFAULT_VOICE_SAMPLE_RATE_HZ, minimum=1)
    width = _as_int((audio_format or {}).get("width"), DEFAULT_VOICE_SAMPLE_WIDTH, minimum=1, maximum=4)
    channels = _as_int((audio_format or {}).get("channels"), DEFAULT_VOICE_CHANNELS, minimum=1, maximum=8)
    frame_bytes = max(1, width * channels)
    frames = max(0, int(round(float(seconds or 0.0) * float(rate))))
    return int(frames * frame_bytes)


def _stt_audio_bytes_for_transcription(session: "VoiceSessionRuntime") -> bytes:
    if not isinstance(session, VoiceSessionRuntime):
        return b""
    data = bytes(session.audio_buffer or b"")
    if not data:
        return b""

    silence_s = max(0.0, float(getattr(session, "silence_duration_s", 0.0) or 0.0))
    keep_s = _get_float_setting(
        "VOICE_STT_TRAILING_SILENCE_KEEP_S",
        DEFAULT_STT_TRAILING_SILENCE_KEEP_S,
        minimum=0.1,
        maximum=2.0,
    )
    trim_s = max(0.0, silence_s - max(0.1, float(keep_s)))
    if trim_s <= 0.0:
        return data

    audio_format = dict(getattr(session, "audio_format", {}) or {})
    trim_bytes = _audio_bytes_for_seconds(audio_format, trim_s)
    frame_bytes = max(
        1,
        _as_int(audio_format.get("width"), DEFAULT_VOICE_SAMPLE_WIDTH, minimum=1, maximum=4)
        * _as_int(audio_format.get("channels"), DEFAULT_VOICE_CHANNELS, minimum=1, maximum=8),
    )
    trim_bytes -= trim_bytes % frame_bytes
    min_keep_bytes = _audio_bytes_for_seconds(audio_format, 0.45)
    if trim_bytes <= 0 or (len(data) - trim_bytes) < min_keep_bytes:
        return data

    trimmed = data[:-trim_bytes]
    _native_debug(
        f"STT trim trailing audio selector={session.selector} session_id={session.session_id} "
        f"silence_s={silence_s:.2f} keep_s={float(keep_s):.2f} "
        f"bytes={len(data)} trimmed={trim_bytes}"
    )
    return trimmed


def _sanitize_stt_transcript(transcript: str) -> str:
    text = re.sub(r"\s+", " ", _text(transcript)).strip()
    if not text:
        return ""

    cleaned = text
    for _ in range(3):
        next_cleaned = re.sub(
            r"(?i)(?P<prefix>.*?\b)(?P<word>[a-z0-9']+)(?:\s+(?P=word)\b){2,}(?P<punct>[.?!]*)$",
            lambda m: f"{m.group('prefix')}{m.group('word')}{m.group('punct') or ''}",
            cleaned,
        ).strip()
        if next_cleaned == cleaned:
            break
        cleaned = next_cleaned

    words = re.findall(r"[a-z0-9']+", cleaned.lower())
    if len(words) >= 8:
        for size in (3, 2):
            if len(words) < size * 3:
                continue
            tail = words[-size:]
            repeats = 1
            offset = len(words) - (size * 2)
            while offset >= 0 and words[offset:offset + size] == tail:
                repeats += 1
                offset -= size
            if repeats >= 3:
                pattern = r"(?i)(?:\b" + r"\s+".join(re.escape(w) for w in tail) + r"\b\s*){" + str(repeats) + r"}([.?!]*)$"
                cleaned = re.sub(pattern, " ".join(tail) + r"\1", cleaned).strip()
                break

    if cleaned != text:
        _native_debug(f"STT transcript repetition trimmed original={text!r} cleaned={cleaned!r}")
    return cleaned


def _pcm_to_pcm16_mono_16k(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    *,
    ratecv_state: Any = None,
) -> Tuple[bytes, Any]:
    data = bytes(audio_bytes or b"")
    if not data:
        return b"", ratecv_state

    rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)

    if width != 2:
        if _audioop is None:
            return b"", ratecv_state
        with contextlib.suppress(Exception):
            data = _audioop.lin2lin(data, width, 2)
            width = 2
        if width != 2:
            return b"", ratecv_state

    if channels > 1:
        if _audioop is None:
            return b"", ratecv_state
        with contextlib.suppress(Exception):
            data = _audioop.tomono(data, 2, 0.5, 0.5)
            channels = 1
        if channels != 1:
            return b"", ratecv_state

    if rate != 16000:
        if _audioop is None:
            return b"", ratecv_state
        with contextlib.suppress(Exception):
            data, ratecv_state = _audioop.ratecv(data, 2, 1, rate, 16000, ratecv_state)
            rate = 16000
        if rate != 16000:
            return b"", ratecv_state

    return data, ratecv_state


class VadBackendBase:
    def process(self, audio_bytes: bytes, audio_format: Dict[str, int]) -> Dict[str, Any]:
        raise NotImplementedError


class SileroVadBackend(VadBackendBase):
    """Use the Silero VAD model directly for per-frame speech probability.

    Instead of the VADIterator wrapper (which manages its own opaque state
    machine and ``triggered`` flag), we call the model directly to get a
    clean 0.0-1.0 probability for every 512-sample frame.  This gives the
    SegmenterState full control over speech-start / speech-end decisions.
    """

    _shared_ready: Optional[bool] = None
    _shared_error: str = ""
    _shared_torch: Any = None
    _shared_np: Any = None
    _shared_load_fn: Any = None
    _shared_lock = threading.RLock()
    _model_cache: Dict[str, Any] = {}
    _model_locks: Dict[str, threading.RLock] = {}

    @classmethod
    def _ensure_shared(cls) -> None:
        with cls._shared_lock:
            if cls._shared_ready is not None:
                return
            try:
                torch_mod = importlib.import_module("torch")
                np_mod = importlib.import_module("numpy")
                silero = importlib.import_module("silero_vad")
                load_fn = getattr(silero, "load_silero_vad", None)
                if not callable(load_fn):
                    raise RuntimeError("silero_vad missing load_silero_vad")
                cls._shared_torch = torch_mod
                cls._shared_np = np_mod
                cls._shared_load_fn = load_fn
                cls._shared_error = ""
                cls._shared_ready = True
            except Exception as exc:
                cls._shared_torch = None
                cls._shared_np = None
                cls._shared_load_fn = None
                cls._shared_error = str(exc)
                cls._shared_ready = False

    def __init__(self, cfg: Dict[str, Any], *, owner_key: str = ""):
        self._owner_key = _text(owner_key) or "default"
        self.threshold = float(cfg.get("silero_threshold") or DEFAULT_SILERO_THRESHOLD)
        self.neg_threshold = _as_float(
            cfg.get("silero_neg_threshold"),
            DEFAULT_SILERO_NEG_THRESHOLD,
            minimum=0.0,
            maximum=1.0,
        )
        self._model_lock = threading.RLock()
        self._available = False
        self._load_error = ""
        self._ratecv_state = None
        self._buffer = b""
        self._frame_samples = int(DEFAULT_SILERO_FRAME_SAMPLES)
        self._frame_bytes = int(self._frame_samples * 2)  # int16 mono
        try:
            self._ensure_shared()
            if not bool(self.__class__._shared_ready):
                raise RuntimeError(self.__class__._shared_error or "silero shared init failed")
            self._torch = self.__class__._shared_torch
            self._np = self.__class__._shared_np
            load_fn = self.__class__._shared_load_fn
            if not callable(load_fn):
                raise RuntimeError("silero_vad loader unavailable")
            with self.__class__._shared_lock:
                lock = self.__class__._model_locks.get(self._owner_key)
                if lock is None:
                    lock = threading.RLock()
                    self.__class__._model_locks[self._owner_key] = lock
                model = self.__class__._model_cache.get(self._owner_key)
                if model is None:
                    model = load_fn()
                    with contextlib.suppress(Exception):
                        model.eval()
                    self.__class__._model_cache[self._owner_key] = model
                self._model_lock = lock
                self._model = model
            with self._model_lock:
                with contextlib.suppress(Exception):
                    self._model.reset_states()
            self._available = True
        except Exception as exc:
            self._torch = None
            self._np = None
            self._model = None
            self._load_error = str(exc)
            self._available = False

    def reset_state(self) -> None:
        if self._available and self._model is not None:
            with self._model_lock:
                with contextlib.suppress(Exception):
                    self._model.reset_states()
        self._buffer = b""
        self._ratecv_state = None

    @classmethod
    def preload_owner(cls, cfg: Dict[str, Any], *, owner_key: str = "") -> bool:
        backend = cls(cfg, owner_key=owner_key)
        return bool(getattr(backend, "_available", False))

    def _to_pcm16_mono_16k(self, audio_bytes: bytes, audio_format: Dict[str, int]) -> bytes:
        data, self._ratecv_state = _pcm_to_pcm16_mono_16k(
            audio_bytes,
            audio_format,
            ratecv_state=self._ratecv_state,
        )
        return data

    def process(self, audio_bytes: bytes, audio_format: Dict[str, int]) -> Dict[str, Any]:
        if not self._available:
            return {
                "backend": "silero",
                "probability": 0.0,
                "is_speech": False,
                "frames": 0,
                "error": self._load_error or "silero unavailable",
            }

        pcm16 = self._to_pcm16_mono_16k(audio_bytes, audio_format)
        if not pcm16:
            return {"backend": "silero", "probability": 0.0, "is_speech": False, "frames": 0}

        try:
            with self._model_lock:
                payload = self._buffer + pcm16
                if len(payload) < self._frame_bytes:
                    self._buffer = payload
                    return {"backend": "silero", "probability": 0.0, "is_speech": False, "frames": 0}

                offset = 0
                total_frames = 0
                prob_sum = 0.0
                max_prob = 0.0
                no_grad = getattr(self._torch, "no_grad", None)
                grad_ctx = no_grad() if callable(no_grad) else contextlib.nullcontext()
                with grad_ctx:
                    while (offset + self._frame_bytes) <= len(payload):
                        frame = payload[offset: offset + self._frame_bytes]
                        offset += self._frame_bytes
                        total_frames += 1
                        samples = self._np.frombuffer(frame, dtype=self._np.int16).astype(self._np.float32) / 32768.0
                        tensor = self._torch.from_numpy(samples)
                        prob = float(self._model(tensor, 16000).item())
                        prob_sum += prob
                        if prob > max_prob:
                            max_prob = prob
                self._buffer = payload[offset:]

            avg_prob = prob_sum / total_frames if total_frames > 0 else 0.0
            is_speech = avg_prob >= self.threshold

            return {
                "backend": "silero",
                "probability": round(avg_prob, 4),
                "max_probability": round(max_prob, 4),
                "is_speech": is_speech,
                "frames": total_frames,
            }
        except Exception as exc:
            return {
                "backend": "silero",
                "probability": 0.0,
                "is_speech": False,
                "frames": 0,
                "error": str(exc),
            }


class WebRtcVadBackend(VadBackendBase):
    """Tiny WebRTC VAD backend for low-power hosts.

    WebRTC VAD is binary, so we expose a 1.0/0.0 score and let the existing
    segmenter smooth speech start/end with its frame counters and silence timers.
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.mode = _as_int(
            cfg.get("webrtc_aggressiveness"),
            DEFAULT_WEBRTC_VAD_AGGRESSIVENESS,
            minimum=0,
            maximum=3,
        )
        frame_ms = _as_int(cfg.get("webrtc_frame_ms"), DEFAULT_WEBRTC_VAD_FRAME_MS, minimum=10, maximum=30)
        self.frame_ms = frame_ms if frame_ms in {10, 20, 30} else DEFAULT_WEBRTC_VAD_FRAME_MS
        self._available = False
        self._load_error = ""
        self._ratecv_state = None
        self._buffer = b""
        self._frame_samples = int(16000 * self.frame_ms / 1000)
        self._frame_bytes = int(self._frame_samples * 2)
        try:
            webrtcvad_mod = importlib.import_module("webrtcvad")
            self._vad = webrtcvad_mod.Vad(int(self.mode))
            self._available = True
        except Exception as exc:
            self._vad = None
            self._load_error = str(exc)
            self._available = False

    def reset_state(self) -> None:
        self._buffer = b""
        self._ratecv_state = None

    def _to_pcm16_mono_16k(self, audio_bytes: bytes, audio_format: Dict[str, int]) -> bytes:
        data, self._ratecv_state = _pcm_to_pcm16_mono_16k(
            audio_bytes,
            audio_format,
            ratecv_state=self._ratecv_state,
        )
        return data

    def process(self, audio_bytes: bytes, audio_format: Dict[str, int]) -> Dict[str, Any]:
        if not self._available or self._vad is None:
            return {
                "backend": "webrtc",
                "probability": 0.0,
                "is_speech": False,
                "frames": 0,
                "error": self._load_error or "webrtcvad unavailable",
            }

        pcm16 = self._to_pcm16_mono_16k(audio_bytes, audio_format)
        if not pcm16:
            return {"backend": "webrtc", "probability": 0.0, "is_speech": False, "frames": 0}

        try:
            payload = self._buffer + pcm16
            if len(payload) < self._frame_bytes:
                self._buffer = payload
                return {"backend": "webrtc", "probability": 0.0, "is_speech": False, "frames": 0}

            offset = 0
            total_frames = 0
            speech_frames = 0
            while (offset + self._frame_bytes) <= len(payload):
                frame = payload[offset: offset + self._frame_bytes]
                offset += self._frame_bytes
                total_frames += 1
                if bool(self._vad.is_speech(frame, 16000)):
                    speech_frames += 1
            self._buffer = payload[offset:]

            is_speech = speech_frames > 0
            ratio = float(speech_frames) / float(total_frames) if total_frames > 0 else 0.0
            probability = 1.0 if is_speech else 0.0
            return {
                "backend": "webrtc",
                "probability": probability,
                "max_probability": probability,
                "speech_ratio": round(ratio, 4),
                "is_speech": is_speech,
                "frames": total_frames,
                "speech_frames": speech_frames,
                "mode": int(self.mode),
            }
        except Exception as exc:
            return {
                "backend": "webrtc",
                "probability": 0.0,
                "is_speech": False,
                "frames": 0,
                "error": str(exc),
            }


# Segmenter modelled after Home Assistant's pipeline VAD.
#
# Two paths to finalization (whichever fires first):
#   Path A (speech detected):
#     WAITING -> speech frames >= min_speech_frames -> IN_SPEECH
#     IN_SPEECH -> silence frames >= min_silence_frames -> DONE
#   Path B (no speech detected):
#     WAITING -> elapsed >= no_speech_timeout_s -> DONE
#     (STT still received all audio, so it can transcribe whatever was said)
#
# A hard ``timeout_s`` caps total listening duration as a safety net.
@dataclass
class SegmenterState:
    silence_s: float
    timeout_s: float
    no_speech_timeout_s: float
    threshold: float
    neg_threshold: float
    min_speech_frames: int
    min_silence_frames: int
    min_silence_short_s: float = DEFAULT_VAD_MIN_SILENCE_SHORT_S
    min_silence_long_s: float = DEFAULT_VAD_MIN_SILENCE_LONG_S

    # running counters
    speech_chunks: int = 0
    speech_seconds_total: float = 0.0
    voice_seen: bool = False
    in_command: bool = False
    timed_out: bool = False

    _consecutive_speech: int = 0
    _consecutive_silence: int = 0
    _soft_silence_s: float = 0.0
    _strong_speech_streak: int = 0
    _total_chunks: int = 0
    _elapsed_s: float = 0.0
    _finalized: bool = False
    _last_process_ts: float = 0.0

    def __post_init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._consecutive_speech = 0
        self._consecutive_silence = 0
        self._soft_silence_s = 0.0
        self._strong_speech_streak = 0
        self._total_chunks = 0
        self._elapsed_s = 0.0
        self._finalized = False
        self._last_process_ts = 0.0
        self.in_command = False
        self.timed_out = False
        self.voice_seen = False
        self.speech_chunks = 0
        self.speech_seconds_total = 0.0

    def _speech_seconds(self) -> float:
        return max(0.0, float(self.speech_seconds_total))

    def process(
        self,
        chunk_seconds: float,
        speech_probability: Optional[float],
        now_ts: float,
        *,
        peak_probability: Optional[float] = None,
    ) -> Dict[str, Any]:
        should_finalize = False
        silence_elapsed = 0.0

        if self._finalized:
            return {
                "should_finalize": True,
                "voice_seen": self.voice_seen,
                "speech_chunks": self.speech_chunks,
                "speech_s": self._speech_seconds(),
                "silence_s": 0.0,
                "timed_out": self.timed_out,
                "in_command": self.in_command,
            }

        chunk_seconds = max(0.001, float(chunk_seconds or 0.0))
        if self._last_process_ts > 0.0:
            wall_delta = max(0.0, float(now_ts) - float(self._last_process_ts))
            if wall_delta > 0.0:
                # Satellite chunk cadence can be sparse; use wall clock so endpointing
                # remains responsive in real time.
                chunk_seconds = max(chunk_seconds, min(wall_delta, 0.5))
        self._last_process_ts = float(now_ts)
        self._elapsed_s += chunk_seconds
        self._total_chunks += 1
        probability = min(1.0, max(0.0, float(speech_probability or 0.0)))
        peak = min(1.0, max(0.0, float(peak_probability if peak_probability is not None else probability)))
        start_probability = max(probability, peak)
        start_threshold = min(self.threshold, max(self.neg_threshold, self.threshold * 0.75))

        # Hard timeout safety net
        if self._elapsed_s >= self.timeout_s:
            self.timed_out = True
            self._finalized = True
            should_finalize = True
        elif not self.in_command:
            # WAITING_FOR_SPEECH
            if start_probability >= start_threshold:
                self._consecutive_speech += 1
                self._consecutive_silence = 0
                self._soft_silence_s = 0.0
                self._strong_speech_streak = self._consecutive_speech if start_probability >= self.threshold else 0
                if self._consecutive_speech >= self.min_speech_frames:
                    self.in_command = True
                    self.voice_seen = True
                    self.speech_chunks += self._consecutive_speech
                    self.speech_seconds_total += chunk_seconds * self._consecutive_speech
                    self._consecutive_silence = 0
                    self._soft_silence_s = 0.0
                    self._strong_speech_streak = (
                        self._consecutive_speech if start_probability >= self.threshold else 0
                    )
            else:
                self._consecutive_speech = 0
                self._soft_silence_s = 0.0
                self._strong_speech_streak = 0
                # No-speech timeout: if we've been waiting for speech and
                # haven't detected any, finalize so STT can process whatever
                # audio it received.  This handles the case where the user
                # speaks their command immediately after the wake word (before
                # the session even starts streaming).
                if self._elapsed_s >= self.no_speech_timeout_s:
                    self._finalized = True
                    should_finalize = True
        else:
            # IN_SPEECH: accumulate speech, look for silence to finalize.
            # We use a "soft silence" timer so mid-confidence noise (between
            # neg_threshold and threshold) doesn't keep the mic open forever.
            if probability >= self.neg_threshold:
                if probability >= self.threshold:
                    self.speech_chunks += 1
                    self.speech_seconds_total += chunk_seconds
                    self._strong_speech_streak += 1
                    if self._strong_speech_streak >= self.min_speech_frames:
                        self._soft_silence_s = 0.0
                else:
                    self._strong_speech_streak = 0
                    self._soft_silence_s += chunk_seconds
                self._consecutive_silence = 0
                silence_elapsed = float(self._soft_silence_s)
            else:
                self._strong_speech_streak = 0
                self._consecutive_silence += 1
                self._soft_silence_s += chunk_seconds
                silence_elapsed = max(float(self._soft_silence_s), float(self._consecutive_silence) * chunk_seconds)
                # Require a small amount of real elapsed silence before the
                # frame-count rule can end a turn. This avoids clipping on
                # sparse chunk cadence where 3 silent chunks can be only ~0.15s.
                min_silence_elapsed = min(
                    float(self.silence_s),
                    float(self.min_silence_long_s)
                    if self.speech_seconds_total >= 1.0
                    else float(self.min_silence_short_s),
                )
                if (
                    (
                        self._consecutive_silence >= self.min_silence_frames
                        and self._soft_silence_s >= min_silence_elapsed
                    )
                    or self._soft_silence_s >= float(self.silence_s)
                ):
                    should_finalize = True
                    self._finalized = True

        return {
            "should_finalize": should_finalize,
            "voice_seen": self.voice_seen,
            "speech_chunks": self.speech_chunks,
            "speech_s": self._speech_seconds(),
            "silence_s": silence_elapsed,
            "timed_out": bool(self.timed_out),
            "in_command": bool(self.in_command),
        }


@dataclass
class EouEngine:
    mode: str
    backend_name: str
    backend: VadBackendBase
    segmenter: SegmenterState

    def process(self, audio_bytes: bytes, audio_format: Dict[str, int], now_ts: float) -> Dict[str, Any]:
        backend_data = self.backend.process(audio_bytes, audio_format)
        rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
        width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
        channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)
        bytes_per_second = max(1, rate * width * channels)
        chunk_seconds = float(len(audio_bytes or b"")) / float(bytes_per_second)

        backend_error = _text(backend_data.get("error"))
        if backend_error:
            return {
                "backend": self.backend_name,
                "binary_active": False,
                "score": 0.0,
                "chunk_score": 0.0,
                "should_finalize": True,
                "vad_error": True,
                "voice_seen": False,
                "speech_chunks": 0,
                "speech_s": 0.0,
                "silence_s": 0.0,
                "timed_out": False,
                "in_command": False,
                **backend_data,
            }

        probability = float(backend_data.get("probability", 0.0))
        peak_probability = float(backend_data.get("max_probability", probability))
        seg = self.segmenter.process(
            chunk_seconds,
            probability,
            now_ts,
            peak_probability=peak_probability,
        )
        is_speech = bool(seg.get("in_command", False))
        merged = {
            "backend": self.backend_name,
            "binary_active": bool(is_speech),
            "score": probability,
            "chunk_score": probability,
            **backend_data,
            **seg,
        }
        return merged

    def reset(self) -> None:
        self.segmenter.reset()
        if hasattr(self.backend, "reset_state"):
            self.backend.reset_state()


@dataclass
class AwaitingAnnouncementState:
    session_id: str
    kind: str
    future: Optional[asyncio.Future[Any]]
    started_ts: float
    timeout_s: float


def _build_eou_engine(
    audio_format: Dict[str, int],
    *,
    selector: str = "",
    reopen_capture_profile: bool = False,
    cfg: Optional[Dict[str, Any]] = None,
) -> EouEngine:
    cfg_row = cfg if isinstance(cfg, dict) else _voice_config_snapshot()
    eou = cfg_row.get("eou") if isinstance(cfg_row.get("eou"), dict) else {}
    selected_backend = _normalize_vad_backend(eou.get("backend"))
    if selected_backend == "auto":
        selected_backend = DEFAULT_VAD_BACKEND
    backend_name = selected_backend
    mode = DEFAULT_EOU_MODE

    threshold = _as_float(eou.get("silero_threshold"), DEFAULT_SILERO_THRESHOLD, minimum=0.01, maximum=0.99)
    neg_threshold = _as_float(eou.get("silero_neg_threshold"), DEFAULT_SILERO_NEG_THRESHOLD, minimum=0.0, maximum=0.99)

    backend: VadBackendBase
    if selected_backend == "silero":
        backend = SileroVadBackend(eou, owner_key=selector)
        backend_name = "silero"
    else:
        backend = WebRtcVadBackend(eou)
        backend_name = "webrtc"
    if not bool(getattr(backend, "_available", True)):
        logger.error(
            "[native-voice] selected VAD backend unavailable backend=%s error=%s",
            backend_name,
            _text(getattr(backend, "_load_error", "")) or "unknown",
        )

    segmenter = SegmenterState(
        silence_s=_as_float(eou.get("silence_s"), DEFAULT_VAD_SILENCE_SECONDS, minimum=0.2, maximum=5.0),
        timeout_s=_as_float(eou.get("timeout_s"), DEFAULT_VAD_TIMEOUT_SECONDS, minimum=2.0, maximum=60.0),
        no_speech_timeout_s=_as_float(eou.get("no_speech_timeout_s"), DEFAULT_VAD_NO_SPEECH_TIMEOUT_S, minimum=1.0, maximum=20.0),
        threshold=threshold,
        neg_threshold=neg_threshold,
        min_speech_frames=_as_int(eou.get("min_speech_frames"), DEFAULT_SILERO_MIN_SPEECH_FRAMES, minimum=1, maximum=30),
        min_silence_frames=_as_int(eou.get("min_silence_frames"), DEFAULT_SILERO_MIN_SILENCE_FRAMES, minimum=3, maximum=60),
        min_silence_short_s=_as_float(eou.get("min_silence_short_s"), DEFAULT_VAD_MIN_SILENCE_SHORT_S, minimum=0.1, maximum=5.0),
        min_silence_long_s=_as_float(eou.get("min_silence_long_s"), DEFAULT_VAD_MIN_SILENCE_LONG_S, minimum=0.1, maximum=5.0),
    )
    if reopen_capture_profile:
        segmenter.silence_s = max(
            float(segmenter.silence_s),
            _as_float(eou.get("continued_chat_reopen_silence_s"), DEFAULT_CONTINUED_CHAT_REOPEN_SILENCE_SECONDS, minimum=0.2, maximum=5.0),
        )
        segmenter.timeout_s = max(
            float(segmenter.timeout_s),
            _as_float(eou.get("continued_chat_reopen_timeout_s"), DEFAULT_CONTINUED_CHAT_REOPEN_TIMEOUT_SECONDS, minimum=2.0, maximum=60.0),
        )
        segmenter.no_speech_timeout_s = max(
            float(segmenter.no_speech_timeout_s),
            _as_float(eou.get("continued_chat_reopen_no_speech_timeout_s"), DEFAULT_CONTINUED_CHAT_REOPEN_NO_SPEECH_TIMEOUT_S, minimum=1.0, maximum=20.0),
        )
        segmenter.min_silence_frames = max(
            int(segmenter.min_silence_frames),
            _as_int(eou.get("continued_chat_reopen_min_silence_frames"), DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_FRAMES, minimum=3, maximum=60),
        )
        segmenter.min_silence_short_s = max(
            float(segmenter.min_silence_short_s),
            _as_float(eou.get("continued_chat_reopen_min_silence_short_s"), DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_SHORT_S, minimum=0.1, maximum=5.0),
        )
        segmenter.min_silence_long_s = max(
            float(segmenter.min_silence_long_s),
            _as_float(eou.get("continued_chat_reopen_min_silence_long_s"), DEFAULT_CONTINUED_CHAT_REOPEN_MIN_SILENCE_LONG_S, minimum=0.1, maximum=5.0),
        )

    return EouEngine(mode=mode, backend_name=backend_name, backend=backend, segmenter=segmenter)


# -------------------- STT + Voice Event Helpers --------------------
def _stt_config_snapshot() -> Dict[str, Any]:
    cfg = _voice_config_snapshot()
    stt = cfg.get("stt") if isinstance(cfg.get("stt"), dict) else {}
    return stt if isinstance(stt, dict) else {}


def _selected_stt_backend() -> str:
    return _normalize_stt_backend(_stt_config_snapshot().get("backend"))


def _stt_backend_available(backend: str) -> Tuple[bool, str]:
    token = _normalize_stt_backend(backend)
    if token == "wyoming":
        ok = (
            AsyncTcpClient is not None
            and Transcribe is not None
            and Transcript is not None
            and WyomingAudioStart is not None
            and WyomingAudioChunk is not None
            and WyomingAudioStop is not None
            and WyomingError is not None
        )
        return ok, _text(WYOMING_IMPORT_ERROR) or "wyoming dependency unavailable"
    if token == "faster_whisper":
        return WhisperModel is not None, _text(FASTER_WHISPER_IMPORT_ERROR) or "faster-whisper dependency unavailable"
    if token == "mlx_whisper":
        return MLXWhisper is not None, _text(MLX_WHISPER_IMPORT_ERROR) or "mlx-whisper dependency unavailable"
    if token == "vosk":
        if VoskModel is None or KaldiRecognizer is None:
            return False, _text(VOSK_IMPORT_ERROR) or "vosk dependency unavailable"
        try:
            model_path = _resolve_vosk_model_path()
        except Exception as exc:
            return False, f"Vosk model unavailable: {exc}"
        if not os.path.isdir(model_path):
            return False, f"Vosk model not found under {_stt_backend_model_root('vosk')}"
        if not _looks_like_vosk_model_dir(model_path):
            return False, f"No Vosk model directory found under {_stt_backend_model_root('vosk')}"
        return True, ""
    return False, f"unsupported STT backend: {token}"


def _resolve_stt_backend_selected(selected: str) -> Tuple[str, str]:
    token = _normalize_stt_backend(selected)
    ok, reason = _stt_backend_available(token)
    if ok:
        return token, ""
    if token != "wyoming":
        for candidate in (DEFAULT_STT_BACKEND, "faster_whisper", "mlx_whisper", "vosk"):
            fallback = _normalize_stt_backend(candidate)
            if fallback == token or fallback == "wyoming":
                continue
            fallback_ok, _fallback_reason = _stt_backend_available(fallback)
            if fallback_ok:
                return fallback, f"{token} unavailable: {reason}. Falling back to {fallback}."
    return token, reason


def _resolve_tts_backend_selected(selected: str) -> Tuple[str, str]:
    token = _normalize_tts_backend(selected)
    ok, reason = _tts_backend_available(token)
    if ok:
        return token, ""
    if token != "wyoming":
        wyoming_ok, _wyoming_reason = _tts_backend_available("wyoming")
        if wyoming_ok:
            return "wyoming", f"{token} unavailable: {reason}. Falling back to Wyoming."
    return token, reason


async def _ensure_voice_stt_end_sent(
    client: Any,
    module: Any,
    *,
    session: "VoiceSessionRuntime",
    transcript: str,
) -> None:
    if bool(session.stt_end_sent):
        return
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_END", "STT_END"), {"text": transcript})
    session.stt_end_sent = True


async def _ensure_voice_intent_active(
    client: Any,
    module: Any,
    *,
    session: "VoiceSessionRuntime",
) -> None:
    if bool(session.intent_active):
        return
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_INTENT_START", "INTENT_START"), None)
    session.intent_active = True


async def _send_voice_intent_end(
    client: Any,
    module: Any,
    *,
    session: "VoiceSessionRuntime",
    continue_conversation: bool = False,
) -> None:
    await _esphome_send_event(
        client,
        module,
        ("VOICE_ASSISTANT_INTENT_END", "INTENT_END"),
        {
            "conversation_id": _text(session.conversation_id) or _text(session.session_id),
            "continue_conversation": "1" if continue_conversation else "0",
        },
    )
    session.intent_active = False


async def _send_voice_no_text_recognized(
    client: Any,
    module: Any,
    *,
    message: str = "No speech was recognized.",
) -> None:
    await _esphome_send_event(
        client,
        module,
        ("VOICE_ASSISTANT_ERROR", "ERROR"),
        {
            "code": "stt-no-text-recognized",
            "message": _text(message) or "No speech was recognized.",
        },
    )


async def _send_tool_call_visual(
    client: Any,
    module: Any,
    *,
    session: "VoiceSessionRuntime",
    wait_text: str = "",
    wait_payload: Optional[Dict[str, Any]] = None,
) -> None:
    if bool(getattr(session, "tool_visual_sent", False)):
        return
    payload = wait_payload if isinstance(wait_payload, dict) else {}
    tool_name = _text(payload.get("tool") or payload.get("tool_name") or payload.get("name") or payload.get("func_name"))
    text = _sanitize_tool_progress_spoken_text(wait_text or _text(payload.get("text")))
    event_payload: Dict[str, Any] = {}
    if text:
        event_payload["text"] = text
    if tool_name:
        event_payload["tool"] = tool_name
    ok = await _esphome_send_event(
        client,
        module,
        ("VOICE_ASSISTANT_TOOL_CALL_START", "TOOL_CALL_START"),
        event_payload,
    )
    if ok:
        session.tool_visual_sent = True


async def _play_live_tool_progress_for_session(
    client: Any,
    module: Any,
    *,
    selector: str,
    runtime: Dict[str, Any],
    session: "VoiceSessionRuntime",
    transcript: str,
    wait_text: str,
    wait_payload: Optional[Dict[str, Any]] = None,
) -> None:
    await _send_tool_call_visual(
        client,
        module,
        session=session,
        wait_text=wait_text,
        wait_payload=wait_payload,
    )
    if not _experimental_live_tool_progress_enabled():
        return
    spoken = _sanitize_tool_progress_spoken_text(wait_text)
    if not spoken:
        return
    last = _lower(_text(session.last_tool_progress_text).rstrip(".!?"))
    current = _lower(spoken.rstrip(".!?"))
    if last and current and last == current:
        return

    audio_bytes, audio_format, backend_used, _backend_note = await _native_synthesize_text(
        spoken,
        session=session,
    )
    if not audio_bytes:
        return

    reply_playback_target = _session_reply_playback_target(selector, session)
    reply_playback_on_device = reply_playback_target == reply_playback.REPLY_PLAYBACK_DEVICE
    timeout_s = _run_end_timeout_s(audio_bytes, audio_format)
    waiter: Optional[asyncio.Future[Any]] = None
    lock = runtime.get("lock")

    await _ensure_voice_stt_end_sent(client, module, session=session, transcript=transcript)
    await _ensure_voice_intent_active(client, module, session=session)
    await _send_voice_intent_end(client, module, session=session, continue_conversation=False)
    await _esphome_send_event(
        client,
        module,
        ("VOICE_ASSISTANT_TTS_START", "TTS_START"),
        {
            "text": spoken,
            "tts_kind": "tool",
        },
    )

    if reply_playback_target == reply_playback.REPLY_PLAYBACK_SILENT:
        await asyncio.sleep(min(max(_estimate_pcm_duration_s(audio_bytes, audio_format), 0.2), 2.0))
        _native_debug(
            f"live tool progress suppressed by silent playback selector={selector} session_id={session.session_id} text={spoken!r}"
        )
    elif not reply_playback_on_device:
        external_result = await _play_reply_on_external_target(
            selector=selector,
            session=session,
            spoken_text=spoken,
            target=reply_playback_target,
            audio_bytes=audio_bytes,
            audio_format=audio_format,
            backend_used=backend_used,
        )
        external_ok = bool(external_result.get("ok")) if isinstance(external_result, dict) else False
        if not external_ok:
            logger.warning(
                "[native-voice] external tool-progress playback failed selector=%s target=%s error=%s",
                selector,
                reply_playback_target,
                _text((external_result or {}).get("error") if isinstance(external_result, dict) else ""),
            )
        playback_delay_s = max(0.25, min(8.0, _estimate_pcm_duration_s(audio_bytes, audio_format) + 0.2))
        await asyncio.sleep(playback_delay_s if external_ok else 0.25)
        _native_debug(
            f"live tool progress external playback selector={selector} target={reply_playback_target} "
            f"ok={external_ok} session_id={session.session_id} text={spoken!r}"
        )
    else:
        url = _store_tts_url(selector, session.session_id, audio_bytes, audio_format)
        if not url:
            return
        waiter = asyncio.get_running_loop().create_future()

        async with lock:
            _set_awaiting_announcement_state(
                runtime,
                session_id=_text(session.session_id),
                kind="tool_progress",
                future=waiter,
                timeout_s=timeout_s,
            )
            _cancel_announcement_wait(runtime)
            _schedule_announcement_timeout(selector, client, module, timeout_s)

        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_TTS_END", "TTS_END"),
            {"url": url, "tts_kind": "tool", "state_after": "tool_call"},
        )
        _native_debug(
            f"live tool progress queued selector={selector} session_id={session.session_id} timeout_s={timeout_s:.2f} text={spoken!r}"
        )
        with contextlib.suppress(Exception):
            await waiter

    await _ensure_voice_intent_active(client, module, session=session)
    session.live_tool_progress_played = True
    session.last_tool_progress_text = spoken


async def _start_experimental_streamed_tts_response(
    selector: str,
    client: Any,
    module: Any,
    *,
    session: VoiceSessionRuntime,
    runtime: Dict[str, Any],
    response_text: str,
    transcript: str,
) -> Optional[Dict[str, Any]]:
    selection = _tts_selection_from_values()
    if _normalize_tts_backend(_text(session.tts_backend_effective)) == "chatterbox" and _chatterbox_tts_streaming_enabled(selection):
        return None
    if not _experimental_tts_early_start_enabled():
        return None
    chunks = _build_experimental_tts_chunks(response_text)
    if len(chunks) < 2:
        return None

    first_text = _text(chunks[0])
    remaining_chunks = [chunk for chunk in chunks[1:] if _text(chunk)]
    if not first_text or not remaining_chunks:
        return None

    tts_started = time.monotonic()
    first_audio, first_format, backend_used, backend_note = await _native_synthesize_text(
        first_text,
        session=session,
    )
    first_latency_ms = max(0.0, (time.monotonic() - tts_started) * 1000.0)
    if not first_audio:
        return None

    first_url = _store_tts_url(selector, session.session_id, first_audio, first_format)
    if not first_url:
        return None

    session.tts_latency_ms = first_latency_ms
    _voice_metrics_record_tts(selector, backend_used or session.tts_backend_effective or session.tts_backend, session.tts_latency_ms)

    prepare_task = asyncio.create_task(
        _prepare_streamed_tts_segments(
            selector,
            session.session_id,
            session=session,
            chunk_texts=remaining_chunks,
        )
    )

    lock = runtime.get("lock")
    timeout_s = _run_end_timeout_s(first_audio, first_format)
    async with lock:
        _clear_streamed_tts_state(runtime)
        runtime["streamed_tts"] = {
            "session_id": _text(session.session_id),
            "ready_segments": [],
            "done": False,
            "error": "",
            "backend_used": backend_used,
            "backend_note": backend_note,
            "prepare_task": prepare_task,
            "segment_count": len(chunks),
        }
        _set_awaiting_announcement_state(
            runtime,
            session_id=_text(session.session_id),
            kind="response_streamed",
            future=None,
            timeout_s=timeout_s,
        )
        _cancel_announcement_wait(runtime)
        _schedule_announcement_timeout(selector, client, module, timeout_s)

    await _ensure_voice_stt_end_sent(client, module, session=session, transcript=transcript)
    if not bool(session.intent_active):
        await _ensure_voice_intent_active(client, module, session=session)
    await _send_voice_intent_end(client, module, session=session, continue_conversation=False)
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_START", "TTS_START"), {"text": response_text})
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_END", "TTS_END"), {"url": first_url})
    _native_debug(
        f"experimental streamed tts started selector={selector} session_id={session.session_id} "
        f"segments={len(chunks)} timeout_s={timeout_s:.2f}"
    )
    return {
        "first_url": first_url,
        "backend_used": backend_used,
        "backend_note": backend_note,
        "tts_bytes": len(first_audio),
        "tts_mode": "segmented_url",
        "run_end_mode": "announcement_streamed",
        "segment_count": len(chunks),
    }


async def _start_chatterbox_streamed_tts_response(
    selector: str,
    client: Any,
    module: Any,
    *,
    session: VoiceSessionRuntime,
    runtime: Dict[str, Any],
    response_text: str,
    transcript: str,
) -> Optional[Dict[str, Any]]:
    selection = _tts_selection_from_values()
    if not _chatterbox_tts_streaming_enabled(selection):
        return None
    effective_backend = _normalize_tts_backend(_text(session.tts_backend_effective))
    if effective_backend != "chatterbox":
        return None
    prompt = _text(response_text)
    if not prompt:
        return None

    tts_started = time.monotonic()
    try:
        stream_url = _store_chatterbox_tts_stream_url(selector, session.session_id, prompt, selection)
    except Exception as exc:
        logger.warning(
            "[native-voice] Chatterbox streaming TTS URL preparation failed selector=%s session_id=%s error=%s",
            selector,
            session.session_id,
            _text(exc),
        )
        _native_debug(
            f"chatterbox streaming tts url failed selector={selector} session_id={session.session_id} error={exc}"
        )
        return None
    if not stream_url:
        return None

    session.tts_latency_ms = max(0.0, (time.monotonic() - tts_started) * 1000.0)
    _voice_metrics_record_tts(selector, effective_backend or session.tts_backend, session.tts_latency_ms)

    timeout_s = _run_end_timeout_for_text_s(prompt)
    lock = runtime.get("lock")
    async with lock:
        _clear_streamed_tts_state(runtime)
        _set_awaiting_announcement_state(
            runtime,
            session_id=_text(session.session_id),
            kind="response_chatterbox_stream",
            future=None,
            timeout_s=timeout_s,
        )
        _cancel_announcement_wait(runtime)
        _schedule_announcement_timeout(selector, client, module, timeout_s)

    await _ensure_voice_stt_end_sent(client, module, session=session, transcript=transcript)
    if not bool(session.intent_active):
        await _ensure_voice_intent_active(client, module, session=session)
    await _send_voice_intent_end(client, module, session=session, continue_conversation=False)
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_START", "TTS_START"), {"text": prompt})
    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_END", "TTS_END"), {"url": stream_url})
    _native_debug(
        f"chatterbox streaming tts started selector={selector} session_id={session.session_id} "
        f"timeout_s={timeout_s:.2f} url={stream_url}"
    )
    return {
        "first_url": stream_url,
        "backend_used": effective_backend,
        "backend_note": "",
        "tts_bytes": 0,
        "tts_mode": "chatterbox_stream_url",
        "run_end_mode": "announcement_streamed",
        "segment_count": 1,
    }


# -------------------- Playback URL Store --------------------
def _pcm_to_wav(audio_bytes: bytes, audio_format: Dict[str, Any]) -> Tuple[bytes, Dict[str, int]]:
    pcm = bytes(audio_bytes or b"")
    if not pcm:
        return b"", {
            "rate": DEFAULT_VOICE_SAMPLE_RATE_HZ,
            "width": DEFAULT_VOICE_SAMPLE_WIDTH,
            "channels": DEFAULT_VOICE_CHANNELS,
        }

    rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)
    if width not in {1, 2, 3, 4}:
        width = DEFAULT_VOICE_SAMPLE_WIDTH
    if channels < 1 or channels > 8:
        channels = DEFAULT_VOICE_CHANNELS

    block_align = max(1, width * channels)
    usable = len(pcm) - (len(pcm) % block_align)
    if usable <= 0:
        return b"", {"rate": rate, "width": width, "channels": channels}
    if usable != len(pcm):
        pcm = pcm[:usable]

    with io.BytesIO() as out:
        with wave.open(out, "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(width)
            wav_file.setframerate(rate)
            wav_file.writeframes(pcm)
        return out.getvalue(), {"rate": rate, "width": width, "channels": channels}


def _tts_url_ttl_s() -> float:
    cfg = _voice_config_snapshot()
    limits = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    return float(limits.get("tts_url_ttl_s") or DEFAULT_TTS_URL_TTL_S)


def _tts_url_prune_locked(now_ts: Optional[float] = None) -> int:
    now = float(now_ts if isinstance(now_ts, (int, float)) else _now())
    removed = 0
    for stream_id, row in list(_tts_url_store.items()):
        if not isinstance(row, dict):
            _tts_url_store.pop(stream_id, None)
            removed += 1
            continue
        expires_ts = float(row.get("expires_ts") or 0.0)
        if expires_ts > 0 and now >= expires_ts:
            _tts_url_store.pop(stream_id, None)
            removed += 1
    return removed


def _service_host_for_peer(peer_host: str) -> str:
    env_host = _text(os.getenv("VOICE_CORE_PUBLIC_HOST"))
    if env_host:
        return env_host

    htmlui_host = _text(os.getenv("HTMLUI_HOST", "0.0.0.0"))
    if htmlui_host not in {"0.0.0.0", "::", "127.0.0.1", "localhost"}:
        return htmlui_host

    targets: List[str] = []
    peer = _lower(peer_host)
    if peer and not peer.startswith("127."):
        targets.append(peer)
    targets.append("8.8.8.8")

    for target in targets:
        with contextlib.suppress(Exception):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.connect((target, 80))
                host = _text(probe.getsockname()[0])
                if host and not host.startswith("127."):
                    return host

    with contextlib.suppress(Exception):
        host = _text(socket.gethostbyname(socket.gethostname()))
        if host and not host.startswith("127."):
            return host

    return "127.0.0.1"


def _selector_host(selector: str) -> str:
    token = _text(selector)
    if token.startswith("host:"):
        return _lower(token.split(":", 1)[1])

    rows = _load_satellite_registry()
    for row in rows:
        if _text(row.get("selector")) == token:
            host = _lower(row.get("host"))
            if host:
                return host
    return ""


def _store_tts_url(selector: str, session_id: str, audio_bytes: bytes, audio_format: Dict[str, Any]) -> str:
    wav_bytes, normalized_format = _pcm_to_wav(audio_bytes, audio_format)
    if not wav_bytes:
        return ""

    stream_id = uuid.uuid4().hex
    expires_ts = _now() + _tts_url_ttl_s()
    with _tts_url_store_lock:
        _tts_url_prune_locked()
        _tts_url_store[stream_id] = {
            "id": stream_id,
            "selector": _text(selector),
            "session_id": _text(session_id),
            "created_ts": _now(),
            "expires_ts": expires_ts,
            "audio_format": normalized_format,
            "wav_bytes": wav_bytes,
        }

    host = _service_host_for_peer(_selector_host(selector))
    url = f"http://{host}:{_main_app_port()}/api/tater/satellite/v1/tts/{stream_id}.wav"
    _native_debug(
        f"native tts url prepared selector={_text(selector)} session_id={_text(session_id)} bytes={len(wav_bytes)} url={url}"
    )
    return url


def _store_chatterbox_tts_stream_url(
    selector: str,
    session_id: str,
    text: str,
    selection: Dict[str, Any],
) -> str:
    prompt = _text(text)
    if not prompt:
        return ""

    endpoint, payload = _chatterbox_tts_request(
        prompt,
        voice=_text((selection or {}).get("voice")),
        base_url=_text((selection or {}).get("chatterbox_base_url")) or DEFAULT_CHATTERBOX_TTS_BASE_URL,
        voice_mode=_text((selection or {}).get("chatterbox_voice_mode")) or DEFAULT_CHATTERBOX_TTS_VOICE_MODE,
        chunk_size=(selection or {}).get("chatterbox_chunk_size"),
        temperature=(selection or {}).get("chatterbox_temperature"),
        exaggeration=(selection or {}).get("chatterbox_exaggeration"),
        cfg_weight=(selection or {}).get("chatterbox_cfg_weight"),
        seed=(selection or {}).get("chatterbox_seed"),
        speed_factor=(selection or {}).get("chatterbox_speed_factor"),
        language=(selection or {}).get("chatterbox_language"),
        stream=True,
    )
    if not endpoint or not payload:
        return ""

    stream_id = uuid.uuid4().hex
    expires_ts = _now() + _tts_url_ttl_s()
    with _tts_url_store_lock:
        _tts_url_prune_locked()
        _tts_url_store[stream_id] = {
            "id": stream_id,
            "selector": _text(selector),
            "session_id": _text(session_id),
            "created_ts": _now(),
            "expires_ts": expires_ts,
            "stream_kind": "chatterbox",
            "endpoint": endpoint,
            "payload": dict(payload),
            "max_bytes": DEFAULT_CHATTERBOX_TTS_STREAM_MAX_BYTES,
        }

    host = _service_host_for_peer(_selector_host(selector))
    url = f"http://{host}:{_main_app_port()}/api/tater/satellite/v1/tts/{stream_id}.wav"
    _native_debug(
        f"chatterbox streaming tts url prepared selector={_text(selector)} session_id={_text(session_id)} "
        f"stream_id={stream_id} url={url}"
    )
    return url


def _fetch_tts_url(stream_id: str) -> Optional[Dict[str, Any]]:
    token = _text(stream_id)
    if not token:
        return None
    with _tts_url_store_lock:
        _tts_url_prune_locked()
        row = _tts_url_store.get(token)
        if not isinstance(row, dict):
            return None
        return dict(row)


def _store_media_url(
    selector: str,
    session_id: str,
    media_bytes: bytes,
    *,
    media_type: str,
    filename: str,
) -> str:
    data = bytes(media_bytes or b"")
    if not data:
        return ""

    stream_id = uuid.uuid4().hex
    expires_ts = _now() + _tts_url_ttl_s()
    mime = _text(media_type).strip() or "application/octet-stream"
    with _tts_url_store_lock:
        _tts_url_prune_locked()
        _tts_url_store[stream_id] = {
            "id": stream_id,
            "selector": _text(selector),
            "session_id": _text(session_id),
            "created_ts": _now(),
            "expires_ts": expires_ts,
            "media_type": mime,
            "filename": _text(filename) or "audio.bin",
            "body_bytes": data,
        }

    host = _service_host_for_peer(_selector_host(selector))
    url = f"http://{host}:{_main_app_port()}/api/tater/satellite/v1/media/{stream_id}"
    _native_debug(
        f"native media url prepared selector={_text(selector)} session_id={_text(session_id)} "
        f"bytes={len(data)} media_type={mime} url={url}"
    )
    return url


async def _download_media_source(source_url: str) -> Tuple[bytes, str]:
    url = _text(source_url).strip()
    if not url:
        raise ValueError("source_url is required")

    def _fetch() -> Tuple[bytes, str]:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        content_type = _text(resp.headers.get("Content-Type")).split(";", 1)[0].strip().lower()
        return bytes(resp.content or b""), content_type

    return await run_background(_fetch)


def _estimate_pcm_duration_s(audio_bytes: bytes, audio_format: Dict[str, Any]) -> float:
    data = bytes(audio_bytes or b"")
    if not data:
        return 0.0
    rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)
    frame_bytes = max(1, width * channels)
    return float(len(data)) / float(max(1, rate * frame_bytes))


def _append_pcm_silence(audio_bytes: bytes, audio_format: Dict[str, Any], *, seconds: float) -> bytes:
    data = bytes(audio_bytes or b"")
    if seconds <= 0:
        return data
    rate = int(audio_format.get("rate") or DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or DEFAULT_VOICE_CHANNELS)
    if width not in {1, 2, 3, 4}:
        width = DEFAULT_VOICE_SAMPLE_WIDTH
    if channels < 1 or channels > 8:
        channels = DEFAULT_VOICE_CHANNELS
    frame_bytes = max(1, width * channels)
    silence_frames = max(1, int(round(float(rate) * float(seconds))))
    return data + (b"\x00" * (silence_frames * frame_bytes))


def _run_end_timeout_s(audio_bytes: bytes, audio_format: Dict[str, Any]) -> float:
    # Leave enough headroom for device-side HTTP fetch, WAV parsing/resampling,
    # and scheduling jitter so a late `announcement_finished` doesn't force an
    # early RUN_END while the satellite is still speaking.
    fetch_headroom_s = (len(audio_bytes or b"") / DEFAULT_TTS_DEVICE_FETCH_BYTES_PER_S) + 8.0
    return max(
        5.0,
        min(
            float(DEFAULT_TTS_ANNOUNCEMENT_TIMEOUT_MAX_S),
            _estimate_pcm_duration_s(audio_bytes, audio_format) + max(12.0, fetch_headroom_s),
        ),
    )


def _run_end_timeout_for_text_s(text: str) -> float:
    prompt = _text(text)
    word_count = len(re.findall(r"[a-z0-9']+", prompt.lower()))
    char_estimate_s = len(prompt) / 12.0 if prompt else 0.0
    word_estimate_s = word_count / 2.35 if word_count else 0.0
    return max(
        8.0,
        min(
            float(DEFAULT_TTS_ANNOUNCEMENT_TIMEOUT_MAX_S),
            max(char_estimate_s, word_estimate_s) + 8.0,
        ),
    )


async def _send_followup_reopen_marker(
    selector: str,
    client: Any,
    module: Any,
    *,
    runtime: Dict[str, Any],
    session: "VoiceSessionRuntime",
    audio_format: Dict[str, Any],
    reason: str,
) -> str:
    marker_audio = _append_pcm_silence(b"", audio_format, seconds=0.16)
    marker_url = _store_tts_url(selector, session.session_id, marker_audio, audio_format)
    if not marker_url:
        return ""

    timeout_s = max(1.0, min(3.0, _run_end_timeout_s(marker_audio, audio_format)))
    lock = runtime.get("lock")
    async with lock:
        _set_awaiting_announcement_state(
            runtime,
            session_id=_text(session.session_id),
            kind="response",
            future=None,
            timeout_s=timeout_s,
        )
        _cancel_announcement_wait(runtime)
        _schedule_announcement_timeout(selector, client, module, timeout_s)

    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_END", "TTS_END"), {"url": marker_url})
    _native_debug(
        f"continued chat reopen marker queued selector={selector} session_id={session.session_id} "
        f"reason={_text(reason) or '-'} timeout_s={timeout_s:.2f}"
    )
    return marker_url


def _set_awaiting_announcement_state(
    runtime: Dict[str, Any],
    *,
    session_id: str,
    kind: str,
    future: Optional[asyncio.Future[Any]],
    timeout_s: float,
) -> None:
    state = AwaitingAnnouncementState(
        session_id=_text(session_id),
        kind=_text(kind),
        future=future,
        started_ts=_now(),
        timeout_s=max(0.0, float(timeout_s or 0.0)),
    )
    runtime["awaiting_announcement_state"] = state
    runtime["awaiting_announcement"] = True
    runtime["awaiting_session_id"] = state.session_id
    runtime["awaiting_announcement_kind"] = state.kind
    runtime["announcement_future"] = future
    runtime["awaiting_announcement_started_ts"] = state.started_ts
    runtime["awaiting_announcement_timeout_s"] = state.timeout_s


def _clear_awaiting_announcement_state(runtime: Dict[str, Any]) -> None:
    runtime["awaiting_announcement_state"] = None
    runtime["awaiting_announcement"] = False
    runtime["awaiting_session_id"] = ""
    runtime["awaiting_announcement_kind"] = ""
    runtime["announcement_future"] = None
    runtime["awaiting_announcement_started_ts"] = 0.0
    runtime["awaiting_announcement_timeout_s"] = 0.0


def _awaiting_announcement_state(runtime: Dict[str, Any]) -> Optional[AwaitingAnnouncementState]:
    state = runtime.get("awaiting_announcement_state")
    if isinstance(state, AwaitingAnnouncementState):
        return state
    if not bool(runtime.get("awaiting_announcement")):
        return None
    future = runtime.get("announcement_future")
    return AwaitingAnnouncementState(
        session_id=_text(runtime.get("awaiting_session_id")),
        kind=_text(runtime.get("awaiting_announcement_kind")),
        future=future if isinstance(future, asyncio.Future) else None,
        started_ts=_as_float(runtime.get("awaiting_announcement_started_ts"), 0.0),
        timeout_s=_as_float(runtime.get("awaiting_announcement_timeout_s"), 0.0),
    )


# -------------------- ESPHome Runtime Bridge --------------------
# Generic ESPHome discovery and device-runtime internals now live in
# `tater_voice.device_runtime`, and UI-only presentation helpers live in
# `tater_voice.ui_helpers`. The voice pipeline keeps only the voice-specific
# runtime hooks that still need to coordinate live sessions.
from .. import device_runtime as _esphome_device_runtime
from .. import ui_helpers as _esphome_ui_helpers

_discover_mdns_once = _esphome_device_runtime.discover_mdns_once
_discovery_loop = _esphome_device_runtime.discovery_loop
_esphome_target_map = _esphome_device_runtime.target_map
_esphome_import = _esphome_device_runtime.esphome_import
_esphome_module_attr = _esphome_device_runtime.esphome_module_attr
_esphome_event_type_value = _esphome_device_runtime.esphome_event_type_value
_esphome_payload_strings = _esphome_device_runtime.esphome_payload_strings
_esphome_client_call = _esphome_device_runtime.esphome_client_call
_esphome_send_event = _esphome_device_runtime.esphome_send_event
_esphome_client_connected = _esphome_device_runtime.esphome_client_connected
_esphome_list_entity_catalog = _esphome_device_runtime.list_entity_catalog
_esphome_subscribe_states = _esphome_device_runtime.subscribe_states
_esphome_logs_start = _esphome_device_runtime.logs_start
_esphome_logs_poll = _esphome_device_runtime.logs_poll
_esphome_logs_stop = _esphome_device_runtime.logs_stop
_esphome_logs_cleanup_idle = _esphome_device_runtime.logs_cleanup_idle
_esphome_verify_connection = _esphome_device_runtime.verify_connection
_esphome_voice_feature_snapshot = _esphome_device_runtime.voice_feature_snapshot
_esphome_build_client = _esphome_device_runtime.build_client
_esphome_disconnect_selector = _esphome_device_runtime.disconnect_selector
_esphome_disconnect_all = _esphome_device_runtime.disconnect_all
_esphome_connect_selector = _esphome_device_runtime.connect_selector
_esphome_reconcile_once = _esphome_device_runtime.reconcile_once
_esphome_loop = _esphome_device_runtime.esphome_loop
_esphome_bootstrap_reconnect = _esphome_device_runtime.bootstrap_reconnect
_esphome_status = _esphome_device_runtime.status
_esphome_entities_for_selector = _esphome_device_runtime.entities_for_selector
_esphome_command_entity = _esphome_device_runtime.command_entity
_esphome_media_player_entity_snapshot = _esphome_device_runtime.media_player_entity_snapshot
_esphome_play_media_announcement = _esphome_device_runtime.play_media_announcement
_esphome_client_row_snapshot_sync = _esphome_device_runtime.client_row_snapshot_sync


async def _send_voice_run_end(
    client: Any,
    module: Any,
    *,
    selector: str = "",
    session_id: str = "",
    reason: str = "",
    skip_if_active_session_mismatch: bool = True,
    skip_if_pending_followup: bool = False,
) -> bool:
    token = _text(selector)
    sid = _text(session_id)
    reason_token = _text(reason)
    stale_session = False
    pending_followup = False
    pending_followup_until = 0.0

    if token and (sid or skip_if_pending_followup):
        runtime = _selector_runtime(token)
        lock = runtime.get("lock")
        if lock is not None and hasattr(lock, "acquire"):
            async with lock:
                if sid and skip_if_active_session_mismatch:
                    active = runtime.get("session")
                    active_sid = _text(active.session_id) if isinstance(active, VoiceSessionRuntime) else ""
                    stale_session = bool(active_sid and active_sid != sid)
                if skip_if_pending_followup:
                    followup_conv = _text(runtime.get("pending_followup_conversation_id"))
                    followup_until = _as_float(runtime.get("pending_followup_until_ts"), 0.0)
                    pending_followup_until = followup_until
                    pending_followup = bool(followup_conv) and _now() <= followup_until

    if stale_session:
        _native_debug(
            f"esphome run_end skipped selector={token or '-'} session_id={sid or '-'} "
            f"reason={reason_token or '-'} stale_session=1"
        )
        return False

    if pending_followup:
        _native_debug(
            f"esphome run_end skipped selector={token or '-'} session_id={sid or '-'} "
            f"reason={reason_token or '-'} pending_followup=1"
        )
        if token and sid and pending_followup_until > 0.0:
            await _wake_arbitration_extend_room_claim_for_session(
                selector=token,
                session_id=sid,
                until_ts=pending_followup_until + DEFAULT_WAKE_ARBITRATION_RELEASE_GRACE_S,
                reason="pending_followup",
                replace=True,
            )
        return False

    ok = await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_END", "RUN_END"), None)
    _native_debug(
        f"esphome run_end sent selector={token or '-'} session_id={sid or '-'} "
        f"reason={reason_token or '-'} ok={1 if ok else 0}"
    )
    if token and sid:
        await _wake_arbitration_release_room_claim_for_session(
            selector=token,
            session_id=sid,
            reason=reason_token or "run_end",
            delay_s=DEFAULT_WAKE_ARBITRATION_RELEASE_GRACE_S,
        )
    return ok


async def _prepare_streamed_tts_segments(
    selector: str,
    session_id: str,
    *,
    session: VoiceSessionRuntime,
    chunk_texts: List[str],
) -> None:
    token = _text(selector)
    runtime = _selector_runtime(token)
    prepared: List[Dict[str, Any]] = []
    error = ""
    backend_used = ""
    backend_note = ""
    try:
        for chunk_text in chunk_texts:
            if not _text(chunk_text):
                continue
            audio_bytes, audio_format, backend_used, backend_note = await _native_synthesize_text(
                chunk_text,
                session=session,
            )
            if not audio_bytes:
                continue
            url = _store_tts_url(token, session_id, audio_bytes, audio_format)
            if not url:
                continue
            prepared.append(
                {
                    "text": _text(chunk_text),
                    "url": url,
                    "timeout_s": _run_end_timeout_s(audio_bytes, audio_format),
                    "tts_bytes": len(audio_bytes),
                    "backend_used": backend_used,
                    "backend_note": backend_note,
                }
            )
    except asyncio.CancelledError:
        return
    except Exception as exc:
        error = _text(exc) or "segment_prepare_failed"

    lock = runtime.get("lock")
    if lock is None or not hasattr(lock, "acquire"):
        return
    async with lock:
        state = runtime.get("streamed_tts")
        if not isinstance(state, dict) or _text(state.get("session_id")) != _text(session_id):
            return
        ready_segments = state.get("ready_segments")
        if not isinstance(ready_segments, list):
            ready_segments = []
            state["ready_segments"] = ready_segments
        ready_segments.extend(prepared)
        state["done"] = True
        state["error"] = error
        if backend_used:
            state["backend_used"] = backend_used
        if backend_note:
            state["backend_note"] = backend_note
        state["prepare_task"] = None


async def _send_streamed_tts_segment(
    selector: str,
    client: Any,
    module: Any,
    *,
    session_id: str,
    segment: Dict[str, Any],
) -> None:
    token = _text(selector)
    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    timeout_s = max(1.0, float(segment.get("timeout_s") or 0.0))
    await _esphome_send_event(
        client,
        module,
        ("VOICE_ASSISTANT_TTS_START", "TTS_START"),
        {"text": _text(segment.get("text")) or "Continuing response."},
    )
    segment_url = _text(segment.get("url"))
    direct_queued = False
    if segment_url.startswith(("http://", "https://")):
        try:
            direct_result = await _esphome_play_media_announcement(token, segment_url)
        except Exception as exc:
            _native_debug(
                f"streamed tts media player announcement unavailable selector={token} "
                f"session_id={session_id} error={_text(exc) or exc.__class__.__name__}"
            )
        else:
            entity_key = _text(direct_result.get("entity_key")) if isinstance(direct_result, dict) else ""
            if entity_key:
                async with lock:
                    _set_awaiting_announcement_state(
                        runtime,
                        session_id=_text(session_id),
                        kind="response_streamed",
                        future=None,
                        timeout_s=timeout_s,
                    )
                    _cancel_announcement_wait(runtime)
                    _schedule_media_player_announcement_monitor(
                        token,
                        client,
                        module,
                        entity_key=entity_key,
                        timeout_s=timeout_s,
                    )
                direct_queued = True

    if not direct_queued:
        async with lock:
            _set_awaiting_announcement_state(
                runtime,
                session_id=_text(session_id),
                kind="response_streamed",
                future=None,
                timeout_s=timeout_s,
            )
            _cancel_announcement_wait(runtime)
            _schedule_announcement_timeout(token, client, module, timeout_s)
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_TTS_END", "TTS_END"),
            {"url": segment_url},
        )
    _native_debug(
        f"streamed tts segment queued selector={token} session_id={session_id} timeout_s={timeout_s:.2f} "
        f"mode={'media_player_announcement' if direct_queued else 'voice_event'} url={segment_url}"
    )


async def _await_and_dispatch_streamed_tts_segment(
    selector: str,
    client: Any,
    module: Any,
    *,
    session_id: str,
) -> None:
    token = _text(selector)
    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    segment: Optional[Dict[str, Any]] = None
    should_finalize = False
    try:
        while True:
            await asyncio.sleep(0.05)
            async with lock:
                state = runtime.get("streamed_tts")
                if not isinstance(state, dict) or _text(state.get("session_id")) != _text(session_id):
                    return
                if _awaiting_announcement_state(runtime) is not None:
                    return
                ready_segments = state.get("ready_segments") if isinstance(state.get("ready_segments"), list) else []
                if ready_segments:
                    segment = dict(ready_segments.pop(0))
                    break
                if bool(state.get("done")):
                    runtime["streamed_tts"] = None
                    should_finalize = True
                    break
        if isinstance(segment, dict):
            await _send_streamed_tts_segment(
                token,
                client,
                module,
                session_id=session_id,
                segment=segment,
            )
            return
        if should_finalize:
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=session_id,
                reason="streamed_tts_dispatch_done",
            )
    except asyncio.CancelledError:
        return
    except Exception as exc:
        _native_debug(f"streamed tts dispatch failed selector={token} session_id={session_id} error={exc}")
        with contextlib.suppress(Exception):
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=session_id,
                reason="streamed_tts_dispatch_error",
            )
    finally:
        async with lock:
            task = runtime.get("streamed_tts_dispatch_task")
            if task is asyncio.current_task():
                runtime["streamed_tts_dispatch_task"] = None


async def _maybe_continue_streamed_tts(
    selector: str,
    client: Any,
    module: Any,
    *,
    reason: str,
) -> bool:
    token = _text(selector)
    if not token:
        return False

    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    segment: Optional[Dict[str, Any]] = None
    should_finalize = False
    session_id = ""

    async with lock:
        state = runtime.get("streamed_tts")
        if not isinstance(state, dict):
            return False
        session_id = _text(state.get("session_id"))
        _clear_awaiting_announcement_state(runtime)
        _cancel_announcement_wait(runtime)

        ready_segments = state.get("ready_segments") if isinstance(state.get("ready_segments"), list) else []
        if ready_segments:
            segment = dict(ready_segments.pop(0))
        elif bool(state.get("done")):
            runtime["streamed_tts"] = None
            should_finalize = True
        else:
            if not isinstance(runtime.get("streamed_tts_dispatch_task"), asyncio.Task):
                runtime["streamed_tts_dispatch_task"] = asyncio.create_task(
                    _await_and_dispatch_streamed_tts_segment(
                        token,
                        client,
                        module,
                        session_id=session_id,
                    )
                )
            _native_debug(
                f"streamed tts awaiting next segment selector={token} session_id={session_id} reason={_text(reason) or 'announcement_finished'}"
            )
            return True

    if isinstance(segment, dict):
        await _send_streamed_tts_segment(
            token,
            client,
            module,
            session_id=session_id,
            segment=segment,
        )
        return True

    if should_finalize:
        await _send_voice_run_end(
            client,
            module,
            selector=token,
            session_id=session_id,
            reason=_text(reason) or "streamed_tts_segment_complete",
        )
        _native_debug(
            f"streamed tts finalize selector={token} session_id={session_id} reason={_text(reason) or 'segment_complete'}"
        )
        return True
    return False


async def _finalize_after_announcement(selector: str, client: Any, module: Any, *, reason: str) -> bool:
    token = _text(selector)
    if not token:
        return False

    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    async with lock:
        state = _awaiting_announcement_state(runtime)
        kind = _text(state.kind if state is not None else "")
    if kind == "response_streamed" and await _maybe_continue_streamed_tts(token, client, module, reason=reason):
        return True

    future: Optional[asyncio.Future[Any]] = None
    skip_run_end_for_followup = False
    async with lock:
        state = _awaiting_announcement_state(runtime)
        if state is None:
            return False
        session_id = _text(state.session_id)
        kind = _text(state.kind)
        future = state.future if isinstance(state.future, asyncio.Future) else None
        if reason == "announcement_timeout":
            runtime["last_announcement_timeout_ts"] = _now()
        else:
            runtime["last_announcement_timeout_ts"] = 0.0
        pending_followup = _text(runtime.get("pending_followup_conversation_id"))
        pending_followup_until = _as_float(runtime.get("pending_followup_until_ts"), 0.0)
        skip_run_end_for_followup = (
            reason == "announcement_finished"
            and kind == "response"
            and bool(pending_followup)
            and _now() <= pending_followup_until
        )
        _clear_awaiting_announcement_state(runtime)
        _cancel_announcement_wait(runtime)

    if isinstance(future, asyncio.Future) and not future.done():
        with contextlib.suppress(Exception):
            future.set_result({"reason": reason, "kind": kind, "session_id": session_id})

    if kind == "tool_progress":
        _native_debug(
            f"esphome tool progress finalize selector={token} session_id={session_id} reason={reason} next_visual=tool_until_response"
        )
        return True
    if kind == "media_player_direct":
        _native_debug(
            f"esphome media player announcement finalize selector={token} session_id={session_id} reason={reason}"
        )
        return True

    await _send_voice_run_end(
        client,
        module,
        selector=token,
        session_id=session_id,
        reason=f"announcement:{reason}",
        skip_if_active_session_mismatch=(kind != "external"),
        skip_if_pending_followup=skip_run_end_for_followup,
    )
    _native_debug(f"esphome announcement finalize selector={token} session_id={session_id} reason={reason}")
    return True


def _media_player_snapshot_is_announcing(snapshot: Dict[str, Any]) -> bool:
    row = snapshot if isinstance(snapshot, dict) else {}
    attrs = row.get("attrs") if isinstance(row.get("attrs"), dict) else {}
    state = _lower(attrs.get("state") or row.get("raw"))
    return "announc" in state or state == "4"


def _schedule_media_player_announcement_monitor(
    selector: str,
    client: Any,
    module: Any,
    *,
    entity_key: str,
    timeout_s: float,
) -> None:
    token = _text(selector)
    if not token:
        return

    runtime = _selector_runtime(token)
    key_text = _text(entity_key)

    async def _timer() -> None:
        started = False
        try:
            deadline = _now() + max(0.5, float(timeout_s or 0.0))
            while _now() < deadline:
                snapshot = _esphome_media_player_entity_snapshot(token, entity_key=key_text)
                announcing = _media_player_snapshot_is_announcing(snapshot)
                if announcing:
                    started = True
                elif started:
                    completed = await _finalize_after_announcement(
                        token,
                        client,
                        module,
                        reason="announcement_finished",
                    )
                    if completed:
                        logger.info("[native-voice] media player announcement finished selector=%s", token)
                    return
                await asyncio.sleep(0.2)

            completed = await _finalize_after_announcement(token, client, module, reason="announcement_timeout")
            if completed:
                logger.info(
                    "[native-voice] media player announcement timeout finalize selector=%s timeout_s=%.2f",
                    token,
                    float(timeout_s),
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _native_debug(f"media player announcement monitor failed selector={token} error={exc}")

    runtime["announcement_task"] = asyncio.create_task(_timer())


def _schedule_announcement_timeout(selector: str, client: Any, module: Any, timeout_s: float) -> None:
    token = _text(selector)
    if not token:
        return

    runtime = _selector_runtime(token)

    async def _timer() -> None:
        try:
            await asyncio.sleep(max(0.2, float(timeout_s)))
            completed = await _finalize_after_announcement(token, client, module, reason="announcement_timeout")
            if completed:
                logger.info(
                    "[native-voice] announcement timeout finalize selector=%s timeout_s=%.2f",
                    token,
                    float(timeout_s),
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _native_debug(f"announcement timeout task failed selector={token} error={exc}")

    runtime["announcement_task"] = asyncio.create_task(_timer())


async def _queue_selector_audio_url(
    selector: str,
    url: str,
    *,
    text: str = "",
    timeout_s: float = 180.0,
    tts_kind: str = "",
    continue_conversation: bool = False,
    conversation_id: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    target_url = _text(url).strip()
    if not token:
        raise ValueError("selector is required")
    if not target_url:
        raise ValueError("url is required")

    module, import_error = _esphome_import()
    if import_error:
        raise RuntimeError(import_error)

    client_row = _esphome_client_row_snapshot_sync(token)
    client = client_row.get("client")
    if not bool(client_row.get("connected")) or client is None:
        raise RuntimeError(f"Satellite {token} is not connected")

    runtime = _selector_runtime(token)
    lock = runtime.get("lock")
    playback_id = uuid.uuid4().hex
    timeout = max(5.0, min(float(timeout_s or 0.0), 900.0))
    continue_flag = bool(continue_conversation)
    conversation_token = _text(conversation_id) or playback_id

    async with lock:
        active_session = runtime.get("session")
        if isinstance(active_session, VoiceSessionRuntime):
            raise RuntimeError(f"Satellite {token} is busy with an active voice session")
        _clear_streamed_tts_state(runtime)
        _cancel_announcement_wait(runtime)

    if not continue_flag:
        direct_error = ""
        try:
            direct_result = await _esphome_play_media_announcement(token, target_url)
        except Exception as exc:
            direct_result = {}
            direct_error = _text(exc) or exc.__class__.__name__
        if isinstance(direct_result, dict) and direct_result.get("entity_key"):
            entity_key = _text(direct_result.get("entity_key"))
            async with lock:
                _set_awaiting_announcement_state(
                    runtime,
                    session_id=playback_id,
                    kind="media_player_direct",
                    future=None,
                    timeout_s=timeout,
                )
                _cancel_announcement_wait(runtime)
                _schedule_media_player_announcement_monitor(
                    token,
                    client,
                    module,
                    entity_key=entity_key,
                    timeout_s=timeout,
                )
            logger.info(
                "[native-voice] media player announcement queued selector=%s playback_id=%s entity_key=%s timeout_s=%.2f url=%s",
                token,
                playback_id,
                entity_key,
                timeout,
                target_url,
            )
            return {
                "selector": token,
                "playback_id": playback_id,
                "timeout_s": timeout,
                "url": target_url,
                "continue_conversation": False,
                "conversation_id": "",
                "dispatch_mode": "media_player_announcement",
                "entity_key": entity_key,
            }
        if direct_error:
            _native_debug(
                f"media player announcement unavailable selector={token} error={direct_error}; falling back to voice event playback"
            )

    async with lock:
        active_session = runtime.get("session")
        if isinstance(active_session, VoiceSessionRuntime):
            raise RuntimeError(f"Satellite {token} is busy with an active voice session")
        _clear_streamed_tts_state(runtime)
        _cancel_announcement_wait(runtime)
        if continue_flag:
            runtime["pending_followup_conversation_id"] = conversation_token
            runtime["pending_followup_until_ts"] = _now() + timeout + float(DEFAULT_CONTINUED_CHAT_REUSE_SECONDS)
        _set_awaiting_announcement_state(
            runtime,
            session_id=playback_id,
            kind="response" if continue_flag else "external",
            future=None,
            timeout_s=timeout,
        )
        _schedule_announcement_timeout(token, client, module, timeout)

    try:
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_START", "RUN_START"), None)
        if continue_flag:
            await _esphome_send_event(client, module, ("VOICE_ASSISTANT_INTENT_START", "INTENT_START"), None)
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_INTENT_END", "INTENT_END"),
                {
                    "conversation_id": conversation_token,
                    "continue_conversation": "1",
                },
            )
        await _esphome_send_event(
            client,
            module,
            ("VOICE_ASSISTANT_TTS_START", "TTS_START"),
            {
                "text": _text(text) or "Playing audio.",
                **({"tts_kind": _text(tts_kind)} if _text(tts_kind) else {}),
            },
        )
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_END", "TTS_END"), {"url": target_url})
    except Exception:
        async with lock:
            if continue_flag:
                _clear_pending_followup(runtime)
            _cancel_announcement_wait(runtime)
            _clear_awaiting_announcement_state(runtime)
        with contextlib.suppress(Exception):
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=playback_id,
                reason="external_audio_queue_error",
                skip_if_active_session_mismatch=False,
            )
        raise

    logger.info(
        "[native-voice] external audio queued selector=%s playback_id=%s timeout_s=%.2f url=%s",
        token,
        playback_id,
        timeout,
        target_url,
    )
    return {
        "selector": token,
        "playback_id": playback_id,
        "timeout_s": timeout,
        "url": target_url,
        "continue_conversation": continue_flag,
        "conversation_id": conversation_token if continue_flag else "",
    }


def _transcript_is_low_signal(transcript: str) -> bool:
    text = _text(transcript).lower()
    if not text:
        return True
    words = re.findall(r"[a-z0-9']+", text)
    if not words:
        return True
    preserved = {"yes", "no", "stop", "cancel", "play", "pause", "next", "back"}
    if len(words) == 1 and words[0] in preserved:
        return False
    filler = {"um", "uh", "hmm", "mm", "huh", "er", "ah", "uhh", "umm", "mmm"}
    if len(words) == 1 and words[0] in filler:
        return True
    compact = "".join(words)
    return len(compact) < 3


def _continued_chat_retry_evidence(session: VoiceSessionRuntime, transcript: str = "") -> bool:
    if not isinstance(session, VoiceSessionRuntime):
        return False
    if not bool(getattr(session, "continued_chat_reopen", False)):
        return False

    transcript_text = _text(transcript)
    if transcript_text:
        return not _transcript_is_low_signal(transcript_text)

    audio_peak = (
        float(getattr(session, "audio_peak_dbfs", -120.0) or -120.0)
        if int(getattr(session, "audio_level_chunks", 0) or 0) > 0
        else -120.0
    )
    vad_peak = (
        float(getattr(session, "vad_audio_peak_dbfs", -120.0) or -120.0)
        if int(getattr(session, "vad_audio_level_chunks", 0) or 0) > 0
        else -120.0
    )
    peak = max(audio_peak, vad_peak)
    chunks = int(getattr(session, "audio_chunks", 0) or 0)
    audio_bytes = int(getattr(session, "audio_bytes", 0) or 0)
    return peak >= -42.0 and (chunks >= 8 or audio_bytes >= 12000)


def _continued_chat_retry_response_text(transcript: str = "") -> str:
    if _text(transcript):
        return "I only caught part of that. Could you say it again?"
    return "I didn't catch that. Could you say it again?"


def _voice_turn_has_low_speech_evidence(session: VoiceSessionRuntime, transcript: str) -> bool:
    if not isinstance(session, VoiceSessionRuntime):
        return False

    words = re.findall(r"[a-z0-9']+", _text(transcript).lower())
    if not words:
        return True

    is_brief_command = len(words) == 1 and words[0] in {"yes", "no", "stop", "cancel", "play", "pause", "next", "back"}
    speech_s = max(0.0, float(getattr(session, "speech_duration_s", 0.0) or 0.0))
    max_prob = max(0.0, float(getattr(session, "max_probability", 0.0) or 0.0))
    vad_peak = (
        float(getattr(session, "vad_audio_peak_dbfs", -120.0) or -120.0)
        if int(getattr(session, "vad_audio_level_chunks", 0) or 0) > 0
        else -120.0
    )

    if speech_s >= 0.35:
        return False
    if max_prob >= 0.50:
        return False
    if speech_s < 0.12 and max_prob < 0.25 and not is_brief_command:
        return True
    if len(words) >= 7 and vad_peak >= -36.0:
        return False
    if is_brief_command and (max_prob >= 0.12 or vad_peak >= -34.0):
        return False
    if len(words) <= 6 and speech_s < 0.22 and max_prob < 0.32 and vad_peak < -34.0:
        return True
    if speech_s < 0.10 and max_prob < 0.45 and vad_peak < -28.0:
        return True
    return False


def _apply_startup_preroll(session: VoiceSessionRuntime) -> int:
    if not isinstance(session, VoiceSessionRuntime):
        return 0
    if bool(session.startup_preroll_applied):
        return 0
    if not bool(session.startup_preroll_buffer):
        session.startup_preroll_applied = True
        return 0
    if int(session.startup_preroll_max_bytes or 0) <= 0:
        session.startup_preroll_buffer.clear()
        session.startup_preroll_applied = True
        return 0

    max_audio_bytes = int(session.max_audio_bytes or DEFAULT_MAX_AUDIO_BYTES)
    allowed = max(0, max_audio_bytes - int(session.audio_bytes or 0))
    applied = 0
    if allowed > 0:
        preroll = bytes(session.startup_preroll_buffer)[-allowed:]
        session.audio_buffer.extend(preroll)
        session.audio_bytes += len(preroll)
        applied = len(preroll)
    session.startup_preroll_buffer.clear()
    session.startup_preroll_applied = True
    return applied


def _mark_stt_stream_unhealthy(session: VoiceSessionRuntime, reason: str) -> None:
    reason_token = _text(reason) or "stream_unhealthy"
    if not bool(session.stt_stream_unhealthy):
        logger.warning(
            "[native-voice] STT stream unhealthy selector=%s session_id=%s reason=%s dropped_chunks=%s queue_max=%s",
            session.selector,
            session.session_id,
            reason_token,
            int(session.stt_queue_dropped_chunks or 0),
            int(session.stt_queue_max_chunks or 0),
        )
    session.stt_stream_unhealthy = True
    session.stt_stream_fallback_reason = reason_token


def _enqueue_stt_stream_item(
    session: VoiceSessionRuntime,
    item: Optional[bytes],
    *,
    reason: str = "",
) -> bool:
    queue = session.stt_queue
    if queue is None:
        return False
    terminal = item is None
    if bool(session.stt_stream_stop_requested) and not terminal:
        return False
    try:
        queue.put_nowait(item)
        if terminal:
            session.stt_stream_stop_requested = True
        return True
    except asyncio.QueueFull:
        if not terminal:
            session.stt_queue_dropped_chunks += 1
            if session.stt_queue_dropped_chunks == 1 or session.stt_queue_dropped_chunks % 5 == 0:
                _native_debug(
                    f"STT stream queue full selector={session.selector} session_id={session.session_id} "
                    f"dropped_chunks={session.stt_queue_dropped_chunks} max_chunks={session.stt_queue_max_chunks}"
                )
            if session.stt_queue_dropped_chunks >= _wyoming_stt_queue_drop_limit():
                _mark_stt_stream_unhealthy(session, reason or "stt_stream_queue_full")
        else:
            _mark_stt_stream_unhealthy(session, reason or "stt_stream_queue_full_on_stop")

        if bool(session.stt_stream_unhealthy) and not bool(session.stt_stream_stop_requested):
            with contextlib.suppress(Exception):
                queue.get_nowait()
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
                session.stt_stream_stop_requested = True
        return False


async def _process_voice_turn(session: VoiceSessionRuntime) -> Dict[str, Any]:
    from .. import speaker_id as esphome_speaker_id

    pending_enrollment = esphome_speaker_id.consume_pending_enrollment(session.selector)
    if pending_enrollment:
        try:
            enrollment = await run_speech(
                esphome_speaker_id.add_enrollment_sample,
                speaker_id=_text(pending_enrollment.get("speaker_id")),
                audio_bytes=bytes(session.audio_buffer),
                audio_format=dict(session.audio_format or {}),
                speech_s=float(session.speech_duration_s or 0.0),
            )
            speaker_name = _text(enrollment.get("speaker_name")) or _text(pending_enrollment.get("speaker_name")) or "speaker"
            sample_count = int(enrollment.get("sample_count") or 0)
            return {
                "transcript": "",
                "response_text": f"Saved a speaker sample for {speaker_name}. Total samples: {sample_count}.",
                "speaker_id_enrollment": True,
            }
        except Exception as exc:
            speaker_name = _text(pending_enrollment.get("speaker_name")) or "speaker"
            logger.exception(
                "[native-voice] speaker id enrollment failed selector=%s session_id=%s speaker=%s",
                session.selector,
                session.session_id,
                speaker_name,
            )
            _native_debug(
                f"speaker id enrollment failed selector={session.selector} session_id={session.session_id} "
                f"speaker={speaker_name!r} error={exc}"
            )
            return {
                "transcript": "",
                "response_text": f"I couldn't save a speaker sample for {speaker_name} yet. Check the Speaker ID status in Tater and try again.",
                "speaker_id_enrollment": True,
            }

    if bool(getattr(session, "no_speech_recovered_transcript", False)) and _text(session.stt_transcript):
        transcript = _text(session.stt_transcript)
    else:
        stt_started = time.monotonic()
        transcript = _text(await _native_transcribe_session_audio(session))
        session.stt_latency_ms = max(0.0, (time.monotonic() - stt_started) * 1000.0)
        if not bool(getattr(session, "stt_metric_recorded", False)):
            _voice_metrics_record_stt(
                session.selector,
                session.stt_backend_effective or session.stt_backend,
                session.stt_latency_ms,
            )
            session.stt_metric_recorded = True
    from .. import intercom as esphome_intercom

    if esphome_intercom.is_broadcast_wake_word(session.wake_word):
        return await esphome_intercom.handle_broadcast_voice_turn(
            selector=session.selector,
            transcript=transcript,
            audio_bytes=bytes(session.audio_buffer),
            audio_format=dict(session.audio_format or {}),
            speech_s=float(session.speech_duration_s or 0.0),
        )

    intercom_result = await esphome_intercom.handle_voice_turn(
        selector=session.selector,
        transcript=transcript,
        audio_bytes=bytes(session.audio_buffer),
        audio_format=dict(session.audio_format or {}),
        speech_s=float(session.speech_duration_s or 0.0),
    )
    if isinstance(intercom_result, dict):
        return intercom_result

    if not transcript:
        retry_text = (
            _continued_chat_retry_response_text(transcript)
            if _continued_chat_retry_evidence(session, transcript)
            else ""
        )
        if retry_text:
            logger.info(
                "[native-voice] continued-chat empty transcript retry selector=%s session_id=%s audio_chunks=%s bytes=%s audio_peak_dbfs=%.1f max_prob=%.3f",
                session.selector,
                session.session_id,
                int(session.audio_chunks),
                int(session.audio_bytes),
                float(session.audio_peak_dbfs or -120.0),
                float(session.max_probability or 0.0),
            )
            return {
                "transcript": "",
                "response_text": retry_text,
            }
        return {
            "transcript": "",
            "no_op": True,
            "no_op_reason": "empty_transcript",
        }

    if _voice_turn_has_low_speech_evidence(session, transcript):
        retry_text = (
            _continued_chat_retry_response_text(transcript)
            if _continued_chat_retry_evidence(session, transcript)
            else ""
        )
        if retry_text:
            logger.info(
                "[native-voice] continued-chat low-evidence retry selector=%s session_id=%s transcript=%r speech_s=%.2f max_prob=%.3f vad_peak_dbfs=%.1f",
                session.selector,
                session.session_id,
                transcript,
                float(session.speech_duration_s or 0.0),
                float(session.max_probability or 0.0),
                float(session.vad_audio_peak_dbfs or -120.0),
            )
            return {
                "transcript": transcript,
                "response_text": retry_text,
            }
        _native_debug(
            f"low-evidence transcript bypass selector={session.selector} session_id={session.session_id} "
            f"transcript={transcript!r} speech_s={float(session.speech_duration_s or 0.0):.2f} "
            f"max_prob={float(session.max_probability or 0.0):.3f} "
            f"vad_peak_dbfs={float(session.vad_audio_peak_dbfs or -120.0):.1f}"
        )
        return {
            "transcript": transcript,
            "no_op": True,
            "no_op_reason": "low_signal_transcript",
        }

    if _transcript_is_low_signal(transcript):
        _native_debug(
            f"low-signal transcript bypass selector={session.selector} session_id={session.session_id} transcript={transcript!r}"
        )
        return {
            "transcript": transcript,
            "no_op": True,
            "no_op_reason": "low_signal_transcript",
        }

    completeness = _transcript_completeness_assessment(transcript)
    word_count = len(re.findall(r"[a-z0-9']+", transcript.lower()))
    if (not bool(completeness.get("complete"))) and word_count <= 6:
        if (
            bool(getattr(session, "no_speech_recovered_partial", False))
            or bool(_text(session.wake_word))
            or _continued_chat_retry_evidence(session, transcript)
        ):
            logger.info(
                "[native-voice] partial transcript recovery selector=%s session_id=%s words=%s reason=%s",
                session.selector,
                session.session_id,
                word_count,
                _text(completeness.get("reason")) or "incomplete",
            )
            return {
                "transcript": transcript,
                "response_text": _continued_chat_retry_response_text(transcript),
            }
        _native_debug(
            f"clipped transcript bypass selector={session.selector} session_id={session.session_id} "
            f"reason={_text(completeness.get('reason'))} transcript={transcript!r}"
        )
        return {
            "transcript": transcript,
            "no_op": True,
            "no_op_reason": "clipped_ambiguous_transcript",
        }

    try:
        speaker_match = await run_speech(
            esphome_speaker_id.match_speaker_for_audio,
            audio_bytes=bytes(session.audio_buffer),
            audio_format=dict(session.audio_format or {}),
            speech_s=float(session.speech_duration_s or 0.0),
        )
    except Exception as exc:
        speaker_match = {"matched": False, "reason": "error"}
        _native_debug(
            f"speaker id error selector={session.selector} session_id={session.session_id} error={exc}"
        )

    session.speaker_match_reason = _text(speaker_match.get("reason"))
    if bool(speaker_match.get("matched")):
        session.speaker_id = _text(speaker_match.get("speaker_id"))
        session.speaker_name = _text(speaker_match.get("speaker_name"))
        session.speaker_score = float(speaker_match.get("score") or 0.0)
        if isinstance(session.context, dict):
            session.context["speaker_id"] = session.speaker_id
            session.context["speaker_name"] = session.speaker_name
            session.context["speaker_score"] = session.speaker_score
        _native_debug(
            f"speaker id match selector={session.selector} session_id={session.session_id} "
            f"speaker={session.speaker_name!r} score={session.speaker_score:.3f}"
        )
    elif _text(speaker_match.get("reason")) not in {"", "disabled", "too_short", "no_speakers"}:
        _native_debug(
            f"speaker id no-match selector={session.selector} session_id={session.session_id} "
            f"reason={_text(speaker_match.get('reason'))} score={float(speaker_match.get('score') or 0.0):.3f}"
        )

    try:
        from .. import emotion_id as esphome_emotion_id

        emotion_result = await run_speech(
            esphome_emotion_id.classify_emotion_for_audio,
            audio_bytes=bytes(session.audio_buffer),
            audio_format=dict(session.audio_format or {}),
            speech_s=float(session.speech_duration_s or 0.0),
        )
    except Exception as exc:
        emotion_result = {"detected": False, "reason": "error"}
        _native_debug(
            f"emotion id error selector={session.selector} session_id={session.session_id} error={exc}"
        )

    if bool(emotion_result.get("detected")):
        session.voice_emotion = _text(emotion_result.get("emotion"))
        session.voice_emotion_score = float(emotion_result.get("score") or 0.0)
        session.voice_emotion_prompt_hint = _text(emotion_result.get("prompt_hint"))
        if isinstance(session.context, dict):
            session.context["voice_emotion"] = session.voice_emotion
            session.context["voice_emotion_score"] = session.voice_emotion_score
            session.context["voice_emotion_prompt_hint"] = session.voice_emotion_prompt_hint
        _native_debug(
            f"emotion id detected selector={session.selector} session_id={session.session_id} "
            f"emotion={session.voice_emotion!r} score={session.voice_emotion_score:.3f} "
            f"prompt_hint={bool(session.voice_emotion_prompt_hint)}"
        )
    elif _text(emotion_result.get("reason")) not in {"", "disabled", "too_short"}:
        _native_debug(
            f"emotion id no-hit selector={session.selector} session_id={session.session_id} "
            f"reason={_text(emotion_result.get('reason'))}"
        )

    _native_debug(
        f"hydra turn start selector={session.selector} session_id={session.session_id} transcript_len={len(transcript)}"
    )
    hydra_started = time.monotonic()
    try:
        response_text = await _run_hydra_turn_for_voice(
            transcript=transcript,
            conv_id=_text(session.conversation_id) or session.session_id,
            session=session,
        )
    finally:
        session.hydra_latency_ms = max(0.0, (time.monotonic() - hydra_started) * 1000.0)
    _native_debug(
        f"hydra turn result selector={session.selector} session_id={session.session_id} "
        f"response_len={len(_text(response_text))} hydra_ms={float(session.hydra_latency_ms or 0.0):.1f}"
    )

    return {
        "transcript": transcript,
        "response_text": _text(response_text),
    }


async def _finalize_session(
    selector: str,
    client: Any,
    module: Any,
    *,
    session_id: str,
    abort: bool,
    reason: str,
) -> None:
    token = _text(selector)
    runtime = _selector_runtime(token)
    lock = runtime.get("lock")

    async with lock:
        session = runtime.get("session")
        if not isinstance(session, VoiceSessionRuntime):
            return
        if _text(session.session_id) != _text(session_id):
            return
        if session.processing:
            return
        session.processing = True
        _cancel_audio_stall_watch(runtime)
        runtime["session"] = None

    preroll_bytes = _apply_startup_preroll(session)
    if preroll_bytes > 0:
        _native_debug(
            f"esphome startup preroll applied at finalize selector={token} session_id={session.session_id} "
            f"bytes={preroll_bytes} preroll_s={float(session.startup_preroll_s or 0.0):.2f}"
        )
        if session.stt_queue is not None:
            _enqueue_stt_stream_item(session, bytes(session.audio_buffer[-preroll_bytes:]), reason="stt_stream_preroll_finalize")

    if session.stt_queue is not None:
        _enqueue_stt_stream_item(session, None, reason="session_finalize")
    if session.partial_stt_task is not None:
        session.partial_stt_task.cancel()
        with contextlib.suppress(Exception):
            await session.partial_stt_task
        session.partial_stt_task = None

    if abort:
        logger.info(
            "[native-voice] session aborted selector=%s session_id=%s reason=%s",
            token,
            _text(session_id),
            _text(reason) or "device_stopped",
        )
        with contextlib.suppress(Exception):
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=session_id,
                reason=_text(reason) or "device_stopped",
            )
        return

    no_speech_reason = _text(reason)
    if no_speech_reason in {"server_vad", "audio_stall_no_speech", "audio_stall_no_audio", "blank_wake_timeout"}:
        seg = None
        if isinstance(session.eou_engine, EouEngine):
            seg = session.eou_engine.segmenter
        seg_voice_seen = bool(seg.voice_seen) if isinstance(seg, SegmenterState) else False
        seg_chunks = int(seg.speech_chunks) if isinstance(seg, SegmenterState) else 0
        seg_speech_s = float(seg._speech_seconds()) if isinstance(seg, SegmenterState) else 0.0
        if (not seg_voice_seen) or seg_chunks <= 0 or seg_speech_s < 0.05:
            stt_started = time.monotonic()
            with contextlib.suppress(Exception):
                await _native_transcribe_session_audio(session)
                session.stt_latency_ms = max(0.0, (time.monotonic() - stt_started) * 1000.0)
                if not bool(session.stt_metric_recorded):
                    _voice_metrics_record_stt(
                        session.selector,
                        session.stt_backend_effective or session.stt_backend,
                        session.stt_latency_ms,
                    )
                    session.stt_metric_recorded = True

            recovered_transcript = _text(session.stt_transcript)
            seg_threshold = float(seg.threshold) if isinstance(seg, SegmenterState) else float(DEFAULT_SILERO_THRESHOLD)
            recovered_words = re.findall(r"[a-z0-9']+", recovered_transcript.lower())
            recovered_assessment = (
                _transcript_completeness_assessment(recovered_transcript) if recovered_transcript else {"complete": True}
            )
            recovered_complete = bool(recovered_assessment.get("complete", True))
            recovered_low_signal = _transcript_is_low_signal(recovered_transcript)
            is_brief_command = bool(
                len(recovered_words) == 1
                and recovered_words[0] in {"yes", "no", "stop", "cancel", "play", "pause", "next", "back"}
            )
            recovery_prob_threshold = (
                max(seg_threshold, 0.28) if is_brief_command else max(0.60, seg_threshold + 0.10)
            )
            recovery_low_prob_threshold = max(0.10, min(0.24, seg_threshold * 0.5))
            recovery_prob = float(session.max_probability or 0.0)
            recovery_audio_peak = (
                float(session.vad_audio_peak_dbfs or -120.0)
                if int(session.vad_audio_level_chunks or 0) > 0
                else -120.0
            )
            recovery_audio_evidence = recovery_audio_peak >= -38.0
            recovery_voice_evidence = (
                seg_speech_s >= 0.12
                or recovery_prob >= 0.25
                or (is_brief_command and recovery_prob >= 0.08)
            )
            recovery_by_transcript = (
                (recovery_voice_evidence and len(recovered_words) >= 2 and recovery_prob >= recovery_low_prob_threshold)
                or (recovery_voice_evidence and len(recovered_words) >= 5 and recovery_audio_evidence)
                or (is_brief_command and recovery_prob >= 0.08)
            )
            partial_recovery = (
                bool(recovered_transcript)
                and (not recovered_low_signal)
                and (not recovered_complete)
                and 2 <= len(recovered_words) <= 6
                and (
                    recovery_prob >= recovery_low_prob_threshold
                    or (len(recovered_words) >= 5 and recovery_audio_evidence)
                )
            )
            recovery_ok = (
                bool(recovered_transcript)
                and (not recovered_low_signal)
                and recovery_voice_evidence
                and (recovered_complete or len(recovered_words) >= 7 or partial_recovery)
                and (
                    recovery_prob >= float(recovery_prob_threshold)
                    or recovery_by_transcript
                    or partial_recovery
                )
            )
            followup_retry_ok = (
                (not recovery_ok)
                and _continued_chat_retry_evidence(session, recovered_transcript)
            )
            if recovery_ok or followup_retry_ok:
                session.no_speech_recovered_transcript = True
                session.no_speech_recovered_partial = bool(partial_recovery or followup_retry_ok)
                if followup_retry_ok:
                    session.followup_retry_prompt = True
                    session.followup_retry_reason = no_speech_reason
                _native_debug(
                    f"no-speech guard recovered transcript selector={token} session_id={session.session_id} "
                    f"reason={no_speech_reason} transcript_len={len(recovered_transcript)} words={len(recovered_words)} "
                    f"max_prob={session.max_probability:.3f} threshold={recovery_prob_threshold:.3f} "
                    f"vad_peak_dbfs={recovery_audio_peak:.1f} "
                    f"transcript_recovery={bool(recovery_by_transcript)} partial={bool(partial_recovery)} "
                    f"followup_retry={bool(followup_retry_ok)} "
                    f"complete={recovered_complete} complete_reason={_text(recovered_assessment.get('reason')) or '-'}"
                )
            else:
                outcome = _classify_no_op_outcome(
                    session,
                    reason=no_speech_reason,
                    transcript=recovered_transcript,
                )
                session.turn_outcome = outcome
                logger.info(
                    "[native-voice] no speech finalize selector=%s session_id=%s reason=%s outcome=%s speech_chunks=%s speech_s=%.2f audio_chunks=%s bytes=%s audio_peak_dbfs=%.1f vad_peak_dbfs=%.1f max_prob=%.3f transcript_len=%s transcript_words=%s complete=%s complete_reason=%s",
                    token,
                    _text(session_id),
                    no_speech_reason,
                    outcome,
                    seg_chunks,
                    seg_speech_s,
                    int(session.audio_chunks),
                    int(session.audio_bytes),
                    float(session.audio_peak_dbfs or -120.0),
                    recovery_audio_peak,
                    float(session.max_probability),
                    len(recovered_transcript),
                    len(recovered_words),
                    recovered_complete,
                    _text(recovered_assessment.get("reason")) or "-",
                )
                _voice_metrics_record_turn(
                    selector=token,
                    outcome=outcome,
                    reason=no_speech_reason,
                    speech_s=float(session.speech_duration_s or seg_speech_s or 0.0),
                    silence_s=float(session.silence_duration_s or 0.0),
                    turn_latency_ms=max(0.0, (_now() - float(session.started_ts or _now())) * 1000.0),
                )
                with contextlib.suppress(Exception):
                    from .. import intercom as esphome_intercom

                    await esphome_intercom.close_auto_reply_for_selector(
                        token,
                        conversation_id=_text(session.conversation_id),
                    )
                with contextlib.suppress(Exception):
                    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_VAD_END", "STT_VAD_END"), None)
                with contextlib.suppress(Exception):
                    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_END", "STT_END"), {"text": ""})
                if _text(session.wake_word):
                    with contextlib.suppress(Exception):
                        await _send_voice_no_text_recognized(client, module)
                with contextlib.suppress(Exception):
                    await _send_voice_run_end(
                        client,
                        module,
                        selector=token,
                        session_id=session_id,
                        reason=no_speech_reason,
                    )
                return

    try:
        turn_timeout_s = _get_float_setting(
            "VOICE_NATIVE_TURN_TIMEOUT_S",
            DEFAULT_VOICE_TURN_PROCESS_TIMEOUT_S,
            minimum=10.0,
            maximum=300.0,
        )
        with contextlib.suppress(Exception):
            await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_VAD_END", "STT_VAD_END"), None)

        async def _live_tool_progress(wait_text: str, _wait_payload: Optional[Dict[str, Any]] = None) -> None:
            await _play_live_tool_progress_for_session(
                client,
                module,
                selector=token,
                runtime=runtime,
                session=session,
                transcript=_text(session.stt_transcript),
                wait_text=wait_text,
                wait_payload=_wait_payload,
            )

        session.live_tool_progress_callback = _live_tool_progress
        try:
            result = await asyncio.wait_for(_process_voice_turn(session), timeout=turn_timeout_s)
        finally:
            session.live_tool_progress_callback = None
        transcript = _text(result.get("transcript"))
        no_op = bool(result.get("no_op"))
        if no_op:
            no_op_reason = _text(result.get("no_op_reason"))
            outcome = _classify_no_op_outcome(session, reason=no_op_reason, transcript=transcript)
            session.turn_outcome = outcome
            await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_END", "STT_END"), {"text": transcript})
            if _text(session.wake_word):
                with contextlib.suppress(Exception):
                    await _send_voice_no_text_recognized(client, module)
            logger.info(
                "[native-voice] no-op transcript finalize selector=%s session_id=%s reason=%s outcome=%s transcript_len=%s stt_ms=%.1f audio_chunks=%s bytes=%s audio_peak_dbfs=%.1f max_prob=%.3f speech_s=%.2f",
                token,
                _text(session.session_id),
                no_op_reason or "unknown",
                outcome,
                len(transcript),
                float(session.stt_latency_ms or 0.0),
                int(session.audio_chunks),
                int(session.audio_bytes),
                float(session.audio_peak_dbfs or -120.0),
                float(session.max_probability or 0.0),
                float(session.speech_duration_s or 0.0),
            )
            _voice_metrics_record_turn(
                selector=token,
                outcome=outcome,
                reason=no_op_reason or "unknown",
                speech_s=float(session.speech_duration_s or 0.0),
                silence_s=float(session.silence_duration_s or 0.0),
                turn_latency_ms=max(0.0, (_now() - float(session.started_ts or _now())) * 1000.0),
            )
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=session.session_id,
                reason=no_op_reason or "no_op",
            )
            return

        response_text = _text(result.get("response_text"))
        spoken_response_text = _sanitize_spoken_response_text(response_text)
        continue_conversation = False
        followup_cue = ""
        if _continued_chat_enabled():
            continue_conversation = bool(await _response_is_followup_question(response_text))
            if continue_conversation:
                followup_cue = await _generate_followup_cue(transcript, response_text)
                _voice_metrics_record_continued_chat_attempt(token)
        tts_backend_used = ""
        tts_backend_note = ""
        tts_url = ""
        tts_bytes = 0
        tts_mode = "stream"
        run_end_mode = "immediate"
        wait_for_announcement = False
        reply_playback_target = _session_reply_playback_target(token, session)
        reply_playback_on_device = reply_playback_target == reply_playback.REPLY_PLAYBACK_DEVICE

        pending_followup_until = 0.0
        async with lock:
            if continue_conversation:
                _arm_pending_followup(runtime, _text(session.conversation_id) or _text(session.session_id))
                pending_followup_until = _as_float(runtime.get("pending_followup_until_ts"), 0.0)
            else:
                _clear_pending_followup(runtime)
        if continue_conversation and pending_followup_until > 0.0:
            await _wake_arbitration_extend_room_claim_for_session(
                selector=token,
                session_id=session.session_id,
                until_ts=pending_followup_until + DEFAULT_WAKE_ARBITRATION_RELEASE_GRACE_S,
                reason="pending_followup",
            )

        streamed_tts = None
        if reply_playback_on_device and not continue_conversation and not bool(session.live_tool_progress_played):
            streamed_tts = await _start_chatterbox_streamed_tts_response(
                token,
                client,
                module,
                session=session,
                runtime=runtime,
                response_text=spoken_response_text,
                transcript=transcript,
            )
            if not streamed_tts:
                streamed_tts = await _start_experimental_streamed_tts_response(
                    token,
                    client,
                    module,
                    session=session,
                    runtime=runtime,
                    response_text=spoken_response_text,
                    transcript=transcript,
                )

        if streamed_tts:
            tts_backend_used = _text(streamed_tts.get("backend_used"))
            tts_backend_note = _text(streamed_tts.get("backend_note"))
            tts_url = _text(streamed_tts.get("first_url"))
            tts_bytes = int(streamed_tts.get("tts_bytes") or 0)
            tts_mode = _text(streamed_tts.get("tts_mode")) or "segmented_url"
            run_end_mode = _text(streamed_tts.get("run_end_mode")) or "announcement_streamed"
            wait_for_announcement = True
        else:
            tts_started = time.monotonic()
            tts_audio, tts_format, tts_backend_used, tts_backend_note = await _synthesize_spoken_response_audio(
                spoken_response_text,
                session=session,
                continue_conversation=continue_conversation,
                followup_cue=followup_cue,
            )
            session.tts_latency_ms = max(0.0, (time.monotonic() - tts_started) * 1000.0)
            _voice_metrics_record_tts(token, tts_backend_used or session.tts_backend_effective or session.tts_backend, session.tts_latency_ms)

            await _ensure_voice_stt_end_sent(client, module, session=session, transcript=transcript)
            if not bool(session.intent_active):
                await _ensure_voice_intent_active(client, module, session=session)
            await _send_voice_intent_end(
                client,
                module,
                session=session,
                continue_conversation=continue_conversation,
            )
            spoken_tts_text = _continued_chat_spoken_reply_text(
                spoken_response_text,
                continue_conversation=continue_conversation,
                followup_cue=followup_cue,
            )
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_TTS_START", "TTS_START"),
                {"text": spoken_tts_text},
            )

            if reply_playback_target == reply_playback.REPLY_PLAYBACK_SILENT:
                tts_mode = "silent"
                run_end_mode = "immediate"
                await asyncio.sleep(0.2)
                if continue_conversation:
                    marker_url = await _send_followup_reopen_marker(
                        token,
                        client,
                        module,
                        runtime=runtime,
                        session=session,
                        audio_format=tts_format,
                        reason="tts_silent",
                    )
                    if marker_url:
                        tts_url = "silent:followup"
                        wait_for_announcement = True
                        run_end_mode = "silent_followup_marker"
                    else:
                        await _send_voice_run_end(
                            client,
                            module,
                            selector=token,
                            session_id=session.session_id,
                            reason="tts_silent_marker_failed",
                        )
                else:
                    await _send_voice_run_end(
                        client,
                        module,
                        selector=token,
                        session_id=session.session_id,
                        reason="tts_silent",
                    )
            elif not reply_playback_on_device:
                external_result = await _play_reply_on_external_target(
                    selector=token,
                    session=session,
                    spoken_text=spoken_tts_text,
                    target=reply_playback_target,
                    audio_bytes=tts_audio,
                    audio_format=tts_format,
                    backend_used=tts_backend_used,
                )
                external_ok = bool(external_result.get("ok")) if isinstance(external_result, dict) else False
                if not external_ok:
                    logger.warning(
                        "[native-voice] external reply playback failed selector=%s target=%s error=%s",
                        token,
                        reply_playback_target,
                        _text((external_result or {}).get("error") if isinstance(external_result, dict) else ""),
                    )
                tts_url = f"external:{reply_playback_target}"
                tts_mode = "external"
                run_end_mode = "external_player"
                playback_delay_s = max(0.5, min(45.0, _estimate_pcm_duration_s(tts_audio, tts_format) + 0.35))
                await asyncio.sleep(playback_delay_s if external_ok else 0.25)
                if continue_conversation:
                    marker_url = await _send_followup_reopen_marker(
                        token,
                        client,
                        module,
                        runtime=runtime,
                        session=session,
                        audio_format=tts_format,
                        reason="tts_external" if external_ok else "tts_external_failed",
                    )
                    if marker_url:
                        wait_for_announcement = True
                        run_end_mode = "external_followup_marker"
                    else:
                        await _send_voice_run_end(
                            client,
                            module,
                            selector=token,
                            session_id=session.session_id,
                            reason="tts_external_marker_failed" if external_ok else "tts_external_failed",
                        )
                else:
                    await _send_voice_run_end(
                        client,
                        module,
                        selector=token,
                        session_id=session.session_id,
                        reason="tts_external" if external_ok else "tts_external_failed",
                    )
            else:
                tts_url = _store_tts_url(token, session.session_id, tts_audio, tts_format)
                if not tts_url:
                    tts_url = "voice-assistant://stream"

                wait_for_announcement = tts_url.startswith(("http://", "https://"))
                direct_queued = False
                if wait_for_announcement:
                    timeout_s = _run_end_timeout_s(tts_audio, tts_format)
                    if not continue_conversation:
                        try:
                            direct_result = await _esphome_play_media_announcement(token, tts_url)
                        except Exception as exc:
                            _native_debug(
                                f"response media player announcement unavailable selector={token} "
                                f"session_id={session.session_id} error={_text(exc) or exc.__class__.__name__}"
                            )
                        else:
                            entity_key = _text(direct_result.get("entity_key")) if isinstance(direct_result, dict) else ""
                            if entity_key:
                                async with lock:
                                    _set_awaiting_announcement_state(
                                        runtime,
                                        session_id=_text(session.session_id),
                                        kind="response",
                                        future=None,
                                        timeout_s=timeout_s,
                                    )
                                    _cancel_announcement_wait(runtime)
                                    _schedule_media_player_announcement_monitor(
                                        token,
                                        client,
                                        module,
                                        entity_key=entity_key,
                                        timeout_s=timeout_s,
                                    )
                                direct_queued = True
                                tts_mode = "media_player_url"
                                run_end_mode = "announcement"
                    if not direct_queued:
                        async with lock:
                            _set_awaiting_announcement_state(
                                runtime,
                                session_id=_text(session.session_id),
                                kind="response",
                                future=None,
                                timeout_s=timeout_s,
                            )
                            _cancel_announcement_wait(runtime)
                            _schedule_announcement_timeout(token, client, module, timeout_s)
                        _native_debug(
                            f"esphome awaiting announcement_finished selector={token} session_id={session.session_id} timeout_s={timeout_s:.2f}"
                        )
                        tts_mode = "url"
                        run_end_mode = "announcement"

                if not direct_queued:
                    await _esphome_send_event(client, module, ("VOICE_ASSISTANT_TTS_END", "TTS_END"), {"url": tts_url})

                if not wait_for_announcement:
                    await _send_voice_run_end(
                        client,
                        module,
                        selector=token,
                        session_id=session.session_id,
                        reason="tts_immediate",
                    )
                    tts_mode = "stream"
                    run_end_mode = "immediate"
            tts_bytes = len(tts_audio)

        session.turn_outcome = _VOICE_OUTCOME_VALID
        _voice_metrics_record_turn(
            selector=token,
            outcome=_VOICE_OUTCOME_VALID,
            reason=_text(reason) or "response",
            speech_s=float(session.speech_duration_s or 0.0),
            silence_s=float(session.silence_duration_s or 0.0),
            turn_latency_ms=max(0.0, (_now() - float(session.started_ts or _now())) * 1000.0),
        )
        logger.info(
            "[native-voice] session result selector=%s session_id=%s transcript_len=%s response_len=%s live_tool_progress=%s stt_ms=%.1f hydra_ms=%.1f tts_ms=%.1f turn_ms=%.1f tts_backend=%s tts_bytes=%s tts_mode=%s run_end_mode=%s continue_conversation=%s tts_url=%s",
            token,
            _text(session.session_id),
            len(transcript),
            len(response_text),
            "1" if bool(session.live_tool_progress_played) else "0",
            float(session.stt_latency_ms or 0.0),
            float(session.hydra_latency_ms or 0.0),
            float(session.tts_latency_ms or 0.0),
            max(0.0, (_now() - float(session.started_ts or _now())) * 1000.0),
            tts_backend_used,
            tts_bytes,
            tts_mode,
            run_end_mode,
            "1" if continue_conversation else "0",
            tts_url,
        )
        if tts_backend_note:
            logger.warning(
                "[native-voice] TTS backend note selector=%s session_id=%s detail=%s",
                token,
                _text(session.session_id),
                tts_backend_note,
            )

    except Exception as exc:
        is_timeout = isinstance(exc, asyncio.TimeoutError)
        msg = (
            f"Voice turn timed out after {float(locals().get('turn_timeout_s', DEFAULT_VOICE_TURN_PROCESS_TIMEOUT_S)):.1f}s"
            if is_timeout
            else _text(exc)
        )
        _voice_metrics_record_connection_event(token, event="error")
        logger.warning(
            "[native-voice] session finalize failed selector=%s session_id=%s error=%s",
            token,
            _text(session.session_id),
            msg,
        )

        code = "tater_pipeline_timeout" if is_timeout else "tater_pipeline_error"
        if "No transcript produced" in msg:
            code = "stt-no-text-recognized"

        with contextlib.suppress(Exception):
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": code, "message": msg or "Voice pipeline error"},
            )
        with contextlib.suppress(Exception):
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=session.session_id,
                reason="pipeline_error",
            )


async def _esphome_subscribe_voice_assistant(selector: str, client: Any, module: Any, *, api_audio_supported: bool) -> Callable[[], None]:
    subscribe = getattr(client, "subscribe_voice_assistant", None)
    if not callable(subscribe):
        raise RuntimeError("ESPHome client does not support subscribe_voice_assistant()")

    token = _text(selector)
    runtime = _selector_runtime(token)
    lock = runtime.get("lock")

    async def _handle_start(conversation_id: str, request_flags: int, audio_settings: Any, wake_word_phrase: Optional[str]) -> Optional[int]:
        handler_started_ts = _now()
        if not api_audio_supported:
            msg = "Device does not report API_AUDIO support. Voice Core currently requires API_AUDIO for stable operation."
            logger.warning("[native-voice] %s selector=%s", msg, token)
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "api_audio_not_supported", "message": msg},
            )
            await _send_voice_run_end(client, module, selector=token, reason="api_audio_not_supported")
            return None

        old = runtime.get("session")
        if isinstance(old, VoiceSessionRuntime):
            with contextlib.suppress(Exception):
                await _finalize_session(token, client, module, session_id=old.session_id, abort=True, reason="new_session_started")

        fmt = _audio_format_from_settings(audio_settings)
        sid = uuid.uuid4().hex
        explicit_conv = _text(conversation_id)
        wake_phrase = _text(wake_word_phrase)
        legacy_wake_listen_requested = False
        with contextlib.suppress(Exception):
            legacy_wake_listen_requested = bool(int(request_flags or 0) & 0x02)
        if legacy_wake_listen_requested and not wake_phrase:
            msg = "Legacy continuous wake-listen requests are no longer supported. Flash firmware that uses remote_wake_word and openwakeword_server_url."
            logger.warning("[native-voice] %s selector=%s", msg, token)
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "legacy-wake-listen-unsupported", "message": msg},
            )
            await _send_voice_run_end(client, module, selector=token, reason="legacy_wake_listen_unsupported")
            return None

        followup_conv = ""
        continued_chat_reopen = False
        async with lock:
            pending_conv = _text(runtime.get("pending_followup_conversation_id"))
            pending_until = _as_float(runtime.get("pending_followup_until_ts"), 0.0)
            pending_valid = bool(pending_conv) and _now() <= pending_until
            if wake_phrase:
                _clear_pending_followup(runtime)
            elif explicit_conv:
                if pending_valid and explicit_conv == pending_conv:
                    followup_conv = explicit_conv
                    continued_chat_reopen = True
                _clear_pending_followup(runtime)
            else:
                followup_conv = _claim_pending_followup(runtime)
                continued_chat_reopen = bool(followup_conv)
        conv = explicit_conv or followup_conv or sid
        if continued_chat_reopen:
            _native_debug(f"continued chat reuse selector={token} session_id={sid} conversation_id={conv}")

        satellite_row = _satellite_lookup(token)
        area_name = _satellite_area_name(satellite_row)
        client_row = _esphome_client_row_snapshot_sync(token)
        busy_claim = await _wake_arbitration_active_room_claim(selector=token, area_name=area_name)
        if busy_claim is not None:
            owner = _text(busy_claim.get("selector"))
            owner_session_id = _text(busy_claim.get("session_id"))
            owner_area = _text(busy_claim.get("area_name")) or area_name or "unknown"
            logger.info(
                "[native-voice] wake arbitration room busy selector=%s session_id=%s owner=%s owner_session_id=%s area=%s reason=%s",
                token,
                sid,
                owner,
                owner_session_id,
                owner_area,
                _text(busy_claim.get("reason")) or "active_session",
            )
            with contextlib.suppress(Exception):
                await _esphome_send_event(
                    client,
                    module,
                    ("VOICE_ASSISTANT_ERROR", "ERROR"),
                    {
                        "code": "duplicate_wake_up_detected",
                        "message": "Another nearby satellite is already handling this room.",
                    },
                )
            await _send_voice_run_end(
                client,
                module,
                selector=token,
                session_id=sid,
                reason="wake_arbitration_room_busy",
                skip_if_active_session_mismatch=False,
            )
            return None

        voice_cfg = _voice_config_snapshot()
        eou_cfg = voice_cfg.get("eou") if isinstance(voice_cfg.get("eou"), dict) else {}
        limits_cfg = voice_cfg.get("limits") if isinstance(voice_cfg.get("limits"), dict) else {}

        wake_word_session = bool(wake_phrase)
        capture_reopen_profile = bool(continued_chat_reopen or wake_word_session)

        try:
            eou_engine = _build_eou_engine(
                fmt,
                selector=token,
                reopen_capture_profile=capture_reopen_profile,
                cfg=voice_cfg,
            )
        except Exception as exc:
            msg = f"Failed to initialize VAD: {exc}"
            logger.warning("[native-voice] %s selector=%s", msg, token)
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "vad_unavailable", "message": msg},
            )
            await _send_voice_run_end(client, module, selector=token, reason="vad_init_failed")
            return None

        backend_ready = bool(getattr(eou_engine.backend, "_available", True))
        backend_error = _text(getattr(eou_engine.backend, "_load_error", ""))
        if not backend_ready:
            msg = f"{eou_engine.backend_name} VAD unavailable: {backend_error or 'unknown error'}"
            logger.warning("[native-voice] %s selector=%s", msg, token)
            await _esphome_send_event(
                client,
                module,
                ("VOICE_ASSISTANT_ERROR", "ERROR"),
                {"code": "vad_unavailable", "message": msg},
            )
            await _send_voice_run_end(client, module, selector=token, reason="vad_unavailable")
            return None

        start_ts = _now()
        startup_gate_s = _as_float(eou_cfg.get("startup_gate_s"), DEFAULT_STARTUP_GATE_S, minimum=0.0, maximum=2.0)
        if continued_chat_reopen:
            startup_gate_s = max(
                startup_gate_s,
                _as_float(
                    eou_cfg.get("continued_chat_reopen_startup_gate_s"),
                    DEFAULT_CONTINUED_CHAT_REOPEN_STARTUP_GATE_S,
                    minimum=0.0,
                    maximum=2.0,
                ),
            )
        elif wake_word_session:
            startup_gate_s = min(startup_gate_s, 0.05)
        vad_startup_ignore_s = 0.0
        if continued_chat_reopen:
            vad_startup_ignore_s = max(
                startup_gate_s,
                _as_float(
                    eou_cfg.get("continued_chat_reopen_vad_startup_ignore_s"),
                    DEFAULT_CONTINUED_CHAT_REOPEN_VAD_STARTUP_IGNORE_S,
                    minimum=0.0,
                    maximum=2.0,
                ),
            )
        elif wake_word_session:
            vad_startup_ignore_s = max(
                startup_gate_s,
                _as_float(
                    eou_cfg.get("wake_vad_startup_ignore_s"),
                    DEFAULT_WAKE_VAD_STARTUP_IGNORE_S,
                    minimum=0.0,
                    maximum=2.0,
                ),
            )
        max_audio_bytes = int(limits_cfg.get("max_audio_bytes") or DEFAULT_MAX_AUDIO_BYTES)
        if capture_reopen_profile:
            startup_preroll_s = _as_float(
                eou_cfg.get("continued_chat_reopen_preroll_s"),
                DEFAULT_CONTINUED_CHAT_REOPEN_PREROLL_S,
                minimum=0.0,
                maximum=1.0,
            )
            if wake_word_session and startup_gate_s > 0.0:
                startup_preroll_s = min(1.0, max(startup_preroll_s, startup_gate_s + 0.10))
        else:
            startup_preroll_s = 0.0
        startup_preroll_max_bytes = _audio_bytes_for_seconds(fmt, startup_preroll_s)
        segmenter = eou_engine.segmenter if isinstance(eou_engine, EouEngine) else None
        base_stall_s = _as_float(
            eou_cfg.get("audio_stall_timeout_s"),
            DEFAULT_AUDIO_STALL_TIMEOUT_S,
            minimum=0.4,
            maximum=20.0,
        )
        audio_stall_timeout_s = base_stall_s
        if isinstance(segmenter, SegmenterState):
            audio_stall_timeout_s = max(audio_stall_timeout_s, float(segmenter.silence_s) + 0.20)
        audio_stall_no_speech_timeout_s = _as_float(
            eou_cfg.get("audio_stall_no_speech_timeout_s"),
            DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S,
            minimum=1.0,
            maximum=30.0,
        )
        if capture_reopen_profile and isinstance(segmenter, SegmenterState):
            audio_stall_no_speech_timeout_s = max(
                audio_stall_no_speech_timeout_s,
                float(segmenter.no_speech_timeout_s) + 0.25,
            )
        blank_wake_timeout_s = _as_float(
            eou_cfg.get("blank_wake_timeout_s"),
            DEFAULT_BLANK_WAKE_TIMEOUT_S,
            minimum=1.0,
            maximum=20.0,
        )
        audio_input_gain = _as_float(
            eou_cfg.get("input_gain"),
            DEFAULT_AUDIO_INPUT_GAIN,
            minimum=0.5,
            maximum=16.0,
        )
        stt_cfg = voice_cfg.get("stt") if isinstance(voice_cfg.get("stt"), dict) else {}
        tts_cfg = voice_cfg.get("tts") if isinstance(voice_cfg.get("tts"), dict) else {}
        requested_stt_backend = _normalize_stt_backend(stt_cfg.get("backend"))
        effective_stt_backend, stt_backend_note = _resolve_stt_backend_selected(requested_stt_backend)
        requested_tts_backend = _normalize_tts_backend(tts_cfg.get("backend"))
        effective_tts_backend, tts_backend_note = _resolve_tts_backend_selected(requested_tts_backend)
        device_info = client_row.get("device_info") if isinstance(client_row.get("device_info"), dict) else {}
        satellite_name = _text(satellite_row.get("name"))
        device_info_name = _text(device_info.get("name"))
        device_friendly_name = _text(device_info.get("friendly_name"))
        device_mac_address = _text(device_info.get("mac_address"))
        device_bluetooth_mac_address = _text(device_info.get("bluetooth_mac_address"))
        device_name = (
            device_friendly_name
            or device_info_name
            or satellite_name
            or _text(getattr(client, "address", None))
            or token
        )
        session_context: Dict[str, Any] = {
            "device_id": token,
            "device_name": device_name,
            "satellite_selector": token,
            "satellite_host": _lower(satellite_row.get("host")) or _satellite_host_from_selector(token),
        }
        if satellite_name:
            session_context["satellite_name"] = satellite_name
        if device_info_name:
            session_context["device_info_name"] = device_info_name
        if device_friendly_name:
            session_context["device_friendly_name"] = device_friendly_name
        if device_mac_address:
            session_context["device_mac_address"] = device_mac_address
            session_context["mac_address"] = device_mac_address
        if device_bluetooth_mac_address:
            session_context["device_bluetooth_mac_address"] = device_bluetooth_mac_address
            session_context["bluetooth_mac_address"] = device_bluetooth_mac_address
        if area_name:
            session_context["area_name"] = area_name
            session_context["room_name"] = area_name
        session_context["reply_playback_target"] = reply_playback.resolve_reply_playback_target(
            satellite_row,
            client_row=client_row,
        )

        session = VoiceSessionRuntime(
            selector=token,
            session_id=sid,
            conversation_id=conv,
            wake_word=wake_phrase,
            audio_format=fmt,
            started_ts=start_ts,
            startup_gate_until_ts=start_ts + max(0.0, startup_gate_s),
            context=session_context,
            stt_backend=requested_stt_backend,
            stt_backend_effective=effective_stt_backend,
            tts_backend=requested_tts_backend,
            tts_backend_effective=effective_tts_backend,
            eou_engine=eou_engine,
            continued_chat_reopen=continued_chat_reopen,
            max_audio_bytes=max_audio_bytes,
            startup_gate_s=startup_gate_s,
            vad_startup_ignore_s=vad_startup_ignore_s,
            vad_startup_ignore_until_ts=start_ts + max(0.0, vad_startup_ignore_s),
            startup_preroll_s=startup_preroll_s,
            startup_preroll_max_bytes=startup_preroll_max_bytes,
            audio_stall_timeout_s=audio_stall_timeout_s,
            audio_stall_no_speech_timeout_s=audio_stall_no_speech_timeout_s,
            blank_wake_timeout_s=blank_wake_timeout_s,
            audio_input_gain=audio_input_gain,
        )
        async with lock:
            _cancel_announcement_wait(runtime)
            _cancel_audio_stall_watch(runtime)
            _clear_streamed_tts_state(runtime)
            _clear_awaiting_announcement_state(runtime)
            runtime["session"] = session

        await _register_wake_arbitration_candidate(
            selector=token,
            session=session,
            client=client,
            module=module,
            wake_phrase=wake_phrase,
            area_name=area_name,
        )
        if not wake_phrase:
            await _wake_arbitration_claim_room(
                selector=token,
                session=session,
                area_name=area_name,
                reason="continued_chat_reopen" if continued_chat_reopen else "active_session",
            )

        metrics_task = asyncio.create_task(
            _voice_metrics_record_session_start_async(
                selector=token,
                continued_chat_reopen=continued_chat_reopen,
                stt_fallback_used=bool(stt_backend_note) or (requested_stt_backend != effective_stt_backend),
                tts_fallback_used=bool(tts_backend_note) or (requested_tts_backend != effective_tts_backend),
            )
        )
        metrics_task.add_done_callback(_drain_task_result)

        _schedule_audio_stall_watch(token, client, module, session_id=sid)

        log_ts = _now()
        logger.info(
            "[native-voice] session start selector=%s conversation_id=%s session_id=%s wake_word=%s followup=%s capture_profile=%s area=%s stt=%s tts=%s vad=%s wake_engine=%s rate=%s width=%s ch=%s input_gain=%.2f gate_s=%.2f vad_ignore_s=%.2f preroll_s=%.2f setup_ms=%.1f ready_ms=%.1f",
            token,
            conv,
            sid,
            _text(wake_word_phrase),
            "1" if continued_chat_reopen else "0",
            "reopen" if capture_reopen_profile else "default",
            area_name,
            effective_stt_backend,
            effective_tts_backend,
            eou_engine.backend_name if isinstance(eou_engine, EouEngine) else "",
            "device",
            int(fmt.get("rate") or 0),
            int(fmt.get("width") or 0),
            int(fmt.get("channels") or 0),
            float(session.audio_input_gain or DEFAULT_AUDIO_INPUT_GAIN),
            float(session.startup_gate_s or 0.0),
            float(session.vad_startup_ignore_s or 0.0),
            float(session.startup_preroll_s or 0.0),
            max(0.0, (log_ts - handler_started_ts) * 1000.0),
            max(0.0, (log_ts - start_ts) * 1000.0),
        )
        if stt_backend_note:
            logger.warning(
                "[native-voice] STT backend fallback selector=%s selected=%s effective=%s reason=%s",
                token,
                requested_stt_backend,
                effective_stt_backend,
                stt_backend_note,
            )
        if tts_backend_note:
            logger.warning(
                "[native-voice] TTS backend fallback selector=%s selected=%s effective=%s reason=%s",
                token,
                requested_tts_backend,
                effective_tts_backend,
                tts_backend_note,
            )

        eou_engine = session.eou_engine if isinstance(session.eou_engine, EouEngine) else None
        if eou_engine is not None:
            seg = eou_engine.segmenter
            _native_debug(
                "esphome vad tuning "
                f"selector={token} backend={eou_engine.backend_name} "
                f"silence_s={seg.silence_s:.2f} timeout_s={seg.timeout_s:.2f} "
                f"no_speech_timeout_s={seg.no_speech_timeout_s:.2f} "
                f"threshold={seg.threshold:.2f} neg_threshold={seg.neg_threshold:.2f} "
                f"min_speech_frames={seg.min_speech_frames} min_silence_frames={seg.min_silence_frames} "
                f"silero_threshold={float(getattr(eou_engine.backend, 'threshold', DEFAULT_SILERO_THRESHOLD)):.2f} "
                f"webrtc_aggressiveness={int(getattr(eou_engine.backend, 'mode', DEFAULT_WEBRTC_VAD_AGGRESSIVENESS))} "
                f"startup_gate_s={float(session.startup_gate_s or 0.0):.2f} "
                f"vad_startup_ignore_s={float(session.vad_startup_ignore_s or 0.0):.2f} "
                f"input_gain={float(session.audio_input_gain or DEFAULT_AUDIO_INPUT_GAIN):.2f} "
                f"preroll_s={float(session.startup_preroll_s or 0.0):.2f} "
                f"stall_after_speech_s={float(session.audio_stall_timeout_s or 0.0):.2f} "
                f"stall_no_speech_s={float(session.audio_stall_no_speech_timeout_s or 0.0):.2f}"
            )

        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_RUN_START", "RUN_START"), None)
        await _esphome_send_event(client, module, ("VOICE_ASSISTANT_STT_START", "STT_START"), None)
        return 0

    async def _handle_audio(data: bytes, data2: Optional[bytes] = None) -> None:
        audio_bytes = bytes(data or b"")
        secondary_audio_bytes = bytes(data2 or b"")
        if not audio_bytes and not secondary_audio_bytes:
            return
        if not audio_bytes:
            _native_debug(
                f"esphome secondary-only audio ignored selector={token} bytes={len(secondary_audio_bytes)}"
            )
            return

        async with lock:
            session = runtime.get("session")
            if not isinstance(session, VoiceSessionRuntime):
                return
            if session.processing:
                return
            sid = session.session_id
            audio_format = session.audio_format
            eou_engine = session.eou_engine
            gate_ts = float(session.startup_gate_until_ts)
            vad_ignore_ts = float(session.vad_startup_ignore_until_ts or 0.0)
            input_gain = float(session.audio_input_gain or DEFAULT_AUDIO_INPUT_GAIN)

        width = int(audio_format.get("width") or DEFAULT_VOICE_SAMPLE_WIDTH)
        audio_bytes = _pcm_apply_gain(audio_bytes, sample_width=width, gain=input_gain)
        if not audio_bytes:
            return
        if secondary_audio_bytes:
            secondary_audio_bytes = _pcm_apply_gain(secondary_audio_bytes, sample_width=width, gain=input_gain)
        chunk_dbfs = _pcm_dbfs(audio_bytes, sample_width=width)
        secondary_chunk_dbfs = (
            _pcm_dbfs(secondary_audio_bytes, sample_width=width)
            if secondary_audio_bytes
            else None
        )

        now_ts = _now()
        async with lock:
            session_for_arbitration = runtime.get("session")
            if isinstance(session_for_arbitration, VoiceSessionRuntime) and session_for_arbitration.session_id == sid:
                _record_wake_arbitration_audio(session_for_arbitration, audio_bytes, audio_format)

        if now_ts < gate_ts:
            async with lock:
                s = runtime.get("session")
                if isinstance(s, VoiceSessionRuntime) and s.session_id == sid:
                    s.last_audio_ts = now_ts
                    s.dropped_startup_chunks += 1
                    if int(s.startup_preroll_max_bytes or 0) > 0:
                        s.startup_preroll_buffer.extend(audio_bytes)
                        overflow = len(s.startup_preroll_buffer) - int(s.startup_preroll_max_bytes or 0)
                        if overflow > 0:
                            del s.startup_preroll_buffer[:overflow]
                    if s.dropped_startup_chunks == 1:
                        _native_debug(
                            f"esphome startup audio gate selector={token} session_id={sid} "
                            f"gate_s={max(0.0, gate_ts - s.started_ts):.2f} "
                            f"preroll_s={float(s.startup_preroll_s or 0.0):.2f}"
                        )
            return

        metrics: Dict[str, Any] = {}
        should_finalize = False
        vad_startup_ignored = False
        if isinstance(eou_engine, EouEngine):
            vad_startup_ignored = vad_ignore_ts > 0.0 and now_ts < vad_ignore_ts
            if vad_startup_ignored:
                metrics = {
                    "backend": eou_engine.backend_name,
                    "binary_active": False,
                    "score": 0.0,
                    "chunk_score": 0.0,
                    "probability": 0.0,
                    "max_probability": 0.0,
                    "should_finalize": False,
                    "voice_seen": False,
                    "speech_chunks": 0,
                    "speech_s": 0.0,
                    "silence_s": 0.0,
                    "timed_out": False,
                    "in_command": False,
                    "vad_startup_ignored": True,
                }
            else:
                metrics = eou_engine.process(audio_bytes, audio_format, now_ts)
                should_finalize = bool(metrics.get("should_finalize"))

        async with lock:
            session = runtime.get("session")
            if not isinstance(session, VoiceSessionRuntime) or session.session_id != sid:
                return
            if session.processing:
                return

            audio_bytes = _apply_stt_noise_suppression(session, audio_bytes, audio_format, metrics)
            chunk_dbfs = _pcm_dbfs(audio_bytes, sample_width=width)
            session.last_audio_ts = now_ts
            if vad_startup_ignored:
                session.vad_startup_ignored_chunks += 1
                if session.vad_startup_ignored_chunks == 1:
                    _native_debug(
                        f"esphome startup vad ignore selector={token} session_id={sid} "
                        f"ignore_s={float(session.vad_startup_ignore_s or 0.0):.2f} "
                        f"remaining_s={max(0.0, float(session.vad_startup_ignore_until_ts or now_ts) - now_ts):.2f}"
                    )
            if float(session.first_audio_ts or 0.0) <= 0.0:
                session.first_audio_ts = now_ts
                logger.info(
                    "[native-voice] capture first audio selector=%s session_id=%s delay_ms=%.1f dropped_startup_chunks=%s gate_s=%.2f vad_ignore_s=%.2f preroll_s=%.2f chunk_dbfs=%s",
                    token,
                    sid,
                    max(0.0, (now_ts - float(session.started_ts or now_ts)) * 1000.0),
                    int(session.dropped_startup_chunks or 0),
                    float(session.startup_gate_s or 0.0),
                    float(session.vad_startup_ignore_s or 0.0),
                    float(session.startup_preroll_s or 0.0),
                    f"{float(chunk_dbfs):.1f}" if chunk_dbfs is not None else "-",
                )
            session.speech_duration_s = max(
                float(session.speech_duration_s or 0.0),
                float(metrics.get("speech_s") or 0.0),
            )
            session.silence_duration_s = float(metrics.get("silence_s") or 0.0)
            with contextlib.suppress(Exception):
                p = float(metrics.get("max_probability", metrics.get("probability", 0.0)) or 0.0)
                if p > session.max_probability:
                    session.max_probability = p
            if chunk_dbfs is not None:
                session.audio_level_chunks += 1
                if float(chunk_dbfs) > float(session.audio_peak_dbfs or -120.0):
                    session.audio_peak_dbfs = float(chunk_dbfs)
                if not vad_startup_ignored:
                    session.vad_audio_level_chunks += 1
                    if float(chunk_dbfs) > float(session.vad_audio_peak_dbfs or -120.0):
                        session.vad_audio_peak_dbfs = float(chunk_dbfs)
            if bool(metrics.get("voice_seen")) and float(session.first_vad_speech_ts or 0.0) <= 0.0:
                session.first_vad_speech_ts = now_ts
                logger.info(
                    "[native-voice] vad speech start selector=%s session_id=%s elapsed_ms=%.1f speech_chunks=%s speech_s=%.2f max_prob=%.3f peak_dbfs=%.1f",
                    token,
                    sid,
                    max(0.0, (now_ts - float(session.started_ts or now_ts)) * 1000.0),
                    int(metrics.get("speech_chunks") or 0),
                    float(metrics.get("speech_s") or 0.0),
                    float(metrics.get("max_probability", metrics.get("probability", 0.0)) or 0.0),
                    float(session.audio_peak_dbfs or -120.0),
                )

            max_audio_bytes = int(session.max_audio_bytes or DEFAULT_MAX_AUDIO_BYTES)

            if not session.capture_started:
                preroll_bytes = _apply_startup_preroll(session)
                if preroll_bytes > 0:
                    _native_debug(
                        f"esphome startup preroll applied selector={token} session_id={session.session_id} "
                        f"bytes={preroll_bytes} preroll_s={float(session.startup_preroll_s or 0.0):.2f}"
                    )
                if _normalize_stt_backend(session.stt_backend_effective) == "wyoming":
                    session.stt_queue_max_chunks = _wyoming_stt_queue_max_chunks()
                    session.stt_queue = asyncio.Queue(maxsize=session.stt_queue_max_chunks)
                    session.stt_task = asyncio.create_task(
                        _native_wyoming_stream_stt_task(
                            token=token,
                            session_id=session.session_id,
                            queue=session.stt_queue,
                            audio_format=session.audio_format,
                            language=session.language,
                            session_ref=session,
                        )
                    )
                elif _experimental_partial_stt_enabled():
                    session.partial_stt_task = asyncio.create_task(
                        _native_local_partial_stt_task(
                            token=token,
                            session_id=session.session_id,
                            session_ref=session,
                        )
                    )
                session.capture_started = True
                if session.stt_queue is not None and session.audio_buffer:
                    _enqueue_stt_stream_item(session, bytes(session.audio_buffer), reason="stt_stream_preroll_queue_full")
                _native_debug(
                    f"esphome capture started selector={token} session_id={session.session_id} "
                    f"mode={'stream' if session.stt_queue is not None else 'buffered'} stt={session.stt_backend_effective} "
                    f"queue_max={session.stt_queue_max_chunks or '-'}"
                )

            if session.audio_bytes + len(audio_bytes) <= max_audio_bytes:
                session.audio_buffer.extend(audio_bytes)
                session.audio_bytes += len(audio_bytes)
                session.audio_chunks += 1
                if session.stt_queue is not None:
                    _enqueue_stt_stream_item(session, audio_bytes, reason="stt_stream_queue_full")
            if secondary_audio_bytes and session.secondary_audio_bytes + len(secondary_audio_bytes) <= max_audio_bytes:
                first_secondary_chunk = session.secondary_audio_chunks <= 0
                session.secondary_audio_buffer.extend(secondary_audio_bytes)
                session.secondary_audio_bytes += len(secondary_audio_bytes)
                session.secondary_audio_chunks += 1
                if secondary_chunk_dbfs is not None:
                    session.secondary_audio_level_chunks += 1
                    if float(secondary_chunk_dbfs) > float(session.secondary_audio_peak_dbfs or -120.0):
                        session.secondary_audio_peak_dbfs = float(secondary_chunk_dbfs)
                if first_secondary_chunk:
                    logger.info(
                        "[native-voice] multi-channel audio detected selector=%s session_id=%s primary_bytes=%s secondary_bytes=%s secondary_dbfs=%s",
                        token,
                        sid,
                        len(audio_bytes),
                        len(secondary_audio_bytes),
                        f"{float(secondary_chunk_dbfs):.1f}" if secondary_chunk_dbfs is not None else "-",
                    )

            chunks = int(session.audio_chunks)
            if chunks in {1, 5, 10} or (chunks % 50 == 0):
                prob = metrics.get("probability", "-")
                peak_prob = metrics.get("max_probability", "-")
                secondary_note = (
                    f" secondary_chunks={session.secondary_audio_chunks} "
                    f"secondary_bytes={session.secondary_audio_bytes} "
                    f"secondary_peak_dbfs={float(session.secondary_audio_peak_dbfs or -120.0):.1f}"
                    if int(session.secondary_audio_chunks or 0) > 0
                    else ""
                )
                _native_debug(
                    "esphome audio "
                    f"selector={token} session_id={sid} chunks={chunks} bytes={session.audio_bytes} "
                    f"probability={prob} peak_probability={peak_prob} voice_seen={bool(metrics.get('voice_seen'))} "
                    f"in_command={bool(metrics.get('in_command'))} speech_s={metrics.get('speech_s', '-')} "
                    f"silence_s={metrics.get('silence_s', '-')} timed_out={bool(metrics.get('timed_out'))} "
                    f"vad_ignored={bool(metrics.get('vad_startup_ignored'))} "
                    f"chunk_dbfs={chunk_dbfs if chunk_dbfs is not None else '-'} "
                    f"peak_dbfs={float(session.audio_peak_dbfs or -120.0):.1f}"
                    f"{secondary_note}"
                )

            if session.completeness_hold_until_ts > 0.0 and float(metrics.get("silence_s") or 0.0) <= 0.05:
                session.completeness_hold_until_ts = 0.0
                session.completeness_hold_reason = ""
                session.completeness_hold_partial = ""

            if should_finalize:
                partial_text = _text(session.partial_transcript) or _text(session.stt_transcript)
                partial_fresh = (
                    bool(partial_text)
                    and session.partial_transcript_updated_ts > 0.0
                    and (now_ts - float(session.partial_transcript_updated_ts)) <= 3.0
                )
                if session.completeness_hold_until_ts > now_ts:
                    should_finalize = False
                elif session.completeness_hold_until_ts > 0.0:
                    session.finalize_reason_detail = (
                        f"completeness_hold_expired:{_text(session.completeness_hold_reason) or 'timeout'}"
                    )
                    session.completeness_hold_until_ts = 0.0
                    session.completeness_hold_reason = ""
                elif partial_fresh:
                    assessment = _transcript_completeness_assessment(partial_text)
                    allow_hold = _text(session.completeness_hold_partial) != partial_text
                    if (
                        allow_hold
                        and (not bool(assessment.get("complete")))
                        and float(metrics.get("speech_s") or 0.0) >= 0.30
                    ):
                        session.completeness_hold_until_ts = now_ts + float(DEFAULT_TRANSCRIPT_COMPLETENESS_EXTENSION_S)
                        session.completeness_hold_reason = _text(assessment.get("reason")) or "incomplete_partial"
                        session.completeness_hold_count += 1
                        session.completeness_hold_partial = partial_text
                        session.finalize_reason_detail = f"completeness_hold:{session.completeness_hold_reason}"
                        _native_debug(
                            f"transcript completeness hold selector={token} session_id={sid} "
                            f"reason={session.completeness_hold_reason} partial={partial_text!r}"
                        )
                        should_finalize = False

        if should_finalize and not bool(metrics.get("vad_error")):
            with contextlib.suppress(Exception):
                from .. import intercom as esphome_intercom

                async with lock:
                    active_session = runtime.get("session")
                    broadcast_hold = (
                        isinstance(active_session, VoiceSessionRuntime)
                        and active_session.session_id == sid
                        and esphome_intercom.is_broadcast_wake_word(active_session.wake_word)
                    )
                if broadcast_hold:
                    should_finalize = False

        if should_finalize:
            vad_error = bool(metrics.get("vad_error"))
            finalize_reason = "vad_unavailable" if vad_error else "server_vad"
            if vad_error:
                logger.error(
                    "[native-voice] VAD backend error selector=%s session_id=%s backend=%s error=%s",
                    token,
                    sid,
                    _text(metrics.get("backend")) or "-",
                    _text(metrics.get("error")) or "unknown",
                )
            _native_debug(
                f"server_vad finalize selector={token} session_id={sid} reason={finalize_reason} "
                f"silence_s={float(metrics.get('silence_s') or 0.0):.2f} speech_chunks={int(metrics.get('speech_chunks') or 0)} "
                f"speech_s={float(metrics.get('speech_s') or 0.0):.2f} timed_out={bool(metrics.get('timed_out'))}"
            )
            with contextlib.suppress(Exception):
                await _finalize_session(token, client, module, session_id=sid, abort=vad_error, reason=finalize_reason)

    async def _handle_stop(abort: bool) -> None:
        sid = ""
        chunks = 0
        total = 0
        finalize_abort = bool(abort)
        finalize_reason = "device_stop"
        async with lock:
            session = runtime.get("session")
            if isinstance(session, VoiceSessionRuntime):
                sid = session.session_id
                chunks = session.audio_chunks
                total = session.audio_bytes
                if bool(abort):
                    with contextlib.suppress(Exception):
                        from .. import intercom as esphome_intercom

                        if esphome_intercom.is_broadcast_wake_word(session.wake_word):
                            finalize_abort = False
                            finalize_reason = "push_to_intercom_release"
            _cancel_announcement_wait(runtime)
            _cancel_audio_stall_watch(runtime)
            _clear_awaiting_announcement_state(runtime)

        logger.info(
            "[native-voice] session stop selector=%s session_id=%s abort=%s finalize_abort=%s reason=%s chunks=%s bytes=%s",
            token,
            sid,
            bool(abort),
            finalize_abort,
            finalize_reason,
            chunks,
            total,
        )
        if sid:
            with contextlib.suppress(Exception):
                await _finalize_session(token, client, module, session_id=sid, abort=finalize_abort, reason=finalize_reason)

    async def _handle_announcement_finished(*_args: Any, **_kwargs: Any) -> None:
        now_ts = _now()
        ignore_stale = False
        kind = ""
        sid = ""
        age_s = 0.0
        async with lock:
            state = _awaiting_announcement_state(runtime)
            if state is not None:
                kind = _text(state.kind)
                sid = _text(state.session_id)
                started_ts = float(state.started_ts or 0.0)
                age_s = max(0.0, now_ts - started_ts) if started_ts > 0.0 else 0.0
                last_timeout_ts = float(runtime.get("last_announcement_timeout_ts") or 0.0)
                if last_timeout_ts > 0.0 and (now_ts - last_timeout_ts) <= 3.0 and age_s < 0.6:
                    ignore_stale = True
        if ignore_stale:
            _native_debug(
                f"stale announcement_finished ignored selector={token} session_id={sid or '-'} "
                f"kind={kind or '-'} age_s={age_s:.2f}"
            )
            return
        completed = await _finalize_after_announcement(token, client, module, reason="announcement_finished")
        if completed:
            logger.info("[native-voice] announcement finished selector=%s", token)

    subscribe_kwargs: Dict[str, Any] = {
        "handle_start": _handle_start,
        "handle_stop": _handle_stop,
    }

    with contextlib.suppress(Exception):
        sig = inspect.signature(subscribe)
        if "handle_audio" in sig.parameters:
            subscribe_kwargs["handle_audio"] = _handle_audio if api_audio_supported else None
        if "handle_announcement_finished" in sig.parameters:
            subscribe_kwargs["handle_announcement_finished"] = _handle_announcement_finished

    if "handle_audio" not in subscribe_kwargs:
        subscribe_kwargs["handle_audio"] = _handle_audio if api_audio_supported else None

    try:
        unsub = subscribe(**subscribe_kwargs)
    except TypeError:
        fallback = dict(subscribe_kwargs)
        fallback.pop("handle_announcement_finished", None)
        unsub = subscribe(**fallback)

    if inspect.isawaitable(unsub):
        unsub = await unsub
    if not callable(unsub):
        raise RuntimeError("subscribe_voice_assistant did not return unsubscribe callback")
    return unsub


# -------------------- Compatibility Helpers --------------------
# These small helpers are still used by `tater_voice.runtime` and the native
# ESPHome settings/runtime surface even though the older HTMLUI tab bridge
# has been removed.
def _format_ts_label(ts_value: Any) -> str:
    return _esphome_ui_helpers.format_ts_label(ts_value)


def _voice_ui_setting_fields() -> List[Dict[str, Any]]:
    from .. import settings as esphome_settings

    return esphome_settings.settings_fields()


def _satellite_host_from_selector(selector: str) -> str:
    token = _text(selector)
    if token.startswith("host:"):
        return _lower(token.split(":", 1)[1])
    return ""


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    body = payload if isinstance(payload, dict) else {}
    values = body.get("values")
    if isinstance(values, dict):
        return dict(values)
    out: Dict[str, Any] = {}
    for key, value in body.items():
        token = _text(key)
        if not token or token in {"id", "selector", "action"}:
            continue
        out[token] = value
    return out


def _payload_selector(payload: Dict[str, Any]) -> str:
    body = payload if isinstance(payload, dict) else {}
    return _text(body.get("selector")) or _text(body.get("id"))


def _run_async_blocking(coro: Any, timeout: float = 45.0) -> Any:
    """Run async coroutine from sync core action handlers."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        holder: Dict[str, Any] = {"done": False, "result": None, "error": None}

        def _worker() -> None:
            worker_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(worker_loop)
                holder["result"] = worker_loop.run_until_complete(asyncio.wait_for(coro, timeout=timeout))
            except Exception as exc:
                holder["error"] = exc
            finally:
                with contextlib.suppress(Exception):
                    worker_loop.close()
                holder["done"] = True

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        thread.join(timeout + 1.0)
        if not holder.get("done"):
            raise TimeoutError("Timed out waiting for async action")
        if holder.get("error") is not None:
            raise holder["error"]
        return holder.get("result")

    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))
