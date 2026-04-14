from __future__ import annotations

import asyncio
import base64
import contextlib
import importlib
import io
import json
import os
import re
import threading
import time
import uuid
import wave
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

from announcement_targets import split_announcement_targets
from helpers import redis_client
from speech_settings import (
    DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
    DEFAULT_KOKORO_MODEL,
    DEFAULT_KOKORO_VOICE,
    DEFAULT_PIPER_MODEL,
    DEFAULT_POCKET_TTS_MODEL,
    DEFAULT_POCKET_TTS_VOICE,
    DEFAULT_TTS_BACKEND,
    DEFAULT_TTS_PUBLIC_BASE_URL,
    DEFAULT_WYOMING_TTS_HOST,
    DEFAULT_WYOMING_TTS_PORT,
    DEFAULT_WYOMING_TTS_VOICE,
    normalize_announcement_tts_backend,
)

try:
    from wyoming.client import AsyncTcpClient
    from wyoming.tts import Synthesize
    from wyoming.audio import AudioStart as WyomingAudioStart, AudioChunk as WyomingAudioChunk, AudioStop as WyomingAudioStop
    from wyoming.error import Error as WyomingError
    from wyoming.info import Describe, Info
    try:
        from wyoming.tts import SynthesizeVoice
    except Exception:
        SynthesizeVoice = None
    WYOMING_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    AsyncTcpClient = None
    Synthesize = None
    SynthesizeVoice = None
    WyomingAudioStart = None
    WyomingAudioChunk = None
    WyomingAudioStop = None
    WyomingError = None
    Describe = None
    Info = None
    WYOMING_IMPORT_ERROR = str(exc)

try:
    from pykokoro import build_pipeline as build_kokoro_pipeline, PipelineConfig as KokoroPipelineConfig
    from pykokoro.tokenizer import TokenizerConfig as KokoroTokenizerConfig
    KOKORO_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    build_kokoro_pipeline = None
    KokoroPipelineConfig = None
    KokoroTokenizerConfig = None
    KOKORO_IMPORT_ERROR = str(exc)

try:
    from pocket_tts import TTSModel as PocketTTSModel
    POCKET_TTS_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    PocketTTSModel = None
    POCKET_TTS_IMPORT_ERROR = str(exc)

try:
    from piper import PiperVoice
    from piper.config import SynthesisConfig as PiperSynthesisConfig
    from piper.download_voices import download_voice as piper_download_voice
    PIPER_IMPORT_ERROR: Optional[str] = None
except Exception as exc:  # pragma: no cover - runtime dependency guard
    PiperVoice = None
    PiperSynthesisConfig = None
    piper_download_voice = None
    PIPER_IMPORT_ERROR = str(exc)


DEFAULT_TTS_MODEL_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "agent_lab", "models", "tts")
)
DEFAULT_KOKORO_PROVIDER = "cpu"
DEFAULT_WYOMING_TIMEOUT_SECONDS = 45.0
DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS = 0.24
DEFAULT_PIPER_PARAGRAPH_PAUSE_SECONDS = 0.46
DEFAULT_PIPER_TAIL_PAD_SECONDS = 0.18
DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS = 180.0
_PIPER_ABBREVIATIONS = {
    "dr",
    "mr",
    "mrs",
    "ms",
    "prof",
    "sr",
    "jr",
    "st",
    "vs",
    "etc",
    "e.g",
    "i.e",
}

_kokoro_pipeline_cache: Dict[Tuple[str, str], Any] = {}
_kokoro_pipeline_lock = threading.Lock()
_pocket_tts_model_cache: Dict[str, Any] = {}
_pocket_tts_model_lock = threading.Lock()
_piper_voice_cache: Dict[str, Any] = {}
_piper_voice_lock = threading.Lock()
_kokoro_ssmd_patch_applied = False
_runtime_tts_asset_store: Dict[str, Dict[str, Any]] = {}
_runtime_tts_asset_lock = threading.Lock()
_RUNTIME_TTS_ASSET_TTL_SECONDS = 900.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def normalize_tts_backend(value: Any) -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
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


def _as_float(value: Any, default: float, *, minimum: Optional[float] = None) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if minimum is not None:
        out = max(float(minimum), out)
    return out


def _append_pcm_silence(audio_bytes: bytes, audio_format: Dict[str, Any], *, seconds: float) -> bytes:
    pcm = bytes(audio_bytes or b"")
    if seconds <= 0:
        return pcm
    rate = _as_int(audio_format.get("rate"), 24000, minimum=1)
    width = _as_int(audio_format.get("width"), 2, minimum=1, maximum=4)
    channels = _as_int(audio_format.get("channels"), 1, minimum=1, maximum=8)
    frame_bytes = max(1, width * channels)
    silence_frames = max(1, int(round(rate * float(seconds))))
    return pcm + (b"\x00" * (silence_frames * frame_bytes))


def _split_piper_sentences(text: str) -> list[str]:
    prompt = _text(text)
    if not prompt:
        return []
    parts: list[str] = []
    start = 0
    length = len(prompt)
    i = 0
    while i < length:
        ch = prompt[i]
        if ch not in ".!?":
            i += 1
            continue
        if i + 1 < length and prompt[i + 1] in ".!?":
            i += 1
            continue
        if ch == "." and i > 0 and i + 1 < length and prompt[i - 1].isdigit() and prompt[i + 1].isdigit():
            i += 1
            continue
        j = i - 1
        while j >= start and (prompt[j].isalnum() or prompt[j] in "_-"):
            j -= 1
        token = prompt[j + 1 : i].lower()
        if ch == "." and (token in _PIPER_ABBREVIATIONS or (len(token) == 1 and token.isalpha())):
            i += 1
            continue
        k = i + 1
        while k < length and prompt[k] in "\"'”’)]}":
            k += 1
        if k < length and not prompt[k].isspace():
            i += 1
            continue
        segment = prompt[start:k].strip()
        if segment:
            parts.append(segment)
        while k < length and prompt[k].isspace():
            k += 1
        start = k
        i = k
    tail = prompt[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _build_piper_segment_plan(text: str) -> list[tuple[str, float]]:
    normalized = re.sub(r"\r\n?", "\n", _text(text))
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", normalized) if _text(part)]
    if not paragraphs:
        return []
    plan: list[tuple[str, float]] = []
    for paragraph_index, paragraph in enumerate(paragraphs):
        sentences = _split_piper_sentences(paragraph)
        if not sentences:
            continue
        last_sentence_index = len(sentences) - 1
        last_paragraph_index = len(paragraphs) - 1
        for sentence_index, sentence in enumerate(sentences):
            pause_seconds = 0.0
            if sentence_index < last_sentence_index:
                pause_seconds = DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS
            elif paragraph_index < last_paragraph_index:
                pause_seconds = DEFAULT_PIPER_PARAGRAPH_PAUSE_SECONDS
            plan.append((sentence, pause_seconds))
    return plan


def _synthesize_piper_segment_sync(voice: Any, prompt: str) -> Tuple[bytes, Dict[str, Any]]:
    audio_out = bytearray()
    sample_rate = 22050
    sample_width = 2
    sample_channels = 1
    syn_config = PiperSynthesisConfig()
    for chunk in voice.synthesize(prompt, syn_config=syn_config):
        audio_out.extend(chunk.audio_int16_bytes)
        sample_rate = int(getattr(chunk, "sample_rate", sample_rate) or sample_rate)
        sample_width = int(getattr(chunk, "sample_width", sample_width) or sample_width)
        sample_channels = int(getattr(chunk, "sample_channels", sample_channels) or sample_channels)
    return bytes(audio_out), {"rate": sample_rate, "width": sample_width, "channels": sample_channels}


def _tts_model_root() -> str:
    return os.path.expanduser(DEFAULT_TTS_MODEL_ROOT)


def _ensure_tts_model_root() -> str:
    root = _tts_model_root()
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _tts_backend_model_root(backend: str) -> str:
    base = _ensure_tts_model_root()
    token = normalize_tts_backend(backend)
    dirname_map = {
        "kokoro": "kokoro",
        "pocket_tts": "pocket-tts",
        "piper": "piper",
        "wyoming": "wyoming",
    }
    dirname = dirname_map.get(token, token or DEFAULT_TTS_BACKEND)
    return os.path.join(base, dirname)


def _ensure_tts_backend_model_root(backend: str) -> str:
    root = _tts_backend_model_root(backend)
    with contextlib.suppress(Exception):
        os.makedirs(root, exist_ok=True)
    return root


def _ha_headers(token: Any) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_text(token)}",
        "Content-Type": "application/json",
    }


def _voice_core_base_url() -> str:
    settings = redis_client.hgetall("voice_core_settings") or {}
    raw_port = _text(settings.get("bind_port")) or "8502"
    try:
        port = int(raw_port)
    except Exception:
        port = 8502
    if port < 1 or port > 65535:
        port = 8502
    return f"http://127.0.0.1:{port}"


def _cleanup_runtime_tts_assets(now_ts: Optional[float] = None) -> None:
    cutoff = float(now_ts if now_ts is not None else time.time())
    expired = []
    with _runtime_tts_asset_lock:
        for asset_id, row in list(_runtime_tts_asset_store.items()):
            expires_at = _as_float((row or {}).get("expires_at"), 0.0, minimum=0.0)
            if expires_at > 0 and expires_at <= cutoff:
                expired.append(asset_id)
        for asset_id in expired:
            _runtime_tts_asset_store.pop(asset_id, None)


def store_runtime_tts_wav(
    wav_bytes: bytes,
    *,
    content_type: str = "audio/wav",
    filename: str = "tts.wav",
    ttl_seconds: float = _RUNTIME_TTS_ASSET_TTL_SECONDS,
) -> str:
    payload = bytes(wav_bytes or b"")
    if not payload:
        raise RuntimeError("No TTS audio was generated.")
    now_ts = time.time()
    _cleanup_runtime_tts_assets(now_ts)
    asset_id = uuid.uuid4().hex
    expires_at = now_ts + max(30.0, float(ttl_seconds))
    with _runtime_tts_asset_lock:
        _runtime_tts_asset_store[asset_id] = {
            "bytes": payload,
            "content_type": _text(content_type) or "audio/wav",
            "filename": _text(filename) or "tts.wav",
            "created_at": now_ts,
            "expires_at": expires_at,
        }
    return asset_id


def get_runtime_tts_wav(asset_id: Any) -> Optional[Dict[str, Any]]:
    token = _text(asset_id)
    if not token:
        return None
    _cleanup_runtime_tts_assets()
    with _runtime_tts_asset_lock:
        row = _runtime_tts_asset_store.get(token)
        if not isinstance(row, dict):
            return None
        return dict(row)


def build_runtime_tts_asset_url(*, public_base_url: Any, asset_id: Any) -> str:
    base = _text(public_base_url).rstrip("/")
    token = _text(asset_id)
    if not base:
        raise RuntimeError(
            "Public Tater Audio URL is not configured. Set it in Tater Settings -> Hydra Models -> TTS."
        )
    if not token:
        raise RuntimeError("TTS runtime asset id is missing.")
    return f"{base}/api/speech/tts/runtime/{token}.wav"


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


def _float_audio_to_pcm16_bytes(audio: Any) -> bytes:
    np_mod = importlib.import_module("numpy")
    array = np_mod.asarray(audio, dtype=np_mod.float32)
    if array.ndim > 1:
        array = np_mod.squeeze(array)
    if array.ndim > 1:
        array = array.reshape(-1)
    if not array.size:
        return b""
    array = np_mod.clip(array, -1.0, 1.0)
    return (array * 32767.0).astype(np_mod.int16).tobytes()


@contextlib.contextmanager
def _temporary_env(overrides: Dict[str, Any]):
    previous: Dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            previous[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = _text(value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _native_wyoming_timeout_s() -> float:
    return float(DEFAULT_WYOMING_TIMEOUT_SECONDS)


def _voice_selection_to_value(selection: Dict[str, Any]) -> str:
    payload = {
        "name": _text(selection.get("name")),
        "language": _text(selection.get("language")),
        "speaker": _text(selection.get("speaker")),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _voice_selection_label(selection: Dict[str, Any]) -> str:
    name = _text(selection.get("name"))
    language = _text(selection.get("language"))
    speaker = _text(selection.get("speaker"))
    parts = [part for part in [name, language, speaker] if part]
    return " • ".join(parts) or "Default"


async def fetch_wyoming_tts_voice_options(
    *,
    host: str,
    port: Any,
    current_value: Any = "",
) -> Dict[str, Any]:
    if AsyncTcpClient is None or Describe is None or Info is None or WyomingError is None:
        raise RuntimeError(f"Wyoming describe dependency unavailable: {WYOMING_IMPORT_ERROR or 'unknown import error'}")

    resolved_host = _text(host) or DEFAULT_WYOMING_TTS_HOST
    resolved_port = _as_int(port, DEFAULT_WYOMING_TTS_PORT, minimum=1, maximum=65535)
    timeout = _native_wyoming_timeout_s()

    info_obj = None
    async with AsyncTcpClient(resolved_host, resolved_port) as client:
        await asyncio.wait_for(client.write_event(Describe().event()), timeout=timeout)
        while True:
            event = await asyncio.wait_for(client.read_event(), timeout=timeout)
            if event is None:
                break
            if WyomingError.is_type(event.type):
                err = WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS describe error: {err.text} ({err.code or 'unknown'})")
            if Info.is_type(event.type):
                info_obj = Info.from_event(event)
                break

    if info_obj is None:
        raise RuntimeError("Wyoming TTS did not return info after describe.")

    rows = [{"value": "", "label": "Default"}]
    seen = {""}
    tts_programs = getattr(info_obj, "tts", None)
    if not isinstance(tts_programs, list):
        tts_programs = []

    for program in tts_programs:
        program_name = _text(getattr(program, "name", None))
        voice_rows = getattr(program, "voices", None)
        if not isinstance(voice_rows, list):
            continue
        for voice in voice_rows:
            voice_name = _text(getattr(voice, "name", None))
            languages = [_text(item) for item in (getattr(voice, "languages", None) or []) if _text(item)]
            speakers = getattr(voice, "speakers", None)
            speaker_rows = speakers if isinstance(speakers, list) else []

            if speaker_rows:
                for speaker in speaker_rows:
                    selection = {
                        "name": voice_name,
                        "language": _text(languages[0]) if languages else "",
                        "speaker": _text(getattr(speaker, "name", None)),
                    }
                    value = _voice_selection_to_value(selection)
                    if not value or value in seen:
                        continue
                    seen.add(value)
                    label = _voice_selection_label(selection)
                    if program_name:
                        label = f"{label} • {program_name}"
                    rows.append({"value": value, "label": label})
                continue

            selection = {
                "name": voice_name,
                "language": _text(languages[0]) if languages else "",
                "speaker": "",
            }
            value = _voice_selection_to_value(selection)
            if not value or value in seen:
                continue
            seen.add(value)
            label = _voice_selection_label(selection)
            if program_name:
                label = f"{label} • {program_name}"
            rows.append({"value": value, "label": label})

    rows = [rows[0]] + sorted(rows[1:], key=lambda row: _text(row.get("label")).lower())
    current = _text(current_value)
    if current and current not in seen:
        rows.append({"value": current, "label": f"{current} (saved)"})
    return {"host": resolved_host, "port": resolved_port, "voices": rows, "count": max(0, len(rows) - 1)}


async def _wyoming_synthesize(
    text: str,
    *,
    host: str,
    port: int,
    voice_name: str = "",
) -> Tuple[bytes, Dict[str, Any]]:
    if (
        AsyncTcpClient is None
        or Synthesize is None
        or WyomingAudioStart is None
        or WyomingAudioChunk is None
        or WyomingAudioStop is None
        or WyomingError is None
    ):
        raise RuntimeError(f"Wyoming client dependency unavailable: {WYOMING_IMPORT_ERROR or 'unknown import error'}")

    prompt = _text(text)
    if not prompt:
        return b"", {}

    timeout = _native_wyoming_timeout_s()
    synth_event = None
    selected_name = _text(voice_name)
    if selected_name and SynthesizeVoice is not None:
        synth_event = Synthesize(text=prompt, voice=SynthesizeVoice(name=selected_name)).event()
    elif selected_name:
        with contextlib.suppress(Exception):
            synth_event = Synthesize(text=prompt, voice={"name": selected_name}).event()
    if synth_event is None:
        synth_event = Synthesize(text=prompt).event()

    audio_out = bytearray()
    audio_format: Dict[str, Any] = {}
    saw_start = False

    async with AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(synth_event), timeout=timeout)
        while True:
            event = await asyncio.wait_for(client.read_event(), timeout=timeout)
            if event is None:
                break
            if WyomingAudioStart.is_type(event.type):
                start = WyomingAudioStart.from_event(event)
                saw_start = True
                audio_format = {"rate": start.rate, "width": start.width, "channels": start.channels}
                continue
            if WyomingAudioChunk.is_type(event.type):
                chunk = WyomingAudioChunk.from_event(event)
                audio_out.extend(chunk.audio or b"")
                continue
            if WyomingAudioStop.is_type(event.type):
                break
            if WyomingError.is_type(event.type):
                err = WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS error: {err.text} ({err.code or 'unknown'})")

    if not saw_start:
        raise RuntimeError("Wyoming TTS did not emit audio-start")
    return bytes(audio_out), audio_format


def _load_kokoro_pipeline(model_id: str) -> Any:
    if build_kokoro_pipeline is None or KokoroPipelineConfig is None:
        raise RuntimeError(f"kokoro dependency unavailable: {KOKORO_IMPORT_ERROR or 'unknown import error'}")

    model_token = _text(model_id) or DEFAULT_KOKORO_MODEL
    variant, _, quality = model_token.partition(":")
    variant = variant or "v1.0"
    quality = quality or "q8"
    key = (variant, quality)

    with _kokoro_pipeline_lock:
        pipeline = _kokoro_pipeline_cache.get(key)
        if pipeline is None:
            root = _ensure_tts_backend_model_root("kokoro")
            onnx_backend_mod = importlib.import_module("pykokoro.onnx_backend")
            _patch_kokoro_ssmd_parser()

            def _kokoro_cache_path(folder: Optional[str] = None):
                base = root
                if folder:
                    base = os.path.join(base, folder)
                os.makedirs(base, exist_ok=True)
                return Path(base)

            setattr(onnx_backend_mod, "get_user_cache_path", _kokoro_cache_path)
            cfg = KokoroPipelineConfig(
                voice=DEFAULT_KOKORO_VOICE,
                model_source="huggingface",
                model_variant=variant,
                model_quality=quality,
                provider=DEFAULT_KOKORO_PROVIDER,
                tokenizer_config=(
                    KokoroTokenizerConfig(use_spacy=False)
                    if KokoroTokenizerConfig is not None
                    else None
                ),
            )
            pipeline = build_kokoro_pipeline(config=cfg, eager=True)
            _kokoro_pipeline_cache[key] = pipeline
        return pipeline


def _load_pocket_tts_model(model_id: str) -> Any:
    if PocketTTSModel is None:
        raise RuntimeError(f"pocket-tts dependency unavailable: {POCKET_TTS_IMPORT_ERROR or 'unknown import error'}")
    token = _text(model_id) or DEFAULT_POCKET_TTS_MODEL
    with _pocket_tts_model_lock:
        model = _pocket_tts_model_cache.get(token)
        if model is None:
            root = _ensure_tts_backend_model_root("pocket_tts")
            hf_root = os.path.join(root, "hf")
            os.makedirs(hf_root, exist_ok=True)
            with _temporary_env(
                {
                    "HF_HOME": hf_root,
                    "HF_HUB_CACHE": os.path.join(hf_root, "hub"),
                    "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub"),
                }
            ):
                model = PocketTTSModel.load_model(config=token)
            _pocket_tts_model_cache[token] = model
        return model


def _piper_model_paths(model_id: str) -> Tuple[str, str]:
    root = _ensure_tts_backend_model_root("piper")
    token = _text(model_id) or DEFAULT_PIPER_MODEL
    return os.path.join(root, f"{token}.onnx"), os.path.join(root, f"{token}.onnx.json")


def _load_piper_voice_model(model_id: str) -> Any:
    if PiperVoice is None or PiperSynthesisConfig is None or piper_download_voice is None:
        raise RuntimeError(f"piper dependency unavailable: {PIPER_IMPORT_ERROR or 'unknown import error'}")
    model_path, config_path = _piper_model_paths(model_id)
    backend_root = _ensure_tts_backend_model_root("piper")
    if not (os.path.isfile(model_path) and os.path.isfile(config_path)):
        piper_download_voice(_text(model_id) or DEFAULT_PIPER_MODEL, download_dir=Path(backend_root))
    cache_key = _text(model_path)
    with _piper_voice_lock:
        voice = _piper_voice_cache.get(cache_key)
        if voice is None:
            voice = PiperVoice.load(model_path=model_path, config_path=config_path, download_dir=backend_root)
            _piper_voice_cache[cache_key] = voice
        return voice


def _synthesize_kokoro_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    pipeline = _load_kokoro_pipeline(model_id)
    result = pipeline.run(_text(text), voice=_text(voice) or DEFAULT_KOKORO_VOICE)
    audio_bytes = _float_audio_to_pcm16_bytes(getattr(result, "audio", None))
    sample_rate = int(getattr(result, "sample_rate", 24000) or 24000)
    return audio_bytes, {"rate": sample_rate, "width": 2, "channels": 1}


def _synthesize_pocket_tts_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    prompt = _text(text)
    if not prompt:
        return b"", {}
    model = _load_pocket_tts_model(model_id)
    root = _ensure_tts_backend_model_root("pocket_tts")
    hf_root = os.path.join(root, "hf")
    os.makedirs(hf_root, exist_ok=True)
    with _temporary_env(
        {
            "HF_HOME": hf_root,
            "HF_HUB_CACHE": os.path.join(hf_root, "hub"),
            "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub"),
        }
    ):
        model_state = model.get_state_for_audio_prompt(_text(voice) or DEFAULT_POCKET_TTS_VOICE)
        audio_tensor = model.generate_audio(model_state, prompt)
    tensor = audio_tensor.detach().cpu().squeeze()
    audio_bytes = _float_audio_to_pcm16_bytes(tensor.numpy())
    return audio_bytes, {"rate": int(getattr(model, "sample_rate", 24000) or 24000), "width": 2, "channels": 1}


def _synthesize_piper_sync(text: str, model_id: str) -> Tuple[bytes, Dict[str, Any]]:
    prompt = _text(text)
    if not prompt:
        return b"", {}
    voice = _load_piper_voice_model(model_id)
    segment_plan = _build_piper_segment_plan(prompt) or [(prompt, 0.0)]
    audio_parts: list[bytes] = []
    audio_format: Dict[str, Any] = {"rate": 22050, "width": 2, "channels": 1}
    for segment_text, pause_seconds in segment_plan:
        segment_audio, segment_format = _synthesize_piper_segment_sync(voice, segment_text)
        if segment_audio:
            audio_parts.append(segment_audio)
            audio_format = dict(segment_format)
        if pause_seconds > 0:
            audio_parts.append(_append_pcm_silence(b"", audio_format, seconds=pause_seconds))
    padded = _append_pcm_silence(
        b"".join(audio_parts),
        audio_format,
        seconds=DEFAULT_PIPER_TAIL_PAD_SECONDS,
    )
    return padded, audio_format


def pcm_to_wav(audio_bytes: bytes, audio_format: Dict[str, Any]) -> bytes:
    pcm = bytes(audio_bytes or b"")
    if not pcm:
        return b""
    sample_rate = _as_int(audio_format.get("rate"), 24000, minimum=1)
    sample_width = _as_int(audio_format.get("width"), 2, minimum=1)
    channels = _as_int(audio_format.get("channels"), 1, minimum=1)
    with io.BytesIO() as buf:
        with wave.open(buf, "wb") as wavf:
            wavf.setnchannels(channels)
            wavf.setsampwidth(sample_width)
            wavf.setframerate(sample_rate)
            wavf.writeframes(pcm)
        return buf.getvalue()


async def synthesize_tts_wav(
    *,
    text: str,
    backend: str,
    model: str = "",
    voice: str = "",
    wyoming_host: str = "",
    wyoming_port: Any = None,
    wyoming_voice: str = "",
) -> bytes:
    selected_backend = normalize_tts_backend(backend)
    prompt = _text(text)
    if not prompt:
        raise RuntimeError("Preview text is required.")

    if selected_backend == "kokoro":
        audio_bytes, audio_format = await asyncio.to_thread(
            _synthesize_kokoro_sync,
            prompt,
            _text(model) or DEFAULT_KOKORO_MODEL,
            _text(voice) or DEFAULT_KOKORO_VOICE,
        )
        return pcm_to_wav(audio_bytes, audio_format)

    if selected_backend == "pocket_tts":
        audio_bytes, audio_format = await asyncio.to_thread(
            _synthesize_pocket_tts_sync,
            prompt,
            _text(model) or DEFAULT_POCKET_TTS_MODEL,
            _text(voice) or DEFAULT_POCKET_TTS_VOICE,
        )
        return pcm_to_wav(audio_bytes, audio_format)

    if selected_backend == "piper":
        audio_bytes, audio_format = await asyncio.to_thread(
            _synthesize_piper_sync,
            prompt,
            _text(model) or DEFAULT_PIPER_MODEL,
        )
        return pcm_to_wav(audio_bytes, audio_format)

    audio_bytes, audio_format = await _wyoming_synthesize(
        prompt,
        host=_text(wyoming_host) or DEFAULT_WYOMING_TTS_HOST,
        port=_as_int(wyoming_port, DEFAULT_WYOMING_TTS_PORT, minimum=1, maximum=65535),
        voice_name=_text(wyoming_voice) or _text(voice) or DEFAULT_WYOMING_TTS_VOICE,
    )
    return pcm_to_wav(audio_bytes, audio_format)


async def synthesize_preview_wav(
    *,
    text: str,
    backend: str,
    model: str = "",
    voice: str = "",
    wyoming_host: str = "",
    wyoming_port: Any = None,
    wyoming_voice: str = "",
) -> bytes:
    return await synthesize_tts_wav(
        text=text,
        backend=backend,
        model=model,
        voice=voice,
        wyoming_host=wyoming_host,
        wyoming_port=wyoming_port,
        wyoming_voice=wyoming_voice,
    )


def _homeassistant_play_media_sync(
    *,
    ha_base: str,
    token: str,
    players: list[str],
    media_url: str,
    media_content_type: str = "music",
) -> Dict[str, Any]:
    if not players:
        return {"ok": False, "sent_count": 0, "error": "No media players selected."}
    headers = _ha_headers(token)
    payload_template = {
        "media_content_id": media_url,
        "media_content_type": _text(media_content_type) or "music",
    }
    failures = []
    sent_count = 0
    for player in players:
        payload = dict(payload_template)
        payload["entity_id"] = _text(player)
        response = requests.post(
            f"{_text(ha_base).rstrip('/')}/api/services/media_player/play_media",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if response.status_code < 400:
            sent_count += 1
            continue
        failures.append(f"{player} (HTTP {response.status_code})")
    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Home Assistant play_media failed."}


def _voice_core_play_media_sync(
    *,
    selectors: list[str],
    source_url: str,
    audio_bytes: bytes | None = None,
    text: str = "",
    media_type: str = "audio/wav",
    filename: str = "tts.wav",
    timeout_s: float = DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    clean_selectors = [_text(item) for item in list(selectors or []) if _text(item)]
    if not clean_selectors:
        return {"ok": False, "sent_count": 0, "error": "No Voice Core satellites selected."}

    base_url = _voice_core_base_url().rstrip("/")
    payload_template = {
        "source_url": _text(source_url),
        "text": _text(text),
        "media_type": _text(media_type) or "audio/wav",
        "filename": _text(filename) or "tts.wav",
        "timeout_s": _as_float(timeout_s, DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS, minimum=30.0),
    }
    if isinstance(audio_bytes, (bytes, bytearray)) and audio_bytes:
        payload_template["audio_b64"] = base64.b64encode(bytes(audio_bytes)).decode("ascii")
    failures = []
    sent_count = 0

    for selector in clean_selectors:
        payload = dict(payload_template)
        payload["selector"] = selector
        try:
            response = requests.post(
                f"{base_url}/tater-ha/v1/voice/esphome/play",
                json=payload,
                timeout=90,
            )
            if response.status_code < 400:
                sent_count += 1
                continue
            detail = ""
            with contextlib.suppress(Exception):
                parsed = response.json()
                detail = _text(parsed.get("detail"))
            failures.append(f"{selector} ({detail or f'HTTP {response.status_code}'})")
        except Exception as exc:
            failures.append(f"{selector} ({exc})")

    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Voice Core playback failed."}


def _homeassistant_tts_speak_sync(
    *,
    ha_base: str,
    token: str,
    tts_entity: str,
    players: list[str],
    message: str,
) -> Dict[str, Any]:
    if not players:
        return {"ok": False, "sent_count": 0, "error": "No media players selected."}
    entity = _text(tts_entity)
    if not entity:
        return {"ok": False, "sent_count": 0, "error": "No Home Assistant TTS entity selected."}
    headers = _ha_headers(token)
    payload_template = {"entity_id": entity, "message": _text(message), "cache": True}
    failures = []
    sent_count = 0
    for player in players:
        payload = dict(payload_template)
        payload["media_player_entity_id"] = _text(player)
        primary = requests.post(
            f"{_text(ha_base).rstrip('/')}/api/services/tts/speak",
            headers=headers,
            json=payload,
            timeout=15,
        )
        if primary.status_code < 400:
            sent_count += 1
            continue
        fallback = requests.post(
            f"{_text(ha_base).rstrip('/')}/api/services/tts/piper_say",
            headers=headers,
            json=payload,
            timeout=15,
        )
        if fallback.status_code < 400:
            sent_count += 1
            continue
        failures.append(f"{player} (speak:{primary.status_code}, piper_say:{fallback.status_code})")
    if sent_count:
        result = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "Home Assistant TTS failed."}


async def speak_homeassistant_media_players(
    *,
    text: str,
    backend: str,
    ha_base: str,
    token: str,
    players: list[str],
    public_base_url: str = DEFAULT_TTS_PUBLIC_BASE_URL,
    tts_entity: str = "",
    model: str = "",
    voice: str = "",
    wyoming_host: str = "",
    wyoming_port: Any = None,
    wyoming_voice: str = "",
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
) -> Dict[str, Any]:
    prompt = _text(text)
    if not prompt:
        raise RuntimeError("TTS text is required.")
    selected_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    clean_players = [item for item in (_text(player) for player in list(players or [])) if item]
    if selected_backend == "homeassistant_api":
        result = await asyncio.to_thread(
            _homeassistant_tts_speak_sync,
            ha_base=_text(ha_base),
            token=_text(token),
            tts_entity=_text(tts_entity),
            players=clean_players,
            message=prompt,
        )
        result["backend"] = selected_backend
        return result

    wav_bytes = await synthesize_tts_wav(
        text=prompt,
        backend=selected_backend,
        model=model,
        voice=voice,
        wyoming_host=wyoming_host,
        wyoming_port=wyoming_port,
        wyoming_voice=wyoming_voice,
    )
    asset_id = store_runtime_tts_wav(
        wav_bytes,
        filename=f"tts-{selected_backend}.wav",
    )
    media_url = build_runtime_tts_asset_url(public_base_url=public_base_url, asset_id=asset_id)
    result = await asyncio.to_thread(
        _homeassistant_play_media_sync,
        ha_base=_text(ha_base),
        token=_text(token),
        players=clean_players,
        media_url=media_url,
        media_content_type="music",
    )
    result["backend"] = selected_backend
    result["media_url"] = media_url
    result["asset_id"] = asset_id
    result["bytes"] = len(wav_bytes or b"")
    return result


async def speak_announcement_targets(
    *,
    text: str,
    backend: str,
    ha_base: str,
    token: str,
    targets: list[str],
    public_base_url: str = DEFAULT_TTS_PUBLIC_BASE_URL,
    tts_entity: str = "",
    model: str = "",
    voice: str = "",
    wyoming_host: str = "",
    wyoming_port: Any = None,
    wyoming_voice: str = "",
    voice_core_backend: str = DEFAULT_TTS_BACKEND,
    voice_core_model: str = "",
    voice_core_voice: str = "",
    voice_core_wyoming_host: str = DEFAULT_WYOMING_TTS_HOST,
    voice_core_wyoming_port: Any = DEFAULT_WYOMING_TTS_PORT,
    voice_core_wyoming_voice: str = DEFAULT_WYOMING_TTS_VOICE,
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
) -> Dict[str, Any]:
    prompt = _text(text)
    if not prompt:
        raise RuntimeError("TTS text is required.")

    grouped = split_announcement_targets(targets)
    ha_players = list(grouped.get("homeassistant_media_players") or [])
    voice_core_selectors = list(grouped.get("voice_core_selectors") or [])
    target_count = len(ha_players) + len(voice_core_selectors)
    if target_count <= 0:
        return {"ok": False, "sent_count": 0, "error": "No announcement targets selected."}

    selected_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    warnings: list[str] = []
    sent_count = 0
    result: Dict[str, Any] = {
        "backend": selected_backend,
        "target_count": target_count,
        "homeassistant_target_count": len(ha_players),
        "voice_core_target_count": len(voice_core_selectors),
    }

    if selected_backend == "homeassistant_api":
        if ha_players:
            ha_result = await asyncio.to_thread(
                _homeassistant_tts_speak_sync,
                ha_base=_text(ha_base),
                token=_text(token),
                tts_entity=_text(tts_entity),
                players=ha_players,
                message=prompt,
            )
            result["homeassistant_sent_count"] = int(ha_result.get("sent_count") or 0)
            sent_count += int(ha_result.get("sent_count") or 0)
            warnings.extend([_text(item) for item in list(ha_result.get("warnings") or []) if _text(item)])
            if not ha_result.get("ok") and _text(ha_result.get("error")):
                warnings.append(_text(ha_result.get("error")))
        else:
            result["homeassistant_sent_count"] = 0

        if voice_core_selectors:
            fallback_backend = normalize_tts_backend(voice_core_backend or DEFAULT_TTS_BACKEND)
            try:
                voice_wav_bytes = await synthesize_tts_wav(
                    text=prompt,
                    backend=fallback_backend,
                    model=voice_core_model,
                    voice=voice_core_voice,
                    wyoming_host=voice_core_wyoming_host,
                    wyoming_port=voice_core_wyoming_port,
                    wyoming_voice=voice_core_wyoming_voice,
                )
                voice_result = await asyncio.to_thread(
                    _voice_core_play_media_sync,
                    selectors=voice_core_selectors,
                    source_url="",
                    audio_bytes=voice_wav_bytes,
                    text=prompt,
                    media_type="audio/wav",
                    filename=f"tts-{fallback_backend}.wav",
                    timeout_s=DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS,
                )
                result["voice_core_backend"] = fallback_backend
                result["voice_core_sent_count"] = int(voice_result.get("sent_count") or 0)
                sent_count += int(voice_result.get("sent_count") or 0)
                warnings.extend([_text(item) for item in list(voice_result.get("warnings") or []) if _text(item)])
                if not voice_result.get("ok") and _text(voice_result.get("error")):
                    warnings.append(_text(voice_result.get("error")))
            except Exception as exc:
                result["voice_core_sent_count"] = 0
                warnings.extend(
                    [
                        f"{selector} (Voice Core fallback TTS failed: {exc})"
                        for selector in voice_core_selectors
                    ]
                )
        else:
            result["voice_core_sent_count"] = 0

        if sent_count > 0:
            result["ok"] = True
            result["sent_count"] = sent_count
            if voice_core_selectors and _text(result.get("voice_core_backend")):
                result["backend"] = f"{selected_backend}+{_text(result.get('voice_core_backend'))}"
            if warnings:
                result["warnings"] = warnings
            return result
        result["ok"] = False
        result["sent_count"] = 0
        result["error"] = "; ".join(warnings) or "Home Assistant API TTS cannot play on the selected targets."
        return result

    wav_bytes = await synthesize_tts_wav(
        text=prompt,
        backend=selected_backend,
        model=model,
        voice=voice,
        wyoming_host=wyoming_host,
        wyoming_port=wyoming_port,
        wyoming_voice=wyoming_voice,
    )
    asset_id = store_runtime_tts_wav(
        wav_bytes,
        filename=f"tts-{selected_backend}.wav",
    )
    media_url = build_runtime_tts_asset_url(public_base_url=public_base_url, asset_id=asset_id)
    result["media_url"] = media_url
    result["asset_id"] = asset_id
    result["bytes"] = len(wav_bytes or b"")

    if ha_players:
        ha_result = await asyncio.to_thread(
            _homeassistant_play_media_sync,
            ha_base=_text(ha_base),
            token=_text(token),
            players=ha_players,
            media_url=media_url,
            media_content_type="music",
        )
        result["homeassistant_sent_count"] = int(ha_result.get("sent_count") or 0)
        sent_count += int(ha_result.get("sent_count") or 0)
        warnings.extend([_text(item) for item in list(ha_result.get("warnings") or []) if _text(item)])
        if not ha_result.get("ok") and _text(ha_result.get("error")):
            warnings.append(_text(ha_result.get("error")))

    if voice_core_selectors:
        voice_result = await asyncio.to_thread(
            _voice_core_play_media_sync,
            selectors=voice_core_selectors,
            source_url=media_url,
            audio_bytes=wav_bytes,
            text=prompt,
            media_type="audio/wav",
            filename=f"tts-{selected_backend}.wav",
            timeout_s=DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS,
        )
        result["voice_core_sent_count"] = int(voice_result.get("sent_count") or 0)
        sent_count += int(voice_result.get("sent_count") or 0)
        warnings.extend([_text(item) for item in list(voice_result.get("warnings") or []) if _text(item)])
        if not voice_result.get("ok") and _text(voice_result.get("error")):
            warnings.append(_text(voice_result.get("error")))

    result["sent_count"] = sent_count
    if sent_count > 0:
        result["ok"] = True
        result["sent_count"] = sent_count
        if warnings:
            result["warnings"] = warnings
        return result

    result["ok"] = False
    result["error"] = "; ".join(warnings) or "Announcement playback failed."
    return result
