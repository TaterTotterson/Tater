from __future__ import annotations

import asyncio
import contextlib
import gc
import importlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import wave
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

from .conversation import VoiceSessionRuntime
from runtime_executors import run_stt, run_tts
from tateros import integration_store as integration_store_module
from managed_tts import (
    DEFAULT_OMNIVOICE_TTS_MODEL,
    DEFAULT_QWEN_TTS_MODEL,
    clear_managed_tts_workers,
    synthesize_managed_tts_pcm,
)
from helpers import (
    _llama_cpp_native_free_port,
    _llama_cpp_native_server_bin,
    _macos_posix_spawn_kwargs,
)
from spud_link_models import allow_local_fallback as spud_link_allow_local_fallback
from spud_link_models import request_stt_async as spud_link_request_stt_async
from spud_link_models import request_tts_wav_async as spud_link_request_tts_wav_async
from spud_link_models import should_use_hub as spud_link_should_use_hub


def _vp():
    return sys.modules[__package__]


_LOCAL_STT_TRANSCRIBE_LOCK = asyncio.Lock()
_FASTER_WHISPER_MODEL_TRANSCRIBE_LOCK = threading.RLock()
_MLX_WHISPER_TRANSCRIBE_LOCK = threading.RLock()
_PARAKEET_ONNX_TRANSCRIBE_LOCK = threading.RLock()
_QWEN3_ASR_LLAMA_CPP_TRANSCRIBE_LOCK = threading.RLock()
_QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK = threading.RLock()
_POCKET_TTS_VOICE_STATE_CACHE_LIMIT = 8
_QWEN3_ASR_LLAMA_CPP_STATE: Dict[str, Any] = {
    "process": None,
    "base_url": "",
    "model_path": "",
    "mmproj_path": "",
    "api_key": "",
    "stdout_tail": [],
    "stderr_tail": [],
    "started_ts": 0.0,
    "gpu_layers": 0,
}


def huggingface_environment(overrides: Optional[Dict[str, Any]] = None, client: Any = None) -> Dict[str, Any]:
    return integration_store_module.huggingface_environment(overrides, client)


def _drain_background_task(task: asyncio.Task[Any]) -> None:
    with contextlib.suppress(BaseException):
        task.result()


async def _run_local_stt_thread(func: Any, *args: Any) -> str:
    vp = _vp()
    task = asyncio.create_task(run_stt(func, *args))
    try:
        timeout_s = vp._get_float_setting(
            "VOICE_NATIVE_LOCAL_STT_TIMEOUT_S",
            vp.DEFAULT_LOCAL_STT_TIMEOUT_SECONDS,
            minimum=5.0,
            maximum=180.0,
        )
        return await asyncio.wait_for(asyncio.shield(task), timeout=max(5.0, float(timeout_s)))
    except asyncio.TimeoutError:
        task.add_done_callback(_drain_background_task)
        raise TimeoutError(f"Local STT timed out after {float(timeout_s):.1f}s")
    except asyncio.CancelledError:
        task.add_done_callback(_drain_background_task)
        raise


def _resolve_stt_backend() -> Tuple[str, str]:
    vp = _vp()
    selected = vp._selected_stt_backend()
    ok, reason = vp._stt_backend_available(selected)
    if ok:
        return selected, ""
    if selected != "wyoming":
        for candidate in (vp.DEFAULT_STT_BACKEND, "faster_whisper", "parakeet_onnx", "mlx_whisper", "vosk"):
            fallback = vp._normalize_stt_backend(candidate)
            if fallback == selected or fallback == "wyoming":
                continue
            fallback_ok, _fallback_reason = vp._stt_backend_available(fallback)
            if fallback_ok:
                return fallback, f"{selected} unavailable: {reason}. Falling back to {fallback}."
    return selected, reason


def _tts_config_snapshot() -> Dict[str, Any]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    tts = cfg.get("tts") if isinstance(cfg.get("tts"), dict) else {}
    return tts if isinstance(tts, dict) else {}


def _selected_tts_backend(source: Optional[Dict[str, Any]] = None) -> str:
    vp = _vp()
    snapshot = source if isinstance(source, dict) else _tts_config_snapshot()
    return vp._normalize_tts_backend(snapshot.get("backend"))


def _tts_selection_from_values(values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    vp = _vp()
    values = values if isinstance(values, dict) else {}
    merged = vp._voice_settings_with_shared_speech(values)

    backend = vp._normalize_tts_backend(merged.get("VOICE_TTS_BACKEND"))
    model = vp._text(merged.get("VOICE_TTS_MODEL"))
    voice = vp._text(merged.get("VOICE_TTS_VOICE"))

    if backend == "kokoro":
        allowed_models = [row.get("value") for row in vp._kokoro_model_option_rows() if vp._text(row.get("value"))]
        if model not in allowed_models:
            model = vp.DEFAULT_KOKORO_MODEL if vp.DEFAULT_KOKORO_MODEL in allowed_models else vp._text(allowed_models[0] if allowed_models else "")
        allowed_voices = [vp._text(row.get("value")) for row in vp._kokoro_voice_option_rows(model_id=model) if vp._text(row.get("value"))]
        if voice not in allowed_voices:
            voice = vp.DEFAULT_KOKORO_VOICE if vp.DEFAULT_KOKORO_VOICE in allowed_voices else vp._text(allowed_voices[0] if allowed_voices else "")
    elif backend == "pocket_tts":
        model = model or vp.DEFAULT_POCKET_TTS_MODEL
        allowed_voices = set(vp.POCKET_TTS_PREDEFINED_VOICES.keys())
        voice = voice if voice in allowed_voices else vp.DEFAULT_POCKET_TTS_VOICE
    elif backend == "openai_compatible":
        model = model or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL
        voice = voice or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE
    elif backend == "chatterbox":
        model = ""
        voice = voice
    elif backend == "piper":
        model = model or vp.DEFAULT_PIPER_MODEL
        voice = ""
    elif backend == "qwen3_tts":
        model = model or DEFAULT_QWEN_TTS_MODEL
        voice = ""
    elif backend == "omnivoice":
        model = model or DEFAULT_OMNIVOICE_TTS_MODEL
        voice = ""
    else:
        model = ""
        voice = vp._text(merged.get("VOICE_WYOMING_TTS_VOICE")) or vp.DEFAULT_WYOMING_TTS_VOICE

    return {
        "backend": backend,
        "model": model,
        "voice": voice,
        "wyoming_host": vp._text(merged.get("VOICE_WYOMING_TTS_HOST")) or vp.DEFAULT_WYOMING_TTS_HOST,
        "wyoming_port": vp._as_int(merged.get("VOICE_WYOMING_TTS_PORT"), vp.DEFAULT_WYOMING_TTS_PORT, minimum=1, maximum=65535),
        "wyoming_voice": vp._text(merged.get("VOICE_WYOMING_TTS_VOICE")) or vp.DEFAULT_WYOMING_TTS_VOICE,
        "openai_base_url": vp._text(merged.get("VOICE_OPENAI_TTS_BASE_URL")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL,
        "openai_api_key": vp._text(merged.get("VOICE_OPENAI_TTS_API_KEY")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY,
        "chatterbox_base_url": vp._text(merged.get("VOICE_CHATTERBOX_TTS_BASE_URL")) or vp.DEFAULT_CHATTERBOX_TTS_BASE_URL,
        "chatterbox_voice_mode": vp._normalize_chatterbox_voice_mode(merged.get("VOICE_CHATTERBOX_TTS_VOICE_MODE")),
        "chatterbox_chunk_size": vp._normalize_chatterbox_chunk_size(merged.get("VOICE_CHATTERBOX_TTS_CHUNK_SIZE")),
        "chatterbox_temperature": merged.get("VOICE_CHATTERBOX_TTS_TEMPERATURE"),
        "chatterbox_exaggeration": merged.get("VOICE_CHATTERBOX_TTS_EXAGGERATION"),
        "chatterbox_cfg_weight": merged.get("VOICE_CHATTERBOX_TTS_CFG_WEIGHT"),
        "chatterbox_seed": merged.get("VOICE_CHATTERBOX_TTS_SEED"),
        "chatterbox_speed_factor": merged.get("VOICE_CHATTERBOX_TTS_SPEED_FACTOR"),
        "chatterbox_language": merged.get("VOICE_CHATTERBOX_TTS_LANGUAGE"),
        "chatterbox_streaming_enabled": vp._as_bool(merged.get("VOICE_CHATTERBOX_TTS_STREAMING_ENABLED"), False),
        "qwen_tts_clone_audio": vp._text(merged.get("VOICE_QWEN_TTS_CLONE_AUDIO")),
        "qwen_tts_clone_text": vp._text(merged.get("VOICE_QWEN_TTS_CLONE_TEXT")),
        "qwen_tts_language": vp._text(merged.get("VOICE_QWEN_TTS_LANGUAGE")) or "English",
        "qwen_tts_instruct": vp._text(merged.get("VOICE_QWEN_TTS_INSTRUCT")),
        "omnivoice_tts_clone_audio": vp._text(merged.get("VOICE_OMNIVOICE_TTS_CLONE_AUDIO")),
        "omnivoice_tts_clone_text": vp._text(merged.get("VOICE_OMNIVOICE_TTS_CLONE_TEXT")),
        "omnivoice_tts_language": vp._text(merged.get("VOICE_OMNIVOICE_TTS_LANGUAGE")) or "English",
        "omnivoice_tts_instruct": vp._text(merged.get("VOICE_OMNIVOICE_TTS_INSTRUCT")),
    }


def _tts_backend_available(backend: str) -> Tuple[bool, str]:
    vp = _vp()
    token = vp._normalize_tts_backend(backend)
    if token == "wyoming":
        ok = (
            vp.AsyncTcpClient is not None
            and vp.Synthesize is not None
            and vp.WyomingAudioStart is not None
            and vp.WyomingAudioChunk is not None
            and vp.WyomingAudioStop is not None
            and vp.WyomingError is not None
        )
        return ok, vp._text(vp.WYOMING_IMPORT_ERROR) or "wyoming dependency unavailable"
    if token == "openai_compatible":
        cfg = _tts_config_snapshot()
        base_url = vp._text((cfg.get("openai_compatible") or {}).get("base_url"))
        return bool(base_url), "OpenAI-compatible TTS base URL is not configured."
    if token == "chatterbox":
        cfg = _tts_config_snapshot()
        base_url = vp._text((cfg.get("chatterbox") or {}).get("base_url"))
        return bool(base_url), "Chatterbox TTS base URL is not configured."
    if token == "kokoro":
        if vp._kokoro_engine() == "torch":
            return (
                vp.KokoroTorchPipeline is not None,
                vp._text(vp.KOKORO_TORCH_IMPORT_ERROR) or "kokoro torch dependency unavailable",
            )
        return (
            vp.build_kokoro_pipeline is not None and vp.KokoroPipelineConfig is not None,
            vp._text(vp.KOKORO_IMPORT_ERROR) or "kokoro dependency unavailable",
        )
    if token == "pocket_tts":
        return vp.PocketTTSModel is not None, vp._text(vp.POCKET_TTS_IMPORT_ERROR) or "pocket-tts dependency unavailable"
    if token == "piper":
        return (
            vp.PiperVoice is not None and vp.PiperSynthesisConfig is not None and vp.piper_download_voice is not None,
            vp._text(vp.PIPER_IMPORT_ERROR) or "piper dependency unavailable",
        )
    if token in {"qwen3_tts", "omnivoice"}:
        return True, ""
    return False, f"unsupported TTS backend: {token}"


def _resolve_tts_backend(values: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    selected = _selected_tts_backend(_tts_selection_from_values(values))
    ok, reason = _tts_backend_available(selected)
    if ok:
        return selected, ""
    if selected != "wyoming":
        wyoming_ok, _wyoming_reason = _tts_backend_available("wyoming")
        if wyoming_ok:
            return "wyoming", f"{selected} unavailable: {reason}. Falling back to Wyoming."
    return selected, reason


def _load_faster_whisper_model() -> Any:
    vp = _vp()
    if vp.WhisperModel is None:
        raise RuntimeError(f"faster-whisper dependency unavailable: {vp.FASTER_WHISPER_IMPORT_ERROR or 'unknown import error'}")

    model_source = vp._resolve_faster_whisper_model_source()
    device = vp._faster_whisper_device()
    compute_type = vp._faster_whisper_compute_type()
    compute_type_label = vp._faster_whisper_compute_type_label()
    key = (model_source, device, compute_type)
    ctranslate2_cuda_devices = "unknown"
    with contextlib.suppress(Exception):
        ctranslate2_mod = importlib.import_module("ctranslate2")
        ctranslate2_cuda_devices = str(int(getattr(ctranslate2_mod, "get_cuda_device_count")()))

    with vp._faster_whisper_model_lock:
        model = vp._faster_whisper_model_cache.get(key)
        if model is None:
            kwargs: Dict[str, Any] = {"device": device, "compute_type": compute_type}
            if not os.path.isdir(model_source):
                kwargs["download_root"] = vp._ensure_stt_backend_model_root("faster_whisper")
            vp.logger.info(
                "[native-voice] faster-whisper model source=%s kind=%s device=%s compute_type=%s selected_compute=%s ctranslate2_cuda_devices=%s",
                model_source,
                "local" if os.path.isdir(model_source) else "alias",
                device,
                compute_type,
                compute_type_label,
                ctranslate2_cuda_devices,
            )
            with _temporary_env(huggingface_environment()):
                model = vp.WhisperModel(model_source, **kwargs)
            vp._faster_whisper_model_cache[key] = model
        return model


def clear_stt_model_caches(*, keep_backend: str = "") -> Dict[str, int]:
    """Release cached local STT models that are no longer selected."""
    vp = _vp()
    keep = vp._normalize_stt_backend(keep_backend) if vp._text(keep_backend) else ""
    cleared = {
        "faster_whisper": 0,
        "parakeet_onnx": 0,
        "qwen3_asr_llama_cpp": 0,
        "vosk": 0,
    }

    if keep != "faster_whisper":
        with _FASTER_WHISPER_MODEL_TRANSCRIBE_LOCK:
            with vp._faster_whisper_model_lock:
                cleared["faster_whisper"] = len(vp._faster_whisper_model_cache)
                vp._faster_whisper_model_cache.clear()

    if keep != "parakeet_onnx":
        with _PARAKEET_ONNX_TRANSCRIBE_LOCK:
            with vp._parakeet_onnx_model_lock:
                cleared["parakeet_onnx"] = len(vp._parakeet_onnx_model_cache)
                vp._parakeet_onnx_model_cache.clear()

    if keep != "vosk":
        with vp._vosk_model_lock:
            cleared["vosk"] = len(vp._vosk_model_cache)
            vp._vosk_model_cache.clear()

    if keep != "qwen3_asr_llama_cpp":
        with _QWEN3_ASR_LLAMA_CPP_TRANSCRIBE_LOCK:
            snapshot = _qwen3_asr_llama_cpp_runtime_snapshot()
            if snapshot.get("running"):
                cleared["qwen3_asr_llama_cpp"] = 1
            _shutdown_qwen3_asr_llama_cpp_server()

    if keep != "mlx_whisper":
        with contextlib.suppress(Exception):
            mlx_core = importlib.import_module("mlx.core")
            clear_cache = getattr(mlx_core, "clear_cache", None)
            if callable(clear_cache):
                clear_cache()

    if any(cleared.values()):
        gc.collect()
    vp.logger.info("[native-voice] cleared STT model caches keep=%s cleared=%s", keep or "-", cleared)
    return cleared


def _qwen3_asr_llama_cpp_repo() -> str:
    vp = _vp()
    return (
        vp._text(os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_REPO"))
        or vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_REPO
    )


def _qwen3_asr_llama_cpp_model_file() -> str:
    vp = _vp()
    return (
        vp._text(os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_MODEL_FILE"))
        or vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MODEL_FILE
    )


def _qwen3_asr_llama_cpp_mmproj_file() -> str:
    vp = _vp()
    return (
        vp._text(os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_MMPROJ_FILE"))
        or vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MMPROJ_FILE
    )


def _qwen3_asr_llama_cpp_model_paths(*, download: bool) -> Tuple[str, str]:
    vp = _vp()
    root = vp._ensure_stt_backend_model_root("qwen3_asr_llama_cpp")
    model_file = _qwen3_asr_llama_cpp_model_file()
    mmproj_file = _qwen3_asr_llama_cpp_mmproj_file()
    model_path = os.path.join(root, model_file)
    mmproj_path = os.path.join(root, mmproj_file)
    if download and (not os.path.isfile(model_path) or not os.path.isfile(mmproj_path)):
        huggingface_hub = importlib.import_module("huggingface_hub")
        vp.logger.info(
            "[native-voice] qwen3-asr llama.cpp download repo=%s model=%s mmproj=%s root=%s",
            _qwen3_asr_llama_cpp_repo(),
            model_file,
            mmproj_file,
            root,
        )
        with _temporary_env(
            huggingface_environment(
                {
                    "HF_HOME": root,
                    "HF_HUB_CACHE": os.path.join(root, "hub"),
                    "HUGGINGFACE_HUB_CACHE": os.path.join(root, "hub"),
                }
            )
        ):
            huggingface_hub.snapshot_download(
                repo_id=_qwen3_asr_llama_cpp_repo(),
                local_dir=root,
                allow_patterns=[model_file, mmproj_file],
            )
    missing = [path for path in (model_path, mmproj_path) if not os.path.isfile(path)]
    if missing:
        raise RuntimeError(
            "Qwen3-ASR llama.cpp model files are missing after download: "
            + ", ".join(missing)
        )
    return model_path, mmproj_path


def _qwen3_asr_llama_cpp_available() -> Tuple[bool, str]:
    server_bin = _llama_cpp_native_server_bin()
    if not server_bin:
        return False, "Tater's native llama-server binary was not found"
    try:
        model_path, mmproj_path = _qwen3_asr_llama_cpp_model_paths(download=False)
    except Exception:
        model_path = mmproj_path = ""
    if model_path and mmproj_path:
        return True, ""
    if importlib.util.find_spec("huggingface_hub") is None:
        return False, "huggingface-hub is required to download the Qwen3-ASR GGUF files"
    return True, ""


def _qwen3_asr_llama_cpp_runtime_snapshot() -> Dict[str, Any]:
    with _QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK:
        proc = _QWEN3_ASR_LLAMA_CPP_STATE.get("process")
        running = bool(proc is not None and callable(getattr(proc, "poll", None)) and proc.poll() is None)
        return {
            "running": running,
            "pid": int(getattr(proc, "pid", 0) or 0) if running else 0,
            "base_url": str(_QWEN3_ASR_LLAMA_CPP_STATE.get("base_url") or ""),
            "model_path": str(_QWEN3_ASR_LLAMA_CPP_STATE.get("model_path") or ""),
            "mmproj_path": str(_QWEN3_ASR_LLAMA_CPP_STATE.get("mmproj_path") or ""),
            "started_ts": float(_QWEN3_ASR_LLAMA_CPP_STATE.get("started_ts") or 0.0),
            "gpu_layers": max(0, int(_QWEN3_ASR_LLAMA_CPP_STATE.get("gpu_layers") or 0)),
            "stdout_tail": list(_QWEN3_ASR_LLAMA_CPP_STATE.get("stdout_tail") or [])[-20:],
            "stderr_tail": list(_QWEN3_ASR_LLAMA_CPP_STATE.get("stderr_tail") or [])[-20:],
        }


def _qwen3_asr_llama_cpp_drain_stream(stream: Any, target: List[str]) -> None:
    if stream is None:
        return
    try:
        for line in stream:
            text_line = str(line or "").rstrip()
            if not text_line:
                continue
            target.append(text_line)
            if len(target) > 100:
                del target[:-100]
    except Exception:
        return


def _qwen3_asr_llama_cpp_gpu_layers() -> int:
    vp = _vp()
    selected = vp.normalize_speech_acceleration(
        vp._voice_settings_with_shared_speech().get("VOICE_ACCELERATION")
    )
    if selected == "cpu":
        return 0
    return vp._as_int(
        os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_N_GPU_LAYERS"),
        999,
        minimum=0,
        maximum=999,
    )


def _qwen3_asr_llama_cpp_server_command(
    *,
    server_bin: str,
    model_path: str,
    mmproj_path: str,
    port: int,
    api_key: str,
) -> List[str]:
    vp = _vp()
    context_size = vp._as_int(
        os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_CONTEXT_SIZE"),
        vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_CONTEXT_SIZE,
        minimum=2048,
        maximum=131072,
    )
    return [
        server_bin,
        "--model",
        model_path,
        "--mmproj",
        mmproj_path,
        "--host",
        "127.0.0.1",
        "--port",
        str(int(port)),
        "--ctx-size",
        str(int(context_size)),
        "--n-gpu-layers",
        str(_qwen3_asr_llama_cpp_gpu_layers()),
        "--parallel",
        "1",
        "--alias",
        "qwen3-asr",
        "--api-key",
        api_key,
        "--no-ui",
        "--jinja",
        "--reasoning",
        "off",
        "--reasoning-budget",
        "0",
        "--reasoning-format",
        "none",
        "--no-context-shift",
        "--cache-ram",
        "0",
    ]


def _shutdown_qwen3_asr_llama_cpp_server() -> None:
    with _QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK:
        proc = _QWEN3_ASR_LLAMA_CPP_STATE.get("process")
        _QWEN3_ASR_LLAMA_CPP_STATE.update(
            {
                "process": None,
                "base_url": "",
                "model_path": "",
                "mmproj_path": "",
                "api_key": "",
                "stdout_tail": [],
                "stderr_tail": [],
                "started_ts": 0.0,
                "gpu_layers": 0,
            }
        )
    if proc is None or not callable(getattr(proc, "poll", None)) or proc.poll() is not None:
        return
    with contextlib.suppress(Exception):
        proc.terminate()
    try:
        proc.wait(timeout=10.0)
    except Exception:
        with contextlib.suppress(Exception):
            proc.kill()
        with contextlib.suppress(Exception):
            proc.wait(timeout=5.0)


def _load_qwen3_asr_llama_cpp_server() -> Dict[str, Any]:
    vp = _vp()
    with _QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK:
        proc = _QWEN3_ASR_LLAMA_CPP_STATE.get("process")
        if proc is not None and callable(getattr(proc, "poll", None)) and proc.poll() is None:
            return _qwen3_asr_llama_cpp_runtime_snapshot()

        model_path, mmproj_path = _qwen3_asr_llama_cpp_model_paths(download=True)
        server_bin = _llama_cpp_native_server_bin()
        if not server_bin:
            raise RuntimeError("Tater's native llama-server binary was not found")
        port = _llama_cpp_native_free_port()
        base_url = f"http://127.0.0.1:{int(port)}"
        api_key = os.urandom(24).hex()
        stdout_tail: List[str] = []
        stderr_tail: List[str] = []
        gpu_layers = _qwen3_asr_llama_cpp_gpu_layers()
        cmd = _qwen3_asr_llama_cpp_server_command(
            server_bin=server_bin,
            model_path=model_path,
            mmproj_path=mmproj_path,
            port=port,
            api_key=api_key,
        )
        vp.logger.info(
            "[native-voice] starting qwen3-asr llama.cpp server model=%s mmproj=%s gpu_layers=%s port=%s",
            model_path,
            mmproj_path,
            _qwen3_asr_llama_cpp_gpu_layers(),
            port,
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=dict(os.environ),
            **_macos_posix_spawn_kwargs(),
        )
        _QWEN3_ASR_LLAMA_CPP_STATE.update(
            {
                "process": proc,
                "base_url": base_url,
                "model_path": model_path,
                "mmproj_path": mmproj_path,
                "api_key": api_key,
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
                "started_ts": time.time(),
                "gpu_layers": gpu_layers,
            }
        )
        threading.Thread(
            target=_qwen3_asr_llama_cpp_drain_stream,
            args=(proc.stdout, stdout_tail),
            daemon=True,
            name="tater-qwen3-asr-stdout",
        ).start()
        threading.Thread(
            target=_qwen3_asr_llama_cpp_drain_stream,
            args=(proc.stderr, stderr_tail),
            daemon=True,
            name="tater-qwen3-asr-stderr",
        ).start()

        timeout_s = vp._as_float(
            os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_STARTUP_TIMEOUT_S"),
            vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_STARTUP_TIMEOUT_SECONDS,
            minimum=10.0,
            maximum=600.0,
        )
        deadline = time.monotonic() + timeout_s
        error = ""
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                error = f"llama-server exited with code {proc.returncode}"
                break
            try:
                response = requests.get(f"{base_url}/health", timeout=1.0)
                if response.status_code == 200:
                    return _qwen3_asr_llama_cpp_runtime_snapshot()
            except Exception:
                pass
            time.sleep(0.1)

        logs = " | ".join((stderr_tail or stdout_tail)[-10:])
        _shutdown_qwen3_asr_llama_cpp_server()
        detail = error or f"llama-server was not ready after {timeout_s:.1f}s"
        if logs:
            detail = f"{detail}. Logs: {logs}"
        raise RuntimeError(f"Qwen3-ASR llama.cpp startup failed: {detail}")


def _qwen3_asr_llama_cpp_response_text(payload: Any) -> str:
    vp = _vp()
    text = ""
    if isinstance(payload, dict):
        text = vp._text(payload.get("text"))
        if not text:
            choices = payload.get("choices")
            if isinstance(choices, list) and choices and isinstance(choices[0], dict):
                message = choices[0].get("message")
                if isinstance(message, dict):
                    text = vp._text(message.get("content"))
    else:
        text = vp._text(payload)
    if "<asr_text>" in text:
        text = text.split("<asr_text>", 1)[1]
    text = re.sub(r"^language\s+[^\s<]+\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<\|(?:im_end|endoftext)\|>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _pcm16_mono_wav_bytes(pcm16: bytes, *, rate: int = 16000) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(int(rate))
        wav_file.writeframes(bytes(pcm16 or b""))
    return buffer.getvalue()


def _transcribe_qwen3_asr_llama_cpp_sync(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    partial: bool = False,
) -> str:
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""
    with _QWEN3_ASR_LLAMA_CPP_TRANSCRIBE_LOCK:
        runtime = _load_qwen3_asr_llama_cpp_server()
        base_url = vp._text(runtime.get("base_url"))
        if not base_url:
            raise RuntimeError("Qwen3-ASR llama.cpp server did not report a base URL")
        max_tokens = vp._as_int(
            os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_MAX_TOKENS"),
            128 if partial else vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MAX_TOKENS,
            minimum=32,
            maximum=2048,
        )
        form: Dict[str, str] = {
            "model": "qwen3-asr",
            "response_format": "json",
            "temperature": "0",
            "max_tokens": str(int(max_tokens)),
        }
        lang = vp._text(language)
        if lang:
            form["language"] = lang
        prompt = vp._text(os.getenv("TATER_QWEN3_ASR_LLAMA_CPP_PROMPT"))
        if prompt:
            form["prompt"] = prompt
        response = requests.post(
            f"{base_url}/v1/audio/transcriptions",
            headers={
                "Authorization": "Bearer "
                + str(_QWEN3_ASR_LLAMA_CPP_STATE.get("api_key") or "")
            },
            data=form,
            files={"file": ("tater-voice.wav", _pcm16_mono_wav_bytes(pcm16), "audio/wav")},
            timeout=vp._get_float_setting(
                "VOICE_NATIVE_LOCAL_STT_TIMEOUT_S",
                vp.DEFAULT_LOCAL_STT_TIMEOUT_SECONDS,
                minimum=5.0,
                maximum=180.0,
            ),
        )
        if response.status_code >= 400:
            detail = _response_error_text(response)
            raise RuntimeError(
                f"Qwen3-ASR llama.cpp transcription failed with HTTP {response.status_code}: {detail}"
            )
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError("Qwen3-ASR llama.cpp returned invalid JSON") from exc
        return _qwen3_asr_llama_cpp_response_text(payload)


def _load_parakeet_onnx_model() -> Any:
    vp = _vp()
    if vp.OnnxASR is None:
        raise RuntimeError(
            f"Parakeet ONNX dependency unavailable: {vp.PARAKEET_ONNX_IMPORT_ERROR or 'unknown import error'}"
        )

    model_name = vp.DEFAULT_PARAKEET_ONNX_MODEL
    quantization = vp._parakeet_onnx_quantization()
    providers = tuple(vp._parakeet_onnx_providers())
    if not providers:
        raise RuntimeError("Parakeet ONNX cannot load because ONNX Runtime has no usable execution provider.")
    key = (model_name, quantization or "fp32", providers)

    with vp._parakeet_onnx_model_lock:
        model = vp._parakeet_onnx_model_cache.get(key)
        if model is None:
            root = vp._ensure_stt_backend_model_root("parakeet_onnx")
            suffix = f".{quantization}" if quantization else ""
            model_patterns = [
                "config.json",
                "vocab.txt",
                f"encoder-model{suffix}.onnx",
                f"encoder-model{suffix}.onnx.data",
                f"decoder_joint-model{suffix}.onnx",
                f"decoder_joint-model{suffix}.onnx.data",
            ]
            required_model_files = [
                "config.json",
                "vocab.txt",
                f"encoder-model{suffix}.onnx",
                f"decoder_joint-model{suffix}.onnx",
            ]
            if not quantization:
                required_model_files.append("encoder-model.onnx.data")
            vp.logger.info(
                "[native-voice] parakeet-onnx model=%s quantization=%s providers=%s root=%s",
                model_name,
                quantization or "fp32",
                ",".join(providers),
                root,
            )
            with _temporary_env(
                huggingface_environment(
                    {
                        "HF_HOME": root,
                        "HF_HUB_CACHE": os.path.join(root, "hub"),
                        "HUGGINGFACE_HUB_CACHE": os.path.join(root, "hub"),
                    }
                )
            ):
                snapshot_root = root
                if not all(os.path.isfile(os.path.join(root, filename)) for filename in required_model_files):
                    huggingface_hub = importlib.import_module("huggingface_hub")
                    snapshot_root = huggingface_hub.snapshot_download(
                        repo_id=vp.DEFAULT_PARAKEET_ONNX_REPO,
                        local_dir=root,
                        allow_patterns=model_patterns,
                    )
                model = vp.OnnxASR.load_model(
                    model_name,
                    snapshot_root,
                    quantization=quantization,
                    providers=list(providers),
                )
            vp._parakeet_onnx_model_cache[key] = model
        return model


def _load_vosk_model() -> Any:
    vp = _vp()
    if vp.VoskModel is None:
        raise RuntimeError(f"vosk dependency unavailable: {vp.VOSK_IMPORT_ERROR or 'unknown import error'}")

    model_path = vp._resolve_vosk_model_path()
    if not os.path.isdir(model_path) or not vp._looks_like_vosk_model_dir(model_path):
        raise RuntimeError(f"Vosk STT selected but no extracted model was found under {vp._stt_backend_model_root('vosk')}")

    with vp._vosk_model_lock:
        model = vp._vosk_model_cache.get(model_path)
        if model is None:
            vp.logger.info("[native-voice] vosk model source=%s", model_path)
            model = vp.VoskModel(model_path)
            vp._vosk_model_cache[model_path] = model
        return model


def _transcribe_faster_whisper_sync(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    partial: bool = False,
    wait_for_model: bool = False,
) -> str:
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    np_mod = importlib.import_module("numpy")
    audio_np = np_mod.frombuffer(pcm16, dtype=np_mod.int16).astype(np_mod.float32) / 32768.0
    model = _load_faster_whisper_model()
    parts = []
    beam_size = vp._faster_whisper_beam_size(partial=bool(partial))
    initial_prompt = "" if bool(partial) else vp._faster_whisper_initial_prompt()
    # Faster Whisper is stateless for a complete audio buffer, but the cached
    # model object may not be safe to decode from multiple worker threads at
    # once. Partial STT cancellation can leave a worker running, so this lock
    # protects the actual model call and segment iteration.
    if bool(partial) and not bool(wait_for_model):
        acquired = _FASTER_WHISPER_MODEL_TRANSCRIBE_LOCK.acquire(blocking=False)
        if not acquired:
            return ""
    else:
        _FASTER_WHISPER_MODEL_TRANSCRIBE_LOCK.acquire()
    try:
        segments, _info = model.transcribe(
            audio_np,
            language=vp._text(language) or "en",
            beam_size=beam_size,
            vad_filter=False,
            condition_on_previous_text=False,
            initial_prompt=initial_prompt or None,
            temperature=0.0,
        )
        for segment in segments:
            text = vp._text(getattr(segment, "text", ""))
            if text:
                parts.append(text)
    finally:
        _FASTER_WHISPER_MODEL_TRANSCRIBE_LOCK.release()
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _write_pcm16_mono_wav(path: str, pcm16: bytes, *, rate: int = 16000) -> None:
    with wave.open(path, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(int(rate))
        wav_file.writeframes(bytes(pcm16 or b""))


def _transcribe_mlx_whisper_sync(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    partial: bool = False,
) -> str:
    vp = _vp()
    if vp.MLXWhisper is None:
        raise RuntimeError(f"mlx-whisper dependency unavailable: {vp.MLX_WHISPER_IMPORT_ERROR or 'unknown import error'}")

    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(prefix="tater-mlx-whisper-", suffix=".wav", delete=False) as tmp:
            temp_path = tmp.name
        _write_pcm16_mono_wav(temp_path, pcm16, rate=16000)
        kwargs: Dict[str, Any] = {
            "path_or_hf_repo": vp._mlx_whisper_model(),
            "verbose": False,
        }
        lang = vp._text(language)
        if lang:
            kwargs["language"] = lang
        with _MLX_WHISPER_TRANSCRIBE_LOCK:
            root = vp._ensure_stt_backend_model_root("mlx_whisper")
            with _temporary_env(
                huggingface_environment(
                    {
                        "HF_HOME": root,
                        "HF_HUB_CACHE": os.path.join(root, "hub"),
                        "HUGGINGFACE_HUB_CACHE": os.path.join(root, "hub"),
                    }
                )
            ):
                try:
                    result = vp.MLXWhisper.transcribe(temp_path, **kwargs)
                except TypeError:
                    result = vp.MLXWhisper.transcribe(temp_path, path_or_hf_repo=vp._mlx_whisper_model())
        if isinstance(result, dict):
            return re.sub(r"\s+", " ", vp._text(result.get("text"))).strip()
        return re.sub(r"\s+", " ", vp._text(result)).strip()
    finally:
        if temp_path:
            with contextlib.suppress(Exception):
                os.unlink(temp_path)


def _transcribe_mlx_whisper_wake_sync(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str] = "en",
) -> str:
    """Decode a short wake clip deterministically without a temporary WAV."""
    vp = _vp()
    if vp.MLXWhisper is None:
        raise RuntimeError(f"mlx-whisper dependency unavailable: {vp.MLX_WHISPER_IMPORT_ERROR or 'unknown import error'}")

    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    np_mod = importlib.import_module("numpy")
    audio_np = np_mod.frombuffer(pcm16, dtype=np_mod.int16).astype(np_mod.float32) / 32768.0
    kwargs: Dict[str, Any] = {
        "path_or_hf_repo": vp._mlx_whisper_model(),
        # None disables mlx-whisper's progress bar. The normal transcription
        # path uses False so interactive transcription can still show progress.
        "verbose": None,
        "temperature": 0.0,
        "condition_on_previous_text": False,
        "compression_ratio_threshold": 2.4,
        "logprob_threshold": -1.0,
        "no_speech_threshold": 0.6,
    }
    lang = vp._text(language)
    if lang:
        kwargs["language"] = lang

    with _MLX_WHISPER_TRANSCRIBE_LOCK:
        root = vp._ensure_stt_backend_model_root("mlx_whisper")
        with _temporary_env(
            huggingface_environment(
                {
                    "HF_HOME": root,
                    "HF_HUB_CACHE": os.path.join(root, "hub"),
                    "HUGGINGFACE_HUB_CACHE": os.path.join(root, "hub"),
                }
            )
        ):
            result = vp.MLXWhisper.transcribe(audio_np, **kwargs)
    if isinstance(result, dict):
        return re.sub(r"\s+", " ", vp._text(result.get("text"))).strip()
    return re.sub(r"\s+", " ", vp._text(result)).strip()


def _transcribe_parakeet_onnx_sync(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    partial: bool = False,
) -> str:
    del partial
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    np_mod = importlib.import_module("numpy")
    audio_np = np_mod.frombuffer(pcm16, dtype=np_mod.int16).astype(np_mod.float32) / 32768.0
    model = _load_parakeet_onnx_model()
    kwargs: Dict[str, Any] = {
        "sample_rate": 16000,
        "channel": "mean",
    }
    lang = vp._text(language)
    if lang:
        kwargs["language"] = lang
    with _PARAKEET_ONNX_TRANSCRIBE_LOCK:
        result = model.recognize(audio_np, **kwargs)
    return re.sub(r"\s+", " ", vp._text(result)).strip()


def _vosk_result_text(payload: Any) -> str:
    vp = _vp()
    raw = vp._text(payload)
    if not raw:
        return ""
    with contextlib.suppress(Exception):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return vp._text(parsed.get("text"))
    return ""


def _transcribe_vosk_sync(audio_bytes: bytes, audio_format: Dict[str, int]) -> str:
    vp = _vp()
    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    model = _load_vosk_model()
    recognizer = vp.KaldiRecognizer(model, 16000.0)
    with contextlib.suppress(Exception):
        recognizer.SetWords(False)
    parts: List[str] = []
    chunk_size = 4000
    for offset in range(0, len(pcm16), chunk_size):
        chunk = pcm16[offset : offset + chunk_size]
        if not chunk:
            continue
        if recognizer.AcceptWaveform(chunk):
            text = _vosk_result_text(recognizer.Result())
            if text:
                parts.append(text)
    final_text = _vosk_result_text(recognizer.FinalResult())
    if final_text:
        parts.append(final_text)
    return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()


def _wyoming_timeout_s() -> float:
    vp = _vp()
    return vp._get_float_setting("VOICE_NATIVE_WYOMING_TIMEOUT_S", vp.DEFAULT_WYOMING_TIMEOUT_SECONDS, minimum=5.0, maximum=180.0)


def _openai_compatible_tts_timeout_s() -> float:
    vp = _vp()
    return float(vp.DEFAULT_OPENAI_COMPATIBLE_TTS_TIMEOUT_SECONDS)


def _chatterbox_tts_timeout_s() -> float:
    vp = _vp()
    return vp._get_float_setting(
        "VOICE_NATIVE_CHATTERBOX_TTS_TIMEOUT_S",
        vp.DEFAULT_CHATTERBOX_TTS_TIMEOUT_SECONDS,
        minimum=5.0,
        maximum=300.0,
    )


def _openai_compatible_tts_endpoint(base_url: Any) -> str:
    vp = _vp()
    base = vp._text(base_url).rstrip("/")
    if not base:
        return ""
    if base.endswith("/v1/audio/speech"):
        return base
    if base.endswith("/v1"):
        return f"{base}/audio/speech"
    return f"{base}/v1/audio/speech"


def _chatterbox_tts_endpoint(base_url: Any) -> str:
    vp = _vp()
    base = vp._text(base_url).rstrip("/")
    if not base:
        return ""
    for suffix in (
        "/tts",
        "/v1/audio/speech",
        "/v1/audio/voices",
        "/v1/models",
        "/audio/speech",
        "/audio/voices",
        "/models",
        "/v1",
    ):
        if base.endswith(suffix):
            root = base[: -len(suffix)].rstrip("/") or base
            return f"{root}/tts"
    return f"{base}/tts"


def _looks_like_wav_bytes(payload: bytes) -> bool:
    raw = bytes(payload or b"")
    return len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WAVE"


def _response_error_text(response: Any) -> str:
    vp = _vp()
    with contextlib.suppress(Exception):
        payload = response.json()
        if isinstance(payload, dict):
            detail = vp._text(payload.get("detail"))
            if detail:
                return detail
            error = payload.get("error")
            if isinstance(error, dict):
                message = vp._text(error.get("message"))
                if message:
                    return message
            message = vp._text(payload.get("message"))
            if message:
                return message
    with contextlib.suppress(Exception):
        body = vp._text(response.text)
        if body:
            return body[:500]
    return f"HTTP {int(getattr(response, 'status_code', 0) or 0)}"


def _decode_wav_bytes(wav_bytes: bytes) -> Tuple[bytes, Dict[str, Any]]:
    raw = bytes(wav_bytes or b"")
    if not raw:
        return b"", {}
    if not _looks_like_wav_bytes(raw):
        raise RuntimeError("TTS did not return WAV audio.")
    with wave.open(io.BytesIO(raw), "rb") as wav_file:
        frames = wav_file.readframes(wav_file.getnframes())
        return frames, {
            "rate": int(wav_file.getframerate() or 24000),
            "width": int(wav_file.getsampwidth() or 2),
            "channels": int(wav_file.getnchannels() or 1),
        }


def _wyoming_stt_endpoint() -> Tuple[str, int]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    stt = cfg.get("wyoming_stt") if isinstance(cfg.get("wyoming_stt"), dict) else {}
    host = vp._text(stt.get("host")) or vp.DEFAULT_WYOMING_STT_HOST
    port = int(stt.get("port") or vp.DEFAULT_WYOMING_STT_PORT)
    return host, port


def _wyoming_tts_endpoint() -> Tuple[str, int]:
    vp = _vp()
    cfg = vp._voice_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = vp._text(tts.get("host")) or vp.DEFAULT_WYOMING_TTS_HOST
    port = int(tts.get("port") or vp.DEFAULT_WYOMING_TTS_PORT)
    return host, port


async def _native_wyoming_refresh_tts_voices() -> Dict[str, Any]:
    vp = _vp()
    if vp.AsyncTcpClient is None or vp.Describe is None or vp.Info is None or vp.WyomingError is None:
        raise RuntimeError(f"Wyoming describe dependency unavailable: {vp.WYOMING_IMPORT_ERROR or 'unknown import error'}")

    host, port = _wyoming_tts_endpoint()
    timeout = _wyoming_timeout_s()

    info_obj = None
    async with vp.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(vp.Describe().event()), timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if vp.WyomingError.is_type(event.type):
                err = vp.WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS describe error: {err.text} ({err.code or 'unknown'})")
            if vp.Info.is_type(event.type):
                info_obj = vp.Info.from_event(event)
                break

    if info_obj is None:
        raise RuntimeError("Wyoming TTS did not return info after describe.")

    voices: List[Dict[str, str]] = []
    seen = set()
    tts_programs = getattr(info_obj, "tts", None)
    if not isinstance(tts_programs, list):
        tts_programs = []

    for program in tts_programs:
        program_name = vp._text(getattr(program, "name", None))
        voice_rows = getattr(program, "voices", None)
        if not isinstance(voice_rows, list):
            continue
        for voice in voice_rows:
            voice_name = vp._text(getattr(voice, "name", None))
            languages = [vp._text(item) for item in (getattr(voice, "languages", None) or []) if vp._text(item)]
            speakers = getattr(voice, "speakers", None)
            speaker_rows = speakers if isinstance(speakers, list) else []

            if speaker_rows:
                for speaker in speaker_rows:
                    selection = {
                        "name": voice_name,
                        "language": vp._text(languages[0]) if languages else "",
                        "speaker": vp._text(getattr(speaker, "name", None)),
                    }
                    value = vp._voice_selection_to_value(selection)
                    if not value or value in seen:
                        continue
                    seen.add(value)
                    label = vp._voice_selection_label(selection)
                    if program_name:
                        label = f"{label} • {program_name}"
                    voices.append({"value": value, "label": label})
                continue

            selection = {"name": voice_name, "language": vp._text(languages[0]) if languages else "", "speaker": ""}
            value = vp._voice_selection_to_value(selection)
            if not value or value in seen:
                continue
            seen.add(value)
            label = vp._voice_selection_label(selection)
            if program_name:
                label = f"{label} • {program_name}"
            voices.append({"value": value, "label": label})

    voices = sorted(voices, key=lambda row: vp._lower(row.get("label")))
    vp._save_wyoming_tts_voice_catalog(voices, host=host, port=port, error="")
    return {"host": host, "port": port, "voices": voices, "count": len(voices)}


async def _native_wyoming_stream_stt_task(
    token: str,
    session_id: str,
    queue: asyncio.Queue,
    audio_format: Dict[str, int],
    language: Optional[str],
    session_ref: Optional[VoiceSessionRuntime] = None,
) -> None:
    vp = _vp()
    if (
        vp.AsyncTcpClient is None
        or vp.Transcribe is None
        or vp.Transcript is None
        or vp.WyomingAudioStart is None
        or vp.WyomingAudioChunk is None
        or vp.WyomingAudioStop is None
        or vp.WyomingError is None
    ):
        if isinstance(session_ref, VoiceSessionRuntime):
            vp._mark_stt_stream_unhealthy(session_ref, "wyoming_dependency_unavailable")
        return

    host, port = _wyoming_stt_endpoint()
    timeout = _wyoming_timeout_s()
    rate = int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS)

    vp._native_debug(f"STT (stream) connect {host}:{port} rate={rate} width={width} ch={channels}")

    try:
        async with vp.AsyncTcpClient(host, port) as client:
            await asyncio.wait_for(client.write_event(vp.Transcribe(language=vp._text(language) or None).event()), timeout=timeout)
            await asyncio.wait_for(client.write_event(vp.WyomingAudioStart(rate=rate, width=width, channels=channels).event()), timeout=timeout)
            stop_sent = asyncio.Event()
            result_future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            async def _update_session_transcript(*, transcript: str, final: bool) -> None:
                text_value = vp._text(transcript)
                runtime = vp._selector_runtime(token)
                if runtime and "lock" in runtime:
                    async with runtime.get("lock"):
                        sess = runtime.get("session")
                        if isinstance(sess, VoiceSessionRuntime) and sess.session_id == session_id:
                            if final:
                                sess.stt_transcript = text_value
                            else:
                                sess.partial_transcript = text_value
                                sess.partial_transcript_updates += 1
                                sess.partial_transcript_updated_ts = vp._now()
                            return
                if isinstance(session_ref, VoiceSessionRuntime):
                    with contextlib.suppress(Exception):
                        if vp._text(session_ref.session_id) == vp._text(session_id):
                            if final:
                                session_ref.stt_transcript = text_value
                            else:
                                session_ref.partial_transcript = text_value
                                session_ref.partial_transcript_updates += 1
                                session_ref.partial_transcript_updated_ts = vp._now()

            async def _reader() -> None:
                try:
                    while True:
                        event = await asyncio.wait_for(client.read_event(), timeout=timeout)
                        if event is None:
                            break
                        if vp.Transcript.is_type(event.type):
                            transcript = vp._text(vp.Transcript.from_event(event).text)
                            if stop_sent.is_set():
                                transcript = vp._sanitize_stt_transcript(transcript)
                                vp._native_debug(f"STT stream transcript={transcript!r}")
                                await _update_session_transcript(transcript=transcript, final=True)
                                if not result_future.done():
                                    result_future.set_result(transcript)
                                return
                            if transcript and vp._experimental_partial_stt_enabled():
                                await _update_session_transcript(transcript=transcript, final=False)
                                vp._native_debug(f"STT partial transcript selector={token} session_id={session_id} transcript={transcript!r}")
                            continue
                        if vp.WyomingError.is_type(event.type):
                            err = vp.WyomingError.from_event(event)
                            vp._native_debug(f"Wyoming STT error: {err.text}")
                            if isinstance(session_ref, VoiceSessionRuntime):
                                vp._mark_stt_stream_unhealthy(session_ref, vp._text(err.text) or "wyoming_stt_error")
                            if not result_future.done():
                                result_future.set_result("")
                            return
                except Exception as exc:
                    vp._native_debug(f"STT stream reader failed: {exc}")
                    if isinstance(session_ref, VoiceSessionRuntime):
                        vp._mark_stt_stream_unhealthy(session_ref, f"wyoming_reader_failed:{exc}")
                    if not result_future.done():
                        result_future.set_result("")

            reader_task = asyncio.create_task(_reader())
            try:
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    await asyncio.wait_for(
                        client.write_event(vp.WyomingAudioChunk(rate=rate, width=width, channels=channels, audio=chunk).event()),
                        timeout=timeout,
                    )

                await asyncio.wait_for(client.write_event(vp.WyomingAudioStop().event()), timeout=timeout)
                stop_sent.set()
                transcript = await asyncio.wait_for(result_future, timeout=timeout)
                if transcript:
                    return
                if isinstance(session_ref, VoiceSessionRuntime):
                    fallback = vp._text(session_ref.partial_transcript)
                    if fallback:
                        session_ref.stt_transcript = fallback
                        return
            finally:
                reader_task.cancel()
                with contextlib.suppress(Exception):
                    await reader_task
    except Exception as exc:
        vp._native_debug(f"STT stream task failed: {exc}")
        if isinstance(session_ref, VoiceSessionRuntime):
            vp._mark_stt_stream_unhealthy(session_ref, f"wyoming_stream_failed:{exc}")


async def _native_wyoming_transcribe_audio_bytes(
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
) -> str:
    """Transcribe one complete buffer through the selected Wyoming STT service."""
    vp = _vp()
    if (
        vp.AsyncTcpClient is None
        or vp.Transcribe is None
        or vp.Transcript is None
        or vp.WyomingAudioStart is None
        or vp.WyomingAudioChunk is None
        or vp.WyomingAudioStop is None
        or vp.WyomingError is None
    ):
        raise RuntimeError(
            f"Wyoming STT dependency unavailable: {vp.WYOMING_IMPORT_ERROR or 'unknown import error'}"
        )

    pcm16, _state = vp._pcm_to_pcm16_mono_16k(audio_bytes, audio_format)
    if not pcm16:
        return ""

    host, port = _wyoming_stt_endpoint()
    timeout = _wyoming_timeout_s()
    rate = 16000
    width = 2
    channels = 1
    vp._native_debug(
        f"STT (wake verifier wyoming) connect {host}:{port} "
        f"rate={rate} width={width} ch={channels} bytes={len(pcm16)}"
    )

    async with vp.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(
            client.write_event(vp.Transcribe(language=vp._text(language) or None).event()),
            timeout=timeout,
        )
        await asyncio.wait_for(
            client.write_event(
                vp.WyomingAudioStart(rate=rate, width=width, channels=channels).event()
            ),
            timeout=timeout,
        )
        for offset in range(0, len(pcm16), 8000):
            chunk = pcm16[offset : offset + 8000]
            if not chunk:
                continue
            await asyncio.wait_for(
                client.write_event(
                    vp.WyomingAudioChunk(
                        rate=rate,
                        width=width,
                        channels=channels,
                        audio=chunk,
                    ).event()
                ),
                timeout=timeout,
            )
        await asyncio.wait_for(
            client.write_event(vp.WyomingAudioStop().event()),
            timeout=timeout,
        )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = await asyncio.wait_for(
                client.read_event(),
                timeout=max(0.1, deadline - time.monotonic()),
            )
            if event is None:
                break
            if vp.Transcript.is_type(event.type):
                transcript = vp._text(vp.Transcript.from_event(event).text)
                return vp._sanitize_stt_transcript(transcript)
            if vp.WyomingError.is_type(event.type):
                err = vp.WyomingError.from_event(event)
                raise RuntimeError(
                    f"Wyoming STT error: {err.text} ({err.code or 'unknown'})"
                )

    raise RuntimeError("Wyoming STT did not return a transcript.")


def _buffered_stt_fallback_backend() -> str:
    vp = _vp()
    candidates = []
    default_backend = vp._normalize_stt_backend(vp.DEFAULT_STT_BACKEND)
    if default_backend != "wyoming":
        candidates.append(default_backend)
    candidates.extend(["parakeet_onnx", "mlx_whisper", "faster_whisper", "vosk"])

    seen = set()
    for candidate in candidates:
        token = vp._normalize_stt_backend(candidate)
        if token in seen or token == "wyoming":
            continue
        seen.add(token)
        ok, _reason = vp._stt_backend_available(token)
        if ok:
            return token
    return ""


async def _transcribe_buffered_stt_fallback(session: VoiceSessionRuntime, reason: str) -> str:
    vp = _vp()
    audio_bytes = vp._stt_audio_bytes_for_transcription(session)
    if not audio_bytes:
        return ""
    backend = _buffered_stt_fallback_backend()
    if not backend:
        vp._native_debug(
            f"STT stream fallback unavailable selector={session.selector} session_id={session.session_id} "
            f"reason={vp._text(reason) or '-'}"
        )
        return ""
    if not bool(session.stt_stream_fallback_used):
        vp._voice_metrics_record_stt_fallback(session.selector, vp._text(reason) or "wyoming_stream_unhealthy")
        session.stt_stream_fallback_used = True
    vp._native_debug(
        f"STT stream fallback selector={session.selector} session_id={session.session_id} "
        f"backend={backend} reason={vp._text(reason) or '-'} bytes={len(audio_bytes)}"
    )
    transcript = await _native_transcribe_local_audio_bytes(
        backend=backend,
        audio_bytes=audio_bytes,
        audio_format=session.audio_format,
        language=session.language,
        selector=session.selector,
        session_id=session.session_id,
        partial=False,
    )
    session.stt_backend_effective = backend
    session.stt_transcript = vp._text(transcript)
    return session.stt_transcript


async def _native_transcribe_session_audio(session: VoiceSessionRuntime) -> str:
    vp = _vp()
    backend = vp._normalize_stt_backend(vp._text(session.stt_backend_effective) or vp._text(session.stt_backend))
    spud_stream = getattr(session, "spud_link_stt_stream", None)
    if spud_stream is not None and bool(getattr(session, "spud_link_endpointing_reuse_stt", False)):
        try:
            remote = await spud_stream.wait_final(timeout=120.0)
            session.stt_backend_effective = "spud_link"
            session.stt_transcript = vp._sanitize_stt_transcript(vp._text(remote.get("text")))
            vp._native_debug(
                f"Spud Hub streaming STT transcript selector={session.selector} "
                f"session_id={session.session_id} transcript_len={len(session.stt_transcript)}"
            )
            return session.stt_transcript
        except Exception as exc:
            session.spud_link_endpointing_error = vp._text(exc) or type(exc).__name__
            vp.logger.warning(
                "[native-voice] Spud Hub streaming STT failed; retrying configured route selector=%s session_id=%s error=%s",
                session.selector,
                session.session_id,
                session.spud_link_endpointing_error,
            )
        finally:
            with contextlib.suppress(Exception):
                await spud_stream.close()
            session.spud_link_stt_stream = None
    elif spud_stream is not None:
        with contextlib.suppress(Exception):
            await spud_stream.close()
        session.spud_link_stt_stream = None

    if spud_link_should_use_hub("stt", redis_conn=vp.redis_client):
        try:
            audio_bytes = vp._stt_audio_bytes_for_transcription(session)
            if not audio_bytes:
                session.stt_transcript = ""
                return ""
            remote = await spud_link_request_stt_async(
                audio_bytes=audio_bytes,
                audio_format=session.audio_format,
                language=session.language or "",
                redis_conn=vp.redis_client,
            )
            session.stt_backend_effective = "spud_link"
            session.stt_transcript = vp._sanitize_stt_transcript(vp._text(remote.get("text")))
            return session.stt_transcript
        except Exception:
            if not spud_link_allow_local_fallback("stt", redis_conn=vp.redis_client):
                raise
            backend, _fallback_note = vp._resolve_stt_backend_selected(vp._selected_stt_backend())
            session.stt_backend_effective = backend
    if backend == "wyoming":
        if session.stt_task is not None:
            wait_timeout = 2.0 if bool(session.stt_stream_unhealthy) else 15.0
            try:
                await asyncio.wait_for(session.stt_task, timeout=wait_timeout)
            except asyncio.TimeoutError:
                vp._mark_stt_stream_unhealthy(session, "wyoming_stream_result_timeout")
                session.stt_task.cancel()
                with contextlib.suppress(BaseException):
                    await session.stt_task
        final_text = vp._sanitize_stt_transcript(session.stt_transcript)
        if final_text != vp._text(session.stt_transcript):
            session.stt_transcript = final_text
        if final_text and not bool(session.stt_stream_unhealthy):
            return final_text
        fallback = vp._text(session.partial_transcript)
        if bool(session.stt_stream_unhealthy):
            buffered = await _transcribe_buffered_stt_fallback(
                session,
                vp._text(session.stt_stream_fallback_reason) or "wyoming_stream_unhealthy",
            )
            if buffered:
                return buffered
        if final_text:
            return final_text
        session.stt_transcript = fallback
        return fallback

    if session.partial_stt_task is not None:
        session.partial_stt_task.cancel()
        with contextlib.suppress(Exception):
            await session.partial_stt_task
        session.partial_stt_task = None

    audio_bytes = vp._stt_audio_bytes_for_transcription(session)
    if not audio_bytes:
        session.stt_transcript = ""
        return ""

    transcript = await _native_transcribe_local_audio_bytes(
        backend=backend,
        audio_bytes=audio_bytes,
        audio_format=session.audio_format,
        language=session.language,
        selector=session.selector,
        session_id=session.session_id,
        partial=False,
    )

    session.stt_transcript = vp._text(transcript)
    vp._native_debug(f"STT {backend} transcript={session.stt_transcript!r}")
    return session.stt_transcript


async def _native_transcribe_local_audio_bytes(
    *,
    backend: str,
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    selector: str,
    session_id: str,
    partial: bool,
) -> str:
    vp = _vp()
    token = vp._normalize_stt_backend(backend)
    data = bytes(audio_bytes or b"")
    if not data:
        return ""

    if spud_link_should_use_hub("stt", redis_conn=vp.redis_client):
        try:
            remote = await spud_link_request_stt_async(
                audio_bytes=data,
                audio_format=audio_format,
                language=language or "",
                redis_conn=vp.redis_client,
            )
            cleaned = vp._text(remote.get("text"))
            return cleaned if partial else vp._sanitize_stt_transcript(cleaned)
        except Exception:
            if not spud_link_allow_local_fallback("stt", redis_conn=vp.redis_client):
                raise
            token, _fallback_note = vp._resolve_stt_backend_selected(vp._selected_stt_backend())

    mode_label = "partial" if partial else "final"
    async with _LOCAL_STT_TRANSCRIBE_LOCK:
        if token == "faster_whisper":
            vp._native_debug(
                f"STT ({mode_label} faster-whisper) local selector={selector} session_id={session_id} "
                f"bytes={len(data)} beam={vp._faster_whisper_beam_size(partial=bool(partial))}"
            )
            transcript = await _run_local_stt_thread(
                _transcribe_faster_whisper_sync,
                data,
                audio_format,
                language,
                bool(partial),
            )
        elif token == "mlx_whisper":
            vp._native_debug(
                f"STT ({mode_label} mlx-whisper) local selector={selector} session_id={session_id} "
                f"bytes={len(data)} model={vp._mlx_whisper_model()}"
            )
            transcript = await _run_local_stt_thread(
                _transcribe_mlx_whisper_sync,
                data,
                audio_format,
                language,
                bool(partial),
            )
        elif token == "parakeet_onnx":
            vp._native_debug(
                f"STT ({mode_label} parakeet-onnx) local selector={selector} session_id={session_id} "
                f"bytes={len(data)} model={vp.DEFAULT_PARAKEET_ONNX_MODEL} "
                f"quantization={vp._parakeet_onnx_quantization() or 'fp32'}"
            )
            transcript = await _run_local_stt_thread(
                _transcribe_parakeet_onnx_sync,
                data,
                audio_format,
                language,
                bool(partial),
            )
        elif token == "qwen3_asr_llama_cpp":
            vp._native_debug(
                f"STT ({mode_label} qwen3-asr llama.cpp) local selector={selector} "
                f"session_id={session_id} bytes={len(data)}"
            )
            transcript = await _run_local_stt_thread(
                _transcribe_qwen3_asr_llama_cpp_sync,
                data,
                audio_format,
                language,
                bool(partial),
            )
        elif token == "vosk":
            vp._native_debug(f"STT ({mode_label} vosk) local selector={selector} session_id={session_id} bytes={len(data)}")
            transcript = await _run_local_stt_thread(_transcribe_vosk_sync, data, audio_format)
        else:
            raise RuntimeError(f"Unsupported local STT backend: {token}")

    cleaned = vp._text(transcript)
    if not bool(partial):
        cleaned = vp._sanitize_stt_transcript(cleaned)
    return cleaned


async def _native_transcribe_wake_audio_bytes(
    *,
    backend: str,
    audio_bytes: bytes,
    audio_format: Dict[str, int],
    language: Optional[str],
    selector: str,
) -> str:
    """Transcribe a short wake clip with the effective user-selected backend."""
    vp = _vp()
    token = vp._normalize_stt_backend(backend)
    data = bytes(audio_bytes or b"")
    if not data:
        return ""

    if token == "wyoming":
        return await _native_wyoming_transcribe_audio_bytes(data, audio_format, language)
    if token == "spud_link":
        remote = await spud_link_request_stt_async(
            audio_bytes=data,
            audio_format=audio_format,
            language=language or "",
            redis_conn=vp.redis_client,
        )
        return vp._text(remote.get("text"))

    vp._native_debug(
        f"STT (wake verifier {token}) selector={selector} bytes={len(data)}"
    )
    async with _LOCAL_STT_TRANSCRIBE_LOCK:
        if token == "faster_whisper":
            transcript = await _run_local_stt_thread(
                _transcribe_faster_whisper_sync,
                data,
                audio_format,
                language,
                True,
                True,
            )
        elif token == "mlx_whisper":
            transcript = await _run_local_stt_thread(
                _transcribe_mlx_whisper_wake_sync,
                data,
                audio_format,
                language,
            )
        elif token == "parakeet_onnx":
            transcript = await _run_local_stt_thread(
                _transcribe_parakeet_onnx_sync,
                data,
                audio_format,
                language,
                True,
            )
        elif token == "qwen3_asr_llama_cpp":
            transcript = await _run_local_stt_thread(
                _transcribe_qwen3_asr_llama_cpp_sync,
                data,
                audio_format,
                language,
                True,
            )
        elif token == "vosk":
            transcript = await _run_local_stt_thread(
                _transcribe_vosk_sync,
                data,
                audio_format,
            )
        else:
            raise RuntimeError(f"Unsupported wake-verifier STT backend: {token}")
    return vp._text(transcript)


async def _native_local_partial_stt_task(
    token: str,
    session_id: str,
    *,
    session_ref: Optional[VoiceSessionRuntime] = None,
) -> None:
    vp = _vp()
    last_audio_bytes = 0
    last_partial = ""
    while True:
        try:
            await asyncio.sleep(float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_INTERVAL_S))
            if not vp._experimental_partial_stt_enabled():
                return

            runtime = vp._selector_runtime(token)
            lock = runtime.get("lock")
            if lock is None or not hasattr(lock, "acquire"):
                return

            async with lock:
                session = runtime.get("session")
                if not isinstance(session, VoiceSessionRuntime) or vp._text(session.session_id) != vp._text(session_id):
                    return
                if bool(session.processing):
                    return
                backend = vp._normalize_stt_backend(vp._text(session.stt_backend_effective) or vp._text(session.stt_backend))
                if backend == "wyoming":
                    return
                audio_bytes = bytes(session.audio_buffer or b"")
                audio_format = dict(session.audio_format or {})
                language = session.language
                speech_s = float(session.speech_duration_s or 0.0)
                partial_updates = int(session.partial_transcript_updates or 0)

            if not audio_bytes:
                continue

            rate = int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ)
            width = int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
            channels = int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS)
            bytes_per_second = max(1, rate * width * channels)
            audio_s = float(len(audio_bytes)) / float(bytes_per_second)
            min_audio_s = float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_AUDIO_S)
            if speech_s < min_audio_s and audio_s < min_audio_s:
                continue
            min_new_bytes = int(bytes_per_second * float(vp.DEFAULT_EXPERIMENTAL_PARTIAL_STT_MIN_NEW_AUDIO_S))
            if last_audio_bytes > 0 and (len(audio_bytes) - last_audio_bytes) < min_new_bytes and partial_updates > 0:
                continue

            transcript = await _native_transcribe_local_audio_bytes(
                backend=backend,
                audio_bytes=audio_bytes,
                audio_format=audio_format,
                language=language,
                selector=token,
                session_id=session_id,
                partial=True,
            )
            text_value = vp._text(transcript)
            if not text_value or text_value == last_partial:
                if text_value:
                    last_audio_bytes = len(audio_bytes)
                continue

            async with lock:
                session = runtime.get("session")
                if not isinstance(session, VoiceSessionRuntime) or vp._text(session.session_id) != vp._text(session_id):
                    return
                if bool(session.processing):
                    return
                session.partial_transcript = text_value
                session.partial_transcript_updates += 1
                session.partial_transcript_updated_ts = vp._now()

            if isinstance(session_ref, VoiceSessionRuntime) and session_ref is not session and vp._text(session_ref.session_id) == vp._text(session_id):
                with contextlib.suppress(Exception):
                    session_ref.partial_transcript = text_value
                    session_ref.partial_transcript_updates += 1
                    session_ref.partial_transcript_updated_ts = vp._now()

            last_partial = text_value
            last_audio_bytes = len(audio_bytes)
            vp._native_debug(f"STT partial transcript selector={token} session_id={session_id} transcript={text_value!r}")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            vp._native_debug(f"local partial STT task failed selector={token} session_id={session_id} error={exc}")
            return


async def _native_wyoming_synthesize(
    text: str,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    voice_value: Any = None,
) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    if (
        vp.AsyncTcpClient is None
        or vp.Synthesize is None
        or vp.WyomingAudioStart is None
        or vp.WyomingAudioChunk is None
        or vp.WyomingAudioStop is None
        or vp.WyomingError is None
    ):
        raise RuntimeError(f"Wyoming client dependency unavailable: {vp.WYOMING_IMPORT_ERROR or 'unknown import error'}")

    prompt = vp._text(text)
    if not prompt:
        return b"", {}

    cfg = vp._voice_config_snapshot()
    tts = cfg.get("wyoming_tts") if isinstance(cfg.get("wyoming_tts"), dict) else {}
    host = vp._text(host) or vp._text(tts.get("host")) or vp.DEFAULT_WYOMING_TTS_HOST
    port = vp._as_int(port, int(tts.get("port") or vp.DEFAULT_WYOMING_TTS_PORT), minimum=1, maximum=65535)
    selected = vp._voice_selection_from_string(voice_value if voice_value is not None else tts.get("voice"))
    selected_label = vp._voice_selection_label(selected) if selected else "default"
    timeout = _wyoming_timeout_s()

    vp._native_debug(f"TTS connect {host}:{port} text_len={len(prompt)} voice={selected_label}")

    synth_event = None
    if selected and vp.SynthesizeVoice is not None:
        voice_obj = vp.SynthesizeVoice(
            name=vp._text(selected.get("name")) or None,
            language=vp._text(selected.get("language")) or None,
            speaker=vp._text(selected.get("speaker")) or None,
        )
        synth_event = vp.Synthesize(text=prompt, voice=voice_obj).event()
    elif selected:
        with contextlib.suppress(Exception):
            synth_event = vp.Synthesize(text=prompt, voice=selected).event()
    if synth_event is None:
        synth_event = vp.Synthesize(text=prompt).event()

    audio_out = bytearray()
    audio_format: Dict[str, Any] = {}
    saw_start = False

    async with vp.AsyncTcpClient(host, port) as client:
        await asyncio.wait_for(client.write_event(synth_event), timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            left = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(client.read_event(), timeout=left)
            if event is None:
                break
            if vp.WyomingAudioStart.is_type(event.type):
                start = vp.WyomingAudioStart.from_event(event)
                saw_start = True
                audio_format = {"rate": start.rate, "width": start.width, "channels": start.channels}
                continue
            if vp.WyomingAudioChunk.is_type(event.type):
                chunk = vp.WyomingAudioChunk.from_event(event)
                audio_out.extend(chunk.audio or b"")
                continue
            if vp.WyomingAudioStop.is_type(event.type):
                break
            if vp.WyomingError.is_type(event.type):
                err = vp.WyomingError.from_event(event)
                raise RuntimeError(f"Wyoming TTS error: {err.text} ({err.code or 'unknown'})")

    if not saw_start:
        raise RuntimeError("Wyoming TTS did not emit audio-start")
    return bytes(audio_out), audio_format


def _native_openai_compatible_synthesize_sync(
    text: str,
    *,
    model: str,
    voice: str,
    base_url: str,
    api_key: str,
) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}

    endpoint = _openai_compatible_tts_endpoint(base_url)
    if not endpoint:
        raise RuntimeError("OpenAI-compatible TTS base URL is required.")

    headers = {"Content-Type": "application/json"}
    bearer = vp._text(api_key)
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    payload = {
        "model": vp._text(model) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL,
        "input": prompt,
        "voice": vp._text(voice) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE,
        "response_format": "wav",
    }

    response = requests.post(
        endpoint,
        json=payload,
        headers=headers,
        timeout=_openai_compatible_tts_timeout_s(),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI-compatible TTS request failed: {_response_error_text(response)}")
    wav_bytes = bytes(response.content or b"")
    if not wav_bytes:
        raise RuntimeError("OpenAI-compatible TTS returned no audio.")
    return _decode_wav_bytes(wav_bytes)


def _optional_chatterbox_float(value: Any, label: str, *, minimum: float, maximum: float) -> Optional[float]:
    vp = _vp()
    raw = vp._text(value)
    if not raw:
        return None
    try:
        parsed = float(raw)
    except Exception as exc:
        raise RuntimeError(f"Chatterbox {label} must be a number.") from exc
    if parsed < minimum or parsed > maximum:
        raise RuntimeError(f"Chatterbox {label} must be between {minimum:g} and {maximum:g}.")
    return parsed


def _optional_chatterbox_seed(value: Any) -> Optional[int]:
    vp = _vp()
    raw = vp._text(value)
    if not raw:
        return None
    try:
        parsed = int(float(raw))
    except Exception as exc:
        raise RuntimeError("Chatterbox seed must be a non-negative integer.") from exc
    if parsed < 0:
        raise RuntimeError("Chatterbox seed must be a non-negative integer.")
    return parsed


def _chatterbox_tts_request(
    text: str,
    *,
    voice: str,
    base_url: str,
    voice_mode: str,
    chunk_size: Any,
    temperature: Any = None,
    exaggeration: Any = None,
    cfg_weight: Any = None,
    seed: Any = None,
    speed_factor: Any = None,
    language: Any = None,
    stream: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return "", {}

    endpoint = _chatterbox_tts_endpoint(base_url)
    if not endpoint:
        raise RuntimeError("Chatterbox TTS base URL is required.")

    selected_voice_mode = vp._normalize_chatterbox_voice_mode(voice_mode)
    selected_voice = vp._text(voice)
    payload: Dict[str, Any] = {
        "text": prompt,
        "voice_mode": selected_voice_mode,
        "output_format": "wav",
        "split_text": True,
        "chunk_size": vp._normalize_chatterbox_chunk_size(chunk_size),
        "stream": bool(stream),
    }
    if selected_voice:
        if selected_voice_mode == "clone":
            payload["reference_audio_filename"] = selected_voice
        else:
            payload["predefined_voice_id"] = selected_voice

    optional_values = {
        "temperature": _optional_chatterbox_float(temperature, "temperature", minimum=0.0, maximum=1.5),
        "exaggeration": _optional_chatterbox_float(exaggeration, "exaggeration", minimum=0.25, maximum=2.0),
        "cfg_weight": _optional_chatterbox_float(cfg_weight, "CFG weight", minimum=0.2, maximum=1.0),
        "seed": _optional_chatterbox_seed(seed),
        "speed_factor": _optional_chatterbox_float(speed_factor, "speed factor", minimum=0.25, maximum=4.0),
    }
    for key, value in optional_values.items():
        if value is not None:
            payload[key] = value
    selected_language = vp._text(language)
    if selected_language:
        payload["language"] = selected_language
    return endpoint, payload


def _native_chatterbox_synthesize_sync(
    text: str,
    *,
    voice: str,
    base_url: str,
    voice_mode: str,
    chunk_size: Any,
    temperature: Any = None,
    exaggeration: Any = None,
    cfg_weight: Any = None,
    seed: Any = None,
    speed_factor: Any = None,
    language: Any = None,
) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}

    endpoint, payload = _chatterbox_tts_request(
        prompt,
        voice=voice,
        base_url=base_url,
        voice_mode=voice_mode,
        chunk_size=chunk_size,
        temperature=temperature,
        exaggeration=exaggeration,
        cfg_weight=cfg_weight,
        seed=seed,
        speed_factor=speed_factor,
        language=language,
        stream=False,
    )

    response = requests.post(
        endpoint,
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "audio/wav"},
        timeout=_chatterbox_tts_timeout_s(),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Chatterbox TTS request failed: {_response_error_text(response)}")
    wav_bytes = bytes(response.content or b"")
    if not wav_bytes:
        raise RuntimeError("Chatterbox TTS returned no audio.")
    return _decode_wav_bytes(wav_bytes)


def _open_chatterbox_tts_stream_response(row: Dict[str, Any]) -> requests.Response:
    vp = _vp()
    endpoint = vp._text((row or {}).get("endpoint"))
    payload = (row or {}).get("payload")
    if not endpoint or not isinstance(payload, dict):
        raise RuntimeError("Chatterbox streaming TTS request is incomplete.")
    stream_payload = dict(payload)
    stream_payload["stream"] = True
    response = requests.post(
        endpoint,
        json=stream_payload,
        headers={"Content-Type": "application/json", "Accept": "audio/wav"},
        timeout=_chatterbox_tts_timeout_s(),
        stream=True,
    )
    if response.status_code >= 400:
        try:
            detail = _response_error_text(response)
        finally:
            with contextlib.suppress(Exception):
                response.close()
        raise RuntimeError(f"Chatterbox streaming TTS request failed: {detail}")
    return response


def _iter_chatterbox_tts_stream_response(response: requests.Response, row: Dict[str, Any]) -> Iterator[bytes]:
    vp = _vp()
    total_bytes = 0
    max_bytes = vp._as_int(
        (row or {}).get("max_bytes"),
        getattr(vp, "DEFAULT_CHATTERBOX_TTS_STREAM_MAX_BYTES", 64 * 1024 * 1024),
        minimum=1024 * 1024,
        maximum=512 * 1024 * 1024,
    )
    stream_id = vp._text((row or {}).get("id"))
    try:
        for chunk in response.iter_content(chunk_size=16 * 1024):
            if not chunk:
                continue
            total_bytes += len(chunk)
            if total_bytes > max_bytes:
                raise RuntimeError(f"Chatterbox streaming TTS exceeded {max_bytes} bytes.")
            yield bytes(chunk)
    finally:
        with contextlib.suppress(Exception):
            response.close()
        vp._native_debug(
            f"chatterbox streaming tts finished stream_id={stream_id} session_id={vp._text((row or {}).get('session_id'))} "
            f"bytes={total_bytes}"
        )


def _kokoro_output_gain() -> float:
    vp = _vp()
    env_value = os.getenv("TATER_KOKORO_OUTPUT_GAIN")
    if env_value is not None and vp._text(env_value):
        return vp._as_float(env_value, vp.DEFAULT_KOKORO_OUTPUT_GAIN, minimum=0.1, maximum=4.0)
    settings = vp._voice_settings_with_shared_speech()
    return vp._as_float(settings.get("VOICE_KOKORO_OUTPUT_GAIN"), vp.DEFAULT_KOKORO_OUTPUT_GAIN, minimum=0.1, maximum=4.0)


def _pocket_tts_output_gain() -> float:
    vp = _vp()
    env_value = os.getenv("TATER_POCKET_TTS_OUTPUT_GAIN")
    if env_value is not None and vp._text(env_value):
        return vp._as_float(env_value, vp.DEFAULT_POCKET_TTS_OUTPUT_GAIN, minimum=0.1, maximum=4.0)
    settings = vp._voice_settings_with_shared_speech()
    return vp._as_float(
        settings.get("VOICE_POCKET_TTS_OUTPUT_GAIN"),
        vp.DEFAULT_POCKET_TTS_OUTPUT_GAIN,
        minimum=0.1,
        maximum=4.0,
    )


def _float_audio_to_pcm16_bytes(audio: Any, *, gain: float = 1.0) -> bytes:
    np_mod = importlib.import_module("numpy")
    array = np_mod.asarray(audio, dtype=np_mod.float32)
    if array.ndim > 1:
        array = np_mod.squeeze(array)
    if array.ndim > 1:
        array = array.reshape(-1)
    if not array.size:
        return b""
    array = np_mod.nan_to_num(array, nan=0.0, posinf=1.0, neginf=-1.0)
    factor = _vp()._as_float(gain, 1.0, minimum=0.0, maximum=4.0)
    if factor != 1.0:
        array = array * factor
    array = np_mod.clip(array, -0.98, 0.98)
    return (array * 32767.0).astype(np_mod.int16).tobytes()


@contextlib.contextmanager
def _temporary_env(overrides: Dict[str, Any]):
    vp = _vp()
    previous: Dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            previous[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = vp._text(value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _load_kokoro_pipeline(model_id: str, voice: Optional[str] = None) -> Any:
    vp = _vp()
    spec = vp._kokoro_model_spec(model_id)
    variant = vp._text(spec.get("variant")) or "v1.0"
    quality = vp._text(spec.get("quality")) or "q8"
    engine = vp._kokoro_engine()
    if engine == "torch":
        if vp.KokoroTorchPipeline is None:
            raise RuntimeError(f"kokoro torch dependency unavailable: {vp.KOKORO_TORCH_IMPORT_ERROR or 'unknown import error'}")
        selected_voice = vp._text(voice) or vp.DEFAULT_KOKORO_VOICE
        lang_code = selected_voice[0:1].lower() or "a"
        device = vp._kokoro_torch_device()
        repo_id = vp.DEFAULT_KOKORO_TORCH_ZH_REPO_ID if variant.startswith("v1.1-zh") else vp.DEFAULT_KOKORO_TORCH_REPO_ID
        key = ("torch", repo_id, lang_code, device)

        with vp._kokoro_pipeline_lock:
            pipeline = vp._kokoro_pipeline_cache.get(key)
            if pipeline is None:
                root = vp._ensure_tts_backend_model_root("kokoro_torch")
                vp.logger.info("[native-voice] kokoro model source=%s model=%s engine=torch device=%s repo=%s", root, model_id, device, repo_id)
                env = huggingface_environment(
                    {
                        "HF_HOME": root,
                        "HF_HUB_CACHE": os.path.join(root, "hub"),
                        "HUGGINGFACE_HUB_CACHE": os.path.join(root, "hub"),
                    }
                )
                if device == "mps":
                    env["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
                with _temporary_env(env):
                    pipeline = vp.KokoroTorchPipeline(lang_code=lang_code, repo_id=repo_id, device=device)
                vp._kokoro_pipeline_cache[key] = pipeline
            return pipeline

    if vp.build_kokoro_pipeline is None or vp.KokoroPipelineConfig is None:
        raise RuntimeError(f"kokoro dependency unavailable: {vp.KOKORO_IMPORT_ERROR or 'unknown import error'}")

    provider = vp._kokoro_provider()
    key = ("onnx", variant, quality, provider)

    with vp._kokoro_pipeline_lock:
        pipeline = vp._kokoro_pipeline_cache.get(key)
        if pipeline is None:
            root = vp._ensure_tts_backend_model_root("kokoro")
            onnx_backend_mod = importlib.import_module("pykokoro.onnx_backend")
            vp._patch_kokoro_ssmd_parser()

            def _kokoro_cache_path(folder: Optional[str] = None):
                base = root
                if folder:
                    base = os.path.join(base, folder)
                os.makedirs(base, exist_ok=True)
                from pathlib import Path
                return Path(base)

            setattr(onnx_backend_mod, "get_user_cache_path", _kokoro_cache_path)
            cfg = vp.KokoroPipelineConfig(
                voice=vp.DEFAULT_KOKORO_VOICE,
                model_source="huggingface",
                model_variant=variant,
                model_quality=quality,
                provider=provider,
                tokenizer_config=(vp.KokoroTokenizerConfig(use_spacy=False) if vp.KokoroTokenizerConfig is not None else None),
            )
            vp.logger.info("[native-voice] kokoro model source=%s model=%s provider=%s", root, model_id, provider)
            with _temporary_env(huggingface_environment()):
                pipeline = vp.build_kokoro_pipeline(config=cfg, eager=True)
            vp._kokoro_pipeline_cache[key] = pipeline
        return pipeline


def _pocket_tts_load_model_for_token(token: str) -> Any:
    vp = _vp()
    model_token = vp._text(token)
    if not model_token or model_token.lower() in {"default", vp.DEFAULT_POCKET_TTS_MODEL.lower()}:
        return vp.PocketTTSModel.load_model()

    expanded = os.path.expanduser(model_token)
    if expanded.lower().endswith((".yaml", ".yml")):
        return vp.PocketTTSModel.load_model(config=expanded)

    try:
        return vp.PocketTTSModel.load_model(model_token)
    except Exception as exc:
        message = str(exc)
        if "Config should be a path" in message:
            raise RuntimeError(
                "PocketTTS model config must be a local .yaml file. "
                "Use the built-in PocketTTS model option unless you are providing a custom YAML config path."
            ) from exc
        raise


def _load_pocket_tts_model(model_id: str) -> Any:
    vp = _vp()
    if vp.PocketTTSModel is None:
        raise RuntimeError(f"pocket-tts dependency unavailable: {vp.POCKET_TTS_IMPORT_ERROR or 'unknown import error'}")

    token = vp._text(model_id) or vp.DEFAULT_POCKET_TTS_MODEL
    with vp._pocket_tts_model_lock:
        model = vp._pocket_tts_model_cache.get(token)
        if model is None:
            root = vp._ensure_tts_backend_model_root("pocket_tts")
            hf_root = os.path.join(root, "hf")
            os.makedirs(hf_root, exist_ok=True)
            vp.logger.info("[native-voice] pocket-tts model source=%s model=%s", hf_root, token)
            with _temporary_env(
                huggingface_environment(
                    {
                        "HF_HOME": hf_root,
                        "HF_HUB_CACHE": os.path.join(hf_root, "hub"),
                        "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub"),
                    }
                )
            ):
                model = _pocket_tts_load_model_for_token(token)
            vp._pocket_tts_model_cache[token] = model
        return model


def clear_tts_model_caches(*, include_piper: bool = True) -> Dict[str, int]:
    vp = _vp()
    cleared: Dict[str, int] = {}
    with vp._kokoro_pipeline_lock:
        cleared["kokoro"] = len(vp._kokoro_pipeline_cache)
        vp._kokoro_pipeline_cache.clear()
    with vp._pocket_tts_model_lock:
        cleared["pocket_tts"] = len(vp._pocket_tts_model_cache)
        vp._pocket_tts_model_cache.clear()
        vp._pocket_tts_voice_state_cache.clear()
    if include_piper:
        with vp._piper_voice_lock:
            cleared["piper"] = len(vp._piper_voice_cache)
            vp._piper_voice_cache.clear()
    cleared.update(clear_managed_tts_workers())
    return cleared


def _piper_model_paths(model_id: str) -> Tuple[str, str]:
    vp = _vp()
    root = vp._ensure_tts_backend_model_root("piper")
    token = vp._text(model_id) or vp.DEFAULT_PIPER_MODEL
    return os.path.join(root, f"{token}.onnx"), os.path.join(root, f"{token}.onnx.json")


def _load_piper_voice_model(model_id: str) -> Any:
    vp = _vp()
    if vp.PiperVoice is None or vp.PiperSynthesisConfig is None or vp.piper_download_voice is None:
        raise RuntimeError(f"piper dependency unavailable: {vp.PIPER_IMPORT_ERROR or 'unknown import error'}")

    model_path, config_path = _piper_model_paths(model_id)
    backend_root = vp._ensure_tts_backend_model_root("piper")
    if not (os.path.isfile(model_path) and os.path.isfile(config_path)):
        vp.logger.info("[native-voice] piper model missing; downloading model=%s target_root=%s", model_id, backend_root)
        with _temporary_env(huggingface_environment()):
            vp.piper_download_voice(
                vp._text(model_id) or vp.DEFAULT_PIPER_MODEL,
                download_dir=importlib.import_module("pathlib").Path(backend_root),
            )

    cache_key = vp._text(model_path)
    with vp._piper_voice_lock:
        voice = vp._piper_voice_cache.get(cache_key)
        if voice is None:
            vp.logger.info("[native-voice] piper model source=%s", model_path)
            with _temporary_env(huggingface_environment()):
                voice = vp.PiperVoice.load(model_path=model_path, config_path=config_path, download_dir=backend_root)
            vp._piper_voice_cache[cache_key] = voice
        return voice


def _synthesize_kokoro_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    engine = vp._kokoro_engine()
    pipeline = _load_kokoro_pipeline(model_id, voice=voice)
    if engine == "torch":
        chunks: List[Any] = []
        prompt = vp._text(text)
        selected_voice = vp._text(voice) or vp.DEFAULT_KOKORO_VOICE
        env = {}
        if vp._kokoro_torch_device() == "mps":
            env["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
        with _temporary_env(env):
            for result in pipeline(prompt, voice=selected_voice, speed=1, split_pattern=r"\n+"):
                audio = getattr(result, "audio", None)
                if audio is not None:
                    chunks.append(audio)
        if not chunks:
            return b"", {"rate": 24000, "width": 2, "channels": 1}
        np_mod = importlib.import_module("numpy")
        audio = np_mod.concatenate([np_mod.asarray(chunk, dtype=np_mod.float32).reshape(-1) for chunk in chunks])
        audio_bytes = _float_audio_to_pcm16_bytes(audio, gain=_kokoro_output_gain())
        return audio_bytes, {"rate": 24000, "width": 2, "channels": 1}

    result = pipeline.run(vp._text(text), voice=vp._text(voice) or vp.DEFAULT_KOKORO_VOICE)
    audio_bytes = _float_audio_to_pcm16_bytes(getattr(result, "audio", None), gain=_kokoro_output_gain())
    sample_rate = int(getattr(result, "sample_rate", 24000) or 24000)
    return audio_bytes, {"rate": sample_rate, "width": 2, "channels": 1}


def _synthesize_pocket_tts_sync(text: str, model_id: str, voice: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}
    model = _load_pocket_tts_model(model_id)
    root = vp._ensure_tts_backend_model_root("pocket_tts")
    hf_root = os.path.join(root, "hf")
    os.makedirs(hf_root, exist_ok=True)
    selected_voice = vp._text(voice) or vp.DEFAULT_POCKET_TTS_VOICE
    env = huggingface_environment(
        {
            "HF_HOME": hf_root,
            "HF_HUB_CACHE": os.path.join(hf_root, "hub"),
            "HUGGINGFACE_HUB_CACHE": os.path.join(hf_root, "hub"),
        }
    )
    with vp._pocket_tts_runtime_lock:
        with _temporary_env(env):
            model_state = _get_pocket_tts_voice_state(model, model_id, selected_voice)
            audio_bytes = _generate_pocket_tts_pcm(
                model,
                model_state,
                prompt,
                gain=_pocket_tts_output_gain(),
            )
    return audio_bytes, {"rate": int(getattr(model, "sample_rate", 24000) or 24000), "width": 2, "channels": 1}


def _pocket_tts_voice_cache_key(model_id: str, voice: str) -> Tuple[str, str]:
    vp = _vp()
    model_token = vp._text(model_id) or vp.DEFAULT_POCKET_TTS_MODEL
    voice_token = vp._text(voice) or vp.DEFAULT_POCKET_TTS_VOICE
    expanded = os.path.abspath(os.path.expanduser(voice_token))
    try:
        stat = os.stat(expanded)
    except OSError:
        return model_token, voice_token
    return model_token, f"file:{expanded}:{stat.st_size}:{stat.st_mtime_ns}"


def _get_pocket_tts_voice_state(model: Any, model_id: str, voice: str) -> Any:
    vp = _vp()
    cache_key = _pocket_tts_voice_cache_key(model_id, voice)
    with vp._pocket_tts_model_lock:
        cached = vp._pocket_tts_voice_state_cache.pop(cache_key, None)
        if cached is not None:
            vp._pocket_tts_voice_state_cache[cache_key] = cached
            return cached

        state = model.get_state_for_audio_prompt(vp._text(voice) or vp.DEFAULT_POCKET_TTS_VOICE)
        vp._pocket_tts_voice_state_cache[cache_key] = state
        while len(vp._pocket_tts_voice_state_cache) > _POCKET_TTS_VOICE_STATE_CACHE_LIMIT:
            oldest_key = next(iter(vp._pocket_tts_voice_state_cache))
            vp._pocket_tts_voice_state_cache.pop(oldest_key, None)
        return state


def _generate_pocket_tts_pcm(model: Any, model_state: Any, text: str, *, gain: float) -> bytes:
    stream = getattr(model, "generate_audio_stream", None)
    if callable(stream):
        chunks: List[bytes] = []
        for audio_chunk in stream(model_state, text):
            tensor = audio_chunk.detach().cpu().squeeze()
            pcm = _float_audio_to_pcm16_bytes(tensor.numpy(), gain=gain)
            if pcm:
                chunks.append(pcm)
        return b"".join(chunks)

    audio_tensor = model.generate_audio(model_state, text)
    tensor = audio_tensor.detach().cpu().squeeze()
    return _float_audio_to_pcm16_bytes(tensor.numpy(), gain=gain)


def _split_piper_sentences(text: str) -> List[str]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return []
    parts: List[str] = []
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
        if ch == "." and (token in vp._PIPER_ABBREVIATIONS or (len(token) == 1 and token.isalpha())):
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


def _build_piper_segment_plan(text: str) -> List[Tuple[str, float]]:
    vp = _vp()
    normalized = re.sub(r"\r\n?", "\n", vp._text(text))
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", normalized) if vp._text(part)]
    if not paragraphs:
        return []
    plan: List[Tuple[str, float]] = []
    last_paragraph_index = len(paragraphs) - 1
    for paragraph_index, paragraph in enumerate(paragraphs):
        sentences = _split_piper_sentences(paragraph)
        if not sentences:
            continue
        last_sentence_index = len(sentences) - 1
        for sentence_index, sentence in enumerate(sentences):
            pause_seconds = 0.0
            if sentence_index < last_sentence_index:
                pause_seconds = vp.DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS
            elif paragraph_index < last_paragraph_index:
                pause_seconds = vp.DEFAULT_PIPER_PARAGRAPH_PAUSE_SECONDS
            plan.append((sentence, pause_seconds))
    return plan


def _build_experimental_tts_chunks(text: str) -> List[str]:
    vp = _vp()
    prompt = vp._sanitize_spoken_response_text(text)
    if len(prompt) < int(vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_CHARS):
        return []
    sentence_plan = _build_piper_segment_plan(prompt)
    sentences = [vp._text(sentence) for sentence, _pause_s in sentence_plan if vp._text(sentence)]
    if len(sentences) < 2:
        return []
    first_chunk = sentences[0]
    remaining = list(sentences[1:])
    if len(first_chunk) < int(vp.DEFAULT_EXPERIMENTAL_TTS_EARLY_START_MIN_FIRST_CHARS) and remaining:
        first_chunk = f"{first_chunk} {remaining.pop(0)}".strip()
    chunks = [first_chunk]
    if remaining:
        remainder = " ".join(part for part in remaining if part).strip()
        if remainder:
            chunks.append(remainder)
    return [chunk for chunk in chunks if vp._text(chunk)]


def _synthesize_piper_segment_sync(voice: Any, prompt: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    audio_out = bytearray()
    sample_rate = 22050
    sample_width = 2
    sample_channels = 1
    syn_config = vp.PiperSynthesisConfig()
    for chunk in voice.synthesize(prompt, syn_config=syn_config):
        audio_out.extend(chunk.audio_int16_bytes)
        sample_rate = int(getattr(chunk, "sample_rate", sample_rate) or sample_rate)
        sample_width = int(getattr(chunk, "sample_width", sample_width) or sample_width)
        sample_channels = int(getattr(chunk, "sample_channels", sample_channels) or sample_channels)
    return bytes(audio_out), {"rate": sample_rate, "width": sample_width, "channels": sample_channels}


def _synthesize_piper_sync(text: str, model_id: str) -> Tuple[bytes, Dict[str, Any]]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}
    voice = _load_piper_voice_model(model_id)
    segment_plan = _build_piper_segment_plan(prompt) or [(prompt, 0.0)]
    audio_parts: List[bytes] = []
    audio_format: Dict[str, Any] = {"rate": 22050, "width": 2, "channels": 1}
    for segment_text, pause_seconds in segment_plan:
        segment_audio, segment_format = _synthesize_piper_segment_sync(voice, segment_text)
        if segment_audio:
            audio_parts.append(segment_audio)
            audio_format = dict(segment_format)
        if pause_seconds > 0:
            audio_parts.append(vp._append_pcm_silence(b"", audio_format, seconds=pause_seconds))
    padded = vp._append_pcm_silence(b"".join(audio_parts), audio_format, seconds=vp.DEFAULT_PIPER_TAIL_PAD_SECONDS)
    return padded, audio_format


async def _native_synthesize_text(
    text: str,
    *,
    session: Optional[VoiceSessionRuntime] = None,
    values: Optional[Dict[str, Any]] = None,
) -> Tuple[bytes, Dict[str, Any], str, str]:
    vp = _vp()
    prompt = vp._text(text)
    if not prompt:
        return b"", {}, "", ""

    if spud_link_should_use_hub("tts", redis_conn=vp.redis_client):
        try:
            wav_bytes = await spud_link_request_tts_wav_async(text=prompt, redis_conn=vp.redis_client)
            with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
                audio_format = {
                    "rate": int(wav_file.getframerate() or 24000),
                    "width": int(wav_file.getsampwidth() or 2),
                    "channels": int(wav_file.getnchannels() or 1),
                }
                audio_bytes = wav_file.readframes(wav_file.getnframes())
            return audio_bytes, audio_format, "spud_link", "Loaded on Spud Hub"
        except Exception:
            if not spud_link_allow_local_fallback("tts", redis_conn=vp.redis_client):
                raise

    selection = _tts_selection_from_values(values)
    selected_backend = vp._normalize_tts_backend(vp._text(session.tts_backend if isinstance(session, VoiceSessionRuntime) else "") or selection.get("backend"))
    effective_backend_value = vp._text(
        session.tts_backend_effective
        if isinstance(session, VoiceSessionRuntime)
        else ""
    )
    effective_backend = (
        vp._normalize_tts_backend(effective_backend_value)
        if effective_backend_value
        else ""
    )
    backend_note = ""
    if not effective_backend:
        effective_backend, backend_note = _resolve_tts_backend(values)

    try:
        if effective_backend == "kokoro":
            vp._native_debug(f"TTS (kokoro) local model={selection.get('model')} voice={selection.get('voice') or vp.DEFAULT_KOKORO_VOICE}")
            audio_bytes, audio_format = await run_tts(
                _synthesize_kokoro_sync,
                prompt,
                vp._text(selection.get("model")) or vp.DEFAULT_KOKORO_MODEL,
                vp._text(selection.get("voice")) or vp.DEFAULT_KOKORO_VOICE,
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "pocket_tts":
            vp._native_debug(f"TTS (pocket-tts) local model={selection.get('model')} voice={selection.get('voice') or vp.DEFAULT_POCKET_TTS_VOICE}")
            audio_bytes, audio_format = await run_tts(
                _synthesize_pocket_tts_sync,
                prompt,
                vp._text(selection.get("model")) or vp.DEFAULT_POCKET_TTS_MODEL,
                vp._text(selection.get("voice")) or vp.DEFAULT_POCKET_TTS_VOICE,
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "piper":
            vp._native_debug(f"TTS (piper) local model={selection.get('model') or vp.DEFAULT_PIPER_MODEL}")
            audio_bytes, audio_format = await run_tts(_synthesize_piper_sync, prompt, vp._text(selection.get("model")) or vp.DEFAULT_PIPER_MODEL)
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend in {"qwen3_tts", "omnivoice"}:
            prefix = "qwen_tts" if effective_backend == "qwen3_tts" else "omnivoice_tts"
            default_model = DEFAULT_QWEN_TTS_MODEL if effective_backend == "qwen3_tts" else DEFAULT_OMNIVOICE_TTS_MODEL
            vp._native_debug(f"TTS ({effective_backend}) managed local model={selection.get('model') or default_model}")
            audio_bytes, audio_format = await run_tts(
                synthesize_managed_tts_pcm,
                prompt,
                backend=effective_backend,
                model=vp._text(selection.get("model")) or default_model,
                clone_audio=selection.get(f"{prefix}_clone_audio"),
                clone_text=selection.get(f"{prefix}_clone_text"),
                language=selection.get(f"{prefix}_language"),
                instruct=selection.get(f"{prefix}_instruct"),
                acceleration=vp._voice_settings_with_shared_speech().get("VOICE_ACCELERATION"),
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "openai_compatible":
            vp._native_debug(
                f"TTS (openai-compatible) remote base={selection.get('openai_base_url')} "
                f"model={selection.get('model') or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL} "
                f"voice={selection.get('voice') or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE}"
            )
            audio_bytes, audio_format = await run_tts(
                _native_openai_compatible_synthesize_sync,
                prompt,
                model=vp._text(selection.get("model")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL,
                voice=vp._text(selection.get("voice")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE,
                base_url=vp._text(selection.get("openai_base_url")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL,
                api_key=vp._text(selection.get("openai_api_key")) or vp.DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY,
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        if effective_backend == "chatterbox":
            vp._native_debug(
                f"TTS (chatterbox) remote base={selection.get('chatterbox_base_url')} "
                f"voice_mode={selection.get('chatterbox_voice_mode')} voice={selection.get('voice')}"
            )
            audio_bytes, audio_format = await run_tts(
                _native_chatterbox_synthesize_sync,
                prompt,
                voice=vp._text(selection.get("voice")),
                base_url=vp._text(selection.get("chatterbox_base_url")) or vp.DEFAULT_CHATTERBOX_TTS_BASE_URL,
                voice_mode=vp._text(selection.get("chatterbox_voice_mode")) or vp.DEFAULT_CHATTERBOX_TTS_VOICE_MODE,
                chunk_size=selection.get("chatterbox_chunk_size"),
                temperature=selection.get("chatterbox_temperature"),
                exaggeration=selection.get("chatterbox_exaggeration"),
                cfg_weight=selection.get("chatterbox_cfg_weight"),
                seed=selection.get("chatterbox_seed"),
                speed_factor=selection.get("chatterbox_speed_factor"),
                language=selection.get("chatterbox_language"),
            )
            return audio_bytes, audio_format, effective_backend, backend_note
        audio_bytes, audio_format = await _native_wyoming_synthesize(
            prompt,
            host=vp._text(selection.get("wyoming_host")) or None,
            port=selection.get("wyoming_port"),
            voice_value=selection.get("wyoming_voice"),
        )
        return audio_bytes, audio_format, "wyoming", backend_note
    except Exception as exc:
        if effective_backend != "wyoming":
            wyoming_ok, _wyoming_reason = _tts_backend_available("wyoming")
            if wyoming_ok:
                vp.logger.warning("[native-voice] TTS backend fallback selected=%s effective=%s reason=%s", selected_backend, effective_backend, vp._text(exc))
                audio_bytes, audio_format = await _native_wyoming_synthesize(
                    prompt,
                    host=vp._text(selection.get("wyoming_host")) or None,
                    port=selection.get("wyoming_port"),
                    voice_value=selection.get("wyoming_voice"),
                )
                fallback_note = (f"{backend_note} " if backend_note else "") + f"{effective_backend} synthesis failed: {vp._text(exc)}. Falling back to Wyoming."
                return audio_bytes, audio_format, "wyoming", fallback_note.strip()
        raise


def _normalized_audio_format(audio_format: Dict[str, Any]) -> Dict[str, int]:
    vp = _vp()
    return {
        "rate": int(audio_format.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ),
        "width": int(audio_format.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH),
        "channels": int(audio_format.get("channels") or vp.DEFAULT_VOICE_CHANNELS),
    }


def _trim_pcm_for_playback(audio_bytes: bytes, audio_format: Dict[str, Any]) -> Tuple[bytes, Dict[str, int]]:
    vp = _vp()
    data = bytes(audio_bytes or b"")
    fmt = _normalized_audio_format(audio_format or {})
    width = int(fmt.get("width") or vp.DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(fmt.get("channels") or vp.DEFAULT_VOICE_CHANNELS)
    if width not in {1, 2, 3, 4}:
        width = vp.DEFAULT_VOICE_SAMPLE_WIDTH
    if channels < 1 or channels > 8:
        channels = vp.DEFAULT_VOICE_CHANNELS
    fmt = {"rate": int(fmt.get("rate") or vp.DEFAULT_VOICE_SAMPLE_RATE_HZ), "width": width, "channels": channels}
    if not data:
        return b"", fmt
    block_align = max(1, width * channels)
    usable = len(data) - (len(data) % block_align)
    if usable <= 0:
        return b"", fmt
    return data[:usable], fmt


def _stitch_pcm_playback_segments(parts: List[Tuple[bytes, Dict[str, Any], float]]) -> Tuple[bytes, Dict[str, int]]:
    vp = _vp()
    segments: List[Tuple[bytes, Dict[str, int], float]] = []
    for audio_bytes, audio_format, pause_s in parts:
        data, fmt = _trim_pcm_for_playback(audio_bytes, audio_format)
        if not data:
            continue
        segments.append((data, fmt, max(0.0, float(pause_s or 0.0))))
    if not segments:
        return b"", {}

    target_fmt = dict(segments[0][1])
    if all(fmt == target_fmt for _, fmt, _ in segments):
        out = bytearray()
        for data, fmt, pause_s in segments:
            out.extend(data)
            if pause_s > 0:
                out.extend(vp._append_pcm_silence(b"", fmt, seconds=pause_s))
        return bytes(out), target_fmt

    normalized_fmt = {"rate": 16000, "width": 2, "channels": 1}
    out = bytearray()
    for data, fmt, pause_s in segments:
        normalized, _state = vp._pcm_to_pcm16_mono_16k(data, fmt)
        if not normalized:
            return b"", {}
        out.extend(normalized)
        if pause_s > 0:
            out.extend(vp._append_pcm_silence(b"", normalized_fmt, seconds=pause_s))
    return bytes(out), normalized_fmt


async def _synthesize_spoken_response_audio(
    response_text: str,
    *,
    session: VoiceSessionRuntime,
    continue_conversation: bool,
    followup_cue: str = "",
) -> Tuple[bytes, Dict[str, Any], str, str]:
    vp = _vp()
    reply = vp._sanitize_spoken_response_text(response_text)
    cue = vp._sanitize_followup_cue_text(followup_cue)
    if not continue_conversation:
        return await _native_synthesize_text(reply, session=session)

    if not cue:
        combined = vp._continued_chat_spoken_reply_text(reply, continue_conversation=True, followup_cue=cue)
        audio_bytes, audio_format, backend_used, backend_note = await _native_synthesize_text(combined, session=session)
        if audio_bytes:
            audio_bytes = vp._append_pcm_silence(audio_bytes, audio_format, seconds=vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S)
        return audio_bytes, audio_format, backend_used, backend_note

    split_error = ""
    try:
        reply_audio = b""
        reply_format: Dict[str, Any] = {}
        reply_backend = ""
        reply_note = ""
        if reply:
            reply_audio, reply_format, reply_backend, reply_note = await _native_synthesize_text(reply, session=session)

        cue_audio, cue_format, cue_backend, cue_note = await _native_synthesize_text(cue, session=session)
        stitched_audio, stitched_format = _stitch_pcm_playback_segments(
            [
                (reply_audio, reply_format, vp.DEFAULT_CONTINUED_CHAT_REPLY_TO_CUE_PAUSE_S if cue_audio else 0.0),
                (cue_audio, cue_format, vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S),
            ]
        )
        if stitched_audio:
            backend_used = reply_backend or cue_backend or vp._text(session.tts_backend_effective)
            backend_note = vp._merge_text_notes(
                reply_note,
                cue_note,
                (f"reply/cue TTS backend mismatch: {reply_backend}->{cue_backend}" if reply_backend and cue_backend and reply_backend != cue_backend else ""),
            )
            return stitched_audio, stitched_format, backend_used, backend_note
    except Exception as exc:
        split_error = vp._text(exc)

    combined = vp._continued_chat_spoken_reply_text(reply, continue_conversation=True, followup_cue=cue)
    audio_bytes, audio_format, backend_used, backend_note = await _native_synthesize_text(combined, session=session)
    if audio_bytes:
        audio_bytes = vp._append_pcm_silence(audio_bytes, audio_format, seconds=vp.DEFAULT_CONTINUED_CHAT_CUE_TO_REOPEN_PAUSE_S)
    backend_note = vp._merge_text_notes(
        backend_note,
        (f"followup split playback fallback: {split_error}" if split_error else ""),
        "followup cue playback used single-pass fallback",
    )
    return audio_bytes, audio_format, backend_used, backend_note
