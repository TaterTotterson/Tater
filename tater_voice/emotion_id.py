from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import wave
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from tateros import integration_store as integration_store_module

from . import runtime as esphome_runtime


logger = logging.getLogger("voice_core")


def huggingface_environment(overrides: Optional[Dict[str, Any]] = None, client: Any = None) -> Dict[str, Any]:
    return integration_store_module.huggingface_environment(overrides, client)


def huggingface_token(client: Any = None) -> str:
    return integration_store_module.huggingface_token(client)


def _vp():
    from . import voice_pipeline as vp

    return vp


def _debug(message: str) -> None:
    with contextlib.suppress(Exception):
        _vp()._native_debug(f"emotion id {message}")


def _log_info(message: str, *args: Any) -> None:
    logger.info("[native-voice] emotion-id " + message, *args)


def _log_warning(message: str, *args: Any) -> None:
    logger.warning("[native-voice] emotion-id " + message, *args)


def _log_exception(message: str, *args: Any) -> None:
    logger.exception("[native-voice] emotion-id " + message, *args)


@contextlib.contextmanager
def _temporary_huggingface_env(cache_root: Optional[Path] = None):
    overrides = huggingface_environment()
    if cache_root is not None:
        cache_root.mkdir(parents=True, exist_ok=True)
        hub_cache = cache_root / "hub"
        transformers_cache = cache_root / "transformers"
        torch_cache = cache_root / "torch"
        hub_cache.mkdir(parents=True, exist_ok=True)
        transformers_cache.mkdir(parents=True, exist_ok=True)
        torch_cache.mkdir(parents=True, exist_ok=True)
        overrides.setdefault("HF_HOME", str(cache_root))
        overrides.setdefault("HF_HUB_CACHE", str(hub_cache))
        overrides.setdefault("HUGGINGFACE_HUB_CACHE", str(hub_cache))
        overrides.setdefault("TRANSFORMERS_CACHE", str(transformers_cache))
        overrides.setdefault("TORCH_HOME", str(torch_cache))
    previous: Dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            previous[key] = os.environ.get(key)
            os.environ[key] = str(value or "").strip()
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


EMOTION_ID_SETTINGS_HASH_KEY = "voice_emotion_id_settings"
EMOTION_ID_LAST_HASH_KEY = "voice_emotion_id_last"
EMOTION_ID_MODEL_ROOT = Path(__file__).resolve().parents[1] / "agent_lab" / "models" / "emotion_id"

DEFAULT_EMOTION_ID_ENABLED = False
DEFAULT_EMOTION_ID_PROMPT_HINT_ENABLED = True
DEFAULT_EMOTION_ID_INCLUDE_NEUTRAL = True
DEFAULT_EMOTION_ID_MODEL_SOURCE = "speechbrain/emotion-recognition-wav2vec2-IEMOCAP"
_DEPRECATED_SPEECHBRAIN_HF_PATH = "speechbrain.lobes.models.huggingface_transformers."
_CURRENT_SPEECHBRAIN_HF_PATH = "speechbrain.integrations.huggingface."
DEFAULT_EMOTION_ID_MIN_SPEECH_S = 1.0
DEFAULT_EMOTION_ID_CONFIDENCE_THRESHOLD = 0.45
_SPEECHBRAIN_REPO_FILES = (
    "custom_interface.py",
    "hyperparams.yaml",
    "model.ckpt",
    "wav2vec2.ckpt",
    "label_encoder.txt",
)

_AUDIOOP = None
with contextlib.suppress(Exception):
    import audioop as _AUDIOOP  # type: ignore[assignment]

_ENGINE_LOCK = threading.Lock()
_ENGINE: Any = None
_ENGINE_SOURCE = ""
_ENGINE_DEVICE = ""
_ENGINE_REQUESTED_DEVICE = ""
_ENGINE_AUTH_FINGERPRINT = ""
_ENGINE_ERROR = ""


def _ensure_dirs() -> None:
    EMOTION_ID_MODEL_ROOT.mkdir(parents=True, exist_ok=True)


def _slugify_token(value: Any) -> str:
    token = _vp()._text(value).lower()
    out = []
    for char in token:
        if char.isalnum():
            out.append(char)
        elif char in {"-", "_", ".", "/"}:
            out.append("-")
    clean = "".join(out).strip("-")
    while "--" in clean:
        clean = clean.replace("--", "-")
    return clean or "model"


def _model_savedir(source: Optional[str] = None) -> Path:
    return EMOTION_ID_MODEL_ROOT / _slugify_token(source or _model_source())


def settings_hash_key() -> str:
    return EMOTION_ID_SETTINGS_HASH_KEY


def _setting_specs() -> List[Dict[str, Any]]:
    return [
        {
            "key": "VOICE_EMOTION_ID_ENABLED",
            "label": "Enable Emotion ID",
            "type": "checkbox",
            "default": DEFAULT_EMOTION_ID_ENABLED,
            "description": "Classify the user's voice tone after STT and before Hydra runs.",
        },
        {
            "key": "VOICE_EMOTION_ID_PROMPT_HINT_ENABLED",
            "label": "Add To Prompt",
            "type": "checkbox",
            "default": DEFAULT_EMOTION_ID_PROMPT_HINT_ENABLED,
            "description": "Include a soft voice-tone hint in the Hydra voice prompt.",
        },
        {
            "key": "VOICE_EMOTION_ID_INCLUDE_NEUTRAL",
            "label": "Use Neutral Context",
            "type": "checkbox",
            "default": DEFAULT_EMOTION_ID_INCLUDE_NEUTRAL,
            "description": "Let neutral detections steady the reply without asking Tater to mention the tone.",
        },
        {
            "key": "VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD",
            "label": "Prompt Confidence Threshold",
            "type": "number",
            "default": DEFAULT_EMOTION_ID_CONFIDENCE_THRESHOLD,
            "min": 0.0,
            "max": 1.0,
            "step": 0.01,
            "description": "Only add the tone hint when the model score is at or above this value.",
        },
        {
            "key": "VOICE_EMOTION_ID_MIN_SPEECH_S",
            "label": "Min Speech For Emotion ID (sec)",
            "type": "number",
            "default": DEFAULT_EMOTION_ID_MIN_SPEECH_S,
            "min": 0.4,
            "max": 15.0,
            "step": 0.05,
            "description": "Skip short commands that are too brief for useful tone classification.",
        },
        {
            "key": "VOICE_EMOTION_ID_MODEL_SOURCE",
            "label": "SpeechBrain Model",
            "type": "text",
            "default": DEFAULT_EMOTION_ID_MODEL_SOURCE,
            "description": "SpeechBrain emotion-recognition model source.",
        },
    ]


def _settings() -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        row = redis_client.hgetall(EMOTION_ID_SETTINGS_HASH_KEY) or {}
        if isinstance(row, dict):
            return dict(row)
    return {}


def _get_bool_setting(name: str, default: bool) -> bool:
    return _vp()._as_bool(_settings().get(name), default)


def _get_float_setting(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    return _vp()._as_float(_settings().get(name), default, minimum=minimum, maximum=maximum)


def _get_text_setting(name: str, default: str) -> str:
    value = _vp()._text(_settings().get(name))
    return value or str(default or "")


def emotion_id_enabled() -> bool:
    return _get_bool_setting("VOICE_EMOTION_ID_ENABLED", DEFAULT_EMOTION_ID_ENABLED)


def prompt_hint_enabled() -> bool:
    return _get_bool_setting("VOICE_EMOTION_ID_PROMPT_HINT_ENABLED", DEFAULT_EMOTION_ID_PROMPT_HINT_ENABLED)


def include_neutral_in_prompt() -> bool:
    return _get_bool_setting("VOICE_EMOTION_ID_INCLUDE_NEUTRAL", DEFAULT_EMOTION_ID_INCLUDE_NEUTRAL)


def _confidence_threshold() -> float:
    return _get_float_setting(
        "VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD",
        DEFAULT_EMOTION_ID_CONFIDENCE_THRESHOLD,
        minimum=0.0,
        maximum=1.0,
    )


def _min_speech_seconds() -> float:
    return _get_float_setting(
        "VOICE_EMOTION_ID_MIN_SPEECH_S",
        DEFAULT_EMOTION_ID_MIN_SPEECH_S,
        minimum=0.4,
        maximum=15.0,
    )


def _model_source() -> str:
    return _get_text_setting("VOICE_EMOTION_ID_MODEL_SOURCE", DEFAULT_EMOTION_ID_MODEL_SOURCE)


def _huggingface_auth_fingerprint() -> str:
    token = str(huggingface_token() or "").strip()
    if not token:
        return "none"
    return hashlib.sha256(token.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _huggingface_auth_label() -> str:
    return "configured" if _huggingface_auth_fingerprint() != "none" else "missing"


def _call_speechbrain_loader_with_hf_token(loader: Any, kwargs: Dict[str, Any]) -> Any:
    # SpeechBrain's foreign_class loader can reject use_auth_token only after it
    # has already started building the model. The temporary HF environment above
    # carries the token without causing a partial load + retry.
    return loader(**kwargs)


def _snapshot_complete(savedir: Path) -> bool:
    return all((savedir / filename).exists() for filename in _SPEECHBRAIN_REPO_FILES)


def _speechbrain_huggingface_integration_available() -> bool:
    with contextlib.suppress(Exception):
        import importlib.util

        return importlib.util.find_spec("speechbrain.integrations.huggingface.wav2vec2") is not None
    return False


def _patch_speechbrain_hyperparams(savedir: Path) -> None:
    if not _speechbrain_huggingface_integration_available():
        return
    hyperparams_path = savedir / "hyperparams.yaml"
    with contextlib.suppress(Exception):
        raw = hyperparams_path.read_text(encoding="utf-8")
        if _DEPRECATED_SPEECHBRAIN_HF_PATH not in raw:
            return
        patched = raw.replace(_DEPRECATED_SPEECHBRAIN_HF_PATH, _CURRENT_SPEECHBRAIN_HF_PATH)
        hyperparams_path.write_text(patched, encoding="utf-8")
        _log_info("patched SpeechBrain deprecated Hugging Face path in %s", str(hyperparams_path))


def _ensure_speechbrain_snapshot(source: str, savedir: Path, hf_cache_dir: Path) -> str:
    if _snapshot_complete(savedir):
        _patch_speechbrain_hyperparams(savedir)
        return str(savedir)
    try:
        from huggingface_hub import snapshot_download  # type: ignore
    except Exception as exc:
        _log_warning("huggingface_hub snapshot download unavailable source=%s detail=%s", source, exc)
        return source

    token = str(huggingface_token() or "").strip() or None
    savedir.mkdir(parents=True, exist_ok=True)
    _log_info("downloading model snapshot source=%s savedir=%s hf_auth=%s", source, str(savedir), _huggingface_auth_label())
    snapshot_download(
        repo_id=source,
        cache_dir=str(hf_cache_dir / "hub"),
        local_dir=str(savedir),
        local_dir_use_symlinks=False,
        token=token,
        allow_patterns=list(_SPEECHBRAIN_REPO_FILES),
    )
    if _snapshot_complete(savedir):
        _patch_speechbrain_hyperparams(savedir)
    return str(savedir) if _snapshot_complete(savedir) else source


def settings_fields() -> List[Dict[str, Any]]:
    vp = _vp()
    current = _settings()
    rows: List[Dict[str, Any]] = []
    for spec in _setting_specs():
        row = dict(spec)
        key = vp._text(row.get("key"))
        field_type = vp._lower(row.get("type") or "text")
        raw_value = current.get(key, row.get("default"))
        if field_type == "checkbox":
            row["value"] = vp._as_bool(raw_value, vp._as_bool(row.get("default"), False))
        elif field_type == "number":
            row["value"] = vp._as_float(
                raw_value,
                float(row.get("default") or 0.0),
                minimum=row.get("min") if isinstance(row.get("min"), (int, float)) else None,
                maximum=row.get("max") if isinstance(row.get("max"), (int, float)) else None,
            )
        else:
            row["value"] = vp._text(raw_value if raw_value is not None else row.get("default"))
        rows.append(row)
    return rows


def save_settings_values(values: Dict[str, Any]) -> Dict[str, Any]:
    vp = _vp()
    incoming = values if isinstance(values, dict) else {}
    specs = {vp._text(spec.get("key")): spec for spec in _setting_specs()}
    changed: List[str] = []
    for key, spec in specs.items():
        if not key or key not in incoming:
            continue
        field_type = vp._lower(spec.get("type") or "text")
        if field_type == "checkbox":
            value: Any = "1" if vp._as_bool(incoming.get(key), vp._as_bool(spec.get("default"), False)) else "0"
        elif field_type == "number":
            value = str(
                vp._as_float(
                    incoming.get(key),
                    float(spec.get("default") or 0.0),
                    minimum=spec.get("min") if isinstance(spec.get("min"), (int, float)) else None,
                    maximum=spec.get("max") if isinstance(spec.get("max"), (int, float)) else None,
                )
            )
        else:
            value = vp._text(incoming.get(key) if incoming.get(key) is not None else spec.get("default"))
        current = vp._text(redis_client.hget(EMOTION_ID_SETTINGS_HASH_KEY, key))
        if current != vp._text(value):
            redis_client.hset(EMOTION_ID_SETTINGS_HASH_KEY, key, value)
            changed.append(key)
    return {"ok": True, "updated_count": len(changed), "changed_keys": changed}


def _speechbrain_import_state() -> Tuple[bool, str]:
    try:
        import torch  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, f"PyTorch is unavailable: {exc}"
    try:
        import transformers  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, (
            "Hugging Face transformers is unavailable. Emotion ID needs transformers for the "
            f"SpeechBrain wav2vec2 emotion model: {exc}"
        )
    try:
        from speechbrain.inference.interfaces import foreign_class  # type: ignore  # noqa: F401
        return True, ""
    except Exception:
        pass
    try:
        from speechbrain.pretrained.interfaces import foreign_class  # type: ignore  # noqa: F401
        return True, ""
    except Exception as exc:
        return False, f"SpeechBrain emotion inference is unavailable: {exc}"


def _foreign_class_loader() -> Any:
    try:
        from speechbrain.inference.interfaces import foreign_class  # type: ignore
        return foreign_class
    except Exception:
        from speechbrain.pretrained.interfaces import foreign_class  # type: ignore
        return foreign_class


def _speechbrain_run_device(device: str) -> str:
    return "cuda:0" if device == "cuda" else device


def _accelerated_device_label(device: str) -> str:
    if device == "cuda":
        with contextlib.suppress(Exception):
            if _vp()._speechbrain_acceleration_setting() == "rocm" or _vp()._torch_rocm_available():
                return "AMD ROCm"
        return "CUDA"
    if device == "mps":
        return "MPS"
    return device.upper()


def _apply_speechbrain_yaml_compat_shim() -> None:
    with contextlib.suppress(Exception):
        import ruamel.yaml  # type: ignore
        import ruamel.yaml.loader  # type: ignore

        for target in (
            getattr(ruamel.yaml, "Loader", None),
            getattr(ruamel.yaml, "SafeLoader", None),
            getattr(ruamel.yaml, "RoundTripLoader", None),
            getattr(ruamel.yaml.loader, "Loader", None),
            getattr(ruamel.yaml.loader, "SafeLoader", None),
            getattr(ruamel.yaml.loader, "RoundTripLoader", None),
        ):
            if target is not None and not hasattr(target, "max_depth"):
                setattr(target, "max_depth", None)
    with contextlib.suppress(Exception):
        import hyperpyyaml.core  # type: ignore

        defaults = getattr(hyperpyyaml.core.load_hyperpyyaml, "__defaults__", None)
        if isinstance(defaults, tuple):
            for target in defaults:
                if isinstance(target, type) and not hasattr(target, "max_depth"):
                    setattr(target, "max_depth", None)
    _debug("yaml compat shim applied")


def _speechbrain_state() -> Tuple[bool, str]:
    global _ENGINE, _ENGINE_SOURCE, _ENGINE_DEVICE, _ENGINE_REQUESTED_DEVICE, _ENGINE_AUTH_FINGERPRINT, _ENGINE_ERROR
    source = _model_source()
    requested_device = _vp()._speechbrain_device()
    auth_fingerprint = _huggingface_auth_fingerprint()
    source_key = f"{source}|{requested_device}|hf:{auth_fingerprint}"
    with _ENGINE_LOCK:
        if (
            _ENGINE is not None
            and _ENGINE_SOURCE == source
            and _ENGINE_REQUESTED_DEVICE == requested_device
            and _ENGINE_AUTH_FINGERPRINT == auth_fingerprint
        ):
            return True, ""
        if _ENGINE_ERROR and _ENGINE_SOURCE == source_key:
            return False, _ENGINE_ERROR
        imports_ok, import_detail = _speechbrain_import_state()
        if not imports_ok:
            _ENGINE = None
            _ENGINE_SOURCE = source_key
            _ENGINE_DEVICE = requested_device
            _ENGINE_REQUESTED_DEVICE = requested_device
            _ENGINE_AUTH_FINGERPRINT = auth_fingerprint
            _ENGINE_ERROR = import_detail
            _log_warning("dependencies unavailable source=%s detail=%s", source, import_detail or "unknown")
            return False, _ENGINE_ERROR
        _apply_speechbrain_yaml_compat_shim()
        foreign_class = _foreign_class_loader()
        load_errors: List[str] = []
        try:
            _ensure_dirs()
            savedir = _model_savedir(source)
            savedir.mkdir(parents=True, exist_ok=True)
            hf_cache_dir = savedir / "huggingface"
            devices = [requested_device]
            if requested_device in {"cuda", "mps"}:
                devices.append("cpu")
            for device in devices:
                run_device = _speechbrain_run_device(device)
                try:
                    _log_info(
                        "loading model source=%s savedir=%s cache=%s device=%s hf_auth=%s",
                        source,
                        str(savedir),
                        str(hf_cache_dir),
                        run_device,
                        _huggingface_auth_label(),
                    )
                    with _temporary_huggingface_env(hf_cache_dir):
                        load_source = _ensure_speechbrain_snapshot(source, savedir, hf_cache_dir)
                        kwargs = {
                            "source": load_source,
                            "pymodule_file": "custom_interface.py",
                            "classname": "CustomEncoderWav2vec2Classifier",
                            "savedir": str(savedir),
                            "run_opts": {"device": run_device},
                        }
                        _ENGINE = _call_speechbrain_loader_with_hf_token(foreign_class, kwargs)
                    _ENGINE_SOURCE = source
                    _ENGINE_DEVICE = run_device
                    _ENGINE_REQUESTED_DEVICE = requested_device
                    _ENGINE_AUTH_FINGERPRINT = auth_fingerprint
                    _ENGINE_ERROR = ""
                    with contextlib.suppress(Exception):
                        if hasattr(_ENGINE, "eval"):
                            _ENGINE.eval()
                    if device != requested_device:
                        _log_warning("model loaded with CPU fallback source=%s requested_device=%s", source, requested_device)
                    _log_info("model loaded source=%s savedir=%s device=%s", source, str(savedir), run_device)
                    return True, ""
                except Exception as exc:
                    load_errors.append(f"{run_device}: {exc.__class__.__name__}: {str(exc) or 'unknown error'}")
                    if device in {"cuda", "mps"}:
                        _log_warning(
                            "%s model load failed source=%s detail=%s; retrying on CPU",
                            _accelerated_device_label(device),
                            source,
                            load_errors[-1],
                        )
                        continue
                    raise
        except Exception as exc:
            _ENGINE = None
            _ENGINE_SOURCE = source_key
            _ENGINE_DEVICE = requested_device
            _ENGINE_REQUESTED_DEVICE = requested_device
            _ENGINE_AUTH_FINGERPRINT = auth_fingerprint
            detail = str(exc) or "unknown SpeechBrain error"
            _ENGINE_ERROR = "; ".join(load_errors) or f"{exc.__class__.__name__}: {detail}"
            _log_exception("model load failed source=%s detail=%s", source, _ENGINE_ERROR)
            return False, _ENGINE_ERROR


def runtime_availability() -> Dict[str, Any]:
    available, detail = _speechbrain_import_state()
    source = _model_source()
    requested_device = _vp()._speechbrain_device()
    auth_fingerprint = _huggingface_auth_fingerprint()
    actual_device = requested_device
    if available:
        with _ENGINE_LOCK:
            actual_device = _ENGINE_DEVICE or requested_device
            if _ENGINE is not None and _ENGINE_SOURCE == source and _ENGINE_AUTH_FINGERPRINT == auth_fingerprint:
                detail = ""
                if _ENGINE_REQUESTED_DEVICE == "cuda" and _ENGINE_DEVICE == "cpu":
                    detail = "CUDA load failed; running on CPU fallback."
                elif _ENGINE_REQUESTED_DEVICE and _ENGINE_REQUESTED_DEVICE != requested_device:
                    detail = "Loaded with the previous acceleration setting; it will reload on next use."
            elif _ENGINE_ERROR and _ENGINE_SOURCE == f"{source}|{requested_device}":
                available = False
                detail = _ENGINE_ERROR
            elif _ENGINE_ERROR and _ENGINE_SOURCE == f"{source}|{requested_device}|hf:{auth_fingerprint}":
                available = False
                detail = _ENGINE_ERROR
    return {
        "available": bool(available),
        "label": "available" if available else "unavailable",
        "detail": detail,
        "model_source": _model_source(),
        "model_dir": str(_model_savedir(source)),
        "huggingface_cache_dir": str(_model_savedir(source) / "huggingface"),
        "huggingface_auth": _huggingface_auth_label(),
        "device": actual_device,
        "acceleration": _vp()._speechbrain_acceleration_setting(),
    }


def warmup_model(*, enabled_only: bool = True) -> str:
    if enabled_only and not emotion_id_enabled():
        return "skipped Emotion ID disabled"
    source = _model_source()
    available, detail = _speechbrain_state()
    if not available:
        raise RuntimeError(detail or "SpeechBrain emotion model is unavailable.")
    return f"loaded Emotion ID SpeechBrain model {source} on {_ENGINE_DEVICE or _vp()._speechbrain_device()}"


def _prepare_pcm_16k_mono(audio_bytes: bytes, audio_format: Dict[str, Any]) -> bytes:
    pcm = bytes(audio_bytes or b"")
    rate = int(audio_format.get("rate") or _vp().DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or _vp().DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or _vp().DEFAULT_VOICE_CHANNELS)
    if width != 2:
        raise RuntimeError(f"Unsupported sample width for Emotion ID: {width}")
    if channels > 1:
        if _AUDIOOP is None:
            raise RuntimeError("Stereo audio requires audioop for Emotion ID conversion.")
        pcm = _AUDIOOP.tomono(pcm, width, 0.5, 0.5)
        channels = 1
    if rate != 16000:
        if _AUDIOOP is None:
            raise RuntimeError("Resampling requires audioop for Emotion ID conversion.")
        pcm, _ = _AUDIOOP.ratecv(pcm, width, channels, rate, 16000, None)
    if not pcm:
        raise RuntimeError("No audio available for Emotion ID.")
    return pcm


def _write_temp_wav(audio_bytes: bytes, audio_format: Dict[str, Any]) -> str:
    pcm = _prepare_pcm_16k_mono(audio_bytes, audio_format)
    fd, path = tempfile.mkstemp(prefix="tater-emotion-", suffix=".wav")
    os.close(fd)
    with wave.open(path, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(pcm)
    _debug(f"temp wav prepared path={path!r} bytes={len(pcm)}")
    return path


def _as_float_score(value: Any) -> float:
    if value is None:
        return 0.0
    with contextlib.suppress(Exception):
        if hasattr(value, "item"):
            return float(value.item())
    if isinstance(value, (list, tuple)) and value:
        return _as_float_score(value[0])
    with contextlib.suppress(Exception):
        return float(value)
    return 0.0


def _label_text(value: Any) -> str:
    vp = _vp()
    if isinstance(value, (list, tuple)) and value:
        return _label_text(value[0])
    with contextlib.suppress(Exception):
        if hasattr(value, "tolist"):
            return _label_text(value.tolist())
    return vp._text(value)


def _normalize_label(value: Any) -> str:
    token = _vp()._lower(_label_text(value)).replace("_", " ").replace("-", " ").strip()
    mapping = {
        "ang": "angry",
        "anger": "angry",
        "hap": "happy",
        "happiness": "happy",
        "exc": "excited",
        "neu": "neutral",
        "neutrality": "neutral",
        "sadness": "sad",
        "fru": "frustrated",
        "frustration": "frustrated",
    }
    return mapping.get(token, token or "unknown")


def _prompt_phrase(label: str, score: float) -> str:
    tone = _vp()._text(label) or "unknown"
    confidence = float(score or 0.0)
    if tone == "neutral":
        return f"Voice tone cue: neutral ({confidence:.2f}, uncertain). Keep the reply steady and natural; do not mention the tone."
    return f"Voice tone cue: {tone} ({confidence:.2f}, uncertain). Briefly acknowledge the tone in natural, varied wording, then answer normally."


def _save_last_result(result: Dict[str, Any]) -> None:
    with contextlib.suppress(Exception):
        redis_client.hset(EMOTION_ID_LAST_HASH_KEY, "last_result", json.dumps(result, ensure_ascii=False))


def last_result() -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        raw = redis_client.hget(EMOTION_ID_LAST_HASH_KEY, "last_result")
        if raw:
            parsed = json.loads(_vp()._text(raw))
            if isinstance(parsed, dict):
                return parsed
    return {}


def _pretty_timestamp(value: Any) -> str:
    try:
        ts = float(value or 0.0)
    except Exception:
        ts = 0.0
    if ts <= 0:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return "-"


def classify_emotion_for_audio(
    *,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    speech_s: float = 0.0,
) -> Dict[str, Any]:
    if not emotion_id_enabled():
        _debug("classification skipped reason=disabled")
        return {"detected": False, "reason": "disabled"}
    if float(speech_s or 0.0) < _min_speech_seconds():
        _debug(f"classification skipped reason=too_short speech_s={float(speech_s or 0.0):.2f}")
        return {"detected": False, "reason": "too_short"}
    available, detail = _speechbrain_state()
    if not available:
        raise RuntimeError(detail or "SpeechBrain emotion model is unavailable")

    wav_path = _write_temp_wav(audio_bytes, audio_format)
    try:
        with _ENGINE_LOCK:
            classifier = _ENGINE
        if classifier is None:
            raise RuntimeError("Emotion ID model is not loaded.")
        out_prob, score, index, text_lab = classifier.classify_file(wav_path)
        label = _normalize_label(text_lab)
        raw_label = _label_text(text_lab)
        confidence = max(0.0, min(_as_float_score(score), 1.0))
        threshold = _confidence_threshold()
        prompt_enabled = bool(
            prompt_hint_enabled()
            and confidence >= threshold
            and (include_neutral_in_prompt() or label != "neutral")
            and label not in {"", "unknown"}
        )
        result = {
            "detected": bool(label and label != "unknown"),
            "reason": "detected" if label and label != "unknown" else "unknown_label",
            "emotion": label,
            "raw_label": raw_label,
            "score": confidence,
            "threshold": threshold,
            "prompt_hint_enabled": prompt_enabled,
            "prompt_hint": _prompt_phrase(label, confidence) if prompt_enabled else "",
            "speech_s": float(speech_s or 0.0),
            "model_source": _model_source(),
            "updated_ts": time.time(),
        }
        _save_last_result(result)
        _log_info("classification emotion=%s score=%.3f prompt_hint=%s", label, confidence, bool(prompt_enabled))
        _debug(f"classification emotion={label!r} raw={raw_label!r} score={confidence:.3f} prompt_hint={bool(prompt_enabled)}")
        return result
    finally:
        with contextlib.suppress(Exception):
            os.unlink(wav_path)


def panel_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    availability = runtime_availability()
    last = last_result()
    return {
        "availability": availability,
        "summary_metrics": [
            {"label": "Enabled", "value": "Yes" if emotion_id_enabled() else "No"},
            {"label": "Prompt Hint", "value": "Yes" if prompt_hint_enabled() else "No"},
            {"label": "Last Tone", "value": _vp()._text(last.get("emotion")) or "-"},
            {"label": "Last Score", "value": f"{float(last.get('score') or 0.0):.2f}" if last else "-"},
            {"label": "Model", "value": _model_source()},
            {"label": "Model Dir", "value": availability.get("model_dir") or "-"},
            {"label": "HF Auth", "value": availability.get("huggingface_auth") or "missing"},
        ],
        "settings_sections": [
            {
                "label": "Runtime",
                "fields": settings_fields(),
            }
        ],
        "last_result": {
            **last,
            "updated_at": _pretty_timestamp(last.get("updated_ts")) if last else "-",
        },
    }


def handle_runtime_action(action_name: str, payload: Dict[str, Any], status: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    action = esphome_runtime.lower(action_name)
    body = payload if isinstance(payload, dict) else {}
    values = esphome_runtime.payload_values(body)
    if action == "emotion_id_settings_save":
        result = save_settings_values(values)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} Emotion ID setting(s)." if updated > 0 else "No Emotion ID settings changed."
        _log_info("settings saved updated_count=%s changed_keys=%s", updated, ",".join(result.get("changed_keys") or []))
        return {"ok": True, "action": action, "message": message, "emotion_id": panel_payload(status), **result}
    return None
