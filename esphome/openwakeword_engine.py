from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib import request as urllib_request
from urllib.parse import unquote, urljoin, urlparse

from helpers import redis_client

from . import runtime as esphome_runtime


logger = logging.getLogger("voice_core")

OPENWAKEWORD_SETTINGS_HASH_KEY = "voice_openwakeword_settings"
OPENWAKEWORD_TRAINER_URL_KEY = "voice_openwakeword:trainer_url"
OPENWAKEWORD_MODEL_ROOT = Path(__file__).resolve().parents[1] / "agent_lab" / "models" / "openwakeword"

DEFAULT_OPENWAKEWORD_TRAINER_URL = "http://127.0.0.1:8791"
DEFAULT_OPENWAKEWORD_ENABLED = True
DEFAULT_OPENWAKEWORD_MODEL_SOURCE = "hey_jarvis"
DEFAULT_OPENWAKEWORD_INFERENCE_FRAMEWORK = "onnx"
DEFAULT_OPENWAKEWORD_THRESHOLD = 0.95
DEFAULT_OPENWAKEWORD_PATIENCE = 4
DEFAULT_OPENWAKEWORD_DEBOUNCE_S = 8.0
DEFAULT_OPENWAKEWORD_VAD_THRESHOLD = 0.0
DEFAULT_OPENWAKEWORD_DEVICE = "auto"
FALLBACK_PRETRAINED_MODELS = [
    "hey_jarvis",
    "alexa",
    "hey_mycroft",
    "hey_rhasspy",
    "timer",
    "weather",
]

_AUDIOOP = None
with contextlib.suppress(Exception):
    import audioop as _AUDIOOP  # type: ignore[assignment]

_ENGINE_LOCK = threading.Lock()
_DETECTOR_INIT_LOCK = threading.Lock()
_DETECTORS: Dict[str, "_DetectorState"] = {}
_WARM_DETECTOR: Optional["_DetectorState"] = None
_ENGINE_KEY = ""
_ENGINE_ERROR = ""
_ENGINE_DETAIL: Dict[str, Any] = {}
_CUDA_FALLBACK_UNTIL_TS = 0.0
_CUDA_FALLBACK_REASON = ""
_CUDA_FALLBACK_SECONDS = 15 * 60
_DETECTOR_IDLE_TTL_S = 60 * 60
_DETECTOR_CLEANUP_INTERVAL_S = 60
_LAST_DETECTOR_CLEANUP_TS = 0.0
_MODEL_SUFFIXES = {".onnx", ".tflite"}


def _text(value: Any) -> str:
    return esphome_runtime.text(value)


def _lower(value: Any) -> str:
    return esphome_runtime.lower(value)


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
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(float(minimum), min(float(maximum), parsed))


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _setting(name: str, default: Any) -> str:
    with contextlib.suppress(Exception):
        raw = redis_client.hget("voice_core_settings", f"VOICE_OPENWAKEWORD_{name.upper()}")
        if raw is not None:
            return _text(raw)
    with contextlib.suppress(Exception):
        raw = redis_client.hget(OPENWAKEWORD_SETTINGS_HASH_KEY, name)
        if raw is not None:
            return _text(raw)
    env_name = f"TATER_OPENWAKEWORD_{name.upper()}"
    if env_name in os.environ:
        return _text(os.getenv(env_name))
    return _text(default)


def settings_snapshot() -> Dict[str, Any]:
    framework = _lower(_setting("inference_framework", DEFAULT_OPENWAKEWORD_INFERENCE_FRAMEWORK))
    if framework not in {"onnx", "tflite"}:
        framework = DEFAULT_OPENWAKEWORD_INFERENCE_FRAMEWORK
    return {
        "enabled": _as_bool(_setting("enabled", DEFAULT_OPENWAKEWORD_ENABLED), DEFAULT_OPENWAKEWORD_ENABLED),
        "model_source": _setting("model_source", DEFAULT_OPENWAKEWORD_MODEL_SOURCE) or DEFAULT_OPENWAKEWORD_MODEL_SOURCE,
        "inference_framework": framework,
        "threshold": _as_float(_setting("threshold", DEFAULT_OPENWAKEWORD_THRESHOLD), DEFAULT_OPENWAKEWORD_THRESHOLD, minimum=0.01, maximum=0.99),
        "patience": _as_int(_setting("patience", DEFAULT_OPENWAKEWORD_PATIENCE), DEFAULT_OPENWAKEWORD_PATIENCE, minimum=1, maximum=10),
        "debounce_s": _as_float(_setting("debounce_s", DEFAULT_OPENWAKEWORD_DEBOUNCE_S), DEFAULT_OPENWAKEWORD_DEBOUNCE_S, minimum=0.0, maximum=30.0),
        "vad_threshold": _as_float(_setting("vad_threshold", DEFAULT_OPENWAKEWORD_VAD_THRESHOLD), DEFAULT_OPENWAKEWORD_VAD_THRESHOLD, minimum=0.0, maximum=0.99),
        "device": _lower(_setting("device", DEFAULT_OPENWAKEWORD_DEVICE)) or DEFAULT_OPENWAKEWORD_DEVICE,
    }


def openwakeword_enabled() -> bool:
    return bool(settings_snapshot().get("enabled"))


def _ensure_dirs() -> None:
    OPENWAKEWORD_MODEL_ROOT.mkdir(parents=True, exist_ok=True)


def _slug(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    return "_".join(part for part in token.split("_") if part)


def _friendly_model_label(value: Any) -> str:
    token = _text(value).strip()
    if not token:
        return "openWakeWord"
    return token.replace("_", " ").replace("-", " ").strip().title()


def _local_model_option(path: Path) -> Dict[str, Any]:
    framework = path.suffix.lower().lstrip(".")
    with contextlib.suppress(Exception):
        rel = path.relative_to(OPENWAKEWORD_MODEL_ROOT)
        source = rel.parts[0] if rel.parts else "local"
        prefix = {
            "trainer": "Trainer",
            "custom": "Custom",
            "pretrained": "Downloaded",
        }.get(source, "Local")
        return {
            "value": str(path),
            "label": f"{prefix}: {_friendly_model_label(path.stem)} ({framework.upper()})",
            "framework": framework,
            "source": source,
        }
    return {
        "value": str(path),
        "label": f"Local: {_friendly_model_label(path.stem)} ({framework.upper()})",
        "framework": framework,
        "source": "local",
    }


def _pretrained_model_options() -> list[Dict[str, Any]]:
    keys: list[str] = []
    with contextlib.suppress(Exception):
        import openwakeword

        models = getattr(openwakeword, "MODELS", {}) or {}
        if isinstance(models, dict):
            keys = sorted(_text(key) for key in models.keys() if _text(key))
    if not keys:
        keys = list(FALLBACK_PRETRAINED_MODELS)
    if "weather" in keys and "current_weather" not in keys:
        keys.append("current_weather")

    seen: set[str] = set()
    rows: list[Dict[str, Any]] = []
    for key in keys:
        token = _text(key).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        rows.append(
            {
                "value": token,
                "label": f"Prebuilt: {_friendly_model_label(token)}",
                "source": "prebuilt",
            }
        )
    return rows


def _local_model_options() -> list[Dict[str, Any]]:
    root = OPENWAKEWORD_MODEL_ROOT
    if not root.exists():
        return []
    rows: list[Dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".onnx", ".tflite"}:
            continue
        with contextlib.suppress(Exception):
            parts = path.relative_to(root).parts
            if parts and parts[0] == "features":
                continue
        rows.append(_local_model_option(path))
    rows.sort(key=lambda row: (_text(row.get("source")).lower(), _text(row.get("label")).lower()))
    return rows


def model_source_options(*, current: Any = "") -> list[Dict[str, Any]]:
    current_value = _text(current).strip()
    prebuilt = _pretrained_model_options()
    local = _local_model_options()
    seen = {
        _text(row.get("value")).strip()
        for group in (prebuilt, local)
        for row in group
        if isinstance(row, dict)
    }
    rows: list[Dict[str, Any]] = []
    if current_value and current_value not in seen:
        rows.append(
            {
                "value": current_value,
                "label": f"Current: {Path(current_value).name if '/' in current_value else current_value}",
                "source": "current",
            }
        )
    if prebuilt:
        rows.append({"label": "Prebuilt", "options": prebuilt})
    if local:
        rows.append({"label": "Downloaded / Local", "options": local})
    return rows


def _download_file(url: str, target: Path, *, force: bool = False) -> Path:
    _ensure_dirs()
    if not force and target.exists() and target.stat().st_size > 0:
        return target
    tmp = target.with_suffix(target.suffix + ".tmp")
    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib_request.urlopen(url, timeout=120) as response:  # noqa: S310 - trusted model URLs from openWakeWord metadata/settings.
        data = response.read()
    if not data:
        raise RuntimeError(f"Downloaded empty openWakeWord resource: {url}")
    tmp.write_bytes(data)
    tmp.replace(target)
    return target


def _normalize_trainer_url(value: Any) -> str:
    token = _text(value).strip()
    if not token:
        token = DEFAULT_OPENWAKEWORD_TRAINER_URL
    if "://" not in token:
        token = f"http://{token}"
    return token.rstrip("/")


def trainer_url() -> str:
    with contextlib.suppress(Exception):
        stored = _text(redis_client.get(OPENWAKEWORD_TRAINER_URL_KEY)).strip()
        if stored:
            return _normalize_trainer_url(stored)
    return DEFAULT_OPENWAKEWORD_TRAINER_URL


def save_trainer_url(value: Any) -> str:
    normalized = _normalize_trainer_url(value)
    with contextlib.suppress(Exception):
        redis_client.set(OPENWAKEWORD_TRAINER_URL_KEY, normalized)
    return normalized


def _trainer_absolute_url(base_url: str, value: Any) -> str:
    token = _text(value).strip()
    if not token:
        return ""
    if token.startswith(("http://", "https://")):
        return token
    if not token.startswith("/"):
        token = f"/api/artifacts/{token}"
    return urljoin(f"{_normalize_trainer_url(base_url)}/", token.lstrip("/"))


def _safe_filename(value: Any, *, fallback: str = "openwakeword.onnx") -> str:
    name = Path(unquote(_text(value).split("?", 1)[0])).name
    if not name:
        name = fallback
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name)
    cleaned = cleaned.strip("._") or fallback
    suffix = Path(cleaned).suffix.lower()
    if suffix not in {".onnx", ".tflite"}:
        raise ValueError("openWakeWord trainer artifact must be an .onnx or .tflite file.")
    return cleaned


def _framework_from_source(source: Any, framework: str = "") -> str:
    suffix = Path(_text(source).split("?", 1)[0]).suffix.lower().lstrip(".")
    if suffix in {"onnx", "tflite"}:
        return suffix
    framework_token = _lower(framework)
    return framework_token if framework_token in {"onnx", "tflite"} else ""


def _looks_like_model_path(value: Any) -> bool:
    token = _text(value).strip()
    if not token:
        return False
    suffix = Path(token.split("?", 1)[0]).suffix.lower()
    return suffix in _MODEL_SUFFIXES or "/" in token or "\\" in token


def _model_root_alias(path_value: Any) -> Optional[Path]:
    token = _text(path_value).strip()
    if not token:
        return None
    parts = Path(token).parts
    marker = ("agent_lab", "models", "openwakeword")
    for idx in range(0, max(0, len(parts) - len(marker) + 1)):
        if tuple(parts[idx : idx + len(marker)]) != marker:
            continue
        tail = parts[idx + len(marker) :]
        if not tail:
            continue
        candidate = OPENWAKEWORD_MODEL_ROOT.joinpath(*tail)
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _local_model_matches(filename: str, *, framework: str = "") -> list[Path]:
    name = _text(filename).strip()
    if not name or not OPENWAKEWORD_MODEL_ROOT.exists():
        return []
    framework_token = _framework_from_source(name, framework)
    matches: list[Path] = []
    for path in sorted(OPENWAKEWORD_MODEL_ROOT.rglob(name)):
        if not path.is_file() or path.suffix.lower() not in _MODEL_SUFFIXES:
            continue
        if framework_token and path.suffix.lower().lstrip(".") != framework_token:
            continue
        matches.append(path)
    return matches


def _copy_external_model_to_root(path: Path) -> Path:
    if not path.exists() or not path.is_file():
        raise RuntimeError(f"openWakeWord model path is not accessible from Tater: {path}")
    if path.suffix.lower() not in _MODEL_SUFFIXES:
        raise ValueError("openWakeWord model path must end in .onnx or .tflite.")
    with contextlib.suppress(Exception):
        path.relative_to(OPENWAKEWORD_MODEL_ROOT)
        return path

    target = OPENWAKEWORD_MODEL_ROOT / "custom" / (_slug(path.stem) or "custom") / path.name
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists() or target.stat().st_size != path.stat().st_size:
        shutil.copy2(path, target)
    return target


def normalize_model_source(source: Any, *, framework: str = "", copy_external: bool = True) -> Tuple[str, str]:
    source_text = _text(source).strip()
    if not source_text:
        return DEFAULT_OPENWAKEWORD_MODEL_SOURCE, _framework_from_source("", framework)
    if source_text.startswith(("http://", "https://")):
        return source_text, _framework_from_source(source_text, framework)
    if not _looks_like_model_path(source_text):
        return source_text, _framework_from_source(source_text, framework)

    framework_token = _framework_from_source(source_text, framework)
    candidate = Path(source_text).expanduser()
    if candidate.exists() and candidate.is_file():
        path = _copy_external_model_to_root(candidate) if copy_external else candidate
        return str(path), framework_token or path.suffix.lower().lstrip(".")

    alias = _model_root_alias(source_text)
    if alias is not None:
        return str(alias), framework_token or alias.suffix.lower().lstrip(".")

    matches = _local_model_matches(Path(source_text).name, framework=framework_token)
    if len(matches) == 1:
        match = matches[0]
        return str(match), framework_token or match.suffix.lower().lstrip(".")
    if len(matches) > 1:
        labels = ", ".join(str(path) for path in matches[:3])
        raise RuntimeError(f"Multiple local openWakeWord models match {Path(source_text).name}: {labels}")
    raise RuntimeError(
        "openWakeWord model path is not accessible from Tater. "
        "Download the trainer model into Tater or use a path under agent_lab/models/openwakeword."
    )


def _trainer_entries_from_payload(payload: Any, *, base_url: str, framework: str = "") -> list[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_rows = payload.get("items") or payload.get("artifacts") or payload.get("models") or payload.get("entries") or []
    if not isinstance(raw_rows, list):
        return []
    wanted_framework = _lower(framework)
    if wanted_framework not in {"onnx", "tflite"}:
        wanted_framework = ""
    rows: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        name = _text(raw.get("name") or raw.get("filename") or raw.get("file") or raw.get("path")).strip()
        url = _trainer_absolute_url(base_url, raw.get("url") or raw.get("download_url") or raw.get("model_url") or name)
        parsed_name = Path(urlparse(url).path).name
        filename = name or parsed_name
        suffix = Path(filename).suffix.lower().lstrip(".")
        if suffix not in {"onnx", "tflite"}:
            continue
        if wanted_framework and suffix != wanted_framework:
            continue
        key = url or filename
        if not key or key in seen:
            continue
        seen.add(key)
        stem = Path(filename).stem or filename
        label = _text(raw.get("label") or raw.get("title") or stem).strip() or stem
        rows.append(
            {
                "value": url,
                "label": f"{label} ({suffix.upper()})",
                "name": filename,
                "stem": stem,
                "framework": suffix,
                "size": raw.get("size") or 0,
                "mtime": raw.get("mtime") or 0,
                "url": url,
            }
        )
    rows.sort(key=lambda row: (_text(row.get("label")).lower(), _text(row.get("name")).lower()))
    return rows


def trainer_model_catalog(*, trainer_url_value: Any = "", framework: str = "") -> Dict[str, Any]:
    base_url = save_trainer_url(trainer_url_value or trainer_url())
    catalog_url = f"{base_url}/api/artifacts"
    with urllib_request.urlopen(catalog_url, timeout=20) as response:  # noqa: S310 - user-configured local trainer URL.
        payload = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
    items = _trainer_entries_from_payload(payload, base_url=base_url, framework=framework)
    return {
        "ok": True,
        "trainer_url": base_url,
        "items": items,
        "count": len(items),
    }


def save_model_source(source: Any, *, framework: str = "") -> None:
    source_text, inferred_framework = normalize_model_source(source, framework=framework, copy_external=True)
    framework_text = _lower(inferred_framework or framework)
    if framework_text not in {"onnx", "tflite"}:
        framework_text = ""
    mapping = {"VOICE_OPENWAKEWORD_MODEL_SOURCE": source_text}
    legacy_mapping = {"model_source": source_text}
    if framework_text:
        mapping["VOICE_OPENWAKEWORD_INFERENCE_FRAMEWORK"] = framework_text
        legacy_mapping["inference_framework"] = framework_text
    with contextlib.suppress(Exception):
        redis_client.hset("voice_core_settings", mapping=mapping)
    with contextlib.suppress(Exception):
        redis_client.hset(OPENWAKEWORD_SETTINGS_HASH_KEY, mapping=legacy_mapping)


def download_trainer_model(*, trainer_url_value: Any, artifact_url: Any, framework: str = "") -> Dict[str, Any]:
    base_url = save_trainer_url(trainer_url_value or trainer_url())
    url = _trainer_absolute_url(base_url, artifact_url)
    if not url:
        raise ValueError("Choose an openWakeWord trainer model first.")
    filename = _safe_filename(Path(urlparse(url).path).name)
    suffix = Path(filename).suffix.lower().lstrip(".")
    requested_framework = _lower(framework)
    if requested_framework in {"onnx", "tflite"} and suffix != requested_framework:
        logger.info(
            "[native-voice] openWakeWord trainer artifact framework differs from selected runtime requested=%s artifact=%s",
            requested_framework,
            suffix,
        )
    target = OPENWAKEWORD_MODEL_ROOT / "trainer" / (_slug(Path(filename).stem) or "custom") / filename
    path = _download_file(url, target, force=True)
    save_model_source(str(path), framework=suffix)
    return {
        "ok": True,
        "trainer_url": base_url,
        "url": url,
        "path": str(path),
        "model_source": str(path),
        "framework": suffix,
        "name": filename,
        "option": _local_model_option(path),
    }


def _resource_url_for_framework(url: str, framework: str) -> str:
    token = _text(url)
    if framework == "onnx":
        return token.replace(".tflite", ".onnx")
    return token.replace(".onnx", ".tflite")


def _resource_path_for_framework(path: str, framework: str) -> str:
    token = _text(path)
    if framework == "onnx":
        return token.replace(".tflite", ".onnx")
    return token.replace(".onnx", ".tflite")


def _pretrained_model_path(openwakeword_mod: Any, model_source: str, framework: str) -> Path:
    model_key = _slug(model_source)
    models = getattr(openwakeword_mod, "MODELS", {}) or {}
    if model_key not in models and model_source == "current_weather":
        model_key = "weather"
    if model_key not in models:
        raise RuntimeError(f"Unknown openWakeWord pretrained model: {model_source}")
    row = models[model_key]
    url = _resource_url_for_framework(_text(row.get("download_url")), framework)
    source_path = Path(_resource_path_for_framework(_text(row.get("model_path")), framework))
    target = OPENWAKEWORD_MODEL_ROOT / "pretrained" / source_path.name
    return _download_file(url, target)


def _feature_model_path(openwakeword_mod: Any, key: str, framework: str) -> Path:
    features = getattr(openwakeword_mod, "FEATURE_MODELS", {}) or {}
    row = features.get(key) if isinstance(features, dict) else None
    if not isinstance(row, dict):
        raise RuntimeError(f"Missing openWakeWord feature model metadata: {key}")
    url = _resource_url_for_framework(_text(row.get("download_url")), framework)
    source_path = Path(_resource_path_for_framework(_text(row.get("model_path")), framework))
    target = OPENWAKEWORD_MODEL_ROOT / "features" / source_path.name
    return _download_file(url, target)


def _resolve_model_path(openwakeword_mod: Any, source: str, framework: str) -> Path:
    token = _text(source).strip()
    if not token:
        token = DEFAULT_OPENWAKEWORD_MODEL_SOURCE
    if token.startswith(("http://", "https://")):
        name = Path(token.split("?", 1)[0]).name or f"openwakeword.{framework}"
        return _download_file(token, OPENWAKEWORD_MODEL_ROOT / "custom" / name)
    if _looks_like_model_path(token):
        normalized, _normalized_framework = normalize_model_source(token, framework=framework, copy_external=True)
        return Path(normalized)
    return _pretrained_model_path(openwakeword_mod, token, framework)


def _requested_device(settings: Dict[str, Any]) -> str:
    device = _lower(settings.get("device")) or DEFAULT_OPENWAKEWORD_DEVICE
    if device in {"gpu", "cuda"}:
        return "gpu"
    if device == "cpu":
        return "cpu"
    with contextlib.suppress(Exception):
        import onnxruntime as ort

        providers = set(ort.get_available_providers() or [])
        if "CUDAExecutionProvider" in providers:
            return "gpu"
    return "cpu"


def _exception_chain_text(exc: BaseException) -> str:
    parts: list[str] = []
    current: Optional[BaseException] = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(part for part in parts if part).strip()


def _looks_like_cuda_startup_error(exc: BaseException) -> bool:
    token = _exception_chain_text(exc).lower()
    if not token:
        return False
    return any(
        marker in token
        for marker in (
            "cuda",
            "cublas",
            "cudnn",
            "cudaexecutionprovider",
            "cuda execution provider",
            "resource allocation failed",
        )
    )


def _cuda_fallback_active() -> bool:
    return time.time() < float(_CUDA_FALLBACK_UNTIL_TS or 0.0)


def _mark_cuda_fallback(reason: str) -> None:
    global _CUDA_FALLBACK_UNTIL_TS, _CUDA_FALLBACK_REASON
    _CUDA_FALLBACK_UNTIL_TS = time.time() + _CUDA_FALLBACK_SECONDS
    _CUDA_FALLBACK_REASON = _text(reason)[:1000]


def _cpu_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    fallback = dict(settings)
    fallback["device"] = "cpu"
    return fallback


class _DetectorState:
    def __init__(self, settings: Dict[str, Any], *, selector: str = "") -> None:
        import numpy as np  # noqa: F401
        import openwakeword
        from openwakeword.model import Model

        framework = _text(settings.get("inference_framework")) or DEFAULT_OPENWAKEWORD_INFERENCE_FRAMEWORK
        model_source = _text(settings.get("model_source")) or DEFAULT_OPENWAKEWORD_MODEL_SOURCE
        model_path = _resolve_model_path(openwakeword, model_source, framework)
        melspec_path = _feature_model_path(openwakeword, "melspectrogram", framework)
        embedding_path = _feature_model_path(openwakeword, "embedding", framework)
        device = _requested_device(settings)
        self.model = Model(
            wakeword_models=[str(model_path)],
            inference_framework=framework,
            vad_threshold=float(settings.get("vad_threshold") or 0.0),
            melspec_model_path=str(melspec_path),
            embedding_model_path=str(embedding_path),
            device=device,
        )
        self.selector = selector
        self.framework = framework
        self.device = device
        self.model_source = model_source
        self.model_path = str(model_path)
        self.feature_paths = [str(melspec_path), str(embedding_path)]
        self.lock = threading.Lock()
        self.ratecv_state: Any = None
        self.counts: Dict[str, int] = {}
        self.last_detection_ts = 0.0
        self.last_seen_ts = time.time()

    def reset(self) -> None:
        with contextlib.suppress(Exception):
            self.model.reset()
        self.ratecv_state = None
        self.counts = {}


def _engine_key(settings: Dict[str, Any]) -> str:
    return "|".join(
        [
            _text(settings.get("model_source")),
            _text(settings.get("inference_framework")),
            _text(settings.get("threshold")),
            _text(settings.get("patience")),
            _text(settings.get("debounce_s")),
            _text(settings.get("vad_threshold")),
            _text(settings.get("device")),
        ]
    )


def _new_detector(settings: Dict[str, Any], *, selector: str = "") -> _DetectorState:
    requested_device = _requested_device(settings)
    if requested_device != "cpu" and _cuda_fallback_active():
        return _DetectorState(_cpu_settings(settings), selector=selector)
    try:
        return _DetectorState(settings, selector=selector)
    except Exception as exc:
        if requested_device != "cpu" and _looks_like_cuda_startup_error(exc):
            reason = _exception_chain_text(exc) or type(exc).__name__
            _mark_cuda_fallback(reason)
            logger.warning(
                "[native-voice] openWakeWord CUDA startup failed; falling back to CPU for %.0fs selector=%s detail=%s",
                float(_CUDA_FALLBACK_SECONDS),
                selector,
                reason,
            )
            return _DetectorState(_cpu_settings(settings), selector=selector)
        raise


def _cleanup_idle_detectors_locked(now_ts: float) -> None:
    global _LAST_DETECTOR_CLEANUP_TS, _WARM_DETECTOR
    if (now_ts - float(_LAST_DETECTOR_CLEANUP_TS or 0.0)) < float(_DETECTOR_CLEANUP_INTERVAL_S):
        return
    _LAST_DETECTOR_CLEANUP_TS = now_ts
    expired: list[str] = []
    for selector, detector in list(_DETECTORS.items()):
        if selector.startswith("__"):
            continue
        if (now_ts - float(getattr(detector, "last_seen_ts", now_ts) or now_ts)) >= float(_DETECTOR_IDLE_TTL_S):
            expired.append(selector)
    for selector in expired:
        detector = _DETECTORS.pop(selector, None)
        if detector is _WARM_DETECTOR:
            _WARM_DETECTOR = None
    if expired:
        logger.info("[native-voice] cleaned up idle openWakeWord detectors selectors=%s", ",".join(sorted(expired)))


def _ensure_detector(selector: str) -> _DetectorState:
    global _ENGINE_KEY, _ENGINE_ERROR, _ENGINE_DETAIL, _WARM_DETECTOR
    settings = settings_snapshot()
    key = _engine_key(settings)
    selector_token = _text(selector) or "default"
    now_ts = time.time()
    with _ENGINE_LOCK:
        if _ENGINE_KEY and _ENGINE_KEY != key:
            _DETECTORS.clear()
            _WARM_DETECTOR = None
        _ENGINE_KEY = key
        _cleanup_idle_detectors_locked(now_ts)
        detector = _DETECTORS.get(selector_token)
        if detector is not None:
            detector.last_seen_ts = now_ts
            return detector
    with _DETECTOR_INIT_LOCK:
        with _ENGINE_LOCK:
            if _ENGINE_KEY and _ENGINE_KEY != key:
                _DETECTORS.clear()
                _WARM_DETECTOR = None
            _ENGINE_KEY = key
            _cleanup_idle_detectors_locked(now_ts)
            detector = _DETECTORS.get(selector_token)
            if detector is not None:
                detector.last_seen_ts = now_ts
                return detector
            if _WARM_DETECTOR is not None and not selector_token.startswith("__"):
                detector = _WARM_DETECTOR
                for warm_key, warm_detector in list(_DETECTORS.items()):
                    if warm_detector is detector:
                        _DETECTORS.pop(warm_key, None)
                detector.selector = selector_token
                detector.reset()
                detector.last_seen_ts = now_ts
                _DETECTORS[selector_token] = detector
                _WARM_DETECTOR = None
                return detector
        try:
            detector = _new_detector(settings, selector=selector_token)
        except Exception as exc:
            _ENGINE_ERROR = str(exc) or type(exc).__name__
            _ENGINE_DETAIL = {"settings": settings, "error": _ENGINE_ERROR}
            raise
    with _ENGINE_LOCK:
        _ENGINE_ERROR = ""
        _ENGINE_DETAIL = {
            "model_source": detector.model_source,
            "model_path": detector.model_path,
            "feature_paths": list(detector.feature_paths),
            "framework": detector.framework,
            "device": detector.device,
        }
        detector.last_seen_ts = now_ts
        _DETECTORS[selector_token] = detector
    return detector


def warmup_model(*, enabled_only: bool = True) -> str:
    global _WARM_DETECTOR
    settings = settings_snapshot()
    if enabled_only and not bool(settings.get("enabled")):
        return "skipped openWakeWord disabled"
    key = _engine_key(settings)
    with _ENGINE_LOCK:
        if _ENGINE_KEY == key and _WARM_DETECTOR is not None:
            detector = _WARM_DETECTOR
            return f"loaded openWakeWord model {detector.model_source} on {detector.device}/{detector.framework}"
    try:
        detector = _ensure_detector("__warmup__")
    except Exception as exc:
        raise RuntimeError(str(exc) or type(exc).__name__) from exc
    with _ENGINE_LOCK:
        _WARM_DETECTOR = detector
    return f"loaded openWakeWord model {detector.model_source} on {detector.device}/{detector.framework}"


def runtime_availability() -> Dict[str, Any]:
    settings = settings_snapshot()
    with _ENGINE_LOCK:
        detail = dict(_ENGINE_DETAIL)
        error = _ENGINE_ERROR
    return {
        "enabled": bool(settings.get("enabled")),
        "available": not bool(error),
        "label": "available" if not error else "unavailable",
        "detail": error,
        "gpu_fallback_active": _cuda_fallback_active(),
        "gpu_fallback_until": float(_CUDA_FALLBACK_UNTIL_TS or 0.0),
        "gpu_fallback_reason": _CUDA_FALLBACK_REASON,
        "settings": settings,
        **detail,
    }


def _pcm_to_pcm16_mono_16k(
    detector: _DetectorState,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data:
        return b""
    rate = int(audio_format.get("rate") or 16000)
    width = int(audio_format.get("width") or 2)
    channels = int(audio_format.get("channels") or 1)

    if width != 2:
        if _AUDIOOP is None:
            return b""
        with contextlib.suppress(Exception):
            data = _AUDIOOP.lin2lin(data, width, 2)
            width = 2
    if width != 2:
        return b""

    if channels > 1:
        if _AUDIOOP is None:
            return b""
        with contextlib.suppress(Exception):
            data = _AUDIOOP.tomono(data, 2, 0.5, 0.5)
            channels = 1
    if channels != 1:
        return b""

    if rate != 16000:
        if _AUDIOOP is None:
            return b""
        with contextlib.suppress(Exception):
            data, detector.ratecv_state = _AUDIOOP.ratecv(data, 2, 1, rate, 16000, detector.ratecv_state)
            rate = 16000
    if rate != 16000:
        return b""
    return data


def process_audio(selector: str, audio_bytes: bytes, audio_format: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not openwakeword_enabled():
        return None
    settings = settings_snapshot()
    detector = _ensure_detector(selector)
    detector.last_seen_ts = time.time()
    threshold = float(settings.get("threshold") or DEFAULT_OPENWAKEWORD_THRESHOLD)
    patience = int(settings.get("patience") or DEFAULT_OPENWAKEWORD_PATIENCE)
    debounce_s = float(settings.get("debounce_s") or DEFAULT_OPENWAKEWORD_DEBOUNCE_S)

    with detector.lock:
        pcm = _pcm_to_pcm16_mono_16k(detector, audio_bytes, audio_format)
        if not pcm:
            return None
        import numpy as np

        samples = np.frombuffer(pcm, dtype=np.int16)
        if samples.size <= 0:
            return None
        predictions = detector.model.predict(samples)
        if not isinstance(predictions, dict) or not predictions:
            return None
        best_label, best_score = max(
            ((_text(label), float(score or 0.0)) for label, score in predictions.items()),
            key=lambda item: item[1],
        )
        now_ts = time.time()
        for label in list(detector.counts.keys()):
            if label != best_label:
                detector.counts[label] = 0
        if best_score >= threshold:
            detector.counts[best_label] = int(detector.counts.get(best_label, 0)) + 1
        else:
            detector.counts[best_label] = 0
        if (
            best_score >= threshold
            and int(detector.counts.get(best_label, 0)) >= patience
            and (now_ts - float(detector.last_detection_ts or 0.0)) >= debounce_s
        ):
            detector.last_detection_ts = now_ts
            detector.reset()
            return {
                "detected": True,
                "wake_word": best_label,
                "score": best_score,
                "threshold": threshold,
                "patience": patience,
                "engine": "openwakeword",
                "model_source": detector.model_source,
            }
    return None
