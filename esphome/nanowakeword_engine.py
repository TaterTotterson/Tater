from __future__ import annotations

import audioop as _AUDIOOP
import contextlib
import json
import logging
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib import request as urllib_request
from urllib.parse import unquote, urljoin, urlparse

from helpers import redis_client


logger = logging.getLogger(__name__)

NANOWAKEWORD_SETTINGS_HASH_KEY = "voice_nanowakeword_settings"
NANOWAKEWORD_TRAINER_URL_KEY = "voice_nanowakeword:trainer_url"
NANOWAKEWORD_MODEL_ROOT = Path(__file__).resolve().parents[1] / "agent_lab" / "models" / "nanowakeword"

DEFAULT_NANOWAKEWORD_TRAINER_URL = "http://127.0.0.1:8792"
DEFAULT_NANOWAKEWORD_ENABLED = True
DEFAULT_NANOWAKEWORD_MODEL_SOURCE = ""
DEFAULT_NANOWAKEWORD_THRESHOLD = 0.90
DEFAULT_NANOWAKEWORD_PATIENCE = 2
DEFAULT_NANOWAKEWORD_DEBOUNCE_S = 4.0
DEFAULT_NANOWAKEWORD_DIAGNOSTIC_LOGGING = False
_MODEL_SUFFIXES = {".onnx", ".pt", ".pth"}

_ENGINE_LOCK = threading.RLock()
_DETECTORS: Dict[str, "_DetectorState"] = {}
_WARM_DETECTOR: Optional["_DetectorState"] = None
_ENGINE_KEY = ""
_ENGINE_ERROR = ""
_DETECTOR_IDLE_TTL_S = 3600.0
_DETECTOR_CLEANUP_INTERVAL_S = 60.0
_LAST_DETECTOR_CLEANUP_TS = 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_bool(value: Any, default: bool) -> bool:
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
        parsed = float(_text(value) or default)
    except Exception:
        parsed = float(default)
    return max(float(minimum), min(float(maximum), parsed))


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value) or default))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _setting(name: str, default: Any = "") -> Any:
    key = f"VOICE_NANOWAKEWORD_{name.upper()}"
    with contextlib.suppress(Exception):
        raw = redis_client.hget("voice_core_settings", key)
        if raw not in (None, ""):
            return raw
    with contextlib.suppress(Exception):
        raw = redis_client.hget(NANOWAKEWORD_SETTINGS_HASH_KEY, name)
        if raw not in (None, ""):
            return raw
    return default


def settings_snapshot() -> Dict[str, Any]:
    return {
        "enabled": _as_bool(_setting("enabled", DEFAULT_NANOWAKEWORD_ENABLED), DEFAULT_NANOWAKEWORD_ENABLED),
        "model_source": _text(_setting("model_source", DEFAULT_NANOWAKEWORD_MODEL_SOURCE)),
        "threshold": _as_float(_setting("threshold", DEFAULT_NANOWAKEWORD_THRESHOLD), DEFAULT_NANOWAKEWORD_THRESHOLD, minimum=0.01, maximum=0.99),
        "patience": _as_int(_setting("patience", DEFAULT_NANOWAKEWORD_PATIENCE), DEFAULT_NANOWAKEWORD_PATIENCE, minimum=1, maximum=10),
        "debounce_s": _as_float(_setting("debounce_s", DEFAULT_NANOWAKEWORD_DEBOUNCE_S), DEFAULT_NANOWAKEWORD_DEBOUNCE_S, minimum=0.0, maximum=30.0),
        "diagnostic_logging": _as_bool(
            _setting("diagnostic_logging", DEFAULT_NANOWAKEWORD_DIAGNOSTIC_LOGGING),
            DEFAULT_NANOWAKEWORD_DIAGNOSTIC_LOGGING,
        ),
    }


def nanowakeword_enabled() -> bool:
    settings = settings_snapshot()
    return bool(settings.get("enabled")) and bool(_text(settings.get("model_source")))


def _safe_filename(value: Any, *, fallback: str = "nanowakeword.onnx") -> str:
    token = Path(unquote(_text(value).split("?", 1)[0])).name.strip()
    clean = "".join(ch for ch in token if ch.isalnum() or ch in {".", "-", "_"}).strip("._")
    clean = clean or fallback
    if Path(clean).suffix.lower() not in _MODEL_SUFFIXES:
        raise ValueError("NanoWakeWord model artifact must be an .onnx, .pt, or .pth file.")
    return clean


def _slug(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    return "_".join(part for part in token.split("_") if part)


def _download_file(url: str, target: Path, *, timeout: float = 120.0, force: bool = False) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if not force and target.exists() and target.stat().st_size > 0:
        return target
    logger.info("[native-voice] downloading NanoWakeWord model url=%s target=%s", url, target)
    with urllib_request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - trusted model URLs from settings.
        payload = response.read()
    if not payload:
        raise RuntimeError(f"Downloaded empty NanoWakeWord model: {url}")
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_bytes(payload)
    tmp.replace(target)
    return target


def _local_model_matches(filename: str) -> list[Path]:
    name = _text(filename)
    if not name or not NANOWAKEWORD_MODEL_ROOT.exists():
        return []
    return [
        path
        for path in sorted(NANOWAKEWORD_MODEL_ROOT.rglob(name))
        if path.is_file() and path.suffix.lower() in _MODEL_SUFFIXES
    ]


def _local_model_option(path: Path) -> Dict[str, Any]:
    try:
        rel = path.relative_to(NANOWAKEWORD_MODEL_ROOT)
        source = rel.parts[0] if rel.parts else "local"
    except Exception:
        rel = path
        source = "local"
    suffix = path.suffix.lower().lstrip(".").upper()
    stem = path.stem or path.name
    return {
        "value": str(path),
        "label": f"{stem} ({suffix})",
        "name": path.name,
        "source": source,
    }


def _local_model_options() -> list[Dict[str, Any]]:
    if not NANOWAKEWORD_MODEL_ROOT.exists():
        return []
    rows = [
        _local_model_option(path)
        for path in sorted(NANOWAKEWORD_MODEL_ROOT.rglob("*"))
        if path.is_file() and path.suffix.lower() in _MODEL_SUFFIXES
    ]
    rows.sort(key=lambda row: (_text(row.get("source")).lower(), _text(row.get("label")).lower()))
    return rows


def model_source_options(*, current: Any = "") -> list[Dict[str, Any]]:
    current_value = _text(current)
    local = _local_model_options()
    seen = {_text(row.get("value")) for row in local if isinstance(row, dict)}
    rows: list[Dict[str, Any]] = [{"value": "", "label": "Choose NanoWakeWord model"}]
    if current_value and current_value not in seen:
        rows.append(
            {
                "value": current_value,
                "label": f"Current: {Path(current_value).name if '/' in current_value else current_value}",
                "source": "current",
            }
        )
    if local:
        rows.append({"label": "Downloaded / Local", "options": local})
    return rows


def _normalize_trainer_url(value: Any) -> str:
    token = _text(value)
    if not token:
        token = DEFAULT_NANOWAKEWORD_TRAINER_URL
    if "://" not in token:
        token = f"http://{token}"
    return token.rstrip("/")


def trainer_url() -> str:
    with contextlib.suppress(Exception):
        stored = _text(redis_client.get(NANOWAKEWORD_TRAINER_URL_KEY))
        if stored:
            return _normalize_trainer_url(stored)
    return DEFAULT_NANOWAKEWORD_TRAINER_URL


def save_trainer_url(value: Any) -> str:
    normalized = _normalize_trainer_url(value)
    with contextlib.suppress(Exception):
        redis_client.set(NANOWAKEWORD_TRAINER_URL_KEY, normalized)
    return normalized


def _trainer_absolute_url(base_url: str, value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    if token.startswith(("http://", "https://")):
        return token
    if not token.startswith("/"):
        token = f"/api/artifacts/{token}"
    return urljoin(f"{_normalize_trainer_url(base_url)}/", token.lstrip("/"))


def _trainer_entries_from_payload(payload: Any, *, base_url: str) -> list[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_rows = payload.get("items") or payload.get("artifacts") or payload.get("models") or payload.get("entries") or []
    if not isinstance(raw_rows, list):
        return []
    rows: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        name = _text(raw.get("name") or raw.get("filename") or raw.get("file") or raw.get("path"))
        raw_url = raw.get("url") or raw.get("download_url") or raw.get("model_url") or raw.get("artifact_url") or name
        url = _trainer_absolute_url(base_url, raw_url)
        parsed_name = Path(urlparse(url).path).name
        filename = name or parsed_name
        suffix = Path(filename or parsed_name).suffix.lower()
        if suffix not in _MODEL_SUFFIXES:
            continue
        key = url or filename
        if not key or key in seen:
            continue
        seen.add(key)
        stem = Path(filename).stem or filename
        label = _text(raw.get("label") or raw.get("title") or stem) or stem
        rows.append(
            {
                "value": url,
                "label": f"{label} ({suffix.lstrip('.').upper()})",
                "name": filename,
                "stem": stem,
                "format": suffix.lstrip("."),
                "size": raw.get("size") or 0,
                "mtime": raw.get("mtime") or 0,
                "url": url,
            }
        )
    rows.sort(key=lambda row: (_text(row.get("label")).lower(), _text(row.get("name")).lower()))
    return rows


def trainer_model_catalog(*, trainer_url_value: Any = "") -> Dict[str, Any]:
    base_url = save_trainer_url(trainer_url_value or trainer_url())
    catalog_paths = ("api/artifacts", "api/trained_wake_words/catalog")
    last_error: Optional[Exception] = None
    empty_payload: Dict[str, Any] = {}
    for catalog_path in catalog_paths:
        catalog_url = urljoin(f"{base_url}/", catalog_path)
        try:
            with urllib_request.urlopen(catalog_url, timeout=20) as response:  # noqa: S310 - user-configured local trainer URL.
                payload = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
            items = _trainer_entries_from_payload(payload, base_url=base_url)
            result = {
                "ok": True,
                "trainer_url": base_url,
                "catalog_url": catalog_url,
                "items": items,
                "count": len(items),
            }
            if items:
                return result
            empty_payload = result
        except Exception as exc:
            last_error = exc
    if empty_payload:
        return empty_payload
    if last_error is not None:
        raise last_error
    return {"ok": True, "trainer_url": base_url, "items": [], "count": 0}


def _model_root_alias(path_value: Any) -> Optional[Path]:
    token = _text(path_value)
    if not token:
        return None
    parts = Path(token).parts
    markers = [
        ("agent_lab", "models", "nanowakeword"),
        ("models", "nanowakeword"),
        ("models",),
    ]
    for marker in markers:
        for idx in range(0, max(0, len(parts) - len(marker) + 1)):
            if tuple(parts[idx : idx + len(marker)]) != marker:
                continue
            tail = parts[idx + len(marker) :]
            if not tail:
                continue
            candidate = NANOWAKEWORD_MODEL_ROOT.joinpath(*tail)
            if candidate.exists() and candidate.is_file():
                return candidate
    return None


def _copy_external_model(path: Path) -> Path:
    if not path.exists() or not path.is_file():
        raise RuntimeError(f"NanoWakeWord model path is not accessible from Tater: {path}")
    if path.suffix.lower() not in _MODEL_SUFFIXES:
        raise ValueError("NanoWakeWord model path must end in .onnx, .pt, or .pth.")
    with contextlib.suppress(Exception):
        path.relative_to(NANOWAKEWORD_MODEL_ROOT)
        return path
    target = NANOWAKEWORD_MODEL_ROOT / "custom" / (_slug(path.stem) or "custom") / path.name
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists() or target.stat().st_size != path.stat().st_size:
        shutil.copy2(path, target)
    return target


def normalize_model_source(source: Any, *, copy_external: bool = True) -> str:
    source_text = _text(source)
    if not source_text:
        return ""
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        suffix = Path(parsed.path).suffix.lower()
        if suffix not in _MODEL_SUFFIXES:
            raise ValueError("NanoWakeWord model URL must end in .onnx, .pt, or .pth.")
        return source_text
    candidate = Path(source_text).expanduser()
    if candidate.exists() and candidate.is_file():
        return str(_copy_external_model(candidate) if copy_external else candidate)
    alias = _model_root_alias(source_text)
    if alias is not None:
        return str(alias)
    matches = _local_model_matches(Path(source_text).name)
    if len(matches) == 1:
        return str(matches[0])
    if len(matches) > 1:
        labels = ", ".join(str(path.relative_to(NANOWAKEWORD_MODEL_ROOT)) for path in matches[:5])
        raise RuntimeError(f"Multiple local NanoWakeWord models match {Path(source_text).name}: {labels}")
    if Path(source_text).suffix.lower() in _MODEL_SUFFIXES:
        raise RuntimeError(
            "NanoWakeWord model path is not accessible from Tater. "
            "Place it under agent_lab/models/nanowakeword or use an HTTP(S) model URL."
        )
    return source_text


def save_model_source(source: Any) -> None:
    source_text = normalize_model_source(source, copy_external=True)
    with contextlib.suppress(Exception):
        redis_client.hset("voice_core_settings", mapping={"VOICE_NANOWAKEWORD_MODEL_SOURCE": source_text})
        redis_client.hset(NANOWAKEWORD_SETTINGS_HASH_KEY, mapping={"model_source": source_text})


def download_trainer_model(*, trainer_url_value: Any, artifact_url: Any) -> Dict[str, Any]:
    base_url = save_trainer_url(trainer_url_value or trainer_url())
    url = _trainer_absolute_url(base_url, artifact_url)
    if not url:
        raise ValueError("Choose a NanoWakeWord trainer model first.")
    filename = _safe_filename(Path(urlparse(url).path).name)
    target = NANOWAKEWORD_MODEL_ROOT / "trainer" / (_slug(Path(filename).stem) or "custom") / filename
    path = _download_file(url, target, force=True)
    save_model_source(str(path))
    reset_detectors()
    return {
        "ok": True,
        "trainer_url": base_url,
        "url": url,
        "path": str(path),
        "model_source": str(path),
        "name": filename,
        "option": _local_model_option(path),
    }


def _resolve_model_path(source: str) -> Path:
    source_text = normalize_model_source(source, copy_external=True)
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        filename = _safe_filename(source_text)
        return _download_file(source_text, NANOWAKEWORD_MODEL_ROOT / "custom" / filename)
    path = Path(source_text).expanduser()
    if not path.exists() or not path.is_file():
        raise RuntimeError("NanoWakeWord model path is not accessible from Tater.")
    return path


class _DetectorState:
    def __init__(self, settings: Dict[str, Any], *, selector: str = "") -> None:
        import numpy as np  # noqa: F401
        from nanowakeword import NanoInterpreter

        model_source = _text(settings.get("model_source"))
        model_path = _resolve_model_path(model_source)
        self.model = NanoInterpreter.load_model(str(model_path))
        self.selector = selector
        self.model_source = model_source
        self.model_path = str(model_path)
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
            _text(settings.get("threshold")),
            _text(settings.get("patience")),
            _text(settings.get("debounce_s")),
        ]
    )


def _new_detector(settings: Dict[str, Any], *, selector: str = "") -> _DetectorState:
    return _DetectorState(settings, selector=selector)


def _cleanup_idle_detectors_locked(now_ts: float) -> None:
    global _LAST_DETECTOR_CLEANUP_TS, _WARM_DETECTOR
    if (now_ts - float(_LAST_DETECTOR_CLEANUP_TS or 0.0)) < float(_DETECTOR_CLEANUP_INTERVAL_S):
        return
    _LAST_DETECTOR_CLEANUP_TS = now_ts
    expired = [
        selector
        for selector, detector in list(_DETECTORS.items())
        if not selector.startswith("__") and (now_ts - float(detector.last_seen_ts or now_ts)) >= _DETECTOR_IDLE_TTL_S
    ]
    for selector in expired:
        detector = _DETECTORS.pop(selector, None)
        if detector is _WARM_DETECTOR:
            _WARM_DETECTOR = None
    if expired:
        logger.info("[native-voice] cleaned up idle NanoWakeWord detectors selectors=%s", ",".join(sorted(expired)))


def _ensure_detector(selector: str) -> _DetectorState:
    global _ENGINE_KEY, _ENGINE_ERROR, _WARM_DETECTOR
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
            logger.info("[native-voice] assigned warm NanoWakeWord detector selector=%s model=%s", selector_token, detector.model_source)
            return detector
    try:
        detector = _new_detector(settings, selector=selector_token)
    except Exception as exc:
        _ENGINE_ERROR = str(exc) or type(exc).__name__
        raise
    with _ENGINE_LOCK:
        _ENGINE_ERROR = ""
        existing = _DETECTORS.get(selector_token)
        if existing is not None:
            return existing
        _DETECTORS[selector_token] = detector
        logger.info("[native-voice] loaded NanoWakeWord detector selector=%s model=%s", selector_token, detector.model_source)
        return detector


def reset_detectors() -> None:
    global _WARM_DETECTOR, _ENGINE_KEY, _ENGINE_ERROR
    with _ENGINE_LOCK:
        _DETECTORS.clear()
        _WARM_DETECTOR = None
        _ENGINE_KEY = ""
        _ENGINE_ERROR = ""


def warmup_model(*, enabled_only: bool = True) -> str:
    global _WARM_DETECTOR, _ENGINE_ERROR
    if enabled_only and not nanowakeword_enabled():
        return "skipped NanoWakeWord disabled or missing model"
    settings = settings_snapshot()
    detector = _new_detector(settings, selector="__warmup__")
    with _ENGINE_LOCK:
        _WARM_DETECTOR = detector
        _DETECTORS["__warmup__"] = detector
        _ENGINE_ERROR = ""
    return f"loaded NanoWakeWord model {detector.model_source}"


def status() -> Dict[str, Any]:
    settings = settings_snapshot()
    with _ENGINE_LOCK:
        return {
            "ok": True,
            "enabled": nanowakeword_enabled(),
            "available": not bool(_ENGINE_ERROR),
            "error": _ENGINE_ERROR,
            "settings": settings,
            "detector_count": len(_DETECTORS),
            "warm_detector_loaded": _WARM_DETECTOR is not None,
            "detectors": {
                key: {
                    "last_seen_ts": detector.last_seen_ts,
                    "last_detection_ts": detector.last_detection_ts,
                    "model_source": detector.model_source,
                    "model_path": detector.model_path,
                }
                for key, detector in _DETECTORS.items()
            },
        }


def _pcm_to_pcm16_mono_16k(detector: _DetectorState, audio_bytes: bytes, audio_format: Dict[str, Any]) -> bytes:
    data = bytes(audio_bytes or b"")
    if not data:
        return b""
    rate = int(audio_format.get("rate") or 16000)
    width = int(audio_format.get("width") or 2)
    channels = int(audio_format.get("channels") or 1)
    if width not in {1, 2, 3, 4}:
        return b""
    if width != 2:
        with contextlib.suppress(Exception):
            data = _AUDIOOP.lin2lin(data, width, 2)
            width = 2
    if width != 2:
        return b""
    if channels <= 0:
        channels = 1
    if channels > 1:
        with contextlib.suppress(Exception):
            data = _AUDIOOP.tomono(data, width, 0.5, 0.5)
            channels = 1
    if channels != 1:
        return b""
    if rate != 16000:
        with contextlib.suppress(Exception):
            data, detector.ratecv_state = _AUDIOOP.ratecv(data, 2, 1, rate, 16000, detector.ratecv_state)
            rate = 16000
    if rate != 16000:
        return b""
    return data


def _float_score(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _result_label_score(result: Any, fallback_label: str) -> Tuple[str, float]:
    label = _text(fallback_label) or "nanowakeword"
    if isinstance(result, dict):
        for key in ("score", "probability", "confidence"):
            if key in result:
                return label, _float_score(result.get(key))
        scored = [
            (_text(key), _float_score(value))
            for key, value in result.items()
            if _text(key)
        ]
        if scored:
            return max(scored, key=lambda item: item[1])
        return label, 0.0
    for attr in ("score", "probability", "confidence"):
        if hasattr(result, attr):
            return label, _float_score(getattr(result, attr))
    return label, 0.0


def process_audio(selector: str, audio_bytes: bytes, audio_format: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not nanowakeword_enabled():
        return None
    settings = settings_snapshot()
    detector = _ensure_detector(selector)
    detector.last_seen_ts = time.time()
    threshold = float(settings.get("threshold") or DEFAULT_NANOWAKEWORD_THRESHOLD)
    patience = int(settings.get("patience") or DEFAULT_NANOWAKEWORD_PATIENCE)
    debounce_s = float(settings.get("debounce_s") or DEFAULT_NANOWAKEWORD_DEBOUNCE_S)

    with detector.lock:
        pcm = _pcm_to_pcm16_mono_16k(detector, audio_bytes, audio_format)
        if not pcm:
            return None
        import numpy as np

        samples = np.frombuffer(pcm, dtype=np.int16)
        if samples.size <= 0:
            return None
        best_label = Path(detector.model_path).stem or "nanowakeword"
        best_score = 0.0
        frame_size = 1280
        if samples.size <= frame_size:
            frames = [samples]
        else:
            frames = []
            for start in range(0, samples.size, frame_size):
                frame = samples[start : start + frame_size]
                if frame.size < frame_size:
                    frame = np.pad(frame, (0, frame_size - frame.size), mode="constant")
                frames.append(frame)

        for frame in frames:
            result = detector.model.predict(frame)
            label, score = _result_label_score(result, best_label)
            if score >= best_score:
                best_score = score
                best_label = label or best_label
            now_ts = time.time()
            if score >= threshold:
                detector.counts[label] = int(detector.counts.get(label, 0)) + 1
            else:
                detector.counts[label] = 0
            if (
                score >= threshold
                and int(detector.counts.get(label, 0)) >= patience
                and (now_ts - float(detector.last_detection_ts or 0.0)) >= debounce_s
            ):
                detector.last_detection_ts = now_ts
                detector.reset()
                return {
                    "detected": True,
                    "wake_word": label or best_label,
                    "score": score,
                    "threshold": threshold,
                    "patience": patience,
                    "engine": "nanowakeword",
                    "model_source": detector.model_source,
                    "diagnostic_logging": bool(settings.get("diagnostic_logging")),
                }
        return {
            "detected": False,
            "best_label": best_label,
            "score": best_score,
            "threshold": threshold,
            "patience": patience,
            "hit_count": int(detector.counts.get(best_label, 0)),
            "engine": "nanowakeword",
            "model_source": detector.model_source,
            "diagnostic_logging": bool(settings.get("diagnostic_logging")),
        }
    return None
