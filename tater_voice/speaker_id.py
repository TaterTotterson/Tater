from __future__ import annotations

import array
import contextlib
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from tater_paths import agent_lab_path
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
        _vp()._native_debug(f"speaker id {message}")


def _log_info(message: str, *args: Any) -> None:
    logger.info("[native-voice] speaker-id " + message, *args)


def _log_warning(message: str, *args: Any) -> None:
    logger.warning("[native-voice] speaker-id " + message, *args)


def _log_exception(message: str, *args: Any) -> None:
    logger.exception("[native-voice] speaker-id " + message, *args)


@contextlib.contextmanager
def _temporary_huggingface_env(cache_root: Optional[Path] = None):
    overrides = huggingface_environment()
    if cache_root is not None:
        hub_cache = cache_root / "hub"
        torch_cache = cache_root / "torch"
        overrides.setdefault("HF_HOME", str(cache_root))
        overrides.setdefault("HF_HUB_CACHE", str(hub_cache))
        overrides.setdefault("HUGGINGFACE_HUB_CACHE", str(hub_cache))
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


SPEAKER_ID_SETTINGS_HASH_KEY = "voice_speaker_id_settings"
SPEAKER_ID_LAST_HASH_KEY = "voice_speaker_id_last"
SPEAKER_ID_PENDING_TTL_S = 15 * 60
SPEAKER_ID_AGENT_LABS_ROOT = agent_lab_path("speaker_id")
SPEAKER_ID_MODEL_ROOT = agent_lab_path("models", "speaker_id")
SPEAKER_ID_PROFILES_PATH = SPEAKER_ID_AGENT_LABS_ROOT / "profiles.json"

DEFAULT_SPEAKER_ID_ENABLED = False
DEFAULT_SPEAKER_ID_BEST_MATCH = False
DEFAULT_SPEAKER_ID_MODEL_SOURCE = "speechbrain/spkrec-ecapa-voxceleb"
DEFAULT_SPEAKER_ID_MATCH_THRESHOLD = 0.68
DEFAULT_SPEAKER_ID_MATCH_MARGIN = 0.05
DEFAULT_SPEAKER_ID_MIN_SPEECH_S = 1.15
DEFAULT_SPEAKER_ID_ENROLL_MIN_SPEECH_S = 2.25
_SPEECHBRAIN_REPO_FILES = (
    "hyperparams.yaml",
    "embedding_model.ckpt",
    "mean_var_norm_emb.ckpt",
    "classifier.ckpt",
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
_PENDING_LOCK = threading.Lock()
_PENDING_ENROLLMENT: Dict[str, Any] = {}


def _ensure_dirs() -> None:
    SPEAKER_ID_AGENT_LABS_ROOT.mkdir(parents=True, exist_ok=True)
    SPEAKER_ID_MODEL_ROOT.mkdir(parents=True, exist_ok=True)


def settings_hash_key() -> str:
    return SPEAKER_ID_SETTINGS_HASH_KEY


def _setting_specs() -> List[Dict[str, Any]]:
    return [
        {
            "key": "VOICE_SPEAKER_ID_ENABLED",
            "label": "Enable Speaker ID",
            "type": "checkbox",
            "default": DEFAULT_SPEAKER_ID_ENABLED,
            "description": "Try to identify which enrolled person is speaking before Hydra runs.",
        },
        {
            "key": "VOICE_SPEAKER_ID_BEST_MATCH",
            "label": "Best Match Mode",
            "type": "checkbox",
            "default": DEFAULT_SPEAKER_ID_BEST_MATCH,
            "description": (
                "Always use the closest enrolled speaker instead of requiring the threshold and margin. "
                "Warning: unknown voices can be assigned to someone who is enrolled."
            ),
        },
        {
            "key": "VOICE_SPEAKER_ID_MATCH_THRESHOLD",
            "label": "Match Threshold",
            "type": "number",
            "default": DEFAULT_SPEAKER_ID_MATCH_THRESHOLD,
            "min": 0.0,
            "max": 1.0,
            "step": 0.01,
            "description": (
                "Higher values are stricter and reduce false matches. "
                "Ignored when Best Match Mode is enabled."
            ),
        },
        {
            "key": "VOICE_SPEAKER_ID_MATCH_MARGIN",
            "label": "Runner-up Margin",
            "type": "number",
            "default": DEFAULT_SPEAKER_ID_MATCH_MARGIN,
            "min": 0.0,
            "max": 1.0,
            "step": 0.01,
            "description": (
                "Require the best speaker score to beat the runner-up by at least this amount. "
                "Ignored when Best Match Mode is enabled."
            ),
        },
        {
            "key": "VOICE_SPEAKER_ID_MIN_SPEECH_S",
            "label": "Min Speech For Matching (sec)",
            "type": "number",
            "default": DEFAULT_SPEAKER_ID_MIN_SPEECH_S,
            "min": 0.4,
            "max": 15.0,
            "step": 0.05,
            "description": "Very short commands are often too brief for reliable speaker matching.",
        },
        {
            "key": "VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S",
            "label": "Min Speech For Enrollment (sec)",
            "type": "number",
            "default": DEFAULT_SPEAKER_ID_ENROLL_MIN_SPEECH_S,
            "min": 1.0,
            "max": 20.0,
            "step": 0.05,
            "description": "Ask the user to speak one clear sentence that lasts at least this long.",
        },
        {
            "key": "VOICE_SPEAKER_ID_MODEL_SOURCE",
            "label": "SpeechBrain Model",
            "type": "text",
            "default": DEFAULT_SPEAKER_ID_MODEL_SOURCE,
            "description": "SpeechBrain speaker-recognition model source used for enrollment and matching.",
        },
    ]


def _settings_map() -> Dict[str, Dict[str, Any]]:
    vp = _vp()
    out: Dict[str, Dict[str, Any]] = {}
    for spec in _setting_specs():
        key = vp._text(spec.get("key"))
        if key:
            out[key] = dict(spec)
    return out


def _settings() -> Dict[str, Any]:
    vp = _vp()
    with contextlib.suppress(Exception):
        row = redis_client.hgetall(SPEAKER_ID_SETTINGS_HASH_KEY) or {}
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


def speaker_id_enabled() -> bool:
    return _get_bool_setting("VOICE_SPEAKER_ID_ENABLED", DEFAULT_SPEAKER_ID_ENABLED)


def _best_match_enabled() -> bool:
    return _get_bool_setting("VOICE_SPEAKER_ID_BEST_MATCH", DEFAULT_SPEAKER_ID_BEST_MATCH)


def _match_threshold() -> float:
    return _get_float_setting("VOICE_SPEAKER_ID_MATCH_THRESHOLD", DEFAULT_SPEAKER_ID_MATCH_THRESHOLD, minimum=0.0, maximum=1.0)


def _match_margin() -> float:
    return _get_float_setting("VOICE_SPEAKER_ID_MATCH_MARGIN", DEFAULT_SPEAKER_ID_MATCH_MARGIN, minimum=0.0, maximum=1.0)


def _min_speech_seconds() -> float:
    return _get_float_setting("VOICE_SPEAKER_ID_MIN_SPEECH_S", DEFAULT_SPEAKER_ID_MIN_SPEECH_S, minimum=0.4, maximum=15.0)


def _save_last_result(result: Dict[str, Any]) -> None:
    with contextlib.suppress(Exception):
        redis_client.hset(SPEAKER_ID_LAST_HASH_KEY, "last_result", json.dumps(result, ensure_ascii=False))


def last_result() -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        raw = redis_client.hget(SPEAKER_ID_LAST_HASH_KEY, "last_result")
        if raw:
            parsed = json.loads(_vp()._text(raw))
            if isinstance(parsed, dict):
                return parsed
    return {}


def _enroll_min_speech_seconds() -> float:
    return _get_float_setting(
        "VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S",
        DEFAULT_SPEAKER_ID_ENROLL_MIN_SPEECH_S,
        minimum=1.0,
        maximum=20.0,
    )


def _model_source() -> str:
    return _get_text_setting("VOICE_SPEAKER_ID_MODEL_SOURCE", DEFAULT_SPEAKER_ID_MODEL_SOURCE)


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
    specs = _settings_map()
    current = _settings()
    mapping: Dict[str, str] = {}
    changed: List[str] = []

    for key, spec in specs.items():
        if key not in incoming:
            continue
        field_type = vp._lower(spec.get("type") or "text")
        raw_value = incoming.get(key)
        if field_type == "checkbox":
            normalized = "true" if vp._as_bool(raw_value, False) else "false"
        elif field_type == "number":
            number = vp._as_float(
                raw_value,
                float(spec.get("default") or 0.0),
                minimum=spec.get("min") if isinstance(spec.get("min"), (int, float)) else None,
                maximum=spec.get("max") if isinstance(spec.get("max"), (int, float)) else None,
            )
            normalized = str(number)
        else:
            normalized = vp._text(raw_value)
        if normalized != vp._text(current.get(key)):
            mapping[key] = normalized
            changed.append(key)

    if mapping:
        redis_client.hset(SPEAKER_ID_SETTINGS_HASH_KEY, mapping=mapping)
    return {"updated_count": len(changed), "changed_keys": changed, "restart_required": False}


def _slugify_token(text: Any) -> str:
    token = re.sub(r"[^a-z0-9]+", "-", str(text or "").strip().lower()).strip("-")
    return token or "speaker"


def _model_savedir(source: Optional[str] = None) -> Path:
    return SPEAKER_ID_MODEL_ROOT / _slugify_token(source or _model_source())


def _huggingface_auth_fingerprint() -> str:
    token = str(huggingface_token() or "").strip()
    if not token:
        return "none"
    return hashlib.sha256(token.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _huggingface_auth_label() -> str:
    return "configured" if _huggingface_auth_fingerprint() != "none" else "missing"


def _call_speechbrain_loader_with_hf_token(loader: Any, kwargs: Dict[str, Any]) -> Any:
    # SpeechBrain loaders can reject use_auth_token after partially building the
    # model, which causes an expensive second load. The temporary HF environment
    # above carries the token without triggering that retry path.
    return loader(**kwargs)


def _snapshot_complete(savedir: Path) -> bool:
    return all((savedir / filename).exists() for filename in _SPEECHBRAIN_REPO_FILES)


def _ensure_speechbrain_snapshot(source: str, savedir: Path, hf_cache_dir: Path) -> str:
    source_path = Path(str(source or "")).expanduser()
    if source_path.exists():
        return str(source_path)
    if _snapshot_complete(savedir):
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
    return str(savedir) if _snapshot_complete(savedir) else source


def _load_profiles() -> Dict[str, Any]:
    _ensure_dirs()
    if not SPEAKER_ID_PROFILES_PATH.is_file():
        return {"version": 1, "speakers": []}
    with contextlib.suppress(Exception):
        payload = json.loads(SPEAKER_ID_PROFILES_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload.setdefault("version", 1)
            payload.setdefault("speakers", [])
            if isinstance(payload.get("speakers"), list):
                return payload
    return {"version": 1, "speakers": []}


def _save_profiles(data: Dict[str, Any]) -> None:
    _ensure_dirs()
    payload = dict(data or {})
    payload["version"] = int(payload.get("version") or 1)
    payload["speakers"] = list(payload.get("speakers") or [])
    SPEAKER_ID_PROFILES_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_speaker_row(row: Dict[str, Any]) -> Dict[str, Any]:
    speaker = dict(row or {})
    speaker["id"] = _vp()._text(speaker.get("id")) or uuid.uuid4().hex
    speaker["name"] = _vp()._text(speaker.get("name")) or "Speaker"
    speaker["preferred_selector"] = _vp()._text(speaker.get("preferred_selector"))
    speaker["created_ts"] = float(speaker.get("created_ts") or time.time())
    speaker["updated_ts"] = float(speaker.get("updated_ts") or speaker["created_ts"])
    samples = []
    for sample in list(speaker.get("samples") or []):
        if not isinstance(sample, dict):
            continue
        embedding = sample.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            continue
        with contextlib.suppress(Exception):
            vector = [float(item) for item in embedding]
            if vector:
                samples.append(
                    {
                        "embedding": vector,
                        "created_ts": float(sample.get("created_ts") or time.time()),
                        "speech_s": float(sample.get("speech_s") or 0.0),
                    }
                )
    speaker["samples"] = samples
    return speaker


def _all_speakers() -> List[Dict[str, Any]]:
    payload = _load_profiles()
    rows = payload.get("speakers") if isinstance(payload.get("speakers"), list) else []
    return [_normalize_speaker_row(row) for row in rows if isinstance(row, dict)]


def speaker_identity_aliases() -> List[Dict[str, Any]]:
    return [
        {
            "platform": "voice_core",
            "external_id": _vp()._text(row.get("id")),
            "label": _vp()._text(row.get("name")) or _vp()._text(row.get("id")),
            "kind": "speaker_id",
        }
        for row in _all_speakers()
        if _vp()._text(row.get("id"))
    ]


def _save_speakers(rows: List[Dict[str, Any]]) -> None:
    payload = _load_profiles()
    payload["speakers"] = [_normalize_speaker_row(row) for row in rows if isinstance(row, dict)]
    _save_profiles(payload)


def _speaker_average_embedding(row: Dict[str, Any]) -> List[float]:
    samples = list(row.get("samples") or [])
    vectors = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        vector = sample.get("embedding")
        if isinstance(vector, list) and vector:
            with contextlib.suppress(Exception):
                values = [float(item) for item in vector]
                if values:
                    vectors.append(values)
    if not vectors:
        return []
    dim = len(vectors[0])
    if dim <= 0:
        return []
    totals = [0.0] * dim
    for vector in vectors:
        if len(vector) != dim:
            continue
        for index, value in enumerate(vector):
            totals[index] += float(value)
    count = max(1, len(vectors))
    avg = [value / float(count) for value in totals]
    norm = sum(value * value for value in avg) ** 0.5
    if norm <= 1e-9:
        return []
    return [value / norm for value in avg]


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    return float(sum(float(left) * float(right) for left, right in zip(a, b)))


def _pretty_timestamp(value: Any) -> str:
    with contextlib.suppress(Exception):
        ts = float(value or 0.0)
        if ts > 0.0:
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
    return "-"


def _pending_enrollment_state() -> Dict[str, Any]:
    with _PENDING_LOCK:
        pending = dict(_PENDING_ENROLLMENT or {})
    if not pending:
        return {}
    expires_ts = float(pending.get("expires_ts") or 0.0)
    if expires_ts > 0.0 and expires_ts < time.time():
        cancel_pending_enrollment()
        return {}
    return pending


def current_pending_enrollment() -> Dict[str, Any]:
    pending = _pending_enrollment_state()
    if not pending:
        return {}
    speaker_name = _vp()._text(pending.get("speaker_name"))
    selector = _vp()._text(pending.get("selector"))
    return {
        "speaker_id": _vp()._text(pending.get("speaker_id")),
        "speaker_name": speaker_name,
        "selector": selector,
        "selector_label": selector or "Any satellite",
        "armed_at": _pretty_timestamp(pending.get("armed_ts")),
        "expires_at": _pretty_timestamp(pending.get("expires_ts")),
    }


def arm_enrollment(*, speaker_id: str, speaker_name: str, selector: str = "") -> Dict[str, Any]:
    pending = {
        "speaker_id": _vp()._text(speaker_id),
        "speaker_name": _vp()._text(speaker_name) or "Speaker",
        "selector": _vp()._text(selector),
        "armed_ts": time.time(),
        "expires_ts": time.time() + SPEAKER_ID_PENDING_TTL_S,
    }
    with _PENDING_LOCK:
        _PENDING_ENROLLMENT.clear()
        _PENDING_ENROLLMENT.update(pending)
    _log_info(
        "enrollment armed speaker=%s selector=%s expires_in_s=%s",
        _vp()._text(pending.get("speaker_name")) or "Speaker",
        _vp()._text(pending.get("selector")) or "any",
        SPEAKER_ID_PENDING_TTL_S,
    )
    _debug(
        f"enrollment armed speaker={_vp()._text(pending.get('speaker_name'))!r} "
        f"selector={_vp()._text(pending.get('selector')) or 'any'} ttl_s={SPEAKER_ID_PENDING_TTL_S}"
    )
    return current_pending_enrollment()


def cancel_pending_enrollment() -> None:
    with _PENDING_LOCK:
        had_pending = bool(_PENDING_ENROLLMENT)
        _PENDING_ENROLLMENT.clear()
    if had_pending:
        _log_info("enrollment canceled")
        _debug("enrollment canceled")


def consume_pending_enrollment(selector: str = "") -> Dict[str, Any]:
    token = _vp()._text(selector)
    with _PENDING_LOCK:
        pending = dict(_PENDING_ENROLLMENT or {})
        if not pending:
            return {}
        expires_ts = float(pending.get("expires_ts") or 0.0)
        if expires_ts > 0.0 and expires_ts < time.time():
            _PENDING_ENROLLMENT.clear()
            _log_warning("enrollment expired selector=%s", token or "unknown")
            _debug(f"enrollment expired selector={token or 'unknown'}")
            return {}
        expected_selector = _vp()._text(pending.get("selector"))
        if expected_selector and expected_selector != token:
            return {}
        _PENDING_ENROLLMENT.clear()
    _log_info(
        "enrollment consumed selector=%s speaker=%s",
        token or "unknown",
        _vp()._text(pending.get("speaker_name")) or "Speaker",
    )
    _debug(
        f"enrollment consumed selector={token or 'unknown'} "
        f"speaker={_vp()._text(pending.get('speaker_name'))!r}"
    )
    return pending


def _speechbrain_import_state() -> Tuple[bool, str]:
    with contextlib.suppress(Exception):
        import speechbrain.inference.speaker  # type: ignore  # noqa: F401
        import torch  # type: ignore  # noqa: F401
        return True, ""
    try:
        import speechbrain.inference.speaker  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, f"SpeechBrain is unavailable: {exc}"
    try:
        import torch  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, f"PyTorch is unavailable: {exc}"
    return False, "Speaker ID dependencies are unavailable."


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
    # SpeechBrain/HyperPyYAML still assumes a max_depth attribute exists on the
    # ruamel loader object, but recent ESPHome releases bring in ruamel.yaml 0.19.x.
    # Add a benign default so Speaker ID can coexist with ESPHome's pinned YAML.
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
    source_key = f"{source}|{requested_device}|{auth_fingerprint}"
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
            _debug(f"dependencies unavailable source={source!r} detail={import_detail!r}")
            return False, _ENGINE_ERROR
        _apply_speechbrain_yaml_compat_shim()
        from speechbrain.inference.speaker import SpeakerRecognition  # type: ignore
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
                    _debug(f"loading model source={source!r} savedir={str(savedir)!r} device={run_device!r}")
                    with _temporary_huggingface_env(hf_cache_dir):
                        load_source = _ensure_speechbrain_snapshot(source, savedir, hf_cache_dir)
                        kwargs = {
                            "source": load_source,
                            "savedir": str(savedir),
                            "run_opts": {"device": run_device},
                        }
                        _ENGINE = _call_speechbrain_loader_with_hf_token(SpeakerRecognition.from_hparams, kwargs)
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
                    _debug(f"model loaded source={source!r} device={run_device!r}")
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
            _debug(f"model load failed source={source!r} detail={_ENGINE_ERROR!r}")
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
            elif _ENGINE_ERROR and _ENGINE_SOURCE == f"{source}|{requested_device}|{auth_fingerprint}":
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
    if enabled_only and not speaker_id_enabled():
        return "skipped Speaker ID disabled"
    source = _model_source()
    available, detail = _speechbrain_state()
    if not available:
        raise RuntimeError(detail or "SpeechBrain model is unavailable.")
    return f"loaded Speaker ID SpeechBrain model {source} on {_ENGINE_DEVICE or _vp()._speechbrain_device()}"


def _pcm_to_waveform(audio_bytes: bytes, audio_format: Dict[str, Any]) -> Any:
    available, detail = _speechbrain_state()
    if not available:
        raise RuntimeError(detail or "SpeechBrain is unavailable")
    import torch  # type: ignore

    pcm = bytes(audio_bytes or b"")
    rate = int(audio_format.get("rate") or _vp().DEFAULT_VOICE_SAMPLE_RATE_HZ)
    width = int(audio_format.get("width") or _vp().DEFAULT_VOICE_SAMPLE_WIDTH)
    channels = int(audio_format.get("channels") or _vp().DEFAULT_VOICE_CHANNELS)
    if width != 2:
        raise RuntimeError(f"Unsupported sample width for Speaker ID: {width}")

    if channels > 1:
        if _AUDIOOP is None:
            raise RuntimeError("Stereo audio requires audioop for Speaker ID conversion.")
        pcm = _AUDIOOP.tomono(pcm, width, 0.5, 0.5)
        channels = 1

    if rate != 16000:
        if _AUDIOOP is None:
            raise RuntimeError("Resampling requires audioop for Speaker ID conversion.")
        pcm, _ = _AUDIOOP.ratecv(pcm, width, channels, rate, 16000, None)
        rate = 16000

    if not pcm:
        raise RuntimeError("No audio available for Speaker ID.")

    samples = array.array("h")
    samples.frombytes(pcm)
    if sys.byteorder != "little":
        samples.byteswap()
    tensor = torch.tensor(samples, dtype=torch.float32) / 32768.0
    if tensor.numel() <= 0:
        raise RuntimeError("Speaker ID waveform was empty.")
    _debug(
        f"waveform prepared rate={rate} width={width} channels={channels} "
        f"bytes={len(pcm)} samples={tensor.numel()}"
    )
    return tensor.unsqueeze(0)


def _compute_embedding(audio_bytes: bytes, audio_format: Dict[str, Any]) -> List[float]:
    available, detail = _speechbrain_state()
    if not available:
        raise RuntimeError(detail or "SpeechBrain is unavailable")
    import torch  # type: ignore

    waveform = _pcm_to_waveform(audio_bytes, audio_format)
    lengths = torch.tensor([1.0], dtype=torch.float32)
    with _ENGINE_LOCK:
        encoder = _ENGINE
        device = _ENGINE_DEVICE or "cpu"
    if encoder is None:
        raise RuntimeError("Speaker ID model is not loaded.")
    if device != "cpu":
        waveform = waveform.to(device)
        lengths = lengths.to(device)
    with torch.no_grad():
        try:
            embedding = encoder.encode_batch(waveform, lengths=lengths)
        except TypeError as exc:
            message = str(exc) or ""
            if "unexpected keyword argument 'lengths'" not in message:
                raise
            _debug("encode_batch does not accept lengths keyword; retrying without lengths")
            embedding = encoder.encode_batch(waveform)
    vector = embedding.squeeze().flatten().detach().cpu().to(torch.float32)
    if vector.numel() <= 0:
        raise RuntimeError("Speaker ID embedding was empty.")
    norm = float(torch.linalg.norm(vector).item())
    if norm <= 1e-9:
        raise RuntimeError("Speaker ID embedding norm was zero.")
    vector = vector / norm
    _debug(f"embedding computed dim={vector.numel()} norm={norm:.6f}")
    return [float(item) for item in vector.tolist()]


def match_speaker_for_audio(
    *,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    speech_s: float = 0.0,
) -> Dict[str, Any]:
    if not speaker_id_enabled():
        _debug("match skipped reason=disabled")
        return {"matched": False, "reason": "disabled"}
    best_match = _best_match_enabled()
    if (not best_match) and float(speech_s or 0.0) < _min_speech_seconds():
        _debug(
            f"match skipped reason=too_short speech_s={float(speech_s or 0.0):.2f} "
            f"minimum_s={_min_speech_seconds():.2f}"
        )
        return {"matched": False, "reason": "too_short"}
    speakers = _all_speakers()
    if not speakers:
        _debug("match skipped reason=no_speakers")
        return {"matched": False, "reason": "no_speakers"}
    _log_info("match start speech_s=%.2f speakers=%s", float(speech_s or 0.0), len(speakers))
    _debug(f"match start speech_s={float(speech_s or 0.0):.2f} speakers={len(speakers)}")
    query = _compute_embedding(audio_bytes, audio_format)
    scored: List[Dict[str, Any]] = []
    for speaker in speakers:
        avg = _speaker_average_embedding(speaker)
        if not avg:
            continue
        score = _cosine_similarity(query, avg)
        scored.append(
            {
                "speaker_id": _vp()._text(speaker.get("id")),
                "speaker_name": _vp()._text(speaker.get("name")),
                "score": float(score),
                "sample_count": len(list(speaker.get("samples") or [])),
            }
        )
    if not scored:
        _debug("match skipped reason=no_embeddings")
        result = {
            "matched": False,
            "reason": "no_embeddings",
            "speech_s": float(speech_s or 0.0),
            "model_source": _model_source(),
            "updated_ts": time.time(),
        }
        _save_last_result(result)
        return result
    scored.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    best = scored[0]
    second_score = float(scored[1].get("score") or 0.0) if len(scored) > 1 else -1.0
    margin = float(best.get("score") or 0.0) - second_score
    matched = bool(
        best_match
        or (float(best.get("score") or 0.0) >= _match_threshold() and margin >= _match_margin())
    )
    top_name = _vp()._text(best.get("speaker_name")) or "unknown"
    top_score = float(best.get("score") or 0.0)
    if matched:
        _log_info(
            "match success speaker=%s score=%.3f margin=%.3f best_match=%s",
            top_name,
            top_score,
            float(margin),
            bool(best_match),
        )
        _debug(
            f"match success speaker={top_name!r} score={top_score:.3f} "
            f"margin={float(margin):.3f} best_match={bool(best_match)}"
        )
    else:
        _log_info(
            "match no-hit top_speaker=%s score=%.3f margin=%.3f threshold=%.3f required_margin=%.3f",
            top_name,
            top_score,
            float(margin),
            _match_threshold(),
            _match_margin(),
        )
        _debug(
            f"match no-hit top_speaker={top_name!r} score={top_score:.3f} margin={float(margin):.3f} "
            f"threshold={_match_threshold():.3f} required_margin={_match_margin():.3f}"
        )
    result = {
        "matched": matched,
        "reason": "best_match" if matched and best_match else ("matched" if matched else "below_threshold"),
        "speaker_id": _vp()._text(best.get("speaker_id")),
        "speaker_name": _vp()._text(best.get("speaker_name")),
        "score": float(best.get("score") or 0.0),
        "margin": float(margin),
        "threshold": _match_threshold(),
        "match_margin": _match_margin(),
        "best_match": bool(best_match),
        "sample_count": int(best.get("sample_count") or 0),
        "candidates": scored[:3],
        "speech_s": float(speech_s or 0.0),
        "model_source": _model_source(),
        "updated_ts": time.time(),
    }
    _save_last_result(result)
    return result


def add_enrollment_sample(
    *,
    speaker_id: str,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    speech_s: float = 0.0,
) -> Dict[str, Any]:
    if float(speech_s or 0.0) < _enroll_min_speech_seconds():
        raise RuntimeError(
            f"Enrollment sample was too short. Speak one clear sentence for at least {_enroll_min_speech_seconds():.2f} seconds."
        )
    _log_info("enrollment sample start speaker_id=%s speech_s=%.2f", _vp()._text(speaker_id), float(speech_s or 0.0))
    _debug(f"enrollment sample start speaker_id={_vp()._text(speaker_id)!r} speech_s={float(speech_s or 0.0):.2f}")
    speakers = _all_speakers()
    target_index = next((index for index, row in enumerate(speakers) if _vp()._text(row.get("id")) == _vp()._text(speaker_id)), -1)
    if target_index < 0:
        raise KeyError("Speaker not found.")
    embedding = _compute_embedding(audio_bytes, audio_format)
    sample = {
        "embedding": embedding,
        "created_ts": time.time(),
        "speech_s": float(speech_s or 0.0),
    }
    speaker = dict(speakers[target_index])
    samples = list(speaker.get("samples") or [])
    samples.append(sample)
    speaker["samples"] = samples
    speaker["updated_ts"] = time.time()
    speakers[target_index] = speaker
    _save_speakers(speakers)
    _log_info(
        "enrollment sample saved speaker=%s sample_count=%s speech_s=%.2f",
        _vp()._text(speaker.get("name")) or "Speaker",
        len(samples),
        float(speech_s or 0.0),
    )
    _debug(
        f"enrollment sample saved speaker={_vp()._text(speaker.get('name'))!r} "
        f"sample_count={len(samples)} speech_s={float(speech_s or 0.0):.2f}"
    )
    return {
        "speaker_id": _vp()._text(speaker.get("id")),
        "speaker_name": _vp()._text(speaker.get("name")),
        "sample_count": len(samples),
    }


def _selector_options(status: Dict[str, Any]) -> List[Dict[str, str]]:
    vp = _vp()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    satellites = []
    with contextlib.suppress(Exception):
        satellites = list(vp.load_satellite_registry() or [])
    labels: Dict[str, str] = {"": "Any satellite"}
    for row in satellites:
        if not isinstance(row, dict):
            continue
        selector = vp._text(row.get("selector"))
        if not selector:
            continue
        host = vp._text(row.get("host"))
        name = vp._text(row.get("name")) or selector
        labels[selector] = f"{name}{f' ({host})' if host else ''}"
    for selector, row in clients.items():
        if not isinstance(row, dict):
            continue
        token = vp._text(selector) or vp._text(row.get("selector"))
        if not token:
            continue
        device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
        label = (
            vp._text(device_info.get("friendly_name"))
            or vp._text(device_info.get("name"))
            or vp._text(row.get("name"))
            or vp._text(token)
        )
        host = vp._text(row.get("host"))
        labels[token] = f"{label}{f' ({host})' if host else ''}"
    ordered = sorted(((key, value) for key, value in labels.items()), key=lambda item: (item[0] != "", item[1].lower()))
    return [{"value": key, "label": value} for key, value in ordered]


def panel_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    vp = _vp()
    speakers = _all_speakers()
    selector_options = _selector_options(status)
    pending = current_pending_enrollment()
    availability = runtime_availability()
    last = last_result()
    return {
        "availability": availability,
        "summary_metrics": [
            {"label": "Enabled", "value": "Yes" if speaker_id_enabled() else "No"},
            {"label": "Mode", "value": "Best match" if _best_match_enabled() else "Threshold"},
            {"label": "Enrolled Speakers", "value": len(speakers)},
            {"label": "Last Speaker", "value": vp._text(last.get("speaker_name")) or "-"},
            {"label": "Last Score", "value": f"{float(last.get('score') or 0.0):.2f}" if last else "-"},
            {"label": "Pending Capture", "value": pending.get("speaker_name") or "-"},
            {"label": "Model", "value": _model_source()},
        ],
        "settings_sections": [
            {
                "label": "Runtime",
                "fields": settings_fields(),
            }
        ],
        "selector_options": selector_options,
        "pending": pending,
        "last_result": {
            **last,
            "updated_at": _pretty_timestamp(last.get("updated_ts")) if last else "-",
        },
        "create_fields": [
            {
                "key": "speaker_name",
                "label": "Speaker Name",
                "type": "text",
                "placeholder": "Tater",
                "description": "Create a speaker profile first, then capture one or more voice samples.",
            },
            {
                "key": "preferred_selector",
                "label": "Preferred Satellite",
                "type": "select",
                "options": selector_options,
                "value": "",
                "description": "Optional. Limit enrollment to one satellite, or leave it on Any satellite.",
            },
        ],
        "speakers": [
            {
                "speaker_id": vp._text(row.get("id")),
                "name": vp._text(row.get("name")),
                "sample_count": len(list(row.get("samples") or [])),
                "preferred_selector": vp._text(row.get("preferred_selector")),
                "updated_at": _pretty_timestamp(row.get("updated_ts")),
                "fields": [
                    {
                        "key": "speaker_name",
                        "label": "Speaker Name",
                        "type": "text",
                        "value": vp._text(row.get("name")),
                    },
                    {
                        "key": "preferred_selector",
                        "label": "Preferred Satellite",
                        "type": "select",
                        "options": selector_options,
                        "value": vp._text(row.get("preferred_selector")),
                        "description": "If set, the next capture must come from this satellite.",
                    },
                ],
            }
            for row in sorted(speakers, key=lambda item: vp._text(item.get("name")).lower())
        ],
    }


def handle_runtime_action(action_name: str, payload: Dict[str, Any], status: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    vp = _vp()
    action = esphome_runtime.lower(action_name)
    body = payload if isinstance(payload, dict) else {}
    values = esphome_runtime.payload_values(body)

    if action == "speaker_id_settings_save":
        result = save_settings_values(values)
        updated = int(result.get("updated_count") or 0)
        message = f"Saved {updated} Speaker ID setting(s)." if updated > 0 else "No Speaker ID settings changed."
        _log_info("settings saved updated_count=%s changed_keys=%s", updated, ",".join(result.get("changed_keys") or []))
        _debug(
            f"settings saved updated_count={updated} changed_keys={','.join(result.get('changed_keys') or [])}"
        )
        return {"ok": True, "action": action, "message": message, "speaker_id": panel_payload(status), **result}

    if action == "speaker_id_pending_cancel":
        cancel_pending_enrollment()
        return {"ok": True, "action": action, "message": "Canceled pending speaker capture.", "speaker_id": panel_payload(status)}

    if action == "speaker_id_speaker_create":
        speaker_name = esphome_runtime.text(values.get("speaker_name") or body.get("speaker_name"))
        if not speaker_name:
            raise ValueError("speaker_name is required")
        preferred_selector = esphome_runtime.text(values.get("preferred_selector") or body.get("preferred_selector"))
        speakers = _all_speakers()
        if any(esphome_runtime.lower(row.get("name")) == esphome_runtime.lower(speaker_name) for row in speakers):
            raise ValueError(f"A speaker named {speaker_name} already exists.")
        row = {
            "id": uuid.uuid4().hex,
            "name": speaker_name,
            "preferred_selector": preferred_selector,
            "created_ts": time.time(),
            "updated_ts": time.time(),
            "samples": [],
        }
        speakers.append(row)
        _save_speakers(speakers)
        _log_info("speaker created name=%s preferred_selector=%s", speaker_name, preferred_selector or "any")
        _debug(f"speaker created name={speaker_name!r} preferred_selector={preferred_selector or 'any'}")
        return {
            "ok": True,
            "action": action,
            "message": f"Created speaker {speaker_name}. Capture a sample next.",
            "speaker_id": panel_payload(status),
        }

    if action == "speaker_id_speaker_save":
        speaker_id = esphome_runtime.text(body.get("speaker_id") or values.get("speaker_id"))
        if not speaker_id:
            raise ValueError("speaker_id is required")
        speakers = _all_speakers()
        index = next((i for i, row in enumerate(speakers) if esphome_runtime.text(row.get("id")) == speaker_id), -1)
        if index < 0:
            raise KeyError("Speaker not found.")
        row = dict(speakers[index])
        new_name = esphome_runtime.text(values.get("speaker_name")) or esphome_runtime.text(row.get("name")) or "Speaker"
        if any(esphome_runtime.text(other.get("id")) != speaker_id and esphome_runtime.lower(other.get("name")) == esphome_runtime.lower(new_name) for other in speakers):
            raise ValueError(f"A speaker named {new_name} already exists.")
        row["name"] = new_name
        row["preferred_selector"] = esphome_runtime.text(values.get("preferred_selector"))
        row["updated_ts"] = time.time()
        speakers[index] = row
        _save_speakers(speakers)
        _log_info("speaker saved id=%s name=%s preferred_selector=%s", speaker_id, new_name, row["preferred_selector"] or "any")
        _debug(
            f"speaker saved id={speaker_id!r} name={new_name!r} preferred_selector={row['preferred_selector'] or 'any'}"
        )
        return {"ok": True, "action": action, "message": f"Saved speaker {new_name}.", "speaker_id": panel_payload(status)}

    if action == "speaker_id_speaker_delete":
        speaker_id = esphome_runtime.text(body.get("speaker_id") or values.get("speaker_id"))
        if not speaker_id:
            raise ValueError("speaker_id is required")
        speakers = _all_speakers()
        target = next((row for row in speakers if esphome_runtime.text(row.get("id")) == speaker_id), None)
        if not isinstance(target, dict):
            raise KeyError("Speaker not found.")
        speakers = [row for row in speakers if esphome_runtime.text(row.get("id")) != speaker_id]
        _save_speakers(speakers)
        pending = _pending_enrollment_state()
        if esphome_runtime.text(pending.get("speaker_id")) == speaker_id:
            cancel_pending_enrollment()
        _log_info("speaker deleted id=%s name=%s", speaker_id, esphome_runtime.text(target.get("name")) or "speaker")
        _debug(f"speaker deleted id={speaker_id!r} name={esphome_runtime.text(target.get('name'))!r}")
        return {
            "ok": True,
            "action": action,
            "message": f"Deleted speaker {esphome_runtime.text(target.get('name')) or 'speaker'}.",
            "speaker_id": panel_payload(status),
        }

    if action == "speaker_id_enrollment_arm":
        speaker_id = esphome_runtime.text(body.get("speaker_id") or values.get("speaker_id"))
        if not speaker_id:
            raise ValueError("speaker_id is required")
        speakers = _all_speakers()
        target = next((row for row in speakers if esphome_runtime.text(row.get("id")) == speaker_id), None)
        if not isinstance(target, dict):
            raise KeyError("Speaker not found.")
        available, detail = _speechbrain_state()
        if not available:
            raise RuntimeError(detail or "SpeechBrain model is unavailable.")
        selector = esphome_runtime.text(values.get("preferred_selector")) or esphome_runtime.text(target.get("preferred_selector"))
        pending = arm_enrollment(
            speaker_id=speaker_id,
            speaker_name=esphome_runtime.text(target.get("name")) or "Speaker",
            selector=selector,
        )
        return {
            "ok": True,
            "action": action,
            "message": f"Armed the next voice turn for {pending.get('speaker_name') or 'speaker'}. Speak one clear sentence from {pending.get('selector_label') or 'a satellite'}.",
            "speaker_id": panel_payload(status),
        }

    return None
