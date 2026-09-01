"""Separate-process DeepFace runtime shared by Tater cores."""

from __future__ import annotations

import atexit
import base64
import contextlib
import json
import os
import platform
import selectors
import shutil
import site
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from tater_paths import runtime_dir
from spud_link_models import should_use_hub as spud_link_should_use_hub


ENABLED_KEY = "tater:face_id:enabled"
MODEL_KEY = "tater:face_id:model"
DEFAULT_MODEL_ID = "facenet512"
# Kept as a compatibility constant for older callers and tests. Runtime code
# should use selected_model() because the active recognizer is configurable.
MODEL_NAME = "Facenet512"
DETECTOR_BACKEND = "retinaface"
DISTANCE_METRIC = "cosine"
# DeepFace's calibrated Facenet512 cosine threshold is 0.30. Keep the
# recognition cutoff aligned with the model while Awareness compares against
# several learned views of a person instead of only one averaged centroid.
MATCH_THRESHOLD = 0.30
MAX_FACES_PER_FRAME = 8
DEEPFACE_VERSION = "0.0.100"
TENSORFLOW_VERSION = "2.21.0"
TENSORFLOW_MACOS_VERSION = "2.18.0"
MODEL_PACK_VERSION = "6"
EMBEDDING_DIMENSIONS = 512

ADAFACE_REPO_ID = "minchul/cvlface_adaface_ir50_webface4m"
ADAFACE_REVISION = "60a65befbcf7e19284c4f3ac730f56867ed29594"

FACE_ID_MODELS: Dict[str, Dict[str, Any]] = {
    DEFAULT_MODEL_ID: {
        "id": DEFAULT_MODEL_ID,
        "label": "FaceNet512",
        "model_name": "Facenet512",
        "engine": "deepface",
        "match_threshold": MATCH_THRESHOLD,
        "embedding_dimensions": EMBEDDING_DIMENSIONS,
        "experimental": False,
    },
    "adaface_ir50_webface4m": {
        "id": "adaface_ir50_webface4m",
        "label": "AdaFace IR-50 · WebFace4M",
        "model_name": "AdaFace IR-50 WebFace4M",
        "engine": "cvlface",
        # This is intentionally conservative until Tater's saved-image
        # bakeoff can calibrate a household-specific cutoff.
        "match_threshold": 0.40,
        "embedding_dimensions": EMBEDDING_DIMENSIONS,
        "repo_id": ADAFACE_REPO_ID,
        "revision": ADAFACE_REVISION,
        "experimental": True,
    },
}

_RUNTIME_MODULES = ("cv2", "deepface", "retinaface", "tensorflow", "tf_keras")
_ADAFACE_RUNTIME_MODULES = ("torch", "transformers", "huggingface_hub", "omegaconf", "fvcore")

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_condition = threading.Condition(threading.RLock())
_inference_lock = threading.Lock()
_worker_lock = threading.RLock()
_model_loaded = False
_loaded_model_id = ""
_load_thread: Optional[threading.Thread] = None
_worker_process: Optional[subprocess.Popen] = None
_worker_warmed_model_ids: set[str] = set()
_worker_stdout_buffer = ""
_state = "idle"
_error = ""
_message = ""
_loaded_at = 0.0
_generation = 0
_accelerator = ""
_device_name = ""
_tensorflow_version = ""
_gpu_count = 0
_accelerator_warning = ""


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="ignore").strip()
    return str(value or "").strip()


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="ignore")
    return str(value).strip().lower() in _TRUE_VALUES


def is_enabled(redis_client: Any = None) -> bool:
    if redis_client is None:
        return False
    try:
        return _bool(redis_client.get(ENABLED_KEY), False)
    except Exception:
        return False


def normalize_model_id(value: Any) -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "facenet": DEFAULT_MODEL_ID,
        "facenet_512": DEFAULT_MODEL_ID,
        "adaface": "adaface_ir50_webface4m",
        "adaface_ir50": "adaface_ir50_webface4m",
    }
    token = aliases.get(token, token)
    return token if token in FACE_ID_MODELS else DEFAULT_MODEL_ID


def selected_model_id(redis_client: Any = None) -> str:
    client = redis_client
    if client is None:
        with contextlib.suppress(Exception):
            from helpers import redis_client as shared_redis

            client = shared_redis
    if client is not None:
        with contextlib.suppress(Exception):
            return normalize_model_id(client.get(MODEL_KEY))
    return DEFAULT_MODEL_ID


def selected_model(redis_client: Any = None, *, model_id: Any = "") -> Dict[str, Any]:
    token = normalize_model_id(model_id) if _text(model_id) else selected_model_id(redis_client)
    return dict(FACE_ID_MODELS[token])


def available_models() -> List[Dict[str, Any]]:
    return [
        {
            "id": model["id"],
            "label": model["label"],
            "experimental": bool(model.get("experimental")),
        }
        for model in FACE_ID_MODELS.values()
    ]


def model_pack_dir() -> Path:
    return runtime_dir() / "models" / "face-id"


def runtime_python() -> Path:
    """Return Tater's current interpreter without resolving its venv symlink."""
    return Path(sys.executable)


def model_pack_ready_path() -> Path:
    return model_pack_dir() / "ready.json"


def _model_pack_ready_path(model_id: Any) -> Path:
    token = normalize_model_id(model_id)
    if token == DEFAULT_MODEL_ID:
        return model_pack_ready_path()
    return model_pack_dir() / f"ready-{token}.json"


def _nvidia_gpu_available() -> bool:
    if sys.platform != "linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
        return False
    visible = str(os.environ.get("NVIDIA_VISIBLE_DEVICES") or "").strip().lower()
    if visible in {"none", "void"}:
        return False
    command = shutil.which("nvidia-smi")
    if command:
        try:
            completed = subprocess.run(
                [command, "-L"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
            if completed.returncode == 0 and "gpu" in str(completed.stdout or "").lower():
                return True
        except Exception:
            pass
    return Path("/dev/nvidiactl").exists() and any(Path("/dev").glob("nvidia[0-9]*"))


def desired_accelerator() -> str:
    machine = platform.machine().lower()
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return "metal"
    if _nvidia_gpu_available():
        return "cuda"
    return "cpu"


def expected_tensorflow_version() -> str:
    if sys.platform == "darwin" and platform.machine().lower() in {"arm64", "aarch64"}:
        return TENSORFLOW_MACOS_VERSION
    return TENSORFLOW_VERSION


def _worker_path() -> Path:
    return Path(__file__).resolve().parent / "face_id_worker.py"


def _missing_runtime_modules(model_id: Any = "") -> List[str]:
    import importlib.util

    model = selected_model(model_id=model_id)
    modules = list(_RUNTIME_MODULES)
    if model.get("engine") == "cvlface":
        modules.extend(_ADAFACE_RUNTIME_MODULES)
    return [name for name in modules if importlib.util.find_spec(name) is None]


def _runtime_dependencies_available(model_id: Any = "") -> bool:
    return not _missing_runtime_modules(model_id)


def _model_pack_available(
    *,
    dependencies_available: Optional[bool] = None,
    redis_client: Any = None,
    model_id: Any = "",
) -> bool:
    model = selected_model(redis_client, model_id=model_id)
    ready_path = _model_pack_ready_path(model["id"])
    dependencies_ready = (
        _runtime_dependencies_available(model["id"])
        if dependencies_available is None
        else bool(dependencies_available)
    )
    if not dependencies_ready or not ready_path.is_file():
        return False
    try:
        ready = json.loads(ready_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(ready, dict) or str(ready.get("model_pack_version") or "") != MODEL_PACK_VERSION:
        return False
    if normalize_model_id(ready.get("model_id")) != model["id"]:
        return False
    if str(ready.get("detector_backend") or "") != DETECTOR_BACKEND:
        return False
    if str(ready.get("acceleration_target") or "cpu") != desired_accelerator():
        return False
    return True


def _public_state(redis_client: Any = None) -> str:
    enabled = is_enabled(redis_client)
    wanted_model_id = selected_model_id(redis_client)
    with _condition:
        state = _state
        loaded = _model_loaded and _loaded_model_id == wanted_model_id
    if not enabled:
        return "disabled"
    if loaded:
        return "ready"
    if state in {"loading", "error"}:
        return state
    return "idle"


def status(redis_client: Any = None, *, force_local: bool = False) -> Dict[str, Any]:
    model = selected_model(redis_client)
    if not force_local and spud_link_should_use_hub("face_id", redis_conn=redis_client):
        return {
            "enabled": True,
            "installed": True,
            "models_ready": True,
            "loaded": True,
            "state": "remote",
            "loading": False,
            "error": "",
            "message": "Loaded on Spud Hub",
            "model": model["label"],
            "model_id": model["id"],
            "available_models": available_models(),
            "detector_backend": DETECTOR_BACKEND,
            "distance_metric": DISTANCE_METRIC,
            "match_threshold": model["match_threshold"],
            "loaded_at": 0.0,
            "model_pack_version": MODEL_PACK_VERSION,
            "deepface_version": DEEPFACE_VERSION,
            "model_pack_path": "",
            "accelerator": "Spud Hub",
            "accelerator_target": "remote",
            "device_name": "Spud Hub",
            "gpu_available": False,
            "gpu_count": 0,
            "tensorflow_version": "",
            "accelerator_warning": "",
            "local_only": False,
            "routed_via": "spud_link",
        }
    enabled = is_enabled(redis_client)
    with _condition:
        loaded = _model_loaded
        error = _error
        message = _message
        loaded_at = _loaded_at
        thread_alive = bool(_load_thread and _load_thread.is_alive())
        accelerator = _accelerator
        device_name = _device_name
        tensorflow_version = _tensorflow_version
        gpu_count = _gpu_count
        accelerator_warning = _accelerator_warning
    state = _public_state(redis_client)
    target = desired_accelerator()
    dependencies_available = _runtime_dependencies_available(model["id"])
    return {
        "enabled": enabled,
        "installed": dependencies_available,
        "models_ready": _model_pack_available(
            dependencies_available=dependencies_available,
            redis_client=redis_client,
            model_id=model["id"],
        ),
        "loaded": loaded and _loaded_model_id == model["id"],
        "state": state,
        "loading": enabled and (thread_alive or state == "loading"),
        "error": error,
        "message": message,
        "model": model["label"],
        "model_id": model["id"],
        "available_models": available_models(),
        "detector_backend": DETECTOR_BACKEND,
        "distance_metric": DISTANCE_METRIC,
        "match_threshold": model["match_threshold"],
        "loaded_at": loaded_at,
        "model_pack_version": MODEL_PACK_VERSION,
        "deepface_version": DEEPFACE_VERSION,
        "model_pack_path": str(model_pack_dir()),
        "accelerator": accelerator or ("detecting" if enabled else "unloaded"),
        "accelerator_target": target,
        "device_name": device_name,
        "gpu_available": bool(gpu_count),
        "gpu_count": gpu_count,
        "tensorflow_version": tensorflow_version,
        "accelerator_warning": accelerator_warning,
        "local_only": True,
    }


def settings_payload(redis_client: Any = None) -> Dict[str, Any]:
    return status(redis_client)


def embedding_model_metadata(redis_client: Any = None, *, model_id: Any = "") -> Dict[str, Any]:
    """Describe embeddings without exposing runtime paths or device details."""
    model = selected_model(redis_client, model_id=model_id)
    return {
        "model_id": model["id"],
        "model_name": model["model_name"],
        "detector_backend": DETECTOR_BACKEND,
        "distance_metric": DISTANCE_METRIC,
        "match_threshold": model["match_threshold"],
        "embedding_dimensions": model["embedding_dimensions"],
        "model_pack_version": MODEL_PACK_VERSION,
        "deepface_version": DEEPFACE_VERSION,
        "model_revision": _text(model.get("revision")),
    }


def _worker_environment() -> Dict[str, str]:
    environment = {
        **os.environ,
        "PYTHONNOUSERSITE": "1",
        "TF_CPP_MIN_LOG_LEVEL": "2",
        "TF_USE_LEGACY_KERAS": "1",
        "DEEPFACE_HOME": str(model_pack_dir()),
        "TATER_FACE_MODEL_DIR": str(model_pack_dir()),
        "TATER_ADAFACE_REPO_ID": ADAFACE_REPO_ID,
        "TATER_ADAFACE_REVISION": ADAFACE_REVISION,
    }
    if sys.platform == "linux":
        site_packages = [Path(path) for path in site.getsitepackages()]
        library_dirs: List[str] = []
        binary_dirs: List[str] = []
        for site_dir in site_packages:
            library_dirs.extend(str(path) for path in site_dir.glob("nvidia/*/lib") if path.is_dir())
            binary_dirs.extend(str(path) for path in site_dir.glob("nvidia/*/bin") if path.is_dir())
        if library_dirs:
            existing = str(environment.get("LD_LIBRARY_PATH") or "").strip()
            environment["LD_LIBRARY_PATH"] = os.pathsep.join([*library_dirs, *([existing] if existing else [])])
        if binary_dirs:
            existing = str(environment.get("PATH") or "").strip()
            environment["PATH"] = os.pathsep.join([*binary_dirs, *([existing] if existing else [])])
    return environment


def _run_checked(command: List[str], *, timeout: float) -> str:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        env=_worker_environment(),
    )
    output = str(completed.stdout or "").strip()
    if completed.returncode != 0:
        tail = "\n".join(output.splitlines()[-12:])
        raise RuntimeError(tail or f"Command exited with status {completed.returncode}.")
    return output


def _worker_result_from_output(output: str) -> Dict[str, Any]:
    for line in reversed(str(output or "").splitlines()):
        if not line.startswith("TATER_FACE_RESULT:"):
            continue
        try:
            payload = json.loads(line.split("TATER_FACE_RESULT:", 1)[1])
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _write_model_ready_marker(model: Dict[str, Any], warmup: Dict[str, Any]) -> None:
    model_pack_dir().mkdir(parents=True, exist_ok=True)
    _model_pack_ready_path(model["id"]).write_text(
        json.dumps(
            {
                "model_pack_version": MODEL_PACK_VERSION,
                "deepface_version": DEEPFACE_VERSION,
                "tensorflow_version": _text(warmup.get("tensorflow_version")) or expected_tensorflow_version(),
                "model_id": model["id"],
                "model_name": model["model_name"],
                "model_revision": _text(model.get("revision")),
                "detector_backend": DETECTOR_BACKEND,
                "acceleration_target": desired_accelerator(),
                "accelerator": _text(warmup.get("accelerator")) or "cpu",
                "device_name": _text(warmup.get("device_name")),
                "gpu_count": int(warmup.get("gpu_count") or 0),
                "ready_at": time.time(),
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _prepare_model_pack(generation: int, redis_client: Any = None, *, model_id: Any = "") -> None:
    global _message, _state
    model = selected_model(redis_client, model_id=model_id)
    pack_dir = model_pack_dir()
    pack_dir.mkdir(parents=True, exist_ok=True)
    with _condition:
        if generation != _generation:
            return
        _state = "loading"
        _message = "Preparing the Face ID models. The first load may download model weights."
        _condition.notify_all()

    missing = _missing_runtime_modules(model["id"])
    if missing:
        raise RuntimeError(
            "Face ID dependencies are missing from Tater's Python environment "
            f"({', '.join(missing)}). Rerun Tater setup to install the regular requirements."
        )
    with _condition:
        if generation != _generation:
            return
    worker = _worker_path()
    if not worker.is_file():
        raise RuntimeError("The Face ID worker is missing from this Tater installation.")
    warmup_command = [
        str(runtime_python()),
        "-s",
        str(worker),
        "--warmup",
        model["id"],
        DETECTOR_BACKEND,
    ]
    output = _run_checked(warmup_command, timeout=1200)
    warmup = _worker_result_from_output(output)
    if not bool(warmup.get("ok")):
        raise RuntimeError("The Face ID models could not complete their first warm-up.")
    _write_model_ready_marker(model, warmup)


def _start_worker(model_id: Any = "") -> subprocess.Popen:
    global _worker_process
    with _worker_lock:
        if _worker_process is not None and _worker_process.poll() is None:
            return _worker_process
        missing = _missing_runtime_modules(model_id)
        if missing:
            raise RuntimeError(
                "Face ID dependencies are missing from Tater's Python environment "
                f"({', '.join(missing)}). Rerun Tater setup."
            )
        if not _worker_path().is_file():
            raise RuntimeError("The Face ID worker is missing from this Tater installation.")
        _worker_process = subprocess.Popen(
            [str(runtime_python()), "-s", str(_worker_path()), "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=_worker_environment(),
        )
        return _worker_process


def _stop_worker() -> None:
    global _worker_process, _worker_stdout_buffer
    with _worker_lock:
        process = _worker_process
        _worker_process = None
        _worker_warmed_model_ids.clear()
        _worker_stdout_buffer = ""
        if process is None:
            return
        with contextlib.suppress(Exception):
            if process.poll() is None and process.stdin is not None:
                process.stdin.write(json.dumps({"action": "shutdown", "request_id": "shutdown"}) + "\n")
                process.stdin.flush()
                process.wait(timeout=4)
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
                process.wait(timeout=4)
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.kill()


def _worker_request(payload: Dict[str, Any], *, timeout: float = 240.0) -> Dict[str, Any]:
    global _worker_stdout_buffer
    request_id = f"face_{uuid.uuid4().hex}"
    request = dict(payload)
    request["request_id"] = request_id
    logs: List[str] = []
    with _worker_lock:
        process = _start_worker(payload.get("model_id") or (payload.get("settings") or {}).get("model_id"))
        if process.stdin is None or process.stdout is None:
            raise RuntimeError("Face ID worker pipes are unavailable.")
        try:
            process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
            process.stdin.flush()
        except Exception as exc:
            _stop_worker()
            raise RuntimeError(f"Could not send data to the Face ID worker: {exc}") from exc

        selector = selectors.DefaultSelector()
        stdout_fd = process.stdout.fileno()
        selector.register(stdout_fd, selectors.EVENT_READ)
        deadline = time.monotonic() + max(5.0, float(timeout))
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    with contextlib.suppress(Exception):
                        remainder = os.read(stdout_fd, 65536).decode("utf-8", errors="replace")
                        _worker_stdout_buffer += remainder
                    if _worker_stdout_buffer.strip():
                        logs.extend(_worker_stdout_buffer.splitlines())
                    _worker_stdout_buffer = ""
                    raise RuntimeError("Face ID worker stopped unexpectedly: " + " | ".join(logs[-8:]))
                events = selector.select(timeout=min(1.0, max(0.0, deadline - time.monotonic())))
                if not events:
                    continue
                try:
                    chunk = os.read(stdout_fd, 65536).decode("utf-8", errors="replace")
                except BlockingIOError:
                    continue
                if not chunk:
                    continue
                _worker_stdout_buffer += chunk
                while "\n" in _worker_stdout_buffer:
                    line, _worker_stdout_buffer = _worker_stdout_buffer.split("\n", 1)
                    text_line = line.strip()
                    if not text_line.startswith("TATER_FACE_RESULT:"):
                        if text_line:
                            logs.append(text_line)
                        continue
                    try:
                        result = json.loads(text_line.split("TATER_FACE_RESULT:", 1)[1])
                    except Exception:
                        continue
                    if not isinstance(result, dict) or str(result.get("request_id") or "") != request_id:
                        continue
                    if not bool(result.get("ok")):
                        raise RuntimeError(str(result.get("error") or "Face ID worker failed."))
                    return result
        finally:
            selector.close()
        _stop_worker()
        raise TimeoutError("Face ID worker timed out.")


def _perform_load(generation: int, redis_client: Any) -> bool:
    global _accelerator, _accelerator_warning, _device_name, _error, _gpu_count, _loaded_model_id
    global _loaded_at, _message, _model_loaded, _state, _tensorflow_version
    model = selected_model(redis_client)
    try:
        if not _model_pack_available(redis_client=redis_client, model_id=model["id"]):
            if model["id"] == DEFAULT_MODEL_ID:
                # Preserve the legacy one-argument preparation hook used by
                # existing installations and tests.
                _prepare_model_pack(generation)
            else:
                _prepare_model_pack(generation, redis_client, model_id=model["id"])
        with _condition:
            if generation != _generation:
                return True
            _state = "loading"
            _message = f"Loading {model['label']} in the private Face ID worker."
            _condition.notify_all()
        warmup = _worker_request(
            {"action": "warmup", "model_id": model["id"], "detector_backend": DETECTOR_BACKEND},
            timeout=1200,
        )
        _worker_warmed_model_ids.add(model["id"])
    except Exception as exc:
        with _condition:
            if generation != _generation:
                return True
            _model_loaded = False
            _loaded_model_id = ""
            _loaded_at = 0.0
            _error = str(exc) or exc.__class__.__name__
            _message = ""
            _state = "error"
            _condition.notify_all()
        _stop_worker()
        return False

    with _condition:
        if generation != _generation:
            _stop_worker()
            return True
        _accelerator = _text(warmup.get("accelerator")) or "cpu"
        _device_name = _text(warmup.get("device_name")) or ("CPU" if _accelerator == "cpu" else "GPU")
        _tensorflow_version = _text(warmup.get("tensorflow_version"))
        _gpu_count = max(0, int(warmup.get("gpu_count") or 0))
        target = desired_accelerator()
        if target != "cpu" and _accelerator == "cpu":
            _accelerator_warning = f"{target.title()} was requested but TensorFlow did not detect a GPU; using CPU."
        else:
            _accelerator_warning = ""
        _model_loaded = True
        _loaded_model_id = model["id"]
        _loaded_at = time.time()
        _error = ""
        _message = ""
        _state = "ready"
        _condition.notify_all()
    return False


def _load_thread_main(generation: int, redis_client: Any) -> None:
    global _load_thread
    cancelled = _perform_load(generation, redis_client)
    with _condition:
        if _load_thread is threading.current_thread():
            _load_thread = None
        _condition.notify_all()
    if cancelled and is_enabled(redis_client):
        start_model_load(redis_client)


def start_model_load(redis_client: Any = None, *, force_local: bool = False) -> Dict[str, Any]:
    global _generation, _load_thread, _state
    if not force_local and spud_link_should_use_hub("face_id", redis_conn=redis_client):
        if _model_loaded or (_load_thread is not None and _load_thread.is_alive()):
            unload_model()
        return status(redis_client, force_local=force_local)
    if not is_enabled(redis_client):
        return status(redis_client, force_local=force_local)
    wanted_model_id = selected_model_id(redis_client)
    if _model_loaded and _loaded_model_id != wanted_model_id:
        unload_model()
    with _condition:
        if _model_loaded and _loaded_model_id == wanted_model_id:
            _state = "ready"
            return status(redis_client, force_local=force_local)
        if _load_thread is not None and _load_thread.is_alive():
            return status(redis_client, force_local=force_local)
        _generation += 1
        generation = _generation
        _state = "loading"
        _load_thread = threading.Thread(
            target=_load_thread_main,
            args=(generation, redis_client),
            name="tater-face-id-load",
            daemon=True,
        )
        _load_thread.start()
    return status(redis_client, force_local=force_local)


def load_model(redis_client: Any = None, *, timeout: float = 2700.0, force_local: bool = False) -> bool:
    if not force_local and spud_link_should_use_hub("face_id", redis_conn=redis_client):
        start_model_load(redis_client, force_local=force_local)
        return True
    if not is_enabled(redis_client):
        raise RuntimeError("Face ID is disabled in Settings > Models.")
    start_model_load(redis_client, force_local=force_local)
    deadline = time.monotonic() + max(1.0, float(timeout))
    with _condition:
        while not _model_loaded and _state in {"idle", "loading"}:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Face ID model load timed out.")
            _condition.wait(timeout=min(remaining, 1.0))
        if _model_loaded:
            return True
        raise RuntimeError(_error or "Face ID model could not be loaded.")


def unload_model() -> Dict[str, Any]:
    global _accelerator, _accelerator_warning, _device_name, _error, _generation, _gpu_count, _loaded_model_id
    global _loaded_at, _message, _model_loaded, _state, _tensorflow_version
    with _inference_lock:
        with _condition:
            _generation += 1
            _model_loaded = False
            _loaded_model_id = ""
            _loaded_at = 0.0
            _error = ""
            _message = ""
            _state = "idle"
            _accelerator = ""
            _device_name = ""
            _tensorflow_version = ""
            _gpu_count = 0
            _accelerator_warning = ""
            _condition.notify_all()
        _stop_worker()
    model = selected_model()
    return {"loaded": False, "state": "disabled", "error": "", "model": model["label"], "model_id": model["id"]}


def set_enabled(redis_client: Any, enabled: bool) -> Dict[str, Any]:
    if redis_client is None:
        raise RuntimeError("Redis is unavailable.")
    enabled_value = bool(enabled)
    redis_client.set(ENABLED_KEY, "true" if enabled_value else "false")
    if enabled_value:
        return start_model_load(redis_client)
    unload_model()
    return status(redis_client)


def set_model(redis_client: Any, model_id: Any, *, load: bool = True) -> Dict[str, Any]:
    """Select a recognition model without touching saved identity vectors."""
    global _loaded_at, _loaded_model_id, _model_loaded, _state
    if redis_client is None:
        raise RuntimeError("Redis is unavailable.")
    token = normalize_model_id(model_id)
    requested = _text(model_id).lower().replace("-", "_").replace(" ", "_")
    if token == DEFAULT_MODEL_ID and requested not in {
        "",
        DEFAULT_MODEL_ID,
        "facenet",
        "facenet_512",
    }:
        raise ValueError(f"Unsupported Face ID model: {_text(model_id)}")
    changed = selected_model_id(redis_client) != token
    redis_client.set(MODEL_KEY, token)
    if changed:
        with _worker_lock:
            can_reuse_worker = (
                token in _worker_warmed_model_ids
                and _worker_process is not None
                and _worker_process.poll() is None
            )
        if can_reuse_worker:
            with _condition:
                _loaded_model_id = token
                _model_loaded = True
                _loaded_at = time.time()
                _state = "ready"
                _condition.notify_all()
        else:
            unload_model()
    if load and is_enabled(redis_client):
        return start_model_load(redis_client)
    return status(redis_client)


def analyze_image(
    image_bytes: bytes,
    redis_client: Any = None,
    *,
    force_local: bool = False,
    model_id: Any = "",
) -> List[Dict[str, Any]]:
    if not image_bytes:
        return []
    requested_model = selected_model(redis_client, model_id=model_id)
    active_model_id = selected_model_id(redis_client)
    if requested_model["id"] == active_model_id:
        load_model(redis_client, force_local=force_local)
    with _inference_lock:
        if not is_enabled(redis_client):
            raise RuntimeError("Face ID is disabled in Settings > Models.")
        if requested_model["id"] != active_model_id and requested_model["id"] not in _worker_warmed_model_ids:
            missing = _missing_runtime_modules(requested_model["id"])
            if missing:
                raise RuntimeError(
                    "Face ID dependencies are missing from Tater's Python environment "
                    f"({', '.join(missing)}). Rerun Tater setup."
                )
            warmup = _worker_request(
                {
                    "action": "warmup",
                    "model_id": requested_model["id"],
                    "detector_backend": DETECTOR_BACKEND,
                },
                timeout=1200,
            )
            _worker_warmed_model_ids.add(requested_model["id"])
            _write_model_ready_marker(requested_model, warmup)
        result = _worker_request(
            {
                "action": "represent",
                "image_b64": base64.b64encode(bytes(image_bytes)).decode("ascii"),
                "settings": {
                    "model_id": requested_model["id"],
                    "model_name": requested_model["model_name"],
                    "detector_backend": DETECTOR_BACKEND,
                    "minimum_confidence": 0.0,
                    "max_faces": MAX_FACES_PER_FRAME,
                },
            },
            timeout=240,
        )
    rows = result.get("detections") if isinstance(result.get("detections"), list) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def remove_model_pack() -> None:
    unload_model()
    pack_dir = model_pack_dir()
    if pack_dir.is_dir():
        shutil.rmtree(pack_dir)


def shutdown() -> None:
    unload_model()


atexit.register(_stop_worker)
