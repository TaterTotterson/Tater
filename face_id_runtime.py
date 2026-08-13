"""Optional isolated DeepFace runtime shared by Tater cores."""

from __future__ import annotations

import atexit
import base64
import contextlib
import json
import os
import platform
import selectors
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from tater_paths import runtime_dir


ENABLED_KEY = "tater:face_id:enabled"
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
TENSORFLOW_METAL_BASE_VERSION = "2.18.0"
TENSORFLOW_METAL_VERSION = "1.2.0"
MODEL_PACK_VERSION = "4"

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_condition = threading.Condition(threading.RLock())
_inference_lock = threading.Lock()
_worker_lock = threading.RLock()
_model_loaded = False
_load_thread: Optional[threading.Thread] = None
_worker_process: Optional[subprocess.Popen] = None
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


def model_pack_dir() -> Path:
    return runtime_dir() / "models" / "face-id"


def model_pack_venv_dir() -> Path:
    return model_pack_dir() / "venv"


def model_pack_python() -> Path:
    if os.name == "nt":
        return model_pack_venv_dir() / "Scripts" / "python.exe"
    return model_pack_venv_dir() / "bin" / "python"


def model_pack_ready_path() -> Path:
    return model_pack_dir() / "ready.json"


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


def _accelerator_requirement(accelerator: str) -> str:
    if accelerator == "metal":
        return f"tensorflow-metal=={TENSORFLOW_METAL_VERSION}"
    if accelerator == "cuda":
        return f"tensorflow[and-cuda]=={TENSORFLOW_VERSION}"
    return ""


def _requirements_path() -> Path:
    return Path(__file__).resolve().parent / "requirements-face.txt"


def _worker_path() -> Path:
    return Path(__file__).resolve().parent / "face_id_worker.py"


def _model_pack_available() -> bool:
    python_bin = model_pack_python()
    ready_path = model_pack_ready_path()
    if not python_bin.is_file() or not ready_path.is_file():
        return False
    try:
        ready = json.loads(ready_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(ready, dict) or str(ready.get("model_pack_version") or "") != MODEL_PACK_VERSION:
        return False
    if str(ready.get("detector_backend") or "") != DETECTOR_BACKEND:
        return False
    if str(ready.get("acceleration_target") or "cpu") != desired_accelerator():
        return False
    venv_dir = model_pack_venv_dir()
    candidates = list(venv_dir.glob("Lib/site-packages/deepface"))
    candidates.extend(venv_dir.glob("lib/python*/site-packages/deepface"))
    return any(path.is_dir() for path in candidates)


def _public_state(redis_client: Any = None) -> str:
    enabled = is_enabled(redis_client)
    with _condition:
        state = _state
        loaded = _model_loaded
    if not enabled:
        return "disabled"
    if loaded:
        return "ready"
    if state in {"installing", "loading", "error"}:
        return state
    return "idle"


def status(redis_client: Any = None) -> Dict[str, Any]:
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
    return {
        "enabled": enabled,
        "installed": _model_pack_available(),
        "loaded": loaded,
        "state": state,
        "loading": enabled and (thread_alive or state in {"installing", "loading"}),
        "error": error,
        "message": message,
        "model": MODEL_NAME,
        "detector_backend": DETECTOR_BACKEND,
        "distance_metric": DISTANCE_METRIC,
        "match_threshold": MATCH_THRESHOLD,
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


def _worker_environment() -> Dict[str, str]:
    environment = {
        **os.environ,
        "PYTHONNOUSERSITE": "1",
        "TF_CPP_MIN_LOG_LEVEL": "2",
        "TF_USE_LEGACY_KERAS": "1",
        "DEEPFACE_HOME": str(model_pack_dir()),
    }
    if sys.platform == "linux":
        site_packages = list(model_pack_venv_dir().glob("lib/python*/site-packages"))
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


def _install_model_pack(generation: int) -> None:
    global _message, _state
    pack_dir = model_pack_dir()
    pack_dir.mkdir(parents=True, exist_ok=True)
    with _condition:
        if generation != _generation:
            return
        _state = "installing"
        _message = "Installing the private DeepFace model pack. This only happens the first time."
        _condition.notify_all()

    if not model_pack_python().is_file():
        _run_checked([sys.executable, "-m", "venv", str(model_pack_venv_dir())], timeout=300)
    python_bin = str(model_pack_python())
    _run_checked([python_bin, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], timeout=600)
    requirements = _requirements_path()
    if not requirements.is_file():
        raise RuntimeError("The Face ID model-pack requirements are missing from this Tater installation.")
    _run_checked([python_bin, "-m", "pip", "install", "--upgrade", "-r", str(requirements)], timeout=2400)
    acceleration_target = desired_accelerator()
    accelerator_install_error = ""
    accelerator_requirement = _accelerator_requirement(acceleration_target)
    if accelerator_requirement:
        with _condition:
            if generation != _generation:
                return
            _message = (
                "Installing Apple Metal acceleration for Face ID."
                if acceleration_target == "metal"
                else "Installing NVIDIA CUDA acceleration for Face ID."
            )
            _condition.notify_all()
        try:
            _run_checked(
                [python_bin, "-m", "pip", "install", "--upgrade", accelerator_requirement],
                timeout=2400,
            )
        except Exception as exc:
            accelerator_install_error = str(exc) or exc.__class__.__name__
    with _condition:
        if generation != _generation:
            return
    worker = _worker_path()
    if not worker.is_file():
        raise RuntimeError("The Face ID worker is missing from this Tater installation.")
    warmup_command = [python_bin, "-s", str(worker), "--warmup", MODEL_NAME, DETECTOR_BACKEND]
    try:
        output = _run_checked(warmup_command, timeout=1200)
    except Exception as exc:
        if acceleration_target != "metal":
            raise
        warmup_error = str(exc) or exc.__class__.__name__
        accelerator_install_error = " | ".join(
            value for value in (accelerator_install_error, warmup_error) if value
        )
        _run_checked(
            [python_bin, "-m", "pip", "uninstall", "-y", "tensorflow-metal"],
            timeout=300,
        )
        with _condition:
            if generation != _generation:
                return
            _message = "Apple Metal was unavailable; validating the Face ID CPU fallback."
            _condition.notify_all()
        output = _run_checked(warmup_command, timeout=1200)
    warmup = _worker_result_from_output(output)
    if not bool(warmup.get("ok")):
        raise RuntimeError("The Face ID model pack installed but its warm-up did not complete.")
    model_pack_ready_path().write_text(
        json.dumps(
            {
                "model_pack_version": MODEL_PACK_VERSION,
                "deepface_version": DEEPFACE_VERSION,
                "tensorflow_version": _text(warmup.get("tensorflow_version")) or TENSORFLOW_VERSION,
                "model_name": MODEL_NAME,
                "detector_backend": DETECTOR_BACKEND,
                "acceleration_target": acceleration_target,
                "accelerator": _text(warmup.get("accelerator")) or "cpu",
                "device_name": _text(warmup.get("device_name")),
                "gpu_count": int(warmup.get("gpu_count") or 0),
                "accelerator_install_error": accelerator_install_error,
                "ready_at": time.time(),
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _start_worker() -> subprocess.Popen:
    global _worker_process
    with _worker_lock:
        if _worker_process is not None and _worker_process.poll() is None:
            return _worker_process
        if not model_pack_python().is_file() or not _worker_path().is_file():
            raise RuntimeError("The Face ID model pack is not installed.")
        _worker_process = subprocess.Popen(
            [str(model_pack_python()), "-s", str(_worker_path()), "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=_worker_environment(),
        )
        return _worker_process


def _stop_worker() -> None:
    global _worker_process
    with _worker_lock:
        process = _worker_process
        _worker_process = None
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
    request_id = f"face_{uuid.uuid4().hex}"
    request = dict(payload)
    request["request_id"] = request_id
    logs: List[str] = []
    with _worker_lock:
        process = _start_worker()
        if process.stdin is None or process.stdout is None:
            raise RuntimeError("Face ID worker pipes are unavailable.")
        try:
            process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
            process.stdin.flush()
        except Exception as exc:
            _stop_worker()
            raise RuntimeError(f"Could not send data to the Face ID worker: {exc}") from exc

        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + max(5.0, float(timeout))
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    remainder = str(process.stdout.read() or "").strip()
                    if remainder:
                        logs.extend(remainder.splitlines())
                    raise RuntimeError("Face ID worker stopped unexpectedly: " + " | ".join(logs[-8:]))
                events = selector.select(timeout=min(1.0, max(0.0, deadline - time.monotonic())))
                if not events:
                    continue
                line = process.stdout.readline()
                if not line:
                    continue
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


def _perform_load(generation: int) -> bool:
    global _accelerator, _accelerator_warning, _device_name, _error, _gpu_count
    global _loaded_at, _message, _model_loaded, _state, _tensorflow_version
    try:
        if not _model_pack_available():
            _install_model_pack(generation)
        with _condition:
            if generation != _generation:
                return True
            _state = "loading"
            _message = f"Loading {MODEL_NAME} in the private Face ID worker."
            _condition.notify_all()
        warmup = _worker_request(
            {"action": "warmup", "model_name": MODEL_NAME, "detector_backend": DETECTOR_BACKEND},
            timeout=1200,
        )
    except Exception as exc:
        with _condition:
            if generation != _generation:
                return True
            _model_loaded = False
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
        install_warning = ""
        try:
            ready = json.loads(model_pack_ready_path().read_text(encoding="utf-8"))
            install_warning = _text(ready.get("accelerator_install_error")) if isinstance(ready, dict) else ""
        except Exception:
            pass
        if install_warning:
            _accelerator_warning = f"{target.title()} support could not be installed; using {_accelerator.upper()}."
        elif target != "cpu" and _accelerator == "cpu":
            _accelerator_warning = f"{target.title()} was requested but TensorFlow did not detect a GPU; using CPU."
        else:
            _accelerator_warning = ""
        _model_loaded = True
        _loaded_at = time.time()
        _error = ""
        _message = ""
        _state = "ready"
        _condition.notify_all()
    return False


def _load_thread_main(generation: int, redis_client: Any) -> None:
    global _load_thread
    cancelled = _perform_load(generation)
    with _condition:
        if _load_thread is threading.current_thread():
            _load_thread = None
        _condition.notify_all()
    if cancelled and is_enabled(redis_client):
        start_model_load(redis_client)


def start_model_load(redis_client: Any = None) -> Dict[str, Any]:
    global _generation, _load_thread, _state
    if not is_enabled(redis_client):
        return status(redis_client)
    with _condition:
        if _model_loaded:
            _state = "ready"
            return status(redis_client)
        if _load_thread is not None and _load_thread.is_alive():
            return status(redis_client)
        _generation += 1
        generation = _generation
        _state = "loading" if _model_pack_available() else "installing"
        _load_thread = threading.Thread(
            target=_load_thread_main,
            args=(generation, redis_client),
            name="tater-face-id-load",
            daemon=True,
        )
        _load_thread.start()
    return status(redis_client)


def load_model(redis_client: Any = None, *, timeout: float = 2700.0) -> bool:
    if not is_enabled(redis_client):
        raise RuntimeError("Face ID is disabled in Settings > Models.")
    start_model_load(redis_client)
    deadline = time.monotonic() + max(1.0, float(timeout))
    with _condition:
        while not _model_loaded and _state in {"idle", "installing", "loading"}:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Face ID model load timed out.")
            _condition.wait(timeout=min(remaining, 1.0))
        if _model_loaded:
            return True
        raise RuntimeError(_error or "Face ID model could not be loaded.")


def unload_model() -> Dict[str, Any]:
    global _accelerator, _accelerator_warning, _device_name, _error, _generation, _gpu_count
    global _loaded_at, _message, _model_loaded, _state, _tensorflow_version
    with _inference_lock:
        with _condition:
            _generation += 1
            _model_loaded = False
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
    return {"loaded": False, "state": "disabled", "error": "", "model": MODEL_NAME}


def set_enabled(redis_client: Any, enabled: bool) -> Dict[str, Any]:
    if redis_client is None:
        raise RuntimeError("Redis is unavailable.")
    enabled_value = bool(enabled)
    redis_client.set(ENABLED_KEY, "true" if enabled_value else "false")
    if enabled_value:
        return start_model_load(redis_client)
    unload_model()
    return status(redis_client)


def analyze_image(image_bytes: bytes, redis_client: Any = None) -> List[Dict[str, Any]]:
    if not image_bytes:
        return []
    load_model(redis_client)
    with _inference_lock:
        if not is_enabled(redis_client):
            raise RuntimeError("Face ID is disabled in Settings > Models.")
        result = _worker_request(
            {
                "action": "represent",
                "image_b64": base64.b64encode(bytes(image_bytes)).decode("ascii"),
                "settings": {
                    "model_name": MODEL_NAME,
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
