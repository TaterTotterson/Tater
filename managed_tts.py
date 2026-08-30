from __future__ import annotations

import atexit
import contextlib
import json
import logging
import os
import platform
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import wave
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from tater_paths import agent_lab_path


logger = logging.getLogger("managed_tts")

QWEN_TTS_BACKEND = "qwen3_tts"
OMNIVOICE_TTS_BACKEND = "omnivoice"
MANAGED_TTS_BACKENDS = {QWEN_TTS_BACKEND, OMNIVOICE_TTS_BACKEND}

DEFAULT_QWEN_TTS_MODEL = "Qwen/Qwen3-TTS-12Hz-0.6B-Base"
QWEN_TTS_VOICE_DESIGN_MODEL = "Qwen/Qwen3-TTS-12Hz-1.7B-VoiceDesign"
DEFAULT_OMNIVOICE_TTS_MODEL = "k2-fsa/OmniVoice"

_MLX_AUDIO_COMMIT = "223afd602c6d583455bdb9f181b91fc090f4aff9"
_OMNIVOICE_COMMIT = "28bc0889d92110491d726a9c79f26a895db5a074"
_ENV_STACK_VERSION = "tater-managed-tts-v1"
_SYNTHESIS_TIMEOUT_SECONDS = 15 * 60.0
_INSTALL_TIMEOUT_SECONDS = 30 * 60.0
_MAX_CLONE_AUDIO_BYTES = 50 * 1024 * 1024
_CLONE_AUDIO_SUFFIXES = {".wav", ".mp3", ".flac", ".m4a", ".ogg", ".opus", ".aac"}


def normalize_managed_tts_backend(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in {"qwen", "qwen_tts", "qwen3", "qwen3tts", "qwen3_tts"}:
        return QWEN_TTS_BACKEND
    if token in {"omni", "omni_voice", "omnivoice", "omnivoice_tts"}:
        return OMNIVOICE_TTS_BACKEND
    return token


def is_managed_tts_backend(value: Any) -> bool:
    return normalize_managed_tts_backend(value) in MANAGED_TTS_BACKENDS


def managed_tts_root(backend: Any) -> Path:
    token = normalize_managed_tts_backend(backend)
    if token not in MANAGED_TTS_BACKENDS:
        raise ValueError(f"Unsupported managed TTS backend: {token or backend}")
    root = Path(agent_lab_path("models", "tts", token.replace("_", "-")))
    root.mkdir(parents=True, exist_ok=True)
    return root


def managed_tts_clone_dir(backend: Any) -> Path:
    target = managed_tts_root(backend) / "clone"
    target.mkdir(parents=True, exist_ok=True)
    return target


def normalize_managed_tts_profile(value: Any) -> str:
    token = str(value or "direct").strip().lower().replace("-", "_").replace(" ", "_")
    return "announcement" if token in {"announcement", "announcements", "announce"} else "direct"


def _managed_tts_clone_stem(profile: Any) -> str:
    return "announcement-reference" if normalize_managed_tts_profile(profile) == "announcement" else "reference"


def validate_clone_audio_path(backend: Any, value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    root = managed_tts_clone_dir(backend).resolve()
    candidate = Path(raw).expanduser().resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("Clone audio must be stored in Tater's managed voice directory.") from exc
    if not candidate.is_file():
        return ""
    if candidate.suffix.lower() not in _CLONE_AUDIO_SUFFIXES:
        return ""
    return str(candidate)


def clone_audio_info(backend: Any, value: Any) -> Dict[str, Any]:
    path = validate_clone_audio_path(backend, value)
    if not path:
        return {"configured": False, "name": "", "size": 0}
    source = Path(path)
    with contextlib.suppress(OSError):
        return {"configured": True, "name": source.name, "size": int(source.stat().st_size)}
    return {"configured": False, "name": "", "size": 0}


def store_clone_audio(backend: Any, *, filename: Any, data: bytes, profile: Any = "direct") -> str:
    payload = bytes(data or b"")
    if not payload:
        raise ValueError("Choose a clone audio file first.")
    if len(payload) > _MAX_CLONE_AUDIO_BYTES:
        raise ValueError("Clone audio must be 50 MB or smaller.")
    suffix = Path(str(filename or "").strip()).suffix.lower()
    if suffix not in _CLONE_AUDIO_SUFFIXES:
        raise ValueError("Clone audio must be WAV, MP3, FLAC, M4A, OGG, Opus, or AAC.")
    root = managed_tts_clone_dir(backend)
    stem = _managed_tts_clone_stem(profile)
    target = root / f"{stem}{suffix}"
    fd, temp_name = tempfile.mkstemp(prefix=f".{stem}-", suffix=suffix, dir=str(root))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, target)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(temp_name)
        raise
    for sibling in root.glob(f"{stem}.*"):
        if sibling != target and sibling.is_file():
            with contextlib.suppress(OSError):
                sibling.unlink()
    return str(target.resolve())


def remove_clone_audio(backend: Any, value: Any = "", *, profile: Any = "direct") -> bool:
    root = managed_tts_clone_dir(backend).resolve()
    removed = False
    path = validate_clone_audio_path(backend, value)
    stem = _managed_tts_clone_stem(profile)
    explicit_path = Path(path) if path and Path(path).name.startswith(f"{stem}.") else None
    candidates = [explicit_path] if explicit_path is not None else list(root.glob(f"{stem}.*"))
    for candidate in candidates:
        with contextlib.suppress(ValueError):
            candidate.resolve().relative_to(root)
            if candidate.is_file():
                candidate.unlink()
                removed = True
    return removed


def decode_clone_audio_pcm(backend: Any, value: Any) -> Tuple[bytes, Dict[str, int]]:
    """Decode a managed clone sample for Tater's configured STT backend."""
    path = validate_clone_audio_path(backend, value)
    if not path:
        raise ValueError("Clone audio is not configured.")

    if Path(path).suffix.lower() == ".wav":
        with contextlib.suppress(Exception):
            with wave.open(path, "rb") as wav_file:
                if wav_file.getcomptype() == "NONE":
                    frames = wav_file.readframes(wav_file.getnframes())
                    if frames:
                        return frames, {
                            "rate": int(wav_file.getframerate() or 16000),
                            "width": int(wav_file.getsampwidth() or 2),
                            "channels": int(wav_file.getnchannels() or 1),
                        }

    ffmpeg = str(os.getenv("TATER_FFMPEG_PATH") or os.getenv("FFMPEG_PATH") or "").strip()
    if not ffmpeg:
        ffmpeg = str(shutil.which("ffmpeg") or "").strip()
    elif not Path(ffmpeg).expanduser().is_file():
        ffmpeg = str(shutil.which(ffmpeg) or "").strip()
    if not ffmpeg or not Path(ffmpeg).expanduser().is_file():
        raise RuntimeError("FFmpeg is needed to transcribe this clone-audio format.")
    try:
        completed = subprocess.run(
            [
                str(Path(ffmpeg).expanduser()),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-i",
                path,
                "-vn",
                "-f",
                "s16le",
                "-acodec",
                "pcm_s16le",
                "-ar",
                "16000",
                "-ac",
                "1",
                "pipe:1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120.0,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Clone audio took too long to decode for transcription.") from exc
    if completed.returncode != 0 or not completed.stdout:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(detail or "Could not decode clone audio for transcription.")
    return bytes(completed.stdout), {"rate": 16000, "width": 2, "channels": 1}


def _apple_silicon() -> bool:
    return sys.platform == "darwin" and platform.machine().lower() in {"arm64", "aarch64"}


def resolve_managed_tts_acceleration(value: Any = "auto") -> str:
    token = str(value or "auto").strip().lower().replace("-", "_") or "auto"
    if token in {"mps", "metal", "apple", "apple_mps"}:
        return "mps" if _apple_silicon() else "cpu"
    if token in {"cuda", "nvidia", "gpu"}:
        return "cuda"
    if token in {"rocm", "amd", "amd_gpu"}:
        return "rocm"
    if token in {"cpu", "none", "off"}:
        return "cpu"
    if _apple_silicon():
        return "mps"
    with contextlib.suppress(Exception):
        import torch

        if torch.cuda.is_available():
            return "rocm" if bool(getattr(getattr(torch, "version", None), "hip", None)) else "cuda"
    return "cpu"


def _environment_root(backend: str) -> Path:
    return managed_tts_root(backend) / "runtime"


def _environment_python(root: Path) -> Path:
    return root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _bootstrap_python() -> str:
    configured = str(os.getenv("TATER_MANAGED_TTS_PYTHON") or "").strip()
    candidates = [
        configured,
        shutil.which("python3.11") or "",
        shutil.which("python3.12") or "",
        sys.executable if sys.version_info[:2] in {(3, 11), (3, 12)} else "",
        shutil.which("python3.13") or "",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).expanduser().is_file():
            return str(Path(candidate).expanduser().resolve())
    raise RuntimeError("A compatible Python 3.11-3.13 interpreter is required for managed TTS runtimes.")


def _stack_id(backend: str, acceleration: str) -> str:
    if backend == QWEN_TTS_BACKEND and _apple_silicon():
        stack = f"mlx-audio-{_MLX_AUDIO_COMMIT}"
    elif backend == QWEN_TTS_BACKEND:
        stack = "qwen-tts-0.1.1-torch-2.9.1"
    else:
        stack = f"omnivoice-{_OMNIVOICE_COMMIT}-torch-2.8.0"
    return f"{_ENV_STACK_VERSION}:{backend}:{stack}:{acceleration or 'auto'}"


def _environment_ready(root: Path, stack_id: str) -> bool:
    python_bin = _environment_python(root)
    marker = root / ".tater-stack"
    if not python_bin.is_file() or not marker.is_file():
        return False
    with contextlib.suppress(OSError):
        return marker.read_text(encoding="utf-8").strip() == stack_id
    return False


def _run_install(command: list[str], *, timeout: float = _INSTALL_TIMEOUT_SECONDS) -> None:
    logger.info("[managed-tts] preparing runtime: %s", " ".join(command[:4]))
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        tail = "\n".join(str(completed.stdout or "").splitlines()[-30:])
        raise RuntimeError(f"Managed TTS runtime installation failed.\n{tail}".strip())


def _torch_index(acceleration: str) -> str:
    if acceleration == "cuda":
        return str(os.getenv("TATER_MANAGED_TTS_TORCH_INDEX") or "https://download.pytorch.org/whl/cu128")
    if acceleration == "rocm":
        return str(os.getenv("TATER_MANAGED_TTS_TORCH_INDEX") or "https://download.pytorch.org/whl/rocm6.4")
    if acceleration == "cpu" and sys.platform != "darwin":
        return "https://download.pytorch.org/whl/cpu"
    return ""


_environment_locks: Dict[str, threading.Lock] = {
    QWEN_TTS_BACKEND: threading.Lock(),
    OMNIVOICE_TTS_BACKEND: threading.Lock(),
}


def ensure_managed_tts_environment(backend: Any, acceleration: Any = "auto") -> Path:
    token = normalize_managed_tts_backend(backend)
    if token not in MANAGED_TTS_BACKENDS:
        raise ValueError(f"Unsupported managed TTS backend: {token or backend}")
    acceleration_token = resolve_managed_tts_acceleration(acceleration)
    root = _environment_root(token)
    bootstrap_python = _bootstrap_python()
    stack_id = f"{_stack_id(token, acceleration_token)}:{Path(bootstrap_python).name}"
    with _environment_locks[token]:
        if _environment_ready(root, stack_id):
            return _environment_python(root)

        root.parent.mkdir(parents=True, exist_ok=True)
        build_root = Path(tempfile.mkdtemp(prefix=f".{token}-runtime-", dir=str(root.parent)))
        old_root: Optional[Path] = None
        try:
            _run_install([bootstrap_python, "-m", "venv", str(build_root)])
            python_bin = _environment_python(build_root)
            _run_install([str(python_bin), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
            if token == QWEN_TTS_BACKEND and _apple_silicon():
                _run_install(
                    [
                        str(python_bin),
                        "-m",
                        "pip",
                        "install",
                        f"mlx-audio[tts] @ git+https://github.com/Blaizzy/mlx-audio.git@{_MLX_AUDIO_COMMIT}",
                        "huggingface_hub[hf_xet]",
                        "soundfile",
                    ]
                )
            else:
                torch_version = "2.9.1" if token == QWEN_TTS_BACKEND else "2.8.0"
                torch_command = [
                    str(python_bin),
                    "-m",
                    "pip",
                    "install",
                    f"torch=={torch_version}",
                    f"torchaudio=={torch_version}",
                ]
                index = _torch_index(acceleration_token)
                if index:
                    torch_command.extend(["--index-url", index])
                _run_install(torch_command)
                package = (
                    "qwen-tts==0.1.1"
                    if token == QWEN_TTS_BACKEND
                    else f"git+https://github.com/k2-fsa/OmniVoice.git@{_OMNIVOICE_COMMIT}"
                )
                _run_install(
                    [str(python_bin), "-m", "pip", "install", package, "huggingface_hub[hf_xet]", "soundfile"]
                )
            (build_root / ".tater-stack").write_text(stack_id, encoding="utf-8")

            old_root = root.with_name(f"{root.name}.old-{uuid.uuid4().hex[:8]}")
            if root.exists():
                os.replace(root, old_root)
            os.replace(build_root, root)
            with contextlib.suppress(OSError):
                shutil.rmtree(old_root)
        except Exception:
            if old_root is not None and old_root.exists() and not root.exists():
                with contextlib.suppress(OSError):
                    os.replace(old_root, root)
            with contextlib.suppress(OSError):
                shutil.rmtree(build_root)
            raise
    return _environment_python(root)


class _ManagedWorker:
    def __init__(self, backend: str) -> None:
        self.backend = backend
        self.lock = threading.RLock()
        self.process: Optional[subprocess.Popen[str]] = None
        self.responses: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue()
        self.stderr_tail: deque[str] = deque(maxlen=40)
        self._reader: Optional[threading.Thread] = None
        self._stderr_reader: Optional[threading.Thread] = None
        self.acceleration = ""
        self.loaded_model = ""
        self.loaded_ts = 0.0
        self.device = ""
        self.estimated_bytes = 0
        self.loaded_models: Dict[str, Dict[str, Any]] = {}

    def _drain_stdout(self, process: subprocess.Popen[str]) -> None:
        stream = process.stdout
        if stream is None:
            self.responses.put(None)
            return
        for raw in stream:
            try:
                row = json.loads(raw)
            except Exception:
                self.stderr_tail.append(f"invalid worker response: {raw.rstrip()}")
                continue
            self.responses.put(row if isinstance(row, dict) else None)
        self.responses.put(None)

    def _drain_stderr(self, process: subprocess.Popen[str]) -> None:
        stream = process.stderr
        if stream is None:
            return
        for raw in stream:
            line = str(raw or "").rstrip()
            if line:
                self.stderr_tail.append(line)

    def _stop_locked(self) -> None:
        process = self.process
        self.process = None
        self.acceleration = ""
        self.loaded_model = ""
        self.loaded_ts = 0.0
        self.device = ""
        self.estimated_bytes = 0
        self.loaded_models = {}
        if process is None:
            return
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.stdin.write(json.dumps({"action": "shutdown"}) + "\n") if process.stdin else None
                process.stdin.flush() if process.stdin else None
            with contextlib.suppress(Exception):
                process.wait(timeout=3.0)
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                process.wait(timeout=3.0)
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.kill()

    def stop(self) -> None:
        with self.lock:
            self._stop_locked()

    def _start_locked(self, acceleration: str) -> None:
        if self.process is not None and self.process.poll() is None and self.acceleration == acceleration:
            return
        self._stop_locked()
        while True:
            with contextlib.suppress(queue.Empty):
                self.responses.get_nowait()
                continue
            break
        python_bin = ensure_managed_tts_environment(self.backend, acceleration)
        worker_path = Path(__file__).resolve().with_name("managed_tts_worker.py")
        env = dict(os.environ)
        cache_root = managed_tts_root(self.backend) / "cache"
        cache_root.mkdir(parents=True, exist_ok=True)
        env["HF_HOME"] = str(cache_root)
        env["HUGGINGFACE_HUB_CACHE"] = str(cache_root / "hub")
        env["TATER_MANAGED_TTS_ACCELERATION"] = acceleration
        process = subprocess.Popen(
            [str(python_bin), str(worker_path), "--backend", self.backend],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        self.process = process
        self.acceleration = acceleration
        self._reader = threading.Thread(target=self._drain_stdout, args=(process,), daemon=True)
        self._stderr_reader = threading.Thread(target=self._drain_stderr, args=(process,), daemon=True)
        self._reader.start()
        self._stderr_reader.start()

    def request(self, payload: Dict[str, Any], *, acceleration: str, timeout: float) -> Dict[str, Any]:
        with self.lock:
            self._start_locked(acceleration)
            process = self.process
            if process is None or process.stdin is None:
                raise RuntimeError("Managed TTS worker did not start.")
            request_id = uuid.uuid4().hex
            row = {**payload, "id": request_id}
            try:
                process.stdin.write(json.dumps(row, ensure_ascii=False) + "\n")
                process.stdin.flush()
            except Exception as exc:
                self._stop_locked()
                raise RuntimeError(f"Managed TTS worker stopped unexpectedly: {exc}") from exc

            deadline = time.monotonic() + max(1.0, timeout)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    tail = "\n".join(self.stderr_tail)
                    self._stop_locked()
                    raise TimeoutError(f"Managed TTS synthesis timed out.\n{tail}".strip())
                try:
                    response = self.responses.get(timeout=remaining)
                except queue.Empty:
                    continue
                if response is None:
                    tail = "\n".join(self.stderr_tail)
                    self._stop_locked()
                    raise RuntimeError(f"Managed TTS worker exited unexpectedly.\n{tail}".strip())
                if str(response.get("id") or "") != request_id:
                    continue
                if not response.get("ok"):
                    raise RuntimeError(str(response.get("error") or "Managed TTS synthesis failed."))
                if bool(response.get("loaded")):
                    self.loaded_model = str(response.get("model") or "").strip()
                    self.loaded_ts = float(response.get("loaded_ts") or time.time())
                    self.device = str(response.get("device") or "").strip()
                    self.estimated_bytes = max(0, int(response.get("estimated_bytes") or 0))
                    if self.loaded_model:
                        self.loaded_models[self.loaded_model] = {
                            "model": self.loaded_model,
                            "loaded_ts": self.loaded_ts,
                            "device": self.device,
                            "estimated_bytes": self.estimated_bytes,
                        }
                return response


_workers: Dict[str, _ManagedWorker] = {
    QWEN_TTS_BACKEND: _ManagedWorker(QWEN_TTS_BACKEND),
    OMNIVOICE_TTS_BACKEND: _ManagedWorker(OMNIVOICE_TTS_BACKEND),
}
_managed_dispatch_lock = threading.RLock()


def clear_managed_tts_workers() -> Dict[str, int]:
    cleared: Dict[str, int] = {}
    with _managed_dispatch_lock:
        for backend, worker in _workers.items():
            alive = worker.process is not None and worker.process.poll() is None
            worker.stop()
            cleared[backend] = 1 if alive else 0
    return cleared


atexit.register(clear_managed_tts_workers)


def managed_tts_workers_snapshot() -> Dict[str, Any]:
    rows = []
    with _managed_dispatch_lock:
        for backend, worker in _workers.items():
            with worker.lock:
                process = worker.process
                running = process is not None and process.poll() is None
                if not running or not worker.loaded_models:
                    continue
                for model_row in worker.loaded_models.values():
                    rows.append(
                        {
                            "backend": backend,
                            "model": str(model_row.get("model") or ""),
                            "pid": int(process.pid or 0),
                            "acceleration": worker.acceleration,
                            "device": str(model_row.get("device") or ""),
                            "estimated_bytes": max(0, int(model_row.get("estimated_bytes") or 0)),
                            "loaded_ts": float(model_row.get("loaded_ts") or 0.0),
                            "model_root": str(managed_tts_root(backend) / "cache"),
                        }
                    )
    rows.sort(key=lambda row: (str(row.get("backend") or ""), str(row.get("model") or "")))
    return {"loaded_count": len(rows), "models": rows}


def warm_managed_tts_model(
    *,
    backend: Any,
    model: Any = "",
    acceleration: Any = "auto",
) -> Dict[str, Any]:
    token = normalize_managed_tts_backend(backend)
    if token not in MANAGED_TTS_BACKENDS:
        raise ValueError(f"Unsupported managed TTS backend: {token or backend}")
    selected_model = str(model or "").strip() or (
        DEFAULT_QWEN_TTS_MODEL if token == QWEN_TTS_BACKEND else DEFAULT_OMNIVOICE_TTS_MODEL
    )
    acceleration_token = resolve_managed_tts_acceleration(acceleration)
    with _managed_dispatch_lock:
        _workers[token].request(
            {"action": "load", "model": selected_model},
            acceleration=acceleration_token,
            timeout=_SYNTHESIS_TIMEOUT_SECONDS,
        )
    snapshot = managed_tts_workers_snapshot()
    for row in snapshot.get("models", []):
        if row.get("backend") == token and row.get("model") == selected_model:
            return {"ok": True, **row}
    raise RuntimeError(f"Managed TTS worker did not report {selected_model} as loaded.")


def synthesize_managed_tts_pcm(
    text: str,
    *,
    backend: Any,
    model: Any = "",
    clone_audio: Any = "",
    clone_text: Any = "",
    language: Any = "",
    instruct: Any = "",
    acceleration: Any = "auto",
) -> Tuple[bytes, Dict[str, int]]:
    token = normalize_managed_tts_backend(backend)
    if token not in MANAGED_TTS_BACKENDS:
        raise ValueError(f"Unsupported managed TTS backend: {token or backend}")
    prompt = str(text or "").strip()
    if not prompt:
        return b"", {}
    reference_path = validate_clone_audio_path(token, clone_audio)
    reference_text = str(clone_text or "").strip()
    selected_model = str(model or "").strip() or (
        DEFAULT_QWEN_TTS_MODEL if token == QWEN_TTS_BACKEND else DEFAULT_OMNIVOICE_TTS_MODEL
    )
    if token == QWEN_TTS_BACKEND and selected_model == DEFAULT_QWEN_TTS_MODEL:
        if not reference_path:
            raise RuntimeError("Qwen voice cloning needs a reference audio file. Add one in Settings > Speech.")
    if token == QWEN_TTS_BACKEND and selected_model == QWEN_TTS_VOICE_DESIGN_MODEL:
        reference_path = ""
        reference_text = ""

    acceleration_token = resolve_managed_tts_acceleration(acceleration)
    output_root = managed_tts_root(token) / "output"
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / f"{uuid.uuid4().hex}.wav"
    try:
        with _managed_dispatch_lock:
            response = _workers[token].request(
                {
                    "action": "synthesize",
                    "text": prompt,
                    "model": selected_model,
                    "clone_audio": reference_path,
                    "clone_text": reference_text,
                    "language": str(language or "").strip(),
                    "instruct": str(instruct or "").strip(),
                    "output_path": str(output_path),
                },
                acceleration=acceleration_token,
                timeout=_SYNTHESIS_TIMEOUT_SECONDS,
            )
    except Exception:
        with contextlib.suppress(OSError):
            output_path.unlink()
        raise
    resolved_output = Path(str(response.get("output_path") or output_path)).resolve()
    try:
        resolved_output.relative_to(output_root.resolve())
    except ValueError as exc:
        raise RuntimeError("Managed TTS worker returned an invalid output path.") from exc
    try:
        with wave.open(str(resolved_output), "rb") as wav_file:
            audio_format = {
                "rate": int(wav_file.getframerate()),
                "width": int(wav_file.getsampwidth()),
                "channels": int(wav_file.getnchannels()),
            }
            audio_bytes = wav_file.readframes(wav_file.getnframes())
    finally:
        with contextlib.suppress(OSError):
            resolved_output.unlink()
    if not audio_bytes:
        raise RuntimeError("Managed TTS produced no audio.")
    return audio_bytes, audio_format
