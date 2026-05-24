from __future__ import annotations

import asyncio
import os
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional


EXECUTOR_WORKER_BOUNDS: Dict[str, Dict[str, int]] = {
    "wake": {"default": 2, "min": 1, "max": 8},
    "stt": {"default": 1, "min": 1, "max": 4},
    "tts": {"default": 1, "min": 1, "max": 4},
    "speech": {"default": 2, "min": 1, "max": 4},
    "dashboard": {"default": 2, "min": 1, "max": 6},
    "background": {"default": 4, "min": 1, "max": 12},
}

_EXECUTOR_LOCK = threading.RLock()
_EXECUTORS: Dict[str, ThreadPoolExecutor] = {}
_WORKERS: Dict[str, int] = {}


def _coerce_workers(name: str, value: Any, *, default: int) -> int:
    bounds = EXECUTOR_WORKER_BOUNDS.get(name) or {}
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    min_value = int(bounds.get("min") or 1)
    max_value = int(bounds.get("max") or max(min_value, parsed))
    return max(min_value, min(max_value, parsed))


def _env_workers(name: str) -> Any:
    upper = name.upper()
    for key in (
        f"TATER_RUNTIME_{upper}_WORKERS",
        f"TATER_{upper}_WORKERS",
        f"TATER_{upper}_EXECUTOR_WORKERS",
    ):
        value = os.getenv(key)
        if value not in {None, ""}:
            return value
    return None


def _desired_worker_count(name: str, explicit: Any = None) -> int:
    bounds = EXECUTOR_WORKER_BOUNDS.get(name) or {}
    default = int(bounds.get("default") or 1)
    value = explicit if explicit is not None else _env_workers(name)
    return _coerce_workers(name, value, default=default)


def _ensure_worker_defaults_locked() -> None:
    for name in EXECUTOR_WORKER_BOUNDS:
        if name not in _WORKERS:
            _WORKERS[name] = _desired_worker_count(name)


def _ensure_executor(name: str) -> ThreadPoolExecutor:
    token = name if name in EXECUTOR_WORKER_BOUNDS else "background"
    with _EXECUTOR_LOCK:
        _ensure_worker_defaults_locked()
        executor = _EXECUTORS.get(token)
        if executor is None:
            workers = _desired_worker_count(token, _WORKERS.get(token))
            _WORKERS[token] = workers
            executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix=f"tater-{token}",
            )
            _EXECUTORS[token] = executor
        return executor


def runtime_executor_snapshot() -> Dict[str, Any]:
    with _EXECUTOR_LOCK:
        _ensure_worker_defaults_locked()
        workers = dict(_WORKERS)
        active = sorted(_EXECUTORS.keys())
    return {
        "mode": "dedicated_threadpools",
        "workers": workers,
        "bounds": {name: dict(bounds) for name, bounds in EXECUTOR_WORKER_BOUNDS.items()},
        "active": active,
        "enabled": True,
    }


def configure_runtime_executors(
    *,
    wake_workers: Optional[Any] = None,
    stt_workers: Optional[Any] = None,
    tts_workers: Optional[Any] = None,
    speech_workers: Optional[Any] = None,
    dashboard_workers: Optional[Any] = None,
    background_workers: Optional[Any] = None,
) -> Dict[str, Any]:
    explicit = {
        "wake": wake_workers,
        "stt": stt_workers if stt_workers is not None else speech_workers,
        "tts": tts_workers if tts_workers is not None else speech_workers,
        "speech": speech_workers,
        "dashboard": dashboard_workers,
        "background": background_workers,
    }
    with _EXECUTOR_LOCK:
        _ensure_worker_defaults_locked()
        for name in EXECUTOR_WORKER_BOUNDS:
            desired = _desired_worker_count(name, explicit.get(name))
            current = int(_WORKERS.get(name) or 0)
            if current == desired:
                continue
            _WORKERS[name] = desired
            executor = _EXECUTORS.pop(name, None)
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
    return runtime_executor_snapshot()


async def _run(name: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    call = functools.partial(func, *args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_ensure_executor(name), call)


async def run_wake(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("wake", func, *args, **kwargs)


async def run_stt(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("stt", func, *args, **kwargs)


async def run_tts(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("tts", func, *args, **kwargs)


async def run_speech(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("speech", func, *args, **kwargs)


async def run_dashboard(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("dashboard", func, *args, **kwargs)


async def run_background(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await _run("background", func, *args, **kwargs)


def shutdown_runtime_executors(*, wait: bool = False, cancel_futures: bool = True) -> None:
    with _EXECUTOR_LOCK:
        executors = list(_EXECUTORS.values())
        _EXECUTORS.clear()
    for executor in executors:
        executor.shutdown(wait=wait, cancel_futures=cancel_futures)
