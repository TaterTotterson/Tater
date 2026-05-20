from __future__ import annotations

import asyncio
import functools
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.getenv(name, "") or "").strip())
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), int(value)))


WAKE_EXECUTOR_WORKERS = _env_int("TATER_WAKE_EXECUTOR_WORKERS", 2, minimum=1, maximum=8)
SPEECH_EXECUTOR_WORKERS = _env_int("TATER_SPEECH_EXECUTOR_WORKERS", 2, minimum=1, maximum=8)
DASHBOARD_EXECUTOR_WORKERS = _env_int("TATER_DASHBOARD_EXECUTOR_WORKERS", 1, minimum=1, maximum=4)
BACKGROUND_EXECUTOR_WORKERS = _env_int("TATER_BACKGROUND_EXECUTOR_WORKERS", 4, minimum=1, maximum=16)

EXECUTOR_WORKER_BOUNDS: Dict[str, Dict[str, int]] = {
    "wake": {"default": WAKE_EXECUTOR_WORKERS, "min": 1, "max": 8},
    "speech": {"default": SPEECH_EXECUTOR_WORKERS, "min": 1, "max": 8},
    "dashboard": {"default": DASHBOARD_EXECUTOR_WORKERS, "min": 1, "max": 4},
    "background": {"default": BACKGROUND_EXECUTOR_WORKERS, "min": 1, "max": 16},
}

_executor_lock = threading.RLock()
_worker_counts: Dict[str, int] = {
    name: int(bounds["default"])
    for name, bounds in EXECUTOR_WORKER_BOUNDS.items()
}


def _make_executor(name: str, workers: int) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(
        max_workers=int(workers),
        thread_name_prefix=f"tater-{name}",
    )


_wake_executor = _make_executor("wake", _worker_counts["wake"])
_speech_executor = _make_executor("speech", _worker_counts["speech"])
_dashboard_executor = _make_executor("dashboard", _worker_counts["dashboard"])
_background_executor = _make_executor("background", _worker_counts["background"])


def _clamp_workers(name: str, value: Any) -> int:
    bounds = EXECUTOR_WORKER_BOUNDS[name]
    try:
        workers = int(value)
    except Exception:
        workers = int(bounds["default"])
    return max(int(bounds["min"]), min(int(bounds["max"]), workers))


def runtime_executor_snapshot() -> Dict[str, Any]:
    with _executor_lock:
        workers = dict(_worker_counts)
    return {
        "workers": workers,
        "bounds": {name: dict(bounds) for name, bounds in EXECUTOR_WORKER_BOUNDS.items()},
    }


def configure_runtime_executors(
    *,
    wake_workers: Optional[Any] = None,
    speech_workers: Optional[Any] = None,
    dashboard_workers: Optional[Any] = None,
    background_workers: Optional[Any] = None,
) -> Dict[str, Any]:
    global _wake_executor, _speech_executor, _dashboard_executor, _background_executor

    with _executor_lock:
        current_counts = dict(_worker_counts)
    requested = {
        "wake": current_counts["wake"] if wake_workers is None else _clamp_workers("wake", wake_workers),
        "speech": current_counts["speech"] if speech_workers is None else _clamp_workers("speech", speech_workers),
        "dashboard": current_counts["dashboard"] if dashboard_workers is None else _clamp_workers("dashboard", dashboard_workers),
        "background": current_counts["background"] if background_workers is None else _clamp_workers("background", background_workers),
    }
    old_executors: list[ThreadPoolExecutor] = []

    with _executor_lock:
        if requested["wake"] != _worker_counts["wake"]:
            old_executors.append(_wake_executor)
            _wake_executor = _make_executor("wake", requested["wake"])
            _worker_counts["wake"] = requested["wake"]
        if requested["speech"] != _worker_counts["speech"]:
            old_executors.append(_speech_executor)
            _speech_executor = _make_executor("speech", requested["speech"])
            _worker_counts["speech"] = requested["speech"]
        if requested["dashboard"] != _worker_counts["dashboard"]:
            old_executors.append(_dashboard_executor)
            _dashboard_executor = _make_executor("dashboard", requested["dashboard"])
            _worker_counts["dashboard"] = requested["dashboard"]
        if requested["background"] != _worker_counts["background"]:
            old_executors.append(_background_executor)
            _background_executor = _make_executor("background", requested["background"])
            _worker_counts["background"] = requested["background"]

    for executor in old_executors:
        executor.shutdown(wait=False, cancel_futures=False)

    return runtime_executor_snapshot()


async def _run(executor: ThreadPoolExecutor, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(executor, call)


async def run_wake(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    with _executor_lock:
        executor = _wake_executor
    return await _run(executor, func, *args, **kwargs)


async def run_speech(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    with _executor_lock:
        executor = _speech_executor
    return await _run(executor, func, *args, **kwargs)


async def run_dashboard(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    with _executor_lock:
        executor = _dashboard_executor
    return await _run(executor, func, *args, **kwargs)


async def run_background(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    with _executor_lock:
        executor = _background_executor
    return await _run(executor, func, *args, **kwargs)


def shutdown_runtime_executors(*, wait: bool = False, cancel_futures: bool = True) -> None:
    with _executor_lock:
        executors = (
            _wake_executor,
            _speech_executor,
            _dashboard_executor,
            _background_executor,
        )
    for executor in executors:
        executor.shutdown(wait=wait, cancel_futures=cancel_futures)
