"""Lifecycle and import management for installed Core and Portal surfaces."""

import importlib
import logging
import os
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Dict
from tater_paths import APP_ROOT

logger = logging.getLogger("tateros")


class SurfaceRuntimeManager:
    """Thread runtime manager for core/portal modules exposing run(stop_event=...)."""

    def __init__(self, *, kind: str, package_name: str, env_name: str, default_subdir: str):
        self.kind = str(kind).strip().lower() or "surface"
        self.package_name = package_name
        self.surface_dir = self._resolve_module_dir(env_name, default_subdir)
        self.lock = threading.RLock()
        self.threads: Dict[str, threading.Thread] = {}
        self.stop_flags: Dict[str, threading.Event] = {}

    def _resolve_module_dir(self, env_name: str, default_subdir: str) -> Path:
        app_root = APP_ROOT
        raw = str(os.getenv(env_name, "") or "").strip()
        if not raw:
            return (app_root / default_subdir).resolve()
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = (app_root / candidate).resolve()
        return candidate.resolve()

    def _ensure_import_context(self) -> None:
        parent = str(self.surface_dir.parent)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)

        package = sys.modules.get(self.package_name)
        if package is not None and not isinstance(package, ModuleType):
            sys.modules.pop(self.package_name, None)
            package = None

        importlib.invalidate_caches()
        if package is None:
            package = importlib.import_module(self.package_name)

        package_paths = getattr(package, "__path__", None)
        if package_paths is not None:
            expected = str(self.surface_dir)
            normalized = {str(Path(path).resolve()) for path in package_paths}
            if expected not in normalized:
                package_paths.append(expected)

    def _import_module(self, module_key: str, *, reload_module: bool = True):
        key = str(module_key or "").strip()
        if not key:
            raise ImportError(f"Missing {self.kind} module key")

        self._ensure_import_context()
        module_name = f"{self.package_name}.{key}"
        errors: List[str] = []
        last_exc: Optional[Exception] = None

        for _ in range(2):
            try:
                importlib.invalidate_caches()

                existing = sys.modules.get(module_name)
                if isinstance(existing, ModuleType):
                    if reload_module:
                        return importlib.reload(existing)
                    return existing

                if module_name in sys.modules:
                    sys.modules.pop(module_name, None)
                return importlib.import_module(module_name)
            except Exception as exc:  # pragma: no cover - defensive import guard
                last_exc = exc
                errors.append(f"{module_name}: {type(exc).__name__}: {exc}")
                sys.modules.pop(module_name, None)

        raise ImportError("; ".join(errors)) from last_exc

    def is_running(self, module_key: str) -> bool:
        with self.lock:
            thread = self.threads.get(module_key)
            return bool(thread and thread.is_alive())

    def start(self, module_key: str) -> Dict[str, Any]:
        key = str(module_key or "").strip()
        if not key:
            raise ValueError(f"Missing {self.kind} module key")

        with self.lock:
            thread = self.threads.get(key)
            stop_flag = self.stop_flags.get(key)

            if thread and thread.is_alive():
                return {"started": False, "running": True, "reason": "already-running"}

            if not isinstance(stop_flag, threading.Event):
                stop_flag = threading.Event()

        def runner():
            try:
                module = self._import_module(key)
                run_fn = getattr(module, "run", None)
                if callable(run_fn):
                    run_fn(stop_event=stop_flag)
                else:
                    logger.warning("[%s] %s missing run(stop_event=...)", self.kind, key)
            except Exception as exc:
                logger.error("[%s] %s crashed: %s", self.kind, key, exc, exc_info=True)
            finally:
                with self.lock:
                    current = self.threads.get(key)
                    if current is threading.current_thread():
                        self.threads.pop(key, None)
                        self.stop_flags.pop(key, None)

        thread = threading.Thread(
            target=runner,
            daemon=True,
            name=f"{self.kind}-{key}",
        )
        with self.lock:
            self.threads[key] = thread
            self.stop_flags[key] = stop_flag

        logger.info("[%s] starting %s", self.kind, key)
        thread.start()
        logger.info("[%s] started %s", self.kind, key)
        return {"started": True, "running": True, "reason": "started"}

    def stop(self, module_key: str, *, timeout: float = 3.0) -> Dict[str, Any]:
        key = str(module_key or "").strip()
        if not key:
            raise ValueError(f"Missing {self.kind} module key")

        with self.lock:
            thread = self.threads.get(key)
            stop_flag = self.stop_flags.get(key)

        logger.info("[%s] stopping %s", self.kind, key)
        if stop_flag:
            stop_flag.set()

        if thread and thread.is_alive():
            thread.join(timeout=timeout)

        running = bool(thread and thread.is_alive())
        with self.lock:
            if not running:
                self.threads.pop(key, None)
                self.stop_flags.pop(key, None)
                logger.info("[%s] stopped %s", self.kind, key)
            else:
                logger.warning("[%s] stop timed out for %s", self.kind, key)

        return {
            "stopped": not running,
            "running": running,
            "reason": "stop-timeout" if running else "stopped",
        }

    def stop_all(self, *, timeout: float = 8.0) -> None:
        with self.lock:
            keys = list(self.threads.keys())
            stop_flags = {key: self.stop_flags.get(key) for key in keys}

        for key in keys:
            logger.info("[%s] stopping %s", self.kind, key)
            stop_flag = stop_flags.get(key)
            if stop_flag:
                stop_flag.set()

        deadline = time.monotonic() + max(0.1, float(timeout or 0))
        pending = set(keys)
        while pending and time.monotonic() < deadline:
            for key in list(pending):
                with self.lock:
                    thread = self.threads.get(key)
                if not thread or not thread.is_alive():
                    with self.lock:
                        self.threads.pop(key, None)
                        self.stop_flags.pop(key, None)
                    logger.info("[%s] stopped %s", self.kind, key)
                    pending.discard(key)
            if pending:
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))

        for key in list(pending):
            with self.lock:
                thread = self.threads.get(key)
            if thread and thread.is_alive():
                logger.warning("[%s] stop timed out for %s", self.kind, key)
            else:
                with self.lock:
                    self.threads.pop(key, None)
                    self.stop_flags.pop(key, None)
                logger.info("[%s] stopped %s", self.kind, key)
