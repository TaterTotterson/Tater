from __future__ import annotations

import asyncio
import inspect
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union


IntervalSource = Union[float, int, Callable[[], Union[float, int]]]
TaskRunner = Callable[[str], Any]
CoreTaskRunner = Callable[[], Any]


@dataclass(frozen=True)
class SystemTaskSpec:
    task_id: str
    label: str
    description: str
    interval_seconds: IntervalSource
    runner: TaskRunner
    initial_delay_seconds: float = 0.0
    order: int = 0


class SystemTaskManager:
    """Small shared scheduler for cached UI and maintenance jobs."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._specs: Dict[str, SystemTaskSpec] = {}
        self._states: Dict[str, Dict[str, Any]] = {}
        self._active: Dict[str, asyncio.Task[Any]] = {}
        self._debounce_handles: Dict[str, asyncio.TimerHandle] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scheduler_task: Optional[asyncio.Task[Any]] = None

    @staticmethod
    def _task_id(value: Any) -> str:
        return str(value or "").strip().lower().replace(" ", "_")

    @staticmethod
    def _interval(spec: SystemTaskSpec) -> float:
        try:
            value = spec.interval_seconds() if callable(spec.interval_seconds) else spec.interval_seconds
            return max(0.0, float(value or 0.0))
        except Exception:
            return 0.0

    def register(
        self,
        task_id: str,
        *,
        label: str,
        description: str,
        interval_seconds: IntervalSource,
        runner: TaskRunner,
        initial_delay_seconds: float = 0.0,
        order: int = 0,
    ) -> None:
        token = self._task_id(task_id)
        if not token:
            raise ValueError("task_id is required")
        if not callable(runner):
            raise ValueError(f"runner is required for system task: {token}")
        spec = SystemTaskSpec(
            task_id=token,
            label=str(label or token).strip() or token,
            description=str(description or "").strip(),
            interval_seconds=interval_seconds,
            runner=runner,
            initial_delay_seconds=max(0.0, float(initial_delay_seconds or 0.0)),
            order=int(order or 0),
        )
        now = time.time()
        interval = self._interval(spec)
        with self._lock:
            self._specs[token] = spec
            state = self._states.setdefault(
                token,
                {
                    "running": False,
                    "started_at": 0.0,
                    "finished_at": 0.0,
                    "duration_ms": 0.0,
                    "next_run_at": 0.0,
                    "last_error": "",
                    "last_reason": "",
                    "run_count": 0,
                    "rerun_requested": False,
                    "rerun_reason": "",
                },
            )
            if not bool(state.get("running")):
                state["next_run_at"] = now + spec.initial_delay_seconds if interval > 0 else 0.0

    def _row(self, spec: SystemTaskSpec, state: Dict[str, Any], *, now: float) -> Dict[str, Any]:
        interval = self._interval(spec)
        running = bool(state.get("running"))
        last_error = str(state.get("last_error") or "").strip()
        next_run_at = float(state.get("next_run_at") or 0.0) if interval > 0 else 0.0
        if running:
            status = "running"
        elif last_error:
            status = "error"
        elif interval <= 0:
            status = "disabled"
        else:
            status = "idle"
        return {
            "id": spec.task_id,
            "label": spec.label,
            "description": spec.description,
            "status": status,
            "running": running,
            "enabled": bool(interval > 0),
            "interval_seconds": interval,
            "started_at": float(state.get("started_at") or 0.0),
            "finished_at": float(state.get("finished_at") or 0.0),
            "duration_ms": max(0.0, float(state.get("duration_ms") or 0.0)),
            "next_run_at": next_run_at,
            "next_run_in_seconds": max(0.0, next_run_at - now) if next_run_at else 0.0,
            "last_error": last_error,
            "last_reason": str(state.get("last_reason") or "").strip(),
            "run_count": max(0, int(state.get("run_count") or 0)),
        }

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for task_id, spec in self._specs.items():
                row = self._row(spec, dict(self._states.get(task_id) or {}), now=now)
                row["_order"] = spec.order
                rows.append(row)
        rows.sort(key=lambda row: (int(row.get("_order") or 0), str(row.get("label") or "").lower()))
        for row in rows:
            row.pop("_order", None)
        return {
            "ok": True,
            "generated_at": now,
            "running_count": len([row for row in rows if row.get("running")]),
            "error_count": len([row for row in rows if row.get("status") == "error"]),
            "tasks": rows,
        }

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        token = self._task_id(task_id)
        payload = self.snapshot()
        return next((row for row in payload["tasks"] if row.get("id") == token), None)

    async def trigger(
        self,
        task_id: str,
        *,
        reason: str = "manual",
        queue_if_running: bool = False,
    ) -> bool:
        token = self._task_id(task_id)
        with self._lock:
            spec = self._specs.get(token)
            state = self._states.get(token)
            if not spec or not state:
                raise KeyError(token)
            if bool(state.get("running")):
                if queue_if_running:
                    state["rerun_requested"] = True
                    state["rerun_reason"] = str(reason or "refresh").strip() or "refresh"
                return False
            state.update(
                {
                    "running": True,
                    "started_at": time.time(),
                    "next_run_at": 0.0,
                    "last_error": "",
                    "last_reason": str(reason or "manual").strip() or "manual",
                }
            )
        task = asyncio.create_task(self._execute(spec, str(reason or "manual").strip() or "manual"))
        with self._lock:
            self._active[token] = task
        return True

    async def _execute(self, spec: SystemTaskSpec, reason: str) -> None:
        error = ""
        cancelled = False
        rerun_reason = ""
        started = time.monotonic()
        try:
            result = spec.runner(reason)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            cancelled = True
            error = "Cancelled during shutdown."
            raise
        except Exception as exc:
            error = str(exc).strip() or exc.__class__.__name__
        finally:
            finished_at = time.time()
            interval = self._interval(spec)
            with self._lock:
                state = self._states.setdefault(spec.task_id, {})
                if bool(state.get("rerun_requested")):
                    rerun_reason = str(state.get("rerun_reason") or "event").strip() or "event"
                state.update(
                    {
                        "running": False,
                        "finished_at": finished_at,
                        "duration_ms": max(0.0, (time.monotonic() - started) * 1000.0),
                        "next_run_at": finished_at + interval if interval > 0 else 0.0,
                        "last_error": error,
                        "run_count": max(0, int(state.get("run_count") or 0)) + 1,
                        "rerun_requested": False,
                        "rerun_reason": "",
                    }
                )
                self._active.pop(spec.task_id, None)
            if rerun_reason and not cancelled:
                asyncio.create_task(
                    self.trigger(spec.task_id, reason=rerun_reason, queue_if_running=True)
                )

    def request_run(self, task_id: str, *, reason: str = "refresh") -> bool:
        token = self._task_id(task_id)
        with self._lock:
            loop = self._loop
            exists = token in self._specs
        if not exists or not loop or loop.is_closed() or not loop.is_running():
            return False

        def queue() -> None:
            asyncio.create_task(self.trigger(token, reason=reason))

        loop.call_soon_threadsafe(queue)
        return True

    def request_run_debounced(
        self,
        task_id: str,
        *,
        reason: str = "event",
        delay_seconds: float = 2.0,
    ) -> bool:
        token = self._task_id(task_id)
        with self._lock:
            loop = self._loop
            exists = token in self._specs
        if not exists or not loop or loop.is_closed() or not loop.is_running():
            return False
        delay = max(0.0, float(delay_seconds or 0.0))

        def arm() -> None:
            with self._lock:
                previous = self._debounce_handles.pop(token, None)
                if previous:
                    previous.cancel()

            def fire() -> None:
                with self._lock:
                    self._debounce_handles.pop(token, None)
                asyncio.create_task(
                    self.trigger(token, reason=reason, queue_if_running=True)
                )

            handle = loop.call_later(delay, fire)
            with self._lock:
                self._debounce_handles[token] = handle

        loop.call_soon_threadsafe(arm)
        return True

    def reschedule(self, task_id: str, *, immediate: bool = False) -> bool:
        token = self._task_id(task_id)
        with self._lock:
            spec = self._specs.get(token)
            state = self._states.get(token)
            if not spec or state is None:
                return False
            interval = self._interval(spec)
            state["next_run_at"] = time.time() if immediate and interval > 0 else time.time() + interval if interval > 0 else 0.0
        return True

    async def _scheduler_loop(self) -> None:
        while True:
            now = time.time()
            due: List[str] = []
            with self._lock:
                for task_id, spec in self._specs.items():
                    state = self._states.get(task_id) or {}
                    interval = self._interval(spec)
                    if interval <= 0:
                        state["next_run_at"] = 0.0
                        continue
                    next_run_at = float(state.get("next_run_at") or 0.0)
                    if next_run_at <= 0:
                        state["next_run_at"] = now + interval
                    elif next_run_at <= now and not bool(state.get("running")):
                        due.append(task_id)
            for task_id in due:
                try:
                    await self.trigger(task_id, reason="schedule")
                except KeyError:
                    continue
            await asyncio.sleep(1.0)

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            self._loop = loop
            if self._scheduler_task and not self._scheduler_task.done():
                return
            self._scheduler_task = loop.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        with self._lock:
            scheduler = self._scheduler_task
            active = list(self._active.values())
            debounce_handles = list(self._debounce_handles.values())
            self._debounce_handles = {}
            self._scheduler_task = None
            self._loop = None
        if scheduler:
            scheduler.cancel()
        for task in active:
            task.cancel()
        for handle in debounce_handles:
            handle.cancel()
        pending = [task for task in [scheduler, *active] if task]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


system_task_manager = SystemTaskManager()


class CoreTaskRunManager:
    """Dispatch core-owned manual tasks without scheduling a second copy of them."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._states: Dict[str, Dict[str, Any]] = {}
        self._active: Dict[str, asyncio.Task[Any]] = {}

    @staticmethod
    def _key(core_key: Any, task_id: Any) -> str:
        core = str(core_key or "").strip().lower()
        task = str(task_id or "").strip().lower()
        return f"{core}:{task}"

    async def trigger(self, core_key: str, task_id: str, runner: CoreTaskRunner) -> bool:
        key = self._key(core_key, task_id)
        if not key or key == ":" or not callable(runner):
            raise ValueError("A core task runner is required.")
        with self._lock:
            state = self._states.setdefault(key, {})
            if bool(state.get("running")):
                return False
            state.update(
                {
                    "running": True,
                    "started_at": time.time(),
                    "last_error": "",
                }
            )
        task = asyncio.create_task(self._execute(key, runner))
        with self._lock:
            self._active[key] = task
        return True

    async def _execute(self, key: str, runner: CoreTaskRunner) -> None:
        started = time.monotonic()
        error = ""
        try:
            if inspect.iscoroutinefunction(runner):
                await runner()
            else:
                result = await asyncio.to_thread(runner)
                if inspect.isawaitable(result):
                    await result
        except asyncio.CancelledError:
            error = "Cancelled during shutdown."
            raise
        except Exception as exc:
            error = str(exc).strip() or exc.__class__.__name__
        finally:
            with self._lock:
                state = self._states.setdefault(key, {})
                state.update(
                    {
                        "running": False,
                        "finished_at": time.time(),
                        "duration_ms": max(0.0, (time.monotonic() - started) * 1000.0),
                        "last_error": error,
                        "run_count": max(0, int(state.get("run_count") or 0)) + 1,
                    }
                )
                self._active.pop(key, None)

    def state(self, core_key: str, task_id: str) -> Dict[str, Any]:
        key = self._key(core_key, task_id)
        with self._lock:
            return dict(self._states.get(key) or {})

    async def stop(self) -> None:
        with self._lock:
            active = list(self._active.values())
            self._active = {}
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)


core_task_run_manager = CoreTaskRunManager()
