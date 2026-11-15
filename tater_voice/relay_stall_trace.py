"""Opt-in, bounded code-location sampling while the host event loop is stalled.

Never reads frame locals, source text, packet contents, audio or images.
"""
from __future__ import annotations

from collections import deque
from pathlib import Path
import sys
import threading
import time
from typing import Any


class RelayStallTrace:
    def __init__(self, started_at: float) -> None:
        self.started_at = started_at
        self.loop_thread = threading.get_ident()
        self.last_heartbeat = time.monotonic()
        self.samples: deque[dict[str, Any]] = deque(maxlen=16)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="tater-relay-stall-trace", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def heartbeat(self) -> None:
        self.last_heartbeat = time.monotonic()

    def _run(self) -> None:
        previous_capture = float("-inf")
        while not self.stop_event.wait(0.05):
            now = time.monotonic()
            lag_ms = (now - self.last_heartbeat) * 1000.0
            if lag_ms < 100.0 or now - previous_capture < 0.1:
                continue
            previous_capture = now
            frame = sys._current_frames().get(self.loop_thread)
            stack = []
            try:
                while frame is not None and len(stack) < 64:
                    stack.append({
                        "file": Path(frame.f_code.co_filename).name,
                        "function": frame.f_code.co_name,
                        "line": frame.f_lineno,
                    })
                    frame = frame.f_back
            finally:
                # Do not retain live frames or the objects referenced by them.
                del frame
            sample = {
                "at_ms": round((now - self.started_at) * 1000.0, 3),
                "heartbeat_age_ms": round(lag_ms, 3),
                "stack_innermost_first": stack,
            }
            with self.lock:
                self.samples.append(sample)

    def snapshot(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.samples)

    def stop(self) -> None:
        self.stop_event.set()

    def join(self) -> None:
        self.thread.join(timeout=1.0)
