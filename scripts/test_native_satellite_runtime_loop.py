from __future__ import annotations

import asyncio
import threading
import unittest
from typing import Any

from tater_voice import native_satellite


class NativeSatelliteRuntimeLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        native_satellite.release_runtime_loop()
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()
        self.loop = asyncio.new_event_loop()
        self.started = threading.Event()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.assertTrue(self.started.wait(2.0))
        native_satellite.bind_runtime_loop(self.loop)

    def tearDown(self) -> None:
        native_satellite.release_runtime_loop(self.loop)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(2.0)
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.started.set()
        self.loop.run_forever()
        self.loop.close()

    def test_sync_call_waits_for_lock_on_owner_loop(self) -> None:
        lock_held = threading.Event()
        release_lock: dict[str, asyncio.Event] = {}

        async def hold_client_lock() -> None:
            release_event = asyncio.Event()
            release_lock["event"] = release_event
            async with native_satellite._clients_lock:
                lock_held.set()
                await release_event.wait()

        holder = asyncio.run_coroutine_threadsafe(hold_client_lock(), self.loop)
        self.assertTrue(lock_held.wait(2.0))

        outcome: dict[str, Any] = {}

        def read_status() -> None:
            try:
                outcome["result"] = native_satellite.run_on_runtime_loop(
                    native_satellite.status(),
                    timeout=2.0,
                )
            except Exception as exc:  # pragma: no cover - asserted below
                outcome["error"] = exc

        worker = threading.Thread(target=read_status, daemon=True)
        worker.start()

        async def wait_for_lock_waiter() -> None:
            for _ in range(200):
                if native_satellite._clients_lock.locked() and native_satellite._clients_lock._waiters:
                    return
                await asyncio.sleep(0.005)
            raise TimeoutError("native status did not wait on the client lock")

        asyncio.run_coroutine_threadsafe(wait_for_lock_waiter(), self.loop).result(2.0)
        self.loop.call_soon_threadsafe(release_lock["event"].set)

        worker.join(3.0)
        holder.result(2.0)
        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", outcome)
        self.assertEqual(0, outcome["result"]["count"])

    def test_rejects_blocking_from_owner_loop(self) -> None:
        async def call_from_owner() -> str:
            with self.assertRaisesRegex(RuntimeError, "Cannot synchronously wait"):
                native_satellite.run_on_runtime_loop(asyncio.sleep(0), timeout=1.0)
            return "ok"

        result = asyncio.run_coroutine_threadsafe(call_from_owner(), self.loop).result(2.0)
        self.assertEqual("ok", result)


if __name__ == "__main__":
    unittest.main()
