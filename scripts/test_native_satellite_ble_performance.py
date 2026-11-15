from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tater_voice import native_ble, native_satellite


class NativeBLEPerformanceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.selector = "native:synthetic"
        self.websocket = object()
        self.hello = {"device_id": "synthetic", "device_name": "Test", "board": "cronos", "room": "Test room"}
        self.row = {"connected": True, "websocket": self.websocket, "hello": {"payload": self.hello}}
        self.old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        self.patches = [
            patch.dict(native_satellite._clients, {self.selector: self.row}, clear=True),
            patch.object(native_satellite, "_upsert_registry_from_hello"),
            patch.object(native_satellite, "_notify_state_change"),
            patch.object(native_satellite, "_vp", return_value=SimpleNamespace(logger=Mock())),
        ]
        _, self.registry, self.notify, _ = [item.start() for item in self.patches]
        self.message = {"type": "ble.advertisements", "payload": {"adverts": [], "batch_id": 1}}

    def tearDown(self) -> None:
        for item in reversed(self.patches):
            item.stop()
        native_satellite._clients_lock = self.old_lock

    async def test_ble_batch_never_marks_registry_disconnected(self) -> None:
        result = {"ok": True, "accepted": 0}
        with patch.object(native_ble, "ingest_advertisements", return_value=result) as ingest:
            await native_satellite._handle_text_message(self.selector, self.message, websocket=self.websocket)
        self.registry.assert_not_called()
        self.notify.assert_not_called()
        self.assertTrue(self.row["connected"])
        self.assertEqual(self.row["ble"], result)
        ingest.assert_called_once_with(self.selector, self.message["payload"], metadata=self.hello)

    async def test_ble_ingest_does_not_block_event_loop_or_hold_clients_lock(self) -> None:
        entered, release = threading.Event(), threading.Event()
        worker_thread = None

        def slow_ingest(*_args, **_kwargs):
            nonlocal worker_thread
            worker_thread = threading.get_ident()
            entered.set()
            release.wait(0.5)
            return {"ok": True}

        with patch.object(native_ble, "ingest_advertisements", side_effect=slow_ingest):
            task = asyncio.create_task(native_satellite._handle_text_message(self.selector, self.message, websocket=self.websocket))
            try:
                for _ in range(100):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(entered.is_set())
                self.assertFalse(task.done(), "BLE processing blocked the event loop")
                self.assertNotEqual(worker_thread, threading.get_ident())
                await asyncio.wait_for(native_satellite._clients_lock.acquire(), 0.1)
                native_satellite._clients_lock.release()
                # A replacement connection must never inherit this older batch.
                replacement = {"connected": True, "websocket": object()}
                native_satellite._clients[self.selector] = replacement
            finally:
                release.set()
                await task
            self.assertNotIn("ble", replacement)
        self.registry.assert_not_called()

    async def test_genuine_setup_transition_still_updates_registry(self) -> None:
        await native_satellite._handle_text_message(
            self.selector, {"type": "status", "payload": {"state": "setup"}}, websocket=self.websocket
        )
        self.registry.assert_called_once_with(self.selector, self.hello, connected=False)
        self.assertFalse(self.row["connected"])


if __name__ == "__main__":
    unittest.main()
