from __future__ import annotations

import asyncio
import threading
import unittest
from unittest import mock

from tater_voice import native_satellite, sat_chat


class NativeStatusPerformanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_canonical_binary_media_send_does_not_read_aliases(self) -> None:
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        sent: list[bytes] = []

        async def sender(payload: bytes) -> None:
            sent.append(payload)

        try:
            with mock.patch.dict(
                native_satellite._clients,
                {"native:test": {"connected": True, "send_binary": sender}},
                clear=True,
            ), mock.patch.object(native_satellite, "_load_selector_aliases") as aliases:
                result = await native_satellite.send_binary("native:test", b"media")
            self.assertEqual(result, {"ok": True, "selector": "native:test", "bytes": 5})
            self.assertEqual(sent, [b"media"])
            aliases.assert_not_called()
        finally:
            native_satellite._clients_lock = old_lock

    async def test_legacy_binary_media_send_resolves_alias_off_loop(self) -> None:
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        sent: list[bytes] = []
        alias_thread = 0

        async def sender(payload: bytes) -> None:
            sent.append(payload)

        def aliases() -> dict[str, str]:
            nonlocal alias_thread
            alias_thread = threading.get_ident()
            return {"native:old": "native:new"}

        try:
            with mock.patch.dict(
                native_satellite._clients,
                {"native:new": {"connected": True, "send_binary": sender}},
                clear=True,
            ), mock.patch.object(native_satellite, "_load_selector_aliases", side_effect=aliases):
                event_thread = threading.get_ident()
                result = await native_satellite.send_binary("native:old", b"media")
            self.assertEqual(result, {"ok": True, "selector": "native:new", "bytes": 5})
            self.assertEqual(sent, [b"media"])
            self.assertNotEqual(alias_thread, event_thread)
        finally:
            native_satellite._clients_lock = old_lock

    async def test_lightweight_status_never_reads_settings(self) -> None:
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        row = {"hello": {"payload": {"board": "cronos", "capabilities": {"sat_chat_audio": True}}}}
        try:
            with mock.patch.dict(native_satellite._clients, {"native:test": row}, clear=True), mock.patch.object(
                native_satellite, "_voice_snapshot", return_value={}
            ), mock.patch.object(native_satellite, "_live_settings_payload") as settings:
                result = await native_satellite.status(include_live_settings=False)
                self.assertEqual(result["count"], 1)
                self.assertEqual(result["clients"]["native:test"]["board"], "cronos")
                self.assertNotIn("live_settings", result["clients"]["native:test"])
                settings.assert_not_called()
        finally:
            native_satellite._clients_lock = old_lock

    async def test_settings_io_releases_event_loop_and_client_lock(self) -> None:
        entered, release = threading.Event(), threading.Event()
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()

        def slow_settings(*_args, **_kwargs):
            entered.set()
            release.wait(1.0)
            return {"test_setting": True}

        row = {"hello": {"payload": {"board": "checkers"}}}
        try:
            with mock.patch.dict(native_satellite._clients, {"native:test": row}, clear=True), mock.patch.object(
                native_satellite, "_voice_snapshot", return_value={}
            ), mock.patch.object(native_satellite, "_live_settings_payload", side_effect=slow_settings):
                task = asyncio.create_task(native_satellite.status())
                try:
                    for _ in range(100):
                        if entered.is_set():
                            break
                        await asyncio.sleep(0.001)
                    self.assertTrue(entered.is_set())
                    self.assertFalse(task.done(), "settings I/O blocked the event loop")
                    await asyncio.wait_for(native_satellite._clients_lock.acquire(), 0.1)
                    native_satellite._clients_lock.release()
                finally:
                    release.set()
                    result = await task
                self.assertEqual(result["clients"]["native:test"]["live_settings"], {"test_setting": True})
        finally:
            native_satellite._clients_lock = old_lock

    async def test_directory_does_not_request_settings(self) -> None:
        snapshot = {"clients": {"native:test": {"board": "checkers"}}}
        with mock.patch.object(native_satellite, "status", new=mock.AsyncMock(return_value=snapshot)) as status:
            self.assertEqual(await sat_chat._native_clients(), snapshot["clients"])
            status.assert_awaited_once_with(include_live_settings=False)


if __name__ == "__main__":
    unittest.main()
