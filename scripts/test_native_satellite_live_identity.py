from __future__ import annotations

import asyncio
import contextlib
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import home, native_satellite


class LiveIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.selector = "native:echo-stable"
        self.queue = asyncio.Queue()
        self.row = {"connected": True, "queue": self.queue, "pending_requests": {}, "hello": {"payload": {"device_id": "echo-stable", "hardware_id": "stable-hardware", "device_name": "Office Echo", "room": "Office", "capabilities": {"identity_update": True}}}}
        self.patches = [
            mock.patch.object(native_satellite, "_clients", {self.selector: self.row}),
            mock.patch.object(native_satellite, "_clients_lock", asyncio.Lock()),
            mock.patch.object(native_satellite, "_canonical_selector", side_effect=lambda value: value),
            mock.patch.object(native_satellite, "_upsert_registry_from_hello"),
            mock.patch.object(native_satellite, "_notify_state_change"),
        ]
        self.mocks = [patch.start() for patch in self.patches]
        self.addCleanup(lambda: [patch.stop() for patch in reversed(self.patches)])

    async def test_round_trip_updates_live_hello_registry_and_room_before_success(self):
        display_identity = self.row["hello"]["payload"]
        operation = asyncio.create_task(native_satellite.update_identity(self.selector, "Kitchen Écho", ""))
        command = await asyncio.wait_for(self.queue.get(), 1)
        self.assertFalse(operation.done())
        self.assertEqual(command["type"], "identity.set")
        self.assertEqual(command["payload"], {"device_name": "Kitchen Écho", "room": ""})
        await native_satellite._handle_text_message(self.selector, {"type": "identity.result", "payload": {"reply_to": command["id"], "ok": True, "device_name": "Kitchen Écho", "room": ""}})
        result = await operation
        self.assertEqual(result["room"], "")
        hello = self.row["hello"]["payload"]
        self.assertEqual((hello["device_name"], hello["room"], hello["area_name"]), ("Kitchen Écho", "", ""))
        self.assertEqual(hello["device_id"], "echo-stable")
        self.assertIs(hello, display_identity)
        self.mocks[3].assert_called_once_with(self.selector, hello, connected=True)
        metadata = native_satellite._registry_metadata_from_hello(hello, connected=True)
        self.assertEqual((metadata["room"], metadata["area_name"], metadata["room_name"]), ("", "", ""))

    async def test_rejected_save_keeps_confirmed_identity(self):
        operation = asyncio.create_task(native_satellite.update_identity(self.selector, "Kitchen", "Kitchen"))
        command = await asyncio.wait_for(self.queue.get(), 1)
        await native_satellite._handle_text_message(self.selector, {"type": "identity.result", "payload": {"reply_to": command["id"], "ok": False, "error": "disk full"}})
        with self.assertRaisesRegex(RuntimeError, "disk full"):
            await operation
        self.assertEqual(self.row["hello"]["payload"]["room"], "Office")
        self.mocks[3].assert_not_called()

    async def test_late_confirmation_reconciles_after_a_request_timeout(self):
        await native_satellite._handle_text_message(self.selector, {"type": "identity.result", "payload": {"reply_to": "expired-request", "ok": True, "device_name": "Kitchen", "room": "Kitchen"}})
        self.assertEqual(self.row["hello"]["payload"]["room"], "Kitchen")
        self.mocks[3].assert_called_once()

    async def test_unsupported_offline_and_invalid_updates_are_not_queued(self):
        self.row["hello"]["payload"]["capabilities"] = {}
        with self.assertRaisesRegex(ValueError, "firmware"):
            await native_satellite.update_identity(self.selector, "Kitchen", "Kitchen")
        self.row["hello"]["payload"]["capabilities"] = {"identity_update": True}
        self.row["connected"] = False
        with self.assertRaises(RuntimeError):
            await native_satellite.update_identity(self.selector, "Kitchen", "Kitchen")
        for bad in (None, 42, "", "a\x00name", "é" * 32):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                await native_satellite.update_identity(self.selector, bad, "Office")
        self.assertTrue(self.queue.empty())


class IdentitySettingsActionTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.existing = {"selector": "native:echo-stable", "name": "Office Echo", "source": "tater_native", "metadata": {"room": "Office", "area_name": "Office", "native_selected": True}}
        self.stack.enter_context(mock.patch.object(home, "_runtime_status_with_native", return_value={}))
        self.stack.enter_context(mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None))
        self.stack.enter_context(mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None))
        self.stack.enter_context(mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None))
        self.stack.enter_context(mock.patch.object(home.esphome_runtime, "satellite_lookup", return_value=self.existing))
        self.stack.enter_context(mock.patch.object(home.esphome_runtime, "status", return_value={}))
        self.saved = self.stack.enter_context(mock.patch.object(home.esphome_runtime, "upsert_satellite"))
        self.update = self.stack.enter_context(mock.patch.object(native_satellite, "update_identity", new_callable=mock.AsyncMock, return_value={"ok": True, "device_name": "Kitchen Echo", "room": "Kitchen"}))
        self.stack.enter_context(mock.patch.object(native_satellite, "run_on_runtime_loop", side_effect=lambda coro, **kwargs: asyncio.run(coro)))

    def test_settings_action_confirms_hardware_before_saving_registry(self):
        result = home.handle_runtime_action(action="voice_satellite_save", payload={"selector": "native:echo-stable", "values": {"name": "Kitchen Echo", "area_name": "Kitchen"}})
        self.assertTrue(result["ok"])
        self.update.assert_awaited_once_with("native:echo-stable", "Kitchen Echo", "Kitchen")
        saved = self.saved.call_args.args[0]
        self.assertEqual(saved["selector"], "native:echo-stable")
        self.assertEqual(saved["name"], "Kitchen Echo")
        self.assertEqual(saved["metadata"]["room"], "Kitchen")

    def test_rejected_settings_action_cannot_save_local_identity_override(self):
        self.update.side_effect = RuntimeError("satellite could not save")
        with self.assertRaisesRegex(RuntimeError, "could not save"):
            home.handle_runtime_action(action="voice_satellite_save", payload={"selector": "native:echo-stable", "values": {"name": "Kitchen Echo", "area_name": "Kitchen"}})
        self.saved.assert_not_called()


if __name__ == "__main__":
    unittest.main()
