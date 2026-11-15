from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from tater_voice import native_satellite


class NativeSatelliteHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.selector = "native:test-right"
        self.websocket = object()
        self.row = {
            "connected": True,
            "websocket": self.websocket,
            "last_seen_ts": 100.0,
            "hello": {"payload": {"device_id": "test-right", "room": "Bedroom"}},
        }
        self.clients = mock.patch.dict(native_satellite._clients, {self.selector: self.row}, clear=True)
        self.clients.start()
        self.old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        self.patches = [
            mock.patch.object(native_satellite, "_now", return_value=120.0),
            mock.patch.object(native_satellite, "_stale_after_s", return_value=15.0),
            mock.patch.object(native_satellite, "_client_snapshot", side_effect=lambda selector, row, **kwargs: {"board": "", "capabilities": {}, **row}),
            mock.patch.object(native_satellite, "_live_settings_payload", return_value={}),
            mock.patch.object(native_satellite, "_upsert_registry_from_hello"),
            mock.patch.object(native_satellite, "_notify_state_change"),
            mock.patch.object(native_satellite, "_schedule_media_disconnect_abort"),
            mock.patch.object(native_satellite, "_cancel_media_disconnect_abort"),
            mock.patch.object(native_satellite, "_vp", return_value=SimpleNamespace(logger=mock.Mock())),
        ]
        for patch in self.patches:
            patch.start()

    def tearDown(self) -> None:
        for patch in reversed(self.patches):
            patch.stop()
        native_satellite._clients_lock = self.old_lock
        self.clients.stop()

    async def test_live_messages_recover_a_stereo_member_after_heartbeat_timeout(self) -> None:
        with mock.patch("tater_voice.sat_chat.disconnected", new=mock.AsyncMock()):
            await native_satellite.status()
        self.assertFalse(self.row["connected"])
        self.assertEqual(self.row["last_error"], "stale heartbeat")
        native_satellite._notify_state_change.reset_mock()

        response = await native_satellite._handle_text_message(
            self.selector, {"type": "ping", "payload": {}}, websocket=self.websocket
        )
        self.assertEqual(response["type"], "pong")
        self.assertTrue(self.row["connected"])
        self.assertEqual(self.row["last_error"], "")
        self.assertEqual(self.row["last_seen_ts"], 120.0)
        native_satellite._upsert_registry_from_hello.assert_called_with(
            self.selector, self.row["hello"]["payload"], connected=True
        )
        native_satellite._notify_state_change.assert_called_once_with("connected", self.selector)
        native_satellite._cancel_media_disconnect_abort.assert_called_once_with(self.selector)
        await native_satellite._record_client_activity(self.selector, self.websocket)
        native_satellite._notify_state_change.assert_called_once()

    async def test_old_socket_cannot_revive_or_update_its_replacement(self) -> None:
        self.row.update(connected=False, last_error="stale heartbeat")
        self.assertFalse(await native_satellite._record_client_activity(self.selector, object()))
        self.assertFalse(self.row["connected"])
        self.assertEqual(self.row["last_seen_ts"], 100.0)
        await native_satellite._handle_text_message(
            self.selector, {"type": "status", "payload": {"state": "idle"}}, websocket=object()
        )
        self.assertEqual(self.row["last_seen_ts"], 100.0)
        native_satellite._notify_state_change.assert_not_called()

    async def test_genuine_disconnect_and_setup_state_are_not_revived(self) -> None:
        for reason in ["websocket closed", "setup state: setup", "forgotten"]:
            self.row.update(connected=False, last_error=reason)
            await native_satellite._record_client_activity(self.selector, self.websocket)
            self.assertFalse(self.row["connected"])
            self.assertEqual(self.row["last_error"], reason)
        self.row.update(connected=False, last_error="stale heartbeat")
        await native_satellite._record_client_activity(self.selector, self.websocket, recover=False)
        self.assertFalse(self.row["connected"])
        native_satellite._notify_state_change.assert_not_called()


if __name__ == "__main__":
    unittest.main()
