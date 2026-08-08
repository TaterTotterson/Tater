from __future__ import annotations

import sys
import asyncio
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import native_satellite


class NativeMediaSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        for task in native_satellite._media_disconnect_tasks.values():
            task.cancel()
        native_satellite._clients.clear()
        native_satellite._stereo_sessions.clear()
        native_satellite._stereo_adjust_tasks.clear()
        native_satellite._media_disconnect_tasks.clear()

    def tearDown(self) -> None:
        for task in native_satellite._media_disconnect_tasks.values():
            task.cancel()
        native_satellite._clients.clear()
        native_satellite._stereo_sessions.clear()
        native_satellite._stereo_adjust_tasks.clear()
        native_satellite._media_disconnect_tasks.clear()

    @staticmethod
    def _session(now_us: int, *, reference_rebuffering: bool = False, follower_rebuffering: bool = False):
        session_id = "session-1"
        return {
            "session_id": session_id,
            "group_id": "group-1",
            "selectors": ["native:sat1-left", "native:sat1-right"],
            "reference_selector": "native:sat1-left",
            "clock_offsets_us": {},
            "member_delays_ms": {},
            "last_adjust_server_us": 0,
            "playheads": {
                "native:sat1-left": {
                    "session_id": session_id,
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                    "source_frames": 1000,
                    "rebuffering": reference_rebuffering,
                },
                "native:sat1-right": {
                    "session_id": session_id,
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                    "source_frames": 900,
                    "rebuffering": follower_rebuffering,
                },
            },
        }

    async def test_new_firmware_receives_gradual_slew_request(self) -> None:
        now_us = 5_000_000
        native_satellite._stereo_sessions["group-1"] = self._session(now_us)
        native_satellite._clients["native:sat1-right"] = {
            "hello": {"payload": {"capabilities": {"media_rate_slew": True}}}
        }
        with mock.patch.object(native_satellite, "_monotonic_us", return_value=now_us), mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            await native_satellite._adjust_stereo_session("group-1")

        send_request.assert_awaited_once()
        payload = send_request.await_args.args[2]
        self.assertEqual(payload["correction_frames"], 48)
        self.assertEqual(payload["mode"], "slew")
        self.assertEqual(payload["settle_ms"], 1000)

    async def test_drift_adjustment_waits_for_underrun_rejoin(self) -> None:
        now_us = 5_000_000
        for reference_rebuffering, follower_rebuffering in ((True, False), (False, True)):
            native_satellite._stereo_sessions["group-1"] = self._session(
                now_us,
                reference_rebuffering=reference_rebuffering,
                follower_rebuffering=follower_rebuffering,
            )
            with mock.patch.object(native_satellite, "_monotonic_us", return_value=now_us), mock.patch.object(
                native_satellite,
                "send_request",
                new=mock.AsyncMock(return_value={"ok": True}),
            ) as send_request:
                await native_satellite._adjust_stereo_session("group-1")
            send_request.assert_not_awaited()

    async def test_member_disconnect_aborts_group_and_stops_remaining_satellite(self) -> None:
        completion = asyncio.get_running_loop().create_future()
        adjustment = asyncio.create_task(asyncio.sleep(30))
        native_satellite._stereo_sessions["group-1"] = {
            **self._session(5_000_000),
            "completion_future": completion,
        }
        native_satellite._stereo_adjust_tasks["group-1"] = adjustment
        logger = mock.Mock()
        with (
            mock.patch.object(
                native_satellite,
                "_stop_stereo_members",
                new=mock.AsyncMock(),
            ) as stop_members,
            mock.patch.object(
                native_satellite,
                "_vp",
                return_value=SimpleNamespace(logger=logger),
            ),
        ):
            aborted = await native_satellite._abort_media_groups_for_disconnect(
                "native:sat1-left",
                reason="network loss",
            )

        self.assertEqual(aborted, 1)
        self.assertNotIn("group-1", native_satellite._stereo_sessions)
        self.assertNotIn("group-1", native_satellite._stereo_adjust_tasks)
        await asyncio.sleep(0)
        self.assertTrue(adjustment.cancelled())
        self.assertFalse(completion.result()["ok"])
        self.assertEqual(completion.result()["disconnected_selector"], "native:sat1-left")
        stop_members.assert_awaited_once_with(
            ["native:sat1-right"],
            session_id="session-1",
        )
        logger.warning.assert_called_once()

    async def test_transient_disconnect_rejoin_keeps_group_running(self) -> None:
        native_satellite._stereo_sessions["group-1"] = self._session(5_000_000)
        logger = mock.Mock()
        with (
            mock.patch.object(native_satellite, "NATIVE_MEDIA_DISCONNECT_GRACE_S", 0.01),
            mock.patch.object(
                native_satellite,
                "_stop_stereo_members",
                new=mock.AsyncMock(),
            ) as stop_members,
            mock.patch.object(
                native_satellite,
                "_vp",
                return_value=SimpleNamespace(logger=logger),
            ),
        ):
            self.assertTrue(
                native_satellite._schedule_media_disconnect_abort(
                    "native:sat1-left",
                    reason="network loss",
                )
            )
            self.assertTrue(
                native_satellite._cancel_media_disconnect_abort("native:sat1-left")
            )
            await asyncio.sleep(0.03)

        self.assertIn("group-1", native_satellite._stereo_sessions)
        self.assertNotIn("native:sat1-left", native_satellite._media_disconnect_tasks)
        stop_members.assert_not_awaited()
        logger.info.assert_called_once()

    async def test_disconnect_grace_expiry_aborts_group(self) -> None:
        completion = asyncio.get_running_loop().create_future()
        native_satellite._stereo_sessions["group-1"] = {
            **self._session(5_000_000),
            "completion_future": completion,
        }
        logger = mock.Mock()
        with (
            mock.patch.object(native_satellite, "NATIVE_MEDIA_DISCONNECT_GRACE_S", 0.01),
            mock.patch.object(
                native_satellite,
                "_stop_stereo_members",
                new=mock.AsyncMock(),
            ) as stop_members,
            mock.patch.object(
                native_satellite,
                "_vp",
                return_value=SimpleNamespace(logger=logger),
            ),
        ):
            self.assertTrue(
                native_satellite._schedule_media_disconnect_abort(
                    "native:sat1-left",
                    reason="network loss",
                )
            )
            await asyncio.sleep(0.03)

        self.assertNotIn("group-1", native_satellite._stereo_sessions)
        self.assertNotIn("native:sat1-left", native_satellite._media_disconnect_tasks)
        self.assertFalse(completion.result()["ok"])
        stop_members.assert_awaited_once_with(
            ["native:sat1-right"],
            session_id="session-1",
        )


if __name__ == "__main__":
    unittest.main()
