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
        clock = {"now": now_us}
        native_satellite._stereo_sessions["group-1"] = self._session(now_us)
        native_satellite._clients["native:sat1-right"] = {
            "hello": {"payload": {"capabilities": {"media_rate_slew": True}}}
        }
        with mock.patch.object(native_satellite, "_monotonic_us", side_effect=lambda: clock["now"]), mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            await native_satellite._adjust_stereo_session("group-1")
            send_request.assert_not_awaited()
            clock["now"] += int(native_satellite.STEREO_ADJUST_INTERVAL_S * 1_000_000) + 1
            for row in native_satellite._stereo_sessions["group-1"]["playheads"].values():
                row["satellite_time_us"] = clock["now"]
            await native_satellite._adjust_stereo_session("group-1")

        send_request.assert_awaited_once()
        payload = send_request.await_args.args[2]
        self.assertEqual(payload["correction_frames"], 96)
        self.assertEqual(payload["mode"], "slew")
        self.assertEqual(payload["settle_ms"], 4000)

    async def test_handoff_stops_only_the_session_that_currently_owns_the_satellite(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        native_satellite._clients["native:kitchen"] = {
            "connected": True,
            "queue": queue,
            "media_session": {"active": True, "session_id": "airplay-1"},
        }

        result = await native_satellite.handoff_media_sessions(
            ["native:kitchen"],
            fade_ms=0,
        )

        self.assertEqual(result["selectors"], ["native:kitchen"])
        self.assertEqual(result["stopped_selectors"], ["native:kitchen"])
        message = queue.get_nowait()
        self.assertEqual(message["type"], "media.session.stop")
        self.assertEqual(message["payload"]["session_id"], "airplay-1")

    async def test_handoff_expands_a_stereo_destination_to_both_owned_members(self) -> None:
        queues = {
            "native:office-left": asyncio.Queue(),
            "native:office-right": asyncio.Queue(),
        }
        for selector, queue in queues.items():
            native_satellite._clients[selector] = {
                "connected": True,
                "queue": queue,
                "media_session": {"active": True, "session_id": "airplay-pair-1"},
            }

        with mock.patch(
            "tater_voice.stereo_pairs.is_stereo_selector",
            return_value=True,
        ), mock.patch(
            "tater_voice.stereo_pairs.get_pair",
            return_value={
                "left_selector": "native:office-left",
                "right_selector": "native:office-right",
            },
        ):
            result = await native_satellite.handoff_media_sessions(
                ["stereo:office"],
                fade_ms=0,
            )

        self.assertEqual(
            result["stopped_selectors"],
            ["native:office-left", "native:office-right"],
        )
        for queue in queues.values():
            self.assertEqual(queue.get_nowait()["payload"]["session_id"], "airplay-pair-1")

    async def test_stale_cleanup_cannot_stop_a_newer_session(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        native_satellite._clients["native:kitchen"] = {
            "connected": True,
            "queue": queue,
            "media_session": {"active": True, "session_id": "music-2"},
        }

        result = await native_satellite.fade_and_stop_media_session_if_matches(
            "native:kitchen",
            "airplay-1",
            fade_ms=0,
        )

        self.assertFalse(result["stopped"])
        self.assertTrue(queue.empty())

    async def test_replacement_session_fades_from_silence_to_target_volume(self) -> None:
        native_satellite._clients["native:kitchen"] = {
            "connected": True,
            "queue": asyncio.Queue(),
            "media_session": {"active": True, "session_id": "music-2"},
        }
        with mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request, mock.patch.object(
            native_satellite.asyncio,
            "sleep",
            new=mock.AsyncMock(),
        ):
            result = await native_satellite.fade_media_sessions_if_matches(
                [
                    {
                        "session_id": "music-2",
                        "selectors": ["native:kitchen"],
                    }
                ],
                target_volume_percent={"voice_core:native:kitchen": 60},
                fade_ms=180,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            [call.args[2]["volume_percent"] for call in send_request.await_args_list],
            [20, 40, 60],
        )

    async def test_owned_stereo_members_receive_the_same_live_group_volume(self) -> None:
        for selector in ("native:office-left", "native:office-right"):
            native_satellite._clients[selector] = {
                "connected": True,
                "queue": asyncio.Queue(),
                "media_session": {"active": True, "session_id": "airplay-pair-1"},
            }
        with mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            result = await native_satellite.set_media_sessions_volume_if_matches(
                [
                    {
                        "session_id": "airplay-pair-1",
                        "selectors": ["native:office-left", "native:office-right"],
                    }
                ],
                target_volume_percent={
                    "native:office-left": 100,
                    "native:office-right": 100,
                },
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            [call.args[2]["volume_percent"] for call in send_request.await_args_list],
            [100, 100],
        )
        self.assertEqual(
            native_satellite._clients["native:office-left"]["media_session"]["volume_percent"],
            100,
        )
        self.assertEqual(
            native_satellite._clients["native:office-right"]["media_session"]["volume_percent"],
            100,
        )

    async def test_live_volume_update_cannot_change_a_replacement_session(self) -> None:
        native_satellite._clients["native:kitchen"] = {
            "connected": True,
            "queue": asyncio.Queue(),
            "media_session": {"active": True, "session_id": "music-2"},
        }
        with mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request, mock.patch.object(
            native_satellite.asyncio,
            "sleep",
            new=mock.AsyncMock(),
        ):
            result = await native_satellite.set_media_sessions_volume_if_matches(
                [{"session_id": "airplay-1", "selectors": ["native:kitchen"]}],
                volume_percent=100,
                wait_timeout_s=0.1,
            )

        self.assertFalse(result["ok"])
        send_request.assert_not_awaited()

    async def test_one_jittery_phase_sample_does_not_change_follower_speed(self) -> None:
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

        send_request.assert_not_awaited()

    async def test_rendered_audio_clock_wins_over_jittery_source_timeline(self) -> None:
        now_us = 5_000_000
        clock = {"now": now_us}
        session = self._session(now_us)
        session["use_rendered_clock"] = True
        session["playheads"]["native:sat1-left"].update(
            {"source_frames": 5000, "rendered_frames": 1000}
        )
        session["playheads"]["native:sat1-right"].update(
            {"source_frames": 900, "rendered_frames": 970}
        )
        native_satellite._stereo_sessions["group-1"] = session
        with mock.patch.object(native_satellite, "_monotonic_us", side_effect=lambda: clock["now"]), mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            await native_satellite._adjust_stereo_session("group-1")
            clock["now"] += int(native_satellite.STEREO_ADJUST_INTERVAL_S * 1_000_000) + 1
            for row in native_satellite._stereo_sessions["group-1"]["playheads"].values():
                row["satellite_time_us"] = clock["now"]
            await native_satellite._adjust_stereo_session("group-1")

        send_request.assert_not_awaited()

    async def test_mixed_firmware_group_uses_one_common_source_clock(self) -> None:
        now_us = 5_000_000
        clock = {"now": now_us}
        session = self._session(now_us)
        session["use_rendered_clock"] = False
        session["playheads"]["native:sat1-left"].update(
            {"source_frames": 1000, "rendered_frames": 5000}
        )
        session["playheads"]["native:sat1-right"].update(
            {"source_frames": 970, "rendered_frames": 900}
        )
        native_satellite._stereo_sessions["group-1"] = session
        with mock.patch.object(native_satellite, "_monotonic_us", side_effect=lambda: clock["now"]), mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            await native_satellite._adjust_stereo_session("group-1")
            clock["now"] += int(native_satellite.STEREO_ADJUST_INTERVAL_S * 1_000_000) + 1
            for row in native_satellite._stereo_sessions["group-1"]["playheads"].values():
                row["satellite_time_us"] = clock["now"]
            await native_satellite._adjust_stereo_session("group-1")

        send_request.assert_not_awaited()

    async def test_rendered_clock_group_waits_for_every_rendered_playhead(self) -> None:
        now_us = 5_000_000
        session = self._session(now_us)
        session["use_rendered_clock"] = True
        session["playheads"]["native:sat1-left"]["rendered_frames"] = 1000
        native_satellite._stereo_sessions["group-1"] = session
        with mock.patch.object(native_satellite, "_monotonic_us", return_value=now_us), mock.patch.object(
            native_satellite,
            "send_request",
            new=mock.AsyncMock(return_value={"ok": True}),
        ) as send_request:
            await native_satellite._adjust_stereo_session("group-1")

        send_request.assert_not_awaited()

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

    async def test_rebuffer_telemetry_is_logged_without_breaking_playhead_recording(self) -> None:
        native_satellite._stereo_sessions["group-1"] = {
            "session_id": "session-1",
            "group_id": "group-1",
            "selectors": ["native:sat1-left"],
            "playheads": {},
        }
        voice_pipeline = mock.Mock(logger=mock.Mock())
        with mock.patch.object(native_satellite, "_vp", return_value=voice_pipeline):
            native_satellite._record_stereo_playhead(
                "native:sat1-left",
                {
                    "session_id": "session-1",
                    "group_id": "group-1",
                    "source_frames": 48000,
                    "sample_rate_hz": 48000,
                    "satellite_time_us": 5_000_000,
                    "buffered_frames": 96000,
                    "rebuffering": True,
                    "underrun_events": 1,
                },
            )

        self.assertTrue(
            native_satellite._stereo_sessions["group-1"]["playheads"]["native:sat1-left"]["rebuffering"]
        )
        voice_pipeline.logger.warning.assert_called_once()

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
