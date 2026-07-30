from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import announcement_targets
from tater_voice import native_satellite, stereo_pairs


class _FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True


class StereoPairPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = _FakeRedis()
        self.redis_patch = mock.patch.object(stereo_pairs, "redis_client", self.redis)
        self.redis_patch.start()

    def tearDown(self) -> None:
        self.redis_patch.stop()

    def test_pair_round_trip_and_member_exclusivity(self) -> None:
        saved = stereo_pairs.save_pair(
            {
                "name": "Bedroom Stereo",
                "left_selector": "native:left",
                "right_selector": "native:right",
                "left_delay_ms": 4,
                "right_volume_percent": 92,
            }
        )
        self.assertTrue(saved["selector"].startswith("stereo:"))
        loaded = stereo_pairs.get_pair(saved["selector"])
        self.assertEqual(loaded["left_selector"], "native:left")
        self.assertEqual(loaded["left_delay_ms"], 4)
        self.assertEqual(loaded["right_volume_percent"], 92)

        with self.assertRaisesRegex(ValueError, "already assigned"):
            stereo_pairs.save_pair(
                {
                    "name": "Conflicting Pair",
                    "left_selector": "native:left",
                    "right_selector": "native:other",
                }
            )

        removed = stereo_pairs.remove_pair(saved["selector"])
        self.assertTrue(removed["removed"])
        self.assertEqual(stereo_pairs.list_pairs(), [])
        document = json.loads(self.redis.values[stereo_pairs.REDIS_STEREO_PAIRS_KEY])
        self.assertEqual(document["version"], 1)

    def test_ready_pair_appears_as_one_announcement_destination(self) -> None:
        saved = stereo_pairs.save_pair(
            {
                "name": "Office Stereo",
                "left_selector": "native:left",
                "right_selector": "native:right",
            }
        )
        capabilities = {
            "audio_session_version": 2,
            "synchronized_media_sessions": True,
            "stereo_channel_selection": True,
            "media_playhead_telemetry": True,
            "media_drift_correction": True,
        }
        connected = {
            "native:left": {
                "connected": True,
                "device_name": "Office Left",
                "capabilities": capabilities,
            },
            "native:right": {
                "connected": True,
                "device_name": "Office Right",
                "capabilities": capabilities,
            },
        }
        self.redis.values[announcement_targets.REDIS_VOICE_SATELLITE_REGISTRY_KEY] = "[]"
        with (
            mock.patch.object(announcement_targets, "_voice_core_connected_clients", return_value=connected),
            mock.patch.object(announcement_targets, "redis_client", self.redis),
        ):
            options = announcement_targets.get_voice_core_satellite_target_options()

        pair_value = f"voice_core:{saved['selector']}"
        pair_option = next(row for row in options if row["value"] == pair_value)
        self.assertIn("Tater Stereo: Office Stereo", pair_option["label"])
        self.assertIn("ready", pair_option["label"])


class StereoCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        native_satellite._stereo_sessions.clear()
        for task in list(native_satellite._stereo_adjust_tasks.values()):
            task.cancel()
        native_satellite._stereo_adjust_tasks.clear()

    async def asyncTearDown(self) -> None:
        for task in list(native_satellite._stereo_adjust_tasks.values()):
            task.cancel()
        native_satellite._stereo_adjust_tasks.clear()
        native_satellite._stereo_sessions.clear()

    async def test_prepare_waits_for_both_then_commits_shared_start(self) -> None:
        calls = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            calls.append((selector, message_type, dict(payload), timeout_s))
            return {"ok": True}

        async def fake_clock(selector):
            return {
                "selector": selector,
                "offset_us": 1000 if selector.endswith("left") else 2000,
                "round_trip_us": 500,
            }

        pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
            "left_volume_percent": 100,
            "right_volume_percent": 90,
            "left_delay_ms": 0,
            "right_delay_ms": 3,
        }
        with (
            mock.patch.object(
                native_satellite,
                "stereo_pair_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "_stereo_clock_probe", side_effect=fake_clock),
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
        ):
            result = await native_satellite.prepare_stereo_media_session(
                pair,
                session_id="session-1",
                media_url="http://tater/media/song",
                volume_percent=80,
                loop=True,
            )

        self.assertTrue(result["stereo_session_started"])
        prepare_calls = [row for row in calls if row[1] == "media.session.prepare"]
        commit_calls = [row for row in calls if row[1] == "media.session.commit"]
        self.assertEqual(len(prepare_calls), 2)
        self.assertEqual(len(commit_calls), 2)
        self.assertEqual(prepare_calls[0][2]["routing"]["channel"], "left")
        self.assertEqual(prepare_calls[1][2]["routing"]["channel"], "right")
        self.assertEqual(prepare_calls[1][2]["media"]["volume_percent"], 72)
        left_start = commit_calls[0][2]["start_at_us"]
        right_start = commit_calls[1][2]["start_at_us"]
        self.assertEqual(right_start - left_start, 4000)
        self.assertIn("bedroom12", native_satellite._stereo_sessions)

    async def test_phase_error_adjusts_right_member_toward_left(self) -> None:
        now_us = native_satellite._monotonic_us()
        native_satellite._stereo_sessions["pair1"] = {
            "group_id": "pair1",
            "session_id": "session-1",
            "left_selector": "native:left",
            "right_selector": "native:right",
            "clock_offsets_us": {"native:left": 0, "native:right": 0},
            "clock_sync_server_us": now_us,
            "last_adjust_server_us": 0,
            "playheads": {
                "native:left": {
                    "session_id": "session-1",
                    "source_frames": 48048,
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                },
                "native:right": {
                    "session_id": "session-1",
                    "source_frames": 48000,
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                },
            },
        }
        sent = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            sent.append((selector, message_type, dict(payload), timeout_s))
            return {"ok": True}

        with mock.patch.object(native_satellite, "send_request", side_effect=fake_request):
            await native_satellite._adjust_stereo_session("pair1")

        self.assertEqual(sent[0][0], "native:right")
        self.assertEqual(sent[0][1], "media.session.adjust")
        self.assertEqual(sent[0][2]["correction_frames"], 48)

    async def test_scheduled_overlay_uses_pair_calibration_and_stops_scene_media(self) -> None:
        now_us = native_satellite._monotonic_us()
        native_satellite._stereo_sessions["pair1"] = {
            "group_id": "pair1",
            "pair_selector": "stereo:pair1",
            "session_id": "background-1",
            "left_selector": "native:left",
            "right_selector": "native:right",
            "clock_offsets_us": {"native:left": 1000, "native:right": 2000},
        }
        pair = {
            "id": "pair1",
            "selector": "stereo:pair1",
            "left_selector": "native:left",
            "right_selector": "native:right",
            "left_delay_ms": 0,
            "right_delay_ms": 3,
            "left_volume_percent": 100,
            "right_volume_percent": 80,
        }
        sent = []

        async def fake_command(selector, message_type, payload):
            sent.append((selector, message_type, dict(payload)))
            return {"ok": True}

        with (
            mock.patch.object(
                native_satellite,
                "stereo_pair_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "send_command", side_effect=fake_command),
            mock.patch.object(native_satellite, "_stop_stereo_members", mock.AsyncMock()) as stop_mock,
        ):
            result = await native_satellite.start_stereo_overlay(
                pair,
                overlay_id="overlay-1",
                foreground_url="http://tater/media/tts",
                foreground_volume_percent=90,
                start_server_us=now_us + 500_000,
                stop_media_when_finished=True,
            )
            self.assertTrue(result["stop_media_when_finished"])
            self.assertEqual(len(sent), 2)
            self.assertEqual(
                sent[1][2]["start_at_us"] - sent[0][2]["start_at_us"],
                4000,
            )
            self.assertEqual(sent[1][2]["foreground"]["volume_percent"], 72)

            native_satellite._record_stereo_overlay_finished(
                "native:left",
                {"overlay_id": "overlay-1", "ok": True},
            )
            await asyncio.sleep(0)
            stop_mock.assert_not_awaited()
            native_satellite._record_stereo_overlay_finished(
                "native:right",
                {"overlay_id": "overlay-1", "ok": True},
            )
            await asyncio.sleep(0)
            stop_mock.assert_awaited_once_with(
                ["native:left", "native:right"],
                session_id="background-1",
            )


if __name__ == "__main__":
    unittest.main()
