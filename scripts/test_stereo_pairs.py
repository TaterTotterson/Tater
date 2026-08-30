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

    def test_saved_native_satellite_remains_available_while_offline(self) -> None:
        self.redis.values[announcement_targets.REDIS_VOICE_SATELLITE_REGISTRY_KEY] = json.dumps(
            [
                {
                    "selector": "native:kitchen",
                    "name": "Kitchen",
                    "source": "tater_native",
                },
                {
                    "selector": "host:legacy.local",
                    "name": "Legacy discovery record",
                    "source": "mdns_esphome",
                },
            ]
        )
        with (
            mock.patch.object(announcement_targets, "_voice_core_connected_clients", return_value={}),
            mock.patch.object(announcement_targets, "redis_client", self.redis),
        ):
            options = announcement_targets.get_voice_core_satellite_target_options()

        values = {row["value"]: row["label"] for row in options}
        self.assertIn("voice_core:native:kitchen", values)
        self.assertIn("offline", values["voice_core:native:kitchen"])
        self.assertNotIn("voice_core:host:legacy.local", values)


class AnnouncementIntegrationTargetTests(unittest.TestCase):
    def test_media_player_with_play_media_action_is_a_playback_target(self) -> None:
        with mock.patch.object(
            announcement_targets,
            "_integration_registry_devices",
            return_value=[
                {
                    "integration_id": "roon",
                    "integration_name": "Roon",
                    "id": "zone-kitchen",
                    "name": "Kitchen",
                    "actions": ["play_media"],
                    "capabilities": ["media_player"],
                }
            ],
        ):
            options = announcement_targets.fetch_integration_playback_target_options()

        self.assertEqual(
            options,
            [{"value": "integration:roon:zone-kitchen", "label": "Roon: Kitchen"}],
        )


class StereoCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        native_satellite._stereo_sessions.clear()
        self.original_clients = dict(native_satellite._clients)
        native_satellite._clients.clear()
        for task in list(native_satellite._stereo_adjust_tasks.values()):
            task.cancel()
        native_satellite._stereo_adjust_tasks.clear()

    async def asyncTearDown(self) -> None:
        for task in list(native_satellite._stereo_adjust_tasks.values()):
            task.cancel()
        native_satellite._stereo_adjust_tasks.clear()
        native_satellite._stereo_sessions.clear()
        native_satellite._clients.clear()
        native_satellite._clients.update(self.original_clients)

    async def test_group_member_status_separates_ready_offline_and_old_firmware(self) -> None:
        capabilities = {
            "audio_session_version": 2,
            "synchronized_media_sessions": True,
            "media_playhead_telemetry": True,
            "media_drift_correction": True,
        }
        clients = {
            "native:kitchen": {
                "connected": True,
                "hello": {"payload": {"capabilities": capabilities}},
            },
            "native:office": {"connected": False, "hello": {}},
            "native:garage": {
                "connected": True,
                "hello": {
                    "payload": {
                        "capabilities": {
                            **capabilities,
                            "audio_session_version": 1,
                        }
                    }
                },
            },
        }

        with mock.patch.object(native_satellite, "_clients", clients):
            status = await native_satellite.media_group_member_status(
                ["native:kitchen", "native:office", "native:garage"]
            )

        self.assertEqual(status["ready_selectors"], ["native:kitchen"])
        self.assertEqual(status["disconnected"], ["native:office"])
        self.assertEqual(status["incompatible"][0]["selector"], "native:garage")
        self.assertIn(
            "audio_session_version_2",
            status["incompatible"][0]["missing_capabilities"],
        )

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

    async def test_multi_room_group_prepares_every_member_and_shares_one_start(self) -> None:
        calls = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            calls.append((selector, message_type, dict(payload), timeout_s))
            return {"ok": True}

        offsets = {
            "native:kitchen": 1000,
            "native:office": 2000,
            "native:bedroom": -500,
        }

        async def fake_clock(selector):
            return {"selector": selector, "offset_us": offsets[selector], "round_trip_us": 250}

        members = [
            {"selector": "native:kitchen", "channel": "mono", "volume_percent": 70},
            {"selector": "native:office", "channel": "mono", "volume_percent": 70},
            {"selector": "native:bedroom", "channel": "mono", "volume_percent": 70},
        ]
        clients = {
            selector: {
                "connected": True,
                "hello": {"payload": {"capabilities": {"media_render_clock": True}}},
            }
            for selector in offsets
        }
        with (
            mock.patch.object(native_satellite, "_clients", clients),
            mock.patch.object(
                native_satellite,
                "media_group_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "_stereo_clock_probe", side_effect=fake_clock),
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
        ):
            result = await native_satellite.prepare_group_media_session(
                members,
                group_id="whole-home",
                session_id="music-1",
                media_url="http://tater/media/song.mp3",
                start_lead_ms=1200,
            )

        self.assertTrue(result["group_session_started"])
        self.assertTrue(result["use_rendered_clock"])
        self.assertEqual(result["start_lead_ms"], 1200)
        prepare_calls = [row for row in calls if row[1] == "media.session.prepare"]
        commit_calls = [row for row in calls if row[1] == "media.session.commit"]
        self.assertEqual(len(prepare_calls), 3)
        self.assertEqual(len(commit_calls), 3)
        starts = {row[0]: row[2]["start_at_us"] for row in commit_calls}
        self.assertEqual(starts["native:office"] - starts["native:kitchen"], 1000)
        self.assertEqual(starts["native:bedroom"] - starts["native:kitchen"], -1500)
        self.assertEqual(
            native_satellite._stereo_sessions["whole-home"]["selectors"],
            ["native:kitchen", "native:office", "native:bedroom"],
        )
        self.assertTrue(
            native_satellite._stereo_sessions["whole-home"]["use_rendered_clock"]
        )

    async def test_mixed_render_clock_support_falls_back_for_entire_group(self) -> None:
        members = [
            {"selector": "native:left", "channel": "left"},
            {"selector": "native:right", "channel": "right"},
        ]
        clients = {
            "native:left": {
                "connected": True,
                "hello": {"payload": {"capabilities": {"media_render_clock": True}}},
            },
            "native:right": {
                "connected": True,
                "hello": {"payload": {"capabilities": {"media_render_clock": False}}},
            },
        }

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            return {"ok": True}

        async def fake_clock(selector):
            return {"selector": selector, "offset_us": 0, "round_trip_us": 100}

        with (
            mock.patch.object(native_satellite, "_clients", clients),
            mock.patch.object(
                native_satellite,
                "media_group_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "_stereo_clock_probe", side_effect=fake_clock),
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
            mock.patch.object(
                native_satellite,
                "_vp",
                return_value=mock.Mock(logger=mock.Mock()),
            ) as voice_pipeline,
        ):
            result = await native_satellite.prepare_group_media_session(
                members,
                group_id="mixed-clocks",
                session_id="music-mixed",
                media_url="http://tater/media/song.wav",
            )

        self.assertFalse(result["use_rendered_clock"])
        self.assertFalse(
            native_satellite._stereo_sessions["mixed-clocks"]["use_rendered_clock"]
        )
        voice_pipeline.return_value.logger.warning.assert_called_once()

    async def test_render_latency_moves_engine_starts_onto_one_audible_anchor(self) -> None:
        offsets = {"native:esp": 1000, "native:linux": 2000}
        clients = {
            "native:esp": {
                "connected": True,
                "hello": {
                    "payload": {
                        "capabilities": {
                            "media_render_clock": True,
                            "media_sample_rate_hz": 48000,
                            "media_output_latency_frames": 960,
                        }
                    }
                },
            },
            "native:linux": {
                "connected": True,
                "hello": {
                    "payload": {
                        "capabilities": {
                            "media_render_clock": True,
                            "media_sample_rate_hz": 48000,
                            "media_output_latency_frames": 6000,
                        }
                    }
                },
            },
        }
        calls = []

        async def fake_clock(selector):
            return {"selector": selector, "offset_us": offsets[selector], "round_trip_us": 100}

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            calls.append((selector, message_type, dict(payload)))
            return {"ok": True, "sample_rate_hz": 48000}

        with (
            mock.patch.object(native_satellite, "_clients", clients),
            mock.patch.object(
                native_satellite,
                "media_group_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "_stereo_clock_probe", side_effect=fake_clock),
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
            mock.patch.object(native_satellite, "_learned_media_render_latency_frames", return_value=0),
        ):
            result = await native_satellite.prepare_group_media_session(
                [
                    {"selector": "native:esp", "channel": "mono"},
                    {"selector": "native:linux", "channel": "mono"},
                ],
                group_id="mixed-audible",
                session_id="mixed-session",
                media_url="http://tater/media/song.wav",
            )

        commit_calls = [row for row in calls if row[1] == "media.session.commit"]
        starts = {row[0]: row[2]["start_at_us"] for row in commit_calls}
        self.assertEqual(starts["native:linux"] - starts["native:esp"], -104000)
        self.assertEqual(result["render_latency_frames"]["native:esp"], 960)
        self.assertEqual(result["render_latency_frames"]["native:linux"], 6000)
        self.assertEqual(result["audible_start_unix_ms"], result["start_unix_ms"])
        self.assertEqual(
            commit_calls[0][2]["audible_start_at_us"] - offsets[commit_calls[0][0]],
            commit_calls[1][2]["audible_start_at_us"] - offsets[commit_calls[1][0]],
        )

    async def test_tts_waits_for_both_members_and_marks_visual_lifecycle(self) -> None:
        calls = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            calls.append((selector, message_type, dict(payload), timeout_s))
            return {"ok": True}

        async def fake_clock(selector):
            return {
                "selector": selector,
                "offset_us": 0,
                "round_trip_us": 100,
            }

        pair = {
            "id": "office12",
            "selector": "stereo:office12",
            "left_selector": "native:left",
            "right_selector": "native:right",
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
            playback = asyncio.create_task(
                native_satellite.prepare_stereo_media_session(
                    pair,
                    session_id="tts-1",
                    media_url="http://tater/media/tts.wav",
                    content_type="tts",
                    channel_mode="mono",
                    wait_for_completion=True,
                    completion_timeout_s=2.0,
                )
            )
            for _ in range(20):
                if "office12" in native_satellite._stereo_sessions:
                    break
                await asyncio.sleep(0)

            prepare_calls = [row for row in calls if row[1] == "media.session.prepare"]
            self.assertEqual(len(prepare_calls), 2)
            self.assertEqual(prepare_calls[0][2]["media"]["content_type"], "tts")
            self.assertEqual(prepare_calls[0][2]["visual_mode"], "speaking")
            self.assertEqual(prepare_calls[0][2]["state_after"], "idle")

            native_satellite._record_stereo_finished(
                "native:left",
                {"session_id": "tts-1", "ok": True},
            )
            await asyncio.sleep(0)
            self.assertFalse(playback.done())

            native_satellite._record_stereo_finished(
                "native:right",
                {"session_id": "tts-1", "ok": True},
            )
            result = await playback

        self.assertTrue(result["playback_completed"])
        self.assertTrue(result["playback_ok"])
        self.assertEqual(result["finished_members"], ["native:left", "native:right"])
        self.assertNotIn("office12", native_satellite._stereo_sessions)

    async def test_large_startup_skew_jumps_late_member_to_shared_timeline(self) -> None:
        native_satellite._stereo_sessions["office-pair"] = {
            "group_id": "office-pair",
            "session_id": "reply-1",
            "selectors": ["native:left", "native:right"],
            "clock_offsets_us": {"native:left": 0, "native:right": 0},
            "member_delays_ms": {"native:left": 0, "native:right": 0},
            "render_latency_frames": {"native:left": 6144, "native:right": 6144},
            "render_sample_rates": {"native:left": 48000, "native:right": 48000},
            "actual_starts_us": {},
            "startup_realign_supported": True,
            "startup_realign_scheduled": False,
        }
        sent = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            sent.append((selector, message_type, dict(payload), timeout_s))
            return {"ok": True}

        voice_pipeline = mock.Mock(logger=mock.Mock())
        with (
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
            mock.patch.object(native_satellite, "_vp", return_value=voice_pipeline),
        ):
            native_satellite._record_stereo_started(
                "native:left",
                {
                    "group_id": "office-pair",
                    "session_id": "reply-1",
                    "actual_start_us": 1_000_000,
                },
            )
            native_satellite._record_stereo_started(
                "native:right",
                {
                    "group_id": "office-pair",
                    "session_id": "reply-1",
                    "actual_start_us": 1_826_000,
                },
            )
            for _index in range(20):
                session = native_satellite._stereo_sessions["office-pair"]
                if "startup_realign_applied" in session:
                    break
                await asyncio.sleep(0)

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][0], "native:right")
        self.assertEqual(sent[0][1], "media.session.adjust")
        self.assertEqual(sent[0][2]["mode"], "jump")
        self.assertEqual(sent[0][2]["correction_frames"], 39648)
        session = native_satellite._stereo_sessions["office-pair"]
        self.assertTrue(session["startup_realign_applied"])
        self.assertEqual(session["startup_spread_us"], 826000)
        voice_pipeline.logger.warning.assert_called_once()

    async def test_small_startup_jitter_does_not_seek_either_member(self) -> None:
        native_satellite._stereo_sessions["office-pair"] = {
            "group_id": "office-pair",
            "session_id": "reply-2",
            "selectors": ["native:left", "native:right"],
            "clock_offsets_us": {"native:left": 0, "native:right": 0},
            "member_delays_ms": {"native:left": 0, "native:right": 0},
            "render_latency_frames": {"native:left": 6144, "native:right": 6144},
            "render_sample_rates": {"native:left": 48000, "native:right": 48000},
            "actual_starts_us": {
                "native:left": 1_000_000,
                "native:right": 1_012_000,
            },
            "startup_realign_supported": True,
        }
        with mock.patch.object(
            native_satellite,
            "send_request",
            mock.AsyncMock(),
        ) as request_mock:
            await native_satellite._realign_stereo_startup("office-pair")

        request_mock.assert_not_awaited()
        session = native_satellite._stereo_sessions["office-pair"]
        self.assertFalse(session["startup_realign_applied"])
        self.assertEqual(session["startup_spread_us"], 12000)

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
            self.assertEqual(sent, [])
            later_us = now_us + int(native_satellite.STEREO_ADJUST_INTERVAL_S * 1_000_000) + 1
            native_satellite._stereo_sessions["pair1"]["last_phase_sample_server_us"] = 0
            for row in native_satellite._stereo_sessions["pair1"]["playheads"].values():
                row["satellite_time_us"] = later_us
            await native_satellite._adjust_stereo_session("pair1")

        self.assertEqual(sent[0][0], "native:right")
        self.assertEqual(sent[0][1], "media.session.adjust")
        self.assertEqual(sent[0][2]["correction_frames"], 48)

    async def test_rendered_playheads_follow_server_timeline_not_first_member(self) -> None:
        now_us = native_satellite._monotonic_us()
        selectors = ["native:linux", "native:esp"]
        native_satellite._stereo_sessions["mixed"] = {
            "group_id": "mixed",
            "session_id": "session-mixed",
            "selectors": selectors,
            "clock_offsets_us": {selector: 0 for selector in selectors},
            "clock_sync_server_us": now_us,
            "last_adjust_server_us": 0,
            "last_phase_sample_server_us": 0,
            "use_rendered_clock": True,
            "audible_start_server_us": now_us - 1_000_000,
            "member_delays_ms": {selector: 0 for selector in selectors},
            "start_position_frames": {selector: 0 for selector in selectors},
            "phase_error_ema_frames": {},
            "phase_error_directions": {},
            "phase_error_stable_samples": {},
            "phase_sample_times_us": {},
            "playheads": {
                "native:linux": {
                    "session_id": "session-mixed",
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                    "rendered_frames": 48240,
                },
                "native:esp": {
                    "session_id": "session-mixed",
                    "sample_rate_hz": 48000,
                    "satellite_time_us": now_us,
                    "rendered_frames": 47520,
                },
            },
        }
        clients = {
            selector: {
                "hello": {"payload": {"capabilities": {"media_rate_slew": True}}}
            }
            for selector in selectors
        }
        sent = []

        async def fake_request(selector, message_type, payload, *, timeout_s=3.0):
            sent.append((selector, message_type, dict(payload)))
            return {"ok": True}

        with (
            mock.patch.object(native_satellite, "_clients", clients),
            mock.patch.object(native_satellite, "send_request", side_effect=fake_request),
        ):
            await native_satellite._adjust_stereo_session("mixed")
            self.assertEqual(sent, [])
            later_us = now_us + int(native_satellite.STEREO_ADJUST_INTERVAL_S * 1_000_000) + 1
            session = native_satellite._stereo_sessions["mixed"]
            session["last_phase_sample_server_us"] = 0
            session["playheads"]["native:linux"].update(
                {"satellite_time_us": later_us, "rendered_frames": 144240}
            )
            session["playheads"]["native:esp"].update(
                {"satellite_time_us": later_us, "rendered_frames": 143520}
            )
            await native_satellite._adjust_stereo_session("mixed")

        corrections = {row[0]: row[2]["correction_frames"] for row in sent}
        self.assertEqual(corrections, {"native:linux": -240, "native:esp": 240})
        self.assertTrue(
            all(row[2]["reference_selector"] == "tater:audible-timeline" for row in sent)
        )

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

    async def test_stereo_overlay_waits_for_both_members_before_completing_reply(self) -> None:
        native_satellite._stereo_sessions["pair1"] = {
            "group_id": "pair1",
            "pair_selector": "stereo:pair1",
            "session_id": "music-1",
            "left_selector": "native:left",
            "right_selector": "native:right",
            "clock_offsets_us": {"native:left": 0, "native:right": 0},
        }
        pair = {
            "id": "pair1",
            "selector": "stereo:pair1",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }

        async def fake_command(_selector, _message_type, _payload):
            return {"ok": True}

        with (
            mock.patch.object(
                native_satellite,
                "stereo_pair_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(native_satellite, "send_command", side_effect=fake_command),
        ):
            playback = asyncio.create_task(
                native_satellite.start_stereo_overlay(
                    pair,
                    overlay_id="reply-1",
                    foreground_url="http://tater/media/reply.wav",
                    start_server_us=native_satellite._monotonic_us() + 500_000,
                    wait_for_completion=True,
                    completion_timeout_s=2.0,
                )
            )
            await asyncio.sleep(0)
            self.assertFalse(playback.done())

            native_satellite._record_stereo_overlay_finished(
                "native:left",
                {"overlay_id": "reply-1", "ok": True},
            )
            await asyncio.sleep(0)
            self.assertFalse(playback.done())

            native_satellite._record_stereo_overlay_finished(
                "native:right",
                {"overlay_id": "reply-1", "ok": True},
            )
            result = await playback

        self.assertTrue(result["playback_completed"])
        self.assertTrue(result["playback_ok"])
        self.assertEqual(result["finished_members"], ["native:left", "native:right"])
        self.assertIn("pair1", native_satellite._stereo_sessions)

    async def test_grouped_stereo_pair_receives_overlay_without_replacing_music(self) -> None:
        native_satellite._stereo_sessions["multi-1"] = {
            "group_id": "multi-1",
            "pair_selector": "group:multi-1",
            "session_id": "music-1",
            "selectors": ["native:left", "native:right", "native:kitchen"],
            "clock_offsets_us": {
                "native:left": 0,
                "native:right": 0,
                "native:kitchen": 0,
            },
            "created_server_us": 123,
        }
        pair = {
            "id": "pair1",
            "selector": "stereo:pair1",
            "left_selector": "native:left",
            "right_selector": "native:right",
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
        ):
            self.assertTrue(native_satellite.stereo_pair_media_active(pair))
            playback = asyncio.create_task(
                native_satellite.start_stereo_overlay(
                    pair,
                    overlay_id="reply-1",
                    foreground_url="http://tater/media/reply.wav",
                    start_server_us=native_satellite._monotonic_us() + 500_000,
                    wait_for_completion=True,
                    completion_timeout_s=2.0,
                )
            )
            for _ in range(10):
                if len(sent) == 2:
                    break
                await asyncio.sleep(0)

            self.assertEqual(
                [row[0] for row in sent],
                ["native:left", "native:right"],
            )
            native_satellite._record_stereo_overlay_finished(
                "native:left",
                {"overlay_id": "reply-1", "ok": True},
            )
            native_satellite._record_stereo_overlay_finished(
                "native:right",
                {"overlay_id": "reply-1", "ok": True},
            )
            result = await playback

        self.assertTrue(result["playback_completed"])
        self.assertTrue(result["playback_ok"])
        self.assertEqual(result["finished_members"], ["native:left", "native:right"])
        self.assertIn("multi-1", native_satellite._stereo_sessions)
        self.assertEqual(
            native_satellite._stereo_sessions["multi-1"]["session_id"],
            "music-1",
        )

    async def test_stereo_overlay_recovers_live_pair_when_coordinator_state_was_lost(self) -> None:
        capabilities = {
            "audio_session_version": 2,
            "synchronized_media_sessions": True,
            "stereo_channel_selection": True,
            "media_playhead_telemetry": True,
            "media_drift_correction": True,
            "media_render_clock": True,
        }
        for selector, channel in (("native:left", "left"), ("native:right", "right")):
            native_satellite._clients[selector] = {
                "connected": True,
                "hello": {"payload": {"capabilities": capabilities}},
                "media_session": {
                    "active": True,
                    "session_id": "music-live-1",
                    "group_id": "pair-live-1",
                    "channel": channel,
                },
            }
        pair = {
            "id": "pair-live-1",
            "selector": "stereo:pair-live-1",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        sent = []

        async def fake_command(selector, message_type, payload):
            sent.append((selector, message_type, dict(payload)))
            return {"ok": True}

        async def fake_clock_probe(selector):
            return {
                "selector": selector,
                "offset_us": 100 if selector.endswith("left") else 200,
                "round_trip_us": 500,
            }

        with (
            mock.patch.object(
                native_satellite,
                "stereo_pair_compatibility",
                mock.AsyncMock(return_value={"ok": True}),
            ),
            mock.patch.object(
                native_satellite,
                "_stereo_clock_probe",
                side_effect=fake_clock_probe,
            ),
            mock.patch.object(native_satellite, "send_command", side_effect=fake_command),
        ):
            self.assertTrue(native_satellite.stereo_pair_media_active(pair))
            recovered = native_satellite._stereo_sessions["pair-live-1"]
            self.assertTrue(recovered["recovered_from_live_state"])
            self.assertEqual(recovered["session_id"], "music-live-1")

            result = await native_satellite.start_stereo_overlay(
                pair,
                overlay_id="reply-live-1",
                foreground_url="http://tater/media/reply.wav",
            )

        self.assertTrue(result["stereo_overlay_started"])
        self.assertEqual(
            [row[0] for row in sent],
            ["native:left", "native:right"],
        )
        self.assertEqual(
            native_satellite._stereo_sessions["pair-live-1"]["clock_offsets_us"],
            {"native:left": 100, "native:right": 200},
        )


if __name__ == "__main__":
    unittest.main()
