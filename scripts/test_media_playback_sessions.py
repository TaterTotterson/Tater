from __future__ import annotations

import unittest
from unittest import mock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import media_playback


class _Response:
    status_code = 200

    @staticmethod
    def json():
        return {
            "ok": True,
            "media_session_started": True,
            "message": {
                "payload": {
                    "session_id": "music-session-1",
                }
            },
        }


class _GroupResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            "ok": True,
            "media_session_started": True,
            "group_id": "multi-1",
            "session_id": "music-group-1",
            "start_lead_ms": 750,
            "start_server_us": 123456789,
            "start_unix_ms": 2000000000000,
            "members": [
                {"selector": "native:kitchen"},
                {"selector": "native:office"},
            ],
        }


class _PartialGroupResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            "ok": True,
            "media_session_started": True,
            "group_id": "multi-1",
            "session_id": "music-group-1",
            "start_lead_ms": 750,
            "played_selectors": ["native:kitchen"],
            "members": [{"selector": "native:kitchen"}],
            "skipped_destinations": [
                {"selector": "native:office", "reason": "native:office (offline)"}
            ],
            "warnings": ["Skipped unavailable playback destinations: native:office (offline)"],
        }


class MediaPlaybackSessionTests(unittest.TestCase):
    def test_voice_core_music_request_declares_persistent_media_role(self) -> None:
        with (
            mock.patch.object(media_playback, "_voice_core_base_url", return_value="http://127.0.0.1:8501"),
            mock.patch.object(media_playback, "_voice_core_auth_headers", return_value={}),
            mock.patch.object(media_playback.requests, "post", return_value=_Response()) as post,
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["native:kitchen"],
                source_url="https://example.test/song.mp3",
                media_type="audio/mpeg",
                media_content_type="music",
                title="Billie Jean",
                artist="Michael Jackson",
                album="Thriller",
                volume_percent=64,
                start_position_seconds=37.25,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["media_session_sent_count"], 1)
        self.assertEqual(
            result["voice_core_sessions"],
            [
                {
                    "target": "native:kitchen",
                    "session_id": "music-session-1",
                    "selectors": ["native:kitchen"],
                }
            ],
        )
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["playback_role"], "media")
        self.assertEqual(payload["media_content_type"], "music")
        self.assertEqual(payload["volume_percent"], 64)
        self.assertEqual(payload["start_position_ms"], 37250)
        self.assertEqual(payload["title"], "Billie Jean")
        self.assertEqual(payload["artist"], "Michael Jackson")
        self.assertEqual(payload["album"], "Thriller")

    def test_multiple_satellites_use_one_synchronized_group_request(self) -> None:
        with (
            mock.patch.object(media_playback, "_voice_core_base_url", return_value="http://127.0.0.1:8501"),
            mock.patch.object(media_playback, "_voice_core_auth_headers", return_value={}),
            mock.patch.object(media_playback.requests, "post", return_value=_GroupResponse()) as post,
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["native:kitchen", "native:office"],
                source_url="https://example.test/song.mp3",
                media_type="audio/mpeg",
                media_content_type="music",
                volume_percent=60,
                target_volume_percent={
                    "voice_core:native:kitchen": 42,
                    "voice_core:native:office": 73,
                },
                target_sync_offset_ms={
                    "voice_core:native:kitchen": -100,
                    "voice_core:native:office": 150,
                },
                start_lead_ms=750,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["synchronized_group"])
        self.assertEqual(result["sent_count"], 2)
        self.assertEqual(result["start_unix_ms"], 2000000000000)
        self.assertTrue(post.call_args.args[0].endswith("/api/tater/satellite/v1/play-group"))
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["selectors"], ["native:kitchen", "native:office"])
        self.assertEqual(payload["start_lead_ms"], 750)
        self.assertEqual(
            payload["player_settings"],
            {
                "native:kitchen": {"volume_percent": 42, "sync_offset_ms": -100},
                "native:office": {"volume_percent": 73, "sync_offset_ms": 150},
            },
        )

    def test_overlapping_destination_starts_silent_then_fades_without_muting_disjoint_target(self) -> None:
        prior_sessions = [
            {
                "selector": "native:kitchen",
                "session_id": "airplay-session-1",
            }
        ]
        replacement_sessions = [
            {
                "target": "multi-1",
                "session_id": "music-group-1",
                "selectors": ["native:kitchen", "native:office"],
            }
        ]
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_handoff_media_sync",
                return_value={
                    "ok": True,
                    "selectors": ["native:kitchen"],
                    "sessions": prior_sessions,
                },
            ),
            mock.patch.object(
                media_playback,
                "_voice_core_fade_in_media_sync",
                return_value={"ok": True},
            ) as fade_in,
            mock.patch.object(media_playback, "_voice_core_base_url", return_value="http://127.0.0.1:8501"),
            mock.patch.object(media_playback, "_voice_core_auth_headers", return_value={}),
            mock.patch.object(media_playback.requests, "post", return_value=_GroupResponse()) as post,
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["native:kitchen", "native:office"],
                source_url="https://example.test/song.mp3",
                volume_percent=60,
                target_volume_percent={
                    "voice_core:native:kitchen": 42,
                    "voice_core:native:office": 73,
                },
                start_lead_ms=750,
            )

        settings = post.call_args.kwargs["json"]["player_settings"]
        self.assertEqual(settings["native:kitchen"]["volume_percent"], 0)
        self.assertEqual(settings["native:office"]["volume_percent"], 73)
        fade_in.assert_called_once_with(
            replacement_sessions,
            target_volume_percent={
                "voice_core:native:kitchen": 42,
                "voice_core:native:office": 73,
            },
            volume_percent=60,
        )
        self.assertEqual(
            result["playback_handoff"]["replaced_selectors"],
            ["native:kitchen"],
        )
        self.assertTrue(result["playback_handoff"]["fade_in_ok"])

    def test_synchronized_group_reports_only_online_destinations_as_sent(self) -> None:
        with (
            mock.patch.object(media_playback, "_voice_core_base_url", return_value="http://127.0.0.1:8501"),
            mock.patch.object(media_playback, "_voice_core_auth_headers", return_value={}),
            mock.patch.object(media_playback.requests, "post", return_value=_PartialGroupResponse()),
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["native:kitchen", "native:office"],
                source_url="https://example.test/song.mp3",
                media_type="audio/mpeg",
                media_content_type="music",
                volume_percent=60,
                start_lead_ms=750,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["media_session_sent_count"], 1)
        self.assertEqual(result["voice_core_sessions"][0]["selectors"], ["native:kitchen"])
        self.assertEqual(result["skipped_destinations"][0]["selector"], "native:office")
        self.assertIn("offline", result["warnings"][0])

    def test_mixed_sonos_and_satellite_schedules_satellite_then_uses_proxy(self) -> None:
        order = []

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {"ok": True, "sent_count": 1, "media_session_sent_count": 1}

        def sonos(**kwargs):
            order.append(("sonos", kwargs))
            return {"ok": True, "sent_count": 1}

        with (
            mock.patch.object(media_playback, "_voice_core_play_media_sync", side_effect=voice),
            mock.patch.object(
                media_playback,
                "_runtime_media_proxy_source_url",
                return_value="http://tater.local:8501/api/media/runtime/asset/song.mp3",
            ),
            mock.patch.object(media_playback, "_sonos_playback_sync", side_effect=sonos),
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "sonos:RINCON_KITCHEN"],
                "https://provider.test/stream?id=1",
                filename="song.mp3",
                mixed_sync_adjustment_ms=125,
                target_volume_percent={
                    "voice_core:native:kitchen": 44,
                    "sonos:RINCON_KITCHEN": 62,
                },
                target_sync_offset_ms={
                    "voice_core:native:kitchen": 100,
                    "sonos:RINCON_KITCHEN": -25,
                },
            )

        self.assertTrue(result["ok"])
        self.assertEqual([row[0] for row in order], ["voice", "sonos"])
        self.assertEqual(order[0][1]["start_lead_ms"], 1125)
        self.assertEqual(order[0][1]["target_volume_percent"]["voice_core:native:kitchen"], 44)
        self.assertEqual(order[0][1]["target_sync_offset_ms"]["voice_core:native:kitchen"], 100)
        self.assertEqual(order[1][1]["volume_by_speaker"], {"RINCON_KITCHEN": 62})
        self.assertIn("/api/media/runtime/", order[1][1]["source_url"])
        self.assertTrue(result["sonos_proxy_used"])

    def test_live_source_can_request_extra_native_start_runway(self) -> None:
        with mock.patch.object(
            media_playback,
            "_voice_core_play_media_sync",
            return_value={"ok": True, "sent_count": 2, "media_session_sent_count": 2},
        ) as voice:
            result = media_playback.play_media_url_targets(
                ["voice_core:native:office-left", "voice_core:native:office-right"],
                "http://tater.local:8501/live.wav",
                media_type="audio/wav",
                minimum_native_start_lead_ms=2500,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(voice.call_args.kwargs["start_lead_ms"], 2500)

    def test_airplay_bridge_uses_the_native_group_wall_clock_anchor(self) -> None:
        import airplay_bridge

        order = []

        def prepare(**kwargs):
            order.append(("prepare", kwargs))
            return {
                "ok": True,
                "group_id": "airplay-group-1",
                "prepared_count": 1,
            }

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "media_session_sent_count": 1,
                "start_unix_ms": 2000000000000,
                "audible_start_unix_ms": 2000000000125,
            }

        def prime(**kwargs):
            order.append(("prime", kwargs))
            return {"ok": True, "group_id": kwargs["group_id"], "primed_count": 1}

        def commit(**kwargs):
            order.append(("commit", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "group_id": kwargs["group_id"],
                "start_unix_ms": kwargs["start_unix_ms"],
            }

        with (
            mock.patch.object(airplay_bridge, "prepare_airplay_group_sync", side_effect=prepare),
            mock.patch.object(airplay_bridge, "prime_airplay_group_sync", side_effect=prime),
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync", side_effect=commit),
            mock.patch.object(media_playback, "_voice_core_play_media_sync", side_effect=voice),
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                "https://provider.test/song.mp3",
                filename="song.mp3",
                source_owner="external_audio",
                target_volume_percent={
                    "voice_core:native:kitchen": 52,
                    "airplay:804af2c57d78": 64,
                },
                target_sync_offset_ms={
                    "voice_core:native:kitchen": -80,
                    "airplay:804af2c57d78": 120,
                },
            )

        self.assertTrue(result["ok"])
        self.assertEqual([entry[0] for entry in order], ["prepare", "prime", "voice", "commit"])
        self.assertEqual(
            order[0][1]["source_url"],
            "https://provider.test/song.mp3",
        )
        self.assertNotIn("airplay_proxy_used", result)
        self.assertEqual(order[0][1]["targets"], ["804af2c57d78"])
        self.assertEqual(order[2][1]["start_lead_ms"], 750)
        self.assertEqual(order[2][1]["source_owner"], "external_audio")
        self.assertEqual(order[3][1]["start_unix_ms"], 2000000000125)
        self.assertEqual(order[3][1]["reference_sync_offset_ms"], -80)
        self.assertEqual(
            order[3][1]["target_sync_offset_ms"],
            {"airplay:804af2c57d78": 120},
        )
        self.assertFalse(order[3][1]["allow_reanchor"])
        self.assertEqual(result["airplay_bridge_primed_count"], 1)
        self.assertEqual(result["airplay_bridge_sent_count"], 1)
        self.assertEqual(result["sent_count"], 2)

    def test_next_track_reuses_the_connected_airplay_group(self) -> None:
        import airplay_bridge

        order = []

        def reuse(**kwargs):
            order.append(("reuse", kwargs))
            return {
                "ok": True,
                "reused": True,
                "group_id": kwargs["group_id"],
                "prepared_count": 1,
                "primed_count": 1,
                "minimum_start_unix_ms": 1001900,
                "clock_readiness": {
                    "airplay:804af2c57d78": {
                        "mode": "ptp",
                        "state": "ready",
                        "ready_at_unix_ms": 1000000,
                    }
                },
            }

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "media_session_sent_count": 1,
                "start_unix_ms": 1001900,
            }

        def commit(**kwargs):
            order.append(("commit", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "group_id": kwargs["group_id"],
                "start_unix_ms": kwargs["start_unix_ms"],
            }

        with (
            mock.patch.object(media_playback.time, "time", return_value=1000.0),
            mock.patch.object(airplay_bridge, "reuse_airplay_group_sync", side_effect=reuse),
            mock.patch.object(airplay_bridge, "prepare_airplay_group_sync") as prepare,
            mock.patch.object(airplay_bridge, "prime_airplay_group_sync") as prime,
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync", side_effect=commit),
            mock.patch.object(media_playback, "_voice_core_play_media_sync", side_effect=voice),
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                "https://provider.test/next.mp3",
                airplay_group_id="airplay-group-1",
                target_sync_offset_ms={
                    "voice_core:native:kitchen": -80,
                    "airplay:804af2c57d78": 120,
                },
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["airplay_bridge_reused"])
        self.assertEqual([entry[0] for entry in order], ["reuse", "voice", "commit"])
        self.assertEqual(order[0][1]["reference_sync_offset_ms"], -80)
        self.assertEqual(order[1][1]["start_lead_ms"], 1900)
        self.assertEqual(order[2][1]["group_id"], "airplay-group-1")
        prepare.assert_not_called()
        prime.assert_not_called()

    def test_failed_warm_reuse_falls_back_to_a_fresh_group(self) -> None:
        import airplay_bridge

        with (
            mock.patch.object(
                airplay_bridge,
                "reuse_airplay_group_sync",
                return_value={"ok": False, "error": "session ended", "reusable": False},
            ) as reuse,
            mock.patch.object(
                airplay_bridge,
                "prepare_airplay_group_sync",
                return_value={"ok": True, "group_id": "airplay-fresh", "prepared_count": 1},
            ) as prepare,
            mock.patch.object(
                airplay_bridge,
                "prime_airplay_group_sync",
                return_value={"ok": True, "group_id": "airplay-fresh", "primed_count": 1},
            ),
            mock.patch.object(
                airplay_bridge,
                "commit_airplay_group_sync",
                return_value={"ok": True, "group_id": "airplay-fresh", "sent_count": 1},
            ),
        ):
            result = media_playback.play_media_url_targets(
                ["airplay:804af2c57d78"],
                "https://provider.test/next.mp3",
                airplay_group_id="airplay-stale",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["airplay_bridge_group_id"], "airplay-fresh")
        self.assertEqual(result["airplay_bridge_reuse_fallback"], "session ended")
        reuse.assert_called_once()
        prepare.assert_called_once()

    def test_automatic_sonos_route_uses_airplay_when_a_satellite_is_selected(self) -> None:
        import airplay_bridge
        import announcement_targets

        order = []

        def prepare(**kwargs):
            order.append(("prepare", kwargs))
            return {"ok": True, "group_id": "airplay-auto-1", "prepared_count": 1}

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "media_session_sent_count": 1,
                "start_unix_ms": 2000000000000,
            }

        def prime(**kwargs):
            order.append(("prime", kwargs))
            return {"ok": True, "group_id": kwargs["group_id"], "primed_count": 1}

        def commit(**kwargs):
            order.append(("commit", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "group_id": kwargs["group_id"],
                "start_unix_ms": kwargs["start_unix_ms"],
            }

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
                return_value="airplay:804af2c57d78",
            ),
            mock.patch.object(airplay_bridge, "prepare_airplay_group_sync", side_effect=prepare),
            mock.patch.object(airplay_bridge, "prime_airplay_group_sync", side_effect=prime),
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync", side_effect=commit),
            mock.patch.object(media_playback, "_voice_core_play_media_sync", side_effect=voice),
            mock.patch.object(media_playback, "_sonos_playback_sync") as sonos,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "sonos:RINCON_KITCHEN"],
                "https://provider.test/song.mp3",
                target_volume_percent={"sonos:RINCON_KITCHEN": 63},
                target_sync_offset_ms={"sonos:RINCON_KITCHEN": 140},
                target_transport_mode={"sonos:RINCON_KITCHEN": "auto"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual([entry[0] for entry in order], ["prepare", "prime", "voice", "commit"])
        self.assertEqual(order[0][1]["targets"], ["804af2c57d78"])
        self.assertEqual(
            order[0][1]["target_volume_percent"]["airplay:804af2c57d78"],
            63,
        )
        self.assertEqual(
            order[3][1]["target_sync_offset_ms"]["airplay:804af2c57d78"],
            140,
        )
        self.assertEqual(result["sonos_airplay_target_count"], 1)
        self.assertEqual(
            result["sonos_airplay_routes"],
            {"sonos:RINCON_KITCHEN": "airplay:804af2c57d78"},
        )
        sonos.assert_not_called()

    def test_automatic_sonos_route_stays_native_for_sonos_only_playback(self) -> None:
        import announcement_targets

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
            ) as resolve_bridge,
            mock.patch.object(
                media_playback,
                "_runtime_media_proxy_source_url",
                return_value="http://127.0.0.1:8501/media/song.mp3",
            ),
            mock.patch.object(
                media_playback,
                "_sonos_playback_sync",
                return_value={"ok": True, "sent_count": 1},
            ) as sonos,
        ):
            result = media_playback.play_media_url_targets(
                ["sonos:RINCON_KITCHEN"],
                "https://provider.test/song.mp3",
                target_transport_mode={"sonos:RINCON_KITCHEN": "auto"},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["sonos_target_count"], 1)
        self.assertEqual(result["sonos_airplay_target_count"], 0)
        resolve_bridge.assert_not_called()
        sonos.assert_called_once()
        self.assertEqual(sonos.call_args.kwargs["speakers"], ["RINCON_KITCHEN"])

    def test_external_audio_sonos_only_route_is_forced_through_airplay(self) -> None:
        import airplay_bridge
        import announcement_targets

        order = []

        def prepare(**kwargs):
            order.append(("prepare", kwargs))
            return {"ok": True, "group_id": "airplay-sonos-live", "prepared_count": 1}

        def prime(**kwargs):
            order.append(("prime", kwargs))
            return {"ok": True, "group_id": kwargs["group_id"], "primed_count": 1}

        def commit(**kwargs):
            order.append(("commit", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "group_id": kwargs["group_id"],
                "start_unix_ms": kwargs["start_unix_ms"],
            }

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
                return_value="airplay:804af2c57d78",
            ),
            mock.patch.object(airplay_bridge, "prepare_airplay_group_sync", side_effect=prepare),
            mock.patch.object(airplay_bridge, "prime_airplay_group_sync", side_effect=prime),
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync", side_effect=commit),
            mock.patch.object(media_playback, "_sonos_playback_sync") as sonos,
        ):
            result = media_playback.play_media_url_targets(
                ["sonos:RINCON_KITCHEN"],
                "https://provider.test/live.wav",
                target_transport_mode={"sonos:RINCON_KITCHEN": "airplay"},
                source_owner="external_audio",
            )

        self.assertTrue(result["ok"])
        self.assertEqual([entry[0] for entry in order], ["prepare", "prime", "commit"])
        self.assertEqual(
            result["sonos_airplay_routes"],
            {"sonos:RINCON_KITCHEN": "airplay:804af2c57d78"},
        )
        sonos.assert_not_called()

    def test_external_audio_skips_sonos_when_its_airplay_endpoint_is_missing(self) -> None:
        import announcement_targets

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
                return_value="",
            ),
            mock.patch.object(media_playback, "_sonos_playback_sync") as sonos,
        ):
            result = media_playback.play_media_url_targets(
                ["sonos:RINCON_KITCHEN"],
                "https://provider.test/live.wav",
                target_transport_mode={"sonos:RINCON_KITCHEN": "airplay"},
                source_owner="external_audio",
            )

        self.assertFalse(result["ok"])
        self.assertIn("skipped to preserve sync", result["warnings"][0])
        sonos.assert_not_called()

    def test_mixed_airplay_group_primes_before_satellite_and_aborts_when_satellite_fails(self) -> None:
        import airplay_bridge

        with (
            mock.patch.object(
                airplay_bridge,
                "prepare_airplay_group_sync",
                return_value={
                    "ok": True,
                    "group_id": "airplay-failed-native",
                    "prepared_count": 1,
                },
            ),
            mock.patch.object(
                airplay_bridge,
                "prime_airplay_group_sync",
                return_value={
                    "ok": True,
                    "group_id": "airplay-failed-native",
                    "primed_count": 1,
                },
            ) as prime,
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync") as commit,
            mock.patch.object(airplay_bridge, "stop_airplay_targets") as stop,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": False, "sent_count": 0, "error": "satellite did not prepare"},
            ),
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                "https://provider.test/song.mp3",
            )

        self.assertFalse(result["ok"])
        stop.assert_called_once_with(["804af2c57d78"])
        prime.assert_called_once_with(group_id="airplay-failed-native", timeout_s=30.0)
        commit.assert_not_called()
        self.assertIn("satellite did not prepare", result["error"])

    def test_mixed_airplay_group_does_not_start_satellite_when_airplay_priming_fails(self) -> None:
        import airplay_bridge

        with (
            mock.patch.object(
                airplay_bridge,
                "prepare_airplay_group_sync",
                return_value={
                    "ok": True,
                    "group_id": "airplay-failed-prime",
                    "prepared_count": 1,
                },
            ),
            mock.patch.object(
                airplay_bridge,
                "prime_airplay_group_sync",
                return_value={
                    "ok": False,
                    "group_id": "airplay-failed-prime",
                    "primed_count": 0,
                    "error": "receiver audio feed did not start",
                },
            ),
            mock.patch.object(airplay_bridge, "commit_airplay_group_sync") as commit,
            mock.patch.object(media_playback, "_voice_core_play_media_sync") as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                "https://provider.test/song.mp3",
            )

        self.assertFalse(result["ok"])
        voice.assert_not_called()
        commit.assert_not_called()
        self.assertIn("receiver audio feed did not start", result["error"])

    def test_mixed_group_compensates_for_normalized_native_member_delays(self) -> None:
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 2},
            ) as voice,
            mock.patch.object(
                media_playback,
                "_runtime_media_proxy_source_url",
                return_value="http://tater.local:8501/api/media/runtime/asset/song.mp3",
            ),
            mock.patch.object(
                media_playback,
                "_sonos_playback_sync",
                return_value={"ok": True, "sent_count": 1},
            ),
        ):
            result = media_playback.play_media_url_targets(
                [
                    "voice_core:native:kitchen",
                    "voice_core:native:office",
                    "sonos:RINCON_LIVING",
                ],
                "https://provider.test/song.mp3",
                mixed_sync_adjustment_ms=175,
                target_sync_offset_ms={
                    "voice_core:native:kitchen": 0,
                    "voice_core:native:office": 100,
                    "sonos:RINCON_LIVING": -25,
                },
            )

        self.assertTrue(result["ok"])
        self.assertEqual(voice.call_args.kwargs["start_lead_ms"], 1125)
        self.assertEqual(result["mixed_sync_adjustment_ms"], 125)

    def test_runtime_media_proxy_registration_does_not_expose_source_url(self) -> None:
        proxy_url = media_playback._runtime_media_proxy_source_url(
            "https://provider.test/stream?player_token=secret",
            content_type="audio/mpeg",
            filename="song.mp3",
        )
        self.assertIn("/api/media/runtime/", proxy_url)
        self.assertTrue(proxy_url.endswith("/song.mp3"))
        self.assertNotIn("secret", proxy_url)

    def test_runtime_media_proxy_can_use_loopback_for_local_airplay_sender(self) -> None:
        proxy_url = media_playback._runtime_media_proxy_source_url(
            "https://provider.test/stream?player_token=secret",
            content_type="audio/flac",
            filename="song.flac",
            prefer_loopback=True,
        )
        self.assertTrue(proxy_url.startswith("http://127.0.0.1:"))
        self.assertTrue(proxy_url.endswith("/song.flac"))
        self.assertNotIn("secret", proxy_url)

    def test_runtime_media_proxy_is_public_for_lan_players(self) -> None:
        app_source = (Path(__file__).resolve().parents[1] / "tateros_app.py").read_text()
        self.assertIn('path.startswith("/api/media/runtime/")', app_source)

    def test_generic_media_player_uses_its_play_media_action(self) -> None:
        devices = [
            {
                "integration_id": "example_player",
                "id": "kitchen",
                "actions": ["play_media"],
                "capabilities": ["media_player"],
            }
        ]
        with mock.patch(
            "integration_registry.get_integration_devices_by_capability",
            return_value=devices,
        ):
            action = media_playback._integration_device_playback_action(
                "example_player",
                "kitchen",
            )

        self.assertEqual(action, "play_media")


if __name__ == "__main__":
    unittest.main()
