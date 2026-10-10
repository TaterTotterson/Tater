from __future__ import annotations

import asyncio
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
    def test_music_core_native_route_uses_one_sendspin_media_timeline(self) -> None:
        from tater_voice import native_satellite, sendspin_playback

        async def targets(_selectors):
            return [
                {
                    "selector": "native:left",
                    "logical_selector": "stereo:office",
                    "host": "192.0.2.10",
                    "port": 8928,
                    "volume_percent": 80,
                    "delay_ms": 5,
                },
                {
                    "selector": "native:right",
                    "logical_selector": "stereo:office",
                    "host": "192.0.2.11",
                    "port": 8928,
                    "volume_percent": 100,
                    "delay_ms": 15,
                },
            ]

        async def start_media(stream_id, resolved_targets, source_url, **kwargs):
            self.assertTrue(stream_id.startswith("music-"))
            self.assertEqual(source_url, "https://example.test/song.flac")
            self.assertEqual(
                [row["volume_percent"] for row in resolved_targets],
                [48, 60],
            )
            self.assertEqual([row["delay_ms"] for row in resolved_targets], [0, 10])
            self.assertEqual(kwargs["start_position_seconds"], 37.25)
            self.assertEqual(kwargs["start_lead_ms"], 900)
            return {
                "ok": True,
                "sendspin_live_stream_started": True,
                "stream_id": stream_id,
                "members": ["native:left", "native:right"],
                "start_unix_ms": 2_000_000_000_000,
            }

        with (
            mock.patch.object(
                media_playback,
                "_voice_core_handoff_media_sync",
                return_value={"ok": True, "selectors": [], "sessions": []},
            ),
            mock.patch.object(native_satellite, "sendspin_targets_for_selectors", side_effect=targets),
            mock.patch.object(sendspin_playback, "start_media_url_stream", side_effect=start_media),
            mock.patch.object(
                native_satellite,
                "run_on_runtime_loop",
                side_effect=lambda awaitable, **_kwargs: asyncio.run(awaitable),
            ),
            mock.patch.object(media_playback.requests, "post") as post,
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["stereo:office"],
                source_url="https://example.test/song.flac",
                media_type="audio/flac",
                media_content_type="music",
                title="Three Little Birds",
                volume_percent=60,
                target_volume_percent={"voice_core:stereo:office": 60},
                target_sync_offset_ms={"voice_core:stereo:office": -25},
                start_position_seconds=37.25,
                start_lead_ms=900,
                source_owner="music_core",
            )

        self.assertTrue(result["sendspin_live_stream_started"])
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["voice_core_sessions"][0]["transport"], "sendspin")
        self.assertEqual(
            result["voice_core_sessions"][0]["selectors"],
            ["native:left", "native:right"],
        )
        self.assertEqual(
            result["voice_core_sessions"][0]["start_unix_ms"],
            2_000_000_000_000,
        )
        post.assert_not_called()

    def test_external_airplay_native_route_uses_sendspin_instead_of_media_session_http(self) -> None:
        import external_audio

        sendspin_result = {
            "ok": True,
            "sent_count": 2,
            "sendspin_sent_count": 2,
            "sendspin_live_stream_started": True,
            "voice_core_sessions": [
                {
                    "target": "sendspin:airplay-1",
                    "session_id": "airplay-1",
                    "selectors": ["native:kitchen", "native:office"],
                    "transport": "sendspin",
                }
            ],
        }
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_handoff_media_sync",
                return_value={"ok": True, "selectors": [], "sessions": []},
            ),
            mock.patch.object(
                external_audio,
                "start_external_audio_sendspin_route",
                return_value=sendspin_result,
            ) as start_sendspin,
            mock.patch.object(media_playback.requests, "post") as post,
        ):
            result = media_playback._voice_core_play_media_sync(
                selectors=["native:kitchen", "native:office"],
                source_url="http://tater.local/live.mp3",
                media_content_type="music",
                volume_percent=72,
                target_sync_offset_ms={"voice_core:native:office": 25},
                start_lead_ms=900,
                source_owner="external_audio",
            )

        self.assertTrue(result["sendspin_live_stream_started"])
        self.assertEqual(result["voice_core_sessions"][0]["transport"], "sendspin")
        start_sendspin.assert_called_once_with(
            ["native:kitchen", "native:office"],
            airplay_targets=[],
            volume_percent=72,
            target_volume_percent=None,
            target_sync_offset_ms={"voice_core:native:office": 25},
            start_lead_ms=900,
            title="AirPlay",
        )
        post.assert_not_called()

    def test_external_airplay_stop_ends_sendspin_without_legacy_media_command(self) -> None:
        from tater_voice import native_satellite, sendspin_playback

        stopped = []

        async def fake_stop(stream_id, *, reason="stopped"):
            stopped.append((stream_id, reason))
            return {"ok": True, "stopped": True}

        with (
            mock.patch.object(sendspin_playback, "stop_live_stream", side_effect=fake_stop),
            mock.patch.object(
                native_satellite,
                "run_on_runtime_loop",
                side_effect=lambda awaitable, **_kwargs: asyncio.run(awaitable),
            ),
            mock.patch.object(native_satellite, "fade_and_stop_media_session_if_matches") as legacy_stop,
        ):
            warnings = media_playback._voice_core_stop_media_sync(
                [],
                expected_sessions=[
                    {
                        "session_id": "airplay-stream-1",
                        "selectors": ["native:kitchen", "native:office"],
                        "transport": "sendspin",
                    }
                ],
                reason="external_audio_manual_stop",
            )

        self.assertEqual(warnings, [])
        self.assertEqual(
            stopped,
            [("airplay-stream-1", "external_audio_manual_stop")],
        )
        legacy_stop.assert_not_called()

    def test_native_only_airplay_group_bypasses_the_shared_relay(self) -> None:
        with (
            mock.patch.object(
                media_playback, "_shared_native_media_source_url"
            ) as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={
                    "ok": True,
                    "sent_count": 2,
                    "sendspin_sent_count": 2,
                    "voice_core_sessions": [],
                },
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "voice_core:native:office"],
                "http://tater.local/api/external-audio/v1/streams/live.mp3",
                media_content_type="music",
                source_owner="external_audio",
            )

        self.assertTrue(result["ok"])
        shared_source.assert_not_called()
        self.assertEqual(
            voice.call_args.kwargs["source_url"],
            "http://tater.local/api/external-audio/v1/streams/live.mp3",
        )

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

    def test_music_core_stereo_pair_uses_sendspin_without_shared_relay(self) -> None:
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_selector_members",
                return_value={
                    "stereo:office": ["native:office-left", "native:office-right"]
                },
            ),
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 1, "media_session_sent_count": 1},
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:stereo:office"],
                "https://provider.test/song.flac",
                media_type="audio/flac",
                media_content_type="music",
                filename="song.flac",
                duration_seconds=196,
                source_owner="music_core",
            )

        self.assertTrue(result["ok"])
        self.assertFalse(result.get("group_shared_stream", False))
        shared_source.assert_not_called()
        self.assertEqual(
            voice.call_args.kwargs["source_url"],
            "https://provider.test/song.flac",
        )
        self.assertEqual(
            voice.call_args.kwargs["start_lead_ms"],
            media_playback.NATIVE_GROUP_START_LEAD_MS,
        )

    def test_external_group_can_opt_in_to_shared_source(self) -> None:
        source_url = "https://personal-music.test/stream/song.wav"
        shared_url = "http://tater.local:8501/api/media/shared/relay/song.wav?token=secret"
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_selector_members",
                return_value={
                    "native:kitchen": ["native:kitchen"],
                    "native:office": ["native:office"],
                },
            ),
            mock.patch.object(
                media_playback,
                "_shared_native_media_source_url",
                return_value=(shared_url, {"initial_bytes": 4096, "complete": False}),
            ) as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 2, "media_session_sent_count": 2},
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "voice_core:native:office"],
                source_url,
                media_type="audio/wav",
                filename="song.wav",
                shared_group_source=True,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["group_shared_stream"])
        shared_source.assert_called_once()
        self.assertEqual(shared_source.call_args.args, (source_url,))
        self.assertEqual(voice.call_args.kwargs["source_url"], shared_url)

    def test_external_group_without_opt_in_keeps_original_source(self) -> None:
        source_url = "https://personal-music.test/stream/song.wav"
        with (
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 2, "media_session_sent_count": 2},
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "voice_core:native:office"],
                source_url,
                media_type="audio/wav",
                filename="song.wav",
            )

        self.assertTrue(result["ok"])
        self.assertFalse(result.get("group_shared_stream", False))
        shared_source.assert_not_called()
        self.assertEqual(voice.call_args.kwargs["source_url"], source_url)

    def test_external_group_opt_in_survives_resume_retry(self) -> None:
        source_url = "https://personal-music.test/stream/song.wav"
        shared_url = "http://tater.local:8501/api/media/shared/relay/song.wav?token=secret"
        with (
            mock.patch.object(
                media_playback,
                "_shared_native_media_source_url",
                return_value=(shared_url, {"initial_bytes": 4096, "complete": False}),
            ) as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                side_effect=[
                    {"ok": False, "sent_count": 0, "error": "resume failed"},
                    {"ok": True, "sent_count": 2, "media_session_sent_count": 2},
                ],
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "voice_core:native:office"],
                source_url,
                media_type="audio/wav",
                start_position_seconds=30.0,
                shared_group_source=True,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["resume_fallback_used"])
        self.assertTrue(result["group_shared_stream"])
        self.assertEqual(shared_source.call_count, 2)
        self.assertEqual(voice.call_count, 2)
        self.assertEqual(voice.call_args.kwargs["source_url"], shared_url)
        self.assertEqual(voice.call_args.kwargs["start_position_seconds"], 0.0)

    def test_single_satellite_keeps_original_source_without_stereo_relay(self) -> None:
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_selector_members",
                return_value={"native:kitchen": ["native:kitchen"]},
            ),
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 1, "media_session_sent_count": 1},
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen"],
                "https://provider.test/song.mp3",
                media_type="audio/mpeg",
                media_content_type="music",
            )

        self.assertTrue(result["ok"])
        shared_source.assert_not_called()
        self.assertEqual(
            voice.call_args.kwargs["source_url"],
            "https://provider.test/song.mp3",
        )
        self.assertEqual(voice.call_args.kwargs["start_lead_ms"], 0)

    def test_single_satellite_opt_in_keeps_original_source(self) -> None:
        source_url = "https://personal-music.test/stream/song.wav"
        with (
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={"ok": True, "sent_count": 1, "media_session_sent_count": 1},
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen"],
                source_url,
                media_type="audio/wav",
                shared_group_source=True,
            )

        self.assertTrue(result["ok"])
        shared_source.assert_not_called()
        self.assertEqual(voice.call_args.kwargs["source_url"], source_url)

    def test_live_airplay_group_uses_one_sendspin_pcm_source_for_native_and_airplay(self) -> None:
        source_url = "http://tater.local:8501/api/external-audio/v1/streams/session/live.mp3?cursor=0"
        with (
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback, "_voice_core_play_media_sync",
                return_value={
                    "ok": True,
                    "sent_count": 2,
                    "native_sent_count": 1,
                    "airplay_bridge_sent_count": 1,
                    "airplay_bridge_prepared_count": 1,
                    "airplay_bridge_group_id": "sendspin-airplay-one",
                    "group_shared_stream": True,
                    "audible_start_unix_ms": 2000000000125,
                },
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                source_url,
                source_owner="external_audio",
                media_content_type="music",
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["group_shared_stream"])
        self.assertEqual(result["sent_count"], 2)
        shared_source.assert_not_called()
        self.assertEqual(voice.call_args.kwargs["source_url"], source_url)
        self.assertEqual(voice.call_args.kwargs["airplay_players"], ["804af2c57d78"])

    def test_music_core_group_uses_one_shared_source_for_native_and_airplay(self) -> None:
        source_url = "https://provider.test/song.flac"
        with (
            mock.patch.object(media_playback, "_shared_native_media_source_url") as shared_source,
            mock.patch.object(
                media_playback, "_voice_core_play_media_sync",
                return_value={
                    "ok": True,
                    "sent_count": 2,
                    "native_sent_count": 1,
                    "airplay_bridge_sent_count": 1,
                    "airplay_bridge_prepared_count": 1,
                    "group_shared_stream": True,
                    "audible_start_unix_ms": 2000000000125,
                },
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                source_url,
                source_owner="music_core",
                media_type="audio/flac",
                media_content_type="music",
                filename="song.flac",
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["group_shared_stream"])
        shared_source.assert_not_called()
        self.assertEqual(voice.call_args.kwargs["source_url"], source_url)
        self.assertEqual(voice.call_args.kwargs["airplay_players"], ["804af2c57d78"])

    def test_airplay_bridge_is_owned_by_the_sendspin_group(self) -> None:
        order = []

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 2,
                "native_sent_count": 1,
                "airplay_bridge_sent_count": 1,
                "airplay_bridge_prepared_count": 1,
                "airplay_bridge_group_id": "sendspin-airplay-1",
                "group_shared_stream": True,
                "start_unix_ms": 2000000000000,
                "audible_start_unix_ms": 2000000000125,
            }

        with (
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
        self.assertEqual([entry[0] for entry in order], ["voice"])
        self.assertNotIn("airplay_proxy_used", result)
        self.assertEqual(order[0][1]["airplay_players"], ["804af2c57d78"])
        self.assertEqual(order[0][1]["source_owner"], "external_audio")
        self.assertEqual(result["airplay_bridge_group_id"], "sendspin-airplay-1")
        self.assertEqual(result["airplay_bridge_sent_count"], 1)
        self.assertEqual(result["sent_count"], 2)

    def test_next_track_replaces_legacy_airplay_group_with_sendspin_bridge(self) -> None:
        order = []

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 2,
                "native_sent_count": 1,
                "airplay_bridge_sent_count": 1,
                "airplay_bridge_prepared_count": 1,
                "airplay_bridge_group_id": "sendspin-airplay-next",
                "group_shared_stream": True,
                "start_unix_ms": 1001900,
            }

        with (
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
        self.assertEqual([entry[0] for entry in order], ["voice"])
        self.assertEqual(result["airplay_bridge_group_id"], "sendspin-airplay-next")
        self.assertEqual(order[0][1]["airplay_players"], ["804af2c57d78"])

    def test_airplay_only_playback_uses_a_fresh_sendspin_bridge(self) -> None:
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={
                    "ok": True,
                    "sent_count": 1,
                    "native_sent_count": 0,
                    "airplay_bridge_sent_count": 1,
                    "airplay_bridge_prepared_count": 1,
                    "airplay_bridge_group_id": "sendspin-airplay-fresh",
                    "group_shared_stream": True,
                },
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["airplay:804af2c57d78"],
                "https://provider.test/next.mp3",
                airplay_group_id="airplay-stale",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["airplay_bridge_group_id"], "sendspin-airplay-fresh")
        self.assertEqual(voice.call_args.kwargs["selectors"], [])
        self.assertEqual(voice.call_args.kwargs["airplay_players"], ["804af2c57d78"])

    def test_automatic_sonos_route_uses_airplay_when_a_satellite_is_selected(self) -> None:
        import announcement_targets

        order = []

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 2,
                "native_sent_count": 1,
                "airplay_bridge_sent_count": 1,
                "airplay_bridge_prepared_count": 1,
                "airplay_bridge_group_id": "sendspin-airplay-auto-1",
                "group_shared_stream": True,
                "start_unix_ms": 2000000000000,
            }

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
                return_value="airplay:804af2c57d78",
            ),
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
        self.assertEqual([entry[0] for entry in order], ["voice"])
        self.assertEqual(order[0][1]["airplay_players"], ["804af2c57d78"])
        self.assertEqual(
            order[0][1]["target_volume_percent"]["airplay:804af2c57d78"],
            63,
        )
        self.assertEqual(
            order[0][1]["target_sync_offset_ms"]["airplay:804af2c57d78"],
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
        import announcement_targets

        order = []

        def voice(**kwargs):
            order.append(("voice", kwargs))
            return {
                "ok": True,
                "sent_count": 1,
                "native_sent_count": 0,
                "airplay_bridge_sent_count": 1,
                "airplay_bridge_prepared_count": 1,
                "airplay_bridge_group_id": "sendspin-airplay-sonos-live",
                "group_shared_stream": True,
            }

        with (
            mock.patch.object(
                announcement_targets,
                "resolve_sonos_airplay_target",
                return_value="airplay:804af2c57d78",
            ),
            mock.patch.object(
                media_playback, "_voice_core_play_media_sync", side_effect=voice
            ),
            mock.patch.object(media_playback, "_sonos_playback_sync") as sonos,
        ):
            result = media_playback.play_media_url_targets(
                ["sonos:RINCON_KITCHEN"],
                "https://provider.test/live.wav",
                target_transport_mode={"sonos:RINCON_KITCHEN": "airplay"},
                source_owner="external_audio",
            )

        self.assertTrue(result["ok"])
        self.assertEqual([entry[0] for entry in order], ["voice"])
        self.assertEqual(order[0][1]["airplay_players"], ["804af2c57d78"])
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

    def test_mixed_sendspin_bridge_reports_group_start_failure_atomically(self) -> None:
        import airplay_bridge

        with (
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
        stop.assert_not_called()
        self.assertIn("satellite did not prepare", result["error"])

    def test_mixed_sendspin_bridge_surfaces_airplay_preparation_failure(self) -> None:
        with (
            mock.patch.object(
                media_playback,
                "_voice_core_play_media_sync",
                return_value={
                    "ok": False,
                    "sent_count": 0,
                    "error": "receiver audio feed did not start",
                },
            ) as voice,
        ):
            result = media_playback.play_media_url_targets(
                ["voice_core:native:kitchen", "airplay:804af2c57d78"],
                "https://provider.test/song.mp3",
            )

        self.assertFalse(result["ok"])
        voice.assert_called_once()
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

    def test_shared_media_relay_is_public_only_through_its_tokenized_path(self) -> None:
        app_source = (Path(__file__).resolve().parents[1] / "tateros_app.py").read_text()
        self.assertIn('path.startswith("/api/media/shared/")', app_source)
        self.assertIn('@app.get("/api/media/shared/{relay_id}/{filename:path}")', app_source)

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

    def test_integration_playback_omits_unspecified_volume(self) -> None:
        devices = [
            {
                "integration_id": "google_cast",
                "id": "office-tv",
                "actions": ["play_media"],
                "capabilities": ["media_player"],
            }
        ]
        with (
            mock.patch(
                "integration_registry.get_integration_devices_by_capability",
                return_value=devices,
            ),
            mock.patch(
                "integration_registry.run_integration_device_action",
                return_value={"ok": True, "sent_count": 1},
            ) as run_action,
        ):
            result = media_playback._integration_playback_sync(
                targets=[{"integration_id": "google_cast", "device_id": "office-tv"}],
                source_url="https://example.test/video.mp4",
                media_type="video/mp4",
                volume_percent=None,
            )

        self.assertTrue(result["ok"])
        self.assertNotIn("volume_percent", run_action.call_args.args[3])


if __name__ == "__main__":
    unittest.main()
