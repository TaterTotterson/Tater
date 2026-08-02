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
                start_lead_ms=750,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["synchronized_group"])
        self.assertEqual(result["sent_count"], 2)
        self.assertTrue(post.call_args.args[0].endswith("/api/tater/satellite/v1/play-group"))
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["selectors"], ["native:kitchen", "native:office"])
        self.assertEqual(payload["start_lead_ms"], 750)

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
            )

        self.assertTrue(result["ok"])
        self.assertEqual([row[0] for row in order], ["voice", "sonos"])
        self.assertEqual(order[0][1]["start_lead_ms"], 1125)
        self.assertIn("/api/media/runtime/", order[1][1]["source_url"])
        self.assertTrue(result["sonos_proxy_used"])

    def test_runtime_media_proxy_registration_does_not_expose_source_url(self) -> None:
        proxy_url = media_playback._runtime_media_proxy_source_url(
            "https://provider.test/stream?player_token=secret",
            content_type="audio/mpeg",
            filename="song.mp3",
        )
        self.assertIn("/api/media/runtime/", proxy_url)
        self.assertTrue(proxy_url.endswith("/song.mp3"))
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
