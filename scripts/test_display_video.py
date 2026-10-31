#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest.mock import AsyncMock, patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tater_voice import display_video


CLIENTS = {
    "native:kitchen": {
        "connected": True,
        "device_name": "Tater Crown Kitchen",
        "room": "Kitchen",
        "capabilities": {
            "screen_video_playback": True,
            "screen_video_transcode_profile": "echo_show_720p",
            "screen_video_width": 1280,
            "screen_video_height": 800,
        },
    },
    "native:office-dot": {
        "connected": True,
        "device_name": "Office Dot",
        "room": "Office",
        "capabilities": {"screen_video_playback": False},
    },
    "native:office-show": {
        "connected": True,
        "device_name": "Office Show",
        "room": "Office",
        "capabilities": {
            "screen_video_playback": True,
            "screen_video_transcode_profile": "echo_checkers_480p",
            "screen_video_width": 854,
            "screen_video_height": 480,
        },
    },
    "native:garage-dot": {
        "connected": True,
        "device_name": "Garage Dot",
        "room": "Garage",
        "capabilities": {"screen_video_playback": False},
    },
}

GARAGE_TV = {
    "selector": "integration:google_cast:garage-tv",
    "name": "Garage TV",
    "room": "Garage",
    "kind": "integration",
    "integration_id": "google_cast",
    "device_id": "garage-tv",
    "aliases": [],
    "transcode_profile": "echo_show_720p",
    "output_container": "hls",
    "media_type": "application/vnd.apple.mpegurl",
    "width": 1280,
    "height": 720,
}


class DisplayVideoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with display_video._lock:
            display_video._sessions.clear()
            display_video._provider_handlers.clear()
        integration_patcher = patch.object(display_video, "_integration_video_rows", return_value=[])
        integration_patcher.start()
        self.addCleanup(integration_patcher.stop)
        preference_patcher = patch.object(display_video, "_preferred_room_target", return_value="")
        preference_patcher.start()
        self.addCleanup(preference_patcher.stop)

    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    def test_target_resolution_requires_a_connected_screen(self, _clients):
        self.assertEqual(display_video.resolve_target("Kitchen"), "native:kitchen")
        self.assertEqual(display_video.resolve_target("native:kitchen"), "native:kitchen")
        self.assertEqual(display_video.resolve_target("Office Dot"), "native:office-show")
        with self.assertRaisesRegex(ValueError, "no screen or compatible TV"):
            display_video.resolve_target("", current_selector="native:garage-dot")

    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    def test_implicit_target_prefers_speaking_screen_then_same_room_screen(self, _clients):
        self.assertEqual(
            display_video.resolve_target("", current_selector="native:kitchen"),
            "native:kitchen",
        )
        self.assertEqual(
            display_video.resolve_target("", current_selector="native:office-dot"),
            "native:office-show",
        )

    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    def test_implicit_target_falls_back_to_same_room_cast_tv(self, _clients):
        with patch.object(display_video, "_integration_video_rows", return_value=[GARAGE_TV]):
            self.assertEqual(
                display_video.resolve_target("", current_selector="native:garage-dot"),
                "integration:google_cast:garage-tv",
            )
            self.assertEqual(
                display_video.match_request_target("play the movie on the garage TV"),
                "integration:google_cast:garage-tv",
            )
            profile = display_video.target_playback_profile("integration:google_cast:garage-tv")
            self.assertEqual(profile["output_container"], "hls")

    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    def test_natural_language_target_does_not_confuse_title_with_room(self, _clients):
        self.assertEqual(display_video.match_request_target("play The Office"), "")
        self.assertEqual(
            display_video.match_request_target("play The Matrix in the office"),
            "native:office-show",
        )

    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    def test_target_profile_comes_from_firmware_capabilities(self, _clients):
        profile = display_video.target_playback_profile("Kitchen")
        self.assertEqual(profile["transcode_profile"], "echo_show_720p")
        self.assertEqual((profile["width"], profile["height"]), (1280, 800))

    @patch.object(display_video, "_send", new_callable=AsyncMock, return_value={"ok": True})
    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    async def test_play_creates_provider_neutral_session(self, _clients, send):
        result = await display_video.play(
            "Kitchen",
            "https://media.invalid/library/movie/master.m3u8?token=private",
            title="A Movie",
            fps=24,
            duration_ms=90_000,
        )
        session = result["session"]
        self.assertTrue(session["active"])
        self.assertEqual(session["state"], "playing")
        self.assertEqual(session["fps"], 24)
        self.assertEqual(session["duration_ms"], 90_000)
        sent = send.await_args.args[1]
        self.assertEqual(display_video._device_payload(sent), session)

    @patch.object(display_video, "_play_external", new_callable=AsyncMock, return_value={"ok": True})
    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    async def test_external_tv_uses_media_playback_path(self, _clients, external_play):
        with patch.object(display_video, "_integration_video_rows", return_value=[GARAGE_TV]):
            result = await display_video.play(
                "integration:google_cast:garage-tv",
                "https://media.invalid/movie.m3u8",
                media_type="application/vnd.apple.mpegurl",
            )
        self.assertEqual(result["selector"], "integration:google_cast:garage-tv")
        external_play.assert_awaited_once()

    @patch.object(display_video, "_send", new_callable=AsyncMock, return_value={"ok": True})
    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    async def test_provider_receives_progress_without_leaking_metadata_to_device(self, _clients, send):
        events = []

        async def provider(selector, event, session):
            events.append((selector, event, session["provider_data"]["playback_id"]))

        display_video.register_provider_handler("tater_tube", provider)
        started = await display_video.play(
            "Kitchen",
            "https://media.invalid/movie.ts",
            provider="tater_tube",
            provider_data={"playback_id": "play-1"},
            server_seek=True,
        )
        session_id = started["session"]["session_id"]
        await display_video.handle_device_action(
            "native:kitchen", "display.video.progress", {"session_id": session_id, "position_ms": 15_000}
        )
        self.assertEqual(events[-1], ("native:kitchen", "progress", "play-1"))
        sent = display_video._device_payload(send.await_args_list[0].args[1])
        self.assertNotIn("provider", sent)
        self.assertNotIn("provider_data", sent)
        self.assertTrue(sent["server_seek"])

    @patch.object(display_video, "_send", new_callable=AsyncMock, return_value={"ok": True})
    @patch.object(display_video, "_client_rows", return_value=CLIENTS)
    async def test_device_pause_updates_authoritative_position(self, _clients, _send):
        started = await display_video.play("Kitchen", "https://media.invalid/movie.mp4")
        session_id = started["session"]["session_id"]
        result = await display_video.handle_device_action(
            "native:kitchen",
            "display.video.pause",
            {"session_id": session_id, "position_ms": 4321},
        )
        self.assertTrue(result["ok"])
        self.assertEqual(display_video.session_payload("native:kitchen")["state"], "paused")
        self.assertEqual(display_video.session_payload("native:kitchen")["position_ms"], 4321)

if __name__ == "__main__":
    unittest.main()
