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
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["media_session_sent_count"], 1)
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["playback_role"], "media")
        self.assertEqual(payload["media_content_type"], "music")


if __name__ == "__main__":
    unittest.main()
