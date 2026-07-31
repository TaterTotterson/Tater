from __future__ import annotations

import unittest
from unittest import mock

import speech_tts


class _CompletedResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            "ok": True,
            "media_session_started": True,
            "playback_completed": True,
        }


class StereoTtsCompletionTests(unittest.IsolatedAsyncioTestCase):
    def test_voice_core_request_propagates_completion_wait(self) -> None:
        with (
            mock.patch.object(speech_tts, "_voice_core_base_url", return_value="http://127.0.0.1:8501"),
            mock.patch.object(speech_tts, "_voice_core_auth_headers", return_value={}),
            mock.patch.object(speech_tts.requests, "post", return_value=_CompletedResponse()) as post,
        ):
            result = speech_tts._voice_core_play_media_sync(
                selectors=["stereo:office12"],
                source_url="",
                audio_bytes=b"wav",
                timeout_s=180.0,
                wait_for_completion=True,
            )

        self.assertTrue(result["playback_completed"])
        self.assertEqual(result["playback_completed_count"], 1)
        self.assertTrue(post.call_args.kwargs["json"]["wait_for_completion"])
        self.assertEqual(post.call_args.kwargs["timeout"], 195.0)

    async def test_announcement_result_exposes_voice_core_completion(self) -> None:
        captured = {}

        async def fake_run_background(function, **kwargs):
            captured["function"] = function
            captured["kwargs"] = dict(kwargs)
            return {
                "ok": True,
                "sent_count": 1,
                "playback_completed": True,
            }

        with mock.patch.object(speech_tts, "run_background", side_effect=fake_run_background):
            result = await speech_tts.play_announcement_audio_targets(
                text="Testing stereo completion.",
                wav_bytes=b"wav",
                ha_base="",
                token="",
                targets=["voice_core:stereo:office12"],
                wait_for_completion=True,
            )

        self.assertIs(captured["function"], speech_tts._voice_core_play_media_sync)
        self.assertTrue(captured["kwargs"]["wait_for_completion"])
        self.assertTrue(result["voice_core_playback_completed"])


if __name__ == "__main__":
    unittest.main()
