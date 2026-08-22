from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from tater_voice import native_satellite
from tater_voice import stereo_pairs
from tater_voice import voice_pipeline


class NativeReplyPlaybackTests(unittest.IsolatedAsyncioTestCase):
    async def test_inactive_stereo_reply_target_skips_spoken_tool_progress(self) -> None:
        pair = {
            "selector": "stereo:office",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        with (
            mock.patch.object(stereo_pairs, "get_pair", return_value=pair),
            mock.patch.object(native_satellite, "stereo_pair_media_active", return_value=False),
        ):
            self.assertTrue(
                voice_pipeline._stereo_reply_target_without_active_media(
                    "voice_core:stereo:office"
                )
            )

        with (
            mock.patch.object(stereo_pairs, "get_pair", return_value=pair),
            mock.patch.object(native_satellite, "stereo_pair_media_active", return_value=True),
        ):
            self.assertFalse(
                voice_pipeline._stereo_reply_target_without_active_media(
                    "voice_core:stereo:office"
                )
            )

    async def test_tts_end_queues_playback_with_clamped_ducking(self) -> None:
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        client = native_satellite._NativeVoiceAssistantClient(
            "native:test-satellite",
            queue,
        )

        with mock.patch(
            "speech_settings.get_speech_settings",
            return_value={
                "satellite_ducking_target_percent": "120",
                "satellite_ducking_attack_ms": "-25",
                "satellite_ducking_release_ms": "invalid",
            },
        ):
            await client.send_voice_assistant_event(
                native_satellite._NativeVoiceAssistantEventType.TTS_END,
                {"url": "http://tater.local/reply.wav"},
            )

        messages = []
        while not queue.empty():
            messages.append(queue.get_nowait())

        self.assertEqual(
            ["voice.event", "state", "play.url"],
            [message["type"] for message in messages],
        )
        playback = messages[-1]["payload"]
        self.assertEqual("http://tater.local/reply.wav", playback["url"])
        self.assertEqual(
            {
                "target_percent": 100,
                "attack_ms": 0,
                "release_ms": 350,
            },
            playback["ducking"],
        )


if __name__ == "__main__":
    unittest.main()
