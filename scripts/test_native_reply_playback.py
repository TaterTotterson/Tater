from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from tater_voice import native_satellite
from tater_voice import voice_pipeline
from tater_voice.voice_pipeline.conversation import VoiceSessionRuntime


class NativeReplyPlaybackTests(unittest.IsolatedAsyncioTestCase):
    async def test_stereo_reply_target_plays_spoken_tool_progress(self) -> None:
        session = VoiceSessionRuntime(
            selector="native:left",
            session_id="session-1",
            conversation_id="conversation-1",
            wake_word="hey tater",
            audio_format={"rate": 16000, "width": 2, "channels": 1},
            started_ts=0.0,
            startup_gate_until_ts=0.0,
            context={"reply_playback_target": "voice_core:stereo:office"},
        )
        external_calls = []

        async def fake_external_playback(**kwargs):
            external_calls.append(kwargs)
            return {"ok": True, "voice_core_playback_completed": True}

        with (
            mock.patch.object(voice_pipeline, "_send_tool_call_visual", new=mock.AsyncMock()),
            mock.patch.object(voice_pipeline, "_experimental_live_tool_progress_enabled", return_value=True),
            mock.patch.object(
                voice_pipeline,
                "_native_synthesize_text",
                new=mock.AsyncMock(
                    return_value=(
                        b"\x00\x00" * 160,
                        {"rate": 16000, "width": 2, "channels": 1},
                        "test",
                        "",
                    )
                ),
            ),
            mock.patch.object(voice_pipeline, "_play_reply_on_external_target", new=fake_external_playback),
            mock.patch.object(voice_pipeline, "_esphome_send_event", new=mock.AsyncMock(return_value=True)),
        ):
            await voice_pipeline._play_live_tool_progress_for_session(
                object(),
                object(),
                selector="native:left",
                runtime={},
                session=session,
                transcript="turn on the lights",
                wait_text="Working on it.",
            )

        self.assertEqual(1, len(external_calls))
        self.assertEqual("voice_core:stereo:office", external_calls[0]["target"])
        self.assertEqual("Working on it.", external_calls[0]["spoken_text"])
        self.assertTrue(session.live_tool_progress_played)
        self.assertEqual("Working on it.", session.last_tool_progress_text)

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
