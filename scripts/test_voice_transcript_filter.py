#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import contextlib
import unittest
from unittest import mock

from tater_voice import voice_pipeline as vp
from tater_voice.voice_pipeline import backends, conversation
from tater_voice import display_cameras, intercom, speaker_id


def session(**kwargs):
    values = {
        "selector": "native:voice-test",
        "session_id": "test-turn",
        "conversation_id": "test-conversation",
        "wake_word": "hey_tater",
        "audio_format": {"rate": 16000, "width": 2, "channels": 1},
        "started_ts": 1.0,
        "startup_gate_until_ts": 0.0,
    }
    return conversation.VoiceSessionRuntime(**(values | kwargs))


class VoiceTranscriptTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        for owner, name, result in (
            (speaker_id, "consume_pending_enrollment", None),
            (display_cameras, "is_talkback_wake_word", False),
            (intercom, "is_broadcast_wake_word", False),
            (vp, "_native_debug", None),
            (vp, "_voice_metrics_record_stt", None),
            (vp, "_voice_metrics_record_turn", None),
            (vp, "_cancel_audio_stall_watch", None),
        ):
            self.stack.enter_context(mock.patch.object(owner, name, return_value=result))
        self.stack.enter_context(
            mock.patch.object(intercom, "handle_voice_turn", new=mock.AsyncMock(return_value=None))
        )
        self.stack.enter_context(
            mock.patch.object(intercom, "close_auto_reply_for_selector", new=mock.AsyncMock())
        )
        self.stack.enter_context(
            mock.patch.object(
                vp, "run_speech", new=mock.AsyncMock(return_value={"reason": "disabled"})
            )
        )
        self.stack.enter_context(
            mock.patch.object(vp, "_publish_conversation_display", new=mock.AsyncMock())
        )
        self.hydra = self.stack.enter_context(
            mock.patch.object(
                vp, "_run_hydra_turn_for_voice", new=mock.AsyncMock(return_value="Understood.")
            )
        )

    async def test_recognized_speech_reaches_hydra_without_word_or_duration_rules(self):
        for text in (
            "You're cool, dude. You know that?",
            "What's that?",
            "I like this.",
            "yes",
            "go",
            "turn on",
            "turn on the",
            "hmm",
            "是",
            "é",
            "こんにちは",
            "5",
        ):
            with self.subTest(text=text):
                self.hydra.reset_mock()
                turn = session(speech_duration_s=0.02)
                with mock.patch.object(
                    vp, "_native_transcribe_session_audio", new=mock.AsyncMock(return_value=text)
                ):
                    result = await vp._process_voice_turn(turn)
                self.assertEqual(result["transcript"], text)
                self.assertEqual(result["response_text"], "Understood.")
                self.hydra.assert_awaited_once_with(
                    transcript=text, conv_id=turn.conversation_id, session=turn
                )

    async def test_blank_or_punctuation_only_text_does_not_call_hydra(self):
        for text in ("", " ", "...", "?!", "—"):
            with self.subTest(text=text):
                self.hydra.reset_mock()
                with mock.patch.object(
                    vp, "_native_transcribe_session_audio", new=mock.AsyncMock(return_value=text)
                ):
                    result = await vp._process_voice_turn(session())
                self.assertTrue(result["no_op"])
                self.hydra.assert_not_awaited()

    async def test_no_speech_finishes_quietly_without_an_error_prompt(self):
        turn = session(audio_buffer=bytearray(32000))
        runtime = {"lock": asyncio.Lock(), "session": turn}
        with (
            mock.patch.object(vp, "_selector_runtime", return_value=runtime),
            mock.patch.object(
                vp, "_native_transcribe_session_audio", new=mock.AsyncMock(return_value="")
            ),
            mock.patch.object(vp, "_esphome_send_event", new=mock.AsyncMock()),
            mock.patch.object(vp, "_send_voice_no_text_recognized", new=mock.AsyncMock()) as error,
            mock.patch.object(vp, "_send_voice_run_end", new=mock.AsyncMock()) as finish,
        ):
            await vp._finalize_session(
                turn.selector,
                object(),
                object(),
                session_id=turn.session_id,
                abort=False,
                reason="server_vad",
            )
        finish.assert_awaited_once()
        error.assert_not_awaited()
        self.hydra.assert_not_awaited()
        self.assertIsNone(runtime["session"])

    async def test_detected_speech_with_two_blank_results_requests_a_repeat_only_after_retry(self):
        turn = session(audio_buffer=bytearray(b"\x01\x00" * 2000), speech_duration_s=0.02)
        runtime = {"lock": asyncio.Lock(), "session": turn}
        with (
            mock.patch.object(vp, "_selector_runtime", return_value=runtime),
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(
                backends,
                "_native_transcribe_local_audio_bytes",
                new=mock.AsyncMock(return_value=""),
            ) as retry,
            mock.patch.object(vp, "_esphome_send_event", new=mock.AsyncMock()),
            mock.patch.object(vp, "_send_voice_no_text_recognized", new=mock.AsyncMock()) as error,
            mock.patch.object(vp, "_send_voice_run_end", new=mock.AsyncMock()),
        ):
            await vp._finalize_session(
                turn.selector,
                object(),
                object(),
                session_id=turn.session_id,
                abort=False,
                reason="server_vad",
            )
            self.assertTrue(turn.stt_empty_retry_attempted)
        retry.assert_awaited_once()
        error.assert_awaited_once()
        self.hydra.assert_not_awaited()


class EmptyTranscriptRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_punctuation_only_results_are_empty_for_retry_and_silence_handling(self):
        for speech, expected in ((0.02, "Yes"), (0.0, "")):
            with self.subTest(speech=speech):
                turn = session(speech_duration_s=speech, audio_buffer=bytearray(32000))
                with (
                    mock.patch.object(
                        backends,
                        "_native_transcribe_session_audio_once",
                        new=mock.AsyncMock(return_value="..."),
                    ),
                    mock.patch.object(
                        backends,
                        "_native_transcribe_local_audio_bytes",
                        new=mock.AsyncMock(return_value="Yes"),
                    ) as retry,
                ):
                    self.assertEqual(
                        await backends._native_transcribe_session_audio(turn), expected
                    )
                self.assertEqual(retry.await_count, int(speech > 0))

    async def test_short_speech_retries_the_full_recording_with_the_selected_provider_once(self):
        turn = session(
            stt_backend="voxtral_realtime_llama_cpp",
            stt_backend_effective="voxtral_realtime_llama_cpp",
            speech_duration_s=0.02,
            audio_buffer=bytearray(b"\x01\x00" * 2000),
        )
        with (
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ) as first,
            mock.patch.object(
                backends,
                "_native_transcribe_local_audio_bytes",
                new=mock.AsyncMock(return_value="Yes."),
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "Yes.")
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "Yes.")
        first.assert_awaited_once()
        retry.assert_awaited_once_with(
            backend="voxtral_realtime_llama_cpp",
            audio_bytes=bytes(turn.audio_buffer),
            audio_format=turn.audio_format,
            language=turn.language,
            selector=turn.selector,
            session_id=turn.session_id,
            partial=False,
        )

    async def test_silence_and_successful_transcripts_do_not_retry(self):
        for text, speech in (("", 0.0), ("No.", 0.02)):
            with self.subTest(text=text):
                turn = session(audio_buffer=bytearray(32000), speech_duration_s=speech)
                with (
                    mock.patch.object(
                        backends,
                        "_native_transcribe_session_audio_once",
                        new=mock.AsyncMock(return_value=text),
                    ),
                    mock.patch.object(
                        backends, "_native_transcribe_local_audio_bytes", new=mock.AsyncMock()
                    ) as retry,
                ):
                    self.assertEqual(await backends._native_transcribe_session_audio(turn), text)
                retry.assert_not_awaited()

    async def test_a_second_empty_result_does_not_loop(self):
        turn = session(speech_duration_s=0.1, audio_buffer=bytearray(32000))
        with (
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ) as first,
            mock.patch.object(
                backends,
                "_native_transcribe_local_audio_bytes",
                new=mock.AsyncMock(return_value=""),
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "")
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "")
        first.assert_awaited_once()
        retry.assert_awaited_once()

    async def test_wyoming_retry_uses_its_whole_buffer_protocol(self):
        turn = session(
            stt_backend="wyoming",
            stt_backend_effective="wyoming",
            speech_duration_s=0.1,
            audio_buffer=bytearray(32000),
        )
        with (
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(backends, "spud_link_should_use_hub", return_value=False),
            mock.patch.object(
                backends,
                "_native_wyoming_transcribe_audio_bytes",
                new=mock.AsyncMock(return_value="Go"),
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "Go")
        retry.assert_awaited_once_with(bytes(turn.audio_buffer), turn.audio_format, turn.language)

    async def test_an_existing_stream_fallback_does_not_add_another_retry(self):
        turn = session(
            stt_stream_fallback_used=True, speech_duration_s=0.1, audio_buffer=bytearray(32000)
        )
        with (
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(
                backends, "_native_transcribe_local_audio_bytes", new=mock.AsyncMock()
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "")
        retry.assert_not_awaited()

    async def test_an_empty_stream_retry_recovers_a_real_short_command(self):
        turn = session(
            stt_backend="voxtral_realtime_llama_cpp",
            stt_backend_effective="voxtral_realtime_llama_cpp",
            speech_duration_s=0.02,
            audio_buffer=bytearray(b"\x01\x00" * 2000),
        )
        with (
            mock.patch.object(backends, "spud_link_should_use_hub", return_value=False),
            mock.patch.object(
                backends,
                "_native_transcribe_local_audio_bytes",
                new=mock.AsyncMock(return_value="What's that?"),
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "What's that?")
        retry.assert_awaited_once()

    async def test_retry_provider_failure_does_not_crash_the_voice_turn(self):
        turn = session(speech_duration_s=0.1, audio_buffer=bytearray(32000))
        with (
            mock.patch.object(
                backends,
                "_native_transcribe_session_audio_once",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(
                backends,
                "_native_transcribe_local_audio_bytes",
                new=mock.AsyncMock(side_effect=RuntimeError("provider unavailable")),
            ) as retry,
        ):
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "")
            self.assertEqual(await backends._native_transcribe_session_audio(turn), "")
        retry.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
