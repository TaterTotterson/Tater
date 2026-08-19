#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import native_live_settings, wake_verifier  # noqa: E402
from tater_voice.wake_verifier import (  # noqa: E402
    build_packet,
    is_wake_verifier_packet,
    parse_packet,
    transcript_match_score,
)


class WakeVerifierPacketTests(unittest.TestCase):
    def test_pcm_packet_round_trip(self) -> None:
        pcm = b"\x01\x02" * 16000
        packet = build_packet(pcm, request_id=42, enforce=True)
        self.assertTrue(is_wake_verifier_packet(packet))
        parsed = parse_packet(packet)
        self.assertEqual(parsed["request_id"], 42)
        self.assertEqual(parsed["sample_count"], 16000)
        self.assertTrue(parsed["enforce"])
        self.assertEqual(parsed["pcm"], pcm)

    def test_packet_size_mismatch_is_rejected(self) -> None:
        packet = build_packet(b"\x00\x00" * 8000, request_id=7)
        with self.assertRaisesRegex(ValueError, "size mismatch"):
            parse_packet(packet[:-2])


class WakeVerifierMatcherTests(unittest.TestCase):
    def test_exact_phrase_inside_short_transcript(self) -> None:
        self.assertEqual(transcript_match_score("Okay. Hey Tater!", "hey_tater"), 1.0)

    def test_known_stt_variant_clears_default_threshold(self) -> None:
        self.assertGreaterEqual(transcript_match_score("Hey, Dater.", "Hey Tater"), 0.85)

    def test_known_false_variant_stays_below_default_threshold(self) -> None:
        self.assertLess(transcript_match_score("Hey, Tanner.", "Hey Tater"), 0.85)

    def test_unrelated_speech_does_not_match(self) -> None:
        self.assertLess(transcript_match_score("That's cool, thank you.", "Hey Tater"), 0.5)


class WakeVerifierBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_verifier_uses_selected_qwen_backend(self) -> None:
        from tater_voice import voice_pipeline as vp

        transcribe = mock.AsyncMock(return_value="Hey Tater")
        packet = build_packet(b"\x00\x00" * 8000, request_id=90, enforce=True)
        with (
            mock.patch.object(
                native_live_settings,
                "settings_snapshot",
                return_value={
                    "wake_word": "hey_tater",
                    "wake_verifier_threshold": 0.85,
                    "wake_verifier_timeout_ms": 500,
                },
            ),
            mock.patch.object(vp, "_selected_stt_backend", return_value="qwen3_asr_llama_cpp"),
            mock.patch.object(
                vp,
                "_resolve_stt_backend",
                return_value=("qwen3_asr_llama_cpp", ""),
            ),
            mock.patch.object(vp, "_native_transcribe_wake_audio_bytes", new=transcribe),
        ):
            result = await wake_verifier.verify_packet(packet, selector="native:test")

        self.assertTrue(result["accepted"])
        self.assertTrue(result["available"])
        self.assertEqual(result["stt_engine_selected"], "qwen3_asr_llama_cpp")
        self.assertEqual(result["stt_engine"], "qwen3_asr_llama_cpp")
        transcribe.assert_awaited_once_with(
            backend="qwen3_asr_llama_cpp",
            audio_bytes=b"\x00\x00" * 8000,
            audio_format={"rate": 16000, "width": 2, "channels": 1},
            language="en",
            selector="native:test",
        )

    async def test_verifier_uses_selected_faster_whisper_backend(self) -> None:
        from tater_voice import voice_pipeline as vp

        transcribe = mock.AsyncMock(return_value="Hey Tater")
        packet = build_packet(b"\x00\x00" * 8000, request_id=91, enforce=True)
        with (
            mock.patch.object(
                native_live_settings,
                "settings_snapshot",
                return_value={
                    "wake_word": "hey_tater",
                    "wake_verifier_threshold": 0.85,
                    "wake_verifier_timeout_ms": 500,
                },
            ),
            mock.patch.object(vp, "_selected_stt_backend", return_value="faster_whisper"),
            mock.patch.object(vp, "_resolve_stt_backend", return_value=("faster_whisper", "")),
            mock.patch.object(vp, "_native_transcribe_wake_audio_bytes", new=transcribe),
        ):
            result = await wake_verifier.verify_packet(packet, selector="native:test")

        self.assertTrue(result["accepted"])
        self.assertTrue(result["available"])
        self.assertEqual(result["stt_engine_selected"], "faster_whisper")
        self.assertEqual(result["stt_engine"], "faster_whisper")
        transcribe.assert_awaited_once_with(
            backend="faster_whisper",
            audio_bytes=b"\x00\x00" * 8000,
            audio_format={"rate": 16000, "width": 2, "channels": 1},
            language="en",
            selector="native:test",
        )

    async def test_verifier_reports_effective_backend_fallback(self) -> None:
        from tater_voice import voice_pipeline as vp

        packet = build_packet(b"\x00\x00" * 4000, request_id=92)
        fallback_reason = "mlx_whisper unavailable; falling back to faster_whisper"
        with (
            mock.patch.object(
                native_live_settings,
                "settings_snapshot",
                return_value={"wake_word": "hey_tater"},
            ),
            mock.patch.object(vp, "_selected_stt_backend", return_value="mlx_whisper"),
            mock.patch.object(
                vp,
                "_resolve_stt_backend",
                return_value=("faster_whisper", fallback_reason),
            ),
            mock.patch.object(
                vp,
                "_native_transcribe_wake_audio_bytes",
                new=mock.AsyncMock(return_value="Hey Tater"),
            ),
        ):
            result = await wake_verifier.verify_packet(packet)

        self.assertEqual(result["stt_engine_selected"], "mlx_whisper")
        self.assertEqual(result["stt_engine"], "faster_whisper")
        self.assertEqual(result["stt_fallback_reason"], fallback_reason)

    async def test_backend_failure_still_fails_open(self) -> None:
        from tater_voice import voice_pipeline as vp

        packet = build_packet(b"\x00\x00" * 4000, request_id=93, enforce=True)
        with (
            mock.patch.object(
                native_live_settings,
                "settings_snapshot",
                return_value={"wake_word": "hey_tater"},
            ),
            mock.patch.object(vp, "_selected_stt_backend", return_value="wyoming"),
            mock.patch.object(
                vp,
                "_resolve_stt_backend",
                return_value=("wyoming", "Wyoming is unavailable"),
            ),
            mock.patch.object(
                vp,
                "_native_transcribe_wake_audio_bytes",
                new=mock.AsyncMock(side_effect=RuntimeError("connection refused")),
            ),
        ):
            result = await wake_verifier.verify_packet(packet)

        self.assertTrue(result["accepted"])
        self.assertFalse(result["available"])
        self.assertEqual(result["stt_engine"], "wyoming")
        self.assertIn("connection refused", result["reason"])

    async def test_local_wake_router_uses_fast_faster_whisper_mode(self) -> None:
        from tater_voice.voice_pipeline import backends

        runner = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="faster_whisper",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        call_args = runner.await_args.args
        self.assertIs(call_args[0], backends._transcribe_faster_whisper_sync)
        self.assertEqual(call_args[-2:], (True, True))

    async def test_local_wake_router_uses_optimized_mlx_path(self) -> None:
        from tater_voice.voice_pipeline import backends

        runner = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="mlx_whisper",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        self.assertIs(
            runner.await_args.args[0],
            backends._transcribe_mlx_whisper_wake_sync,
        )

    async def test_local_wake_router_uses_parakeet_onnx_path(self) -> None:
        from tater_voice.voice_pipeline import backends

        runner = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="parakeet_onnx",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        self.assertIs(runner.await_args.args[0], backends._transcribe_parakeet_onnx_sync)
        self.assertTrue(runner.await_args.args[-1])

    async def test_local_wake_router_uses_qwen_path(self) -> None:
        from tater_voice.voice_pipeline import backends

        runner = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="qwen3_asr_llama_cpp",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        self.assertIs(
            runner.await_args.args[0],
            backends._transcribe_qwen3_asr_llama_cpp_sync,
        )
        self.assertTrue(runner.await_args.args[-1])

    async def test_local_wake_router_uses_vosk_path(self) -> None:
        from tater_voice.voice_pipeline import backends

        runner = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="vosk",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        self.assertIs(runner.await_args.args[0], backends._transcribe_vosk_sync)

    async def test_wyoming_wake_router_uses_one_shot_transcription(self) -> None:
        from tater_voice.voice_pipeline import backends

        transcribe = mock.AsyncMock(return_value="Hey Tater")
        with mock.patch.object(
            backends,
            "_native_wyoming_transcribe_audio_bytes",
            new=transcribe,
        ):
            transcript = await backends._native_transcribe_wake_audio_bytes(
                backend="wyoming",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
            )

        self.assertEqual(transcript, "Hey Tater")
        transcribe.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
