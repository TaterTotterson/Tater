#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import unittest
import wave
from pathlib import Path
from unittest import mock
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import managed_tts  # noqa: E402
import managed_tts_worker  # noqa: E402


class ManagedTtsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.path_patch = mock.patch.object(
            managed_tts,
            "agent_lab_path",
            side_effect=lambda *parts: self.root.joinpath(*parts),
        )
        self.path_patch.start()

    def tearDown(self) -> None:
        managed_tts.clear_managed_tts_workers()
        self.path_patch.stop()
        self.temp_dir.cleanup()

    def test_backend_aliases_are_normalized(self) -> None:
        for value, expected in (
            ("qwen", "qwen3_tts"),
            ("Qwen3-TTS", "qwen3_tts"),
            ("omni voice", "omnivoice"),
        ):
            self.assertEqual(managed_tts.normalize_managed_tts_backend(value), expected)

    def test_clone_audio_is_kept_inside_backend_profile(self) -> None:
        path = managed_tts.store_clone_audio(
            "qwen3_tts",
            filename="My Voice.WAV",
            data=b"RIFF-reference-data",
        )
        self.assertEqual(Path(path).name, "reference.wav")
        self.assertEqual(managed_tts.validate_clone_audio_path("qwen3_tts", path), path)
        self.assertEqual(
            managed_tts.clone_audio_info("qwen3_tts", path),
            {"configured": True, "name": "reference.wav", "size": 19},
        )
        with self.assertRaises(ValueError):
            managed_tts.validate_clone_audio_path("qwen3_tts", self.root / "outside.wav")

    def test_direct_and_announcement_clone_audio_are_independent(self) -> None:
        direct_path = managed_tts.store_clone_audio(
            "omnivoice",
            filename="direct.wav",
            data=b"RIFF-direct",
        )
        announcement_path = managed_tts.store_clone_audio(
            "omnivoice",
            filename="announcement.wav",
            data=b"RIFF-announcement",
            profile="announcement",
        )

        self.assertEqual(Path(direct_path).name, "reference.wav")
        self.assertEqual(Path(announcement_path).name, "announcement-reference.wav")
        self.assertEqual(Path(direct_path).read_bytes(), b"RIFF-direct")
        self.assertEqual(Path(announcement_path).read_bytes(), b"RIFF-announcement")

        self.assertTrue(
            managed_tts.remove_clone_audio(
                "omnivoice",
                announcement_path,
                profile="announcement",
            )
        )
        self.assertTrue(Path(direct_path).is_file())
        self.assertFalse(Path(announcement_path).exists())

    def test_qwen_clone_requires_audio_but_transcript_is_optional(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "reference audio"):
            managed_tts.synthesize_managed_tts_pcm(
                "Hello",
                backend="qwen3_tts",
                model=managed_tts.DEFAULT_QWEN_TTS_MODEL,
            )

        clone_path = managed_tts.store_clone_audio(
            "qwen3_tts",
            filename="voice.wav",
            data=b"RIFF-reference-data",
        )
        worker = managed_tts._workers[managed_tts.QWEN_TTS_BACKEND]
        sent = {}

        def fake_request(payload, *, acceleration, timeout):
            sent.update(payload)
            output = Path(payload["output_path"])
            with wave.open(str(output), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(24000)
                wav_file.writeframes(b"\x01\x00")
            return {"ok": True, "output_path": str(output), "sample_rate": 24000}

        with mock.patch.object(worker, "request", side_effect=fake_request):
            pcm, _audio_format = managed_tts.synthesize_managed_tts_pcm(
                "Hello",
                backend="qwen3_tts",
                model=managed_tts.DEFAULT_QWEN_TTS_MODEL,
                clone_audio=clone_path,
            )
        self.assertEqual(pcm, b"\x01\x00")
        self.assertEqual(sent["clone_text"], "")

    def test_clone_wav_can_be_decoded_for_automatic_transcription(self) -> None:
        wav_path = self.root / "source.wav"
        with wave.open(str(wav_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(16000)
            wav_file.writeframes(b"\x01\x00\x02\x00")
        clone_path = managed_tts.store_clone_audio(
            "omnivoice",
            filename="source.wav",
            data=wav_path.read_bytes(),
        )

        pcm, audio_format = managed_tts.decode_clone_audio_pcm("omnivoice", clone_path)

        self.assertEqual(pcm, b"\x01\x00\x02\x00")
        self.assertEqual(audio_format, {"rate": 16000, "width": 2, "channels": 1})

    def test_worker_wav_is_returned_as_pcm_and_removed(self) -> None:
        clone_path = managed_tts.store_clone_audio(
            "qwen3_tts",
            filename="voice.wav",
            data=b"RIFF-reference-data",
        )
        worker = managed_tts._workers[managed_tts.QWEN_TTS_BACKEND]

        def fake_request(payload, *, acceleration, timeout):
            output = Path(payload["output_path"])
            with wave.open(str(output), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(24000)
                wav_file.writeframes(b"\x01\x00\x02\x00")
            return {"ok": True, "output_path": str(output), "sample_rate": 24000}

        with mock.patch.object(worker, "request", side_effect=fake_request):
            pcm, audio_format = managed_tts.synthesize_managed_tts_pcm(
                "Hello",
                backend="qwen3_tts",
                model=managed_tts.DEFAULT_QWEN_TTS_MODEL,
                clone_audio=clone_path,
                clone_text="This is my voice.",
                language="English",
            )

        self.assertEqual(pcm, b"\x01\x00\x02\x00")
        self.assertEqual(audio_format, {"rate": 24000, "width": 2, "channels": 1})
        self.assertEqual(list((managed_tts.managed_tts_root("qwen3_tts") / "output").glob("*.wav")), [])

    def test_snapshot_only_reports_models_loaded_in_live_workers(self) -> None:
        worker = managed_tts._workers[managed_tts.QWEN_TTS_BACKEND]
        process = mock.Mock()
        process.poll.return_value = None
        process.pid = 4321
        worker.process = process
        worker.acceleration = "mps"
        worker.loaded_models = {
            managed_tts.DEFAULT_QWEN_TTS_MODEL: {
                "model": managed_tts.DEFAULT_QWEN_TTS_MODEL,
                "loaded_ts": 123.0,
                "device": "MLX / Metal",
                "estimated_bytes": 456,
            }
        }

        snapshot = managed_tts.managed_tts_workers_snapshot()

        self.assertEqual(snapshot["loaded_count"], 1)
        self.assertEqual(snapshot["models"][0]["pid"], 4321)
        self.assertEqual(snapshot["models"][0]["model"], managed_tts.DEFAULT_QWEN_TTS_MODEL)
        self.assertEqual(snapshot["models"][0]["estimated_bytes"], 456)

        process.poll.return_value = 1
        self.assertEqual(managed_tts.managed_tts_workers_snapshot()["loaded_count"], 0)

    def test_warm_model_requires_worker_to_report_actual_loaded_state(self) -> None:
        worker = managed_tts._workers[managed_tts.OMNIVOICE_TTS_BACKEND]
        process = mock.Mock()
        process.poll.return_value = None
        process.pid = 7654

        def fake_request(payload, *, acceleration, timeout):
            model = str(payload["model"])
            worker.process = process
            worker.acceleration = acceleration
            worker.loaded_models[model] = {
                "model": model,
                "loaded_ts": 321.0,
                "device": "cuda:0",
                "estimated_bytes": 654,
            }
            return {"ok": True, "loaded": True, "model": model}

        with mock.patch.object(worker, "request", side_effect=fake_request):
            loaded = managed_tts.warm_managed_tts_model(
                backend="omnivoice",
                model="example/omni",
                acceleration="cuda",
            )

        self.assertTrue(loaded["ok"])
        self.assertEqual(loaded["model"], "example/omni")
        self.assertEqual(loaded["pid"], 7654)
        self.assertEqual(loaded["estimated_bytes"], 654)

    def test_qwen_worker_uses_embedding_only_fallback_without_transcript(self) -> None:
        clone_path = managed_tts.store_clone_audio(
            "qwen3_tts",
            filename="voice.wav",
            data=b"RIFF-reference-data",
        )

        class FakeQwenModel:
            def __init__(self) -> None:
                self.prompt_args = {}

            def create_voice_clone_prompt(self, **kwargs):
                self.prompt_args = kwargs
                return "clone-prompt"

            def generate_voice_clone(self, **kwargs):
                return [b"audio"], 24000

        model = FakeQwenModel()
        managed_tts_worker._clone_prompts.clear()
        with (
            mock.patch.object(managed_tts_worker, "_apple_silicon", return_value=False),
            mock.patch.object(managed_tts_worker, "_qwen_model", return_value=model),
        ):
            _audio, sample_rate = managed_tts_worker._generate_qwen(
                {
                    "model": managed_tts.DEFAULT_QWEN_TTS_MODEL,
                    "text": "Hello",
                    "clone_audio": clone_path,
                    "clone_text": "",
                    "language": "English",
                }
            )

        self.assertEqual(sample_rate, 24000)
        self.assertIsNone(model.prompt_args["ref_text"])
        self.assertTrue(model.prompt_args["x_vector_only_mode"])

    def test_omnivoice_worker_delegates_missing_transcript_to_model(self) -> None:
        clone_path = managed_tts.store_clone_audio(
            "omnivoice",
            filename="voice.wav",
            data=b"RIFF-reference-data",
        )

        class FakeOmniVoiceModel:
            sampling_rate = 24000

            def __init__(self) -> None:
                self.prompt_args = {}

            def create_voice_clone_prompt(self, **kwargs):
                self.prompt_args = kwargs
                return "clone-prompt"

            def generate(self, **kwargs):
                return [b"audio"]

        model = FakeOmniVoiceModel()
        managed_tts_worker._clone_prompts.clear()
        with mock.patch.object(managed_tts_worker, "_omnivoice_model", return_value=model):
            _audio, sample_rate = managed_tts_worker._generate_omnivoice(
                {
                    "model": managed_tts.DEFAULT_OMNIVOICE_TTS_MODEL,
                    "text": "Hello",
                    "clone_audio": clone_path,
                    "clone_text": "",
                    "language": "English",
                }
            )

        self.assertEqual(sample_rate, 24000)
        self.assertIsNone(model.prompt_args["ref_text"])


if __name__ == "__main__":
    unittest.main()
