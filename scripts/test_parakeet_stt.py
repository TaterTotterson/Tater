#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import speech_settings  # noqa: E402
from tater_voice import voice_pipeline as vp  # noqa: E402
from tater_voice.voice_pipeline import backends  # noqa: E402


class ParakeetSettingsTests(unittest.TestCase):
    def test_parakeet_aliases_normalize(self) -> None:
        for value in ("parakeet", "parakeet-onnx", "onnx_parakeet"):
            self.assertEqual(speech_settings._normalize_stt_backend(value), "parakeet_onnx")
            self.assertEqual(vp._normalize_stt_backend(value), "parakeet_onnx")

    def test_parakeet_is_a_selectable_stt_backend(self) -> None:
        rows = speech_settings._stt_backend_option_rows()
        row = next(item for item in rows if item["value"] == "parakeet_onnx")
        self.assertEqual(row["label"], "Parakeet ONNX")

    def test_provider_selection_follows_voice_acceleration(self) -> None:
        ort = SimpleNamespace(
            get_available_providers=lambda: [
                "CoreMLExecutionProvider",
                "CUDAExecutionProvider",
                "CPUExecutionProvider",
            ]
        )
        with (
            mock.patch.object(vp, "_effective_speech_acceleration", return_value="cuda"),
            mock.patch.object(vp.importlib, "import_module", return_value=ort),
        ):
            self.assertEqual(
                vp._parakeet_onnx_providers(),
                ["CUDAExecutionProvider", "CPUExecutionProvider"],
            )

        with (
            mock.patch.object(vp, "_effective_speech_acceleration", return_value="mps"),
            mock.patch.object(vp.importlib, "import_module", return_value=ort),
        ):
            self.assertEqual(
                vp._parakeet_onnx_providers(),
                ["CoreMLExecutionProvider", "CPUExecutionProvider"],
            )


class ParakeetBackendTests(unittest.TestCase):
    def tearDown(self) -> None:
        with vp._parakeet_onnx_model_lock:
            vp._parakeet_onnx_model_cache.clear()

    def test_loader_downloads_int8_to_tater_model_root_and_caches(self) -> None:
        fake_model = object()
        onnx_asr = SimpleNamespace(load_model=mock.Mock(return_value=fake_model))
        huggingface_hub = SimpleNamespace(snapshot_download=mock.Mock())
        with tempfile.TemporaryDirectory() as temp_dir:
            huggingface_hub.snapshot_download.return_value = temp_dir
            with (
                mock.patch.object(vp, "OnnxASR", onnx_asr),
                mock.patch.object(vp, "_parakeet_onnx_quantization", return_value="int8"),
                mock.patch.object(
                    vp,
                    "_parakeet_onnx_providers",
                    return_value=["CPUExecutionProvider"],
                ),
                mock.patch.object(
                    vp,
                    "_ensure_stt_backend_model_root",
                    return_value=temp_dir,
                ),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
                mock.patch.object(
                    backends.importlib,
                    "import_module",
                    return_value=huggingface_hub,
                ),
            ):
                first = backends._load_parakeet_onnx_model()
                second = backends._load_parakeet_onnx_model()

        self.assertIs(first, fake_model)
        self.assertIs(second, fake_model)
        huggingface_hub.snapshot_download.assert_called_once_with(
            repo_id=vp.DEFAULT_PARAKEET_ONNX_REPO,
            local_dir=temp_dir,
            allow_patterns=[
                "config.json",
                "vocab.txt",
                "encoder-model.int8.onnx",
                "encoder-model.int8.onnx.data",
                "decoder_joint-model.int8.onnx",
                "decoder_joint-model.int8.onnx.data",
            ],
        )
        onnx_asr.load_model.assert_called_once_with(
            vp.DEFAULT_PARAKEET_ONNX_MODEL,
            temp_dir,
            quantization="int8",
            providers=["CPUExecutionProvider"],
        )

    def test_loader_reuses_complete_local_snapshot_without_hub_access(self) -> None:
        fake_model = object()
        onnx_asr = SimpleNamespace(load_model=mock.Mock(return_value=fake_model))
        with tempfile.TemporaryDirectory() as temp_dir:
            for filename in (
                "config.json",
                "vocab.txt",
                "encoder-model.int8.onnx",
                "decoder_joint-model.int8.onnx",
            ):
                pathlib.Path(temp_dir, filename).touch()
            with (
                mock.patch.object(vp, "OnnxASR", onnx_asr),
                mock.patch.object(vp, "_parakeet_onnx_quantization", return_value="int8"),
                mock.patch.object(
                    vp,
                    "_parakeet_onnx_providers",
                    return_value=["CPUExecutionProvider"],
                ),
                mock.patch.object(
                    vp,
                    "_ensure_stt_backend_model_root",
                    return_value=temp_dir,
                ),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
                mock.patch.object(backends.importlib, "import_module") as import_module,
            ):
                loaded = backends._load_parakeet_onnx_model()

        self.assertIs(loaded, fake_model)
        import_module.assert_not_called()
        onnx_asr.load_model.assert_called_once_with(
            vp.DEFAULT_PARAKEET_ONNX_MODEL,
            temp_dir,
            quantization="int8",
            providers=["CPUExecutionProvider"],
        )

    def test_transcriber_passes_float_audio_to_cached_model(self) -> None:
        model = SimpleNamespace(recognize=mock.Mock(return_value="  Hey   Tater.  "))
        pcm = b"\x00\x00\xff\x7f"
        with (
            mock.patch.object(vp, "_pcm_to_pcm16_mono_16k", return_value=(pcm, None)),
            mock.patch.object(backends, "_load_parakeet_onnx_model", return_value=model),
        ):
            result = backends._transcribe_parakeet_onnx_sync(
                pcm,
                {"rate": 16000, "width": 2, "channels": 1},
                "en",
            )

        self.assertEqual(result, "Hey Tater.")
        args, kwargs = model.recognize.call_args
        self.assertEqual(args[0].dtype.name, "float32")
        self.assertEqual(kwargs["sample_rate"], 16000)
        self.assertEqual(kwargs["channel"], "mean")
        self.assertEqual(kwargs["language"], "en")

    def test_cache_clear_keeps_selected_backend_only(self) -> None:
        with vp._faster_whisper_model_lock:
            vp._faster_whisper_model_cache[("base.en", "cpu", "int8")] = object()
        with vp._parakeet_onnx_model_lock:
            vp._parakeet_onnx_model_cache[
                ("nemo-parakeet-tdt-0.6b-v3", "int8", ("CPUExecutionProvider",))
            ] = object()
        with vp._vosk_model_lock:
            vp._vosk_model_cache["/tmp/vosk-test"] = object()

        cleared = backends.clear_stt_model_caches(keep_backend="parakeet_onnx")

        self.assertEqual(cleared["faster_whisper"], 1)
        self.assertEqual(cleared["parakeet_onnx"], 0)
        self.assertEqual(cleared["vosk"], 1)
        self.assertFalse(vp._faster_whisper_model_cache)
        self.assertTrue(vp._parakeet_onnx_model_cache)
        self.assertFalse(vp._vosk_model_cache)


class ParakeetRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_local_transcription_routes_to_parakeet(self) -> None:
        runner = mock.AsyncMock(return_value="Turn on the kitchen lights.")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            result = await backends._native_transcribe_local_audio_bytes(
                backend="parakeet_onnx",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
                session_id="session:test",
                partial=False,
            )

        self.assertEqual(result, "Turn on the kitchen lights.")
        self.assertIs(runner.await_args.args[0], backends._transcribe_parakeet_onnx_sync)
        self.assertFalse(runner.await_args.args[-1])


class ParakeetWarmupTests(unittest.TestCase):
    def test_speech_warmup_runs_a_parakeet_decode(self) -> None:
        import tateros_app

        transcribe = mock.Mock(return_value="")
        with (
            mock.patch.object(vp, "_stt_backend_available", return_value=(True, "")),
            mock.patch.object(vp, "_transcribe_parakeet_onnx_sync", new=transcribe),
            mock.patch.object(
                vp,
                "_parakeet_onnx_providers",
                return_value=["CPUExecutionProvider"],
            ),
        ):
            message = tateros_app._warm_speech_model_item(
                {"kind": "stt", "backend": "parakeet_onnx"}
            )

        self.assertIn("warmed ONNX decode", message)
        transcribe.assert_called_once()
        self.assertEqual(len(transcribe.call_args.args[0]), 16000 * 2)

    def test_running_warmup_queues_latest_stt_selection(self) -> None:
        import tateros_app

        with tateros_app.speech_model_warmup_lock:
            original = dict(tateros_app.speech_model_warmup_state)
            tateros_app.speech_model_warmup_state.update(
                {
                    "running": True,
                    "pending_settings": {},
                    "pending_reason": "",
                }
            )
        try:
            snapshot = tateros_app._start_speech_model_warmup(
                {"stt_backend": "parakeet_onnx"},
                reason="settings-save",
            )
            self.assertTrue(snapshot["already_running"])
            self.assertTrue(snapshot["queued"])
            with tateros_app.speech_model_warmup_lock:
                self.assertEqual(
                    tateros_app.speech_model_warmup_state["pending_settings"]["stt_backend"],
                    "parakeet_onnx",
                )
        finally:
            with tateros_app.speech_model_warmup_lock:
                tateros_app.speech_model_warmup_state.clear()
                tateros_app.speech_model_warmup_state.update(original)


if __name__ == "__main__":
    unittest.main()
