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


class Qwen3AsrSettingsTests(unittest.TestCase):
    def test_aliases_normalize(self) -> None:
        for value in ("qwen3-asr", "qwen3_asr_llama", "qwen-asr-llama-cpp"):
            self.assertEqual(
                speech_settings._normalize_stt_backend(value),
                "qwen3_asr_llama_cpp",
            )
            self.assertEqual(vp._normalize_stt_backend(value), "qwen3_asr_llama_cpp")

    def test_backend_is_selectable_and_marked_experimental(self) -> None:
        rows = speech_settings._stt_backend_option_rows()
        row = next(item for item in rows if item["value"] == "qwen3_asr_llama_cpp")
        self.assertIn("Experimental", row["label"])


class Qwen3AsrBackendTests(unittest.TestCase):
    def tearDown(self) -> None:
        with backends._QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK:
            backends._QWEN3_ASR_LLAMA_CPP_STATE.update(
                {
                    "process": None,
                    "base_url": "",
                    "model_path": "",
                    "mmproj_path": "",
                    "api_key": "",
                    "stdout_tail": [],
                    "stderr_tail": [],
                    "started_ts": 0.0,
                    "gpu_layers": 0,
                }
            )

    def test_model_paths_download_expected_q8_files(self) -> None:
        hub = SimpleNamespace(snapshot_download=mock.Mock())
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = pathlib.Path(temp_dir, vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MODEL_FILE)
            mmproj_path = pathlib.Path(temp_dir, vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MMPROJ_FILE)

            def download(**_kwargs):
                model_path.touch()
                mmproj_path.touch()

            hub.snapshot_download.side_effect = download
            with (
                mock.patch.object(vp, "_ensure_stt_backend_model_root", return_value=temp_dir),
                mock.patch.object(backends.importlib, "import_module", return_value=hub),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
            ):
                resolved = backends._qwen3_asr_llama_cpp_model_paths(download=True)

        self.assertEqual(resolved, (str(model_path), str(mmproj_path)))
        hub.snapshot_download.assert_called_once_with(
            repo_id=vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_REPO,
            local_dir=temp_dir,
            allow_patterns=[
                vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MODEL_FILE,
                vp.DEFAULT_QWEN3_ASR_LLAMA_CPP_MMPROJ_FILE,
            ],
        )

    def test_transcriber_posts_wav_and_strips_qwen_prefix(self) -> None:
        response = SimpleNamespace(
            status_code=200,
            json=lambda: {
                "type": "transcript.text.done",
                "text": "language English<asr_text>Turn on the kitchen lights.<|im_end|>",
            },
        )
        pcm = b"\x00\x00" * 160
        with (
            mock.patch.object(vp, "_pcm_to_pcm16_mono_16k", return_value=(pcm, None)),
            mock.patch.object(
                backends,
                "_load_qwen3_asr_llama_cpp_server",
                return_value={"base_url": "http://127.0.0.1:12345"},
            ),
            mock.patch.object(backends.requests, "post", return_value=response) as post,
        ):
            text = backends._transcribe_qwen3_asr_llama_cpp_sync(
                pcm,
                {"rate": 16000, "width": 2, "channels": 1},
                "en",
            )

        self.assertEqual(text, "Turn on the kitchen lights.")
        self.assertEqual(post.call_args.args[0], "http://127.0.0.1:12345/v1/audio/transcriptions")
        self.assertEqual(post.call_args.kwargs["data"]["language"], "en")
        wav_payload = post.call_args.kwargs["files"]["file"][1]
        self.assertEqual(wav_payload[:4], b"RIFF")
        self.assertEqual(wav_payload[8:12], b"WAVE")

    def test_cache_clear_stops_dedicated_server(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.pid = 123
        with backends._QWEN3_ASR_LLAMA_CPP_PROCESS_LOCK:
            backends._QWEN3_ASR_LLAMA_CPP_STATE["process"] = proc
        cleared = backends.clear_stt_model_caches(keep_backend="parakeet_onnx")
        self.assertEqual(cleared["qwen3_asr_llama_cpp"], 1)
        proc.terminate.assert_called_once()


class Qwen3AsrRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_local_transcription_routes_to_qwen(self) -> None:
        runner = mock.AsyncMock(return_value="Set a timer for five minutes.")
        with mock.patch.object(backends, "_run_local_stt_thread", new=runner):
            result = await backends._native_transcribe_local_audio_bytes(
                backend="qwen3_asr_llama_cpp",
                audio_bytes=b"\x00\x00" * 100,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                selector="native:test",
                session_id="session:test",
                partial=False,
            )

        self.assertEqual(result, "Set a timer for five minutes.")
        self.assertIs(
            runner.await_args.args[0],
            backends._transcribe_qwen3_asr_llama_cpp_sync,
        )


class Qwen3AsrWarmupTests(unittest.TestCase):
    def test_warmup_starts_dedicated_server(self) -> None:
        import tateros_app

        with (
            mock.patch.object(vp, "_stt_backend_available", return_value=(True, "")),
            mock.patch.object(
                vp,
                "_load_qwen3_asr_llama_cpp_server",
                return_value={"pid": 456},
            ) as load,
        ):
            message = tateros_app._warm_speech_model_item(
                {"kind": "stt", "backend": "qwen3_asr_llama_cpp"}
            )

        self.assertIn("pid=456", message)
        load.assert_called_once()

    def test_running_server_is_in_loaded_model_entries(self) -> None:
        import tateros_app

        runtime = {
            "running": True,
            "pid": 456,
            "model_path": "/models/Qwen3-ASR-0.6B-Q8_0.gguf",
            "mmproj_path": "/models/mmproj-Qwen3-ASR-0.6B-Q8_0.gguf",
            "started_ts": 123.0,
            "gpu_layers": 999,
        }
        with (
            mock.patch.object(vp, "_qwen3_asr_llama_cpp_runtime_snapshot", return_value=runtime),
            mock.patch.object(tateros_app, "_runtime_path_bytes", return_value=1_000_000_000),
        ):
            rows = tateros_app._runtime_voice_pipeline_model_rows()

        row = next(item for item in rows if item["provider"] == "voice_stt_qwen3_asr_llama_cpp")
        self.assertEqual(row["category"], "stt")
        self.assertEqual(row["kind_label"], "STT")
        self.assertEqual(row["device"], "Metal")
        self.assertEqual(row["memory_kind"], "unified")
        self.assertEqual(row["estimated_bytes"], 1_000_000_000)
        self.assertEqual(row["loaded_ts"], 123.0)
        self.assertIn("Projector mmproj-Qwen3-ASR-0.6B-Q8_0.gguf", row["details"])
        self.assertIn("PID 456", row["details"])


if __name__ == "__main__":
    unittest.main()
