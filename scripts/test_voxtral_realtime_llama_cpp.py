#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
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


class _FakeRealtimeSocket:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.commit_sent = asyncio.Event()
        self.recv_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def send(self, payload: str) -> None:
        event = json.loads(payload)
        self.sent.append(event)
        if event.get("type") == "input_audio_buffer.commit":
            self.commit_sent.set()

    async def recv(self) -> str:
        self.recv_count += 1
        if self.recv_count == 1:
            return json.dumps(
                {
                    "type": "session.created",
                    "session": {"audio": {"input": {"format": {"rate": 16000}}}},
                }
            )
        if self.recv_count == 2:
            return json.dumps({"type": "session.updated"})
        await self.commit_sent.wait()
        if self.recv_count == 3:
            return json.dumps(
                {
                    "type": "conversation.item.input_audio_transcription.delta",
                    "delta": "Turn on ",
                }
            )
        if self.recv_count == 4:
            return json.dumps(
                {
                    "type": "conversation.item.input_audio_transcription.delta",
                    "delta": "the kitchen lights",
                }
            )
        return json.dumps(
            {
                "type": "conversation.item.input_audio_transcription.completed",
                "transcript": "Turn on the kitchen lights.",
            }
        )


class _FakePersistentRealtimeSocket:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.recv_count = 0
        self.pending_commits = 0
        self.completed = 0
        self.close_code = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.close_code = 1000
        return False

    async def send(self, payload: str) -> None:
        event = json.loads(payload)
        self.sent.append(event)
        if event.get("type") == "input_audio_buffer.commit":
            self.pending_commits += 1

    async def recv(self) -> str:
        self.recv_count += 1
        if self.recv_count == 1:
            return json.dumps(
                {
                    "type": "session.created",
                    "session": {"audio": {"input": {"format": {"rate": 16000}}}},
                }
            )
        if self.recv_count == 2:
            return json.dumps({"type": "session.updated"})
        if self.pending_commits <= 0:
            await asyncio.sleep(0)
        self.pending_commits -= 1
        self.completed += 1
        return json.dumps(
            {
                "type": "conversation.item.input_audio_transcription.completed",
                "transcript": f"hey tater {self.completed}",
            }
        )


class VoxtralRealtimeSettingsTests(unittest.TestCase):
    def test_aliases_normalize(self) -> None:
        for value in ("voxtral", "voxtral-realtime", "voxtral-realtime-llama-cpp"):
            self.assertEqual(
                speech_settings._normalize_stt_backend(value),
                "voxtral_realtime_llama_cpp",
            )
            self.assertEqual(vp._normalize_stt_backend(value), "voxtral_realtime_llama_cpp")

    def test_backend_is_selectable_and_marked_experimental(self) -> None:
        rows = speech_settings._stt_backend_option_rows()
        row = next(item for item in rows if item["value"] == "voxtral_realtime_llama_cpp")
        self.assertIn("Voxtral Realtime", row["label"])
        self.assertIn("Experimental", row["label"])


class VoxtralRealtimeBackendTests(unittest.TestCase):
    def tearDown(self) -> None:
        with backends._VOXTRAL_REALTIME_LLAMA_CPP_PROCESS_LOCK:
            backends._VOXTRAL_REALTIME_LLAMA_CPP_STATE.update(
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

    def test_model_paths_auto_download_expected_files(self) -> None:
        hub = SimpleNamespace(snapshot_download=mock.Mock())
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = pathlib.Path(temp_dir, vp.DEFAULT_VOXTRAL_REALTIME_LLAMA_CPP_MODEL_FILE)
            mmproj_path = pathlib.Path(temp_dir, vp.DEFAULT_VOXTRAL_REALTIME_LLAMA_CPP_MMPROJ_FILE)

            def download(**_kwargs):
                model_path.touch()
                mmproj_path.touch()

            hub.snapshot_download.side_effect = download
            with (
                mock.patch.object(vp, "_ensure_stt_backend_model_root", return_value=temp_dir),
                mock.patch.object(backends.importlib, "import_module", return_value=hub),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
            ):
                resolved = backends._voxtral_realtime_llama_cpp_model_paths(download=True)

        self.assertEqual(resolved, (str(model_path), str(mmproj_path)))
        hub.snapshot_download.assert_called_once_with(
            repo_id=vp.DEFAULT_VOXTRAL_REALTIME_LLAMA_CPP_REPO,
            local_dir=temp_dir,
            allow_patterns=[
                vp.DEFAULT_VOXTRAL_REALTIME_LLAMA_CPP_MODEL_FILE,
                vp.DEFAULT_VOXTRAL_REALTIME_LLAMA_CPP_MMPROJ_FILE,
            ],
        )

    def test_server_command_uses_dedicated_realtime_alias_and_acceleration(self) -> None:
        with mock.patch.object(vp, "_effective_speech_acceleration", return_value="cuda"):
            command = backends._voxtral_realtime_llama_cpp_server_command(
                server_bin="/opt/llama-server",
                model_path="/models/model.gguf",
                mmproj_path="/models/mmproj.gguf",
                port=12345,
                api_key="secret",
            )

        self.assertEqual(command[command.index("--alias") + 1], "voxtral-realtime")
        self.assertEqual(command[command.index("--parallel") + 1], "2")
        self.assertEqual(command[command.index("--temp") + 1], "0.0")
        self.assertEqual(command[command.index("--n-gpu-layers") + 1], "999")
        self.assertEqual(command[command.index("--ctx-size") + 1], "2048")
        self.assertEqual(command[command.index("--cache-ram") + 1], "0")

        with mock.patch.object(vp, "_effective_speech_acceleration", return_value="cpu"):
            self.assertEqual(backends._voxtral_realtime_llama_cpp_gpu_layers(), 0)

    def test_cache_clear_stops_dedicated_server(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.pid = 123
        with backends._VOXTRAL_REALTIME_LLAMA_CPP_PROCESS_LOCK:
            backends._VOXTRAL_REALTIME_LLAMA_CPP_STATE["process"] = proc
        cleared = backends.clear_stt_model_caches(keep_backend="parakeet_onnx")
        self.assertEqual(cleared["voxtral_realtime_llama_cpp"], 1)
        proc.terminate.assert_called_once()


class VoxtralRealtimeStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        await backends._close_realtime_wake_verifier_backend("voxtral_realtime_llama_cpp")
        backends._REALTIME_WAKE_VERIFIER_CLIENTS.pop("voxtral_realtime_llama_cpp", None)

    async def test_stream_sends_pcm_and_returns_final_transcript(self) -> None:
        socket = _FakeRealtimeSocket()
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait(b"\x00\x00" * 320)
        queue.put_nowait(None)
        session = backends.VoiceSessionRuntime(
            selector="native:test",
            session_id="turn-1",
            conversation_id="conversation-1",
            wake_word="hey tater",
            audio_format={"rate": 16000, "width": 2, "channels": 1},
            started_ts=0.0,
            startup_gate_until_ts=0.0,
        )
        with backends._VOXTRAL_REALTIME_LLAMA_CPP_PROCESS_LOCK:
            backends._VOXTRAL_REALTIME_LLAMA_CPP_STATE["api_key"] = "secret"

        with (
            mock.patch.object(
                backends,
                "run_stt",
                new=mock.AsyncMock(return_value={"base_url": "http://127.0.0.1:12345"}),
            ),
            mock.patch.object(backends, "websocket_connect", side_effect=lambda *_a, **_k: socket),
        ):
            transcript = await backends._voxtral_realtime_transcribe_queue(
                queue=queue,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
                session_ref=session,
            )

        self.assertEqual(transcript, "Turn on the kitchen lights.")
        self.assertEqual(session.partial_transcript, "Turn on the kitchen lights")
        self.assertEqual(socket.sent[0]["type"], "session.update")
        self.assertEqual(socket.sent[0]["session"]["type"], "transcription")
        self.assertEqual(socket.sent[0]["session"]["audio"]["input"]["transcription"]["language"], "en")
        self.assertTrue(any(item.get("type") == "input_audio_buffer.append" for item in socket.sent))
        commit = next(item for item in socket.sent if item.get("type") == "input_audio_buffer.commit")
        self.assertIs(commit["final"], True)

    async def test_wake_verifier_reuses_warmed_session(self) -> None:
        socket = _FakePersistentRealtimeSocket()
        with backends._VOXTRAL_REALTIME_LLAMA_CPP_PROCESS_LOCK:
            backends._VOXTRAL_REALTIME_LLAMA_CPP_STATE["api_key"] = "secret"

        with (
            mock.patch.object(
                backends,
                "run_stt",
                new=mock.AsyncMock(return_value={"base_url": "http://127.0.0.1:12345"}),
            ),
            mock.patch.object(
                backends,
                "websocket_connect",
                side_effect=lambda *_a, **_k: socket,
            ) as connect,
        ):
            warmed = await backends._warm_realtime_wake_verifier_backend(
                "voxtral_realtime_llama_cpp"
            )
            first = await backends._transcribe_realtime_wake_verifier_audio_bytes(
                backend="voxtral_realtime_llama_cpp",
                audio_bytes=b"\x00\x00" * 320,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
            )
            second = await backends._transcribe_realtime_wake_verifier_audio_bytes(
                backend="voxtral_realtime_llama_cpp",
                audio_bytes=b"\x00\x00" * 320,
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                language="en",
            )

        self.assertTrue(warmed["connected"])
        self.assertEqual(first, "hey tater 1")
        self.assertEqual(second, "hey tater 2")
        self.assertEqual(connect.call_count, 1)
        commits = [item for item in socket.sent if item.get("type") == "input_audio_buffer.commit"]
        self.assertEqual(len(commits), 2)
        self.assertTrue(all(item.get("final") is False for item in commits))


class VoxtralRealtimeWarmupTests(unittest.TestCase):
    def test_warmup_starts_dedicated_server(self) -> None:
        import tateros_app

        with (
            mock.patch.object(vp, "_stt_backend_available", return_value=(True, "")),
            mock.patch.object(
                vp,
                "_load_voxtral_realtime_llama_cpp_server",
                return_value={"pid": 456},
            ) as load,
        ):
            message = tateros_app._warm_speech_model_item(
                {"kind": "stt", "backend": "voxtral_realtime_llama_cpp"}
            )

        self.assertIn("pid=456", message)
        load.assert_called_once()


if __name__ == "__main__":
    unittest.main()
