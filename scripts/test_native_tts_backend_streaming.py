#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import pathlib
import struct
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import voice_pipeline as vp  # noqa: E402
from tater_voice.voice_pipeline import backends  # noqa: E402
from tater_voice.voice_pipeline import routes  # noqa: E402


class _FakeTensor:
    def __init__(self, values):
        self._values = np.asarray(values, dtype=np.float32)

    def detach(self):
        return self

    def cpu(self):
        return self

    def squeeze(self):
        return self

    def numpy(self):
        return self._values


class NativeTtsPcmStreamTests(unittest.TestCase):
    def test_pcm_stream_uses_open_ended_wav_and_preserves_chunks(self) -> None:
        audio_format = {"rate": 24000, "width": 2, "channels": 1}
        chunks = list(
            backends._iter_streaming_pcm_wav(
                [(b"\x01\x02\x03\x04", audio_format), (b"\x05\x06", audio_format)],
                {"id": "stream", "max_bytes": 1024 * 1024},
                backend="test",
            )
        )

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][:4], b"RIFF")
        self.assertEqual(chunks[0][8:12], b"WAVE")
        self.assertEqual(struct.unpack_from("<I", chunks[0], 4)[0], 0xFFFFFFFF)
        self.assertEqual(struct.unpack_from("<I", chunks[0], 40)[0], 0xFFFFFFFF)
        self.assertEqual(chunks[0][44:], b"\x01\x02\x03\x04")
        self.assertEqual(chunks[1], b"\x05\x06")

    def test_pcm_stream_rejects_format_changes(self) -> None:
        stream = backends._iter_streaming_pcm_wav(
            [
                (b"\x00\x00", {"rate": 24000, "width": 2, "channels": 1}),
                (b"\x00\x00", {"rate": 22050, "width": 2, "channels": 1}),
            ],
            {"max_bytes": 1024 * 1024},
            backend="test",
        )
        next(stream)
        with self.assertRaisesRegex(RuntimeError, "changed audio format"):
            next(stream)

    def test_pocket_tts_yields_model_chunks_without_buffering(self) -> None:
        class PocketModel:
            sample_rate = 24000

            def get_state_for_audio_prompt(self, _voice):
                return object()

            def generate_audio_stream(self, _state, _text):
                yield _FakeTensor([-0.5, 0.25])
                yield _FakeTensor([0.75])

            generate_audio = mock.Mock(side_effect=AssertionError("full generation must not run"))

        model = PocketModel()
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(backends, "_load_pocket_tts_model", return_value=model),
                mock.patch.object(vp, "_ensure_tts_backend_model_root", return_value=temp_dir),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
                mock.patch.object(backends, "_pocket_tts_output_gain", return_value=1.0),
            ):
                chunks = list(
                    backends._iter_native_tts_stream_response(
                        {
                            "id": "pocket",
                            "backend": "pocket_tts",
                            "text": "Hello",
                            "selection": {"model": "default", "voice": "alba"},
                            "max_bytes": 1024 * 1024,
                        }
                    )
                )

        self.assertEqual(len(chunks), 2)
        self.assertEqual(
            chunks[0][44:],
            backends._float_audio_to_pcm16_bytes(np.asarray([-0.5, 0.25]), gain=1.0),
        )
        self.assertEqual(
            chunks[1],
            backends._float_audio_to_pcm16_bytes(np.asarray([0.75]), gain=1.0),
        )
        model.generate_audio.assert_not_called()

    def test_piper_yields_each_native_chunk(self) -> None:
        class PiperVoice:
            def synthesize(self, _text, *, syn_config):
                self.syn_config = syn_config
                yield SimpleNamespace(
                    audio_int16_bytes=b"\x01\x02\x03\x04",
                    sample_rate=22050,
                    sample_width=2,
                    sample_channels=1,
                )
                yield SimpleNamespace(
                    audio_int16_bytes=b"\x05\x06",
                    sample_rate=22050,
                    sample_width=2,
                    sample_channels=1,
                )

        voice = PiperVoice()
        with (
            mock.patch.object(backends, "_load_piper_voice_model", return_value=voice),
            mock.patch.object(vp, "PiperSynthesisConfig", return_value=object()),
            mock.patch.object(vp, "DEFAULT_PIPER_TAIL_PAD_SECONDS", 0.0),
        ):
            chunks = list(
                backends._iter_native_tts_stream_response(
                    {
                        "id": "piper",
                        "backend": "piper",
                        "text": "Hello",
                        "selection": {"model": "test"},
                        "max_bytes": 1024 * 1024,
                    }
                )
            )

        self.assertEqual([chunks[0][44:], chunks[1]], [b"\x01\x02\x03\x04", b"\x05\x06"])

    def test_kokoro_torch_yields_pipeline_results(self) -> None:
        calls = []

        def pipeline(text, **kwargs):
            calls.append((text, kwargs))
            yield SimpleNamespace(audio=np.asarray([-0.25, 0.25], dtype=np.float32))
            yield SimpleNamespace(audio=np.asarray([0.5], dtype=np.float32))

        with (
            mock.patch.object(vp, "_kokoro_engine", return_value="torch"),
            mock.patch.object(vp, "_kokoro_torch_device", return_value="cpu"),
            mock.patch.object(backends, "_load_kokoro_pipeline", return_value=pipeline),
            mock.patch.object(backends, "_kokoro_output_gain", return_value=1.0),
        ):
            chunks = list(
                backends._iter_native_tts_stream_response(
                    {
                        "id": "kokoro",
                        "backend": "kokoro",
                        "text": "Hello. There.",
                        "selection": {"model": "test", "voice": "af_heart"},
                        "max_bytes": 1024 * 1024,
                    }
                )
            )

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][44:], backends._float_audio_to_pcm16_bytes([-0.25, 0.25]))
        self.assertEqual(chunks[1], backends._float_audio_to_pcm16_bytes([0.5]))
        self.assertEqual(calls[0][0], "Hello. There.")

    def test_chatterbox_local_streams_sentence_results_from_managed_worker(self) -> None:
        calls = []

        def synthesize(text, **kwargs):
            calls.append((text, kwargs))
            return b"\x01\x00", {"rate": 24000, "width": 2, "channels": 1}

        with (
            mock.patch.object(backends, "synthesize_managed_tts_pcm", side_effect=synthesize),
            mock.patch.object(vp, "DEFAULT_PIPER_SENTENCE_PAUSE_SECONDS", 0.0),
            mock.patch.object(vp, "DEFAULT_PIPER_TAIL_PAD_SECONDS", 0.0),
            mock.patch.object(vp, "_voice_settings_with_shared_speech", return_value={"VOICE_ACCELERATION": "cpu"}),
        ):
            chunks = list(
                backends._iter_native_tts_stream_response(
                    {
                        "id": "chatterbox-local",
                        "backend": "chatterbox_local",
                        "text": "Hello. There.",
                        "selection": {
                            "model": "ResembleAI/chatterbox-nano",
                            "chatterbox_local_tts_clone_audio": "/voices/reference.wav",
                            "chatterbox_temperature": "0.7",
                            "chatterbox_seed": "42",
                        },
                        "max_bytes": 1024 * 1024,
                    }
                )
            )

        self.assertEqual([call[0] for call in calls], ["Hello.", "There."])
        self.assertEqual(chunks[0][44:], b"\x01\x00")
        self.assertEqual(chunks[1], b"\x01\x00")
        self.assertEqual(calls[0][1]["clone_audio"], "/voices/reference.wav")
        self.assertEqual(calls[0][1]["temperature"], "0.7")


class NativeTtsRemoteStreamTests(unittest.TestCase):
    def test_stream_setup_failure_falls_back_to_wyoming_before_audio_starts(self) -> None:
        def failed_stream():
            raise RuntimeError("local model failed")
            yield b""  # pragma: no cover

        async def wyoming_stream(row):
            self.assertEqual(row["backend"], "wyoming")
            yield b"RIFF-fallback"

        async def collect():
            result = []
            async for chunk in routes._iter_sync_tts_stream_with_fallback(
                failed_stream(),
                {"backend": "piper"},
            ):
                result.append(chunk)
            return result

        with (
            mock.patch.object(vp, "_tts_backend_available", return_value=(True, "")),
            mock.patch.object(vp, "_iter_wyoming_tts_stream_response", side_effect=wyoming_stream),
        ):
            chunks = asyncio.run(collect())

        self.assertEqual(chunks, [b"RIFF-fallback"])

    def test_openai_compatible_uses_http_streaming_and_relays_wav_chunks(self) -> None:
        wav_header = backends._streaming_pcm_wav_header(
            {"rate": 24000, "width": 2, "channels": 1}
        )

        class Response:
            status_code = 200

            def __init__(self):
                self.closed = False

            def iter_content(self, chunk_size):
                self.chunk_size = chunk_size
                yield wav_header[:7]
                yield wav_header[7:] + b"\x01\x02"
                yield b"\x03\x04"

            def close(self):
                self.closed = True

        response = Response()
        row = {
            "id": "openai",
            "text": "Hello",
            "selection": {
                "model": "tts-1",
                "voice": "alloy",
                "openai_base_url": "https://api.openai.com/v1",
                "openai_api_key": "secret",
            },
            "max_bytes": 1024 * 1024,
        }
        with mock.patch.object(backends.requests, "post", return_value=response) as post:
            opened = backends._open_openai_compatible_tts_stream_response(row)
            chunks = list(backends._iter_openai_compatible_tts_stream_response(opened, row))

        self.assertEqual(b"".join(chunks), wav_header + b"\x01\x02\x03\x04")
        self.assertTrue(response.closed)
        self.assertTrue(post.call_args.kwargs["stream"])
        self.assertEqual(post.call_args.kwargs["json"]["response_format"], "wav")
        self.assertEqual(post.call_args.kwargs["headers"]["Authorization"], "Bearer secret")

    def test_wyoming_relays_audio_events_as_open_ended_wav(self) -> None:
        events = [
            SimpleNamespace(type="start", value=SimpleNamespace(rate=16000, width=2, channels=1)),
            SimpleNamespace(type="chunk", value=SimpleNamespace(audio=b"\x01\x02\x03\x04")),
            SimpleNamespace(type="chunk", value=SimpleNamespace(audio=b"\x05\x06")),
            SimpleNamespace(type="stop", value=SimpleNamespace()),
        ]

        class EventType:
            def __init__(self, name):
                self.name = name

            def is_type(self, event_type):
                return event_type == self.name

            def from_event(self, event):
                return event.value

        class Client:
            def __init__(self, host, port):
                self.host = host
                self.port = port
                self.writes = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def write_event(self, event):
                self.writes.append(event)

            async def read_event(self):
                return events.pop(0) if events else None

        class Synthesize:
            def __init__(self, text, voice=None):
                self.text = text
                self.voice = voice

            def event(self):
                return {"text": self.text}

        async def collect():
            result = []
            async for chunk in backends._iter_wyoming_tts_stream_response(
                {
                    "id": "wyoming",
                    "text": "Hello",
                    "selection": {
                        "wyoming_host": "localhost",
                        "wyoming_port": 10200,
                        "wyoming_voice": "",
                    },
                    "max_bytes": 1024 * 1024,
                }
            ):
                result.append(chunk)
            return result

        with (
            mock.patch.object(vp, "AsyncTcpClient", Client),
            mock.patch.object(vp, "Synthesize", Synthesize),
            mock.patch.object(vp, "SynthesizeVoice", None),
            mock.patch.object(vp, "WyomingAudioStart", EventType("start")),
            mock.patch.object(vp, "WyomingAudioChunk", EventType("chunk")),
            mock.patch.object(vp, "WyomingAudioStop", EventType("stop")),
            mock.patch.object(vp, "WyomingError", EventType("error")),
            mock.patch.object(backends, "_wyoming_timeout_s", return_value=5.0),
        ):
            chunks = asyncio.run(collect())

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][:4], b"RIFF")
        self.assertEqual(chunks[0][44:], b"\x01\x02\x03\x04")
        self.assertEqual(chunks[1], b"\x05\x06")


if __name__ == "__main__":
    unittest.main()
