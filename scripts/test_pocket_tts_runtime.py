#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import speech_tts  # noqa: E402
from tater_voice import voice_pipeline as vp  # noqa: E402
from tater_voice.voice_pipeline import backends  # noqa: E402


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


class _StreamingPocketModel:
    sample_rate = 24000

    def __init__(self):
        self.state_loads = []
        self.stream_calls = []
        self.generate_audio = mock.Mock(side_effect=AssertionError("streaming API should be used"))

    def get_state_for_audio_prompt(self, voice):
        state = object()
        self.state_loads.append((voice, state))
        return state

    def generate_audio_stream(self, state, text):
        self.stream_calls.append((state, text))
        yield _FakeTensor([-0.5, 0.25])
        yield _FakeTensor([0.75])


class _LegacyPocketModel:
    def __init__(self):
        self.generate_audio = mock.Mock(return_value=_FakeTensor([-0.25, 0.5]))


class PocketTtsNativeRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        with vp._pocket_tts_model_lock:
            vp._pocket_tts_model_cache.clear()
            vp._pocket_tts_voice_state_cache.clear()

    def tearDown(self) -> None:
        self.setUp()

    def test_synthesis_reuses_voice_state_and_consumes_streaming_chunks(self) -> None:
        model = _StreamingPocketModel()
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(backends, "_load_pocket_tts_model", return_value=model),
                mock.patch.object(vp, "_ensure_tts_backend_model_root", return_value=temp_dir),
                mock.patch.object(backends, "huggingface_environment", return_value={}),
                mock.patch.object(backends, "_pocket_tts_output_gain", return_value=1.0),
            ):
                first_audio, first_format = backends._synthesize_pocket_tts_sync(
                    "First message", "default", "alba"
                )
                second_audio, second_format = backends._synthesize_pocket_tts_sync(
                    "Second message", "default", "alba"
                )

        expected = backends._float_audio_to_pcm16_bytes(
            np.asarray([-0.5, 0.25, 0.75], dtype=np.float32),
            gain=1.0,
        )
        self.assertEqual(first_audio, expected)
        self.assertEqual(second_audio, expected)
        self.assertEqual(first_format, {"rate": 24000, "width": 2, "channels": 1})
        self.assertEqual(second_format, first_format)
        self.assertEqual(len(model.state_loads), 1)
        self.assertEqual(len(model.stream_calls), 2)
        self.assertIs(model.stream_calls[0][0], model.stream_calls[1][0])
        model.generate_audio.assert_not_called()

    def test_generation_falls_back_when_streaming_api_is_unavailable(self) -> None:
        model = _LegacyPocketModel()
        state = object()

        result = backends._generate_pocket_tts_pcm(
            model,
            state,
            "Legacy message",
            gain=1.0,
        )

        self.assertEqual(
            result,
            backends._float_audio_to_pcm16_bytes(
                np.asarray([-0.25, 0.5], dtype=np.float32),
                gain=1.0,
            ),
        )
        model.generate_audio.assert_called_once_with(state, "Legacy message")

    def test_cache_clear_removes_prepared_voice_states(self) -> None:
        with vp._pocket_tts_model_lock:
            vp._pocket_tts_model_cache["default"] = object()
            vp._pocket_tts_voice_state_cache[("default", "alba")] = object()

        cleared = backends.clear_tts_model_caches(include_piper=False)

        self.assertEqual(cleared["pocket_tts"], 1)
        self.assertFalse(vp._pocket_tts_model_cache)
        self.assertFalse(vp._pocket_tts_voice_state_cache)


class PocketTtsAnnouncementRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        with speech_tts._pocket_tts_model_lock:
            speech_tts._pocket_tts_model_cache.clear()
            speech_tts._pocket_tts_voice_state_cache.clear()

    def tearDown(self) -> None:
        self.setUp()

    def test_synthesis_reuses_voice_state_and_consumes_streaming_chunks(self) -> None:
        model = _StreamingPocketModel()
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(speech_tts, "_load_pocket_tts_model", return_value=model),
                mock.patch.object(speech_tts, "_ensure_tts_backend_model_root", return_value=temp_dir),
                mock.patch.object(speech_tts, "huggingface_environment", return_value={}),
                mock.patch.object(speech_tts, "_pocket_tts_output_gain", return_value=1.0),
            ):
                first_audio, _first_format = speech_tts._synthesize_pocket_tts_sync(
                    "First announcement", "default", "alba"
                )
                second_audio, _second_format = speech_tts._synthesize_pocket_tts_sync(
                    "Second announcement", "default", "alba"
                )

        expected = speech_tts._float_audio_to_pcm16_bytes(
            np.asarray([-0.5, 0.25, 0.75], dtype=np.float32),
            gain=1.0,
        )
        self.assertEqual(first_audio, expected)
        self.assertEqual(second_audio, expected)
        self.assertEqual(len(model.state_loads), 1)
        self.assertEqual(len(model.stream_calls), 2)
        self.assertIs(model.stream_calls[0][0], model.stream_calls[1][0])
        model.generate_audio.assert_not_called()


if __name__ == "__main__":
    unittest.main()
