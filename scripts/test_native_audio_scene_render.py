from __future__ import annotations

import io
import math
import subprocess
import struct
import tempfile
import unittest
import wave
from array import array
from pathlib import Path

from tater_voice import voice_pipeline as vp


def _tone_wav(duration_s: float, frequency_hz: float, *, channels: int) -> bytes:
    sample_rate_hz = 16000
    frame_count = int(round(duration_s * sample_rate_hz))
    output = io.BytesIO()
    with wave.open(output, "wb") as writer:
        writer.setnchannels(channels)
        writer.setsampwidth(2)
        writer.setframerate(sample_rate_hz)
        frames = bytearray()
        for frame_index in range(frame_count):
            sample = int(
                round(
                    9000
                    * math.sin(
                        (2.0 * math.pi * frequency_hz * frame_index)
                        / sample_rate_hz
                    )
                )
            )
            frames.extend(struct.pack("<h", sample) * channels)
        writer.writeframes(bytes(frames))
    return output.getvalue()


def _decode_mono_pcm(audio_bytes: bytes, *, sample_rate_hz: int = 16000) -> array:
    completed = subprocess.run(
        [
            vp._native_media_ffmpeg_binary(),
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-map",
            "0:a:0",
            "-vn",
            "-ac",
            "1",
            "-ar",
            str(sample_rate_hz),
            "-f",
            "s16le",
            "pipe:1",
        ],
        input=bytes(audio_bytes or b""),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30.0,
    )
    if completed.returncode != 0 or not completed.stdout:
        raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
    samples = array("h")
    samples.frombytes(completed.stdout)
    return samples


def _tone_amplitude(
    samples: array,
    frequency_hz: float,
    start_s: float,
    end_s: float,
    *,
    sample_rate_hz: int = 16000,
) -> float:
    start = max(0, int(round(start_s * sample_rate_hz)))
    end = min(len(samples), int(round(end_s * sample_rate_hz)))
    window = samples[start:end]
    if not window:
        return 0.0
    sin_sum = 0.0
    cos_sum = 0.0
    for index, sample in enumerate(window, start=start):
        phase = (2.0 * math.pi * frequency_hz * index) / sample_rate_hz
        sin_sum += sample * math.sin(phase)
        cos_sum += sample * math.cos(phase)
    return 2.0 * math.hypot(sin_sum, cos_sum) / len(window)


@unittest.skipUnless(vp._native_media_ffmpeg_binary(), "ffmpeg is required")
class NativeAudioSceneRenderTests(unittest.TestCase):
    def test_renders_one_seekable_scene_with_the_requested_timeline(self) -> None:
        rendered = vp._render_native_audio_scene_asset_sync(
            _tone_wav(0.25, 880.0, channels=1),
            foreground_media_type="audio/wav",
            foreground_filename="tts.wav",
            background_bytes=_tone_wav(0.12, 220.0, channels=2),
            background_media_type="audio/wav",
            background_filename="music.wav",
            background_loop=False,
            foreground_volume_percent=90,
            background_volume_percent=55,
            start_delay_ms=250,
            ducking_target_percent=30,
            ducking_attack_ms=50,
            ducking_release_ms=100,
            fade_ms=150,
        )

        self.assertEqual(rendered["media_type"], "audio/mpeg")
        self.assertEqual(rendered["filename"], "announcement-scene.mp3")
        self.assertTrue(rendered["rendered_audio_scene"])
        self.assertEqual(rendered["start_delay_ms"], 250)
        self.assertAlmostEqual(rendered["foreground_duration_s"], 0.25, places=2)
        self.assertAlmostEqual(rendered["duration_s"], 0.75, places=2)
        self.assertTrue(vp._native_media_mp3_has_duration_header(rendered["bytes"]))

        with tempfile.TemporaryDirectory(prefix="tater-scene-test-") as temp_dir:
            scene_path = Path(temp_dir) / "scene.mp3"
            scene_path.write_bytes(rendered["bytes"])
            decoded_duration_s = vp._native_media_duration_s_sync(
                vp._native_media_ffmpeg_binary(),
                scene_path,
            )
        self.assertAlmostEqual(decoded_duration_s, 0.75, delta=0.05)

    def test_bakes_speech_offset_and_ducking_envelope_into_rendered_audio(self) -> None:
        rendered = vp._render_native_audio_scene_asset_sync(
            _tone_wav(1.0, 880.0, channels=1),
            foreground_media_type="audio/wav",
            foreground_filename="tts.wav",
            background_bytes=_tone_wav(0.4, 220.0, channels=2),
            background_media_type="audio/wav",
            background_filename="music.wav",
            background_loop=True,
            foreground_volume_percent=100,
            background_volume_percent=80,
            start_delay_ms=500,
            ducking_target_percent=25,
            ducking_attack_ms=200,
            ducking_release_ms=200,
            fade_ms=200,
        )

        self.assertEqual(rendered["duck_start_ms"], 300)
        self.assertEqual(rendered["speech_start_ms"], 500)
        self.assertEqual(rendered["speech_end_ms"], 1500)
        self.assertEqual(rendered["release_end_ms"], 1700)
        self.assertEqual(rendered["fade_start_ms"], 1700)

        samples = _decode_mono_pcm(rendered["bytes"])
        background_before = _tone_amplitude(samples, 220.0, 0.10, 0.20)
        background_during = _tone_amplitude(samples, 220.0, 0.70, 0.90)
        background_releasing = _tone_amplitude(samples, 220.0, 1.58, 1.64)
        speech_before = _tone_amplitude(samples, 880.0, 0.35, 0.45)
        speech_during = _tone_amplitude(samples, 880.0, 0.60, 0.75)

        self.assertGreater(background_before, 1000.0)
        self.assertAlmostEqual(
            background_during / background_before,
            0.25,
            delta=0.06,
        )
        self.assertGreater(background_releasing, background_during * 1.7)
        self.assertLess(speech_before, speech_during * 0.05)
        self.assertGreater(speech_during, 1000.0)


if __name__ == "__main__":
    unittest.main()
