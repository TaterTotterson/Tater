from __future__ import annotations

import io
import math
import struct
import tempfile
import unittest
import wave
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


if __name__ == "__main__":
    unittest.main()
