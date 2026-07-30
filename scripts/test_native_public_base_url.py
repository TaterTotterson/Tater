#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import tempfile
import threading
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import voice_pipeline as vp  # noqa: E402


class NativePublicBaseUrlTests(unittest.TestCase):
    def setUp(self) -> None:
        with vp._tts_url_store_lock:
            vp._tts_url_store.clear()

    def tearDown(self) -> None:
        with vp._tts_url_store_lock:
            vp._tts_url_store.clear()

    def test_public_base_url_takes_precedence_and_preserves_path(self) -> None:
        env = {
            "VOICE_CORE_PUBLIC_BASE_URL": "https://tater.example.com/tater/",
            "VOICE_CORE_PUBLIC_HOST": "internal.example.com",
            "HTMLUI_PORT": "8501",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertEqual(
                vp._service_base_url_for_peer("192.168.1.50"),
                "https://tater.example.com/tater",
            )

    def test_public_host_accepts_full_url_without_double_scheme(self) -> None:
        env = {
            "VOICE_CORE_PUBLIC_BASE_URL": "",
            "VOICE_CORE_PUBLIC_HOST": "https://tater.example.com/",
            "HTMLUI_PORT": "8501",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertEqual(
                vp._service_base_url_for_peer("192.168.1.50"),
                "https://tater.example.com",
            )

    def test_bare_public_host_uses_app_port_without_duplicating_explicit_port(self) -> None:
        env = {
            "VOICE_CORE_PUBLIC_BASE_URL": "",
            "VOICE_CORE_PUBLIC_HOST": "tater.example.com",
            "HTMLUI_PORT": "8501",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertEqual(
                vp._service_base_url_for_peer("192.168.1.50"),
                "http://tater.example.com:8501",
            )

        env["VOICE_CORE_PUBLIC_HOST"] = "tater.example.com:9443"
        with mock.patch.dict(os.environ, env, clear=False):
            self.assertEqual(
                vp._service_base_url_for_peer("192.168.1.50"),
                "http://tater.example.com:9443",
            )

    def test_native_playback_urls_share_public_base_url(self) -> None:
        public_base_url = "https://tater.example.com/tater"
        with (
            mock.patch.object(vp, "_service_base_url_for_peer", return_value=public_base_url),
            mock.patch.object(
                vp,
                "_pcm_to_wav",
                return_value=(b"wav", {"rate": 16000, "width": 2, "channels": 1}),
            ),
            mock.patch.object(
                vp,
                "_chatterbox_tts_request",
                return_value=("http://chatterbox/tts", {"text": "hello"}),
            ),
            mock.patch.object(vp, "_tts_url_ttl_s", return_value=180.0),
        ):
            tts_url = vp._store_tts_url(
                "native:test",
                "session",
                b"pcm",
                {"rate": 16000, "width": 2, "channels": 1},
            )
            chatterbox_url = vp._store_chatterbox_tts_stream_url(
                "native:test",
                "session",
                "hello",
                {},
            )
            media_url = vp._store_media_url(
                "native:test",
                "session",
                b"media",
                media_type="audio/mpeg",
                filename="music.mp3",
            )

        self.assertRegex(
            tts_url,
            r"^https://tater\.example\.com/tater/api/tater/satellite/v1/tts/[0-9a-f]+\.wav$",
        )
        self.assertRegex(
            chatterbox_url,
            r"^https://tater\.example\.com/tater/api/tater/satellite/v1/tts/[0-9a-f]+\.wav$",
        )
        self.assertRegex(
            media_url,
            r"^https://tater\.example\.com/tater/api/tater/satellite/v1/media/[0-9a-f]+$",
        )

    def test_background_audio_download_reads_agent_lab_preset_directly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            preset = (
                pathlib.Path(temp_dir)
                / "ai_task"
                / "background_audio"
                / "presets"
                / "morning_glow.wav"
            )
            preset.parent.mkdir(parents=True)
            preset.write_bytes(b"RIFF-background-audio")

            env = {"TATER_AGENT_ROOT": temp_dir, "HTMLUI_PORT": "8501"}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    vp.requests,
                    "get",
                    side_effect=AssertionError("local presets must not use HTTP"),
                ),
            ):
                data, content_type = asyncio.run(
                    vp._download_media_source(
                        "http://127.0.0.1:8501/api/ai-tasks/background-audio/presets/morning_glow.wav"
                    )
                )

        self.assertEqual(b"RIFF-background-audio", data)
        self.assertEqual("audio/wav", content_type)

    def test_external_background_audio_download_uses_background_http_fetch(self) -> None:
        class FakeResponse:
            content = b"RIFF-background-audio"
            headers = {"Content-Type": "audio/wav; charset=binary"}

            @staticmethod
            def raise_for_status() -> None:
                return None

        main_thread = threading.get_ident()
        fetch_thread = {"id": main_thread}

        def fake_get(url: str, *, timeout: int):
            fetch_thread["id"] = threading.get_ident()
            self.assertEqual("https://example.test/background.wav", url)
            self.assertEqual(180, timeout)
            return FakeResponse()

        with mock.patch.object(vp.requests, "get", side_effect=fake_get):
            data, content_type = asyncio.run(
                vp._download_media_source("https://example.test/background.wav")
            )

        self.assertEqual(b"RIFF-background-audio", data)
        self.assertEqual("audio/wav", content_type)
        self.assertNotEqual(main_thread, fetch_thread["id"])


if __name__ == "__main__":
    unittest.main()
