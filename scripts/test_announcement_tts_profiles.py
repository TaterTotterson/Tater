from __future__ import annotations

import ast
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

helpers_stub = ModuleType("helpers")
helpers_stub.redis_client = mock.Mock()
sys.modules.setdefault("helpers", helpers_stub)

import speech_settings  # noqa: E402


def _load_speak_announcement_targets():
    source = (ROOT / "speech_tts.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "speak_announcement_targets"
    )
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    namespace = {
        "ANNOUNCEMENT_TTS_DIRECT_BACKEND": speech_settings.ANNOUNCEMENT_TTS_DIRECT_BACKEND,
        "DEFAULT_ANNOUNCEMENT_TTS_BACKEND": speech_settings.DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
        "DEFAULT_TTS_BACKEND": speech_settings.DEFAULT_TTS_BACKEND,
        "DEFAULT_WYOMING_TTS_HOST": speech_settings.DEFAULT_WYOMING_TTS_HOST,
        "DEFAULT_WYOMING_TTS_PORT": speech_settings.DEFAULT_WYOMING_TTS_PORT,
        "DEFAULT_WYOMING_TTS_VOICE": speech_settings.DEFAULT_WYOMING_TTS_VOICE,
        "DEFAULT_VOICE_CORE_PLAY_TIMEOUT_SECONDS": 180.0,
        "normalize_announcement_tts_backend": speech_settings.normalize_announcement_tts_backend,
        "normalize_tts_backend": speech_settings._normalize_tts_backend,
        "_text": lambda value: str(value or "").strip(),
        "logger": mock.Mock(),
    }
    module = ast.Module(body=[future, function], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<announcement-tts-runtime>", "exec"), namespace)
    return namespace["speak_announcement_targets"], namespace


SPEAK_ANNOUNCEMENT_TARGETS, SPEECH_TTS_NAMESPACE = _load_speak_announcement_targets()


class _FakeRedis:
    def __init__(self, values=None) -> None:
        self.values = dict(values or {})

    def hgetall(self, _key):
        return dict(self.values)

    def hset(self, _key, field=None, value=None, *, mapping=None):
        if mapping is not None:
            self.values.update(mapping)
        elif field is not None:
            self.values[str(field)] = value

    def hdel(self, _key, *fields):
        for field in fields:
            self.values.pop(str(field), None)


class AnnouncementTtsProfileTests(unittest.TestCase):
    def test_same_as_direct_is_preserved_as_an_explicit_mode(self) -> None:
        for value in ("same_as_direct", "Same as Direct TTS", "direct", "shared"):
            with self.subTest(value=value):
                self.assertEqual(
                    speech_settings.normalize_announcement_tts_backend(value),
                    speech_settings.ANNOUNCEMENT_TTS_DIRECT_BACKEND,
                )

    def test_existing_direct_profiles_seed_announcement_profiles_once(self) -> None:
        fake = _FakeRedis(
            {
                "qwen_tts_clone_audio": "/profiles/reference.wav",
                "qwen_tts_clone_text": "Direct transcript",
                "qwen_tts_language": "English",
                "announcement_qwen_tts_clone_text": "Announcement transcript",
            }
        )
        with mock.patch.object(speech_settings, "redis_client", fake):
            settings = speech_settings.get_speech_settings()

        self.assertEqual(settings["announcement_qwen_tts_clone_audio"], "/profiles/reference.wav")
        self.assertEqual(settings["announcement_qwen_tts_clone_text"], "Announcement transcript")
        self.assertEqual(settings["announcement_qwen_tts_language"], "English")

    def test_custom_announcement_profiles_are_saved_independently(self) -> None:
        fake = _FakeRedis()
        with mock.patch.object(speech_settings, "redis_client", fake):
            speech_settings.save_speech_settings(
                stt_backend="faster_whisper",
                wyoming_stt_host="127.0.0.1",
                wyoming_stt_port="10300",
                tts_backend="omnivoice",
                tts_model="direct-model",
                tts_voice="",
                wyoming_tts_host="127.0.0.1",
                wyoming_tts_port="10200",
                wyoming_tts_voice="",
                openai_tts_base_url="",
                openai_tts_api_key="",
                announcement_tts_backend="qwen3_tts",
                announcement_tts_model="announcement-model",
                announcement_tts_voice="",
                omnivoice_tts_clone_audio="/profiles/direct.wav",
                omnivoice_tts_clone_text="Direct voice",
                announcement_qwen_tts_clone_audio="/profiles/announcement.wav",
                announcement_qwen_tts_clone_text="Announcement voice",
                announcement_qwen_tts_language="Spanish",
                announcement_qwen_tts_instruct="A bright announcement voice.",
            )

        self.assertEqual(fake.values["omnivoice_tts_clone_audio"], "/profiles/direct.wav")
        self.assertEqual(fake.values["omnivoice_tts_clone_text"], "Direct voice")
        self.assertEqual(fake.values["announcement_qwen_tts_clone_audio"], "/profiles/announcement.wav")
        self.assertEqual(fake.values["announcement_qwen_tts_clone_text"], "Announcement voice")
        self.assertEqual(fake.values["announcement_qwen_tts_language"], "Spanish")
        self.assertEqual(fake.values["announcement_qwen_tts_instruct"], "A bright announcement voice.")

    def test_ui_exposes_reuse_and_independent_managed_voice_cards(self) -> None:
        app_source = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_source = (ROOT / "speech_settings.py").read_text(encoding="utf-8")

        self.assertIn('"label": "Same as Direct Reply TTS"', settings_source)
        self.assertIn('id="speech-announcement-qwen-tts-profile-wrap"', app_source)
        self.assertIn('id="speech-announcement-omnivoice-tts-profile-wrap"', app_source)
        self.assertIn("scope=${encodeURIComponent(scope)}", app_source)
        self.assertIn("speech_announcement_qwen_tts_clone_text", app_source)
        self.assertIn("speech_announcement_omnivoice_tts_clone_text", app_source)


class AnnouncementTtsRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def _synthesize(self, backend: str, settings: dict) -> dict:
        async_synthesize = mock.AsyncMock(return_value=b"RIFF-audio")
        async_playback = mock.AsyncMock(return_value={"ok": True, "sent_count": 1})
        grouped = {
            "homeassistant_media_players": [],
            "voice_core_selectors": ["kitchen"],
            "unifi_protect_cameras": [],
            "sonos_speakers": [],
            "integration_devices": [],
        }
        with mock.patch.dict(
            SPEECH_TTS_NAMESPACE,
            {
                "get_speech_settings": lambda: settings,
                "split_announcement_targets": lambda _targets: grouped,
                "synthesize_tts_wav": async_synthesize,
                "run_background": async_playback,
                "_voice_core_play_media_sync": object(),
            },
        ):
            result = await SPEAK_ANNOUNCEMENT_TARGETS(
                text="Dinner is ready.",
                backend=backend,
                ha_base="",
                token="",
                targets=["kitchen"],
            )
        self.assertTrue(result["ok"])
        return dict(async_synthesize.await_args.kwargs)

    async def test_same_as_direct_resolves_the_complete_direct_profile(self) -> None:
        kwargs = await self._synthesize(
            "same_as_direct",
            {
                "tts_backend": "omnivoice",
                "tts_model": "direct-omni-model",
                "tts_voice": "",
                "omnivoice_tts_clone_audio": "/profiles/direct.wav",
                "omnivoice_tts_clone_text": "Direct transcript",
                "omnivoice_tts_language": "English",
                "omnivoice_tts_instruct": "Direct voice",
                "acceleration": "cpu",
            },
        )

        self.assertEqual(kwargs["backend"], "omnivoice")
        self.assertEqual(kwargs["model"], "direct-omni-model")
        self.assertEqual(kwargs["clone_audio"], "/profiles/direct.wav")
        self.assertEqual(kwargs["clone_text"], "Direct transcript")
        self.assertEqual(kwargs["managed_instruct"], "Direct voice")

    async def test_custom_announcement_uses_its_own_managed_profile(self) -> None:
        kwargs = await self._synthesize(
            "qwen3_tts",
            {
                "announcement_tts_backend": "qwen3_tts",
                "announcement_tts_model": "announcement-qwen-model",
                "announcement_tts_voice": "",
                "announcement_qwen_tts_clone_audio": "/profiles/announcement.wav",
                "announcement_qwen_tts_clone_text": "Announcement transcript",
                "announcement_qwen_tts_language": "Spanish",
                "announcement_qwen_tts_instruct": "Announcement voice",
                "acceleration": "cuda",
            },
        )

        self.assertEqual(kwargs["backend"], "qwen3_tts")
        self.assertEqual(kwargs["model"], "announcement-qwen-model")
        self.assertEqual(kwargs["clone_audio"], "/profiles/announcement.wav")
        self.assertEqual(kwargs["clone_text"], "Announcement transcript")
        self.assertEqual(kwargs["managed_language"], "Spanish")
        self.assertEqual(kwargs["managed_instruct"], "Announcement voice")


if __name__ == "__main__":
    unittest.main()
