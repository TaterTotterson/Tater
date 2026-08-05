from __future__ import annotations

import unittest
from unittest import mock

import speech_tts


class _FakeSynthesizeVoice:
    def __init__(self, *, name=None, language=None, speaker=None):
        self.name = name
        self.language = language
        self.speaker = speaker


class _FakeSynthesize:
    last_instance = None

    def __init__(self, *, text, voice=None):
        self.text = text
        self.voice = voice
        type(self).last_instance = self

    def event(self):
        return self


class WyomingTtsVoiceSelectionTests(unittest.TestCase):
    def _build_event(self, voice_value):
        with (
            mock.patch.object(speech_tts, "Synthesize", _FakeSynthesize),
            mock.patch.object(speech_tts, "SynthesizeVoice", _FakeSynthesizeVoice),
        ):
            return speech_tts._wyoming_synthesize_event("Hello from Tater.", voice_value)

    def test_structured_voice_value_preserves_wyoming_fields(self) -> None:
        event = self._build_event(
            '{"name":"en_US-amy-low","language":"en_US","speaker":""}'
        )

        self.assertEqual(event.voice.name, "en_US-amy-low")
        self.assertEqual(event.voice.language, "en_US")
        self.assertIsNone(event.voice.speaker)

    def test_plain_voice_name_remains_supported(self) -> None:
        event = self._build_event("en_US-amy-low")

        self.assertEqual(event.voice.name, "en_US-amy-low")
        self.assertIsNone(event.voice.language)
        self.assertIsNone(event.voice.speaker)

    def test_blank_voice_uses_server_default(self) -> None:
        event = self._build_event("")

        self.assertIsNone(event.voice)


if __name__ == "__main__":
    unittest.main()
