#!/usr/bin/env python3
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "tateros_static" / "app.js"


class VoiceModelSaveScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = APP_JS.read_text(encoding="utf-8")

    def test_voice_panel_has_a_dedicated_save_action(self) -> None:
        self.assertIn('id="settings-speech-model-save"', self.source)
        self.assertIn('saveModelSettings("speech")', self.source)

    def test_voice_scope_only_collects_speech_settings(self) -> None:
        start = self.source.index("const saveModelSettings = async")
        end = self.source.index("const hydraSubtabButtons", start)
        implementation = self.source[start:end]

        self.assertIn(
            'if (normalizedScope === "all" || normalizedScope === "speech")',
            implementation,
        )
        self.assertIn(
            'if (normalizedScope !== "speech") {\n'
            "      payload.hydra_local_model_load_targets",
            implementation,
        )


if __name__ == "__main__":
    unittest.main()
