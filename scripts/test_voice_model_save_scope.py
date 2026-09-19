#!/usr/bin/env python3
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODELS_COMPONENT = ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue"


class VoiceModelSaveScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = MODELS_COMPONENT.read_text(encoding="utf-8")

    def test_voice_panel_has_a_dedicated_save_action(self) -> None:
        self.assertIn('case "speech": return { label: "Save Speech Settings"', self.source)
        self.assertIn('activeTab.value === "speech"', self.source)
        self.assertIn("speechPayload()", self.source)

    def test_voice_scope_only_collects_speech_settings(self) -> None:
        start = self.source.index("const speechKeys = [")
        end = self.source.index("function mediaPayload", start)
        implementation = self.source[start:end]

        self.assertIn("function speechPayload", implementation)
        self.assertIn("Object.fromEntries(speechKeys.map", implementation)
        self.assertIn("esphome_settings", implementation)
        self.assertNotIn("hydra_local_model_load_targets", implementation)


if __name__ == "__main__":
    unittest.main()
