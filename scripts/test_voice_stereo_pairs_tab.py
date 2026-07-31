from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "tateros_static" / "app.js"
VOICE_HOME = ROOT / "tater_voice" / "home.py"


class VoiceStereoPairsTabTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app_source = APP_JS.read_text(encoding="utf-8")
        cls.home_source = VOICE_HOME.read_text(encoding="utf-8")

    def test_stereo_pairs_tab_is_between_firmware_and_stats(self) -> None:
        firmware = self.app_source.index('data-esphome-tab="firmware"')
        stereo = self.app_source.index('data-esphome-tab="stereo"')
        stats = self.app_source.index('data-esphome-tab="stats"')

        self.assertLess(firmware, stereo)
        self.assertLess(stereo, stats)
        self.assertIn('data-esphome-panel="stereo"', self.app_source)
        self.assertIn('id="settings-esphome-runtime-stereo"', self.app_source)

    def test_stereo_pairs_have_a_dedicated_runtime_panel(self) -> None:
        self.assertIn('"firmware", "stereo", "platform"', self.app_source)
        self.assertIn('targetPanel === "stereo"', self.app_source)
        self.assertIn('"firmware", "stereo", "platform"', self.home_source)
        self.assertIn('include_stereo_pairs = panel_token in {"", "stereo"}', self.home_source)
        self.assertIn("if include_stereo_pairs:", self.home_source)

    def test_satellites_panel_no_longer_mounts_stereo_pairs(self) -> None:
        render_start = self.app_source.index("wakeVerifierHost.innerHTML")
        satellites_branch_start = self.app_source.index('if (targetPanel === "satellites") {', render_start)
        firmware_branch_start = self.app_source.index('if (targetPanel === "firmware") {', satellites_branch_start)
        satellites_branch = self.app_source[satellites_branch_start:firmware_branch_start]

        self.assertNotIn("stereoPairHtml", satellites_branch)
        self.assertNotIn("core-tab-items-group-stereo-pair", satellites_branch)


if __name__ == "__main__":
    unittest.main()
