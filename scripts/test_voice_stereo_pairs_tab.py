from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VOICE_COMPONENT = ROOT / "frontend" / "src" / "settings" / "components" / "VoiceSettings.vue"
VOICE_HOME = ROOT / "tater_voice" / "home.py"


class VoiceStereoPairsTabTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app_source = VOICE_COMPONENT.read_text(encoding="utf-8")
        cls.home_source = VOICE_HOME.read_text(encoding="utf-8")

    def test_stereo_pairs_tab_is_between_firmware_and_stats(self) -> None:
        firmware = self.app_source.index('{ id: "firmware"')
        stereo = self.app_source.index('{ id: "stereo"')
        stats = self.app_source.index('{ id: "stats"')

        self.assertLess(firmware, stereo)
        self.assertLess(stereo, stats)
        self.assertIn("<VoiceStereo v-else-if=\"activeTab === 'stereo' && !loading\"", self.app_source)

    def test_stereo_pairs_have_a_dedicated_runtime_panel(self) -> None:
        self.assertIn('{ id: "stereo"', self.app_source)
        self.assertIn("activeTab === 'stereo'", self.app_source)
        self.assertIn('"firmware", "stereo", "airplay", "platform"', self.home_source)
        self.assertIn('include_stereo_pairs = panel_token in {"", "stereo"}', self.home_source)
        self.assertIn("if include_stereo_pairs:", self.home_source)

    def test_satellites_panel_no_longer_mounts_stereo_pairs(self) -> None:
        satellites_start = self.app_source.index("<VoiceSatellites")
        stereo_start = self.app_source.index("<VoiceStereo", satellites_start)
        satellites_branch = self.app_source[satellites_start:stereo_start]

        self.assertNotIn("VoiceStereo", satellites_branch)
        self.assertNotIn("stereo_pairs", satellites_branch)


if __name__ == "__main__":
    unittest.main()
