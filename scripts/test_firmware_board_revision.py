#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import types
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Firmware version comparison does not depend on Tater's external services.
# Stub those runtime modules so this focused regression test also runs in the
# repository's minimal Python environment.
helpers_stub = types.ModuleType("helpers")
helpers_stub.redis_client = object()
sys.modules["helpers"] = helpers_stub
sys.modules["tater_voice.display_bus"] = types.ModuleType("tater_voice.display_bus")
runtime_stub = types.ModuleType("tater_voice.runtime")
runtime_stub.text = lambda value: str(value or "").strip()
runtime_stub.lower = lambda value: runtime_stub.text(value).lower()
sys.modules["tater_voice.runtime"] = runtime_stub
sys.modules["tater_voice.ui_helpers"] = types.ModuleType("tater_voice.ui_helpers")

from tater_voice import firmware  # noqa: E402


class FirmwareBoardRevisionTests(unittest.TestCase):
    def test_revision_orders_after_base_release(self) -> None:
        self.assertGreater(
            firmware._semver_tuple("native-satellite1-0.3.3-rev1"),
            firmware._semver_tuple("native-satellite1-0.3.3"),
        )
        self.assertGreater(
            firmware._semver_tuple("native-satellite1-0.3.3-r2"),
            firmware._semver_tuple("native-satellite1-0.3.3-rev1"),
        )

    def test_next_patch_orders_after_any_prior_revision(self) -> None:
        self.assertGreater(
            firmware._semver_tuple("native-satellite1-0.3.4"),
            firmware._semver_tuple("native-satellite1-0.3.3-rev99"),
        )

    def test_numeric_suffix_without_revision_marker_is_ignored(self) -> None:
        self.assertEqual(
            firmware._semver_tuple("native-satellite1-0.3.3-1"),
            firmware._semver_tuple("native-satellite1-0.3.3"),
        )

    @mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={})
    def test_only_revised_board_reports_an_update(self, _recorded: mock.Mock) -> None:
        satellite = firmware._firmware_version_snapshot(
            "native:sat1",
            "satellite1",
            {"project_version": "native-satellite1-0.3.3"},
            "native-satellite1-0.3.3-rev1",
        )
        voicepe = firmware._firmware_version_snapshot(
            "native:voicepe",
            "voicepe",
            {"project_version": "native-voicepe-0.3.3"},
            "native-voicepe-0.3.3",
        )

        self.assertTrue(satellite["update_available"])
        self.assertFalse(voicepe["update_available"])


if __name__ == "__main__":
    unittest.main()
