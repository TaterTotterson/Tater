#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import native_live_settings  # noqa: E402


class AutomaticAecSettingsTests(unittest.TestCase):
    AEC_KEYS = {"aec_enabled", "aec_strength_percent", "aec_delay_ms"}

    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
            "aec_enabled": "false",
            "aec_strength_percent": "17",
            "aec_delay_ms": "91",
        }
        fake.hget.return_value = "off"
        return fake

    def test_aec_controls_are_hidden_for_every_satellite_family(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            for board in (
                "biscuit",
                "checkers",
                "rook",
                "voice-pe",
                "s3-box-3",
                "thirdreality-s420",
                "satellite1",
            ):
                fields = native_live_settings.settings_fields(f"native:{board}", board=board)
                keys = {str(field.get("key") or "") for field in fields}
                self.assertTrue(self.AEC_KEYS.isdisjoint(keys), board)

    def test_aec_values_are_not_transmitted_to_firmware(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            for board in (
                "biscuit",
                "checkers",
                "rook",
                "voice-pe",
                "s3-box-3",
                "thirdreality-s420",
                "satellite1",
            ):
                payload = native_live_settings.firmware_settings_snapshot(
                    f"native:{board}",
                    board=board,
                )
                self.assertTrue(self.AEC_KEYS.isdisjoint(payload), board)

    def test_legacy_saved_values_remain_readable_but_inert(self) -> None:
        settings = native_live_settings.normalize_settings(
            {
                "aec_enabled": "true",
                "aec_strength_percent": "44",
                "aec_delay_ms": "123",
            }
        )

        self.assertTrue(settings["aec_enabled"])
        self.assertEqual(settings["aec_strength_percent"], 44)
        self.assertEqual(settings["aec_delay_ms"], 123)
        self.assertTrue(self.AEC_KEYS.isdisjoint(native_live_settings.FIRMWARE_SETTING_KEYS))


if __name__ == "__main__":
    unittest.main()
