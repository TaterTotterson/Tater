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

helpers_stub = types.ModuleType("helpers")
helpers_stub.redis_client = mock.Mock()
sys.modules.setdefault("helpers", helpers_stub)

from tater_voice import native_live_settings  # noqa: E402


class EchoScreenPowerSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
        }
        fake.hget.return_value = "off"
        return fake

    @staticmethod
    def _capabilities() -> dict[str, object]:
        return {"screen": True, "screen_power_management_version": 1}

    def test_all_echo_screen_boards_offer_per_satellite_display_and_night_controls(self) -> None:
        expected = {
            "screen_section",
            "screen_brightness",
            "screen_auto_brightness_enabled",
            "screen_auto_brightness_minimum",
            "screen_night_mode_enabled",
            "screen_night_start",
            "screen_night_end",
            "screen_night_display",
            "screen_night_brightness",
        }
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            for board in ("checkers", "crown", "athena", "rook"):
                fields = native_live_settings.settings_fields(
                    f"native:{board}", board=board, capabilities=self._capabilities()
                )
                by_key = {str(field.get("key") or ""): field for field in fields}
                self.assertTrue(expected.issubset(by_key), board)
                self.assertEqual(by_key["screen_section"]["label"], "Display & Night")
                self.assertEqual(by_key["screen_brightness"]["value"], 35)
                self.assertEqual(by_key["screen_brightness"]["default"], 35)
                self.assertEqual(by_key["screen_night_brightness"]["value"], 3)
                self.assertEqual(by_key["screen_night_brightness"]["default"], 3)
                self.assertEqual(
                    {str(option["value"]) for option in by_key["screen_night_display"]["options"]},
                    {"dim", "red_clock", "off"},
                )

    def test_old_echo_firmware_does_not_offer_controls_it_cannot_apply(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            fields = native_live_settings.settings_fields(
                "native:old-checkers", board="checkers", capabilities={"screen": True}
            )
        keys = {str(field.get("key") or "") for field in fields}
        self.assertTrue(native_live_settings._SCREEN_FIELD_KEYS.isdisjoint(keys))

    def test_values_are_normalized_and_sent_to_capable_echo_firmware(self) -> None:
        normalized = native_live_settings.normalize_settings(
            {
                "screen_auto_brightness_enabled": "yes",
                "screen_auto_brightness_minimum": 99,
                "screen_night_display": "clock",
            }
        )
        self.assertTrue(normalized["screen_auto_brightness_enabled"])
        self.assertEqual(normalized["screen_auto_brightness_minimum"], 50)
        self.assertEqual(normalized["screen_night_display"], "red_clock")

        fake = self._redis()
        fake.hgetall.side_effect = [
            {native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true"},
            {
                "screen_brightness": "42",
                "screen_auto_brightness_enabled": "true",
                "screen_auto_brightness_minimum": "8",
                "screen_night_mode_enabled": "true",
                "screen_night_start": "21:30",
                "screen_night_end": "06:15",
                "screen_night_display": "red_clock",
                "screen_night_brightness": "3",
            },
        ]
        with mock.patch.object(native_live_settings, "redis_client", fake):
            payload = native_live_settings.firmware_settings_snapshot(
                "native:athena", board="athena", capabilities=self._capabilities()
            )
        self.assertEqual(payload["screen_brightness"], 42)
        self.assertTrue(payload["screen_auto_brightness_enabled"])
        self.assertEqual(payload["screen_auto_brightness_minimum"], 8)
        self.assertTrue(payload["screen_night_mode_enabled"])
        self.assertEqual(payload["screen_night_display"], "red_clock")
        self.assertEqual(payload["screen_night_brightness"], 3)


if __name__ == "__main__":
    unittest.main()
