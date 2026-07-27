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


class S3BoxScreenSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
        }
        fake.hget.return_value = "off"
        return fake

    def test_screen_controls_only_appear_for_a_specific_s3_box(self) -> None:
        fake_redis = self._redis()
        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            s3_keys = {
                str(field.get("key") or "")
                for field in native_live_settings.settings_fields("native:bedroom", board="s3-box")
            }
            voicepe_keys = {
                str(field.get("key") or "")
                for field in native_live_settings.settings_fields("native:kitchen", board="voice-pe")
            }
            global_keys = {
                str(field.get("key") or "")
                for field in native_live_settings.settings_fields()
            }

        expected = {
            "screen_section",
            "screen_brightness",
            "screen_night_mode_enabled",
            "screen_night_brightness",
            "screen_night_start",
            "screen_night_end",
        }
        self.assertTrue(expected.issubset(s3_keys))
        self.assertTrue(expected.isdisjoint(voicepe_keys))
        self.assertTrue(expected.isdisjoint(global_keys))

    def test_screen_values_are_normalized(self) -> None:
        values = native_live_settings.normalize_settings(
            {
                "screen_brightness": 140,
                "screen_night_mode_enabled": "yes",
                "screen_night_brightness": -5,
                "screen_night_start": "7:05",
                "screen_night_end": "not-a-time",
            }
        )

        self.assertEqual(values["screen_brightness"], 100)
        self.assertTrue(values["screen_night_mode_enabled"])
        self.assertEqual(values["screen_night_brightness"], 0)
        self.assertEqual(values["screen_night_start"], "07:05")
        self.assertEqual(values["screen_night_end"], "07:00")

    def test_firmware_time_sync_is_only_sent_to_s3_box(self) -> None:
        fake_redis = self._redis()
        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            s3_payload = native_live_settings.firmware_settings_snapshot(
                "native:bedroom",
                board="s3-box",
            )
            voicepe_payload = native_live_settings.firmware_settings_snapshot(
                "native:kitchen",
                board="voice-pe",
            )

        self.assertGreaterEqual(s3_payload["screen_local_time_seconds"], 0)
        self.assertLess(s3_payload["screen_local_time_seconds"], 24 * 60 * 60)
        self.assertIn("screen_brightness", s3_payload)
        self.assertNotIn("screen_local_time_seconds", voicepe_payload)
        self.assertNotIn("screen_brightness", voicepe_payload)


if __name__ == "__main__":
    unittest.main()
