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


class MemoryRedis:
    def __init__(self, rows: dict[str, dict[str, str]] | None = None) -> None:
        self.rows = {key: dict(value) for key, value in (rows or {}).items()}

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.rows.get(str(key), {}))

    def hget(self, key: str, field: str):
        return self.rows.get(str(key), {}).get(str(field))

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        row = self.rows.setdefault(str(key), {})
        row.update({str(field): str(value) for field, value in mapping.items()})
        return len(mapping)

    def hdel(self, key: str, *fields: str) -> int:
        row = self.rows.setdefault(str(key), {})
        removed = 0
        for field in fields:
            if field in row:
                removed += 1
                del row[field]
        return removed

    def scan_iter(self, match: str):
        prefix = str(match).rstrip("*")
        return iter(key for key in self.rows if key.startswith(prefix))


class WakeFamilySettingsTests(unittest.TestCase):
    def test_existing_global_choice_seeds_both_families_once(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    "wake_engine": "micro_wake_word",
                    "wake_mww_enabled": "false",
                    "wake_oww_enabled": "true",
                    "oww_wake_word": "hey_tater",
                }
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            mww = native_live_settings.wake_family_settings_snapshot("mww")
            echo = native_live_settings.wake_family_settings_snapshot("echo")

        self.assertEqual(mww["wake_detector_mode"], "mww")
        self.assertTrue(mww["wake_mww_enabled"])
        self.assertFalse(mww["wake_oww_enabled"])
        self.assertEqual(echo["wake_detector_mode"], "oww")
        self.assertFalse(echo["wake_mww_enabled"])
        self.assertTrue(echo["wake_oww_enabled"])
        self.assertEqual(
            redis.rows[native_live_settings.SETTINGS_HASH_KEY][
                native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY
            ],
            "true",
        )

    def test_echo_mode_selector_maps_to_legacy_firmware_booleans(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_detector_mode": "dual",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "true",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            result = native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "oww",
                    "oww_wake_word": "hey_tater",
                },
            )
            firmware = native_live_settings.firmware_settings_snapshot(
                board="future-echo",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(result["settings"]["wake_detector_mode"], "oww")
        self.assertFalse(firmware["wake_mww_enabled"])
        self.assertTrue(firmware["wake_oww_enabled"])

    def test_non_oww_satellites_receive_only_the_mww_profile(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("mww"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "mww",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "false",
                    "wake_word": "custom_url",
                    "wake_word_url": "https://models.example/esp.json",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "oww",
                    "wake_mww_enabled": "false",
                    "wake_oww_enabled": "true",
                    "oww_wake_word": "custom_url",
                    "oww_wake_word_url": "https://models.example/echo.wake-bundle.json",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            firmware = native_live_settings.firmware_settings_snapshot(
                board="voice-pe",
                capabilities={"local_wake": True},
            )

        self.assertEqual(firmware["wake_word_url"], "https://models.example/esp.json")
        self.assertTrue(firmware["wake_mww_enabled"])
        self.assertNotIn("wake_oww_enabled", firmware)
        self.assertNotIn("oww_wake_word", firmware)
        self.assertNotIn("oww_wake_word_url", firmware)

    def test_echo_ui_uses_one_three_choice_mode_selector(self) -> None:
        source = (
            REPO_ROOT
            / "frontend"
            / "src"
            / "settings"
            / "components"
            / "models"
            / "WakeWordModels.vue"
        ).read_text(encoding="utf-8")
        self.assertIn('wake_detector_mode', source)
        self.assertIn('Dual Wake Word', source)
        self.assertNotIn('v-model="wakeValues.wake_mww_enabled"', source)
        self.assertNotIn('v-model="wakeValues.wake_oww_enabled"', source)

    def test_oww_only_ui_describes_bundle_as_an_oww_package(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_detector_mode": "oww",
                    "wake_mww_enabled": "false",
                    "wake_oww_enabled": "true",
                    "oww_wake_word": "custom_url",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            fields = {
                field["key"]: field
                for field in native_live_settings.settings_fields(wake_family="echo")
            }

        custom_option = next(
            option
            for option in fields["oww_wake_word"]["options"]
            if option["value"] == "custom_url"
        )
        self.assertEqual(custom_option["label"], "Custom openWakeWord Package")
        self.assertIn(
            {"value": "catalog", "label": "openWakeWord Catalog"},
            fields["oww_wake_word"]["options"],
        )
        self.assertEqual(fields["oww_wake_word_catalog_url"]["label"], "openWakeWord Catalog")
        self.assertEqual(fields["oww_wake_word_url"]["label"], "openWakeWord Package URL")
        self.assertIn("Only its openWakeWord model is used", fields["oww_wake_word_url"]["description"])

    def test_dual_ui_describes_bundle_as_a_matched_pair(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_detector_mode": "dual",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "true",
                    "oww_wake_word": "custom_url",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            fields = {
                field["key"]: field
                for field in native_live_settings.settings_fields(wake_family="echo")
            }

        custom_option = next(
            option
            for option in fields["oww_wake_word"]["options"]
            if option["value"] == "custom_url"
        )
        self.assertEqual(custom_option["label"], "Custom Matched Bundle")
        self.assertIn(
            {"value": "catalog", "label": "Dual Wake Word Catalog"},
            fields["oww_wake_word"]["options"],
        )
        self.assertEqual(fields["oww_wake_word_catalog_url"]["label"], "Dual Wake Word Catalog")
        self.assertEqual(fields["oww_wake_word_url"]["label"], "Matched Dual-Model Bundle URL")
        self.assertIn("installs both wake models together", fields["oww_wake_word_url"]["description"])


if __name__ == "__main__":
    unittest.main()
