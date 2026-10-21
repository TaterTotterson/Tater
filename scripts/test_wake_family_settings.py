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
    @staticmethod
    def _custom_mww_profile(url: str) -> dict[str, object]:
        return {
            "wake_profile_key": f"custom:{url}",
            "wake_profile_name": "Jarvis",
            "wake_profile_source_url": url,
            "wake_profile_model_url": url.rsplit(".", 1)[0] + ".tflite",
            "wake_profile_threshold": 0.82,
            "wake_profile_sliding_window": 4,
            "wake_profile_close_miss_threshold": 0.66,
            "wake_profile_error": "",
            "wake_model_revision": "a" * 64,
        }

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

    def test_unified_migration_preserves_a_custom_mww_profile_over_default_echo(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("mww"): {
                    "wake_detector_mode": "mww",
                    "wake_word": "custom_url",
                    "wake_word_url": "https://models.example/jarvis.json",
                    "wake_profile_name": "Jarvis",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_detector_mode": "dual",
                    "wake_word": "hey_tater",
                    "oww_wake_word": "hey_tater",
                    "wake_profile_name": "Hey Tater",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            shared = native_live_settings.shared_wake_settings_snapshot()

        self.assertEqual(shared["wake_detector_mode"], "mww")
        self.assertEqual(shared["wake_profile_name"], "Jarvis")
        self.assertEqual(shared["wake_word_url"], "https://models.example/jarvis.json")
        self.assertEqual(
            redis.rows[native_live_settings.SETTINGS_HASH_KEY][
                native_live_settings.UNIFIED_WAKE_SETTINGS_MIGRATION_KEY
            ],
            "true",
        )

    def test_oww_where_supported_projects_oww_to_capable_and_mww_to_esp32(self) -> None:
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
            capable_firmware = native_live_settings.firmware_settings_snapshot(
                board="third-reality-s420",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )
            esp32_firmware = native_live_settings.firmware_settings_snapshot(
                board="voice-pe",
                capabilities={"local_wake": True},
            )

        self.assertEqual(result["settings"]["wake_detector_mode"], "oww")
        self.assertEqual(capable_firmware["wake_detector_mode"], "oww")
        self.assertFalse(capable_firmware["wake_mww_enabled"])
        self.assertTrue(capable_firmware["wake_oww_enabled"])
        self.assertNotIn("wake_word", capable_firmware)
        self.assertNotIn("wake_word_url", capable_firmware)
        self.assertNotIn("wake_model_revision", capable_firmware)
        self.assertTrue(esp32_firmware["wake_mww_enabled"])
        self.assertNotIn("wake_detector_mode", esp32_firmware)
        self.assertNotIn("wake_oww_enabled", esp32_firmware)
        self.assertNotIn("oww_wake_word", esp32_firmware)

    def test_dual_to_mww_removes_stale_oww_package_from_firmware_payload(self) -> None:
        custom_url = "https://models.example/jarvis.json"
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "dual",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "true",
                    "wake_word": "custom_url",
                    "wake_word_url": "https://models.example/old.json",
                    "oww_wake_word": "custom_url",
                    "oww_wake_word_url": "https://models.example/old.wake-bundle.json",
                },
            }
        )
        with (
            mock.patch.object(native_live_settings, "redis_client", redis),
            mock.patch.object(
                native_live_settings,
                "_fetch_wake_profile_json",
                return_value=self._custom_mww_profile(custom_url),
            ),
        ):
            native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "mww",
                    "wake_word": "custom_url",
                    "wake_word_url": custom_url,
                },
            )
            firmware = native_live_settings.firmware_settings_snapshot(
                board="biscuit",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(firmware["wake_detector_mode"], "mww")
        self.assertTrue(firmware["wake_mww_enabled"])
        self.assertFalse(firmware["wake_oww_enabled"])
        self.assertEqual(firmware["wake_word_url"], custom_url)
        self.assertNotIn("oww_wake_word", firmware)
        self.assertNotIn("oww_wake_word_url", firmware)

    def test_mww_to_dual_replaces_custom_mww_with_matching_builtin_pair(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "mww",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "false",
                    "wake_word": "custom_url",
                    "wake_word_url": "https://models.example/jarvis.json",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "dual",
                    "oww_wake_word": "hey_tater",
                },
            )
            firmware = native_live_settings.firmware_settings_snapshot(
                board="rook",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(firmware["wake_detector_mode"], "mww_oww")
        self.assertTrue(firmware["wake_mww_enabled"])
        self.assertTrue(firmware["wake_oww_enabled"])
        self.assertEqual(firmware["wake_word"], "hey_tater")
        self.assertEqual(firmware["oww_wake_word"], "hey_tater")
        self.assertEqual(firmware["wake_word_url"], "")
        self.assertEqual(firmware["oww_wake_word_url"], "")

    def test_invalid_active_custom_url_is_rejected_before_persisting(self) -> None:
        key = native_live_settings.wake_family_hash_key("echo")
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                key: {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "mww",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "false",
                    "wake_word": "hey_tater",
                },
            }
        )
        before = dict(redis.rows[key])
        with (
            mock.patch.object(native_live_settings, "redis_client", redis),
            self.assertRaisesRegex(ValueError, "missing or invalid"),
        ):
            native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "mww",
                    "wake_word": "custom_url",
                    "wake_word_url": "",
                },
            )
        self.assertEqual(redis.rows[key], before)

    def test_non_oww_satellites_receive_only_the_mww_profile(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "dual",
                    "wake_mww_enabled": "true",
                    "wake_oww_enabled": "true",
                    "wake_word": "custom_url",
                    "wake_word_url": "https://models.example/shared.json",
                    "wake_profile_name": "Jarvis",
                    "oww_wake_word": "custom_url",
                    "oww_wake_word_url": "https://models.example/shared.wake-bundle.json",
                    "oww_profile_name": "Jarvis",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            firmware = native_live_settings.firmware_settings_snapshot(
                board="voice-pe",
                capabilities={"local_wake": True},
            )

        self.assertEqual(firmware["wake_word_url"], "https://models.example/shared.json")
        self.assertTrue(firmware["wake_mww_enabled"])
        self.assertNotIn("wake_detector_mode", firmware)
        self.assertNotIn("wake_oww_enabled", firmware)
        self.assertNotIn("oww_wake_word", firmware)
        self.assertNotIn("oww_wake_word_url", firmware)

    def test_ui_uses_shared_single_and_dual_mode_rows(self) -> None:
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
        self.assertIn('Single wake engine', source)
        self.assertIn('Dual wake engines', source)
        self.assertIn('NanoWakeWord architecture', source)
        self.assertIn('Step 1 · All satellites', source)
        self.assertIn('=== "wake_model_settings"', source)
        self.assertNotIn('=== "mww_satellite_model_settings"', source)
        self.assertNotIn('=== "echo_satellite_model_settings"', source)
        self.assertNotIn('v-model="wakeValues.wake_mww_enabled"', source)
        self.assertNotIn('v-model="wakeValues.wake_oww_enabled"', source)
        self.assertEqual(source.count("defer_push: true"), 1)

    def test_detector_mode_is_global_but_not_in_satellite_popups(self) -> None:
        global_keys = {
            field["key"]
            for field in native_live_settings.settings_fields(wake_family="echo")
        }
        popup_fields = {
            field["key"]: field
            for field in native_live_settings.settings_fields(
                "native:test-echo",
                board="biscuit",
                capabilities={"openwakeword": True},
            )
        }
        popup_keys = set(popup_fields)

        self.assertIn("wake_detector_mode", global_keys)
        self.assertNotIn("wake_detector_mode", popup_keys)
        self.assertIn("wake_tuning_section", popup_keys)
        self.assertIn("wake_sensitivity", popup_keys)
        self.assertIn("wake_environment", popup_keys)
        self.assertIn(
            {"value": "tv_nearby", "label": "TV Nearby"},
            popup_fields["wake_environment"]["options"],
        )
        self.assertNotIn("mww_tuning_override_enabled", popup_keys)
        self.assertNotIn("oww_tuning_override_enabled", popup_keys)
        self.assertNotIn("nww_tuning_override_enabled", popup_keys)
        self.assertNotIn("wake_tuning_override_enabled", popup_keys)

    def test_shared_wake_form_contains_all_model_override_controls(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.UNIFIED_WAKE_SETTINGS_MIGRATION_KEY: "true",
                }
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            keys = {
                field["key"]
                for section in native_live_settings.shared_wake_model_settings_sections()
                for field in section["fields"]
            }
        self.assertTrue(
            {
                "mww_tuning_override_enabled",
                "mww_threshold_override",
                "mww_sliding_window_override",
                "oww_tuning_override_enabled",
                "oww_threshold_override",
                "oww_patience_override",
                "nww_tuning_override_enabled",
                "nww_threshold_override",
                "nww_patience_override",
            }.issubset(keys)
        )

    def test_model_overrides_project_only_the_selected_detector_values(self) -> None:
        base = {
            "wake_detector_mode": "mww_nww",
            "mww_tuning_override_enabled": True,
            "mww_threshold_override": 0.91,
            "mww_sliding_window_override": 4,
            "oww_tuning_override_enabled": True,
            "oww_threshold_override": 0.79,
            "oww_patience_override": 2,
            "nww_tuning_override_enabled": True,
            "nww_threshold_override": 0.985,
            "nww_patience_override": 2,
        }
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.UNIFIED_WAKE_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    key: str(value).lower() if isinstance(value, bool) else str(value)
                    for key, value in base.items()
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            nww = native_live_settings.firmware_settings_snapshot(
                board="biscuit",
                capabilities={"nanowakeword": True, "wake_detector_selection": True},
            )
            native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_detector_mode": "oww",
                    "oww_tuning_override_enabled": True,
                    "oww_threshold_override": 0.79,
                    "oww_patience_override": 2,
                },
            )
            oww = native_live_settings.firmware_settings_snapshot(
                board="biscuit",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(nww["wake_threshold"], 0.91)
        self.assertEqual(nww["wake_sliding_window"], 4)
        self.assertEqual(nww["nww_threshold"], 0.985)
        self.assertEqual(nww["nww_patience"], 2)
        self.assertNotIn("oww_threshold", nww)
        self.assertNotIn("oww_patience", nww)
        self.assertEqual(oww["oww_threshold"], 0.79)
        self.assertEqual(oww["oww_patience"], 2)
        self.assertNotIn("nww_threshold", oww)
        self.assertNotIn("nww_patience", oww)

    def test_persisted_oww_only_profile_is_presented_as_single_oww(self) -> None:
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
        self.assertEqual(fields["wake_detector_mode"]["value"], "oww")
        self.assertEqual(
            fields["wake_detector_mode"]["options"],
            [
                {"value": "mww", "label": "MWW", "group": "single"},
                {"value": "oww", "label": "OWW", "group": "single"},
                {"value": "nww", "label": "NWW", "group": "single"},
                {"value": "mww_oww", "label": "MWW + OWW", "group": "dual"},
                {"value": "mww_nww", "label": "MWW + NWW", "group": "dual"},
            ],
        )
        self.assertEqual(custom_option["label"], "Custom Wake Bundle")
        self.assertIn(
            {"value": "catalog", "label": "Wake Bundle Catalog"},
            fields["oww_wake_word"]["options"],
        )
        self.assertEqual(fields["oww_wake_word_catalog_url"]["label"], "Wake Bundle Catalog")
        self.assertEqual(fields["oww_wake_word_url"]["label"], "Wake Bundle URL")
        self.assertIn("sends microWakeWord to MWW-only satellites", fields["oww_wake_word_url"]["description"])

    def test_dual_ui_describes_one_generic_wake_bundle(self) -> None:
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
        self.assertEqual(custom_option["label"], "Custom Wake Bundle")
        self.assertIn(
            {"value": "catalog", "label": "Wake Bundle Catalog"},
            fields["oww_wake_word"]["options"],
        )
        self.assertEqual(fields["oww_wake_word_catalog_url"]["label"], "Wake Bundle Catalog")
        self.assertEqual(fields["oww_wake_word_url"]["label"], "Wake Bundle URL")
        self.assertIn("sends microWakeWord to MWW-only satellites", fields["oww_wake_word_url"]["description"])

    def test_nww_mode_projects_to_capable_firmware_and_falls_back_to_mww(self) -> None:
        bundle_url = "https://trainer.example/hey_tater.wake-bundle.json"
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "nww",
                    "wake_mww_enabled": "false",
                    "wake_oww_enabled": "false",
                    "wake_nww_enabled": "true",
                    "oww_wake_word": "custom_url",
                    "oww_wake_word_url": bundle_url,
                    "nww_architecture": "gru",
                    "nww_model_revision": "c" * 64,
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            capable = native_live_settings.firmware_settings_snapshot(
                board="biscuit",
                capabilities={"nanowakeword": True, "wake_detector_selection": True},
            )
            older = native_live_settings.firmware_settings_snapshot(
                board="biscuit",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(capable["wake_detector_mode"], "nww")
        self.assertFalse(capable["wake_mww_enabled"])
        self.assertTrue(capable["wake_nww_enabled"])
        self.assertEqual(capable["nww_wake_word"], "custom_url")
        self.assertEqual(capable["nww_wake_word_url"], bundle_url)
        self.assertEqual(capable["nww_architecture"], "gru")
        self.assertNotIn("oww_wake_word", capable)
        self.assertEqual(older["wake_detector_mode"], "mww")
        self.assertTrue(older["wake_mww_enabled"])
        self.assertFalse(older["wake_nww_enabled"])
        self.assertNotIn("nww_wake_word", older)

    def test_builtin_nww_mode_uses_the_on_device_hey_tater_models(self) -> None:
        redis = MemoryRedis(
            {
                native_live_settings.SETTINGS_HASH_KEY: {
                    native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                    native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
                },
                native_live_settings.wake_family_hash_key("echo"): {
                    "wake_engine": "micro_wake_word",
                    "wake_detector_mode": "nww",
                    "wake_mww_enabled": "false",
                    "wake_oww_enabled": "false",
                    "wake_nww_enabled": "true",
                    "oww_wake_word": "hey_tater",
                    "nww_architecture": "cnn",
                },
            }
        )
        with mock.patch.object(native_live_settings, "redis_client", redis):
            saved = native_live_settings.save_wake_family_settings(
                "echo",
                {
                    "wake_detector_mode": "nww",
                    "oww_wake_word": "hey_tater",
                    "nww_architecture": "gru",
                },
            )
            firmware = native_live_settings.firmware_settings_snapshot(
                board="checkers",
                capabilities={"nanowakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(saved["settings"]["wake_detector_mode"], "nww")
        self.assertEqual(saved["settings"]["nww_available_architectures"], "cnn,gru")
        self.assertEqual(firmware["nww_wake_word"], "hey_tater")
        self.assertEqual(firmware["nww_architecture"], "gru")
        self.assertNotIn("nww_wake_word_url", firmware)


if __name__ == "__main__":
    unittest.main()
