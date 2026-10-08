#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import native_live_settings, wake_word_catalog  # noqa: E402


V1_URL = (
    "https://raw.githubusercontent.com/TaterTotterson/"
    "Tater-Wake-Words/main/microWakeWordsV1/hey_tater.json"
)
V6_URL = (
    "https://raw.githubusercontent.com/TaterTotterson/"
    "Tater-Wake-Words/main/microWakeWordsV6/hey_tater.json"
)
V7_URL = (
    "https://raw.githubusercontent.com/TaterTotterson/"
    "Tater-Wake-Words/main/microWakeWordsV7/hey_tater.json"
)
V7_BUNDLE_URL = (
    "https://raw.githubusercontent.com/TaterTotterson/"
    "Tater-Wake-Words/main/microWakeWordsV7/hey_tater.wake-bundle.json"
)


class WakeWordCatalogTests(unittest.TestCase):
    def test_new_echo_defaults_use_the_paired_hey_tater_detectors(self) -> None:
        settings = native_live_settings.normalize_settings({})
        self.assertTrue(settings["wake_mww_enabled"])
        self.assertTrue(settings["wake_oww_enabled"])
        self.assertEqual(settings["wake_word"], "hey_tater")
        self.assertEqual(settings["oww_wake_word"], "hey_tater")
        self.assertEqual(settings["wake_threshold"], 0.98)
        self.assertEqual(settings["close_miss_threshold"], 0.81)
        self.assertEqual(settings["oww_profile_confirmation_threshold"], 0.92)

    def test_explicit_detector_choice_survives_the_new_default(self) -> None:
        settings = native_live_settings.normalize_settings(
            {
                "wake_mww_enabled": True,
                "wake_oww_enabled": False,
            }
        )
        self.assertTrue(settings["wake_mww_enabled"])
        self.assertFalse(settings["wake_oww_enabled"])

    def test_unrelated_save_preserves_an_existing_oww_opt_out(self) -> None:
        existing = native_live_settings.normalize_settings({"wake_oww_enabled": False})
        with (
            mock.patch.object(
                native_live_settings,
                "_global_settings_with_migration",
                return_value=dict(existing),
            ),
            mock.patch.object(native_live_settings, "_raw_settings", return_value={}),
            mock.patch.object(
                native_live_settings,
                "settings_snapshot",
                return_value=dict(existing),
            ),
            mock.patch.object(native_live_settings.redis_client, "hset") as hset,
        ):
            result = native_live_settings.save_settings({"wake_sound_enabled": True})

        self.assertFalse(result["settings"]["wake_oww_enabled"])
        self.assertTrue(result["settings"]["wake_sound_enabled"])
        hset.assert_called_once()

    def test_custom_oww_bundle_profile_is_exposed_to_firmware(self) -> None:
        bundle = {
            "schema_version": 1,
            "type": "tater_wake_word_bundle",
            "wake_word": "Jojo",
            "micro_wake_word": {
                "manifest": "jojo.json",
                "model": "jojo.tflite",
                "manifest_sha256": "a" * 64,
                "model_sha256": "b" * 64,
            },
            "open_wake_word": {
                "metadata": "jojo.oww.json",
                "metadata_sha256": "c" * 64,
                "recommended_threshold": 0.91,
                "recommended_patience": 4,
                "recommended_confirmation_threshold": 0.86,
                "recommended_confirmation_patience": 2,
                "artifacts": {"onnx": {"file": "jojo.oww.onnx", "sha256": "d" * 64}},
            },
        }

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit: int) -> bytes:
                return __import__("json").dumps(bundle).encode()

        with mock.patch.object(native_live_settings, "urlopen", return_value=FakeResponse()):
            profile = native_live_settings._fetch_oww_profile_json("https://example.test/jojo.wake-bundle.json")
        settings = native_live_settings.normalize_settings(
            {
                "wake_oww_enabled": True,
                "oww_wake_word": "custom_url",
                "oww_wake_word_url": "https://example.test/jojo.wake-bundle.json",
                **profile,
            }
        )
        self.assertTrue(settings["wake_oww_enabled"])
        self.assertEqual(settings["oww_profile_name"], "Jojo")
        self.assertEqual(settings["oww_profile_threshold"], 0.91)
        self.assertEqual(settings["oww_profile_patience"], 4)
        self.assertEqual(settings["oww_profile_confirmation_threshold"], 0.86)
        self.assertEqual(settings["oww_profile_confirmation_patience"], 2)
        firmware = {key: settings[key] for key in native_live_settings.FIRMWARE_SETTING_KEYS}
        self.assertEqual(firmware["oww_wake_word_url"], "https://example.test/jojo.wake-bundle.json")
        self.assertTrue(firmware["oww_model_revision"])

    def test_dual_profile_uses_mww_manifest_named_by_the_bundle(self) -> None:
        manifest = json.dumps({"wake_word": "Jojo", "model": "jojo.tflite"}).encode()
        bundle = {
            "schema_version": 1,
            "type": "tater_wake_word_bundle",
            "wake_word": "Jojo",
            "micro_wake_word": {
                "manifest": "jojo.json",
                "model": "jojo.tflite",
                "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
                "model_sha256": "b" * 64,
            },
            "open_wake_word": {
                "metadata": "jojo.oww.json",
                "metadata_sha256": "c" * 64,
                "recommended_threshold": 0.91,
                "recommended_patience": 4,
                "recommended_confirmation_threshold": 0.86,
                "recommended_confirmation_patience": 2,
                "artifacts": {
                    "onnx": {"file": "jojo.oww.onnx", "sha256": "d" * 64},
                },
            },
        }

        class FakeResponse:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit: int) -> bytes:
                return self.body

        with mock.patch.object(
            native_live_settings,
            "urlopen",
            side_effect=[FakeResponse(json.dumps(bundle).encode()), FakeResponse(manifest)],
        ):
            profile = native_live_settings._dual_profile_for_save(
                {
                    "wake_mww_enabled": True,
                    "wake_oww_enabled": True,
                    "oww_wake_word": "custom_url",
                    "oww_wake_word_url": "https://trainer.test/jojo.wake-bundle.json",
                },
                {},
            )

        self.assertEqual(profile["wake_profile_name"], "Jojo")
        self.assertEqual(profile["wake_profile_source_url"], "https://trainer.test/jojo.json")
        self.assertEqual(profile["oww_profile_name"], "Jojo")

    def test_custom_model_revision_is_stable_until_package_changes(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit: int) -> bytes:
                return self.payload

        first = b'{"wake_word":"Hey Test","model":"hey_test.tflite"}'
        updated = b'{"wake_word":"Hey Test","model":"hey_test_v2.tflite"}'
        with mock.patch.object(native_live_settings, "urlopen", return_value=FakeResponse(first)):
            first_profile = native_live_settings._fetch_wake_profile_json("https://example.test/hey.json")
        with mock.patch.object(native_live_settings, "urlopen", return_value=FakeResponse(first)):
            same_profile = native_live_settings._fetch_wake_profile_json("https://example.test/hey.json")
        with mock.patch.object(native_live_settings, "urlopen", return_value=FakeResponse(updated)):
            updated_profile = native_live_settings._fetch_wake_profile_json("https://example.test/hey.json")

        self.assertEqual(first_profile["wake_model_revision"], same_profile["wake_model_revision"])
        self.assertNotEqual(first_profile["wake_model_revision"], updated_profile["wake_model_revision"])

    def test_manifest_parser_keeps_versioned_official_models_only(self) -> None:
        entries = wake_word_catalog.entries_from_manifest(
            {
                "entries": [
                    {
                        "source": "microWakeWordsV6",
                        "slug": "hey_tater",
                        "label": "Hey Tater",
                        "url": V6_URL,
                    },
                    {
                        "source": "microWakeWordsV1",
                        "slug": "hey_tater",
                        "label": "Hey Tater",
                        "path": "microWakeWordsV1/hey_tater.json",
                    },
                    {
                        "source": "microWakeWordsV7",
                        "slug": "not_official",
                        "label": "Not Official",
                        "url": "https://example.test/not_official.json",
                    },
                ]
            }
        )

        self.assertEqual([entry["url"] for entry in entries], [V1_URL, V6_URL])
        self.assertEqual([entry["version_label"] for entry in entries], ["V1", "V6"])

    def test_manifest_parser_exposes_verified_v7_bundle_to_oww_catalogs(self) -> None:
        entries = wake_word_catalog.entries_from_manifest(
            {
                "schema_version": 2,
                "entries": [
                    {
                        "source": "microWakeWordsV7",
                        "slug": "hey_tater",
                        "label": "Hey Tater",
                        "url": V7_URL,
                        "dual_model": True,
                        "bundle_url": V7_BUNDLE_URL,
                        "openwakeword_metadata_url": V7_BUNDLE_URL.replace(
                            ".wake-bundle.json", ".oww.json"
                        ),
                        "openwakeword_model_url": V7_BUNDLE_URL.replace(
                            ".wake-bundle.json", ".oww.onnx"
                        ),
                    }
                ],
            }
        )

        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0]["dual_model"])
        self.assertEqual(entries[0]["bundle_url"], V7_BUNDLE_URL)

        catalog = {"entries": entries, "warning": ""}
        with mock.patch.object(wake_word_catalog, "load_catalog", return_value=catalog):
            oww = wake_word_catalog.field_payload(model_type="oww")
            dual = wake_word_catalog.field_payload(model_type="dual")

        expected = [{"value": V7_BUNDLE_URL, "label": "Hey Tater [V7]"}]
        self.assertEqual(oww["options"], expected)
        self.assertEqual(dual["options"], expected)
        self.assertEqual(oww["catalog_type"], "oww")
        self.assertEqual(dual["catalog_type"], "dual")

    def test_empty_oww_catalog_is_ready_for_future_v7_models(self) -> None:
        with mock.patch.object(
            wake_word_catalog,
            "load_catalog",
            return_value={"entries": [], "warning": ""},
        ):
            field = wake_word_catalog.field_payload(model_type="oww")

        self.assertEqual(field["options"], [])
        self.assertEqual(field["count"], 0)
        self.assertIn("populate automatically", field["description"])

    def test_picker_labels_show_each_model_version(self) -> None:
        with mock.patch.object(
            wake_word_catalog,
            "load_catalog",
            return_value={
                "entries": [
                    {"label": "Hey Tater", "url": V1_URL, "version_label": "V1"},
                    {"label": "Hey Tater", "url": V6_URL, "version_label": "V6"},
                ],
                "versions": [1, 6],
                "warning": "",
            },
        ):
            field = wake_word_catalog.field_payload(current_url=V6_URL)

        self.assertEqual(
            field["options"],
            [
                {"value": V1_URL, "label": "Hey Tater [V1]"},
                {"value": V6_URL, "label": "Hey Tater [V6]"},
            ],
        )
        self.assertEqual(field["selected_url"], V6_URL)
        self.assertIn("V1–V6", field["description"])

    def test_global_wake_word_source_exposes_catalog_picker(self) -> None:
        current = native_live_settings.normalize_settings({})
        with (
            mock.patch.object(native_live_settings, "settings_snapshot", return_value=current),
            mock.patch.object(
                native_live_settings.wake_word_catalog,
                "field_payload",
                return_value={
                    "options": [{"value": V6_URL, "label": "Hey Tater [V6]"}],
                    "selected_url": "",
                    "description": "One official model.",
                },
            ),
        ):
            fields = native_live_settings.settings_fields()

        by_key = {str(field.get("key") or ""): field for field in fields}
        self.assertIn(
            {"value": "catalog", "label": "Tater Wake Word Catalog"},
            by_key["wake_word"]["options"],
        )
        self.assertEqual(
            by_key["wake_word_catalog_url"]["show_when"],
            {"source_key": "wake_word", "equals": "catalog"},
        )
        self.assertEqual(
            by_key["wake_word_catalog_url"]["options"],
            [{"value": V6_URL, "label": "Hey Tater [V6]"}],
        )

    def test_saved_catalog_model_is_inferred_for_the_ui(self) -> None:
        current = native_live_settings.normalize_settings({})
        current.update(
            {
                "wake_word": "custom_url",
                "wake_word_url": V6_URL,
                "wake_profile_name": "Hey Tater",
            }
        )
        with (
            mock.patch.object(native_live_settings, "settings_snapshot", return_value=current),
            mock.patch.object(
                native_live_settings.wake_word_catalog,
                "field_payload",
                return_value={
                    "options": [{"value": V6_URL, "label": "Hey Tater [V6]"}],
                    "selected_url": V6_URL,
                    "description": "One official model.",
                },
            ),
        ):
            fields = native_live_settings.settings_fields()

        by_key = {str(field.get("key") or ""): field for field in fields}
        self.assertEqual(by_key["wake_word"]["value"], "catalog")
        self.assertEqual(by_key["wake_word_catalog_url"]["value"], V6_URL)

    def test_catalog_selection_uses_existing_firmware_custom_url_contract(self) -> None:
        resolved = native_live_settings.resolve_wake_word_source_values(
            {
                "wake_word": "catalog",
                "wake_word_catalog_url": V6_URL,
                "capture_wake_audio": True,
            }
        )

        self.assertEqual(resolved["wake_word"], "custom_url")
        self.assertEqual(resolved["wake_word_url"], V6_URL)
        self.assertTrue(resolved["capture_wake_audio"])
        self.assertNotIn("wake_word_catalog_url", resolved)

    def test_oww_catalog_selection_uses_existing_bundle_contract(self) -> None:
        resolved = native_live_settings.resolve_wake_word_source_values(
            {
                "wake_detector_mode": "oww",
                "oww_wake_word": "catalog",
                "oww_wake_word_catalog_url": V7_BUNDLE_URL,
            }
        )

        self.assertEqual(resolved["oww_wake_word"], "custom_url")
        self.assertEqual(resolved["oww_wake_word_url"], V7_BUNDLE_URL)
        self.assertNotIn("oww_wake_word_catalog_url", resolved)

    def test_dual_catalog_rejects_a_mww_manifest_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "Dual Wake Word Catalog"):
            native_live_settings.resolve_wake_word_source_values(
                {
                    "wake_detector_mode": "dual",
                    "oww_wake_word": "catalog",
                    "oww_wake_word_catalog_url": V7_URL,
                }
            )

    def test_catalog_selection_rejects_non_catalog_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "official Tater Wake Word Catalog"):
            native_live_settings.resolve_wake_word_source_values(
                {
                    "wake_word": "catalog",
                    "wake_word_catalog_url": "https://example.test/not_official.json",
                }
            )


if __name__ == "__main__":
    unittest.main()
