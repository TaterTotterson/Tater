#!/usr/bin/env python3
from __future__ import annotations

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


class WakeWordCatalogTests(unittest.TestCase):
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
