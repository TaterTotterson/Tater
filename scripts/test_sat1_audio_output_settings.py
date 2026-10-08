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


class Sat1AudioOutputSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis(device_settings: dict[str, str] | None = None) -> mock.Mock:
        fake = mock.Mock()

        def hgetall(key: str) -> dict[str, str]:
            if str(key).startswith(native_live_settings.DEVICE_SETTINGS_HASH_PREFIX):
                return dict(device_settings or {})
            return {
                native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
            }

        fake.hgetall.side_effect = hgetall
        fake.hget.return_value = "off"
        return fake

    def test_sat1_shows_all_output_routes_and_other_boards_do_not(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            sat1_fields = native_live_settings.settings_fields(
                "native:office-sat1",
                board="satellite1-beta-rev41",
            )
            voice_pe_fields = native_live_settings.settings_fields(
                "native:office-voice-pe",
                board="voice-pe",
            )

        sat1 = {str(field.get("key") or ""): field for field in sat1_fields}
        voice_pe_keys = {str(field.get("key") or "") for field in voice_pe_fields}
        self.assertEqual(sat1["audio_output_mode"]["value"], "auto")
        self.assertEqual(
            [option["value"] for option in sat1["audio_output_mode"]["options"]],
            ["auto", "internal", "aux", "both"],
        )
        self.assertNotIn("audio_output_mode", voice_pe_keys)
        self.assertNotIn("audio_output_section", voice_pe_keys)

    def test_invalid_saved_route_falls_back_to_automatic(self) -> None:
        with mock.patch.object(
            native_live_settings,
            "redis_client",
            self._redis({"audio_output_mode": "not-a-real-output"}),
        ):
            settings = native_live_settings.settings_snapshot(
                "native:office-sat1",
                board="sat1",
            )

        self.assertEqual(settings["audio_output_mode"], "auto")

    def test_firmware_payload_sends_route_only_to_sat1(self) -> None:
        fake = self._redis({"audio_output_mode": "aux"})
        with mock.patch.object(native_live_settings, "redis_client", fake):
            sat1_payload = native_live_settings.firmware_settings_snapshot(
                "native:office-sat1",
                board="sat1",
            )
            voice_pe_payload = native_live_settings.firmware_settings_snapshot(
                "native:office-voice-pe",
                board="voice-pe",
            )

        self.assertEqual(sat1_payload["audio_output_mode"], "aux")
        self.assertNotIn("audio_output_mode", voice_pe_payload)

    def test_selected_route_is_saved_as_a_device_setting(self) -> None:
        fake = self._redis()
        with mock.patch.object(native_live_settings, "redis_client", fake):
            result = native_live_settings.save_settings(
                {"audio_output_mode": "both"},
                selector="native:office-sat1",
                board="sat1",
            )

        self.assertEqual(result["settings"]["audio_output_mode"], "both")
        mappings = [call.kwargs.get("mapping", {}) for call in fake.hset.call_args_list]
        self.assertTrue(any(mapping.get("audio_output_mode") == "both" for mapping in mappings))


if __name__ == "__main__":
    unittest.main()
