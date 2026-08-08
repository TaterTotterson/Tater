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


class S420LedSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
        }
        fake.hget.return_value = "off"
        return fake

    def test_s420_shows_single_status_light_controls_without_ring_preview(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            fields = native_live_settings.settings_fields(
                "native:kitchen-s420",
                board="thirdreality-s420",
            )

        by_key = {str(field.get("key") or ""): field for field in fields}
        self.assertEqual(by_key["led_section"]["label"], "Tater S420 Status Light")
        self.assertEqual(by_key["led_color"]["label"], "Tater Status Color")
        self.assertNotIn("led_preview", by_key)
        self.assertNotIn("led_tool_call_animation", by_key)
        expected = {"pulse", "breathe", "heartbeat", "solid"}
        for key in (
            "led_listening_animation",
            "led_thinking_animation",
            "led_replying_animation",
        ):
            self.assertEqual(
                {str(option["value"]) for option in by_key[key]["options"]},
                expected,
            )

    def test_s420_converts_saved_ring_effects_to_hardware_safe_defaults(self) -> None:
        fake = self._redis()
        fake.hgetall.side_effect = [
            {
                native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                "led_listening_animation": "directional",
                "led_thinking_animation": "sparkle",
                "led_tool_call_animation": "ping_pong",
                "led_replying_animation": "voice_ring",
            },
            {},
        ]
        with mock.patch.object(native_live_settings, "redis_client", fake):
            settings = native_live_settings.settings_snapshot(
                "native:kitchen-s420",
                board="s420",
            )

        self.assertEqual(settings["led_listening_animation"], "pulse")
        self.assertEqual(settings["led_thinking_animation"], "breathe")
        self.assertEqual(settings["led_tool_call_animation"], "heartbeat")
        self.assertEqual(settings["led_replying_animation"], "pulse")

    def test_s420_firmware_payload_keeps_tater_status_light_values(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            payload = native_live_settings.firmware_settings_snapshot(
                "native:kitchen-s420",
                board="TATER-S420-001122334455",
            )

        self.assertEqual(payload["led_color"], "#ff5a1f")
        self.assertEqual(payload["led_listening_animation"], "pulse")
        self.assertEqual(payload["led_thinking_animation"], "breathe")
        self.assertEqual(payload["led_replying_animation"], "pulse")


if __name__ == "__main__":
    unittest.main()
