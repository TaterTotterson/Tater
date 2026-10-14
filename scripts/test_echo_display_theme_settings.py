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


class EchoDisplayThemeSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
        }
        fake.hget.return_value = "off"
        return fake

    def test_echo_screens_replace_led_controls_with_display_theme(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            for board in ("checkers", "crown", "rook"):
                fields = native_live_settings.settings_fields(f"native:{board}", board=board)
                by_key = {str(field.get("key") or ""): field for field in fields}

                self.assertIn("display_theme_section", by_key)
                self.assertIn("display_theme", by_key)
                self.assertIn("display_wallpaper_interval_minutes", by_key)
                self.assertIn("display_theme_preview", by_key)
                self.assertEqual(by_key["display_theme"]["value"], "tater")
                self.assertEqual(by_key["display_wallpaper_interval_minutes"]["value"], 1440)
                self.assertEqual(
                    {int(option["value"]) for option in by_key["display_wallpaper_interval_minutes"]["options"]},
                    {0, 60, 360, 720, 1440},
                )
                self.assertIs(by_key["display_edge_glow"]["value"], True)
                self.assertEqual(
                    {str(option["value"]) for option in by_key["display_theme"]["options"]},
                    {"tater", "ocean", "violet", "forest", "sunset"},
                )
                for led_key in native_live_settings._LED_FIELD_KEYS:
                    self.assertNotIn(led_key, by_key)

        self.assertTrue(native_live_settings._board_supports_display_photos("checkers"))
        self.assertTrue(native_live_settings._board_supports_display_photos("crown"))
        self.assertTrue(native_live_settings._board_supports_display_photos("rook"))
        self.assertTrue(native_live_settings._board_supports_display_photos("echo-spot"))

    def test_echo_ring_satellites_keep_led_controls_and_hide_theme(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            fields_by_board = {
                board: native_live_settings.settings_fields(f"native:{board}", board=board)
                for board in ("biscuit", "radar")
            }

        for fields in fields_by_board.values():
            keys = {str(field.get("key") or "") for field in fields}
            self.assertIn("led_section", keys)
            self.assertIn("led_replying_animation", keys)
            self.assertIn("led_music_animation", keys)
            self.assertNotIn("display_wallpaper_interval_minutes", keys)
            self.assertTrue(native_live_settings._DISPLAY_THEME_FIELD_KEYS.isdisjoint(keys))

    def test_music_animation_is_available_on_echo_and_esp32_rings(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            fields_by_board = {
                board: native_live_settings.settings_fields(f"native:{board}", board=board)
                for board in ("biscuit", "radar", "voice-pe", "satellite1", "respeaker-xvf3800")
            }
            unsupported_fields = native_live_settings.settings_fields("native:s420", board="thirdreality_s420")

        for board_fields in fields_by_board.values():
            fields = {str(field.get("key") or ""): field for field in board_fields}
            self.assertEqual(fields["led_music_animation"]["value"], "music_bars")
            self.assertEqual(
                {str(option["value"]) for option in fields["led_music_animation"]["options"]},
                {"off", "audio_glow", "music_pulse", "music_bars", "music_orbit", "music_wave"},
            )
        self.assertNotIn(
            "led_music_animation",
            {str(field.get("key") or "") for field in unsupported_fields},
        )

        fake = self._redis()
        with mock.patch.object(native_live_settings, "redis_client", fake):
            payloads = {
                board: native_live_settings.firmware_settings_snapshot(f"native:{board}", board=board)
                for board in ("biscuit", "radar", "voice-pe", "satellite1", "respeaker-xvf3800")
            }
            unsupported_payload = native_live_settings.firmware_settings_snapshot(
                "native:s420",
                board="thirdreality_s420",
            )
        for payload in payloads.values():
            self.assertEqual(payload["led_music_animation"], "music_bars")
        self.assertNotIn("led_music_animation", unsupported_payload)

    def test_voice_animations_offer_and_preserve_no_animation(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            biscuit_fields = native_live_settings.settings_fields("native:biscuit", board="biscuit")
            s420_fields = native_live_settings.settings_fields("native:s420", board="thirdreality_s420")

        biscuit = {str(field.get("key") or ""): field for field in biscuit_fields}
        for key in (
            "led_listening_animation",
            "led_thinking_animation",
            "led_tool_call_animation",
            "led_replying_animation",
        ):
            self.assertEqual(biscuit[key]["options"][0], {"value": "off", "label": "No Animation"})

        s420 = {str(field.get("key") or ""): field for field in s420_fields}
        self.assertNotIn("led_tool_call_animation", s420)
        for key in ("led_listening_animation", "led_thinking_animation", "led_replying_animation"):
            self.assertIn({"value": "off", "label": "No Animation"}, s420[key]["options"])

        normalized = native_live_settings.normalize_settings(
            {
                "led_listening_animation": "off",
                "led_thinking_animation": "off",
                "led_tool_call_animation": "off",
                "led_replying_animation": "off",
            }
        )
        for key in (
            "led_listening_animation",
            "led_thinking_animation",
            "led_tool_call_animation",
            "led_replying_animation",
        ):
            self.assertEqual(normalized[key], "off")
        self.assertEqual(native_live_settings._s420_led_animation("off", "led_listening_animation"), "off")

    def test_display_theme_is_normalized_and_sent_without_led_settings(self) -> None:
        fake = self._redis()
        fake.hgetall.side_effect = [
            {native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true"},
            {"display_theme": "ocean"},
        ]
        with mock.patch.object(native_live_settings, "redis_client", fake):
            payload = native_live_settings.firmware_settings_snapshot(
                "native:checkers",
                board="checkers",
            )

        self.assertEqual(payload["display_theme"], "ocean")
        self.assertEqual(payload["display_wallpaper_interval_minutes"], 1440)
        self.assertIs(payload["display_edge_glow"], True)
        for led_key in (
            "led_brightness",
            "led_color",
            "led_listening_animation",
            "led_thinking_animation",
            "led_tool_call_animation",
            "led_replying_animation",
            "led_music_animation",
        ):
            self.assertNotIn(led_key, payload)
        self.assertEqual(
            native_live_settings.normalize_settings({"display_theme": "unknown"})["display_theme"],
            "tater",
        )
        self.assertIs(
            native_live_settings.normalize_settings({"display_edge_glow": False})["display_edge_glow"],
            False,
        )
        self.assertEqual(
            native_live_settings.normalize_settings({"display_wallpaper_interval_minutes": 0})[
                "display_wallpaper_interval_minutes"
            ],
            0,
        )
        self.assertEqual(
            native_live_settings.normalize_settings({"display_wallpaper_interval_minutes": 17})[
                "display_wallpaper_interval_minutes"
            ],
            1440,
        )

    def test_theme_preview_is_rendered_by_the_vue_settings_form(self) -> None:
        source = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "ModelFields.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("typeOf(field) === 'display_theme_preview'", source)
        self.assertIn("selectedDisplayTheme", source)
        self.assertIn("tm-display-theme-stage", source)
        self.assertIn(".tm-display-theme-stage", styles)

    def test_checkers_crown_and_rook_photo_controls_are_in_satellite_settings(self) -> None:
        source = (
            REPO_ROOT
            / "frontend"
            / "src"
            / "settings"
            / "components"
            / "voice"
            / "VoiceSatellites.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("voice_native_display_photo_upload", source)
        self.assertIn("voice_native_display_photo_remove", source)
        self.assertIn("Seven Tater scenery photos are active", source)
        self.assertIn(".tvoice-display-photo-grid", styles)


if __name__ == "__main__":
    unittest.main()
