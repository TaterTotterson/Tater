#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import native_live_settings, native_satellite, ui_helpers  # noqa: E402


class OutputChainSettingsTests(unittest.TestCase):
    @staticmethod
    def _redis() -> mock.Mock:
        fake = mock.Mock()
        fake.hgetall.return_value = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
            native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
        }
        fake.hget.return_value = "off"
        return fake

    def test_equalizer_fields_are_capability_gated_per_satellite(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis()):
            supported = native_live_settings.settings_fields(
                "native:radar",
                board="radar",
                capabilities={"output_chain": True},
            )
            unsupported = native_live_settings.settings_fields(
                "native:legacy",
                board="voice-pe",
                capabilities={},
            )

        supported_by_key = {str(field.get("key") or ""): field for field in supported}
        unsupported_keys = {str(field.get("key") or "") for field in unsupported}
        self.assertEqual("equalizer", supported_by_key["eq_bands"]["type"])
        self.assertEqual(8, len(supported_by_key["eq_bands"]["bands"]))
        self.assertEqual([0.0] * 8, supported_by_key["eq_bands"]["value"])
        self.assertIn("bass_guard_enabled", supported_by_key)
        self.assertIn("limiter_enabled", supported_by_key)
        self.assertTrue(native_live_settings._OUTPUT_CHAIN_FIELD_KEYS.isdisjoint(unsupported_keys))

    def test_output_chain_values_are_bounded_and_padded(self) -> None:
        normalized = native_live_settings.normalize_settings(
            {
                "eq_bands": [-99, -11.7, 0.24, 1.26, 11.9, 99],
                "eq_loudness": "true",
                "bass_guard_enabled": "false",
                "bass_guard_db": -200,
                "limiter_enabled": "true",
                "limiter_threshold_db": 4,
                "limiter_release_ms": 5000,
            }
        )

        self.assertEqual([-12.0, -11.5, 0.0, 1.5, 12.0, 12.0, 0.0, 0.0], normalized["eq_bands"])
        self.assertTrue(normalized["eq_loudness"])
        self.assertFalse(normalized["bass_guard_enabled"])
        self.assertEqual(-60.0, normalized["bass_guard_db"])
        self.assertTrue(normalized["limiter_enabled"])
        self.assertEqual(0.0, normalized["limiter_threshold_db"])
        self.assertEqual(1000.0, normalized["limiter_release_ms"])

    def test_capable_firmware_receives_echo_wire_keys_only(self) -> None:
        fake = self._redis()
        with mock.patch.object(native_live_settings, "redis_client", fake):
            capable = native_live_settings.firmware_settings_snapshot(
                "native:radar",
                board="radar",
                capabilities={"output_chain": True},
            )
            legacy = native_live_settings.firmware_settings_snapshot(
                "native:legacy",
                board="voice-pe",
                capabilities={},
            )

        self.assertEqual([0.0] * 8, capable["eqBands"])
        self.assertFalse(capable["eqLoudness"])
        self.assertTrue(capable["bassGuardEnabled"])
        # Radar has a larger speaker, so its board default floor is -10 dB.
        self.assertEqual(-10.0, capable["bassGuardDb"])
        self.assertTrue(capable["limiterEnabled"])
        self.assertEqual(-1.0, capable["limiterThreshold"])
        self.assertEqual(150.0, capable["limiterRelease"])
        self.assertNotIn("eq_bands", capable)
        for wire_key in native_live_settings._OUTPUT_CHAIN_WIRE_KEYS.values():
            self.assertNotIn(wire_key, legacy)

    def test_list_style_echo_capabilities_are_understood(self) -> None:
        self.assertEqual(
            {"speaker": True, "output_chain": True},
            native_satellite._capabilities(
                {"capabilities": ["speaker", "output_chain"]}
            ),
        )

    def test_supported_echo_satellite_popups_contain_equalizer(self) -> None:
        clients = {}
        for board in ("biscuit", "radar", "checkers", "cronos", "crown", "rook"):
            selector = f"native:{board}-test"
            clients[selector] = {
                "selector": selector,
                "source": "tater_native",
                "name": f"Test {board.title()}",
                "connected": True,
                "capabilities": {"speaker": True, "output_chain": True},
                "metadata": {
                    "native_selected": True,
                    "board": board,
                },
                "device_info": {"model": board},
                "voice_api_audio_supported": True,
                "voice_speaker_supported": True,
            }
        status = {"clients": clients}

        with (
            mock.patch.object(native_live_settings, "redis_client", self._redis()),
            mock.patch.object(
                ui_helpers,
                "_as_float",
                side_effect=lambda value, default=0.0: float(
                    value if value not in (None, "") else default
                ),
            ),
            mock.patch.object(
                ui_helpers.esphome_runtime,
                "text",
                side_effect=lambda value: str(value or "").strip(),
            ),
            mock.patch.object(
                ui_helpers.esphome_runtime,
                "lower",
                side_effect=lambda value: str(value or "").strip().lower(),
            ),
            mock.patch.object(ui_helpers.esphome_runtime, "load_satellite_registry", return_value=[]),
            mock.patch.object(
                ui_helpers.esphome_runtime,
                "satellite_host_from_selector",
                side_effect=lambda value: str(value or "").removeprefix("host:"),
            ),
            mock.patch.object(
                ui_helpers.reply_playback,
                "resolve_reply_playback_target",
                return_value=ui_helpers.reply_playback.REPLY_PLAYBACK_DEVICE,
            ),
            mock.patch.object(
                ui_helpers.reply_playback,
                "build_reply_playback_options",
                return_value=[
                    {
                        "value": ui_helpers.reply_playback.REPLY_PLAYBACK_DEVICE,
                        "label": "This device speaker",
                    }
                ],
            ),
        ):
            items = ui_helpers.satellite_item_forms(status)

        self.assertEqual(6, len(items))
        for item in items:
            popup_keys = {str(field.get("key") or "") for field in item["popup_fields"]}
            self.assertIn("output_chain_section", popup_keys, item["board"])
            self.assertIn("eq_bands", popup_keys, item["board"])
            self.assertIn("limiter_enabled", popup_keys, item["board"])
            self.assertEqual(
                "voice_native_satellite_settings_save",
                item["settings_save_action"],
            )

    def test_vue_form_renders_a_real_equalizer_control(self) -> None:
        source = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "ModelFields.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("typeOf(field) === 'equalizer'", source)
        self.assertIn("setEqualizerBand", source)
        self.assertIn("Reset flat", source)
        self.assertIn(".tm-equalizer-bands", styles)

    @staticmethod
    def _redis_with_device(device_row: dict) -> mock.Mock:
        fake = OutputChainSettingsTests._redis()
        markers = dict(fake.hgetall.return_value)

        def hgetall(key: str) -> dict:
            if str(key).startswith(native_live_settings.DEVICE_SETTINGS_HASH_PREFIX):
                return dict(device_row)
            return dict(markers)

        fake.hgetall.side_effect = hgetall
        return fake

    def test_larger_echo_speakers_default_to_a_shallower_bass_floor(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis_with_device({})):
            for board in ("radar", "crown", "athena", "A8H3N2", "echo-show-8"):
                current = native_live_settings.settings_snapshot(f"native:{board}", board=board)
                self.assertEqual(-10.0, current["bass_guard_db"], board)
            for board in ("biscuit", "checkers", "cronos", "rook"):
                current = native_live_settings.settings_snapshot(f"native:{board}", board=board)
                self.assertEqual(-30.0, current["bass_guard_db"], board)

    def test_a_satellite_value_replaces_the_board_speaker_default(self) -> None:
        fake = self._redis_with_device({"bass_guard_db": "-25"})
        with mock.patch.object(native_live_settings, "redis_client", fake):
            current = native_live_settings.settings_snapshot("native:radar", board="radar")
        self.assertEqual(-25.0, current["bass_guard_db"])

    def test_bass_floor_field_shows_the_board_default(self) -> None:
        with mock.patch.object(native_live_settings, "redis_client", self._redis_with_device({})):
            radar = native_live_settings.settings_fields(
                "native:radar", board="radar", capabilities={"output_chain": True}
            )
            biscuit = native_live_settings.settings_fields(
                "native:biscuit", board="biscuit", capabilities={"output_chain": True}
            )
        radar_floor = next(field for field in radar if field.get("key") == "bass_guard_db")
        biscuit_floor = next(field for field in biscuit if field.get("key") == "bass_guard_db")
        self.assertEqual(-10.0, radar_floor["default"])
        self.assertEqual(-10.0, radar_floor["value"])
        self.assertEqual(-30.0, biscuit_floor["default"])

    def test_offline_partial_save_keeps_the_saved_board_speaker_default(self) -> None:
        fake = self._redis_with_device({})
        credentials = {
            "devices": {
                "native:radar": {
                    "selector": "native:radar",
                    "board": "radar",
                }
            }
        }
        with (
            mock.patch.object(native_live_settings, "redis_client", fake),
            mock.patch.object(native_satellite, "_clients", {}),
            mock.patch.object(native_satellite, "_load_selector_aliases", return_value={}),
            mock.patch.object(native_satellite, "_load_credentials_unlocked", return_value=credentials),
        ):
            result = asyncio.run(
                native_satellite.save_live_settings(
                    {"volume_percent": 81},
                    selector="native:radar",
                    push=False,
                )
            )

        self.assertEqual(-10.0, result["settings"]["bass_guard_db"])
        device_write = next(
            call
            for call in fake.hset.call_args_list
            if str(call.args[0]).startswith(native_live_settings.DEVICE_SETTINGS_HASH_PREFIX)
        )
        self.assertEqual(-10.0, float(device_write.kwargs["mapping"]["bass_guard_db"]))


if __name__ == "__main__":
    unittest.main()
