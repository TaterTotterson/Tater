#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

helpers_stub = types.ModuleType("helpers")
helpers_stub.redis_client = mock.Mock()
sys.modules.setdefault("helpers", helpers_stub)
registry_stub = types.ModuleType("integration_registry")
registry_stub.get_integration_device_registry = mock.Mock()
registry_stub.run_integration_device_action = mock.Mock()
registry_stub.refresh_integration_device_registry_cache = mock.Mock()
sys.modules.setdefault("integration_registry", registry_stub)
runtime_stub = types.ModuleType("integration_runtime")
runtime_stub.integration_runtime_states = mock.Mock(return_value={"states": []})
sys.modules.setdefault("integration_runtime", runtime_stub)

from tater_voice import display_climate  # noqa: E402


class DisplayClimateTests(unittest.TestCase):
    def test_rook_uses_only_three_configured_room_temperature_slots(self) -> None:
        profile = {
            "screen_target": "rook",
            "slots": {
                "room_temp_1": "sensor_provider::sensor:kitchen",
                "room_temp_2": "sensor_provider::sensor:bedroom",
                "room_temp_3": "sensor_provider::sensor:office",
                "room_temp_4": "sensor_provider::sensor:garage",
            },
        }
        registry = {"devices": []}
        with (
            mock.patch.object(display_climate.display_feed, "_saved_display_profile_from_query", return_value=(profile, True)),
            mock.patch.object(display_climate, "get_integration_device_registry", return_value=registry),
            mock.patch.object(display_climate, "build_satellite_climate_snapshot", return_value={"available": False}) as build,
        ):
            result = display_climate.build_climate_summary(selector="native:rook")

        self.assertFalse(result["available"])
        self.assertEqual(
            build.call_args.kwargs["room_sensor_refs"],
            [
                "sensor_provider::sensor:kitchen",
                "sensor_provider::sensor:bedroom",
                "sensor_provider::sensor:office",
            ],
        )
        self.assertEqual(build.call_args.kwargs["maximum_rooms"], 3)

    def test_climate_action_reuses_normalized_integration_action(self) -> None:
        thermostat = {
            "integration_id": "ecobee_homekit",
            "id": "thermostat:living-room",
            "room": "Living Room",
            "room_id": "living_room",
            "category_ids": ["climate"],
            "actions": ["set_temperature", "set_hvac_mode"],
        }
        registry = {"devices": [thermostat], "category_definitions": [{"id": "climate", "order": 80}]}
        with (
            mock.patch.object(display_climate, "get_integration_device_registry", return_value=registry),
            mock.patch.object(display_climate, "run_integration_device_action", return_value={"ok": True}) as run,
        ):
            result = display_climate.run_climate_action(
                {"action": "set_temperature", "room_id": "living_room", "value": 72, "temperature_unit": "F"}
            )

        self.assertTrue(result["ok"])
        run.assert_called_once_with(
            "ecobee_homekit",
            "set_temperature",
            "thermostat:living-room",
            {"temperature": 72.0, "target_temperature": 72.0, "temperature_unit": "F"},
        )

    def test_temperature_action_stays_visible_while_provider_state_catches_up(self) -> None:
        summary = {"target_temperature": 71.0, "temperature_unit": "F", "hvac_mode": "auto"}

        updated = display_climate.apply_action_to_summary(
            summary,
            {"action": "set_temperature", "value": 72, "temperature_unit": "F"},
        )

        self.assertEqual(updated["target_temperature"], 72.0)
        self.assertEqual(summary["target_temperature"], 71.0)

    def test_mode_action_stays_visible_while_provider_state_catches_up(self) -> None:
        summary = {"target_temperature": 72.0, "temperature_unit": "F", "hvac_mode": "auto"}

        updated = display_climate.apply_action_to_summary(
            summary,
            {"action": "set_hvac_mode", "mode": "cool"},
        )

        self.assertEqual(updated["hvac_mode"], "cool")
        self.assertEqual(summary["hvac_mode"], "auto")


if __name__ == "__main__":
    unittest.main()
