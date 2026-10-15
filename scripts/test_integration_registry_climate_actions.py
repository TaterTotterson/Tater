#!/usr/bin/env python3
from __future__ import annotations

import importlib
import sys
import unittest
from types import SimpleNamespace
from unittest import mock


if not hasattr(sys.modules.get("integration_registry"), "_update_cached_device_after_action"):
    sys.modules.pop("integration_registry", None)
integration_registry = importlib.import_module("integration_registry")


class IntegrationRegistryClimateActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.thermostat = {
            "integration_id": "ecobee_homekit",
            "id": "ecobee:1:16",
            "ref": "thermostat:ecobee:1:16",
            "state": "71 F",
            "details": {
                "current_temperature_f": 71.0,
                "target_temperature_f": 71.0,
                "target_hvac_mode": "auto",
            },
        }
        self.registry = {"devices": [self.thermostat]}

    def _apply(self, action: str, payload: dict) -> None:
        with (
            mock.patch.object(integration_registry, "_cache_client", return_value=object()),
            mock.patch.object(integration_registry, "get_cached_integration_device_registry", return_value=self.registry),
            mock.patch.object(integration_registry, "save_integration_device_registry_cache") as save,
        ):
            integration_registry._update_cached_device_after_action(
                "ecobee_homekit",
                action,
                "ecobee:1:16",
                {"ok": True},
                payload=payload,
            )
        save.assert_called_once()

    def test_temperature_action_updates_cached_setpoint(self) -> None:
        self._apply(
            "set_temperature",
            {"temperature": 72.0, "target_temperature": 72.0, "temperature_unit": "F"},
        )

        self.assertEqual(self.thermostat["target_temperature"], 72.0)
        self.assertEqual(self.thermostat["details"]["target_temperature_f"], 72.0)

    def test_hvac_action_updates_cached_mode(self) -> None:
        self._apply("set_hvac_mode", {"mode": "cool", "hvac_mode": "cool"})

        self.assertEqual(self.thermostat["hvac_mode"], "cool")
        self.assertEqual(self.thermostat["details"]["target_hvac_mode"], "cool")

    def test_ecobee_homekit_action_uses_homekit_module(self) -> None:
        payload = {
            "temperature": 72.0,
            "target_temperature": 72.0,
            "temperature_unit": "F",
        }
        runner = mock.Mock(return_value={"ok": True})
        module = SimpleNamespace(
            INTEGRATION={"id": "ecobee_homekit"},
            run_integration_device_action=runner,
        )

        with (
            mock.patch.object(
                integration_registry.integration_store_module,
                "integration_module",
                return_value=module,
            ) as load_module,
            mock.patch.object(integration_registry, "_update_cached_device_after_action") as update_cache,
        ):
            result = integration_registry.run_integration_device_action(
                "ecobee_homekit",
                "set_temperature",
                "ecobee:1:16",
                payload,
            )

        self.assertEqual(result, {"ok": True})
        load_module.assert_called_once_with(
            "homekit",
            auto_restore=False,
            require_enabled=True,
        )
        runner.assert_called_once_with("set_temperature", "ecobee:1:16", payload)
        update_cache.assert_called_once_with(
            "ecobee_homekit",
            "set_temperature",
            "ecobee:1:16",
            {"ok": True},
            payload=payload,
        )


if __name__ == "__main__":
    unittest.main()
