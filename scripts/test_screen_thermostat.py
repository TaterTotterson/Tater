from __future__ import annotations

import unittest
import asyncio
from unittest import mock

from tater_voice import display_feed, native_satellite, screen_thermostat


def thermostat(**changes):
    row = {
        "id": "ecobee:1:2", "name": "Family Room", "temperature_unit": "F",
        "current_temperature_f": 68, "target_temperature_f": 72,
        "target_hvac_mode": "cool", "target_mode_writable": True,
        "target_temperature_writable": True,
    }
    row.update(changes)
    return row


class FakeHomeKit:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def list_homekit_thermostats(self):
        return self.rows

    def set_homekit_thermostat_temperature(self, *args, **kwargs):
        self.calls.append(("temperature", args, kwargs))
        return {"target_hvac_mode": kwargs.get("mode") or "cool"}

    def set_homekit_thermostat_mode(self, *args, **kwargs):
        self.calls.append(("mode", args, kwargs))
        return {"target_hvac_mode": args[0]}


class ScreenThermostatTests(unittest.TestCase):
    def test_missing_core_and_thermostat_are_explicit(self):
        with mock.patch.object(screen_thermostat, "_homekit") as loader:
            self.assertIn("Environment Core", screen_thermostat.summary(environment_installed=False)["message"])
            loader.assert_not_called()
        with mock.patch.object(screen_thermostat, "_homekit", return_value=FakeHomeKit([])), mock.patch.object(screen_thermostat, "_ha_rows", return_value=[]):
            self.assertIn("No thermostat", screen_thermostat.summary(environment_installed=True)["message"])

    def test_single_homekit_thermostat_and_validated_write(self):
        homekit = FakeHomeKit([thermostat()])
        with mock.patch.object(screen_thermostat, "_homekit", return_value=homekit):
            summary = screen_thermostat.summary(environment_installed=True)
            self.assertEqual("homekit:ecobee:1:2", summary["id"])
            self.assertEqual(68, summary["current"])
            self.assertEqual(72, summary["target"])
            self.assertTrue(summary["writable"])
            result = screen_thermostat.set_target(
                {"thermostat_id": summary["id"], "target": 70, "unit": "F"},
                environment_installed=True,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(1, len(homekit.calls))
            self.assertEqual("temperature", homekit.calls[0][0])
            self.assertEqual("ecobee:1:2", homekit.calls[0][2]["thermostat_id"])

    def test_multiple_thermostats_do_not_guess(self):
        homekit = FakeHomeKit([thermostat(), thermostat(id="other", name="Bedroom")])
        with mock.patch.object(screen_thermostat, "_homekit", return_value=homekit):
            self.assertFalse(screen_thermostat.summary(environment_installed=True)["available"])
            self.assertEqual("Family Room", screen_thermostat.summary(environment_installed=True, room="Family Room")["name"])
            with self.assertRaisesRegex(ValueError, "unambiguous"):
                screen_thermostat.set_target(
                    {"thermostat_id": "homekit:ecobee:1:2", "target": 70, "unit": "F"},
                    environment_installed=True,
                )
            self.assertFalse(homekit.calls)


class ScreenThermostatProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_clients = native_satellite._clients
        self.original_lock = native_satellite._clients_lock
        native_satellite._clients = {}
        native_satellite._clients_lock = asyncio.Lock()

    async def asyncTearDown(self):
        native_satellite._clients = self.original_clients
        native_satellite._clients_lock = self.original_lock

    async def _send(self, authorized: bool):
        selector = "checkers-test"
        queue = asyncio.Queue()
        native_satellite._clients[selector] = {
            "connected": True,
            "hello": {"payload": {"room": "Family Room", "capabilities": {"screen_thermostat": authorized}}},
            "queue": queue,
        }
        message = {
            "id": "touch-1", "type": "thermostat.set",
            "payload": {"thermostat_id": "homekit:ecobee:1:2", "target": 70, "unit": "F"},
        }
        with mock.patch.object(display_feed, "_environment_core_installed", return_value=True), \
             mock.patch.object(screen_thermostat, "set_target", return_value={"ok": True}) as setter, \
             mock.patch.object(native_satellite, "_screen_weather_payload", return_value={"available": True, "thermostat": {"available": True}}):
            ack = await native_satellite._handle_text_message(selector, message)
        return ack, setter, queue

    async def test_authorized_screen_write_refreshes_weather(self):
        ack, setter, queue = await self._send(True)
        self.assertEqual("thermostat.set.ack", ack["type"])
        self.assertTrue(ack["payload"]["ok"])
        setter.assert_called_once()
        self.assertEqual("display.weather", queue.get_nowait()["type"])

    async def test_satellite_without_capability_cannot_write(self):
        ack, setter, queue = await self._send(False)
        self.assertFalse(ack["payload"]["ok"])
        setter.assert_not_called()
        self.assertEqual("display.weather", queue.get_nowait()["type"])

    def test_home_assistant_fallback_uses_climate_service(self):
        homekit = FakeHomeKit([])
        ha_row = thermostat(
            _source="ha", id="climate.family_room", name="Family Room",
            _hvac_modes=["off", "heat", "cool", "heat_cool"],
        )
        with mock.patch.object(screen_thermostat, "_homekit", return_value=homekit), \
             mock.patch.object(screen_thermostat, "_ha_rows", return_value=[ha_row]), \
             mock.patch.object(screen_thermostat, "_ha_request", return_value={}) as request:
            summary = screen_thermostat.summary(environment_installed=True)
            self.assertEqual("ha:climate.family_room", summary["id"])
            screen_thermostat.set_target(
                {"thermostat_id": summary["id"], "mode": "heat"},
                environment_installed=True,
            )
            request.assert_called_once_with(
                "POST", "/api/services/climate/set_hvac_mode",
                {"entity_id": "climate.family_room", "hvac_mode": "heat"},
            )

    def test_rejects_wrong_device_unit_range_and_read_only(self):
        homekit = FakeHomeKit([thermostat()])
        with mock.patch.object(screen_thermostat, "_homekit", return_value=homekit):
            for payload in (
                {"thermostat_id": "homekit:other", "target": 70, "unit": "F"},
                {"thermostat_id": "homekit:ecobee:1:2", "target": 70, "unit": "C"},
                {"thermostat_id": "homekit:ecobee:1:2", "target": 200, "unit": "F"},
                {"thermostat_id": "homekit:ecobee:1:2", "mode": "emergency_heat"},
            ):
                with self.subTest(payload=payload), self.assertRaises(ValueError):
                    screen_thermostat.set_target(payload, environment_installed=True)
            homekit.rows = [thermostat(target_temperature_writable=False)]
            with self.assertRaisesRegex(ValueError, "read-only"):
                screen_thermostat.set_target(
                    {"thermostat_id": "homekit:ecobee:1:2", "target": 70, "unit": "F"},
                    environment_installed=True,
                )
            self.assertFalse(homekit.calls)


if __name__ == "__main__":
    unittest.main()
