#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from little_spud_home import (  # noqa: E402
    build_home_snapshot,
    home_action_payload,
    resolve_home_action_targets,
    resolve_home_camera_target,
)


def sample_registry():
    return {
        "category_definitions": [
            {"id": "light", "name": "Lights", "order": 10},
            {"id": "switch", "name": "Switches", "order": 20},
            {"id": "fan", "name": "Fans", "order": 35},
            {"id": "garage_door", "name": "Garage Doors", "order": 40},
            {"id": "climate", "name": "Climate", "order": 80},
            {"id": "temperature", "name": "Temperature", "order": 90},
            {"id": "humidity", "name": "Humidity", "order": 100},
            {"id": "sensor", "name": "Sensors", "order": 220},
        ],
        "devices": [
            {
                "integration_id": "hue",
                "id": "light:desk",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["light", "switch"],
                "actions": ["turn_on", "turn_off", "set_brightness"],
                "state": "on",
                "details": {"dimming": {"brightness": 64}},
            },
            {
                "integration_id": "shelly",
                "id": "fan:ceiling",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["fan"],
                "actions": ["turn_on", "turn_off"],
                "state": "off",
            },
            {
                "integration_id": "sensor_provider",
                "id": "sensor:climate",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["temperature", "humidity", "sensor"],
                "actions": [],
                "state": "72.5",
                "details": {"temperature": 72.5, "temperature_unit": "F", "humidity": 43},
            },
            {
                "integration_id": "aladdin",
                "id": "door:main",
                "room": "Garage",
                "room_id": "garage",
                "category_ids": ["garage_door"],
                "actions": ["open", "close"],
                "state": "closed",
            },
            {
                "integration_id": "ecobee_homekit",
                "id": "thermostat:office",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["climate", "temperature", "humidity"],
                "actions": ["set_temperature", "set_hvac_mode"],
                "state": "71 F",
                "details": {
                    "current_temperature_f": 71,
                    "target_temperature_f": 69,
                    "current_humidity": 41,
                    "target_hvac_mode": "heat",
                },
            },
        ],
        "cache": {"cached": True, "age_seconds": 3},
    }


class LittleSpudHomeTests(unittest.TestCase):
    def test_snapshot_groups_rooms_without_exposing_devices(self):
        snapshot = build_home_snapshot(sample_registry())
        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["room_count"], 2)
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        self.assertEqual(office["device_count"], 4)
        self.assertNotIn("devices", office)
        self.assertNotIn("devices", office["categories"][0])
        light = next(category for category in office["categories"] if category["id"] == "light")
        self.assertEqual(light["brightness"], 64)

    def test_specific_control_category_is_not_duplicated_as_switch(self):
        snapshot = build_home_snapshot(sample_registry())
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        category_ids = [category["id"] for category in office["categories"]]
        self.assertIn("light", category_ids)
        self.assertNotIn("switch", category_ids)

    def test_multi_sensor_reports_each_specific_reading(self):
        snapshot = build_home_snapshot(sample_registry())
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        sensors = {category["id"]: category for category in office["sensors"]}
        self.assertEqual(sensors["temperature"]["summary"], "71.8°F")
        self.assertEqual(sensors["humidity"]["summary"], "42%")
        self.assertNotIn("sensor", sensors)

    def test_action_targets_are_limited_to_room_category_and_capability(self):
        targets = resolve_home_action_targets(
            sample_registry(),
            room_id="office",
            category_id="light",
            action="set_brightness",
        )
        self.assertEqual([target["id"] for target in targets], ["light:desk"])
        with self.assertRaises(ValueError):
            resolve_home_action_targets(
                sample_registry(),
                room_id="office",
                category_id="temperature",
                action="turn_on",
            )

    def test_brightness_payload_supports_provider_aliases_and_clamps(self):
        payload = home_action_payload("set_brightness", 140)
        self.assertEqual(payload["brightness"], 100)
        self.assertEqual(payload["brightness_pct"], 100)
        self.assertEqual(payload["level"], 100)
        self.assertEqual(payload["percent"], 100)

    def test_thermostat_payload_exposes_controls_and_setpoint(self):
        snapshot = build_home_snapshot(sample_registry())
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        climate = next(category for category in office["controls"] if category["id"] == "climate")
        self.assertEqual(climate["control_type"], "thermostat")
        self.assertEqual(climate["current_temperature"], 71)
        self.assertEqual(climate["target_temperature"], 69)
        self.assertEqual(climate["temperature_unit"], "F")
        self.assertEqual(climate["hvac_mode"], "heat")
        self.assertEqual(climate["available_hvac_modes"], ["off", "heat", "cool", "auto"])
        self.assertEqual(
            climate["available_actions"],
            ["set_temperature", "set_hvac_mode"],
        )

    def test_thermostat_actions_are_scoped_and_build_provider_payloads(self):
        targets = resolve_home_action_targets(
            sample_registry(),
            room_id="office",
            category_id="climate",
            action="set_temperature",
        )
        self.assertEqual([target["id"] for target in targets], ["thermostat:office"])
        self.assertEqual(
            home_action_payload("set_temperature", 72, temperature_unit="F"),
            {
                "temperature": 72.0,
                "target_temperature": 72.0,
                "temperature_unit": "F",
            },
        )
        self.assertEqual(
            home_action_payload("set_hvac_mode", mode="cool"),
            {"mode": "cool", "hvac_mode": "cool"},
        )

    def test_unknown_safety_sensor_is_not_reported_as_clear(self):
        registry = sample_registry()
        registry["category_definitions"].append(
            {"id": "leak", "name": "Leak Sensors", "order": 75}
        )
        registry["devices"].append(
            {
                "integration_id": "sensor_provider",
                "id": "sensor:leak",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["leak"],
                "actions": [],
                "state": "unavailable",
            }
        )
        snapshot = build_home_snapshot(registry)
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        leak = next(category for category in office["sensors"] if category["id"] == "leak")
        self.assertEqual(leak["summary"], "Status unavailable")

    def test_nested_hue_temperature_keeps_celsius_unit(self):
        registry = sample_registry()
        climate = next(
            device for device in registry["devices"] if device["id"] == "sensor:climate"
        )
        climate["state"] = "23 C"
        climate["details"] = {
            "temperature": {"temperature": 23},
            "humidity": {"humidity": 48},
        }
        snapshot = build_home_snapshot(registry)
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        sensors = {category["id"]: category for category in office["sensors"]}
        self.assertEqual(sensors["temperature"]["summary"], "22.3°C")
        self.assertEqual(sensors["temperature"]["current_temperature"], 22.3)
        self.assertEqual(sensors["temperature"]["temperature_unit"], "C")
        self.assertEqual(sensors["humidity"]["summary"], "44%")

    def test_non_temperature_state_is_not_mistaken_for_celsius(self):
        registry = sample_registry()
        registry["devices"].append(
            {
                "integration_id": "entry_provider",
                "id": "entry:back-door",
                "room": "Back Door",
                "room_id": "back_door",
                "category_ids": ["entry_sensor", "temperature"],
                "actions": [],
                "state": "Closed",
            }
        )
        snapshot = build_home_snapshot(registry)
        back_door = next(room for room in snapshot["rooms"] if room["id"] == "back_door")
        temperature = next(
            category
            for category in back_door["sensors"]
            if category["id"] == "temperature"
        )
        self.assertEqual(temperature["summary"], "Status unavailable")
        self.assertIsNone(temperature["current_temperature"])
        self.assertEqual(temperature["temperature_unit"], "F")

    def test_camera_previews_are_opaque_and_scoped_to_the_room_and_client(self):
        registry = sample_registry()
        registry["category_definitions"].append(
            {"id": "camera", "name": "Cameras", "order": 15}
        )
        registry["devices"].append(
            {
                "integration_id": "protect",
                "id": "camera:office-entry",
                "room": "Office",
                "room_id": "office",
                "category_ids": ["camera", "motion"],
                "actions": ["camera_snapshot"],
                "state": "connected",
            }
        )

        snapshot = build_home_snapshot(registry, camera_ref_secret="client-one-secret")
        office = next(room for room in snapshot["rooms"] if room["id"] == "office")
        camera = next(category for category in office["categories"] if category["id"] == "camera")
        preview = camera["camera_previews"][0]
        self.assertRegex(preview["id"], r"^[a-f0-9]{24}$")
        self.assertNotIn("camera:office-entry", str(snapshot))
        self.assertNotIn("protect", str(snapshot))

        target, action = resolve_home_camera_target(
            registry,
            room_id="office",
            camera_id=preview["id"],
            camera_ref_secret="client-one-secret",
        )
        self.assertEqual(target["id"], "camera:office-entry")
        self.assertEqual(action, "camera_snapshot")
        with self.assertRaises(LookupError):
            resolve_home_camera_target(
                registry,
                room_id="garage",
                camera_id=preview["id"],
                camera_ref_secret="client-one-secret",
            )
        with self.assertRaises(LookupError):
            resolve_home_camera_target(
                registry,
                room_id="office",
                camera_id=preview["id"],
                camera_ref_secret="another-client-secret",
            )


if __name__ == "__main__":
    unittest.main()
