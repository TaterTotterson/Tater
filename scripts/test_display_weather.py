from __future__ import annotations

import json
import time
import unittest
from unittest import mock

from notify import core as notify_core
from tater_voice import display_feed, firmware, native_satellite


class FakeRedis:
    def __init__(self, values=None, settings=None, hashes=None):
        self.values = values or {}
        self.settings = settings or {}
        self.hashes = hashes or {}

    def get(self, key):
        value = self.values.get(key)
        return json.dumps(value) if isinstance(value, (dict, list)) else value

    def hget(self, key, field):
        if key != "environment_core_settings":
            return self.hashes.get(key, {}).get(field)
        return self.settings.get(field)

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    def hdel(self, key, field):
        self.hashes.setdefault(key, {}).pop(field, None)


def weather_snapshot(**overrides):
    snapshot = {
        "provider": "weather_api",
        "model": "WeatherAPI.com",
        "received_at": time.time(),
        "readings": [
            {"key": "weather_api_temperature", "category": "temperature", "value": 74.4, "unit": "F"},
            {"key": "weather_api_condition", "category": "condition", "value": "Partly cloudy"},
            {"key": "weather_api_feelslike", "category": "temperature", "value": 72.3, "unit": "F"},
            {"key": "weather_api_humidity", "category": "humidity", "value": 43, "unit": "%"},
            {"key": "weather_api_wind_speed", "category": "wind_speed", "value": 8, "unit": "mph"},
            {"key": "weather_api_wind_dir", "category": "wind_direction", "value": "SE"},
        ],
    }
    snapshot.update(overrides)
    return snapshot


class DisplayWeatherTests(unittest.TestCase):
    def test_environment_weather_is_compact_and_display_ready(self) -> None:
        client = FakeRedis(
            {"environment:latest:weather_api": weather_snapshot()},
            {"ENVIRONMENT_TEMPERATURE_UNIT": "F"},
        )
        result = display_feed.build_weather_summary(client=client, core_installed=True)

        self.assertTrue(result["available"])
        self.assertEqual("74°", result["temperature_text"])
        self.assertEqual("Partly cloudy", result["condition"])
        self.assertEqual("partly", result["condition_kind"])
        self.assertEqual("Feels like 72°", result["feels_like_text"])
        self.assertEqual("cooler", result["feels_like_relation"])
        self.assertEqual("43%", result["humidity_text"])
        self.assertEqual("SE 8 mph", result["wind_text"])
        self.assertNotIn("readings", result)
        self.assertGreater(result["clock_unix_ms"], 0)
        self.assertIsInstance(result["utc_offset_seconds"], int)
        self.assertTrue(result["timezone"])

    def test_preferred_temperature_unit_is_applied(self) -> None:
        client = FakeRedis(
            {"environment:latest:weather_api": weather_snapshot()},
            {"ENVIRONMENT_TEMPERATURE_UNIT": "C"},
        )
        result = display_feed.build_weather_summary(client=client, core_installed=True)

        self.assertEqual("24°", result["temperature_text"])
        self.assertEqual("C", result["temperature_unit"])
        self.assertEqual("Feels like 22°", result["feels_like_text"])

    def test_feels_like_relation_tracks_displayed_temperature(self) -> None:
        cases = ((68, "cooler"), (74, "same"), (81, "warmer"))
        for feels_like, expected in cases:
            with self.subTest(feels_like=feels_like):
                snapshot = weather_snapshot()
                snapshot["readings"][2]["value"] = feels_like
                client = FakeRedis({"environment:latest:weather_api": snapshot})
                result = display_feed.build_weather_summary(
                    client=client,
                    core_installed=True,
                )
                self.assertEqual(expected, result["feels_like_relation"])

    def test_numeric_wind_bearing_is_presented_as_compass_direction(self) -> None:
        snapshot = weather_snapshot()
        snapshot["readings"][-1] = {
            "key": "weather_api_wind_dir",
            "category": "wind_direction",
            "value": 22,
            "display": "22 deg",
            "unit": "deg",
        }
        client = FakeRedis({"environment:latest:weather_api": snapshot})
        result = display_feed.build_weather_summary(client=client, core_installed=True)
        self.assertEqual("NNE 8 mph", result["wind_text"])

    def test_saved_echo_display_profile_selects_indoor_and_outdoor_sensors(self) -> None:
        snapshot = weather_snapshot(
            provider="ecowitt",
            source_id="station",
            readings=[
                {"key": "patio", "category": "temperature", "area": "Outside", "value": 81, "unit": "F"},
                {"key": "hallway", "category": "temperature", "area": "Hallway", "value": 70, "unit": "F"},
                {"key": "outside_humidity", "category": "humidity", "area": "Outside", "value": 51, "unit": "%"},
                {"key": "inside_humidity", "category": "humidity", "area": "Hallway", "value": 39, "unit": "%"},
                {"key": "wind", "category": "wind_speed", "value": 12, "unit": "mph"},
                {"key": "rain", "category": "rain", "value": 0.2, "unit": "in/hr"},
                {"key": "lightning_num", "category": "lightning", "value": 3, "unit": ""},
                {"key": "condition", "category": "condition", "value": "Sunny"},
            ],
        )
        profile = {
            "target": "native_echo_show",
            "template": "native_screen",
            "selector": "native:echo-show",
            "slots": {
                "temp_out": "environment:ecowitt:station:patio",
                "temp_in": "environment:ecowitt:station:hallway",
                "humidity_out": "environment:ecowitt:station:outside_humidity",
                "humidity_in": "environment:ecowitt:station:inside_humidity",
                "wind_speed": "environment:ecowitt:station:wind",
                "rain_rate": "environment:ecowitt:station:rain",
                "lightning_strikes": "environment:ecowitt:station:lightning_num",
            },
        }
        client = FakeRedis(
            {"environment:latest:ecowitt": snapshot},
            {"ENVIRONMENT_TEMPERATURE_UNIT": "F"},
            {"tater:display:profiles:v1": {"native_echo_show": json.dumps(profile)}},
        )
        result = display_feed.build_weather_summary(
            client=client,
            core_installed=True,
            selector="native:echo-show",
        )

        self.assertEqual("81°", result["temperature_text"])
        self.assertEqual("70°", result["indoor_temperature_text"])
        self.assertEqual("51%", result["humidity_text"])
        self.assertEqual("39%", result["indoor_humidity_text"])
        self.assertEqual("12 mph", result["wind_text"])
        self.assertEqual("0.2 in/hr", result["rain_text"])
        self.assertEqual("3", result["lightning_text"])

    def test_cleared_profile_slots_do_not_fall_back_to_other_sensors(self) -> None:
        profile = {
            "target": "native_echo_show",
            "template": "native_screen",
            "selector": "native:echo-show",
            "slots": {
                "temp_out": "environment:weather_api:weather_api:weather_api_temperature",
                "temp_in": "",
                "humidity_out": "",
                "humidity_in": "",
                "wind_speed": "",
                "rain_rate": "",
                "lightning_strikes": "",
            },
        }
        client = FakeRedis(
            {"environment:latest:weather_api": weather_snapshot()},
            {"ENVIRONMENT_TEMPERATURE_UNIT": "F"},
            {"tater:display:profiles:v1": {"native_echo_show": json.dumps(profile)}},
        )

        result = display_feed.build_weather_summary(
            client=client,
            core_installed=True,
            selector="native:echo-show",
        )

        self.assertEqual("74°", result["temperature_text"])
        self.assertEqual("Feels like 72°", result["feels_like_text"])
        self.assertEqual("", result["indoor_temperature_text"])
        self.assertEqual("", result["indoor_humidity_text"])
        self.assertEqual("", result["humidity_text"])
        self.assertEqual("", result["wind_text"])
        self.assertEqual("", result["rain_text"])
        self.assertEqual("", result["lightning_text"])

    def test_uninstalled_core_uses_device_orb_even_with_cached_data(self) -> None:
        client = FakeRedis({"environment:latest:weather_api": weather_snapshot()})
        result = display_feed.build_weather_summary(client=client, core_installed=False)
        self.assertFalse(result["available"])
        self.assertGreater(result["clock_unix_ms"], 0)

    def test_only_capable_screen_satellites_receive_weather(self) -> None:
        self.assertTrue(native_satellite._screen_weather_supported({
            "capabilities": {"screen_weather": True},
        }))
        self.assertFalse(native_satellite._screen_weather_supported({
            "capabilities": {"screen": True},
        }))
        self.assertTrue(native_satellite._screen_notifications_supported({
            "capabilities": {"screen_notifications": True},
        }))
        self.assertFalse(native_satellite._screen_notifications_supported({
            "capabilities": {"screen_weather": True},
        }))

    def test_native_display_target_matching_supports_selector_name_and_all(self) -> None:
        targets = native_satellite._native_display_targets(
            "native:echo-show",
            {"device_name": "Family Room Show", "room": "Family Room"},
        )
        self.assertTrue(native_satellite._native_display_event_matches({"target": "all"}, targets))
        self.assertTrue(native_satellite._native_display_event_matches({"target": "family_room_show"}, targets))
        self.assertFalse(native_satellite._native_display_event_matches({"target": "garage"}, targets))

    def test_awareness_snapshot_is_embedded_for_native_screen_delivery(self) -> None:
        snapshot_id = "a" * 32
        client = FakeRedis({
            f"awareness:event_snapshot:{snapshot_id}": {
                "content_type": "image/jpeg",
                "data_b64": "AQIDBA==",
            }
        })
        event = {
            "id": "event-1",
            "seq": 7,
            "kind": "camera",
            "title": "Front Door Camera",
            "message": "A package was delivered.",
            "expires_at": time.time() + 90,
            "image_url": f"/tater-ha/v1/display/snapshots/{snapshot_id}",
        }
        with mock.patch.object(native_satellite._vp(), "redis_client", client):
            payload = native_satellite._native_display_notification_payload(event)

        self.assertEqual("Front Door Camera", payload["camera_name"])
        self.assertEqual("A package was delivered.", payload["description"])
        self.assertEqual("AQIDBA==", payload["image_data_b64"])
        self.assertEqual("image/jpeg", payload["image_content_type"])

    def test_display_notifier_maps_awareness_snapshot_id_to_display_image(self) -> None:
        captured = {}
        with mock.patch("tater_voice.display_bus.publish_display_event", side_effect=lambda payload: captured.update(payload)):
            result = notify_core._dispatch_display(
                "Front Door Camera",
                "A package was delivered.",
                {"target": "all"},
                {"platform": "awareness_core"},
                {"snapshot_id": "b" * 32},
                None,
            )

        self.assertEqual("Queued notification for display", result)
        self.assertEqual(
            "/tater-ha/v1/display/snapshots/" + "b" * 32,
            captured["image_url"],
        )

    def test_echo_show_receives_the_existing_display_sensor_profile_ui(self) -> None:
        status = {
            "clients": {
                "native:echo-show": {
                    "connected": True,
                    "name": "Family Room Show",
                    "capabilities": {"screen_weather": True},
                    "metadata": {"board": "checkers"},
                    "device_info": {
                        "friendly_name": "Family Room Show",
                        "model": "checkers",
                    },
                }
            }
        }
        with mock.patch.object(
            firmware,
            "_tater_sensor_select_state",
            return_value={"ready": True, "options": [{"value": "", "label": "None"}], "message": ""},
        ), mock.patch.object(firmware, "_display_profile_rows_from_store", return_value={}):
            payload = firmware.display_sensor_profiles_payload(status)

        self.assertEqual(1, len(payload["profiles"]))
        profile = payload["profiles"][0]
        self.assertEqual("native_screen", profile["profile_kind"])
        self.assertEqual("native:echo-show", profile["selector"])
        self.assertEqual("Family Room Show", profile["title"])
        self.assertEqual("Indoor Temperature", next(
            field["label"] for field in profile["fields"] if field["key"] == "temp_in"
        ))
        for field in profile["fields"]:
            self.assertEqual("", field["options"][0]["value"])
            self.assertEqual("Do not show", field["options"][0]["label"])

    def test_echo_profile_save_skips_s3_entities_and_refreshes_the_native_screen(self) -> None:
        client = FakeRedis()
        send_command = mock.Mock(return_value="queued")
        with mock.patch.object(firmware, "redis_client", client), mock.patch.object(
            firmware, "_apply_s3box_display_url"
        ) as apply_url, mock.patch.object(
            firmware, "_apply_s3box_display_target"
        ) as apply_target, mock.patch.object(
            display_feed, "build_weather_summary", return_value={"available": True}
        ), mock.patch.object(
            firmware.display_bus, "request_display_refresh", return_value={"ok": True}
        ), mock.patch.object(
            native_satellite, "send_command", new=send_command
        ), mock.patch.object(
            native_satellite, "run_on_runtime_loop", return_value={"ok": True}
        ):
            result = firmware._save_display_sensor_profile({
                "target": "native_echo_show",
                "selector": "native:echo-show",
                "profile_kind": "native_screen",
                "slots": {"temp_in": "environment:ecowitt:station:hallway"},
            })

        self.assertTrue(result["ok"])
        apply_url.assert_not_called()
        apply_target.assert_not_called()
        send_command.assert_called_once_with(
            "native:echo-show", "display.weather", {"available": True}
        )
        stored = json.loads(client.hashes["tater:display:profiles:v1"]["native_echo_show"])
        self.assertEqual("native_screen", stored["template"])
        self.assertEqual(
            "environment:ecowitt:station:hallway",
            stored["slots"]["temp_in"],
        )


if __name__ == "__main__":
    unittest.main()
