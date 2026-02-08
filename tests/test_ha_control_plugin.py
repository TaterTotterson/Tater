import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from plugins.ha_control import HAControlPlugin


class _FakeLightClient:
    def __init__(self):
        self.calls = []

    def list_states(self):
        return []

    def call_service(self, domain, service, data):
        self.calls.append((domain, service, dict(data or {})))
        return {}

    def get_state(self, entity_id):
        return {
            "state": "off",
            "attributes": {"friendly_name": "Kitchen Light"},
        }


class _FakeRemoteClient:
    def __init__(self):
        self.command_payloads = []

    def list_states(self):
        return []

    def call_service(self, domain, service, data):
        if domain == "remote" and service == "send_command":
            command_value = (data or {}).get("command")
            self.command_payloads.append(command_value)
            if isinstance(command_value, list):
                raise RuntimeError("list command format not supported")
            return {}
        return {}

    def get_state(self, entity_id):
        return {
            "state": "on",
            "attributes": {"friendly_name": "Living Room Remote", "activity_list": ["Watch TV"]},
        }


class HAControlPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = HAControlPlugin()

    def test_is_light_color_command_detects_lights_at_start(self):
        self.assertTrue(self.plugin._is_light_color_command("lights to blue"))
        self.assertTrue(self.plugin._is_light_color_command("set kitchen lights color red"))
        self.assertFalse(self.plugin._is_light_color_command("set thermostat to 72"))

    def test_validated_entity_id_accepts_explicit_entity(self):
        self.assertEqual(self.plugin._validated_entity_id("Light.Kitchen", "turn it off"), "light.kitchen")
        self.assertEqual(self.plugin._validated_entity_id("light-kitchen", "turn it off"), "")

    def test_choose_entity_llm_handles_zero_chunk_settings(self):
        candidates = [
            {"entity_id": "light.first", "domain": "light", "name": "First", "device_class": None, "unit": None},
            {"entity_id": "light.second", "domain": "light", "name": "Second", "device_class": None, "unit": None},
        ]
        with patch.object(
            self.plugin,
            "_get_plugin_settings",
            return_value={"HA_MAX_CANDIDATES": "0", "HA_CHUNK_SIZE": "0"},
        ):
            chosen = asyncio.run(
                self.plugin._choose_entity_llm(
                    "turn off first light",
                    {"intent": "control", "action": "turn_off"},
                    candidates,
                    llm_client=None,
                )
            )
        self.assertEqual(chosen, "light.first")

    def test_handle_accepts_entity_id_without_forcing_action_arg(self):
        fake_client = _FakeLightClient()
        with patch.object(self.plugin, "_get_client", return_value=fake_client), patch.object(
            self.plugin, "_get_catalog_cached", return_value=[]
        ), patch("plugins.ha_control.asyncio.sleep", new=AsyncMock()):
            result = asyncio.run(
                self.plugin._handle(
                    {"query": "turn off kitchen light", "entity_id": "light.kitchen"},
                    llm_client=None,
                )
            )

        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(fake_client.calls[0][0], "light")
        self.assertEqual(fake_client.calls[0][1], "turn_off")
        self.assertEqual(fake_client.calls[0][2].get("entity_id"), "light.kitchen")

    def test_remote_send_command_tries_string_when_list_fails(self):
        fake_client = _FakeRemoteClient()
        with patch.object(self.plugin, "_get_client", return_value=fake_client), patch.object(
            self.plugin, "_get_catalog_cached", return_value=[]
        ):
            result = asyncio.run(
                self.plugin._handle(
                    {
                        "query": "mute living room tv",
                        "intent": "control",
                        "action": "send_command",
                        "entity_id": "remote.living_room",
                        "desired": {"command": "mute"},
                    },
                    llm_client=None,
                )
            )

        self.assertIsInstance(result, str)
        self.assertIn("sent", result.lower())
        self.assertTrue(any(isinstance(item, list) for item in fake_client.command_payloads))
        self.assertTrue(any(isinstance(item, str) for item in fake_client.command_payloads))


if __name__ == "__main__":
    unittest.main()
