from __future__ import annotations

import json
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

helpers_stub = types.ModuleType("helpers")
helpers_stub.extract_json = lambda _text: ""
helpers_stub.redis_client = None
sys.modules.setdefault("helpers", helpers_stub)

requests_stub = types.ModuleType("requests")
requests_stub.get = lambda *_args, **_kwargs: None
sys.modules.setdefault("requests", requests_stub)

import tateros

integration_store_stub = types.ModuleType("tateros.integration_store")
integration_store_stub.get_enabled_integration_ids = lambda: []
sys.modules.setdefault("tateros.integration_store", integration_store_stub)
setattr(tateros, "integration_store", integration_store_stub)

import integration_registry
from category_device_control import CategoryDeviceControlBase
from verba_supersession import replacement_for_verba


class UnifiedDeviceControl(CategoryDeviceControlBase):
    inventory_scope = "all"
    allowed_actions = {
        "list",
        "status",
        "turn_on",
        "turn_off",
        "toggle",
        "set_brightness",
        "set_color",
        "set_percentage",
        "set_position",
        "open",
        "close",
        "stop",
        "lock",
        "unlock",
        "set_temperature",
        "set_hvac_mode",
        "camera_snapshot",
        "send_command",
        "activate",
        "run",
        "playpause",
        "play",
        "pause",
        "next",
        "previous",
        "mute",
        "unmute",
        "set_volume",
        "volume_up",
        "volume_down",
        "announce",
        "play_media",
    }
    control_actions = allowed_actions - {"list", "status"}
    ignored_target_words = {
        "device",
        "devices",
        "light",
        "lights",
        "lamp",
        "lamps",
        "switch",
        "switches",
        "plug",
        "plugs",
        "outlet",
        "outlets",
        "fan",
        "fans",
        "the",
        "my",
        "all",
    }


class DeviceRegistryAliasTests(unittest.TestCase):
    def test_integration_aliases_are_preserved(self) -> None:
        row = integration_registry._coerce_device_row(
            "shelly",
            {
                "id": "relay.tree",
                "name": "Christmas Tree Plug",
                "type": "outlet",
                "aliases": ["Christmas tree lights", "Tree lights"],
                "actions": ["turn_on", "turn_off"],
            },
        )

        self.assertEqual(
            row["aliases"],
            ["Christmas tree lights", "Tree lights"],
        )
        self.assertIn("plug", row["category_ids"])
        self.assertIn("switch", row["category_ids"])

    def test_removed_control_verbas_do_not_fallback_to_device_control(self) -> None:
        for verba_id in (
            "camera_control",
            "climate_control",
            "cover_control",
            "fan_control",
            "garage_door_control",
            "light_control",
            "lock_control",
            "media_player_control",
            "plug_control",
            "remote_control",
            "scene_control",
            "script_control",
            "switch_control",
        ):
            self.assertEqual(replacement_for_verba(verba_id), "")

class UnifiedDeviceSelectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.plugin = UnifiedDeviceControl()
        self.tree_plug = {
            "integration_id": "shelly",
            "id": "relay.tree",
            "name": "Christmas Tree Plug",
            "aliases": ["Christmas tree lights", "Tree lights"],
            "room": "Living Room",
            "capabilities": ["plug", "switch"],
            "category_ids": ["plug", "switch"],
            "actions": ["turn_on", "turn_off", "toggle"],
        }
        self.floor_light = {
            "integration_id": "hue",
            "id": "light.floor",
            "name": "Floor Lamp",
            "room": "Living Room",
            "capabilities": ["light", "dimmable"],
            "category_ids": ["light"],
            "actions": ["turn_on", "turn_off", "set_brightness"],
        }
        self.ceiling_fan = {
            "integration_id": "homeassistant",
            "id": "fan.ceiling",
            "name": "Ceiling Fan",
            "room": "Living Room",
            "capabilities": ["fan"],
            "category_ids": ["fan"],
            "actions": ["turn_on", "turn_off", "set_percentage"],
        }

    async def _resolve(self, query: str, llm_client=None):
        intent = await self.plugin._interpret_query({}, query, None)
        selected, needs = await self.plugin._select_devices(
            devices=[self.tree_plug, self.floor_light, self.ceiling_fan],
            payload={},
            query=query,
            intent=intent,
            llm_client=llm_client,
        )
        return intent, selected, needs

    async def test_light_word_can_resolve_to_plug_alias(self) -> None:
        intent, selected, needs = await self._resolve("Turn on the Christmas tree lights")

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual([row["id"] for row in selected], ["relay.tree"])
        self.assertEqual(needs, [])

    async def test_room_light_group_excludes_non_lighting_fan(self) -> None:
        intent, selected, needs = await self._resolve("Turn off the living room lights")

        self.assertEqual(intent["action"], "turn_off")
        self.assertEqual(
            {row["id"] for row in selected},
            {"relay.tree", "light.floor"},
        )
        self.assertEqual(needs, [])

    async def test_brightness_filters_to_dimmable_device(self) -> None:
        intent, selected, needs = await self._resolve("Set the living room lights to 30 percent")

        self.assertEqual(intent["action"], "set_brightness")
        self.assertEqual(intent["brightness_pct"], 30)
        self.assertEqual([row["id"] for row in selected], ["light.floor"])
        self.assertEqual(needs, [])

    async def test_fuzzy_score_cannot_select_without_ai(self) -> None:
        intent, selected, needs = await self._resolve("Turn on Christmas")

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual(selected, [])
        self.assertTrue(any("could not confidently match" in item.lower() for item in needs))

    async def test_ai_selects_every_non_exact_device_match(self) -> None:
        class PickingLlm:
            def __init__(self) -> None:
                self.calls = 0

            async def chat(self, **_kwargs):
                self.calls += 1
                return {
                    "message": {
                        "content": json.dumps({"device_id": "relay.tree"}),
                    }
                }

        llm_client = PickingLlm()
        intent, selected, needs = await self._resolve(
            "Turn on Christmas",
            llm_client=llm_client,
        )

        self.assertEqual(intent["action"], "turn_on")
        self.assertEqual(llm_client.calls, 1)
        self.assertEqual([row["id"] for row in selected], ["relay.tree"])
        self.assertEqual(needs, [])

    def test_overlapping_action_words_use_device_semantics(self) -> None:
        cases = {
            "set the office fan to 40 percent": "set_percentage",
            "close the bedroom blinds": "close",
            "set the thermostat to 70 degrees": "set_temperature",
            "open lock on the front door": "unlock",
            "mute the living room TV": "mute",
            "press mute on the den remote": "send_command",
            "activate the movie scene": "activate",
            "turn on the cleanup script": "run",
        }

        for query, expected in cases.items():
            with self.subTest(query=query):
                self.assertEqual(
                    self.plugin._normalize_action("", query),
                    expected,
                )


if __name__ == "__main__":  # pragma: no cover - direct script entry point
    unittest.main()
