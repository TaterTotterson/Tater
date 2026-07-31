from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from verba_supersession import replacement_for_verba


def _load_integration_registry():
    integration_store_stub = types.ModuleType("tateros.integration_store")
    integration_store_stub.get_enabled_integration_ids = lambda: []
    tateros_stub = types.ModuleType("tateros")
    tateros_stub.integration_store = integration_store_stub

    path = ROOT / "integration_registry.py"
    spec = importlib.util.spec_from_file_location("test_integration_registry", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load integration_registry.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "tateros": tateros_stub,
            "tateros.integration_store": integration_store_stub,
        },
    ):
        spec.loader.exec_module(module)
    return module


class DeviceRegistryAliasTests(unittest.TestCase):
    def test_integration_aliases_are_preserved(self) -> None:
        registry = _load_integration_registry()
        row = registry._coerce_device_row(
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

    def test_tater_has_no_hidden_category_device_verba(self) -> None:
        self.assertFalse((ROOT / "category_device_control.py").exists())


if __name__ == "__main__":  # pragma: no cover - direct script entry point
    unittest.main()
