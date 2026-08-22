#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest

import integration_runtime


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(self, name: str, key: str = "", value: str = "", mapping=None) -> int:
        target = self.hashes.setdefault(name, {})
        if mapping is not None:
            added = 0
            for field, item in mapping.items():
                if str(field) not in target:
                    added += 1
                target[str(field)] = str(item)
            return added
        created = key not in target
        target[key] = value
        return 1 if created else 0


class IntegrationRegistryBackgroundTests(unittest.TestCase):
    def test_new_runtime_device_emits_one_registry_refresh_event(self) -> None:
        redis = _FakeRedis()
        events: list[tuple[str, str]] = []

        def listener(event: str, device_key: str) -> None:
            events.append((event, device_key))

        integration_runtime.add_device_registry_change_listener(listener)
        try:
            integration_runtime._state_set(redis, "homeassistant", "light.office", {"state": "on"})
            integration_runtime._state_set(redis, "homeassistant", "light.office", {"state": "off"})
        finally:
            integration_runtime.remove_device_registry_change_listener(listener)

        self.assertEqual(events, [("device-discovered", "homeassistant:light.office")])

    def test_tater_disables_the_legacy_duplicate_registry_loop(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn("start_integration_runtime(redis_client, manage_device_registry_cache=False)", source)
        self.assertIn("ensure_integration_runtime_started(\n                redis_client,\n                manage_device_registry_cache=False,", source)

    def test_integration_updates_refresh_capabilities_after_runtime_restart(self) -> None:
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        registry_source = (REPO_ROOT / "integration_registry.py").read_text(encoding="utf-8")
        helper_start = app_source.index("def _restart_integration_runtime_if_running(")
        helper_end = app_source.index("\n\n@app.get(\"/api/shop/integrations\")", helper_start)
        helper_source = app_source[helper_start:helper_end]

        self.assertLess(
            helper_source.index("restart_integration_runtime(redis_client)"),
            helper_source.index("refresh_integration_device_group_cache("),
        )
        self.assertIn("refresh_integration_ids=[integration_id]", app_source)
        self.assertIn("expected_generation=generation", registry_source)
        self.assertIn("integration_store_module.integration_module(", registry_source)


if __name__ == "__main__":
    unittest.main()
