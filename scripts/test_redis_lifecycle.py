#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import integration_registry
import redis_runtime


class _FakeRedis:
    def __init__(self, existing: dict):
        self.value = json.dumps(existing, separators=(",", ":"))
        self.set_calls: list[tuple[str, str]] = []

    def get(self, _key: str) -> str:
        return self.value

    def set(self, key: str, value: str) -> None:
        self.set_calls.append((key, value))
        self.value = value


class _GenerationRedis:
    def __init__(self, existing: dict):
        self.values = {
            integration_registry.INTEGRATION_DEVICE_REGISTRY_CACHE_KEY: json.dumps(existing, separators=(",", ":")),
            integration_registry.INTEGRATION_DEVICE_REGISTRY_GENERATION_KEY: "0",
        }
        self.set_calls: list[tuple[str, str]] = []

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.set_calls.append((key, value))
        self.values[key] = value

    def incr(self, key: str) -> int:
        value = int(self.values.get(key) or 0) + 1
        self.values[key] = str(value)
        return value

    def hgetall(self, _key: str) -> dict:
        return {}


def _registry_payload(*, name: str = "Kitchen Light", updated_at: float = 1.0) -> dict:
    device = {
        "id": "light.kitchen",
        "name": name,
        "state": "on",
        "status": "on",
        "online": True,
        "runtime_state": {
            "updated_at": updated_at,
            "payload": {"state": "on"},
        },
    }
    return {
        "devices": [device],
        "total": 1,
        "cache": {
            "version": integration_registry._DEVICE_REGISTRY_CACHE_VERSION,
            "enabled_integrations": ["homekit"],
            "generated_at": updated_at,
            "updated_at": updated_at,
            "duration_ms": updated_at,
        },
    }


class RedisLifecycleTests(unittest.TestCase):
    def test_internal_config_enables_bounded_aof_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "data_path": root / "dump.rdb",
                "data_dir": root,
                "pid_path": root / "redis.pid",
                "log_path": root / "redis.log",
                "socket_path": root / "redis.sock",
            }
            text = redis_runtime._internal_redis_config_text(
                paths,
                port=6380,
                use_unix_socket=False,
            )
        self.assertIn("appendonly yes", text)
        self.assertIn("aof-use-rdb-preamble yes", text)
        self.assertIn("auto-aof-rewrite-percentage 100", text)
        self.assertIn("auto-aof-rewrite-min-size 512mb", text)

    def test_registry_skips_rewrite_for_only_runtime_timestamp_changes(self) -> None:
        existing = _registry_payload(updated_at=1.0)
        fake = _FakeRedis(existing)
        incoming = _registry_payload(updated_at=99.0)
        with mock.patch.object(
            integration_registry,
            "_enabled_integration_ids",
            return_value=["homekit"],
        ):
            integration_registry.save_integration_device_registry_cache(incoming, fake)
        self.assertEqual(fake.set_calls, [])

    def test_registry_rewrites_when_inventory_changes(self) -> None:
        existing = _registry_payload(name="Kitchen Light")
        fake = _FakeRedis(existing)
        incoming = copy.deepcopy(existing)
        incoming["devices"][0]["name"] = "Island Light"
        with mock.patch.object(
            integration_registry,
            "_enabled_integration_ids",
            return_value=["homekit"],
        ):
            integration_registry.save_integration_device_registry_cache(incoming, fake)
        self.assertEqual(len(fake.set_calls), 1)

    def test_older_registry_scan_cannot_overwrite_new_generation(self) -> None:
        existing = _registry_payload(name="Current Camera")
        fake = _GenerationRedis(existing)
        stale = _registry_payload(name="Stale Camera")
        with mock.patch.object(integration_registry, "_enabled_integration_ids", return_value=["homekit"]):
            generation = integration_registry._device_registry_generation(fake)
            integration_registry.bump_integration_device_registry_generation(fake)
            result = integration_registry.save_integration_device_registry_cache(
                stale,
                fake,
                expected_generation=generation,
            )

        self.assertEqual(result["devices"][0]["name"], "Current Camera")
        saved = json.loads(fake.values[integration_registry.INTEGRATION_DEVICE_REGISTRY_CACHE_KEY])
        self.assertEqual(saved["devices"][0]["name"], "Current Camera")

    def test_targeted_integration_refresh_replaces_capabilities_immediately(self) -> None:
        existing = {
            "groups": [
                {
                    "id": "unifi_protect",
                    "name": "UniFi Protect",
                    "order": 70,
                    "devices": [
                        {
                            "id": "back-yard",
                            "name": "Back Yard",
                            "type": "camera",
                            "capabilities": ["camera", "snapshot"],
                            "actions": ["camera_snapshot"],
                        }
                    ],
                    "device_count": 1,
                }
            ],
            "devices": [],
            "total": 1,
            "errors": [],
            "cache": {
                "version": integration_registry._DEVICE_REGISTRY_CACHE_VERSION,
                "enabled_integrations": ["unifi_protect"],
                "generated_at": 1.0,
                "updated_at": 1.0,
            },
        }
        fake = _GenerationRedis(existing)
        module = SimpleNamespace(
            INTEGRATION={"id": "unifi_protect", "name": "UniFi Protect", "order": 70},
            integration_status=lambda: {"configured": True},
            integration_devices=lambda: {
                "devices": [
                    {
                        "id": "back-yard",
                        "name": "Back Yard",
                        "type": "camera",
                        "capabilities": ["camera", "snapshot", "video_clip"],
                        "actions": ["camera_snapshot", "camera_clip"],
                    }
                ]
            },
        )
        with (
            mock.patch.object(integration_registry, "_enabled_integration_ids", return_value=["unifi_protect"]),
            mock.patch.object(integration_registry, "_module_for_integration", return_value=module),
        ):
            refreshed = integration_registry.refresh_integration_device_group_cache(
                "unifi_protect",
                fake,
            )

        camera = refreshed["devices"][0]
        self.assertIn("video_clip", camera["capabilities"])
        self.assertIn("camera_clip", camera["actions"])
        self.assertEqual(refreshed["cache"]["generation"], 1)


if __name__ == "__main__":
    unittest.main()
