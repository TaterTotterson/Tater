#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import time
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
        self.assertIn('save ""', text)
        self.assertNotIn("save 300 10", text)

    def test_internal_shutdown_uses_aof_safe_nosave(self) -> None:
        commands: list[tuple] = []

        class _FakeClient:
            def execute_command(self, *args):
                commands.append(args)
                raise redis_runtime.redis.exceptions.ConnectionError("server closed the connection")

            def close(self):
                return None

        info = {
            "managed": True,
            "adopted": True,
            "pid": 43210,
            "host": "127.0.0.1",
            "port": 6380,
            "use_unix_socket": False,
        }
        with (
            mock.patch.object(redis_runtime, "_INTERNAL_REDIS_PROCESS", None),
            mock.patch.object(redis_runtime, "_INTERNAL_REDIS_INFO", info),
            mock.patch.object(redis_runtime.redis, "Redis", return_value=_FakeClient()),
            mock.patch.object(redis_runtime, "_pid_is_running", return_value=False),
        ):
            redis_runtime._stop_internal_redis_locked()

        self.assertEqual(commands, [("SHUTDOWN", "NOSAVE")])

    def test_stale_rdb_temp_cleanup_keeps_live_and_recent_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stale = root / "temp-111111.rdb"
            live = root / "temp-222222.rdb"
            recent = root / "temp-333333.rdb"
            stale.write_bytes(b"stale")
            live.write_bytes(b"live")
            recent.write_bytes(b"recent")
            old = time.time() - 3600
            os.utime(stale, (old, old))
            os.utime(live, (old, old))
            paths = {"data_dir": root}

            with mock.patch.object(redis_runtime, "_pid_is_running", side_effect=lambda pid: pid == 222222):
                result = redis_runtime._cleanup_stale_internal_redis_temp_files(
                    paths,
                    minimum_age_seconds=300,
                )

            self.assertEqual(result, {"removed": 1, "bytes": 5})
            self.assertFalse(stale.exists())
            self.assertTrue(live.exists())
            self.assertTrue(recent.exists())

    def test_orphan_selection_prefers_recorded_server_then_newest(self) -> None:
        instances = [
            {"pid": 10, "port": 61000, "started_at": 10.0},
            {"pid": 20, "port": 62000, "started_at": 20.0},
        ]
        selected = redis_runtime._select_internal_redis_instance(
            instances,
            preferred_port=61000,
            preferred_pid=10,
        )
        self.assertEqual(selected["pid"], 10)
        newest = redis_runtime._select_internal_redis_instance(instances)
        self.assertEqual(newest["pid"], 20)

    def test_transient_cached_ping_does_not_restart_internal_redis(self) -> None:
        config = redis_runtime._normalize_config(
            {"mode": "internal", "data_path": "/tmp/tater-test-redis/dump.rdb"},
            allow_empty_host=True,
        )
        broken = mock.Mock()
        broken.ping.side_effect = redis_runtime.redis.exceptions.ConnectionError("stale pooled connection")
        healthy = mock.Mock()
        healthy.ping.return_value = True
        with (
            mock.patch.object(redis_runtime, "_load_config_locked", return_value=config),
            mock.patch.object(redis_runtime, "get_redis_client", side_effect=[broken, healthy]),
            mock.patch.object(redis_runtime, "_reset_clients_locked"),
            mock.patch.object(redis_runtime, "_stop_internal_redis_locked") as stop,
        ):
            status = redis_runtime.get_redis_connection_status()

        self.assertTrue(status["connected"])
        stop.assert_not_called()

    def test_live_internal_process_is_not_killed_after_short_ping_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = redis_runtime._normalize_config(
                {"mode": "internal", "data_path": str(Path(tmp) / "dump.rdb")},
                allow_empty_host=True,
            )
            state_key = redis_runtime._internal_redis_state_key(config)
            fake_process = mock.Mock()
            fake_process.poll.return_value = None
            tracked = {
                "managed": True,
                "pid": 43210,
                "port": 6380,
                "host": "127.0.0.1",
                "use_unix_socket": False,
                "state_key": state_key,
            }
            with (
                mock.patch.object(redis_runtime, "_INTERNAL_REDIS_PROCESS", fake_process),
                mock.patch.object(redis_runtime, "_INTERNAL_REDIS_INFO", tracked),
                mock.patch.object(redis_runtime, "_internal_redis_existing_ping", return_value=(False, "busy")),
                mock.patch.object(redis_runtime, "_stop_internal_redis_locked") as stop,
                self.assertRaises(redis_runtime.RedisNotConfiguredError),
            ):
                redis_runtime._ensure_internal_redis_server_locked(config)

            stop.assert_not_called()

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

    def test_cached_registry_can_skip_runtime_state_overlay(self) -> None:
        existing = _registry_payload()
        fake = _FakeRedis(existing)
        with (
            mock.patch.object(integration_registry, "_enabled_integration_ids", return_value=["homekit"]),
            mock.patch.object(integration_registry, "_apply_runtime_state_overlay_to_registry") as overlay,
        ):
            registry = integration_registry.get_cached_integration_device_registry(
                fake,
                overlay_runtime_state=False,
            )

        self.assertEqual(registry["devices"][0]["name"], "Kitchen Light")
        overlay.assert_not_called()

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
