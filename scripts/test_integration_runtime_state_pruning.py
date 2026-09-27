#!/usr/bin/env python3
from __future__ import annotations

import json
import unittest

import integration_runtime


class _FakeRedis:
    def __init__(self, states: dict[str, dict]) -> None:
        self.hashes = {
            integration_runtime.INTEGRATION_RUNTIME_STATES_KEY: {
                key: json.dumps(value, separators=(",", ":"))
                for key, value in states.items()
            }
        }

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, field: str) -> int:
        target = self.hashes.get(key, {})
        existed = field in target
        target.pop(field, None)
        return int(existed)


class IntegrationRuntimeStatePruningTests(unittest.TestCase):
    def test_prunes_only_inactive_expired_provider_states(self) -> None:
        redis = _FakeRedis(
            {
                "unifi_network:client:active": {
                    "provider": "unifi_network",
                    "id": "client:active",
                    "updated_at": 10.0,
                    "payload": {},
                },
                "unifi_network:client:recent": {
                    "provider": "unifi_network",
                    "id": "client:recent",
                    "updated_at": 950.0,
                    "payload": {},
                },
                "unifi_network:client:stale": {
                    "provider": "unifi_network",
                    "id": "client:stale",
                    "updated_at": 10.0,
                    "payload": {},
                },
                "hue:light:stale": {
                    "provider": "hue",
                    "id": "light:stale",
                    "updated_at": 10.0,
                    "payload": {},
                },
            }
        )

        removed = integration_runtime._prune_runtime_provider_states(
            redis,
            "unifi_network",
            {"client:active"},
            retention_seconds=300,
            now=1000.0,
        )

        remaining = redis.hgetall(integration_runtime.INTEGRATION_RUNTIME_STATES_KEY)
        self.assertEqual(removed, 1)
        self.assertNotIn("unifi_network:client:stale", remaining)
        self.assertIn("unifi_network:client:active", remaining)
        self.assertIn("unifi_network:client:recent", remaining)
        self.assertIn("hue:light:stale", remaining)


if __name__ == "__main__":
    unittest.main()
