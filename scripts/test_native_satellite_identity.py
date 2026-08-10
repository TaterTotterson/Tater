from __future__ import annotations

import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import home, native_live_settings, native_satellite, stereo_pairs


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.hashes: dict[str, dict[str, object]] = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(dict(mapping or {}))
        return True

    def delete(self, key):
        self.hashes.pop(key, None)
        self.values.pop(key, None)
        return True


class NativeSatelliteIdentityTests(unittest.TestCase):
    def test_legacy_selector_matches_corrected_board_by_mac_suffix(self) -> None:
        self.assertTrue(
            native_satellite._same_native_hardware(
                "native:voicepe-2e88e8",
                "",
                "native:sat1-2e88e8",
                "aabbcc2e88e8",
            )
        )
        self.assertFalse(
            native_satellite._same_native_hardware(
                "native:voicepe-2e88e8",
                "",
                "native:sat1-ffffff",
                "aabbccffffff",
            )
        )

    def test_only_known_bad_default_name_is_corrected(self) -> None:
        self.assertEqual(
            native_satellite._device_name_from_hello(
                {"device_name": "Tater Voice PE", "board": "satellite1"}
            ),
            "Tater Sat1",
        )
        self.assertEqual(
            native_satellite._device_name_from_hello(
                {"device_name": "Tater Display Kitchen", "board": "satellite1"}
            ),
            "Tater Display Kitchen",
        )

    def test_paired_token_migrates_to_corrected_selector(self) -> None:
        device_token = "paired-device-token"
        old_selector = "native:voicepe-2e88e8"
        new_selector = "native:sat1-2e88e8"
        credentials = {
            "devices": {
                old_selector: {
                    "selector": old_selector,
                    "device_id": "voicepe-2e88e8",
                    "device_name": "Tater Display Kitchen",
                    "token_hash": native_satellite._token_hash(device_token),
                }
            }
        }
        saved: dict[str, object] = {}

        with mock.patch.object(
            native_satellite,
            "_load_credentials_unlocked",
            return_value=credentials,
        ), mock.patch.object(
            native_satellite,
            "_save_credentials_unlocked",
            side_effect=lambda value: saved.update(copy.deepcopy(value)),
        ):
            matched = native_satellite._valid_device_credential(
                device_token,
                new_selector,
                {
                    "device_id": "sat1-2e88e8",
                    "hardware_id": "aabbcc2e88e8",
                    "device_name": "Tater Display Kitchen",
                    "board": "satellite1",
                },
            )

        self.assertIsNotNone(matched)
        self.assertNotIn(old_selector, saved["devices"])
        self.assertEqual(saved["devices"][new_selector]["hardware_id"], "aabbcc2e88e8")

    def test_alias_and_device_settings_survive_selector_migration(self) -> None:
        redis = _FakeRedis()
        old_selector = "native:voicepe-2e88e8"
        new_selector = "native:sat1-2e88e8"
        redis.hashes[native_live_settings.settings_hash_key(old_selector)] = {
            "volume_percent": "63",
            "aec_delay_ms": "91",
        }

        with mock.patch.object(native_live_settings, "redis_client", redis), mock.patch.object(
            native_satellite,
            "_vp",
            return_value=SimpleNamespace(redis_client=redis),
        ):
            self.assertTrue(native_live_settings.migrate_selector(old_selector, new_selector))
            native_satellite._save_selector_alias(old_selector, new_selector)
            self.assertEqual(native_satellite._canonical_selector(old_selector), new_selector)

        self.assertEqual(
            redis.hashes[native_live_settings.settings_hash_key(new_selector)]["volume_percent"],
            "63",
        )

    def test_stereo_pair_member_is_migrated(self) -> None:
        redis = _FakeRedis()
        old_selector = "native:voicepe-2e88e8"
        new_selector = "native:sat1-2e88e8"
        redis.values[stereo_pairs.REDIS_STEREO_PAIRS_KEY] = json.dumps(
            {
                "version": 1,
                "pairs": [
                    {
                        "id": "kitchen1",
                        "name": "Kitchen",
                        "left_selector": old_selector,
                        "right_selector": "native:sat1-ffffff",
                    }
                ],
            }
        )
        with mock.patch.object(stereo_pairs, "redis_client", redis):
            self.assertTrue(stereo_pairs.migrate_member_selector(old_selector, new_selector))
            self.assertEqual(stereo_pairs.list_pairs()[0]["left_selector"], new_selector)

    def test_forget_action_uses_full_native_cleanup(self) -> None:
        selector = "native:voicepe-2e88e8"
        forget_call = ("forget", selector)
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.native_satellite, "forget", new=lambda _selector: forget_call),
            mock.patch.object(
                home.native_satellite,
                "run_on_runtime_loop",
                return_value={"ok": True, "removed": True, "runtime_removed": True},
            ) as run_mock,
            mock.patch.object(home.esphome_runtime, "status", return_value={}),
        ):
            result = home.handle_runtime_action(
                action="voice_satellite_remove",
                payload={"id": selector},
            )

        self.assertTrue(result["removed"])
        self.assertTrue(result["native_cleanup"]["runtime_removed"])
        self.assertEqual("Satellite forgotten.", result["message"])
        run_mock.assert_called_once_with(forget_call, timeout=5.0)


class NativeSatelliteForgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()

    async def asyncTearDown(self) -> None:
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()

    async def test_forget_purges_disconnected_runtime_credential_registry_and_aliases(self) -> None:
        selector = "native:voicepe-2e88e8"
        redis = _FakeRedis()
        redis.values[native_satellite.NATIVE_SELECTOR_ALIASES_KEY] = json.dumps(
            {
                "native:older-2e88e8": selector,
                selector: "native:sat1-2e88e8",
                "native:unrelated": "native:sat1-ffffff",
            }
        )
        credentials = {
            "devices": {
                selector: {
                    "selector": selector,
                    "device_id": "voicepe-2e88e8",
                    "token_hash": native_satellite._token_hash("old-device-token"),
                },
                "native:sat1-ffffff": {
                    "selector": "native:sat1-ffffff",
                    "device_id": "sat1-ffffff",
                    "token_hash": native_satellite._token_hash("other-device-token"),
                },
            }
        }
        saved_credentials: dict[str, object] = {}
        remove_registry = mock.Mock(return_value=True)
        notify = mock.Mock()
        native_satellite._clients[selector] = {
            "selector": selector,
            "connected": False,
            "pending_requests": {},
        }

        with mock.patch.object(
            native_satellite,
            "_vp",
            return_value=SimpleNamespace(
                redis_client=redis,
                _remove_satellite=remove_registry,
            ),
        ), mock.patch.object(
            native_satellite,
            "_load_credentials_unlocked",
            return_value=credentials,
        ), mock.patch.object(
            native_satellite,
            "_save_credentials_unlocked",
            side_effect=lambda value: saved_credentials.update(copy.deepcopy(value)),
        ), mock.patch.object(
            native_satellite,
            "_notify_state_change",
            notify,
        ):
            result = await native_satellite.forget(selector)
            status = await native_satellite.status()

        self.assertTrue(result["removed"])
        self.assertTrue(result["runtime_removed"])
        self.assertTrue(result["registry_removed"])
        self.assertEqual(1, result["credentials_removed"])
        self.assertEqual(2, result["aliases_removed"])
        self.assertNotIn(selector, status["clients"])
        remove_registry.assert_called_once_with(selector)
        notify.assert_called_once_with("forgotten", selector)
        self.assertNotIn(selector, saved_credentials["devices"])
        self.assertIn("native:sat1-ffffff", saved_credentials["devices"])
        self.assertEqual(
            {"native:unrelated": "native:sat1-ffffff"},
            json.loads(str(redis.values[native_satellite.NATIVE_SELECTOR_ALIASES_KEY])),
        )

    async def test_forget_rejects_connected_native_satellite(self) -> None:
        selector = "native:sat1-2e88e8"
        native_satellite._clients[selector] = {
            "selector": selector,
            "connected": True,
        }

        with self.assertRaisesRegex(RuntimeError, "cannot be forgotten"):
            await native_satellite.forget(selector)

        self.assertIn(selector, native_satellite._clients)


if __name__ == "__main__":
    unittest.main()
