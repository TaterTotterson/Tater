from __future__ import annotations

import asyncio
import copy
import json
import sys
import unittest
from collections import deque
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
    def test_pairing_retries_return_same_token_only_to_same_hardware(self) -> None:
        pairing_code = "123456"
        selector = "native:s3box-05469c"
        payload = {
            "device_id": "s3box-05469c",
            "hardware_id": "3c842705469c",
            "device_name": "Tater S3 Box Living Room",
            "board": "s3-box",
        }
        pairing_session = {
            "id": "pairing-session",
            "code_hash": native_satellite._token_hash(pairing_code),
            "display_code": "123 456",
            "created_ts": 1000.0,
            "expires_ts": 1600.0,
            "state": "waiting",
        }

        with (
            mock.patch.dict(
                native_satellite._pairing_sessions,
                {"pairing-session": pairing_session},
                clear=True,
            ),
            mock.patch.object(native_satellite, "_now", return_value=1000.0),
            mock.patch.object(
                native_satellite,
                "_new_device_token",
                return_value="tns_retry_safe_token",
            ),
            mock.patch.object(native_satellite, "_save_device_credential") as save_credential,
        ):
            first = native_satellite._redeem_pairing_code(pairing_code, selector, payload)
            retry = native_satellite._redeem_pairing_code(pairing_code, selector, payload)
            wrong_hardware = native_satellite._redeem_pairing_code(
                pairing_code,
                selector,
                {**payload, "hardware_id": "ffffffffffff"},
            )

        self.assertEqual(first["device_token"], "tns_retry_safe_token")
        self.assertEqual(retry["device_token"], "tns_retry_safe_token")
        self.assertIsNone(wrong_hardware)
        save_credential.assert_called_once_with(selector, payload, "tns_retry_safe_token")

    def test_expired_pairing_retry_cannot_recover_device_token(self) -> None:
        pairing_code = "123456"
        selector = "native:s3box-05469c"
        payload = {
            "device_id": "s3box-05469c",
            "hardware_id": "3c842705469c",
        }
        pairing_session = {
            "id": "pairing-session",
            "code_hash": native_satellite._token_hash(pairing_code),
            "state": "paired",
            "selector": selector,
            "device_id": payload["device_id"],
            "hardware_id": payload["hardware_id"],
            "device_token": "tns_expired_token",
            "expires_ts": 1029.0,
        }

        with (
            mock.patch.dict(
                native_satellite._pairing_sessions,
                {"pairing-session": pairing_session},
                clear=True,
            ),
            mock.patch.object(native_satellite, "_now", return_value=1030.0),
        ):
            retry = native_satellite._redeem_pairing_code(pairing_code, selector, payload)

        self.assertIsNone(retry)
        self.assertNotIn("device_token", pairing_session)

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
        self.assertEqual(
            native_satellite._device_name_from_hello(
                {
                    "device_name": "Tater Voice PE",
                    "board": "satellite1-beta-rev41",
                }
            ),
            "Tater Sat1 Beta.1",
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

    def test_reported_device_volume_is_persisted_as_an_override(self) -> None:
        redis = _FakeRedis()
        selector = "native:sat1-2e88e8"

        with mock.patch.object(native_live_settings, "redis_client", redis):
            self.assertTrue(
                native_live_settings.adopt_reported_device_volume(
                    65,
                    selector=selector,
                    board="satellite1",
                )
            )
            self.assertFalse(
                native_live_settings.adopt_reported_device_volume(
                    65,
                    selector=selector,
                    board="satellite1",
                )
            )

        self.assertEqual(
            redis.hashes[native_live_settings.settings_hash_key(selector)]["volume_percent"],
            "65",
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

    async def test_setup_mode_queues_reset_before_forgetting_pairing(self) -> None:
        selector = "native:voicepe-2e88e8"
        calls: list[str] = []

        async def send_command(target, message_type, payload):
            calls.append("reset")
            self.assertEqual(target, selector)
            self.assertEqual(message_type, "setup.reset")
            self.assertEqual(payload["reason"], "user_requested_setup_mode")
            return {"ok": True, "selector": target}

        async def forget(target):
            calls.append("forget")
            self.assertEqual(target, selector)
            return {"ok": True, "removed": True, "credentials_removed": 1}

        with mock.patch.object(native_satellite, "_canonical_selector", return_value=selector), mock.patch.object(
            native_satellite,
            "send_command",
            side_effect=send_command,
        ), mock.patch.object(
            native_satellite,
            "forget",
            side_effect=forget,
        ):
            result = await native_satellite.enter_setup_mode_and_forget(selector)

        self.assertEqual(calls, ["reset", "forget"])
        self.assertTrue(result["removed"])
        self.assertEqual(result["forgotten"]["credentials_removed"], 1)

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

    async def test_reconnect_preserves_ota_log_ring(self) -> None:
        selector = "native:voicepe-2e88e8"
        hello = {
            "type": "hello",
            "payload": {
                "device_id": "voicepe-2e88e8",
                "device_name": "Voice PE",
                "board": "voice-pe",
                "firmware_version": "native-voicepe-0.3.13",
            },
        }
        first_socket = SimpleNamespace(client=SimpleNamespace(host="192.0.2.10"))
        second_socket = SimpleNamespace(client=SimpleNamespace(host="192.0.2.10"))

        with mock.patch.object(native_satellite, "_cancel_media_disconnect_abort"), mock.patch.object(
            native_satellite,
            "_upsert_registry_from_hello",
        ), mock.patch.object(
            native_satellite,
            "_load_selector_aliases",
            return_value={},
        ), mock.patch.object(
            native_satellite,
            "_notify_state_change",
        ):
            await native_satellite._record_client(selector, first_socket, hello)
            native_satellite._clients[selector]["logs"] = deque(
                [
                    {
                        "seq": 7,
                        "type": "ota.status",
                        "payload": {"status": "rebooting", "progress": 100},
                    }
                ],
                maxlen=native_satellite.MAX_LOG_ROWS,
            )
            native_satellite._clients[selector]["log_seq"] = 7

            await native_satellite._record_client(selector, second_socket, hello)

        row = native_satellite._clients[selector]
        self.assertEqual(7, row["log_seq"])
        self.assertEqual("rebooting", row["logs"][-1]["payload"]["status"])

    async def test_hardware_volume_messages_refresh_the_saved_slider(self) -> None:
        selector = "native:sat1-2e88e8"
        native_satellite._clients[selector] = {
            "selector": selector,
            "connected": True,
            "hello": {
                "type": "hello",
                "payload": {"board": "satellite1"},
            },
            "pending_requests": {},
        }

        for message_type in ("settings.changed", "status"):
            native_satellite._clients[selector].pop("reported_volume_percent", None)
            with self.subTest(message_type=message_type), mock.patch.object(
                native_live_settings,
                "adopt_reported_device_volume",
                return_value=True,
            ) as adopt, mock.patch.object(
                native_satellite,
                "_notify_state_change",
            ) as notify:
                message = {
                    "type": message_type,
                    "payload": {
                        "settings": {"volume_percent": 65},
                    },
                }
                await native_satellite._handle_text_message(selector, message)
                await native_satellite._handle_text_message(selector, message)

                adopt.assert_called_once_with(
                    65,
                    selector=selector,
                    board="satellite1",
                )
                notify.assert_called_once_with("settings", selector)

    async def test_settings_apply_failure_is_retained_and_exposed_in_diagnostics(self) -> None:
        selector = "native:echo-test"
        native_satellite._clients[selector] = {
            "selector": selector,
            "connected": True,
            "hello": {
                "type": "hello",
                "payload": {
                    "device_id": "echo-test",
                    "device_name": "Kitchen Echo",
                    "board": "biscuit",
                    "capabilities": {"speaker": True},
                },
            },
            "last_status": {
                "state": "idle",
                "wake_engine": {
                    "ready": True,
                    "active_wake_word": "hey_tater",
                    "active_model_source": "embedded",
                },
            },
            "last_settings_result": {},
            "pending_requests": {},
        }

        with mock.patch.object(native_satellite, "_notify_state_change") as notify:
            await native_satellite._handle_text_message(
                selector,
                {
                    "type": "settings.changed",
                    "payload": {
                        "ok": False,
                        "error": "wake model HTTPS certificate is not yet valid",
                        "settings": {"wake_word": "custom_url", "volume_percent": 75},
                    },
                },
            )

        notify.assert_any_call("settings_result", selector)
        snapshot = native_satellite._client_snapshot(selector, native_satellite._clients[selector])
        self.assertFalse(snapshot["settings_result"]["ok"])
        self.assertIn("certificate", snapshot["settings_result"]["error"])

        runtime = home._native_client_to_runtime_row(selector, snapshot)
        diagnostics = next(
            section["rows"]
            for section in runtime["native_detail_sections"]
            if section["title"] == "Diagnostics"
        )
        values = {row["label"]: row["value"] for row in diagnostics}
        self.assertEqual("Failed", values["Settings Apply"])
        self.assertIn("certificate", values["Settings Error"])

        # A physical volume delta is not a full apply and must not hide the
        # model error. A later full settings acknowledgement clears it.
        await native_satellite._handle_text_message(
            selector,
            {"type": "settings.changed", "payload": {"ok": True, "settings": {"volume_percent": 60}}},
        )
        self.assertFalse(native_satellite._clients[selector]["last_settings_result"]["ok"])
        await native_satellite._handle_text_message(
            selector,
            {"type": "settings.changed", "payload": {"ok": True, "settings": {"wake_word": "custom_url"}}},
        )
        self.assertTrue(native_satellite._clients[selector]["last_settings_result"]["ok"])


if __name__ == "__main__":
    unittest.main()
