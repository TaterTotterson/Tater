#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import types
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

fake_helpers = types.ModuleType("helpers")
fake_helpers.redis_client = mock.Mock()
sys.modules.setdefault("helpers", fake_helpers)

from tater_voice import native_live_settings  # noqa: E402


class FakeRedis:
    def __init__(self) -> None:
        self.hashes = {
            native_live_settings.SETTINGS_HASH_KEY: {
                native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                native_live_settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
            }
        }

    def hgetall(self, key):
        return dict(self.hashes.get(str(key), {}))

    def hset(self, key, mapping=None, **_kwargs):
        self.hashes.setdefault(str(key), {}).update({str(k): str(v) for k, v in dict(mapping or {}).items()})
        return len(mapping or {})

    def hdel(self, key, *names):
        row = self.hashes.setdefault(str(key), {})
        for name in names:
            row.pop(str(name), None)

    def hget(self, key, name):
        return self.hashes.get(str(key), {}).get(str(name))


class SSHSatelliteSettingsTests(unittest.TestCase):
    capabilities = {"ssh_management": True, "ssh_management_version": 1}

    def test_supported_satellite_gets_unique_disabled_credentials_in_popup(self) -> None:
        redis = FakeRedis()
        with mock.patch.object(native_live_settings, "redis_client", redis):
            first = native_live_settings.settings_fields(
                "native:kitchen",
                board="biscuit",
                capabilities=self.capabilities,
            )
            second = native_live_settings.settings_fields(
                "native:office",
                board="radar",
                capabilities=self.capabilities,
            )

        first_by_key = {str(field.get("key") or ""): field for field in first}
        second_by_key = {str(field.get("key") or ""): field for field in second}
        first_password = str(first_by_key["ssh_password"]["value"])
        second_password = str(second_by_key["ssh_password"]["value"])
        self.assertFalse(first_by_key["ssh_enabled"]["value"])
        self.assertRegex(first_password, r"^tater-[a-z2-9]{4}-[a-z2-9]{4}-[a-z2-9]{4}$")
        self.assertNotEqual(first_password, second_password)
        self.assertTrue(first_by_key["ssh_password"]["copyable"])
        self.assertTrue(first_by_key["ssh_password"]["generatable"])

    def test_password_is_sent_only_when_enabled_and_capable(self) -> None:
        redis = FakeRedis()
        selector = "native:kitchen"
        password = "tater-k7pm-4xq2"
        with mock.patch.object(native_live_settings, "redis_client", redis):
            disabled = native_live_settings.firmware_settings_snapshot(
                selector,
                board="biscuit",
                capabilities=self.capabilities,
            )
            save_result = native_live_settings.save_settings(
                {"ssh_enabled": True, "ssh_password": password},
                selector=selector,
                board="biscuit",
            )
            enabled = native_live_settings.firmware_settings_snapshot(
                selector,
                board="biscuit",
                capabilities=self.capabilities,
            )
            general_snapshot = native_live_settings.settings_snapshot(selector, board="biscuit")
            unsupported = native_live_settings.firmware_settings_snapshot(
                selector,
                board="biscuit",
                capabilities={},
            )

        self.assertFalse(disabled["ssh_enabled"])
        self.assertNotIn("ssh_password", disabled)
        self.assertTrue(enabled["ssh_enabled"])
        self.assertEqual(password, enabled["ssh_password"])
        self.assertNotIn("ssh_password", general_snapshot)
        self.assertNotIn("ssh_password", save_result["settings"])
        self.assertNotIn("ssh_enabled", unsupported)
        self.assertNotIn("ssh_password", unsupported)

    def test_unsupported_firmware_has_no_ssh_controls(self) -> None:
        redis = FakeRedis()
        with mock.patch.object(native_live_settings, "redis_client", redis):
            fields = native_live_settings.settings_fields(
                "native:legacy",
                board="biscuit",
                capabilities={},
            )
        keys = {str(field.get("key") or "") for field in fields}
        self.assertTrue({"ssh_section", "ssh_enabled", "ssh_login", "ssh_password"}.isdisjoint(keys))
        self.assertNotIn(native_live_settings.settings_hash_key("native:legacy"), redis.hashes)

    def test_invalid_password_is_rejected_before_persistence(self) -> None:
        redis = FakeRedis()
        with mock.patch.object(native_live_settings, "redis_client", redis):
            with self.assertRaisesRegex(ValueError, "SSH password"):
                native_live_settings.save_settings(
                    {"ssh_enabled": True, "ssh_password": "same password everywhere"},
                    selector="native:kitchen",
                    board="biscuit",
                )

    def test_frontend_exposes_show_copy_and_new_password_tools(self) -> None:
        component = (
            REPO_ROOT / "frontend/src/settings/components/models/ModelFields.vue"
        ).read_text(encoding="utf-8")
        satellites = (
            REPO_ROOT / "frontend/src/settings/components/voice/VoiceSatellites.vue"
        ).read_text(encoding="utf-8")
        helpers = (REPO_ROOT / "tater_voice/ui_helpers.py").read_text(encoding="utf-8")

        self.assertIn("field.revealable", component)
        self.assertIn("field.copyable", component)
        self.assertIn("field.generatable", component)
        self.assertIn("tater-${groups.join", component)
        self.assertIn('field.get("key")) == "ssh_login"', helpers)
        self.assertIn("root@{host}", helpers)
        self.assertIn("Satellite Settings", satellites)


if __name__ == "__main__":
    unittest.main()
