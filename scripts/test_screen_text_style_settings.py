from __future__ import annotations

import asyncio
import threading
import unittest
from unittest import mock

from tater_voice import native_live_settings as settings, native_satellite


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def hdel(self, key, *fields):
        for field in fields:
            self.hashes.get(key, {}).pop(field, None)

    def hget(self, *_args):
        return "off"


CAPS = {"screen": True, "screen_text_style": True, "screen_power_management_version": 1}


class ScreenTextStyleSettingsTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.redis.hashes[settings.SETTINGS_HASH_KEY] = {
            settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
        }
        self.patch = mock.patch.object(settings, "redis_client", self.redis)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_default_validation_and_partial_updates(self):
        for value, expected in ((None, "smooth"), ("matrix", "matrix"), (" MATRIX ", "matrix"),
                                ("unknown", "smooth"), (True, "smooth"), ({}, "smooth")):
            self.assertEqual(settings.normalize_settings({"screen_text_style": value})["screen_text_style"], expected)
        base = settings.normalize_settings({"screen_text_style": "matrix"})
        self.assertEqual(settings.normalize_settings({"screen_brightness": 30}, base=base)["screen_text_style"], "matrix")

    def test_only_capable_per_satellite_screens_show_or_receive_the_choice(self):
        for board in ("checkers", "cronos", "crown", "athena", "rook"):
            fields = settings.settings_fields("native:one", board=board, capabilities=CAPS)
            field = next(row for row in fields if row.get("key") == "screen_text_style")
            self.assertEqual(field["type"], "select")
            self.assertEqual(field["value"], "smooth")
            self.assertEqual([row["value"] for row in field["options"]], ["smooth", "matrix"])
            self.assertEqual(settings.firmware_settings_snapshot("native:one", board=board, capabilities=CAPS)["screen_text_style"], "smooth")
        for selector, board, caps in (("", "athena", CAPS), ("native:one", "athena", {}),
                                      ("native:one", "biscuit", CAPS), ("native:one", "s3-box", CAPS),
                                      ("native:one", "voice-pe", CAPS),
                                      ("native:one", "athena", {"screen_text_style": "true"})):
            self.assertNotIn("screen_text_style", {row.get("key") for row in settings.settings_fields(selector, board=board, capabilities=caps)})
            self.assertNotIn("screen_text_style", settings.firmware_settings_snapshot(selector, board=board, capabilities=caps))

    def test_saves_are_isolated_and_survive_other_settings_saves(self):
        result = settings.save_settings({"screen_text_style": "matrix"}, selector="native:one", board="athena")
        self.assertIn("screen_text_style", result["changed_keys"])
        settings.save_settings({"screen_brightness": 44}, selector="native:one", board="athena")
        self.assertEqual(settings.settings_snapshot("native:one", board="athena")["screen_text_style"], "matrix")
        self.assertEqual(settings.settings_snapshot("native:two", board="athena")["screen_text_style"], "smooth")
        self.redis.hashes[settings.SETTINGS_HASH_KEY]["screen_text_style"] = "matrix"
        self.assertEqual(settings.settings_snapshot("native:two", board="athena")["screen_text_style"], "smooth")
        settings.save_settings({"screen_brightness": 28}, selector="native:two", board="athena")
        self.assertEqual(settings.settings_snapshot("native:two", board="athena")["screen_text_style"], "smooth")
        self.redis.hashes[settings.SETTINGS_HASH_KEY].pop("screen_text_style")
        settings.save_settings({"screen_text_style": "matrix"})
        self.assertNotIn("screen_text_style", self.redis.hashes[settings.SETTINGS_HASH_KEY])
        payload = settings.firmware_settings_snapshot("native:one", board="athena", capabilities=CAPS)
        self.assertEqual(payload["screen_text_style"], "matrix")

    def test_local_choices_are_validated_and_persisted_without_touching_other_preferences(self):
        key = settings.settings_hash_key("native:one")
        self.redis.hashes[key] = {"screen_brightness": "43"}
        self.assertTrue(settings.adopt_reported_device_text_style("matrix", selector="native:one", board="athena"))
        self.assertFalse(settings.adopt_reported_device_text_style("matrix", selector="native:one", board="athena"))
        self.assertEqual(self.redis.hashes[key], {"screen_brightness": "43", "screen_text_style": "matrix"})
        for value, selector, board in (("bad", "native:one", "athena"), ("matrix", "", "athena"), ("matrix", "native:two", "voice-pe")):
            self.assertFalse(settings.adopt_reported_device_text_style(value, selector=selector, board=board))


class ReportedTextStyleTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_choice_is_adopted_off_the_event_loop_and_only_after_success(self):
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        selector = "native:one"
        row = {"hello": {"payload": {"board": "athena", "capabilities": CAPS}}, "pending_requests": {}}
        threads = []

        def adopt(*_args, **_kwargs):
            threads.append(threading.get_ident())
            return True

        try:
            with mock.patch.dict(native_satellite._clients, {selector: row}, clear=True), \
                 mock.patch.object(settings, "adopt_reported_device_text_style", side_effect=adopt) as adopted, \
                 mock.patch.object(native_satellite, "_notify_state_change") as notify:
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"ok": True, "settings": {"screen_text_style": "matrix"}}})
                adopted.assert_called_once_with("matrix", selector=selector, board="athena")
                self.assertNotEqual(threads[0], threading.get_ident())
                notify.assert_called_once_with("settings", selector)
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"ok": False, "error": "Rejected", "settings": {"screen_text_style": "smooth"}}})
                self.assertEqual(adopted.call_count, 1)
                row["hello"]["payload"]["capabilities"] = {}
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"settings": {"screen_text_style": "smooth"}}})
                self.assertEqual(adopted.call_count, 1)
        finally:
            native_satellite._clients_lock = old_lock


if __name__ == "__main__":
    unittest.main()
