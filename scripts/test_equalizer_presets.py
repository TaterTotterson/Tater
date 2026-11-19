from __future__ import annotations

import asyncio
import json
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from tater_voice import native_live_settings as settings, native_satellite


class FakeRedis:
    def __init__(self):
        self.hashes = {settings.SETTINGS_HASH_KEY: {
            settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
            settings.WAKE_FAMILY_SETTINGS_MIGRATION_KEY: "true",
        }}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def hdel(self, key, *fields):
        for field in fields:
            self.hashes.get(key, {}).pop(field, None)

    def hget(self, *_args):
        return "off"


class EqualizerPresetSettingsTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.patch = mock.patch.object(settings, "redis_client", self.redis)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_warm_default_keeps_each_boards_speaker_protection(self):
        for board, floor in (("radar", -10), ("crown", -10), ("athena", -10), ("biscuit", -30),
                             ("checkers", -30), ("cronos", -30), ("rook", -30)):
            current = settings.settings_snapshot(f"native:{board}", board=board)
            self.assertEqual("warm", current["eq_preset"])
            self.assertEqual(settings.EQ_PRESETS["warm"], current["eq_bands"])
            self.assertFalse(current["eq_loudness"])
            self.assertEqual(floor, current["bass_guard_db"])

    def test_legacy_curves_remain_manual_even_when_they_match_a_preset(self):
        for curve in ([0] * 8, settings.EQ_PRESETS["warm"], [1, 2, 3, 4, 5, 6, 7, 8]):
            self.redis.hashes[settings.settings_hash_key("native:one")] = {"eq_bands": json.dumps(curve)}
            current = settings.settings_snapshot("native:one", board="rook")
            self.assertEqual("manual", current["eq_preset"])
            self.assertEqual(curve, current["eq_bands"])
            settings.save_settings({"screen_brightness": 44}, selector="native:one", board="rook")
            self.assertEqual(curve, settings.settings_snapshot("native:one", board="rook")["eq_bands"])

    def test_named_presets_manual_edits_and_unrelated_saves_survive_reconnect(self):
        for preset, curve in settings.EQ_PRESETS.items():
            settings.save_settings({"eq_preset": preset}, selector="native:one", board="crown")
            settings.save_settings({"screen_brightness": 44}, selector="native:one", board="crown")
            wire = settings.firmware_settings_snapshot("native:one", board="crown", capabilities={"output_chain": True})
            self.assertEqual(preset, wire["eqPreset"])
            self.assertEqual(curve, wire["eqBands"])
            self.assertFalse(wire["eqLoudness"])
            self.assertEqual(-10, wire["bassGuardDb"])
        settings.save_settings({"eq_bands": [1] * 8}, selector="native:one", board="crown")
        current = settings.settings_snapshot("native:one", board="crown")
        self.assertEqual("manual", current["eq_preset"])
        self.assertEqual([1] * 8, current["eq_bands"])
        settings.save_settings({"eq_preset": "warm"}, selector="native:one", board="crown")
        settings.save_settings({"eq_bands": settings.EQ_PRESETS["warm"], "screen_brightness": 33}, selector="native:one", board="crown")
        self.assertEqual("warm", settings.settings_snapshot("native:one", board="crown")["eq_preset"])

    def test_local_changes_are_validated_persisted_and_isolated(self):
        key = settings.settings_hash_key("native:one")
        self.redis.hashes[key] = {"screen_brightness": "43", "bass_guard_db": "-25"}
        selected = {"eq_preset": "warm", "eq_bands": settings.EQ_PRESETS["warm"], "eq_loudness": False}
        self.assertTrue(settings.adopt_reported_device_equalizer(selected, selector="native:one", board="crown"))
        self.assertFalse(settings.adopt_reported_device_equalizer(selected, selector="native:one", board="crown"))
        selected = {"eq_preset": "manual", "eq_bands": [2] * 8}
        self.assertTrue(settings.adopt_reported_device_equalizer(selected, selector="native:one", board="crown"))
        payload = settings.firmware_settings_snapshot("native:one", board="crown", capabilities={"output_chain": True})
        self.assertEqual("manual", payload["eqPreset"])
        self.assertEqual([2] * 8, payload["eqBands"])
        self.assertEqual(-25, payload["bassGuardDb"])
        self.assertEqual("43", self.redis.hashes[key]["screen_brightness"])
        self.assertEqual("warm", settings.settings_snapshot("native:two", board="crown")["eq_preset"])
        for invalid in ({"eq_preset": "unknown"}, {"eq_preset": "warm", "eq_bands": [99] * 8},
                        {"eq_preset": []}, {"eq_preset": {}},
                        {"eq_preset": "manual", "eq_bands": [True] * 8},
                        {"eq_preset": "manual", "eq_loudness": "false"}):
            self.assertFalse(settings.adopt_reported_device_equalizer(invalid, selector="native:one", board="crown"))

    def test_first_connection_adopts_local_eq_but_preserves_existing_host_eq(self):
        saved = {"eq_preset": "manual", "eq_bands": [4] * 8, "eq_loudness": False}
        self.assertTrue(settings.adopt_reported_device_equalizer(saved, selector="native:one", board="rook", only_if_missing=True))
        first = settings.firmware_settings_snapshot("native:one", board="rook", capabilities={"output_chain": True})
        self.assertEqual([4] * 8, first["eqBands"])
        self.assertEqual("manual", first["eqPreset"])
        settings.save_settings({"eq_preset": "voice"}, selector="native:one", board="rook")
        self.assertFalse(settings.adopt_reported_device_equalizer(saved, selector="native:one", board="rook", only_if_missing=True))
        self.assertEqual("voice", settings.settings_snapshot("native:one", board="rook")["eq_preset"])


class ReportedEqualizerTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_eq_is_adopted_before_initial_settings_are_built(self):
        saved = {"eq_preset": "manual", "eq_bands": [4] * 8, "eq_loudness": False}

        class FakeWebSocket:
            client = SimpleNamespace(host="127.0.0.1")
            headers = {}
            query_params = {}

            async def accept(self):
                pass

            async def receive_text(self):
                return json.dumps({"type": "hello", "payload": {
                    "device_id": "eq-test", "board": "rook", "eq_settings": saved,
                    "capabilities": {"output_chain": True, "eq_presets": True},
                }})

            async def send_json(self, _message):
                pass

        order = []

        def adopt(values, **kwargs):
            self.assertEqual(saved, values)
            self.assertTrue(kwargs["only_if_missing"])
            self.assertNotEqual(threading.get_ident(), owner_thread)
            order.append("adopt")
            return True

        def initial_settings(*_args, **_kwargs):
            self.assertEqual(["adopt"], order)
            order.append("snapshot")
            raise native_satellite.WebSocketDisconnect()

        owner_thread = threading.get_ident()
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        try:
            with mock.patch.object(native_satellite, "_authorize_websocket_hello", return_value={"ok": True, "mode": "paired"}), \
                 mock.patch.object(native_satellite, "_record_client", new=mock.AsyncMock(return_value=asyncio.Queue())), \
                 mock.patch.object(native_satellite, "_voice_bridge_for_websocket", new=mock.AsyncMock(return_value=None)), \
                 mock.patch.object(native_satellite, "_mark_disconnected", new=mock.AsyncMock(return_value=False)), \
                 mock.patch.object(native_satellite, "_vp", return_value=SimpleNamespace(logger=mock.Mock())), \
                 mock.patch.object(native_satellite, "_firmware_settings_payload", side_effect=initial_settings), \
                 mock.patch.object(settings, "adopt_reported_device_equalizer", side_effect=adopt):
                await native_satellite.handle_websocket(FakeWebSocket())
            self.assertEqual(["adopt", "snapshot"], order)
        finally:
            native_satellite._clients_lock = old_lock

    async def test_only_successful_capable_reports_are_adopted_off_the_event_loop(self):
        old_lock = native_satellite._clients_lock
        native_satellite._clients_lock = asyncio.Lock()
        selector = "native:one"
        row = {"hello": {"payload": {"board": "rook", "capabilities": {"eq_presets": True}}}, "pending_requests": {}}
        threads = []

        def adopt(*_args, **_kwargs):
            threads.append(threading.get_ident())
            return True

        try:
            with mock.patch.dict(native_satellite._clients, {selector: row}, clear=True), \
                 mock.patch.object(settings, "adopt_reported_device_equalizer", side_effect=adopt) as adopted, \
                 mock.patch.object(native_satellite, "_notify_state_change") as notify:
                values = {"eq_preset": "manual", "eq_bands": [1] * 8}
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"ok": True, "settings": values}})
                adopted.assert_called_once_with(values, selector=selector, board="rook")
                self.assertNotEqual(threads[0], threading.get_ident())
                notify.assert_called_once_with("settings", selector)
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"ok": False, "error": "Rejected", "settings": values}})
                self.assertEqual(adopted.call_count, 1)
                row["hello"]["payload"]["capabilities"] = {}
                await native_satellite._handle_text_message(selector, {"type": "settings.changed", "payload": {"ok": True, "settings": values}})
                self.assertEqual(adopted.call_count, 1)
        finally:
            native_satellite._clients_lock = old_lock


if __name__ == "__main__":
    unittest.main()
