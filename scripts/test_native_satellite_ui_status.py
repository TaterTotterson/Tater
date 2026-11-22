from __future__ import annotations

import ast
import copy
from pathlib import Path
import sys
from typing import Any, Dict
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import home, native_satellite, reply_playback, ui_helpers


SELECTOR = "native:voicepe-test"


def client(*, connected=True, ready=True, interface="usb_ethernet"):
    return {
        "device_id": "voicepe-test", "device_name": "Test Voice PE", "board": "voice-pe",
        "connected": connected, "host": "192.0.2.10", "last_seen_ts": 1700000000,
        "binary_frames": 128, "binary_bytes": 81920, "log_count": 12,
        "last_status": {
            "state": "idle", "wifi_rssi": -52,
            "network": {"active_interface": interface, "ethernet_link": True,
                        "ethernet_ip": "192.0.2.10", "wifi_ip": "192.0.2.11"},
            "wake_engine": {"ready": ready, "active_wake_word": "hey_tater",
                            "active_wake_label": "Hey Tater", "active_model_source": "url"},
            "transport": {"audio_send_failure_total": 0},
        },
    }


def cache():
    return {
        "header_stats": [{"label": "Connected", "value": 1}],
        "ui": {"item_forms": [{
            "id": SELECTOR, "group": "satellite", "connected": True,
            "connection_transport": "wifi", "connection_note": "Connected by Wi-Fi.",
            "hero_badges": [{"label": "Connected", "tone": "success"}],
            "popup_fields": [{"key": "volume_percent", "value": 61},
                             {"key": "wake_word", "value": "hey_tater"}],
            "fields": [{"key": "name", "value": "Test Voice PE"}],
            "detail_sections": [{"title": "Overview", "rows": [
                {"label": "State", "value": "starting"},
                {"label": "Wake Tuning", "value": "keep configured tuning"},
            ]}],
            "info_fields": ui_helpers._native_popup_fields_from_sections([
                {"title": "Connection", "rows": [{"label": "Last Seen", "value": "old"}]},
                {"title": "Diagnostics", "rows": [
                    {"label": "Network", "value": "Connected by Wi-Fi"},
                    {"label": "Wake Engine", "value": "microWakeWord · Not Ready"},
                    {"label": "Voice Session Frames", "value": "0"},
                ]},
                {"title": "Settings", "rows": [{"label": "Volume", "value": "61%"}]},
            ]),
        }, {"id": "other-form", "group": "platform", "fields": [{"key": "keep", "value": 42}]}]},
    }


def fields_in(item, section):
    result = {}
    active = False
    for field in item["info_fields"]:
        if field.get("type") == "section":
            active = field["label"] == section
        elif active:
            result[field["label"]] = field["value"]
    return result


class NativeSatelliteUiStatusTests(unittest.TestCase):
    def refresh(self, payload, row):
        with mock.patch.object(native_satellite, "status_snapshot_sync", return_value={
            "clients": {SELECTOR: row}
        }) as snapshot, mock.patch.object(
            reply_playback, "build_reply_playback_options", side_effect=AssertionError("discovery ran")
        ), mock.patch.object(
            native_satellite, "_live_settings_payload", side_effect=AssertionError("settings I/O ran")
        ):
            result = home.refresh_satellite_runtime_payload(payload)
        snapshot.assert_called_once_with(include_live_settings=False)
        return result

    def test_each_esp_board_uses_reported_active_interface(self):
        for board in ("voice-pe", "satellite1", "respeaker-xvf3800", "s3-box"):
            with self.subTest(board=board):
                row = client()
                row["board"] = board
                runtime = home._native_client_to_runtime_row(SELECTOR, row)
                self.assertEqual(runtime["connection_transport"], "ethernet")
                self.assertEqual(runtime["connection_note"], "Connected by USB Ethernet.")

    def test_active_wifi_wins_over_link_presence_and_legacy_ethernet_status(self):
        row = client(interface="wifi")
        row["last_status"]["usb_ethernet"] = {"active": True, "state": "rescue", "transport": "ethernet"}
        runtime = home._native_client_to_runtime_row(SELECTOR, row)
        self.assertEqual(runtime["connection_transport"], "wifi")
        self.assertEqual(runtime["connection_note"], "Connected by Wi-Fi.")
        self.assertFalse(runtime["connection_degraded"])

    def test_no_active_interface_is_not_reported_as_wifi(self):
        runtime = home._native_client_to_runtime_row(SELECTOR, client(interface="none"))
        self.assertEqual(runtime["connection_transport"], "unknown")
        self.assertIn("unavailable", runtime["connection_note"])

    def test_latest_heartbeat_replaces_cached_diagnostics_without_mutating_settings(self):
        payload = cache()
        original = copy.deepcopy(payload)
        result = self.refresh(payload, client())
        item = result["ui"]["item_forms"][0]
        details = fields_in(item, "Diagnostics")
        self.assertEqual(details["Network"], "Connected by USB Ethernet")
        self.assertEqual(details["Wake Engine"], "microWakeWord · Ready")
        self.assertEqual(details["Voice Session Frames"], "128")
        self.assertEqual(details["Voice Session Bytes"], "81920")
        self.assertEqual(details["Buffered Logs"], "12")
        self.assertEqual(fields_in(item, "Transport")["Audio Send Failures"], "0")
        self.assertNotEqual(fields_in(item, "Connection")["Last Seen"], "old")
        self.assertEqual(fields_in(item, "Settings")["Volume"], "61%")
        self.assertEqual(item["popup_fields"], original["ui"]["item_forms"][0]["popup_fields"])
        self.assertEqual(item["fields"], original["ui"]["item_forms"][0]["fields"])
        self.assertEqual(result["ui"]["item_forms"][1], original["ui"]["item_forms"][1])
        self.assertEqual(payload, original)

    def test_poll_updates_ready_false_and_then_wifi_fallback(self):
        first = self.refresh(cache(), client())
        second = self.refresh(first, client(ready=False, interface="wifi"))
        item = second["ui"]["item_forms"][0]
        self.assertEqual(item["connection_transport"], "wifi")
        self.assertEqual(fields_in(item, "Diagnostics")["Wake Engine"], "microWakeWord · Not Ready")

    def test_ready_engine_does_not_turn_cache_success_note_into_an_error(self):
        row = client()
        row["last_status"]["wake_engine"]["last_error"] = "wake model cache loaded"
        item = self.refresh(cache(), row)["ui"]["item_forms"][0]
        self.assertEqual(fields_in(item, "Wake Model")["Status"], "Ready")

    def test_missing_first_status_is_unavailable_and_clears_old_transport(self):
        payload = self.refresh(cache(), client())
        row = client()
        row["last_status"] = {}
        result = self.refresh(payload, row)
        item = result["ui"]["item_forms"][0]
        self.assertEqual(fields_in(item, "Diagnostics")["Wake Engine"], "microWakeWord · Unavailable")
        self.assertEqual(fields_in(item, "Transport"), {})

    def test_disconnect_updates_card_badge_and_connection_count(self):
        result = self.refresh(cache(), client(connected=False))
        item = result["ui"]["item_forms"][0]
        self.assertFalse(item["connected"])
        self.assertEqual(item["connection_transport"], "offline")
        self.assertEqual(item["connection_note"], "Satellite is offline.")
        self.assertEqual(item["hero_badges"][0]["label"], "Offline")
        self.assertEqual(result["header_stats"][0]["value"], 0)

    def test_lightweight_synchronous_snapshot_skips_settings_io(self):
        row = {"hello": {"payload": {"board": "voice-pe"}}, "last_status": {"wake_engine": {"ready": True}}}
        with mock.patch.dict(native_satellite._clients, {SELECTOR: row}, clear=True), mock.patch.object(
            native_satellite, "_voice_snapshot", return_value={}
        ), mock.patch.object(native_satellite, "_live_settings_payload") as settings:
            snapshot = native_satellite.status_snapshot_sync(include_live_settings=False)
        settings.assert_not_called()
        self.assertTrue(snapshot["clients"][SELECTOR]["last_status"]["wake_engine"]["ready"])
        self.assertNotIn("live_settings", snapshot["clients"][SELECTOR])

    def test_real_cached_endpoint_applies_live_overlay_without_rebuilding_snapshot(self):
        path = Path(__file__).resolve().parents[1] / "tater/web/application.py"
        node = next(node for node in ast.parse(path.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name == "get_voice_runtime_payload")
        node.decorator_list = []
        scheduler = mock.Mock()
        payload = cache()
        namespace = {"Any": Any, "Dict": Dict, "esphome_home_module": home,
                     "_esphome_platform_tab_spec": lambda: {},
                     "_voice_satellite_snapshot_load": lambda: (payload, {"age_seconds": 30}),
                     "VOICE_SATELLITE_SNAPSHOT_INTERVAL_SECONDS": 300,
                     "system_task_manager": scheduler}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
        with mock.patch.object(native_satellite, "status_snapshot_sync", return_value={"clients": {SELECTOR: client()}}):
            result = namespace["get_voice_runtime_payload"]("satellites")
        self.assertEqual(fields_in(result["payload"]["ui"]["item_forms"][0], "Diagnostics")["Wake Engine"],
                         "microWakeWord · Ready")
        scheduler.request_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
