from __future__ import annotations

import pathlib
import sys
import time
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import firmware


def _session() -> dict[str, object]:
    return {
        "selector": "native:voicepe-test",
        "template_key": "voicepe",
        "display_name": "Test Voice PE",
        "firmware_version": "native-voicepe-0.3.13",
        "operation": "native_tater_ota",
        "active": True,
        "phase": "live_logs",
        "progress_percent": 18.0,
        "ota_initial_connected_ts": 100.0,
        "ota_disconnect_seen": False,
        "ota_reboot_requested": False,
        "ota_verify_deadline_ts": time.time() + 60.0,
        "returncode": None,
        "error": "",
        "message": "",
    }


def _ota_entry(status: str, progress: int, message: str = "") -> dict[str, object]:
    return {
        "level": "error" if status == "error" else "info",
        "message": message,
        "payload": {
            "status": status,
            "progress": progress,
            "message": message,
        },
    }


class NativeFirmwareOtaCompletionTests(unittest.TestCase):
    def test_writing_progress_waits_for_reboot_verification(self) -> None:
        session = _session()

        firmware._apply_native_ota_update_locked(
            session,
            [_ota_entry("writing", 95, "OTA writing")],
            {
                "connected": True,
                "connected_ts": 100.0,
                "firmware_version": "native-voicepe-0.3.11",
            },
        )

        self.assertTrue(session["active"])
        self.assertEqual(95.0, session["progress_percent"])

    def test_expected_version_reconnect_completes_when_final_status_was_missed(self) -> None:
        session = _session()

        with mock.patch.object(firmware, "_save_recorded_firmware_version") as save_version:
            firmware._apply_native_ota_update_locked(
                session,
                [],
                {
                    "connected": True,
                    "connected_ts": 200.0,
                    "firmware_version": "native-voicepe-0.3.13",
                },
            )

        self.assertFalse(session["active"])
        self.assertEqual("completed", session["phase"])
        self.assertEqual(100.0, session["progress_percent"])
        self.assertEqual(0, session["returncode"])
        save_version.assert_called_once()

    def test_old_version_reconnect_reports_rollback_instead_of_hanging(self) -> None:
        session = _session()

        firmware._apply_native_ota_update_locked(
            session,
            [],
            {
                "connected": True,
                "connected_ts": 200.0,
                "firmware_version": "native-voicepe-0.3.11",
            },
        )

        self.assertFalse(session["active"])
        self.assertEqual("failed", session["phase"])
        self.assertEqual(1, session["returncode"])
        self.assertIn("native-voicepe-0.3.11", str(session["error"]))
        self.assertIn("native-voicepe-0.3.13", str(session["error"]))

    def test_device_error_is_returned_to_the_ui(self) -> None:
        session = _session()

        firmware._apply_native_ota_update_locked(
            session,
            [_ota_entry("error", 95, "OTA failed during ota end")],
            {"connected": True, "connected_ts": 100.0},
        )

        self.assertFalse(session["active"])
        self.assertEqual("failed", session["phase"])
        self.assertEqual("OTA failed during ota end", session["error"])

    def test_missing_reconnect_times_out_instead_of_hanging_forever(self) -> None:
        session = _session()
        session["ota_verify_deadline_ts"] = time.time() - 1.0

        firmware._apply_native_ota_update_locked(
            session,
            [],
            {"connected": False},
        )

        self.assertFalse(session["active"])
        self.assertEqual("failed", session["phase"])
        self.assertIn("Timed out", str(session["error"]))


if __name__ == "__main__":
    unittest.main()
