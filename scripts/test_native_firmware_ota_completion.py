from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
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


def _self_ota_session(session_id: str, version: str, created_ts: float) -> dict[str, object]:
    session = _session()
    session.update(
        {
            "id": session_id,
            "selector": "native:tater-sat1-local",
            "template_key": "satellite1_rpi_standalone",
            "display_name": "Tater Embedded SAT1",
            "firmware_version": version,
            "created_ts": created_ts,
            "ota_verify_deadline_ts": created_ts + 3600.0,
            "binary_name": "firmware.bin",
            "binary_size": 1234,
            "self_ota_recovery": True,
        }
    )
    return session


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

    def test_embedded_sat1_session_recovers_after_tater_restarts(self) -> None:
        session_id = "fw_self_ota_success"
        version = "tater-sat1-standalone-v0.2.0"
        created_ts = time.time()
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = pathlib.Path(temporary)
            environment = {firmware._SAT1_RPI_SELF_OTA_STATE_ENV: str(state_dir)}
            with mock.patch.dict(os.environ, environment, clear=False):
                session = _self_ota_session(session_id, version, created_ts)
                stale_result = state_dir / firmware._SAT1_RPI_SELF_OTA_SUCCESS_NAME
                stale_result.write_text(
                    json.dumps({"status": "accepted", "version": version, "timestamp": created_ts - 100.0}),
                    encoding="utf-8",
                )
                firmware._prepare_sat1_rpi_self_ota_handoff_locked(session)
                self.assertFalse(stale_result.exists())
                (state_dir / firmware._SAT1_RPI_SELF_OTA_SUCCESS_NAME).write_text(
                    json.dumps(
                        {
                            "ok": True,
                            "status": "accepted",
                            "version": version,
                            "previous_version": "tater-sat1-standalone-v0.1.2",
                            "timestamp": created_ts + 10.0,
                        }
                    ),
                    encoding="utf-8",
                )
                firmware._FIRMWARE_SESSIONS.pop(session_id, None)
                try:
                    with mock.patch.object(firmware, "_save_recorded_firmware_version") as save_version:
                        result = firmware._poll_flash_session(session_id)
                    self.assertFalse(result["active"])
                    self.assertEqual("completed", result["phase"])
                    self.assertEqual(100.0, result["progress_percent"])
                    self.assertTrue(result["self_ota_recovery"])
                    self.assertFalse((state_dir / firmware._SAT1_RPI_SELF_OTA_HANDOFF_NAME).exists())
                    save_version.assert_called_once()
                finally:
                    firmware._FIRMWARE_SESSIONS.pop(session_id, None)

    def test_embedded_sat1_rollback_is_reported_after_tater_restarts(self) -> None:
        session_id = "fw_self_ota_rollback"
        version = "tater-sat1-standalone-v0.2.0"
        previous_version = "tater-sat1-standalone-v0.1.2"
        created_ts = time.time()
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = pathlib.Path(temporary)
            environment = {firmware._SAT1_RPI_SELF_OTA_STATE_ENV: str(state_dir)}
            with mock.patch.dict(os.environ, environment, clear=False):
                firmware._persist_sat1_rpi_self_ota_handoff_locked(
                    _self_ota_session(session_id, version, created_ts)
                )
                (state_dir / firmware._SAT1_RPI_SELF_OTA_FAILURE_NAME).write_text(
                    json.dumps(
                        {
                            "ok": False,
                            "status": "rolled_back",
                            "version": version,
                            "previous_version": previous_version,
                            "timestamp": created_ts + 10.0,
                        }
                    ),
                    encoding="utf-8",
                )
                firmware._FIRMWARE_SESSIONS.pop(session_id, None)
                try:
                    result = firmware._poll_flash_session(session_id)
                    self.assertFalse(result["active"])
                    self.assertEqual("failed", result["phase"])
                    self.assertIn(previous_version, str(result["error"]))
                    self.assertFalse((state_dir / firmware._SAT1_RPI_SELF_OTA_HANDOFF_NAME).exists())
                finally:
                    firmware._FIRMWARE_SESSIONS.pop(session_id, None)

    def test_self_update_handoff_is_not_used_for_other_satellites(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = pathlib.Path(temporary)
            environment = {firmware._SAT1_RPI_SELF_OTA_STATE_ENV: str(state_dir)}
            with mock.patch.dict(os.environ, environment, clear=False):
                self.assertTrue(
                    firmware._sat1_rpi_self_ota_enabled(
                        "satellite1_rpi_standalone",
                        "127.0.0.1",
                    )
                )
                self.assertFalse(
                    firmware._sat1_rpi_self_ota_enabled(
                        "satellite1_rpi_standalone",
                        "10.4.20.198",
                    )
                )
                for template_key in ("satellite1_rpi_satellite", "thirdreality_s420", "satellite1"):
                    with self.subTest(template_key=template_key):
                        session = _self_ota_session(f"fw_{template_key}", "test-v2", time.time())
                        session["template_key"] = template_key
                        firmware._persist_sat1_rpi_self_ota_handoff_locked(session)
                        self.assertFalse((state_dir / firmware._SAT1_RPI_SELF_OTA_HANDOFF_NAME).exists())


if __name__ == "__main__":
    unittest.main()
