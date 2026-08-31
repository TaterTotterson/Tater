from __future__ import annotations

import hashlib
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


class _ChunkedResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.read_sizes: list[int] = []

    def __enter__(self) -> _ChunkedResponse:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 1:
            raise AssertionError("Firmware downloads must use bounded reads.")
        self.read_sizes.append(size)
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


def _prebuilt_context(payload: bytes, *, size_bytes: int | None = None) -> dict[str, object]:
    return {
        "template_key": "satellite1_rpi_standalone",
        "firmware_version": "tater-sat1-standalone-v0.2.0",
        "prebuilt_firmware": {
            "native": True,
            "version": "tater-sat1-standalone-v0.2.0",
            "manifest_url": "https://example.test/manifest.json",
            "artifacts": {
                "ota": {
                    "kind": "ota",
                    "path": "https://example.test/tater-sat1-standalone-v0.2.0-ota.sat1",
                    "size_bytes": len(payload) if size_bytes is None else size_bytes,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            },
        },
    }


class PrebuiltFirmwareDownloadTests(unittest.TestCase):
    def test_remote_firmware_download_streams_in_bounded_chunks(self) -> None:
        payload = b"0123456789"
        response = _ChunkedResponse(payload)
        with tempfile.TemporaryDirectory() as temporary:
            cache_root = pathlib.Path(temporary)
            with (
                mock.patch.object(firmware, "FIRMWARE_PREBUILT_ROOT", cache_root),
                mock.patch.object(firmware, "_PREBUILT_FIRMWARE_DOWNLOAD_CHUNK_BYTES", 4),
                mock.patch.object(firmware.urllib_request, "urlopen", return_value=response),
            ):
                result = firmware._download_prebuilt_firmware_binary(_prebuilt_context(payload), "ota")

            self.assertEqual(payload, pathlib.Path(result["path"]).read_bytes())
            self.assertFalse(result["cached"])
            self.assertEqual([4, 4, 4, 4], response.read_sizes)

    def test_failed_stream_download_removes_partial_file(self) -> None:
        payload = b"0123456789"
        response = _ChunkedResponse(payload)
        with tempfile.TemporaryDirectory() as temporary:
            cache_root = pathlib.Path(temporary)
            with (
                mock.patch.object(firmware, "FIRMWARE_PREBUILT_ROOT", cache_root),
                mock.patch.object(firmware, "_PREBUILT_FIRMWARE_DOWNLOAD_CHUNK_BYTES", 4),
                mock.patch.object(firmware.urllib_request, "urlopen", return_value=response),
            ):
                with self.assertRaisesRegex(RuntimeError, "exceeded its expected size"):
                    firmware._download_prebuilt_firmware_binary(
                        _prebuilt_context(payload, size_bytes=len(payload) - 1),
                        "ota",
                    )

            self.assertEqual([], [path for path in cache_root.rglob("*") if path.is_file()])


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
    def test_thirdreality_ota_completes_after_device_accepts_handoff(self) -> None:
        session_id = "fw_thirdreality_handoff"
        session = _session()
        session.update(
            {
                "id": session_id,
                "selector": "native:tater-thirdreality-test",
                "template_key": "thirdreality_s420",
                "display_name": "Office S420",
                "firmware_version": "tater-thirdreality-0.2.11",
                "ota_url": "http://tater.test/firmware.swu",
                "binary_sha256": "a" * 64,
                "binary_size": 118391808,
            }
        )
        firmware._FIRMWARE_SESSIONS[session_id] = session
        try:
            with (
                mock.patch.object(
                    firmware,
                    "_native_client_status",
                    return_value={"connected": True, "connected_ts": 100.0},
                ),
                mock.patch.object(firmware, "_native_logs_fetch", return_value={"cursor": 0}),
                mock.patch(
                    "tater_voice.native_satellite.send_command",
                    new=mock.Mock(return_value=object()),
                ),
                mock.patch(
                    "tater_voice.native_satellite.run_on_runtime_loop",
                    return_value={"ok": True},
                ),
                mock.patch.object(firmware, "_save_recorded_firmware_version") as save_version,
            ):
                firmware._native_tater_ota_session_worker(session_id)

            result = firmware._FIRMWARE_SESSIONS[session_id]
            self.assertFalse(result["active"])
            self.assertEqual("completed", result["phase"])
            self.assertEqual(100.0, result["progress_percent"])
            self.assertEqual(0, result["returncode"])
            self.assertIn("finish installing it in the background", str(result["message"]))
            save_version.assert_called_once()
        finally:
            firmware._FIRMWARE_SESSIONS.pop(session_id, None)

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
