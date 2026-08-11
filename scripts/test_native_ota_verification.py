#!/usr/bin/env python3
from __future__ import annotations

import time
import sys
import types
import unittest
from unittest import mock

# Keep this focused backend test runnable in the lightweight bundled test
# runtime. A full Tater environment uses the real modules and skips this shim.
try:
    import helpers as _helpers  # noqa: F401
    from tater_voice import display_bus as _display_bus  # noqa: F401
    from tater_voice import runtime as _runtime  # noqa: F401
    from tater_voice import ui_helpers as _ui_helpers  # noqa: F401
except (ImportError, ModuleNotFoundError, TypeError):
    for module_name in (
        "helpers",
        "tater_voice.display_bus",
        "tater_voice.runtime",
        "tater_voice.ui_helpers",
    ):
        sys.modules.pop(module_name, None)
    helpers_stub = types.ModuleType("helpers")
    helpers_stub.redis_client = mock.Mock()
    sys.modules["helpers"] = helpers_stub
    for dependency_name in ("tater_voice.display_bus", "tater_voice.ui_helpers"):
        sys.modules[dependency_name] = types.ModuleType(dependency_name)
    runtime_stub = types.ModuleType("tater_voice.runtime")
    runtime_stub.text = lambda value: "" if value is None else str(value).strip()
    runtime_stub.lower = lambda value: runtime_stub.text(value).lower()
    sys.modules["tater_voice.runtime"] = runtime_stub

from tater_voice import firmware


class NativeOtaVerificationTests(unittest.TestCase):
    @staticmethod
    def _session() -> dict[str, object]:
        return {
            "selector": "native:test-s420",
            "template_key": "thirdreality_s420",
            "display_name": "Test S420",
            "firmware_version": "tater-thirdreality-0.2.3",
            "operation": "native_tater_ota",
            "active": True,
            "returncode": None,
            "phase": "live_logs",
            "progress_percent": 18.0,
            "ota_initial_connected_ts": 100.0,
            "ota_reboot_requested": False,
            "ota_disconnect_seen": False,
            "ota_verify_deadline_ts": time.time() + 60.0,
        }

    @staticmethod
    def _rebooting_entry(progress: int = 92) -> dict[str, object]:
        return {
            "level": "info",
            "message": "Verified signed firmware; rebooting into recovery.",
            "payload": {"status": "rebooting", "progress": progress},
        }

    def test_rebooting_is_not_terminal_success(self) -> None:
        session = self._session()

        firmware._apply_native_ota_update_locked(
            session,
            [self._rebooting_entry(progress=100)],
            {
                "connected": True,
                "connected_ts": 100.0,
                "firmware_version": "tater-thirdreality-0.2.2",
            },
        )

        self.assertTrue(session["active"])
        self.assertIsNone(session["returncode"])
        self.assertEqual(session["phase"], "awaiting_device_logs")
        self.assertEqual(session["progress_percent"], 99.0)

    def test_expected_version_after_disconnect_completes_ota(self) -> None:
        session = self._session()
        firmware._apply_native_ota_update_locked(
            session,
            [self._rebooting_entry()],
            {"connected": False},
        )

        with mock.patch.object(firmware, "_save_recorded_firmware_version") as save_version:
            firmware._apply_native_ota_update_locked(
                session,
                [],
                {
                    "connected": True,
                    "connected_ts": 200.0,
                    "firmware_version": "tater-thirdreality-0.2.3",
                },
            )

        self.assertFalse(session["active"])
        self.assertEqual(session["returncode"], 0)
        self.assertEqual(session["phase"], "completed")
        self.assertEqual(session["progress_percent"], 100.0)
        save_version.assert_called_once()

    def test_wrong_version_after_reconnect_fails_ota(self) -> None:
        session = self._session()
        session["ota_reboot_requested"] = True
        session["ota_disconnect_seen"] = True

        firmware._apply_native_ota_update_locked(
            session,
            [],
            {
                "connected": True,
                "connected_ts": 200.0,
                "firmware_version": "tater-thirdreality-0.2.2",
            },
        )

        self.assertFalse(session["active"])
        self.assertEqual(session["returncode"], 1)
        self.assertEqual(session["phase"], "failed")
        self.assertIn("0.2.2", str(session["error"]))

    def test_latest_error_after_rebooting_status_wins(self) -> None:
        session = self._session()
        entries = [
            self._rebooting_entry(),
            {
                "level": "error",
                "message": "swupdate rejected the recovery command",
                "payload": {"status": "error", "progress": 0},
            },
        ]

        firmware._apply_native_ota_update_locked(session, entries, {"connected": True})

        self.assertFalse(session["active"])
        self.assertEqual(session["phase"], "failed")
        self.assertEqual(session["error"], "swupdate rejected the recovery command")

    def test_verification_timeout_is_a_failure(self) -> None:
        session = self._session()
        session["ota_reboot_requested"] = True
        session["ota_verify_deadline_ts"] = 10.0

        with mock.patch.object(firmware.time, "time", return_value=11.0):
            firmware._apply_native_ota_update_locked(
                session,
                [],
                None,
                status_error="native status unavailable",
            )

        self.assertFalse(session["active"])
        self.assertEqual(session["phase"], "failed")
        self.assertIn("Timed out", str(session["error"]))
        self.assertIn("native status unavailable", str(session["error"]))


if __name__ == "__main__":
    unittest.main()
