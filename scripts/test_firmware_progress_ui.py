#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _function_source(source: str, name: str, next_name: str) -> str:
    start = source.index(f"function {name}") if f"function {name}" in source else source.index(f"async function {name}")
    end_candidates = [
        source.find(f"\nfunction {next_name}", start + 1),
        source.find(f"\nasync function {next_name}", start + 1),
    ]
    end = min(candidate for candidate in end_candidates if candidate >= 0)
    return source[start:end]


class FirmwareProgressUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app_source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        cls.style_source = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

    def test_updater_flows_mount_progress_ui_instead_of_a_terminal(self) -> None:
        flows = (
            ("openEspHomeFirmwareFlashViewer", "esphomeFirmwareLogTone"),
            ("openEspHomeBrowserUsbFlashFlow", "prepareAmlogicUsbImage"),
            ("prepareAmlogicUsbImage", "openEspHomeFirmwareOtaLogs"),
            ("openEspHomeFirmwareUpdateAllFlow", "bindEspHomeFirmwareActions"),
        )
        for name, next_name in flows:
            with self.subTest(flow=name):
                source = _function_source(self.app_source, name, next_name)
                self.assertIn("createFirmwareProgressView", source)
                self.assertIn("firmware-progress-modal", source)
                self.assertNotIn('consoleEl.className = "voice-log-console"', source)

    def test_browser_usb_uses_esptool_write_progress(self) -> None:
        source = _function_source(self.app_source, "flashBrowserUsbPort", "sleep")
        self.assertIn("reportProgress:", source)
        self.assertIn("20 + pct * 0.72", source)
        self.assertIn('phase: "restarting"', source)

    def test_progress_popup_has_success_error_and_mobile_styles(self) -> None:
        self.assertIn(".firmware-progress-view.is-success", self.style_source)
        self.assertIn(".firmware-progress-view.is-error", self.style_source)
        self.assertIn(".runtime-settings-dialog.runtime-settings-dialog-firmware-progress", self.style_source)
        self.assertIn("@media (max-width: 560px)", self.style_source)

    def test_backend_exposes_real_ota_progress(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        self.assertIn("def _native_ota_progress", source)
        self.assertIn('_set_session_progress_locked(session, native_progress)', source)
        self.assertIn('_set_session_progress_locked(live, percent, completed_bytes=sent, total_bytes=total)', source)
        self.assertIn('"progress_percent": round(', source)
        self.assertIn('"progress_bytes": int(session.get("progress_bytes") or 0)', source)
        self.assertIn('"progress_total_bytes": int(session.get("progress_total_bytes") or 0)', source)


if __name__ == "__main__":
    unittest.main()
