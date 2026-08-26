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
            ("prepareAmlogicUsbImage", "openEspHomeLocalEspUsbFlashFlow"),
            ("openEspHomeLocalEspUsbFlashFlow", "openEspHomeLocalUsbLogs"),
            ("openEspHomeFirmwareUpdateAllFlow", "bindEspHomeFirmwareActions"),
        )
        for name, next_name in flows:
            with self.subTest(flow=name):
                source = _function_source(self.app_source, name, next_name)
                self.assertIn("createFirmwareProgressView", source)
                self.assertIn("firmware-progress-modal", source)
                self.assertNotIn('consoleEl.className = "voice-log-console"', source)

    def test_local_esp_usb_flow_selects_a_port_without_showing_a_console(self) -> None:
        source = _function_source(self.app_source, "openEspHomeLocalEspUsbFlashFlow", "openEspHomeLocalUsbLogs")
        self.assertIn('"voice_firmware_esp_usb_ports"', source)
        self.assertIn('"voice_firmware_esp_usb_flash_start"', source)
        self.assertIn("createFirmwareProgressView", source)
        self.assertNotIn('consoleEl.className = "voice-log-console"', source)
        self.assertNotIn("Local USB Flash Log", source)
        self.assertIn("Connected USB Serial Device", source)

    def test_local_usb_logs_support_esp_and_s420_without_writing(self) -> None:
        source = _function_source(self.app_source, "openEspHomeLocalUsbLogs", "openEspHomeFirmwareOtaLogs")
        backend = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

        self.assertIn('data-firmware-action="voice_firmware_local_usb_logs"', self.app_source)
        self.assertIn('"voice_firmware_local_usb_log_ports"', source)
        self.assertIn('"voice_firmware_local_usb_log_start"', source)
        self.assertIn("S420 Debug Console", source)
        self.assertIn('consoleEl.className = "voice-log-console"', source)
        self.assertIn('"voice_firmware_local_usb_log_ports"', backend)
        self.assertIn('"voice_firmware_local_usb_log_start"', backend)

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

    def test_update_all_uses_aggregate_progress_and_per_device_states(self) -> None:
        batch_view = _function_source(
            self.app_source,
            "createFirmwareBatchProgressView",
            "openEspHomeFirmwareFlashViewer",
        )
        update_flow = _function_source(
            self.app_source,
            "openEspHomeFirmwareUpdateAllFlow",
            "bindEspHomeFirmwareActions",
        )
        single_ota_flow = _function_source(
            self.app_source,
            "openEspHomeFirmwareFlashViewer",
            "esphomeFirmwareLogTone",
        )
        browser_usb_flow = _function_source(
            self.app_source,
            "openEspHomeBrowserUsbFlashFlow",
            "prepareAmlogicUsbImage",
        )

        self.assertIn("Overall firmware update progress", batch_view)
        self.assertIn("firmware-batch-device-list", batch_view)
        self.assertIn('Boolean(payload?.batchComplete)', batch_view)
        self.assertNotIn('phase === "completed"', batch_view)
        self.assertIn("createFirmwareBatchProgressView", update_flow)
        self.assertIn("activeIndex: index - 1", update_flow)
        self.assertIn('deviceState: rowFailed ? "error" : "success"', update_flow)
        self.assertIn("batchComplete: !singleUpdate", update_flow)
        self.assertIn("firmware-batch-progress-modal", update_flow)
        self.assertNotIn("firmware-batch-progress-modal", single_ota_flow)
        self.assertNotIn("firmware-batch-progress-modal", browser_usb_flow)

        self.assertIn(".firmware-batch-progress-view", self.style_source)
        self.assertIn(".firmware-batch-device.is-active", self.style_source)
        self.assertIn(".firmware-batch-device.is-success", self.style_source)
        self.assertIn(".firmware-batch-device.is-error", self.style_source)
        self.assertIn(".runtime-settings-dialog.runtime-settings-dialog-firmware-batch", self.style_source)

    def test_backend_exposes_real_ota_progress(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        self.assertIn("def _native_ota_progress", source)
        self.assertIn("def _apply_native_ota_update_locked", source)
        self.assertIn("_set_session_progress_locked(session, min(progress, 99.0))", source)
        self.assertIn('if status in {"rebooting", "complete", "completed"}:', source)
        self.assertIn("ota_verify_deadline_ts", source)
        self.assertIn("Native OTA verified after reboot", source)
        self.assertIn("but Tater expected", source)
        self.assertIn('_set_session_progress_locked(live, percent, completed_bytes=sent, total_bytes=total)', source)
        self.assertIn('"progress_percent": round(', source)
        self.assertIn('"progress_bytes": int(session.get("progress_bytes") or 0)', source)
        self.assertIn('"progress_total_bytes": int(session.get("progress_total_bytes") or 0)', source)


if __name__ == "__main__":
    unittest.main()
