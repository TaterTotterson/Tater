#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
class FirmwareProgressUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.component = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "voice" / "VoiceFirmware.vue").read_text(encoding="utf-8")
        cls.style_source = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

    def test_updater_flows_mount_progress_ui_instead_of_a_terminal(self) -> None:
        for action in (
            "voice_firmware_flash_start",
            "voice_firmware_amlogic_flash_start",
            "voice_firmware_esp_usb_flash_start",
            "voice_firmware_browser_build",
        ):
            self.assertIn(action, self.component)
        self.assertIn('<progress :value="progress" max="100" />', self.component)
        self.assertIn("sessionEntries", self.component)
        self.assertNotIn("createFirmwareProgressView", self.component)

    def test_local_esp_usb_flow_selects_a_port_without_showing_a_console(self) -> None:
        self.assertIn('"voice_firmware_esp_usb_ports"', self.component)
        self.assertIn('"voice_firmware_esp_usb_flash_start"', self.component)
        self.assertIn('v-model="selectedPort"', self.component)
        self.assertIn("Start Local USB Flash", self.component)

    def test_local_usb_logs_support_esp_and_s420_without_writing(self) -> None:
        backend = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

        self.assertIn('"voice_firmware_local_usb_log_ports"', self.component)
        self.assertIn('"voice_firmware_local_usb_log_start"', self.component)
        self.assertIn("Open USB Logs", self.component)
        self.assertIn('"voice_firmware_local_usb_log_ports"', backend)
        self.assertIn('"voice_firmware_local_usb_log_start"', backend)

    def test_browser_usb_prepares_an_image_for_the_secure_web_flasher(self) -> None:
        self.assertIn('"voice_firmware_browser_build"', self.component)
        self.assertIn("Prepare Browser USB Image", self.component)
        self.assertIn("https://taterassistant.com/usb-flasher/", self.component)
        self.assertIn("Download firmware image", self.component)

    def test_progress_popup_has_success_error_and_mobile_styles(self) -> None:
        self.assertIn(".tvoice-firmware-session", self.style_source)
        self.assertIn(".tvoice-firmware-session progress", self.style_source)
        self.assertIn(".tvoice-firmware-session pre", self.style_source)
        self.assertIn('<details class="tvoice-firmware-session">', self.component)
        self.assertIn("Technical details", self.component)
        self.assertIn("@media (max-width:", self.style_source)

    def test_update_all_uses_aggregate_progress_and_per_device_states(self) -> None:
        self.assertIn("async function updateAll", self.component)
        self.assertIn("for (let index = 0; index < rows.length; index += 1)", self.component)
        self.assertIn("Starting ${index + 1} of ${rows.length}", self.component)
        self.assertIn("Updated ${rows.length} satellite", self.component)
        self.assertIn("Update All ({{ updates.length }})", self.component)

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

    def test_embedded_sat1_reconnect_isolated_from_other_update_paths(self) -> None:
        backend = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

        self.assertIn("session.value?.self_ota_recovery", self.component)
        self.assertIn("Tater Embedded is restarting. Reconnecting to verify the update", self.component)
        self.assertIn("pollFailureCount.value <= 120", self.component)
        self.assertIn("schedulePoll();", self.component)
        self.assertIn('== "satellite1_rpi_standalone"', backend)
        self.assertIn('== "native_tater_ota"', backend)
        self.assertIn("and _is_loopback_host(host)", backend)
        self.assertIn("TATER_SAT1_SELF_OTA_STATE_DIR", backend)
        self.assertIn("def _recover_sat1_rpi_self_ota_session", backend)
        self.assertIn("last-success.json", backend)
        self.assertIn("last-failure.json", backend)


if __name__ == "__main__":
    unittest.main()
