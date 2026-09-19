#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
FIRMWARE_COMPONENT = REPO_ROOT / "frontend" / "src" / "settings" / "components" / "voice" / "VoiceFirmware.vue"


class VoiceFirmwareFamilySelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.component = FIRMWARE_COMPONENT.read_text(encoding="utf-8")
        cls.backend = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

    def test_satellite_selection_updates_its_firmware_family(self) -> None:
        self.assertIn("function templateForDevice", self.component)
        self.assertIn("selectedTemplate.value = templateForDevice(selectedSelector.value)", self.component)
        self.assertIn("function chooseTarget", self.component)
        self.assertIn("selectedTemplate.value = templateForDevice(value)", self.component)

    def test_transport_picker_separates_connected_and_usb_targets(self) -> None:
        self.assertIn('const connectedDevices = computed(() => devices.value.filter', self.component)
        self.assertIn('String(device.value || "") !== "__usb_recovery__"', self.component)
        self.assertIn("const usbTemplates = computed", self.component)
        self.assertIn('const targetOptions = computed(() => transport.value === "ota" ? connectedDevices.value : usbTemplates.value)', self.component)
        for transport in ("ota", "local_usb", "browser_usb"):
            self.assertIn(f"chooseTransport('{transport}')", self.component)

    def test_all_connected_satellites_are_available_in_the_target_selector(self) -> None:
        self.assertIn('v-for="option in targetOptions"', self.component)
        self.assertIn("connectedDevices.value.map", self.component)
        self.assertIn("templates.value.filter", self.component)

    def test_usb_flashing_offers_factory_and_keep_settings_images(self) -> None:
        self.assertIn('const flashKind = ref<"factory" | "ota">("factory")', self.component)
        self.assertIn("@click=\"flashKind = 'factory'\"", self.component)
        self.assertIn("@click=\"flashKind = 'ota'\"", self.component)
        self.assertIn("Factory · erase settings", self.component)
        self.assertIn("Keep settings", self.component)
        self.assertIn("flash_kind: flashKind.value", self.component)
        self.assertIn('"voice_firmware_browser_build"', self.component)
        self.assertIn("https://taterassistant.com/usb-flasher/", self.component)

        self.assertIn('"flash_addresses": flash_addresses', self.backend)
        self.assertIn('"preserves_settings": kind == "ota"', self.backend)
        self.assertIn('_download_prebuilt_firmware_binary(context, kind', self.backend)

    def test_browser_usb_uses_the_secure_external_flasher(self) -> None:
        self.assertIn("Prepare Browser USB Image", self.component)
        self.assertIn("Open Secure Web Flasher", self.component)
        self.assertIn('target="_blank" rel="noreferrer"', self.component)
        self.assertNotIn("navigator.serial", self.component)
        self.assertNotIn("requestPort", self.component)

    def test_payload_identifies_each_matched_devices_family(self) -> None:
        start = self.backend.index("append_device_option(\n                    {\n                        **device_option")
        end = self.backend.index("variants.setdefault", start)
        append_source = self.backend[start:end]

        self.assertIn('"template_key": template_key', append_source)
        self.assertIn('"unmatched_template": not bool(matched_template_key)', append_source)

    def test_s420_uses_its_own_release_manifest_and_never_esptool(self) -> None:
        self.assertIn('"thirdreality_s420",', self.backend)
        self.assertIn("TATER_S420_FIRMWARE_LATEST_URL", self.backend)
        self.assertIn('flash_transport != "esp_serial"', self.backend)
        self.assertIn('flashTransport.value === "amlogic_usb_burn"', self.component)
        self.assertIn('"voice_firmware_amlogic_flash_start"', self.component)
        self.assertIn("Start Local USB Flash", self.component)
        self.assertIn("Open USB Logs", self.component)
        self.assertIn('"voice_firmware_esp_usb_flash_start"', self.backend)
        self.assertIn('"voice_firmware_esp_usb_ports"', self.backend)

    def test_sat1_rpi_flavors_use_their_own_release_feed(self) -> None:
        self.assertIn('"satellite1_rpi_standalone",', self.backend)
        self.assertIn('"satellite1_rpi_satellite",', self.backend)
        self.assertIn("TATER_SAT1_RPI_FIRMWARE_LATEST_URL", self.backend)
        self.assertIn('"Tater-SAT1-RPi"', self.backend)
        self.assertNotIn('"Tater-SAT1-Standalone"', self.backend)
        self.assertIn(
            'for source_key in ("", "thirdreality_s420", "satellite1_rpi_standalone"):',
            self.backend,
        )

    def test_sat1_rpi_hardware_identities_do_not_alias_the_esp_sat1(self) -> None:
        self.assertIn('return "satellite1_rpi_standalone"', self.backend)
        self.assertIn('return "satellite1_rpi_satellite"', self.backend)
        self.assertIn("_SAT1_RPI_NATIVE_OTA_VERIFY_TIMEOUT_SECONDS = 60 * 60.0", self.backend)

    def test_native_ota_sends_manifest_integrity_fields(self) -> None:
        self.assertIn('"sha256": _file_sha256(target_binary_path)', self.backend)
        self.assertIn('{"url": ota_url, "sha256": ota_sha256, "size_bytes": ota_size}', self.backend)

    def test_local_release_assets_resolve_beside_latest_json(self) -> None:
        self.assertIn("Path(latest_url).parent", self.backend)
        self.assertIn('root / "release_assets" / clean', self.backend)


if __name__ == "__main__":
    unittest.main()
