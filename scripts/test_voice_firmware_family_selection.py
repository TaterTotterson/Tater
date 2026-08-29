#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import subprocess
import textwrap
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_JS = REPO_ROOT / "tateros_static" / "app.js"


class VoiceFirmwareFamilySelectionTests(unittest.TestCase):
    def test_satellite_selection_updates_its_firmware_family(self) -> None:
        script = textwrap.dedent(
            r"""
            const assert = require("assert");
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync(process.argv[1], "utf8");

            function extractFunction(name) {
              const start = source.indexOf(`function ${name}(`);
              assert.notStrictEqual(start, -1, `Missing ${name}`);
              const bodyStart = source.indexOf("{", start);
              let depth = 0;
              for (let index = bodyStart; index < source.length; index += 1) {
                if (source[index] === "{") depth += 1;
                if (source[index] === "}") depth -= 1;
                if (depth === 0) return source.slice(start, index + 1);
              }
              throw new Error(`Unterminated ${name}`);
            }

            const context = {
              state: { esphomeFirmwareSelection: { templateKey: "", selector: "" } },
              boolFromAny(value, fallback = false) {
                if (value === undefined || value === null || value === "") return fallback;
                if (typeof value === "string") return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
                return Boolean(value);
              },
            };
            vm.createContext(context);
            vm.runInContext([
              extractFunction("espHomeFirmwareDevicesForTemplate"),
              extractFunction("espHomeFirmwareAllDevices"),
              extractFunction("espHomeFirmwareTemplateForSelector"),
              extractFunction("normalizeEspHomeFirmwareSelection"),
            ].join("\n"), context);

            const payload = {
              active_template_key: "voicepe",
              active_selector: "native:office",
              templates: [
                { value: "voicepe", label: "Voice PE" },
                { value: "satellite1", label: "Satellite1" },
                { value: "s3box_display", label: "S3 Box" },
              ],
              devices: [
                { value: "native:office", label: "Office", template_key: "voicepe", unmatched_template: false },
                { value: "native:kitchen", label: "Kitchen", template_key: "satellite1", unmatched_template: false },
                { value: "native:unknown", label: "Unknown", template_key: "voicepe", unmatched_template: true },
                { value: "__usb_recovery__", label: "Browser USB Recovery" },
              ],
              devices_by_template: {
                voicepe: [
                  { value: "native:office", template_key: "voicepe", unmatched_template: false },
                  { value: "native:unknown", template_key: "voicepe", unmatched_template: true },
                  { value: "__usb_recovery__", template_key: "voicepe" },
                ],
                satellite1: [
                  { value: "native:kitchen", template_key: "satellite1", unmatched_template: false },
                  { value: "native:unknown", template_key: "satellite1", unmatched_template: true },
                  { value: "__usb_recovery__", template_key: "satellite1" },
                ],
                s3box_display: [
                  { value: "native:unknown", template_key: "s3box_display", unmatched_template: true },
                  { value: "__usb_recovery__", template_key: "s3box_display" },
                ],
              },
              variants: {
                voicepe: {
                  "native:office": { template_key: "voicepe", unmatched_template: false },
                  "native:unknown": { template_key: "voicepe", unmatched_template: true },
                },
                satellite1: {
                  "native:kitchen": { template_key: "satellite1", unmatched_template: false },
                  "native:unknown": { template_key: "satellite1", unmatched_template: true },
                },
                s3box_display: {
                  "native:unknown": { template_key: "s3box_display", unmatched_template: true },
                },
              },
            };

            context.state.esphomeFirmwareSelection = { templateKey: "voicepe", selector: "native:kitchen" };
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareSelection(payload))),
              { templateKey: "satellite1", selector: "native:kitchen" }
            );

            context.state.esphomeFirmwareSelection = { templateKey: "s3box_display", selector: "__usb_recovery__" };
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareSelection(payload))),
              { templateKey: "s3box_display", selector: "__usb_recovery__" }
            );

            context.state.esphomeFirmwareSelection = { templateKey: "satellite1", selector: "native:unknown" };
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareSelection(payload))),
              { templateKey: "satellite1", selector: "native:unknown" }
            );
            """
        )

        result = subprocess.run(
            ["node", "-e", script, str(APP_JS)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_transport_picker_separates_connected_local_and_browser_targets(self) -> None:
        script = textwrap.dedent(
            r"""
            const assert = require("assert");
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync(process.argv[1], "utf8");

            function extractFunction(name) {
              const start = source.indexOf(`function ${name}(`);
              assert.notStrictEqual(start, -1, `Missing ${name}`);
              const bodyStart = source.indexOf("{", start);
              let depth = 0;
              for (let index = bodyStart; index < source.length; index += 1) {
                if (source[index] === "{") depth += 1;
                if (source[index] === "}") depth -= 1;
                if (depth === 0) return source.slice(start, index + 1);
              }
              throw new Error(`Unterminated ${name}`);
            }

            const context = {
              state: {
                esphomeFirmwareSelection: { templateKey: "", selector: "" },
                esphomeFirmwareTransport: "ota",
              },
              boolFromAny(value, fallback = false) {
                if (value === undefined || value === null || value === "") return fallback;
                if (typeof value === "string") return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
                return Boolean(value);
              },
            };
            vm.createContext(context);
            vm.runInContext([
              extractFunction("espHomeFirmwareDevicesForTemplate"),
              extractFunction("espHomeFirmwareAllDevices"),
              extractFunction("espHomeFirmwareTemplateForSelector"),
              extractFunction("normalizeEspHomeFirmwareSelection"),
              extractFunction("resolveEspHomeFirmwareVariant"),
              extractFunction("espHomeFirmwareFactoryTransport"),
              extractFunction("espHomeFirmwareTemplatesForTransport"),
              extractFunction("espHomeFirmwareConnectedDevices"),
              extractFunction("normalizeEspHomeFirmwareTransportSelection"),
            ].join("\n"), context);

            const recoveryVariant = (transport, path) => ({
              prebuilt_firmware: {
                artifacts: { factory: { path, flash_transport: transport } },
              },
            });
            const payload = {
              active_template_key: "voicepe",
              active_selector: "native:office",
              templates: [
                { value: "thirdreality_s420", label: "ThirdReality S420" },
                { value: "voicepe", label: "Voice PE" },
                { value: "satellite1", label: "Satellite1" },
              ],
              devices: [
                { value: "native:office", template_key: "voicepe", connected: true, unmatched_template: false },
                { value: "native:kitchen", template_key: "satellite1", connected: false, unmatched_template: false },
              ],
              devices_by_template: {
                thirdreality_s420: [{ value: "__usb_recovery__", template_key: "thirdreality_s420" }],
                voicepe: [
                  { value: "native:office", template_key: "voicepe", connected: true, unmatched_template: false },
                  { value: "__usb_recovery__", template_key: "voicepe" },
                ],
                satellite1: [
                  { value: "native:kitchen", template_key: "satellite1", connected: false, unmatched_template: false },
                  { value: "__usb_recovery__", template_key: "satellite1" },
                ],
              },
              variants: {
                thirdreality_s420: {
                  "__usb_recovery__": recoveryVariant("amlogic_usb_burn", "s420.img"),
                },
                voicepe: {
                  "native:office": { template_key: "voicepe", connected: true, unmatched_template: false },
                  "__usb_recovery__": recoveryVariant("esp_serial", "voicepe.bin"),
                },
                satellite1: {
                  "native:kitchen": { template_key: "satellite1", connected: false, unmatched_template: false },
                  "__usb_recovery__": recoveryVariant("esp_serial", "satellite1.bin"),
                },
              },
            };

            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.espHomeFirmwareConnectedDevices(payload).map((row) => row.value))),
              ["native:office"]
            );
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.espHomeFirmwareTemplatesForTransport(payload, "local_usb").map((row) => row.value))),
              ["thirdreality_s420", "voicepe", "satellite1"]
            );
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.espHomeFirmwareTemplatesForTransport(payload, "browser_usb").map((row) => row.value))),
              ["voicepe", "satellite1"]
            );

            context.state.esphomeFirmwareTransport = "local_usb";
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareTransportSelection(payload))),
              { transport: "local_usb", templateKey: "voicepe", selector: "__usb_recovery__" }
            );
            context.state.esphomeFirmwareTransport = "browser_usb";
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareTransportSelection(payload))),
              { transport: "browser_usb", templateKey: "voicepe", selector: "__usb_recovery__" }
            );
            context.state.esphomeFirmwareTransport = "ota";
            assert.deepStrictEqual(
              JSON.parse(JSON.stringify(context.normalizeEspHomeFirmwareTransportSelection(payload))),
              { transport: "ota", templateKey: "voicepe", selector: "native:office" }
            );
            """
        )

        result = subprocess.run(
            ["node", "-e", script, str(APP_JS)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_all_satellites_are_available_in_the_target_selector(self) -> None:
        source = APP_JS.read_text(encoding="utf-8")
        start = source.index("function renderEspHomeFirmwareCard")
        end = source.index("\nfunction renderEspHomeFirmwarePanel", start)
        render_source = source[start:end]

        self.assertIn("const connectedDevices = espHomeFirmwareConnectedDevices(body);", render_source)
        self.assertIn("options: connectedDevices", render_source)
        self.assertIn("options: transportTemplates", render_source)

    def test_usb_flashing_offers_factory_and_keep_settings_images(self) -> None:
        source = APP_JS.read_text(encoding="utf-8")
        start = source.index("function renderEspHomeFirmwareCard")
        end = source.index("\nfunction renderEspHomeFirmwarePanel", start)
        render_source = source[start:end]
        flow_start = source.index("function openEspHomeBrowserUsbFlashFlow")
        flow_end = source.index("\nasync function prepareAmlogicUsbImage", flow_start)
        flow_source = source[flow_start:flow_end]

        self.assertIn('data-firmware-usb-image="factory"', render_source)
        self.assertIn('data-firmware-usb-image="ota"', render_source)
        self.assertIn("OTA Update · Keep Settings", render_source)
        self.assertIn("Erases Wi-Fi, pairing, and saved settings", render_source)
        self.assertIn("amlogicFactoryImage && prebuiltOtaAvailable", render_source)
        self.assertIn("https://taterassistant.com/usb-flasher/", source)
        self.assertIn("Secure Tater USB Flasher", render_source)
        self.assertIn('target="_blank"', render_source)

        self.assertIn("flash_kind: flashKind", source)
        self.assertIn("artifact?.flash_addresses", source)
        self.assertIn("Existing Wi-Fi, pairing, and settings were kept", source)
        self.assertIn('const flashKind = String(card.dataset?.firmwareUsbImage || "factory")', flow_source)
        self.assertIn('const preservesSettings = flashKind === "ota";', flow_source)
        self.assertIn("let logConsole = null;", flow_source)
        self.assertNotIn('browserCapability.available ? "" : " disabled"', render_source)

        firmware_source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        self.assertIn('"flash_addresses": flash_addresses', firmware_source)
        self.assertIn('"preserves_settings": kind == "ota"', firmware_source)
        self.assertIn('_download_prebuilt_firmware_binary(context, kind', firmware_source)

    def test_browser_usb_capability_relies_on_web_serial_instead_of_https(self) -> None:
        script = textwrap.dedent(
            r"""
            const assert = require("assert");
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync(process.argv[1], "utf8");

            function extractFunction(name) {
              const start = source.indexOf(`function ${name}(`);
              assert.notStrictEqual(start, -1, `Missing ${name}`);
              const bodyStart = source.indexOf("{", start);
              let depth = 0;
              for (let index = bodyStart; index < source.length; index += 1) {
                if (source[index] === "{") depth += 1;
                if (source[index] === "}") depth -= 1;
                if (depth === 0) return source.slice(start, index + 1);
              }
              throw new Error(`Unterminated ${name}`);
            }

            function capability(windowValue, navigatorValue) {
              const context = { window: windowValue, navigator: navigatorValue };
              vm.createContext(context);
              vm.runInContext(extractFunction("browserUsbCapability"), context);
              return context.browserUsbCapability();
            }

            const httpWithSerial = capability({ isSecureContext: false }, { serial: { requestPort() {} } });
            assert.strictEqual(httpWithSerial.available, true);

            const missingSerial = capability({ isSecureContext: true }, {});
            assert.strictEqual(missingSerial.available, false);
            assert.match(missingSerial.message, /does not expose Web Serial/i);

            const ready = capability({ isSecureContext: true }, { serial: { requestPort() {} } });
            assert.strictEqual(ready.available, true);
            """
        )

        result = subprocess.run(
            ["node", "-e", script, str(APP_JS)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_payload_identifies_each_matched_devices_family(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        start = source.index("append_device_option(\n                    {\n                        **device_option")
        end = source.index("variants.setdefault", start)
        append_source = source[start:end]

        self.assertIn('"template_key": template_key', append_source)
        self.assertIn('"unmatched_template": not bool(matched_template_key)', append_source)

    def test_s420_uses_its_own_release_manifest_and_never_esptool(self) -> None:
        firmware_source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        app_source = APP_JS.read_text(encoding="utf-8")

        self.assertIn('"thirdreality_s420",', firmware_source)
        self.assertIn("TATER_S420_FIRMWARE_LATEST_URL", firmware_source)
        self.assertIn('flash_transport != "esp_serial"', firmware_source)
        self.assertIn('factoryFlashTransport === "amlogic_usb_burn"', app_source)
        self.assertIn("prepareAmlogicUsbImage(card, coreKey)", app_source)
        self.assertIn('"voice_firmware_amlogic_flash_start"', firmware_source)
        self.assertIn('"Start Local USB Flash"', app_source)
        self.assertIn("Browser ESP flashing cannot write", app_source)
        self.assertIn("This browser session does not expose Web Serial", app_source)
        self.assertIn("typeof serial.requestPort", app_source)
        self.assertNotIn('message: "Browser USB needs a secure Tater page', app_source)
        self.assertNotIn("!window.isSecureContext", app_source)
        self.assertNotIn('label: "Local USB Flash Log"', app_source)
        self.assertIn("createFirmwareProgressView", app_source)
        self.assertIn("Browser USB Logs", app_source)
        self.assertIn('"voice_firmware_esp_usb_flash_start"', firmware_source)
        self.assertIn('"voice_firmware_esp_usb_ports"', firmware_source)
        self.assertIn("openEspHomeLocalEspUsbFlashFlow", app_source)

    def test_sat1_rpi_flavors_use_their_own_release_feed(self) -> None:
        firmware_source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

        self.assertIn('"satellite1_rpi_standalone",', firmware_source)
        self.assertIn('"satellite1_rpi_satellite",', firmware_source)
        self.assertIn("TATER_SAT1_RPI_FIRMWARE_LATEST_URL", firmware_source)
        self.assertIn('"Tater-SAT1-RPi"', firmware_source)
        self.assertNotIn('"Tater-SAT1-Standalone"', firmware_source)
        self.assertIn(
            'for source_key in ("", "thirdreality_s420", "satellite1_rpi_standalone"):',
            firmware_source,
        )

    def test_sat1_rpi_hardware_identities_do_not_alias_the_esp_sat1(self) -> None:
        firmware_source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")

        self.assertIn('return "satellite1_rpi_standalone"', firmware_source)
        self.assertIn('return "satellite1_rpi_satellite"', firmware_source)
        self.assertIn("_SAT1_RPI_NATIVE_OTA_VERIFY_TIMEOUT_SECONDS = 60 * 60.0", firmware_source)

    def test_native_ota_sends_manifest_integrity_fields(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        self.assertIn('"sha256": _file_sha256(target_binary_path)', source)
        self.assertIn('{"url": ota_url, "sha256": ota_sha256, "size_bytes": ota_size}', source)

    def test_local_release_assets_resolve_beside_latest_json(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        self.assertIn("Path(latest_url).parent", source)
        self.assertIn('root / "release_assets" / clean', source)


if __name__ == "__main__":
    unittest.main()
