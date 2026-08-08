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

    def test_all_satellites_are_available_in_the_target_selector(self) -> None:
        source = APP_JS.read_text(encoding="utf-8")
        start = source.index("function renderEspHomeFirmwareCard")
        end = source.index("\nfunction renderEspHomeFirmwarePanel", start)
        render_source = source[start:end]

        self.assertIn("const devices = espHomeFirmwareAllDevices(body);", render_source)
        self.assertIn("options: devices", render_source)

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
        self.assertIn("Browser ESP flashing cannot write", app_source)

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
