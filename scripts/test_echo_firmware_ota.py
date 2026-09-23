#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import firmware, native_satellite, ui_helpers  # noqa: E402


def _echo_manifest() -> dict[str, object]:
    return {
        "schema": 1,
        "product": "Tater Echo Firmware",
        "version": "v0.4.0",
        "targets": {
            "biscuit": {
                "display_name": "Amazon Echo Dot 2nd Generation (2016)",
                "amazon_codename": "biscuit",
                "cpu": "armv7a",
                "unlock": "amonet-biscuit-v2.0.0",
                "factory_install": True,
                "ota": True,
                "status": "hardware-tested",
                "artifacts": {
                    "factory": {
                        "name": "tater-echo-biscuit-v0.4.0-factory.tar.gz",
                        "sha256": "a" * 64,
                        "size": 4096,
                    },
                    "ota": {
                        "name": "tater-echo-biscuit-v0.4.0-ota.bin",
                        "sha256": "b" * 64,
                        "size": 2048,
                    },
                },
            }
        },
    }


class EchoFirmwareOtaTests(unittest.TestCase):
    def test_echo_release_manifest_is_adapted_to_verified_native_ota(self) -> None:
        source = firmware._firmware_manifest_source("biscuit")
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            manifest = firmware._load_native_firmware_manifest(template_key="biscuit")

        device = manifest["devices_by_key"]["biscuit"]
        ota = device["artifacts"]["ota"]
        factory = device["artifacts"]["factory"]
        self.assertEqual("v0.4.0", device["firmware_version"])
        self.assertEqual("tater_native_ota", ota["flash_transport"])
        self.assertEqual(2048, ota["size_bytes"])
        self.assertEqual("b" * 64, ota["sha256"])
        self.assertEqual(
            f"{source['release_base_url']}/tater-echo-biscuit-v0.4.0-ota.bin",
            ota["path"],
        )
        self.assertFalse(factory["browser_flash_supported"])

    def test_echo_manifest_rejects_unverified_artifact_metadata(self) -> None:
        payload = _echo_manifest()
        payload["targets"]["biscuit"]["artifacts"]["ota"]["sha256"] = "unverified"
        with (
            mock.patch.object(firmware, "_remote_json", return_value=payload),
            self.assertRaisesRegex(RuntimeError, "artifact metadata is invalid"),
        ):
            firmware._load_native_firmware_manifest(template_key="biscuit")

    def test_biscuit_identity_selects_echo_firmware_and_art(self) -> None:
        row = {
            "connected": True,
            "firmware_target": "biscuit",
            "board": "biscuit",
            "firmware_version": "v0.3.0",
        }
        spec = firmware._match_template_spec("native:echo-test", row)
        self.assertEqual("biscuit", spec["key"])
        self.assertFalse(spec["usb_recovery"])
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-dot-2.png"),
            ui_helpers.device_image_src("biscuit", "Tater Echo Dot 2"),
        )

    def test_echo_hello_keeps_its_release_target(self) -> None:
        payload = {
            "device_id": "echo-test",
            "device_name": "Family Room Echo",
            "board": "biscuit",
            "firmware_target": "biscuit",
            "firmware_version": "v0.4.0",
        }
        credential = native_satellite._credential_row("native:echo-test", payload, "hash")
        metadata = native_satellite._registry_metadata_from_hello(payload, connected=True)
        self.assertEqual("biscuit", credential["firmware_target"])
        self.assertEqual("biscuit", metadata["firmware_target"])


if __name__ == "__main__":
    unittest.main()
