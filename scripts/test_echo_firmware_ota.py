#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import tempfile
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
            },
            "checkers": {
                "display_name": "Amazon Echo Show 5 1st Generation (2019)",
                "amazon_codename": "checkers",
                "cpu": "armv7a",
                "unlock": "amonet-checkers-v2.0.1+",
                "factory_install": True,
                "ota": True,
                "status": "native-hardware-test",
                "artifacts": {
                    "factory": {
                        "name": "tater-echo-checkers-v0.4.0-factory.tar.gz",
                        "sha256": "c" * 64,
                        "size": 8192,
                    },
                    "ota": {
                        "name": "tater-echo-checkers-v0.4.0-ota.zip",
                        "sha256": "d" * 64,
                        "size": 6144,
                    },
                    "ota_bridge": {
                        "name": "tater-echo-checkers-v0.4.0-ota-bridge.zip",
                        "sha256": "0" * 64,
                        "size": 5120,
                    },
                },
                "ota_migrations": [{
                    "id": "linux-app-components-v1",
                    "required_capability": "ota_bundle_components_version",
                    "minimum_capability_version": 1,
                    "bridge_artifact": "ota_bridge",
                    "final_artifact": "ota",
                }],
            },
            "crown": {
                "display_name": "Amazon Echo Show 8 1st Generation (2019)",
                "amazon_codename": "crown",
                "cpu": "armv7a",
                "unlock": "amonet-crown-v2.0.1+",
                "factory_install": True,
                "ota": True,
                "status": "experimental",
                "artifacts": {
                    "factory": {
                        "name": "tater-echo-crown-v0.4.0-factory.tar.gz",
                        "sha256": "3" * 64,
                        "size": 9216,
                    },
                    "ota": {
                        "name": "tater-echo-crown-v0.4.0-ota.tar.gz",
                        "sha256": "4" * 64,
                        "size": 7168,
                    },
                },
            },
            "radar": {
                "display_name": "Amazon Echo 2nd Generation (2017)",
                "amazon_codename": "radar",
                "cpu": "armv7a",
                "unlock": "amonet-radar-v1.0.0",
                "factory_install": True,
                "ota": True,
                "status": "hardware-tested",
                "artifacts": {
                    "factory": {
                        "name": "tater-echo-radar-v0.4.0-factory.tar.gz",
                        "sha256": "1" * 64,
                        "size": 7168,
                    },
                    "ota": {
                        "name": "tater-echo-radar-v0.4.0-ota.bin",
                        "sha256": "2" * 64,
                        "size": 3072,
                    },
                },
            },
            "rook": {
                "display_name": "Amazon Echo Spot 1st Generation (2017)",
                "amazon_codename": "rook",
                "cpu": "armv7a",
                "unlock": "amonet-rook-v2.0.0",
                "factory_install": True,
                "ota": True,
                "status": "hardware-tested",
                "artifacts": {
                    "factory": {
                        "name": "tater-echo-rook-v0.4.0-factory.tar.gz",
                        "sha256": "e" * 64,
                        "size": 8192,
                    },
                    "ota": {
                        "name": "tater-echo-rook-v0.4.0-ota.tar.gz",
                        "sha256": "f" * 64,
                        "size": 6144,
                    },
                },
            },
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

    def test_echo_manifest_adapts_reusable_staged_ota_migration(self) -> None:
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            manifest = firmware._load_native_firmware_manifest(template_key="checkers")

        device = manifest["devices_by_key"]["checkers"]
        self.assertEqual("tater_native_ota", device["artifacts"]["ota_bridge"]["flash_transport"])
        self.assertEqual(device["ota_migrations"], [{
            "id": "linux-app-components-v1",
            "required_capability": "ota_bundle_components_version",
            "minimum_capability_version": 1,
            "bridge_artifact": "ota_bridge",
            "final_artifact": "ota",
        }])

    def test_echo_manifest_rejects_migration_with_missing_bridge(self) -> None:
        payload = _echo_manifest()
        del payload["targets"]["checkers"]["artifacts"]["ota_bridge"]
        with (
            mock.patch.object(firmware, "_remote_json", return_value=payload),
            self.assertRaisesRegex(RuntimeError, "OTA migration is invalid"),
        ):
            firmware._load_native_firmware_manifest(template_key="checkers")

    def test_staged_migration_selects_bridge_then_resumable_final(self) -> None:
        prebuilt = {
            "version": "v0.4.0",
            "device": _echo_manifest()["targets"]["checkers"],
        }
        bridge = firmware._required_ota_migration(prebuilt, {"ota": True}, {})
        self.assertEqual("bridge", bridge["stage"])

        current = firmware._required_ota_migration(
            prebuilt,
            {"ota_bundle_components_version": 1},
            {
                "migration_id": "linux-app-components-v1",
                "version": "v0.4.0",
                "stage": "final",
            },
        )
        self.assertEqual("final", current["stage"])
        self.assertEqual(
            {},
            firmware._required_ota_migration(
                prebuilt, {"ota_bundle_components_version": 1}, {}
            ),
        )

    def test_staged_session_precaches_both_artifacts_and_starts_with_bridge(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            bridge_path = root / "bridge.tar.gz"
            final_path = root / "final.tar.gz"
            bridge_path.write_bytes(b"bridge")
            final_path.write_bytes(b"final")
            context = {
                "selector": "native:show-staged-test",
                "host": "10.4.20.26",
                "display_base_url": "http://10.4.20.10:8501",
                "display_name": "Kitchen Show",
                "template_key": "checkers",
                "template_label": "Echo Show 5",
                "firmware_version": "v0.4.0",
                "native_firmware": True,
                "ota_supported": True,
                "prebuilt_firmware": {
                    "native": True,
                    "artifacts": {
                        "ota_bridge": {"path": "bridge.tar.gz"},
                        "ota": {"path": "final.tar.gz"},
                    },
                },
                "ota_migration": {
                    "id": "linux-app-components-v1",
                    "stage": "bridge",
                    "bridge_artifact": "ota_bridge",
                    "final_artifact": "ota",
                    "required_capability": "ota_bundle_components_version",
                    "minimum_capability_version": 1,
                },
            }

            def download(_context, kind):
                path = bridge_path if kind == "ota_bridge" else final_path
                return {"path": path, "cached": False}

            def serve(_context, path):
                kind = "bridge" if path == bridge_path else "final"
                return {
                    "artifact_id": kind,
                    "ota_url": f"http://tater.test/{kind}/firmware.bin",
                    "binary_name": "firmware.bin",
                    "binary_size": path.stat().st_size,
                    "sha256": kind[0] * 64,
                    "source_binary": str(path),
                }

            worker = mock.Mock()
            with (
                mock.patch.object(firmware, "_download_prebuilt_firmware_binary", side_effect=download) as downloads,
                mock.patch.object(firmware, "_create_native_ota_artifact", side_effect=serve),
                mock.patch.object(firmware.threading, "Thread", return_value=worker),
                mock.patch.object(firmware, "_save_ota_migration") as save_migration,
            ):
                result = firmware._start_flash_session(context)
            try:
                self.assertEqual("bridge", result["ota_stage"])
                self.assertEqual(1, result["ota_stage_index"])
                self.assertEqual(2, result["ota_stage_count"])
                self.assertEqual(
                    [mock.call(context, "ota_bridge"), mock.call(context, "ota")],
                    downloads.call_args_list,
                )
                session = firmware._FIRMWARE_SESSIONS[result["session_id"]]
                self.assertEqual("http://tater.test/bridge/firmware.bin", session["ota_url"])
                self.assertEqual(
                    "http://tater.test/final/firmware.bin",
                    session["ota_final_artifact"]["ota_url"],
                )
                save_migration.assert_called_once()
                worker.start.assert_called_once_with()
            finally:
                firmware._FIRMWARE_SESSIONS.pop(result["session_id"], None)

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

    def test_checkers_identity_selects_coordinated_echo_ota(self) -> None:
        row = {
            "connected": True,
            "firmware_target": "checkers",
            "board": "checkers",
            "firmware_version": "v0.3.0",
            "capabilities": {"ota": True, "screen": True},
        }
        spec = firmware._match_template_spec("native:show-test", row)
        self.assertEqual("checkers", spec["key"])
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-show-5.png"),
            ui_helpers.device_image_src("checkers", "Tater Echo Show 5"),
        )
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            info = firmware._native_firmware_info("checkers")
        self.assertTrue(info["available"])
        self.assertEqual("tater_native_ota", info["artifacts"]["ota"]["flash_transport"])
        self.assertTrue(info["artifacts"]["ota"]["path"].endswith("tater-echo-checkers-v0.4.0-ota.zip"))

    def test_crown_identity_selects_its_own_verified_ota(self) -> None:
        row = {
            "connected": True,
            "firmware_target": "crown",
            "board": "crown",
            "firmware_version": "v0.3.0",
            "capabilities": {"ota": True, "screen": True},
        }
        spec = firmware._match_template_spec("native:crown-test", row)
        self.assertEqual("crown", spec["key"])
        self.assertFalse(spec["usb_recovery"])
        self.assertEqual("Tater Crown", native_satellite._default_name_for_board("crown"))
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-show-5.png"),
            ui_helpers.device_image_src("crown", "Tater Echo Show 8"),
        )
        source = firmware._firmware_manifest_source("crown")
        self.assertEqual("echo_targets", source["format"])
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            info = firmware._native_firmware_info("crown")
        self.assertTrue(info["available"])
        self.assertEqual("tater_native_ota", info["artifacts"]["ota"]["flash_transport"])
        self.assertEqual("4" * 64, info["artifacts"]["ota"]["sha256"])
        self.assertTrue(info["artifacts"]["ota"]["path"].endswith("tater-echo-crown-v0.4.0-ota.tar.gz"))
        self.assertIn("factory/crown-linux/README.md", info["artifacts"]["factory"]["instructions_url"])

    def test_rook_identity_selects_its_own_ota_and_art(self) -> None:
        row = {
            "connected": True,
            "firmware_target": "rook",
            "board": "rook",
            "firmware_version": "v0.3.0",
            "capabilities": {"ota": True, "screen": True},
        }
        spec = firmware._match_template_spec("native:spot-test", row)
        self.assertEqual("rook", spec["key"])
        self.assertFalse(spec["usb_recovery"])
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-spot-rook.png"),
            ui_helpers.device_image_src("rook", "Tater Echo Spot"),
        )
        source = firmware._firmware_manifest_source("rook")
        self.assertEqual("echo_targets", source["format"])
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            info = firmware._native_firmware_info("rook")
        self.assertTrue(info["available"])
        self.assertEqual("tater_native_ota", info["artifacts"]["ota"]["flash_transport"])
        self.assertTrue(info["artifacts"]["ota"]["path"].endswith("tater-echo-rook-v0.4.0-ota.tar.gz"))
        self.assertIn("factory/rook-linux/README.md", info["artifacts"]["factory"]["instructions_url"])

    def test_radar_identity_selects_its_own_verified_ota(self) -> None:
        row = {
            "connected": True,
            "firmware_target": "radar",
            "board": "radar",
            "firmware_version": "v0.3.0",
            "capabilities": {"ota": True},
        }
        spec = firmware._match_template_spec("native:echo-2-test", row)
        self.assertEqual("radar", spec["key"])
        self.assertFalse(spec["usb_recovery"])
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-2-radar.png"),
            ui_helpers.device_image_src("radar", "Tater Echo 2"),
        )
        source = firmware._firmware_manifest_source("radar")
        self.assertEqual("echo_targets", source["format"])
        with mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()):
            info = firmware._native_firmware_info("radar")
        self.assertTrue(info["available"])
        self.assertEqual("tater_native_ota", info["artifacts"]["ota"]["flash_transport"])
        self.assertEqual("2" * 64, info["artifacts"]["ota"]["sha256"])
        self.assertTrue(info["artifacts"]["ota"]["path"].endswith("tater-echo-radar-v0.4.0-ota.bin"))
        self.assertIn("factory/radar/README.md", info["artifacts"]["factory"]["instructions_url"])

    def test_rook_manifest_ota_flag_prevents_premature_update(self) -> None:
        payload = _echo_manifest()
        payload["targets"]["rook"]["ota"] = False
        with mock.patch.object(firmware, "_remote_json", return_value=payload):
            info = firmware._native_firmware_info("rook")
        self.assertTrue(info["available"])
        self.assertNotIn("ota", info["artifacts"])

    def test_old_rook_build_is_not_offered_an_ota_it_rejects(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "rook",
            "board": "rook",
            "device_info": {
                "friendly_name": "Bedroom Spot",
                "model": "rook",
                "project_version": "v0.0.1",
            },
            "capabilities": {"ota": False, "screen": True},
        }
        rook_spec = firmware._template_spec_by_key("rook")
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[rook_spec]),
            mock.patch.object(firmware, "_prebuilt_firmware_panel_summary", return_value={"available": True, "device_count": 3}),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:spot-test": row}})
        self.assertEqual([], panel["firmware_updates"])
        self.assertEqual([], panel["firmware_flash_targets"])
        variant = panel["variants"]["rook"]["native:spot-test"]
        self.assertFalse(variant["ota_supported"])
        self.assertNotIn("ota", variant["prebuilt_firmware"]["artifacts"])
        self.assertEqual(
            ui_helpers._named_satellite_image_src("echo-spot-rook.png"), variant["hero_image_src"]
        )

    def test_old_radar_build_is_not_offered_an_ota_it_rejects(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "radar",
            "board": "radar",
            "device_info": {
                "friendly_name": "Living Room Echo",
                "model": "radar",
                "project_version": "v0.0.1",
            },
            "capabilities": {"ota": False},
        }
        radar_spec = firmware._template_spec_by_key("radar")
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[radar_spec]),
            mock.patch.object(firmware, "_prebuilt_firmware_panel_summary", return_value={"available": True, "device_count": 4}),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:radar-test": row}})
        self.assertEqual([], panel["firmware_updates"])
        self.assertEqual([], panel["firmware_flash_targets"])
        variant = panel["variants"]["radar"]["native:radar-test"]
        self.assertFalse(variant["ota_supported"])
        self.assertNotIn("ota", variant["prebuilt_firmware"]["artifacts"])

    def test_radar_update_is_reported_in_firmware_panel(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "radar",
            "board": "radar",
            "device_info": {
                "name": "radar-test",
                "friendly_name": "Living Room Echo",
                "manufacturer": "Tater",
                "model": "radar",
                "project_name": "tater.native_satellite",
                "project_version": "v0.3.0",
            },
            "metadata": {
                "firmware_target": "radar",
                "board": "radar",
            },
            "capabilities": {"ota": True},
        }
        radar_spec = firmware._template_spec_by_key("radar")
        self.assertIsNotNone(radar_spec)
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[radar_spec]),
            mock.patch.object(
                firmware,
                "_prebuilt_firmware_panel_summary",
                return_value={"available": True, "device_count": 4, "version": "v0.4.0"},
            ),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:radar-test": row}})

        self.assertEqual(1, panel["firmware_update_count"])
        update = panel["firmware_updates"][0]
        self.assertEqual("native:radar-test", update["selector"])
        self.assertEqual("radar", update["template_key"])
        self.assertEqual("v0.3.0", update["installed"])
        self.assertEqual("v0.4.0", update["latest"])
        self.assertTrue(update["prebuilt_firmware_ota_available"])
        self.assertTrue(
            update["prebuilt_firmware"]["artifacts"]["ota"]["path"].endswith(
                "tater-echo-radar-v0.4.0-ota.bin"
            )
        )

    def test_checkers_update_is_reported_in_firmware_panel(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "checkers",
            "board": "checkers",
            "device_info": {
                "name": "show-test",
                "friendly_name": "Kitchen Show",
                "manufacturer": "Tater",
                "model": "checkers",
                "project_name": "tater.native_satellite",
                "project_version": "v0.3.0",
            },
            "metadata": {
                "firmware_target": "checkers",
                "board": "checkers",
            },
            "capabilities": {"ota": True, "screen": True},
        }
        checkers_spec = firmware._template_spec_by_key("checkers")
        self.assertIsNotNone(checkers_spec)
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[checkers_spec]),
            mock.patch.object(
                firmware,
                "_prebuilt_firmware_panel_summary",
                return_value={"available": True, "device_count": 2, "version": "v0.4.0"},
            ),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:show-test": row}})

        self.assertEqual(1, panel["firmware_update_count"])
        update = panel["firmware_updates"][0]
        self.assertEqual("native:show-test", update["selector"])
        self.assertEqual("checkers", update["template_key"])
        self.assertEqual("v0.3.0", update["installed"])
        self.assertEqual("v0.4.0", update["latest"])
        self.assertTrue(update["prebuilt_firmware_ota_available"])
        self.assertTrue(
            update["prebuilt_firmware"]["artifacts"]["ota"]["path"].endswith(
                "tater-echo-checkers-v0.4.0-ota.zip"
            )
        )

    def test_crown_update_is_reported_in_firmware_panel(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "crown",
            "board": "crown",
            "device_info": {
                "name": "crown-test",
                "friendly_name": "Kitchen Crown",
                "manufacturer": "Tater",
                "model": "crown",
                "project_name": "tater.native_satellite",
                "project_version": "v0.3.0",
            },
            "metadata": {
                "firmware_target": "crown",
                "board": "crown",
            },
            "capabilities": {"ota": True, "screen": True},
        }
        crown_spec = firmware._template_spec_by_key("crown")
        self.assertIsNotNone(crown_spec)
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[crown_spec]),
            mock.patch.object(
                firmware,
                "_prebuilt_firmware_panel_summary",
                return_value={"available": True, "device_count": 5, "version": "v0.4.0"},
            ),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:crown-test": row}})

        self.assertEqual(1, panel["firmware_update_count"])
        update = panel["firmware_updates"][0]
        self.assertEqual("native:crown-test", update["selector"])
        self.assertEqual("crown", update["template_key"])
        self.assertEqual("v0.3.0", update["installed"])
        self.assertEqual("v0.4.0", update["latest"])
        self.assertTrue(update["prebuilt_firmware_ota_available"])
        self.assertTrue(
            update["prebuilt_firmware"]["artifacts"]["ota"]["path"].endswith(
                "tater-echo-crown-v0.4.0-ota.tar.gz"
            )
        )

    def test_crown_without_ota_capability_is_not_offered_an_update(self) -> None:
        row = {
            "connected": True,
            "selected": True,
            "source": "tater_native",
            "firmware_target": "crown",
            "board": "crown",
            "device_info": {"model": "crown", "project_version": "v0.3.0"},
            "capabilities": {"ota": False, "screen": True},
        }
        crown_spec = firmware._template_spec_by_key("crown")
        with (
            mock.patch.object(firmware, "_remote_json", return_value=_echo_manifest()),
            mock.patch.object(firmware, "_native_template_specs", return_value=[crown_spec]),
            mock.patch.object(firmware, "_prebuilt_firmware_panel_summary", return_value={"available": True}),
            mock.patch.object(firmware, "_load_recorded_firmware_version", return_value={}),
        ):
            panel = firmware.firmware_panel_payload({"clients": {"native:crown-test": row}})
        self.assertEqual([], panel["firmware_updates"])
        self.assertEqual([], panel["firmware_flash_targets"])
        self.assertFalse(panel["variants"]["crown"]["native:crown-test"]["ota_supported"])


if __name__ == "__main__":
    unittest.main()
