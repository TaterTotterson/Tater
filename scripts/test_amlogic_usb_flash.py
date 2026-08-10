#!/usr/bin/env python3
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class AmlogicUsbFlashTests(unittest.TestCase):
    def _helper_tree(self, root: pathlib.Path) -> pathlib.Path:
        flash_tool = root / "flash-tool"
        flash_tool.write_text("#!/bin/bash\n", encoding="utf-8")
        system_dir = "macos"
        for relative in (
            pathlib.Path("tools") / system_dir / "update",
            pathlib.Path("tools") / system_dir / "aml_image_v2_packer",
            pathlib.Path("tools") / "datas" / "usbbl2runpara_ddrinit.bin",
            pathlib.Path("tools") / "datas" / "usbbl2runpara_runfipimg.bin",
        ):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"test")
            path.chmod(0o755)
        return flash_tool

    def test_helper_is_validated_and_s420_runner_is_fixed(self) -> None:
        from tater_voice import amlogic_usb

        with tempfile.TemporaryDirectory() as temp_dir:
            root = pathlib.Path(temp_dir)
            flash_tool = self._helper_tree(root)
            image = root / "tater-s420.img"
            image.write_bytes(b"factory")
            with mock.patch("platform.system", return_value="Darwin"):
                info = amlogic_usb.inspect_flash_tool(flash_tool)
            self.assertTrue(info["available"])
            command = amlogic_usb.flash_command(info, image)
            self.assertEqual(command[0], sys.executable)
            self.assertEqual(pathlib.Path(command[1]).name, "amlogic_s420_flash.py")
            self.assertEqual(command[command.index("--tool-root") + 1], str(info["root"]))
            self.assertEqual(command[command.index("--image") + 1], str(image.resolve()))

    def test_incomplete_helper_is_rejected(self) -> None:
        from tater_voice import amlogic_usb

        with tempfile.TemporaryDirectory() as temp_dir:
            flash_tool = pathlib.Path(temp_dir) / "flash-tool"
            flash_tool.write_text("#!/bin/bash\n", encoding="utf-8")
            with mock.patch("platform.system", return_value="Darwin"):
                info = amlogic_usb.inspect_flash_tool(flash_tool)
            self.assertFalse(info["available"])
            self.assertIn("incomplete", info["error"].lower())

    def test_configured_helper_is_preferred(self) -> None:
        from tater_voice import amlogic_usb

        with tempfile.TemporaryDirectory() as temp_dir:
            flash_tool = self._helper_tree(pathlib.Path(temp_dir))
            with mock.patch.dict(os.environ, {"TATER_AMLOGIC_FLASH_TOOL": str(flash_tool)}), mock.patch(
                "platform.system", return_value="Darwin"
            ):
                info = amlogic_usb.inspect_flash_tool()
            self.assertTrue(info["available"])
            self.assertEqual(pathlib.Path(info["path"]), flash_tool.resolve())

    def test_existing_helper_does_not_trigger_a_download(self) -> None:
        from tater_voice import amlogic_usb

        with tempfile.TemporaryDirectory() as temp_dir:
            flash_tool = self._helper_tree(pathlib.Path(temp_dir))
            with mock.patch.dict(os.environ, {"TATER_AMLOGIC_FLASH_TOOL": str(flash_tool)}), mock.patch(
                "platform.system", return_value="Darwin"
            ), mock.patch.object(amlogic_usb.urllib_request, "urlopen") as urlopen:
                info = amlogic_usb.ensure_flash_tool()
            self.assertTrue(info["available"])
            urlopen.assert_not_called()

    def test_tater_backend_never_routes_s420_through_esptool(self) -> None:
        firmware_source = (REPO_ROOT / "tater_voice" / "firmware.py").read_text(encoding="utf-8")
        app_source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        self.assertIn('"voice_firmware_amlogic_flash_start"', firmware_source)
        self.assertIn('operation": "amlogic_usb_factory_flash"', firmware_source)
        self.assertIn('"--soc=axg"', (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"))
        self.assertIn(
            '[str(update), "bulkcmd", "burn_complete 1"]',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            '[str(update), "partition", partition, str(source), "normal"]',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            'command.extend(["--debug-port", debug_port])',
            firmware_source,
        )
        self.assertIn(
            '"setenv upgrade_step 1"',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            '"setenv upgrade_step 2"',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            '[str(update), "bulkcmd", "echo 12345"]',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            '"save"',
            (REPO_ROOT / "tater_voice" / "amlogic_s420_flash.py").read_text(encoding="utf-8"),
        )
        self.assertIn('runCoreManagerAction(card, coreKey, "voice_firmware_amlogic_flash_start"', app_source)
        self.assertIn('flash_transport != "esp_serial"', firmware_source)

    def test_s420_progress_ignores_amlogic_chunk_percentages(self) -> None:
        import re

        progress_pattern = re.compile(r"\((\d{1,3}(?:\.\d+)?)\s*%\)\.?$")
        self.assertIsNone(progress_pattern.search("[ 93%/ 12MB]"))
        self.assertEqual(
            progress_pattern.search("boot: verified 4194304/13465600 bytes (21.1%).").group(1),
            "21.1",
        )


if __name__ == "__main__":
    unittest.main()
