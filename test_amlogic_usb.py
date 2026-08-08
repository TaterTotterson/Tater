from __future__ import annotations

import os
import plistlib
import pty
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from tater_voice import amlogic_s420_flash, amlogic_usb


class AmlogicUsbTests(unittest.TestCase):
    def test_macos_port_detection_only_returns_ch340(self) -> None:
        payload = plistlib.dumps(
            [
                {
                    "idVendor": 0x1A86,
                    "idProduct": 0x7523,
                    "IORegistryEntryChildren": [{"IOCalloutDevice": "/dev/cu.usbserial-s420"}],
                },
                {
                    "idVendor": 0x0403,
                    "idProduct": 0x6001,
                    "IORegistryEntryChildren": [{"IOCalloutDevice": "/dev/cu.usbserial-m5"}],
                },
            ]
        )
        completed = __import__("subprocess").CompletedProcess([], 0, stdout=payload, stderr=b"")
        with patch.object(amlogic_usb.subprocess, "run", return_value=completed), patch.object(
            amlogic_usb.Path, "exists", return_value=True
        ):
            self.assertEqual(amlogic_usb._macos_ch340_ports(), ["/dev/cu.usbserial-s420"])

    def test_prepare_device_keeps_an_existing_burn_connection(self) -> None:
        with patch.object(
            amlogic_usb,
            "probe_device",
            return_value={"connected": True, "output": "This firmware version is 0-7"},
        ), patch.object(amlogic_usb, "find_debug_serial_ports") as find_ports:
            result = amlogic_usb.prepare_device({"available": True})
        self.assertTrue(result["connected"])
        self.assertTrue(result["already_in_burn_mode"])
        find_ports.assert_not_called()

    def test_serial_console_must_report_the_s420_dtb(self) -> None:
        master_fd, slave_fd = pty.openpty()
        port = os.ttyname(slave_fd)

        def respond() -> None:
            os.read(master_fd, 4096)
            os.write(master_fd, b"__TATER_DTB_BEGIN__axg_s420_v03trspk__TATER_DTB_END__\r\n")

        responder = threading.Thread(target=respond, daemon=True)
        responder.start()
        console_fd = -1
        try:
            console_fd, output = amlogic_usb._open_verified_s420_console(port, timeout=0.5)
        finally:
            if console_fd >= 0:
                os.close(console_fd)
            os.close(slave_fd)
            os.close(master_fd)
        responder.join(timeout=1.0)
        self.assertIn("axg_s420_v03trspk", output)

    def test_prepare_device_refuses_ambiguous_debug_adapters(self) -> None:
        with patch.object(amlogic_usb, "probe_device", return_value={"connected": False}), patch.object(
            amlogic_usb,
            "find_debug_serial_ports",
            return_value=["/dev/cu.usbserial-one", "/dev/cu.usbserial-two"],
        ):
            result = amlogic_usb.prepare_device({"available": True})
        self.assertFalse(result["connected"])
        self.assertIn("More than one CH340", result["error"])

    def test_prepare_device_polls_before_requesting_serial_reboot(self) -> None:
        master_fd, slave_fd = pty.openpty()
        calls = [
            {"connected": False},
            {"connected": True, "output": "This firmware version is 0-7"},
        ]
        try:
            with patch.object(amlogic_usb, "probe_device", side_effect=calls), patch.object(
                amlogic_usb, "find_debug_serial_ports", return_value=["/dev/cu.usbserial-s420"]
            ), patch.object(
                amlogic_usb,
                "_open_verified_s420_console",
                return_value=(slave_fd, "axg_s420_v03trspk"),
            ):
                result = amlogic_usb.prepare_device({"available": True}, timeout=3.0)
        finally:
            os.close(master_fd)
        self.assertTrue(result["connected"])
        self.assertTrue(result["auto_rebooted"])
        self.assertEqual(result["debug_port"], "/dev/cu.usbserial-s420")

    def test_factory_manifest_must_be_for_s420(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            files = {
                "_aml_dtb": "dtb.bin",
                "bootloader": "bootloader.bin",
                "boot": "boot.bin",
                "recovery": "recovery.bin",
                "system": "system.bin",
            }
            lines = []
            for name, filename in files.items():
                (root / filename).write_bytes(b"not-an-s420" if name == "_aml_dtb" else b"payload")
                lines.append(
                    f'file="{filename}" main_type="PARTITION" sub_type="{name}" file_type="normal"'
                )
            cfg = root / "image.cfg"
            cfg.write_text("\n".join(lines), encoding="utf-8")
            parsed = amlogic_s420_flash._parse_partition_files(cfg)
            self.assertEqual(set(parsed), set(files))
            self.assertNotIn(b"axg_s420_v03trspk", parsed["_aml_dtb"].read_bytes())

    def test_chunking_never_crosses_the_s420_memory_hole(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            payload = Path(temp_name) / "system.bin"
            payload.write_bytes(b"x" * (amlogic_s420_flash._CHUNK_SIZE + 7))
            chunks = list(amlogic_s420_flash._chunks(payload))
        self.assertEqual([len(data) for _, data in chunks], [amlogic_s420_flash._CHUNK_SIZE, 7])
        self.assertLessEqual(
            amlogic_s420_flash._MEMORY_ADDRESS + max(len(data) for _, data in chunks),
            0x05000000,
        )

    def test_raw_nand_write_is_page_padded_and_read_back(self) -> None:
        commands = []
        memory = {"payload": b""}

        def fake_command(command, **_kwargs):
            commands.append(command)
            if command[1] == "mwrite":
                memory["payload"] = Path(command[2]).read_bytes()
            elif command[1] == "mread":
                Path(command[-1]).write_bytes(memory["payload"])
            return ""

        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            source = root / "boot.bin"
            source.write_bytes(b"x" * 2050)
            with patch.object(amlogic_s420_flash, "_captured_command", side_effect=fake_command):
                amlogic_s420_flash._write_and_verify_partition(
                    root / "update",
                    root,
                    "boot",
                    source,
                    0x02C00000,
                    root,
                    0,
                    source.stat().st_size,
                )
        nand_write = next(command[-1] for command in commands if "nand write" in command[-1])
        self.assertIn("0x2c00000", nand_write)
        self.assertTrue(nand_write.endswith("0x1000"))


if __name__ == "__main__":
    unittest.main()
