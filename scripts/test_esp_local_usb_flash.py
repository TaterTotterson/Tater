#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import tempfile
import unittest
from unittest import mock

from tater_voice import esp_usb


class EspLocalUsbFlashTests(unittest.TestCase):
    def test_flash_command_erases_and_writes_merged_factory_image_at_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            image = root / "voicepe-factory.bin"
            python = root / "python"
            image.write_bytes(b"factory")
            python.write_bytes(b"runtime")

            command = esp_usb.flash_command(
                "/dev/cu.usbmodem4101",
                image,
                flash_size="16MB",
                flash_mode="dio",
                flash_freq="40m",
                python_executable=str(python),
            )

        self.assertEqual(command[:4], [str(python.absolute()), "-u", "-m", "esptool"])
        self.assertIn("--erase-all", command)
        self.assertIn("--after", command)
        self.assertIn("hard-reset", command)
        self.assertEqual(command[-2:], ["0x0", str(image.resolve())])

    def test_flash_command_preserves_private_runtime_python_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            image = root / "satellite1-factory.bin"
            system_python = root / "homebrew-python"
            private_python = root / "private-runtime" / "bin" / "python"
            image.write_bytes(b"factory")
            system_python.write_bytes(b"runtime")
            private_python.parent.mkdir(parents=True)
            private_python.symlink_to(system_python)

            command = esp_usb.flash_command(
                "/dev/cu.usbmodem5101",
                image,
                python_executable=str(private_python),
            )

        self.assertEqual(command[0], str(private_python.absolute()))
        self.assertNotEqual(command[0], str(system_python.resolve()))

    def test_serial_ports_are_filtered_and_labeled(self) -> None:
        ports = [
            mock.Mock(
                device="/dev/cu.usbmodem4101",
                description="Espressif USB JTAG/serial debug unit",
                manufacturer="Espressif",
                vid=0x303A,
                pid=0x1001,
            ),
            mock.Mock(
                device="/dev/cu.Bluetooth-Incoming-Port",
                description="Bluetooth",
                manufacturer="Apple",
                vid=None,
                pid=None,
            ),
        ]
        with mock.patch.object(esp_usb, "_pyserial_ports", return_value=ports):
            rows = esp_usb.serial_ports()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"], "/dev/cu.usbmodem4101")
        self.assertIn("Espressif", rows[0]["label"])
        self.assertEqual(rows[0]["vid"], "303A")
        self.assertEqual(rows[0]["pid"], "1001")

    def test_progress_maps_esptool_write_and_verification_into_tater_progress(self) -> None:
        self.assertEqual(esp_usb.progress_percent("Connecting........"), 8.0)
        self.assertEqual(esp_usb.progress_percent("Writing at 0x000d3c00... (50 %)"), 56.0)
        self.assertEqual(esp_usb.progress_percent("Hash of data verified."), 97.0)
        self.assertEqual(esp_usb.progress_percent("Hard resetting via RTS pin..."), 99.0)


if __name__ == "__main__":
    unittest.main()
