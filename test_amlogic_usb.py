from __future__ import annotations

import io
import os
import plistlib
import pty
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from tater_voice import amlogic_s420_flash, amlogic_usb


class AmlogicUsbTests(unittest.TestCase):
    def test_flash_tool_watchdog_stops_a_stalled_helper(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            with self.assertRaisesRegex(amlogic_s420_flash.FlashError, "timed out"):
                amlogic_s420_flash._stream_command(
                    [sys.executable, "-c", "import time; time.sleep(5)"],
                    cwd=Path(temp_name),
                    timeout=0.1,
                )

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
        with patch.object(amlogic_usb, "_run_capture", return_value=completed), patch.object(
            amlogic_usb.Path, "exists", return_value=True
        ):
            self.assertEqual(amlogic_usb._macos_ch340_ports(), ["/dev/cu.usbserial-s420"])

    def test_macos_capture_uses_posix_spawn_instead_of_subprocess_fork(self) -> None:
        with patch.object(amlogic_usb.platform, "system", return_value="Darwin"), patch.object(
            amlogic_usb.subprocess,
            "run",
            side_effect=AssertionError("subprocess.run must not be used on macOS"),
        ):
            result = amlogic_usb._run_capture(
                ["/usr/bin/printf", "worldcup-ready"],
                timeout=2.0,
                text=True,
            )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "worldcup-ready")

    def test_macos_flash_runner_uses_posix_spawn_and_streams_output(self) -> None:
        with patch.object(amlogic_usb.platform, "system", return_value="Darwin"), patch.object(
            amlogic_usb.subprocess,
            "Popen",
            side_effect=AssertionError("subprocess.Popen must not be used on macOS"),
        ):
            process = amlogic_usb.launch_flash_process(["/usr/bin/printf", "flash-ready"])
            output = process.stdout.read()
            process.stdout.close()
            returncode = process.wait(timeout=2.0)
        self.assertEqual(returncode, 0)
        self.assertEqual(output, "flash-ready")

    def test_prepare_device_keeps_an_existing_burn_connection(self) -> None:
        with patch.object(
            amlogic_usb,
            "probe_device",
            return_value={"connected": True, "output": "This firmware version is 0-7"},
        ), patch.object(
            amlogic_usb,
            "find_debug_serial_ports",
            return_value=["/dev/cu.usbserial-s420"],
        ) as find_ports:
            result = amlogic_usb.prepare_device({"available": True})
        self.assertTrue(result["connected"])
        self.assertTrue(result["already_in_burn_mode"])
        self.assertEqual(result["debug_port"], "/dev/cu.usbserial-s420")
        find_ports.assert_called_once_with()

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

    def test_serial_console_accepts_the_exact_s420_uboot_prompt(self) -> None:
        master_fd, slave_fd = pty.openpty()
        port = os.ttyname(slave_fd)

        def respond() -> None:
            os.read(master_fd, 4096)
            os.write(master_fd, b"Unknown command 'printf'\r\naxg_s420_v1_trspk#")

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
        self.assertIn("axg_s420_v1_trspk#", output)

    def test_boot_verification_extracts_only_reported_firmware_version(self) -> None:
        output = (
            "printf '__TATER_FW_BEGIN__'; cat /proc/cmdline; printf '__TATER_FW_END__'\r\n"
            "__TATER_FW_BEGIN__root=/dev/ubi0_0 firmware_version=0.2.1 console=ttyS0"
            " tater_runtime=ready__TATER_FW_END__\r\n"
        )
        self.assertEqual(amlogic_usb._firmware_version_from_console(output), "0.2.1")
        self.assertEqual(amlogic_usb._tater_runtime_from_console(output), "ready")
        self.assertEqual(amlogic_usb._firmware_version_from_console("axg_s420_v1_trspk#"), "")

    def test_boot_verification_accepts_manifest_release_name_for_device_version(self) -> None:
        self.assertEqual(
            amlogic_usb._normalized_s420_firmware_version("tater-thirdreality-0.2.1"),
            "0.2.1",
        )
        self.assertEqual(
            amlogic_usb._normalized_s420_firmware_version("tater-thirdreality-s420-0.2.1"),
            "0.2.1",
        )
        self.assertEqual(amlogic_usb._normalized_s420_firmware_version("0.2.1"), "0.2.1")

    def test_boot_verification_ignores_runtime_literal_in_echoed_serial_command(self) -> None:
        output = (
            "printf '__TATER_FW_BEGIN__'; if test -x /usr/bin/tater; "
            "then printf ' tater_runtime=ready'; else printf ' tater_runtime=missing'; fi; "
            "printf '__TATER_FW\r\n_END__'\r\n"
            "__TATER_FW_BEGIN__root=/dev/ubi0_0 firmware_version=0.2.1"
            " tater_runtime=ready\r\n__TATER_FW_END__\r\n"
        )
        self.assertEqual(amlogic_usb._firmware_version_from_console(output), "0.2.1")
        self.assertEqual(amlogic_usb._tater_runtime_from_console(output), "ready")

    def test_boot_verification_waits_silently_before_opening_console(self) -> None:
        events = []

        def sleep(seconds: float) -> None:
            events.append(("sleep", seconds))

        def open_console(*_args, **_kwargs):
            events.append(("open", 0.0))
            raise SystemExit("stop after first console access")

        with patch.object(amlogic_usb.time, "sleep", side_effect=sleep), patch.object(
            amlogic_usb,
            "_open_verified_s420_console",
            side_effect=open_console,
        ):
            with self.assertRaisesRegex(SystemExit, "first console access"):
                amlogic_usb.verify_tater_boot(
                    "/dev/cu.usbserial-s420",
                    "0.2.1",
                    timeout=30.0,
                    boot_grace=12.0,
                )

        self.assertEqual(events, [("sleep", 12.0), ("open", 0.0)])

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
        reboot_written = threading.Event()
        probe_count = 0
        real_write = os.write

        def probe(*_args, **_kwargs):
            nonlocal probe_count
            probe_count += 1
            if probe_count <= 2:
                return {"connected": False}
            reboot_written.wait(timeout=1.0)
            return {"connected": True, "output": "This firmware version is 0-7"}

        def serial_write(fd, payload):
            written = real_write(fd, payload)
            if payload == b"reboot\r":
                reboot_written.set()
            return written

        try:
            with patch.object(amlogic_usb, "probe_device", side_effect=probe), patch.object(
                amlogic_usb, "find_debug_serial_ports", return_value=["/dev/cu.usbserial-s420"]
            ), patch.object(
                amlogic_usb,
                "_open_verified_s420_console",
                return_value=(slave_fd, "axg_s420_v03trspk"),
            ), patch.object(
                amlogic_usb.os,
                "write",
                side_effect=serial_write,
            ):
                result = amlogic_usb.prepare_device({"available": True}, timeout=3.0)
        finally:
            os.close(master_fd)
        self.assertTrue(result["connected"])
        self.assertTrue(result["auto_rebooted"])
        self.assertEqual(result["debug_port"], "/dev/cu.usbserial-s420")
        self.assertGreaterEqual(result["probe_count"], 2)

    def test_prepare_device_enters_burn_mode_directly_from_uboot(self) -> None:
        master_fd, slave_fd = pty.openpty()
        burn_written = threading.Event()
        payloads = []
        real_write = os.write

        def probe(*_args, **_kwargs):
            if burn_written.is_set():
                return {"connected": True, "output": "This firmware version is 0-7"}
            return {"connected": False}

        def serial_write(fd, payload):
            payloads.append(payload)
            written = real_write(fd, payload)
            if payload == b"update 1000\r":
                burn_written.set()
            return written

        try:
            with patch.object(amlogic_usb, "probe_device", side_effect=probe), patch.object(
                amlogic_usb, "find_debug_serial_ports", return_value=["/dev/cu.usbserial-s420"]
            ), patch.object(
                amlogic_usb,
                "_open_verified_s420_console",
                return_value=(slave_fd, "axg_s420_v1_trspk#"),
            ), patch.object(amlogic_usb.os, "write", side_effect=serial_write):
                result = amlogic_usb.prepare_device({"available": True}, timeout=3.0)
        finally:
            os.close(master_fd)
        self.assertTrue(result["connected"])
        self.assertTrue(result["entered_from_uboot"])
        self.assertIn(b"update 1000\r", payloads)
        self.assertNotIn(b"reboot\r", payloads)

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

    def test_partition_write_uses_vendor_store_writer_and_uart_confirmation(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.stdout = io.StringIO("[ 50%/ 4MB]\n[update]mwrite success\n")
                self.returncode = None
                self.pid = 123
                self.poll_count = 0

            def poll(self):
                self.poll_count += 1
                if self.poll_count == 1:
                    return None
                self.returncode = 0
                return 0

        process = FakeProcess()
        uart_result = b"[MSG]Burn Start...\r\n[MSG]Burn complete\r\n[info]success\r\n"
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            update = root / "update"
            source = root / "boot.bin"
            port = root / "usbserial-s420"
            source.write_bytes(b"x" * 2050)
            port.touch()
            attrs = [0, 0, 0, 0, 0, 0, [0] * 32]
            with patch.object(amlogic_s420_flash.subprocess, "Popen", return_value=process) as popen, patch.object(
                amlogic_s420_flash.os, "open", return_value=41
            ), patch.object(amlogic_s420_flash.os, "read", return_value=uart_result), patch.object(
                amlogic_s420_flash.os, "close"
            ), patch.object(amlogic_s420_flash.select, "select", return_value=([41], [], [])), patch(
                "termios.tcgetattr", return_value=attrs
            ), patch("termios.tcsetattr"), patch("termios.tcflush"), patch.object(
                amlogic_s420_flash, "_UART_POST_WRITE_DRAIN_SECONDS", 0.0
            ):
                amlogic_s420_flash._write_partition(
                    update,
                    root,
                    "boot",
                    source,
                    0,
                    source.stat().st_size,
                    str(port),
                )

        command = popen.call_args.args[0]
        self.assertEqual(command, [str(update), "partition", "boot", str(source), "normal"])
        self.assertNotIn("nand", " ".join(command).lower())

    def test_post_reset_restore_primes_usb_and_rewrites_all_runtime_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            update = root / "update"
            boot = root / "boot.bin"
            recovery = root / "recovery.bin"
            system = root / "system.bin"
            boot.write_bytes(b"boot-image")
            recovery.write_bytes(b"recovery-image")
            system.write_bytes(b"system-image")
            events = []

            def capture(command, **_kwargs):
                events.append(("capture", command))
                return ""

            def write_partition(*args, **_kwargs):
                events.append(("write", args[2], args[3]))

            def uboot_output(_update, _root, command, _port):
                events.append(("uboot", command))
                return "[info]success"

            with patch.object(amlogic_s420_flash.time, "sleep"), patch.object(
                amlogic_s420_flash,
                "_wait_for_burn_mode",
                return_value=True,
            ), patch.object(
                amlogic_s420_flash,
                "_captured_command",
                side_effect=capture,
            ), patch.object(
                amlogic_s420_flash,
                "_write_partition",
                side_effect=write_partition,
            ), patch.object(
                amlogic_s420_flash,
                "_capture_uboot_output",
                side_effect=uboot_output,
            ):
                repaired = amlogic_s420_flash._restore_payloads_after_first_reset(
                    update,
                    root,
                    {"boot": boot, "recovery": recovery, "system": system},
                    "/dev/cu.usbserial-s420",
                )

        self.assertTrue(repaired)
        self.assertEqual(events[0], ("capture", [str(update), "bulkcmd", "echo 12345"]))
        self.assertEqual(
            events[1:4],
            [
                ("write", "recovery", recovery),
                ("write", "system", system),
                ("write", "boot", boot),
            ],
        )
        self.assertEqual(events[4:6], [("uboot", "setenv upgrade_step 2"), ("uboot", "save")])
        self.assertEqual(events[6], ("capture", [str(update), "bulkcmd", "burn_complete 1"]))


if __name__ == "__main__":
    unittest.main()
