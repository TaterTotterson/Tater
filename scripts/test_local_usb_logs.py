#!/usr/bin/env python3
from __future__ import annotations

import unittest
import time
from unittest import mock

from tater_voice import firmware, usb_serial_logs


class LocalUsbLogTests(unittest.TestCase):
    def test_s420_uses_only_the_ch340_debug_console(self) -> None:
        with mock.patch.object(
            usb_serial_logs.amlogic_usb,
            "find_debug_serial_ports",
            return_value=["/dev/cu.usbserial-830"],
        ):
            ports = usb_serial_logs.serial_ports("thirdreality_s420")

        self.assertEqual(ports[0]["path"], "/dev/cu.usbserial-830")
        self.assertEqual(ports[0]["kind"], "s420_debug_console")
        self.assertIn("CH340 debug console", ports[0]["label"])

    def test_esp_uses_detected_local_serial_ports(self) -> None:
        detected = [{"path": "/dev/cu.usbmodem5101", "value": "/dev/cu.usbmodem5101", "label": "ESP USB"}]
        with mock.patch.object(usb_serial_logs.esp_usb, "serial_ports", return_value=detected):
            ports = usb_serial_logs.serial_ports("satellite1")

        self.assertEqual(ports[0]["kind"], "esp_serial")
        self.assertEqual(ports[0]["baudrate"], 115200)

    def test_pyserial_fallback_releases_reset_lines_before_open_and_never_writes(self) -> None:
        handle = mock.Mock()
        serial_module = mock.Mock()
        serial_module.Serial.return_value = handle
        with mock.patch.dict("sys.modules", {"serial": serial_module}):
            result = usb_serial_logs._open_pyserial("COM4", 115200)

        self.assertIs(result, handle)
        self.assertFalse(handle.dtr)
        self.assertFalse(handle.rts)
        self.assertEqual(handle.port, "COM4")
        handle.open.assert_called_once_with()
        handle.write.assert_not_called()

    def test_posix_serial_open_is_read_only_and_disables_hangup(self) -> None:
        attrs = [1, 1, 1, 1, 1, 1, [0] * 32]
        termios_module = mock.Mock(
            CS8=0x01,
            CREAD=0x02,
            CLOCAL=0x04,
            B115200=115200,
            VMIN=6,
            VTIME=5,
            TCSANOW=0,
        )
        termios_module.tcgetattr.return_value = attrs
        with (
            mock.patch.dict("sys.modules", {"termios": termios_module}),
            mock.patch.object(usb_serial_logs.os, "open", return_value=42) as open_mock,
        ):
            handle = usb_serial_logs._open_posix_serial("/dev/cu.usbmodem5101", 115200)

        flags = open_mock.call_args.args[1]
        self.assertTrue(flags & usb_serial_logs.os.O_RDONLY == usb_serial_logs.os.O_RDONLY)
        self.assertFalse(flags & usb_serial_logs.os.O_RDWR)
        self.assertEqual(attrs[2], 0x01 | 0x02 | 0x04)
        termios_module.tcsetattr.assert_called_once()
        handle.is_open = False

    def test_log_level_marks_errors_and_warnings(self) -> None:
        self.assertEqual(usb_serial_logs.log_level("Guru Meditation Error: panic"), "error")
        self.assertEqual(usb_serial_logs.log_level("[warn] wifi retry"), "warn")
        self.assertEqual(usb_serial_logs.log_level("satellite ready"), "info")

    def test_backend_session_streams_and_stops_passive_usb_logs(self) -> None:
        handle = mock.Mock()
        delivered = False

        def read_line(_handle: object) -> str:
            nonlocal delivered
            if not delivered:
                delivered = True
                return "satellite ready"
            time.sleep(0.01)
            return ""

        port = {
            "path": "/dev/cu.usbmodem5101",
            "kind": "esp_serial",
            "baudrate": 115200,
        }
        context = {
            "selector": "test:local-usb-log",
            "template_key": "satellite1",
            "template_label": "Satellite1",
            "firmware_version": "test",
        }
        session_id = ""
        worker = None
        with (
            mock.patch.object(usb_serial_logs, "resolve_serial_port", return_value=port),
            mock.patch.object(usb_serial_logs, "open_serial", return_value=handle),
            mock.patch.object(usb_serial_logs, "read_line", side_effect=read_line),
        ):
            result = firmware._start_local_usb_log_session(context, port["path"])
            session_id = str(result["session_id"])
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                polled = firmware._poll_flash_session(session_id, after_seq=0)
                if any("satellite ready" in str(row.get("message")) for row in polled.get("entries", [])):
                    break
                time.sleep(0.01)
            else:
                self.fail("Local USB log entry was not streamed into the firmware session.")
            with firmware._FIRMWARE_SESSION_LOCK:
                worker = firmware._FIRMWARE_SESSIONS[session_id].get("worker")
            stopped = firmware._stop_flash_session(session_id)

        if worker is not None:
            worker.join(timeout=1.0)
        self.assertEqual(stopped["phase"], "completed")
        handle.write.assert_not_called()
        handle.close.assert_called()
        with firmware._FIRMWARE_SESSION_LOCK:
            firmware._FIRMWARE_SESSIONS.pop(session_id, None)


if __name__ == "__main__":
    unittest.main()
