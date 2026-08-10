from __future__ import annotations

import contextlib
import os
import select
import time
from typing import Any, Dict, List

from . import amlogic_usb, esp_usb


_S420_TEMPLATE_KEY = "thirdreality_s420"
_DEFAULT_BAUDRATE = 115200


class _PosixSerialReader:
    def __init__(self, fd: int, port: str, baudrate: int, *, timeout: float = 0.25) -> None:
        self.fd = int(fd)
        self.port = port
        self.baudrate = int(baudrate)
        self.timeout = max(0.01, float(timeout))
        self.is_open = True
        self.dtr = False
        self.rts = False
        self._buffer = bytearray()

    def read_until(self, expected: bytes = b"\n", size: int = 8192) -> bytes:
        limit = max(1, int(size))
        marker = bytes(expected or b"\n")
        deadline = time.monotonic() + self.timeout
        while self.is_open:
            marker_index = self._buffer.find(marker) if marker else -1
            if marker_index >= 0:
                end = min(limit, marker_index + len(marker))
                result = bytes(self._buffer[:end])
                del self._buffer[:end]
                return result
            if len(self._buffer) >= limit:
                result = bytes(self._buffer[:limit])
                del self._buffer[:limit]
                return result
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ready, _, _ = select.select([self.fd], [], [], min(0.1, remaining))
            if not ready:
                continue
            try:
                chunk = os.read(self.fd, min(4096, limit - len(self._buffer)))
            except BlockingIOError:
                continue
            if not chunk:
                break
            self._buffer.extend(chunk)
        if not self._buffer:
            return b""
        result = bytes(self._buffer[:limit])
        del self._buffer[:limit]
        return result

    def close(self) -> None:
        if not self.is_open:
            return
        self.is_open = False
        os.close(self.fd)


def _text(value: Any) -> str:
    return str(value or "").strip()


def is_s420(template_key: Any) -> bool:
    return _text(template_key).lower() == _S420_TEMPLATE_KEY


def serial_ports(template_key: Any) -> List[Dict[str, Any]]:
    if is_s420(template_key):
        return [
            {
                "value": port,
                "path": port,
                "label": f"S420 CH340 debug console · {port}",
                "description": "ThirdReality debug-board console",
                "kind": "s420_debug_console",
                "baudrate": _DEFAULT_BAUDRATE,
            }
            for port in amlogic_usb.find_debug_serial_ports()
        ]

    rows: List[Dict[str, Any]] = []
    for raw in esp_usb.serial_ports():
        row = dict(raw) if isinstance(raw, dict) else {}
        path = _text(row.get("path") or row.get("value"))
        if not path:
            continue
        row.update(
            {
                "value": path,
                "path": path,
                "kind": "esp_serial",
                "baudrate": _DEFAULT_BAUDRATE,
            }
        )
        rows.append(row)
    return rows


def resolve_serial_port(template_key: Any, port: Any) -> Dict[str, Any]:
    token = _text(port)
    ports = serial_ports(template_key)
    if not token and len(ports) == 1:
        return dict(ports[0])
    match = next((row for row in ports if _text(row.get("path")) == token), None)
    if isinstance(match, dict):
        return dict(match)
    if not ports:
        if is_s420(template_key):
            raise RuntimeError(
                "The S420 CH340 debug console was not found. Connect the debug-board USB cable and try again."
            )
        raise RuntimeError("No local ESP USB serial devices were found. Connect the satellite with a data cable and try again.")
    available = ", ".join(_text(row.get("path")) for row in ports[:6])
    raise RuntimeError(f"The selected Local USB log port is no longer available. Detected ports: {available}.")


def _open_posix_serial(path: str, baudrate: int) -> _PosixSerialReader:
    import termios

    # O_RDONLY avoids CDC modem-control writes. Clearing HUPCL prevents close()
    # from generating a hang-up pulse, which otherwise resets ESP32-S3 devices
    # that expose the built-in USB Serial/JTAG console.
    fd = os.open(path, os.O_RDONLY | os.O_NOCTTY | os.O_NONBLOCK)
    try:
        attrs = termios.tcgetattr(fd)
        attrs[0] = 0
        attrs[1] = 0
        attrs[2] = termios.CS8 | termios.CREAD | termios.CLOCAL
        attrs[3] = 0
        speed = getattr(termios, f"B{int(baudrate)}", termios.B115200)
        attrs[4] = speed
        attrs[5] = speed
        attrs[6][termios.VMIN] = 0
        attrs[6][termios.VTIME] = 1
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        return _PosixSerialReader(fd, path, baudrate)
    except Exception:
        os.close(fd)
        raise


def _open_pyserial(path: str, baudrate: int) -> Any:
    import serial

    options = {
        "port": None,
        "baudrate": max(1200, int(baudrate)),
        "timeout": 0.25,
        "write_timeout": 1.0,
        "rtscts": False,
        "dsrdtr": False,
    }
    try:
        handle = serial.Serial(exclusive=True, **options)
    except TypeError:
        handle = serial.Serial(**options)
    try:
        # Configure the inactive control-line state before opening the port so
        # ESP auto-reset circuits never see an initial DTR/RTS pulse.
        handle.dtr = False
        handle.rts = False
        handle.port = path
        handle.open()
        return handle
    except Exception:
        with contextlib.suppress(Exception):
            handle.close()
        raise


def open_serial(port: Any, *, baudrate: int = _DEFAULT_BAUDRATE) -> Any:
    path = _text(port)
    if not path:
        raise ValueError("A Local USB serial port is required.")
    speed = max(1200, int(baudrate))
    if os.name == "posix":
        return _open_posix_serial(path, speed)
    return _open_pyserial(path, speed)


def read_line(handle: Any, *, size: int = 8192) -> str:
    data = handle.read_until(b"\n", max(1, int(size)))
    if isinstance(data, str):
        return data.replace("\x00", "").strip("\r\n")
    return bytes(data or b"").decode("utf-8", errors="replace").replace("\x00", "").strip("\r\n")


def log_level(line: Any) -> str:
    lowered = _text(line).lower()
    if any(token in lowered for token in ("fatal", "panic", "exception", "traceback", "error:")):
        return "error"
    if any(token in lowered for token in ("warning", "warn:", "[warn]")):
        return "warn"
    if any(token in lowered for token in ("debug", "verbose")):
        return "debug"
    return "info"
