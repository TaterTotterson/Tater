from __future__ import annotations

import importlib.util
import os
import platform
import re
import sys
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Optional


_IGNORED_PORT_TOKENS = (
    "bluetooth-incoming-port",
    "debug-console",
)
_FALLBACK_PORT_PATTERNS = (
    "/dev/cu.usbmodem*",
    "/dev/cu.usbserial*",
    "/dev/cu.SLAB_USBtoUART*",
    "/dev/cu.wchusbserial*",
    "/dev/ttyACM*",
    "/dev/ttyUSB*",
)
APP_PARTITION_SIZE = 0x300000
_APP_PARTITION_OFFSETS_BY_FLASH_SIZE = {
    "8mb": ("0x20000", "0x320000"),
    "16mb": ("0x20000", "0x320000", "0x620000"),
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _hex_identifier(value: Any) -> str:
    try:
        return f"{int(value):04X}"
    except (TypeError, ValueError):
        return ""


def _port_is_usable(path: str) -> bool:
    token = _text(path)
    lowered = token.lower()
    if not token or any(ignored in lowered for ignored in _IGNORED_PORT_TOKENS):
        return False
    if platform.system().lower() == "darwin":
        return token.startswith("/dev/cu.")
    return token.startswith(("/dev/ttyACM", "/dev/ttyUSB")) or os.path.exists(token)


def _pyserial_ports() -> List[Any]:
    try:
        from serial.tools import list_ports

        return list(list_ports.comports())
    except Exception:
        return []


def serial_ports() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in _pyserial_ports():
        path = _text(getattr(item, "device", ""))
        if not _port_is_usable(path) or path in seen:
            continue
        seen.add(path)
        description = _text(getattr(item, "description", ""))
        manufacturer = _text(getattr(item, "manufacturer", ""))
        vid = _hex_identifier(getattr(item, "vid", None))
        pid = _hex_identifier(getattr(item, "pid", None))
        identity = f"USB {vid}:{pid}" if vid and pid else "USB serial"
        friendly = description if description and description.lower() not in {"n/a", path.lower()} else manufacturer
        label = f"{friendly or identity} · {path}"
        rows.append(
            {
                "value": path,
                "label": label,
                "path": path,
                "description": friendly or identity,
                "vid": vid,
                "pid": pid,
            }
        )

    if not rows:
        for pattern in _FALLBACK_PORT_PATTERNS:
            base = Path(pattern).parent
            for candidate in sorted(base.glob(Path(pattern).name)):
                path = str(candidate)
                if not _port_is_usable(path) or path in seen:
                    continue
                seen.add(path)
                rows.append(
                    {
                        "value": path,
                        "label": f"USB serial · {path}",
                        "path": path,
                        "description": "USB serial",
                        "vid": "",
                        "pid": "",
                    }
                )
    return sorted(rows, key=lambda row: _text(row.get("path")).lower())


def resolve_serial_port(port: str) -> Dict[str, Any]:
    token = _text(port)
    ports = serial_ports()
    if not token and len(ports) == 1:
        return dict(ports[0])
    match = next((row for row in ports if _text(row.get("path")) == token), None)
    if isinstance(match, dict):
        return dict(match)
    if not ports:
        raise RuntimeError("No local ESP USB serial devices were found. Connect the satellite and try again.")
    available = ", ".join(_text(row.get("path")) for row in ports[:6])
    raise RuntimeError(f"The selected ESP USB serial port is no longer available. Detected ports: {available}.")


def inspect_esptool() -> Dict[str, Any]:
    if importlib.util.find_spec("esptool") is None:
        return {
            "available": False,
            "version": "",
            "python": sys.executable,
            "error": "Tater's local ESP USB helper is not installed. Restart Tater so its private runtime can refresh.",
        }
    try:
        version = metadata.version("esptool")
    except metadata.PackageNotFoundError:
        version = "installed"
    return {
        "available": True,
        "version": _text(version),
        "python": sys.executable,
        "error": "",
    }


def app_partition_offsets(flash_size: str) -> List[str]:
    size_key = (_text(flash_size) or "16MB").lower()
    offsets = _APP_PARTITION_OFFSETS_BY_FLASH_SIZE.get(size_key)
    if not offsets:
        raise ValueError(f"USB keep-settings updates do not support ESP flash size {flash_size!r}.")
    return list(offsets)


def flash_command(
    port: str,
    image_path: Path,
    *,
    flash_size: str = "16MB",
    flash_mode: str = "dio",
    flash_freq: str = "40m",
    baud: int = 921600,
    python_executable: Optional[str] = None,
    flash_kind: str = "factory",
) -> List[str]:
    port_token = _text(port)
    image = Path(image_path).expanduser().resolve()
    # Keep a virtual environment's Python path intact. On macOS the venv
    # executable is a symlink to Homebrew Python; resolving that symlink before
    # launching drops the venv's site-packages, including esptool.
    python_path = Path(os.path.abspath(os.path.expanduser(_text(python_executable) or sys.executable)))
    if not port_token:
        raise ValueError("ESP USB serial port is required.")
    kind = _text(flash_kind).lower() or "factory"
    if kind not in {"factory", "ota"}:
        raise ValueError(f"Unsupported ESP USB flash kind: {flash_kind!r}")
    if not image.is_file():
        raise FileNotFoundError(f"ESP {kind} image not found: {image}")
    if not python_path.is_file():
        raise FileNotFoundError(f"Tater Python runtime not found: {python_path}")
    if kind == "ota" and image.stat().st_size > APP_PARTITION_SIZE:
        raise ValueError(
            f"ESP OTA image is too large for the app partition: {image.stat().st_size} > {APP_PARTITION_SIZE}."
        )
    command = [
        str(python_path),
        "-u",
        "-m",
        "esptool",
        "--chip",
        "esp32s3",
        "--port",
        port_token,
        "--baud",
        str(max(115200, int(baud))),
        "--before",
        "default-reset",
        "--after",
        "hard-reset",
        "write-flash",
    ]
    if kind == "factory":
        command.append("--erase-all")
    command.extend(
        [
            "--flash-mode",
            _text(flash_mode) or "dio",
            "--flash-freq",
            _text(flash_freq) or "40m",
            "--flash-size",
            _text(flash_size) or "16MB",
        ]
    )
    if kind == "factory":
        command.extend(["0x0", str(image)])
        return command

    for offset in app_partition_offsets(flash_size):
        command.extend([offset, str(image)])
    return command


def progress_percent(line: Any) -> Optional[float]:
    text = _text(line)
    if not text:
        return None
    lowered = text.lower()
    if "hard resetting" in lowered or "leaving" in lowered:
        return 99.0
    if "hash of data verified" in lowered or "verified" in lowered and "hash" in lowered:
        return 97.0
    if "configuring flash size" in lowered or "flash will be erased" in lowered:
        return 16.0
    if "connected to esp32" in lowered or "connected to esp32-s3" in lowered:
        return 12.0
    if "connecting" in lowered:
        return 8.0
    matches = re.findall(r"(?<!\d)(\d{1,3}(?:\.\d+)?)\s*%", text)
    if matches:
        raw = max(0.0, min(100.0, float(matches[-1])))
        return round(18.0 + raw * 0.76, 1)
    if "wrote " in lowered and " bytes" in lowered:
        return 95.0
    if "writing at" in lowered or "compressed " in lowered:
        return 20.0
    return None
