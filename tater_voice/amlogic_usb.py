from __future__ import annotations

import hashlib
import os
import platform
import plistlib
import re
import select
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request

from tater_paths import agent_lab_path


_SOURCE_ROOT = Path(__file__).resolve().parents[1]
_FLASH_TOOL_RELATIVE_PATH = Path("aml-flash-tool") / "flash-tool"
_KHADAS_UTILS_COMMIT = "ec65caddc35497a4c739c9977641d2233bc9264e"
_KHADAS_RAW_BASE = f"https://raw.githubusercontent.com/khadas/utils/{_KHADAS_UTILS_COMMIT}/aml-flash-tool"
_MANAGED_HELPER_ROOT = agent_lab_path("firmware_tools", "khadas-utils", _KHADAS_UTILS_COMMIT, "aml-flash-tool")
_HELPER_SHA256 = {
    "flash-tool": "a752b68b4b51ccfc61fdff4b8ea4746d5c3a3abcc6aef62ef0901bf2eb9e482a",
    "tools/macos/update": "19f37cc129d061512c24bffbbad3a51355d976c12d6091366fe7dfce3c949b30",
    "tools/macos/aml_image_v2_packer": "3666083d441eb1abcb1e4a6a2c7ba89c2b134984d99afc1f203222080476c2e4",
    "tools/linux-x86/update": "82217e5adf6888771a2f353f53ef1927a69c6782d381f9ece9e993447afda6dc",
    "tools/linux-x86/aml_image_v2_packer": "8123b1295abb3262c76b650ba024975e38aedfd49b19f59a09f5920738ef1597",
    "tools/linux-arm/update": "7a842fb1d053b48bc20b008338e217cd6b4542ef6a45e950ccc05af598e52cae",
    "tools/linux-arm/aml_image_v2_packer": "c0aae85b07b8e343aa84c35a6fe375a63ad0d2b4023b6a8f091127866497b1f6",
    "tools/datas/usbbl2runpara_ddrinit.bin": "683421dc5900f8b6b10105f2a6d541fa35eab1d58555b7478ebadbec0948e850",
    "tools/datas/usbbl2runpara_runfipimg.bin": "39996bded3c2977384ae21cba323b4849d4b223f5ae0b2b3176d8e0c71198da1",
}
_CH340_VENDOR_ID = 0x1A86
_CH340_PRODUCT_IDS = {0x5523, 0x7523}
_S420_DTB_PATTERN = re.compile(r"axg_s420_[A-Za-z0-9_-]*trspk", re.IGNORECASE)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().lower()


def _candidate_flash_tools() -> List[Path]:
    candidates: List[Path] = []
    configured = str(os.getenv("TATER_AMLOGIC_FLASH_TOOL", "") or "").strip()
    if configured:
        path = Path(configured).expanduser()
        candidates.append(path / "flash-tool" if path.is_dir() else path)

    resources = str(os.getenv("TATER_APP_RESOURCES_DIR", "") or "").strip()
    if resources:
        candidates.append(Path(resources).expanduser() / "Native" / _FLASH_TOOL_RELATIVE_PATH)

    candidates.extend(
        (
            _SOURCE_ROOT / "vendor" / _FLASH_TOOL_RELATIVE_PATH,
            _MANAGED_HELPER_ROOT / "flash-tool",
            _SOURCE_ROOT.parent / "Khadas-Utils" / _FLASH_TOOL_RELATIVE_PATH,
            Path.home() / "Scripts" / "Khadas-Utils" / _FLASH_TOOL_RELATIVE_PATH,
            Path.home() / "Khadas-Utils" / _FLASH_TOOL_RELATIVE_PATH,
        )
    )

    unique: List[Path] = []
    seen = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        token = str(resolved)
        if token in seen:
            continue
        seen.add(token)
        unique.append(resolved)
    return unique


def _system_tool_dir() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    if system == "linux":
        return "linux-x86" if platform.machine().lower() in {"x86_64", "amd64"} else "linux-arm"
    return ""


def inspect_flash_tool(path: Optional[Path] = None) -> Dict[str, Any]:
    flash_tool = Path(path).expanduser().resolve() if path else next(
        (candidate for candidate in _candidate_flash_tools() if candidate.is_file()),
        Path(),
    )
    if not flash_tool.is_file():
        return {
            "available": False,
            "error": (
                "The Amlogic USB helper is not installed. Set TATER_AMLOGIC_FLASH_TOOL to the "
                "Khadas aml-flash-tool path, then restart Tater."
            ),
        }

    system_dir = _system_tool_dir()
    if not system_dir:
        return {
            "available": False,
            "path": str(flash_tool),
            "error": "S420 USB flashing is currently supported on macOS and Linux.",
        }

    root = flash_tool.parent
    update_tool = root / "tools" / system_dir / "update"
    packer_tool = root / "tools" / system_dir / "aml_image_v2_packer"
    data_files = (
        root / "tools" / "datas" / "usbbl2runpara_ddrinit.bin",
        root / "tools" / "datas" / "usbbl2runpara_runfipimg.bin",
    )
    missing = [str(item) for item in (update_tool, packer_tool, *data_files) if not item.is_file()]
    if missing:
        return {
            "available": False,
            "path": str(flash_tool),
            "error": "The Amlogic USB helper is incomplete: " + ", ".join(missing),
        }

    if not os.access(update_tool, os.X_OK) or not os.access(packer_tool, os.X_OK):
        return {
            "available": False,
            "path": str(flash_tool),
            "error": "The Amlogic USB helper binaries are not executable.",
        }

    return {
        "available": True,
        "path": str(flash_tool),
        "root": str(root),
        "update_path": str(update_tool),
        "packer_path": str(packer_tool),
        "platform": system_dir,
        "source_url": "https://github.com/khadas/utils/tree/ec65caddc35497a4c739c9977641d2233bc9264e/aml-flash-tool",
    }


def _download_verified_file(relative_path: str, target: Path, *, timeout: float) -> None:
    expected_sha = _HELPER_SHA256[relative_path]
    if target.is_file() and _file_sha256(target) == expected_sha:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    request = urllib_request.Request(
        f"{_KHADAS_RAW_BASE}/{relative_path}",
        headers={"User-Agent": "Tater/1.0", "Accept": "application/octet-stream, */*"},
    )
    try:
        with urllib_request.urlopen(request, timeout=max(5.0, float(timeout))) as response, tmp_path.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
        actual_sha = _file_sha256(tmp_path)
        if actual_sha != expected_sha:
            raise RuntimeError(f"Khadas helper verification failed for {relative_path}.")
        tmp_path.chmod(0o755 if target.name in {"flash-tool", "update", "aml_image_v2_packer"} else 0o644)
        tmp_path.replace(target)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def ensure_flash_tool(*, timeout: float = 60.0) -> Dict[str, Any]:
    existing = inspect_flash_tool()
    if bool(existing.get("available")):
        return existing

    system_dir = _system_tool_dir()
    if not system_dir:
        return existing
    required_paths = (
        "flash-tool",
        f"tools/{system_dir}/update",
        f"tools/{system_dir}/aml_image_v2_packer",
        "tools/datas/usbbl2runpara_ddrinit.bin",
        "tools/datas/usbbl2runpara_runfipimg.bin",
    )
    try:
        for relative_path in required_paths:
            _download_verified_file(relative_path, _MANAGED_HELPER_ROOT / relative_path, timeout=timeout)
    except Exception as exc:
        return {
            "available": False,
            "error": f"Tater could not install the verified Amlogic USB helper: {str(exc) or exc.__class__.__name__}",
        }
    return inspect_flash_tool(_MANAGED_HELPER_ROOT / "flash-tool")


def probe_device(tool_info: Dict[str, Any], *, timeout: float = 8.0) -> Dict[str, Any]:
    if not bool(tool_info.get("available")):
        return {"connected": False, "error": str(tool_info.get("error") or "Amlogic USB helper is unavailable.")}
    update_path = str(tool_info.get("update_path") or "").strip()
    try:
        result = subprocess.run(
            [update_path, "identify", "7"],
            cwd=str(tool_info.get("root") or "") or None,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout)),
            check=False,
        )
    except Exception as exc:
        return {"connected": False, "error": str(exc) or exc.__class__.__name__}

    output = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part and part.strip())
    connected = "firmware" in output.lower() and "can not find device" not in output.lower()
    if connected:
        return {"connected": True, "output": output}
    error = "S420 debug board was not detected in Amlogic USB burn mode."
    lowered = output.lower()
    if "dyld" in lowered or "library not loaded" in lowered:
        error = "The Amlogic helper could not load libusb-compat. Install it with Homebrew and restart Tater."
    return {"connected": False, "error": error, "output": output}


def _integer_property(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _registry_callout_devices(node: Any) -> List[str]:
    if not isinstance(node, dict):
        return []
    devices: List[str] = []
    callout = str(node.get("IOCalloutDevice") or "").strip()
    if callout:
        devices.append(callout)
    for child in node.get("IORegistryEntryChildren") or []:
        devices.extend(_registry_callout_devices(child))
    return devices


def _macos_ch340_ports() -> List[str]:
    ioreg = shutil.which("ioreg") or "/usr/sbin/ioreg"
    try:
        result = subprocess.run(
            [ioreg, "-r", "-c", "IOUserSerial", "-l", "-a"],
            capture_output=True,
            timeout=5.0,
            check=False,
        )
        services = plistlib.loads(result.stdout) if result.returncode == 0 and result.stdout else []
    except Exception:
        return []

    ports: List[str] = []
    for service in services if isinstance(services, list) else []:
        if not isinstance(service, dict):
            continue
        personality = service.get("IOMatchedPersonality")
        personality = personality if isinstance(personality, dict) else {}
        vendor = _integer_property(service.get("idVendor") or personality.get("idVendor"))
        product = _integer_property(service.get("idProduct") or personality.get("idProduct"))
        if vendor != _CH340_VENDOR_ID or product not in _CH340_PRODUCT_IDS:
            continue
        ports.extend(_registry_callout_devices(service))
        suffix = str(service.get("IOTTYSuffix") or "").strip()
        if suffix:
            ports.append(f"/dev/cu.usbserial-{suffix}")
    return sorted({port for port in ports if Path(port).exists()})


def _linux_ch340_ports() -> List[str]:
    ports: List[str] = []
    for sys_tty in sorted(Path("/sys/class/tty").glob("ttyUSB*")):
        current = (sys_tty / "device").resolve()
        for parent in (current, *current.parents):
            vendor_path = parent / "idVendor"
            product_path = parent / "idProduct"
            if not vendor_path.is_file() or not product_path.is_file():
                continue
            try:
                vendor = int(vendor_path.read_text(encoding="ascii").strip(), 16)
                product = int(product_path.read_text(encoding="ascii").strip(), 16)
            except (OSError, ValueError):
                break
            if vendor == _CH340_VENDOR_ID and product in _CH340_PRODUCT_IDS:
                device = Path("/dev") / sys_tty.name
                if device.exists():
                    ports.append(str(device))
            break
    return sorted(set(ports))


def find_debug_serial_ports() -> List[str]:
    configured = str(os.getenv("TATER_S420_DEBUG_PORT", "") or "").strip()
    if configured:
        configured_path = Path(configured).expanduser()
        return [str(configured_path)] if configured_path.exists() else []
    system = platform.system().lower()
    if system == "darwin":
        return _macos_ch340_ports()
    if system == "linux":
        return _linux_ch340_ports()
    return []


def _open_verified_s420_console(port: str, *, timeout: float = 2.5) -> tuple[int, str]:
    # termios is intentionally imported lazily so this module remains importable
    # on unsupported platforms and can still return a useful platform error.
    import termios

    fd = os.open(port, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    try:
        attrs = termios.tcgetattr(fd)
        attrs[0] = 0
        attrs[1] = 0
        attrs[2] = termios.CS8 | termios.CREAD | termios.CLOCAL
        attrs[3] = 0
        attrs[4] = termios.B115200
        attrs[5] = termios.B115200
        attrs[6][termios.VMIN] = 0
        attrs[6][termios.VTIME] = 1
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        termios.tcflush(fd, termios.TCIFLUSH)
        query = (
            b"\rprintf '__TATER_DTB_BEGIN__'; "
            b"tr -d '\\000' </proc/device-tree/amlogic-dt-id 2>/dev/null; "
            b"printf '__TATER_DTB_END__\\n'\r"
        )
        os.write(fd, query)
        output = bytearray()
        deadline = time.monotonic() + max(0.5, float(timeout))
        while time.monotonic() < deadline:
            ready, _, _ = select.select([fd], [], [], min(0.2, max(0.0, deadline - time.monotonic())))
            if not ready:
                continue
            try:
                chunk = os.read(fd, 8192)
            except BlockingIOError:
                continue
            if chunk:
                output.extend(chunk)
        text = output.decode("utf-8", errors="replace").replace("\x00", "")
        matches = re.findall(r"__TATER_DTB_BEGIN__(.*?)__TATER_DTB_END__", text, flags=re.DOTALL)
        if not any(_S420_DTB_PATTERN.search(match) for match in matches):
            raise RuntimeError(
                f"The CH340 adapter at {port} did not identify an S420 console. "
                "Close any serial terminal and verify the debug-board ribbon connection."
            )
        return fd, text
    except Exception:
        os.close(fd)
        raise


def prepare_device(tool_info: Dict[str, Any], *, timeout: float = 20.0) -> Dict[str, Any]:
    """Enter the S420's short U-Boot USB-burn window through its debug UART."""
    initial = probe_device(tool_info, timeout=min(2.0, max(1.0, float(timeout))))
    if bool(initial.get("connected")):
        return {**initial, "already_in_burn_mode": True, "auto_rebooted": False}

    ports = find_debug_serial_ports()
    if not ports:
        return {
            "connected": False,
            "error": (
                "The S420 CH340 debug console was not found. Connect the debug-board USB cable, "
                "leave the board powered on, and try again."
            ),
        }
    if len(ports) > 1:
        return {
            "connected": False,
            "error": (
                "More than one CH340 debug adapter is connected. Disconnect the unrelated adapter "
                f"and try again: {', '.join(ports)}"
            ),
        }

    port = ports[0]
    try:
        serial_fd, console_output = _open_verified_s420_console(port)
    except Exception as exc:
        return {"connected": False, "error": str(exc) or exc.__class__.__name__, "debug_port": port}

    stop_event = threading.Event()
    ready_event = threading.Event()
    connected_event = threading.Event()
    detected: Dict[str, Any] = {}

    def detector() -> None:
        ready_event.set()
        deadline = time.monotonic() + max(3.0, float(timeout))
        while not stop_event.is_set() and time.monotonic() < deadline:
            result = probe_device(tool_info, timeout=1.0)
            if bool(result.get("connected")):
                detected.update(result)
                connected_event.set()
                return

    worker = threading.Thread(target=detector, name="tater-s420-usb-detector", daemon=True)
    worker.start()
    try:
        if not ready_event.wait(timeout=1.0):
            raise RuntimeError("The S420 USB detector did not start.")
        os.write(serial_fd, b"sync; reboot\r")
        deadline = time.monotonic() + max(3.0, float(timeout))
        while not connected_event.is_set() and time.monotonic() < deadline:
            try:
                ready, _, _ = select.select([serial_fd], [], [], 0.1)
                if ready:
                    os.read(serial_fd, 8192)
            except (OSError, ValueError):
                # The UART can briefly disappear while USB re-enumerates.
                pass
    except Exception as exc:
        stop_event.set()
        worker.join(timeout=2.0)
        return {"connected": False, "error": str(exc) or exc.__class__.__name__, "debug_port": port}
    finally:
        try:
            os.close(serial_fd)
        except OSError:
            pass

    stop_event.set()
    worker.join(timeout=2.0)
    if connected_event.is_set():
        return {
            **detected,
            "connected": True,
            "already_in_burn_mode": False,
            "auto_rebooted": True,
            "debug_port": port,
            "console_output": console_output,
        }
    return {
        "connected": False,
        "debug_port": port,
        "error": (
            "Tater verified the S420 debug console and rebooted it, but did not catch the short "
            "Amlogic USB-burn window. Keep both USB cables connected and try again."
        ),
    }


def flash_command(tool_info: Dict[str, Any], image_path: Path) -> List[str]:
    if not bool(tool_info.get("available")):
        raise RuntimeError(str(tool_info.get("error") or "Amlogic USB helper is unavailable."))
    image = Path(image_path).expanduser().resolve()
    if not image.is_file():
        raise RuntimeError(f"S420 factory image is missing: {image.name}")
    runner = Path(__file__).with_name("amlogic_s420_flash.py")
    if not runner.is_file():
        raise RuntimeError("The Tater S420 raw-NAND flash backend is missing.")
    return [
        sys.executable,
        str(runner),
        "--tool-root",
        str(tool_info["root"]),
        "--image",
        str(image),
    ]
