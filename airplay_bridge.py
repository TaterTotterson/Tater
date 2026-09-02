from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import platform
import queue
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

from helpers import redis_client
from tater_paths import runtime_dir

logger = logging.getLogger("airplay_bridge")

AIRPLAY_TARGET_PREFIX = "airplay:"
AIRPLAY_REGISTRY_KEY = "tater:airplay:players:registry:v1"
AIRPLAY_DISCOVERY_CACHE_TTL_SECONDS = 60.0
AIRPLAY_DISCOVERY_STALE_TTL_SECONDS = 24 * 60 * 60.0
AIRPLAY_CLI_VERSION = "0.4.12"
AIRPLAY_CLI_RELEASE_ROOT = (
    f"https://github.com/music-assistant/airplay-cli/releases/download/v{AIRPLAY_CLI_VERSION}"
)
AIRPLAY_CLI_ASSETS = {
    ("darwin", "arm64"): (
        "cliairplay-macos-arm64",
        "87cc9f230969d0c0047b909e4a16451d906864c0056581347031c23fa040f1a8",
    ),
    ("darwin", "x86_64"): (
        "cliairplay-macos-x86_64",
        "1aad67e44bffde637e002b2c69cc1908170fd09a6f2ddb28fe16372597f55bff",
    ),
    ("linux", "aarch64"): (
        "cliairplay-linux-aarch64",
        "91a5d31f0722c2b0497bbb5494f2a386dd6693ddf8ec0d24d5df00a659d7a46d",
    ),
    ("linux", "x86_64"): (
        "cliairplay-linux-x86_64",
        "59490922adb8ac6aa3be8a1110b5472f4147fc429c3c042f986245fdb9e996ca",
    ),
}
AIRPLAY_PREPARE_TIMEOUT_SECONDS = 18.0
AIRPLAY_START_ACK_TIMEOUT_SECONDS = 5.0
AIRPLAY_SOLO_START_LEAD_MS = 500
AIRPLAY_NATIVE_START_LEAD_MS = 3000
AIRPLAY_CLOCK_READY_TIMEOUT_SECONDS = 4.0
AIRPLAY_SONOS_BUFFER_DEPTH_MS = 1750
AIRPLAY_WARM_FLUSH_TIMEOUT_SECONDS = 3.0
AIRPLAY_WARM_SPLICE_MARGIN_MS = 150
AIRPLAY_PTP_DAEMON_START_TIMEOUT_SECONDS = 3.0
AIRPLAY_PTP_DAEMON_RESTART_LIMIT = 1
AIRPLAY_PTP_CONTROL_HOST = "127.0.0.1"
AIRPLAY_PTP_CONTROL_PORT = 9010

_discovery_lock = threading.RLock()
_discovery_rows: List[Dict[str, Any]] = []
_discovery_ts = 0.0
_session_lock = threading.RLock()
_active_groups: Dict[str, "_AirPlayGroup"] = {}
_target_groups: Dict[str, str] = {}
_ptp_daemon_lock = threading.RLock()
_ptp_daemon_process: Optional[subprocess.Popen[bytes]] = None
_ptp_daemon_binary = ""
_ptp_daemon_source_ip = ""
_ptp_daemon_external = False
_ptp_daemon_stop_requested = False
_ptp_daemon_restart_count = 0
_ptp_daemon_last_ack = ""
_ptp_daemon_last_error = ""


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "ignore").strip()
    return str(value or "").strip()


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(_text(value)))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def normalize_airplay_id(value: Any) -> str:
    token = _text(value)
    if token.lower().startswith(AIRPLAY_TARGET_PREFIX):
        token = token[len(AIRPLAY_TARGET_PREFIX) :]
    return re.sub(r"[^0-9a-z]+", "", token.lower())


def airplay_target_value(value: Any) -> str:
    device_id = normalize_airplay_id(value)
    return f"{AIRPLAY_TARGET_PREFIX}{device_id}" if device_id else ""


def _runtime_root() -> Path:
    return runtime_dir() / "airplay_bridge"


def _find_ffmpeg() -> str:
    configured = _text(os.getenv("TATER_FFMPEG_PATH") or os.getenv("FFMPEG_PATH"))
    candidates = [
        configured,
        _text(shutil.which("ffmpeg")),
        "/opt/homebrew/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "/usr/bin/ffmpeg",
    ]
    try:
        import imageio_ffmpeg

        candidates.append(_text(imageio_ffmpeg.get_ffmpeg_exe()))
    except Exception:
        pass
    for raw_path in candidates:
        path = Path(raw_path).expanduser() if raw_path else None
        if path and path.is_file() and os.access(path, os.X_OK):
            return str(path)
    return ""


def _platform_key() -> tuple[str, str]:
    system = sys.platform.lower()
    system = "darwin" if system.startswith("darwin") else "linux" if system.startswith("linux") else system
    machine = platform.machine().lower()
    if machine in {"amd64", "x64"}:
        machine = "x86_64"
    elif machine in {"arm64", "aarch64"}:
        machine = "arm64" if system == "darwin" else "aarch64"
    return system, machine


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_subprocess_options() -> Dict[str, bool]:
    """Use posix_spawn instead of fork in Tater's multi-threaded runtime."""
    return {
        "close_fds": False,
        "start_new_session": False,
    }


def _ptp_daemon_probe(timeout_s: float = 0.25) -> tuple[bool, str]:
    """Probe cliairplay's local shared-clock control channel."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(max(0.05, float(timeout_s)))
        sock.sendto(b"?", (AIRPLAY_PTP_CONTROL_HOST, AIRPLAY_PTP_CONTROL_PORT))
        payload, _address = sock.recvfrom(512)
        ack = _text(payload)
        return ack.startswith("OK "), ack
    except OSError:
        return False, ""
    finally:
        sock.close()


def _ptp_daemon_status() -> Dict[str, Any]:
    with _ptp_daemon_lock:
        process = _ptp_daemon_process
        running = bool(process is not None and process.poll() is None)
        return {
            "running": running or _ptp_daemon_external,
            "owned": running,
            "external": bool(_ptp_daemon_external),
            "pid": int(process.pid) if running and process is not None else 0,
            "source_ip": _ptp_daemon_source_ip,
            "restart_count": int(_ptp_daemon_restart_count),
            "last_ack": _ptp_daemon_last_ack,
            "last_error": _ptp_daemon_last_error,
        }


def _read_ptp_daemon_output(process: subprocess.Popen[bytes]) -> None:
    global _ptp_daemon_process
    global _ptp_daemon_restart_count
    global _ptp_daemon_last_error

    stream = process.stdout
    if stream is not None:
        try:
            for raw_line in iter(stream.readline, b""):
                line = _text(raw_line)
                if not line:
                    continue
                if "error" in line.casefold() or "cannot" in line.casefold():
                    logger.warning("[airplay-ptp] %s", line)
                else:
                    logger.info("[airplay-ptp] %s", line)
        except Exception as exc:
            logger.debug("[airplay-ptp] daemon output reader ended: %s", exc)

    returncode = process.wait()
    with _ptp_daemon_lock:
        if _ptp_daemon_process is not process:
            return
        _ptp_daemon_process = None
        expected = bool(_ptp_daemon_stop_requested)
        binary = _ptp_daemon_binary
        source_ip = _ptp_daemon_source_ip
        should_restart = (
            not expected
            and bool(binary)
            and _ptp_daemon_restart_count < AIRPLAY_PTP_DAEMON_RESTART_LIMIT
        )
        if should_restart:
            _ptp_daemon_restart_count += 1
        if not expected:
            _ptp_daemon_last_error = f"Shared PTP daemon exited with status {returncode}."

    if expected:
        logger.info("[airplay-ptp] shared clock stopped")
        return

    logger.warning("[airplay-ptp] shared clock exited unexpectedly status=%s", returncode)
    # Existing streams retain a mapping to the old shared-memory object and
    # cannot safely continue on a replacement clock.  Tear them down so the
    # next playback attaches cleanly to the restarted daemon.
    with contextlib.suppress(Exception):
        _stop_all_airplay_groups()
    if should_restart:
        time.sleep(0.25)
        try:
            ensure_airplay_ptp_daemon(
                binary=binary,
                source_ip=source_ip,
                automatic_restart=True,
            )
            logger.info("[airplay-ptp] shared clock restarted automatically")
        except Exception as exc:
            with _ptp_daemon_lock:
                _ptp_daemon_last_error = _text(exc)
            logger.warning("[airplay-ptp] automatic restart failed: %s", exc)


def ensure_airplay_ptp_daemon(
    *,
    binary: str = "",
    source_ip: str = "",
    automatic_restart: bool = False,
) -> Dict[str, Any]:
    """Ensure every native AirPlay stream on this host shares one PTP clock."""
    global _ptp_daemon_process
    global _ptp_daemon_binary
    global _ptp_daemon_source_ip
    global _ptp_daemon_external
    global _ptp_daemon_stop_requested
    global _ptp_daemon_restart_count
    global _ptp_daemon_last_ack
    global _ptp_daemon_last_error

    sender = _text(binary) or ensure_airplay_cli()
    bind_ip = _text(source_ip)
    with _ptp_daemon_lock:
        process = _ptp_daemon_process
        if process is not None and process.poll() is None:
            return _ptp_daemon_status()

        live, ack = _ptp_daemon_probe()
        if live:
            _ptp_daemon_external = True
            _ptp_daemon_binary = sender
            _ptp_daemon_source_ip = bind_ip or _ptp_daemon_source_ip
            _ptp_daemon_last_ack = ack
            _ptp_daemon_last_error = ""
            return _ptp_daemon_status()

        if not automatic_restart:
            _ptp_daemon_restart_count = 0
        _ptp_daemon_external = False
        _ptp_daemon_stop_requested = False
        _ptp_daemon_binary = sender
        _ptp_daemon_source_ip = bind_ip
        _ptp_daemon_last_ack = ""
        _ptp_daemon_last_error = ""
        args = [sender, "--ptp-daemon", "--dacp", _dacp_id()]
        if bind_ip:
            args.extend(["--if", bind_ip])
        args.extend(["--debug", "3"])
        process = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **_safe_subprocess_options(),
        )
        _ptp_daemon_process = process
        threading.Thread(
            target=_read_ptp_daemon_output,
            args=(process,),
            name="airplay-ptp-daemon",
            daemon=True,
        ).start()

        deadline = time.monotonic() + AIRPLAY_PTP_DAEMON_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            live, ack = _ptp_daemon_probe(timeout_s=0.15)
            if live:
                _ptp_daemon_last_ack = ack
                logger.info(
                    "[airplay-ptp] shared clock ready pid=%s source_ip=%s %s",
                    process.pid,
                    bind_ip or "auto",
                    ack,
                )
                return _ptp_daemon_status()
            time.sleep(0.05)

        returncode = process.poll()
        _ptp_daemon_last_error = (
            f"Shared PTP daemon exited with status {returncode}."
            if returncode is not None
            else "Shared PTP daemon did not open its control channel."
        )
        _ptp_daemon_stop_requested = True
        if returncode is None:
            with contextlib.suppress(Exception):
                process.terminate()
        raise RuntimeError(
            _ptp_daemon_last_error
            + " On Linux/Docker, allow UDP 319/320 with root or CAP_NET_BIND_SERVICE."
        )


def _validate_cli(path: Path, expected_sha256: str = "") -> bool:
    if not path.is_file() or not os.access(path, os.X_OK):
        return False
    if expected_sha256 and _sha256(path) != expected_sha256:
        return False
    try:
        result = subprocess.run(
            [str(path), "--check"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=5.0,
            check=False,
            text=True,
            **_safe_subprocess_options(),
        )
    except Exception:
        return False
    return result.returncode == 0 and "cliairplay" in _text(result.stdout).lower()


def ensure_airplay_cli() -> str:
    override = _text(os.getenv("TATER_AIRPLAY_CLI_PATH"))
    if override:
        path = Path(override).expanduser()
        if not _validate_cli(path):
            raise RuntimeError(f"Configured AirPlay sender is not usable: {path}")
        return str(path)

    asset = AIRPLAY_CLI_ASSETS.get(_platform_key())
    if not asset:
        raise RuntimeError(
            f"Tater AirPlay Bridge does not yet provide a sender for {_platform_key()[0]}/{_platform_key()[1]}."
        )
    asset_name, expected_sha256 = asset
    install_dir = _runtime_root() / AIRPLAY_CLI_VERSION
    binary_path = install_dir / asset_name
    if _validate_cli(binary_path, expected_sha256):
        return str(binary_path)

    install_dir.mkdir(parents=True, exist_ok=True)
    download_path = install_dir / f".{asset_name}.download"
    with requests.get(
        f"{AIRPLAY_CLI_RELEASE_ROOT}/{asset_name}",
        stream=True,
        timeout=(15.0, 120.0),
    ) as response:
        response.raise_for_status()
        with download_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    if _sha256(download_path) != expected_sha256:
        with contextlib.suppress(Exception):
            download_path.unlink()
        raise RuntimeError("Downloaded AirPlay sender failed its SHA-256 integrity check.")
    download_path.chmod(0o755)
    os.replace(download_path, binary_path)

    for notice_name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
        notice_path = install_dir / notice_name
        if notice_path.exists():
            continue
        with contextlib.suppress(Exception):
            response = requests.get(
                f"{AIRPLAY_CLI_RELEASE_ROOT}/{notice_name}",
                timeout=(10.0, 30.0),
            )
            response.raise_for_status()
            notice_path.write_bytes(response.content)

    if not _validate_cli(binary_path, expected_sha256):
        raise RuntimeError("Installed AirPlay sender did not pass its self-check.")
    return str(binary_path)


def airplay_bridge_status() -> Dict[str, Any]:
    asset = AIRPLAY_CLI_ASSETS.get(_platform_key())
    override = _text(os.getenv("TATER_AIRPLAY_CLI_PATH"))
    if override:
        path = Path(override).expanduser()
        ready = _validate_cli(path)
    elif asset:
        path = _runtime_root() / AIRPLAY_CLI_VERSION / asset[0]
        ready = _validate_cli(path, asset[1])
    else:
        path = Path()
        ready = False
    with _session_lock:
        groups = list(_active_groups.values())
    active_timing_modes = sorted(
        {
            member.route_timing
            for group in groups
            for member in group.members
            if member.route_timing
        }
    )
    return {
        "supported": bool(asset or override),
        "ready": ready,
        "version": AIRPLAY_CLI_VERSION,
        "binary_path": str(path) if str(path) != "." else "",
        "active_group_count": len(groups),
        "active_target_count": sum(len(group.members) for group in groups),
        "timing_mode": active_timing_modes[0] if len(active_timing_modes) == 1 else "auto",
        "ptp_daemon": _ptp_daemon_status(),
    }


def _decode_properties(properties: Dict[Any, Any]) -> Dict[str, str]:
    return {
        _text(key): _text(value)
        for key, value in dict(properties or {}).items()
        if _text(key)
    }


def _service_display_name(service_type: str, service_name: str) -> str:
    suffix = service_type if service_type.startswith("_") else f"_{service_type}"
    name = _text(service_name)
    if name.lower().endswith(suffix.lower()):
        name = name[: -len(suffix)].rstrip(".")
    if service_type.startswith("_raop") and "@" in name:
        name = name.split("@", 1)[1]
    return name or service_name


def _record_device_id(service_type: str, service_name: str, properties: Dict[str, str]) -> str:
    device_id = normalize_airplay_id(properties.get("deviceid"))
    if device_id:
        return device_id
    if service_type.startswith("_raop"):
        instance = _text(service_name).split(".", 1)[0]
        if "@" in instance:
            return normalize_airplay_id(instance.split("@", 1)[0])
    return ""


def _merge_discovery_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        service_type = _text(record.get("service_type"))
        service_name = _text(record.get("service_name"))
        properties = record.get("properties") if isinstance(record.get("properties"), dict) else {}
        device_id = _record_device_id(service_type, service_name, properties)
        if not device_id:
            continue
        row = merged.setdefault(
            device_id,
            {
                "id": device_id,
                "target": airplay_target_value(device_id),
                "name": "",
                "model": "",
                "manufacturer": "",
                "host": "",
                "addresses": [],
                "airplay_port": 0,
                "raop_port": 0,
                "airplay_properties": {},
                "raop_properties": {},
                "airplay_service_name": "",
                "raop_service_name": "",
                "server": "",
                "available": True,
            },
        )
        addresses = [
            address
            for address in list(record.get("addresses") or [])
            if _text(address) and ":" not in _text(address)
        ]
        for address in addresses:
            if address not in row["addresses"]:
                row["addresses"].append(address)
        non_loopback = [address for address in row["addresses"] if not address.startswith("127.")]
        if non_loopback:
            row["host"] = non_loopback[0]
        elif row["addresses"]:
            row["host"] = row["addresses"][0]
        display_name = _service_display_name(service_type, service_name)
        if service_type.startswith("_airplay"):
            row["name"] = display_name or row["name"]
            row["model"] = _text(properties.get("model")) or row["model"]
            row["manufacturer"] = _text(properties.get("manufacturer")) or row["manufacturer"]
            row["airplay_port"] = _as_int(record.get("port"), 7000, 1, 65535)
            row["airplay_properties"] = dict(properties)
            row["airplay_service_name"] = service_name
            row["server"] = _text(record.get("server")) or row["server"]
        elif service_type.startswith("_raop"):
            row["name"] = row["name"] or display_name
            row["model"] = row["model"] or _text(properties.get("am"))
            row["raop_port"] = _as_int(record.get("port"), 5000, 1, 65535)
            row["raop_properties"] = dict(properties)
            row["raop_service_name"] = service_name
            row["server"] = row["server"] or _text(record.get("server"))
    rows = []
    for row in merged.values():
        if not _text(row.get("host")):
            continue
        if not _text(row.get("manufacturer")) and "sonos" in json.dumps(row).lower():
            row["manufacturer"] = "Sonos"
        row["name"] = _text(row.get("name")) or _text(row.get("host")) or row["id"]
        row["protocol"] = "airplay2" if row.get("airplay_port") else "raop"
        rows.append(row)
    rows.sort(key=lambda item: (_text(item.get("name")).casefold(), _text(item.get("host"))))
    return rows


def _browse_airplay(timeout_s: float) -> List[Dict[str, Any]]:
    try:
        from zeroconf import ServiceBrowser, ServiceListener, Zeroconf
    except Exception as exc:
        raise RuntimeError("zeroconf is required for AirPlay discovery") from exc

    class Listener(ServiceListener):
        def __init__(self) -> None:
            self.names: set[tuple[str, str]] = set()

        def add_service(self, zc: Any, service_type: str, name: str) -> None:
            self.names.add((service_type, name))

        def update_service(self, zc: Any, service_type: str, name: str) -> None:
            self.names.add((service_type, name))

        def remove_service(self, zc: Any, service_type: str, name: str) -> None:
            return None

    zc = Zeroconf()
    listener = Listener()
    browsers = [
        ServiceBrowser(zc, service_type, listener)
        for service_type in ("_airplay._tcp.local.", "_raop._tcp.local.")
    ]
    records: List[Dict[str, Any]] = []
    try:
        time.sleep(max(0.25, min(8.0, float(timeout_s or 2.0))))
        service_names = sorted(listener.names)

        def resolve(service: tuple[str, str]) -> Optional[Dict[str, Any]]:
            service_type, name = service
            info = zc.get_service_info(service_type, name, timeout=1200)
            if not info:
                return None
            return {
                "service_type": service_type,
                "service_name": name,
                "addresses": info.parsed_addresses(),
                "port": info.port,
                "server": _text(info.server),
                "properties": _decode_properties(info.properties),
            }

        with ThreadPoolExecutor(max_workers=max(1, min(12, len(service_names)))) as executor:
            for record in executor.map(resolve, service_names):
                if record:
                    records.append(record)
    finally:
        for browser in browsers:
            with contextlib.suppress(Exception):
                browser.cancel()
        zc.close()
    return _merge_discovery_records(records)


def _cache_discovery_rows(rows: List[Dict[str, Any]], *, now_ts: Optional[float] = None) -> None:
    payload = {"ts": float(now_ts if now_ts is not None else time.time()), "rows": rows}
    with contextlib.suppress(Exception):
        redis_client.set(AIRPLAY_REGISTRY_KEY, json.dumps(payload, ensure_ascii=False))


def _cached_discovery_rows(*, maximum_age_s: float) -> tuple[float, List[Dict[str, Any]]]:
    try:
        raw = redis_client.get(AIRPLAY_REGISTRY_KEY)
        payload = json.loads(raw) if raw else {}
        timestamp = float(payload.get("ts") or 0.0)
        rows = [dict(row) for row in payload.get("rows") or [] if isinstance(row, dict)]
    except Exception:
        return 0.0, []
    if timestamp <= 0 or time.time() - timestamp > maximum_age_s:
        return timestamp, []
    return timestamp, rows


def discover_airplay_devices(
    *,
    timeout_s: float = 2.0,
    force: bool = False,
) -> List[Dict[str, Any]]:
    global _discovery_rows, _discovery_ts
    now = time.time()
    with _discovery_lock:
        if not force and _discovery_rows and now - _discovery_ts <= AIRPLAY_DISCOVERY_CACHE_TTL_SECONDS:
            return [dict(row) for row in _discovery_rows]
        if not force:
            timestamp, cached = _cached_discovery_rows(
                maximum_age_s=AIRPLAY_DISCOVERY_CACHE_TTL_SECONDS
            )
            if cached:
                _discovery_rows = cached
                _discovery_ts = timestamp
                return [dict(row) for row in cached]
        try:
            rows = _browse_airplay(timeout_s)
        except Exception as exc:
            logger.warning("[airplay_bridge] discovery failed: %s", exc)
            rows = []
        if rows:
            _discovery_rows = rows
            _discovery_ts = now
            _cache_discovery_rows(rows, now_ts=now)
            return [dict(row) for row in rows]
        _timestamp, stale = _cached_discovery_rows(
            maximum_age_s=AIRPLAY_DISCOVERY_STALE_TTL_SECONDS
        )
        for row in stale:
            row["available"] = False
        return stale


def resolve_airplay_target(value: Any, *, refresh: bool = False) -> Dict[str, Any]:
    device_id = normalize_airplay_id(value)
    if not device_id:
        return {}
    rows = discover_airplay_devices(force=refresh)
    row = next((item for item in rows if normalize_airplay_id(item.get("id")) == device_id), None)
    if row or refresh:
        return dict(row or {})
    rows = discover_airplay_devices(force=True)
    row = next((item for item in rows if normalize_airplay_id(item.get("id")) == device_id), None)
    return dict(row or {})


def _target_setting(settings: Optional[Dict[str, Any]], target: str, default: int) -> int:
    source = settings if isinstance(settings, dict) else {}
    device_id = normalize_airplay_id(target)
    for alias in (target, airplay_target_value(target), device_id):
        if alias in source:
            return _as_int(source.get(alias), default, -1000, 1000)
    return int(default)


def _serialize_txt(properties: Dict[str, Any]) -> str:
    return " ".join(
        f"{_text(key)}={_text(value)}"
        for key, value in sorted(dict(properties or {}).items())
        if _text(key) and _text(value)
    )


def _dacp_id() -> str:
    seed = f"tater-airplay:{socket.gethostname()}".encode("utf-8")
    return hashlib.sha256(seed).hexdigest()[:16].upper()


def _source_ip_for_peer(host: Any) -> str:
    """Return the LAN address the kernel would use to reach a receiver."""
    target = _text(host)
    if not target:
        return ""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((target, 9))
        address = _text(sock.getsockname()[0])
        return "" if address.startswith("127.") else address
    except OSError:
        return ""
    finally:
        sock.close()


def _command_value(value: Any) -> str:
    return " ".join(_text(value).replace("\r", " ").replace("\n", " ").split())


class _AirPlayMember:
    def __init__(
        self,
        *,
        target: str,
        device: Dict[str, Any],
        binary: str,
        ffmpeg: str,
        source_url: str,
        start_position_seconds: float,
        volume_percent: int,
        title: str,
        artist: str,
        album: str,
        duration_seconds: float,
        group_id: str,
    ) -> None:
        self.target = airplay_target_value(target)
        self.device = dict(device)
        self.binary = binary
        self.ffmpeg_binary = ffmpeg
        self.source_url = _text(source_url)
        self.start_position_seconds = max(0.0, float(start_position_seconds or 0.0))
        self.volume_percent = _as_int(volume_percent, 75, 0, 100)
        self.title = _text(title) or "Tater Music"
        self.artist = _text(artist) or "Tater"
        self.album = _text(album) or "Tater Music"
        self.duration_seconds = max(0, int(float(duration_seconds or 0.0)))
        self.group_id = group_id
        self.run_dir: Optional[Path] = None
        self.command_fd: Optional[int] = None
        self.process: Optional[subprocess.Popen[bytes]] = None
        self.ffmpeg_process: Optional[subprocess.Popen[bytes]] = None
        self.connected = False
        self.audio_present = False
        self.playing = False
        self.clock_ready_resolved = False
        self.clock_ready_mode = ""
        self.clock_ready_state = ""
        self.clock_ready_at_unix_ms = 0
        self.route_protocol = ""
        self.route_flow = ""
        self.route_timing = ""
        self.latency_lead_ms = 0
        self.warm_lead_ms = 0
        self.flushed = False
        self.flushed_head_unix_ms = 0
        self.start_ack_ms = 0
        self.error = ""
        self.status_lines: queue.Queue[str] = queue.Queue(maxsize=200)
        self._condition = threading.Condition()
        self._replace_lock = threading.Lock()
        self._closed = False

    def _build_args(self, command_pipe: Path) -> List[str]:
        airplay_properties = (
            self.device.get("airplay_properties")
            if isinstance(self.device.get("airplay_properties"), dict)
            else {}
        )
        raop_properties = (
            self.device.get("raop_properties")
            if isinstance(self.device.get("raop_properties"), dict)
            else {}
        )
        airplay_port = _as_int(self.device.get("airplay_port"), 0, 0, 65535)
        raop_port = _as_int(self.device.get("raop_port"), 0, 0, 65535)
        manufacturer = _text(self.device.get("manufacturer")).casefold()
        # Let cliairplay resolve modern receivers from their complete AirPlay
        # advertisement. AirPlay 2 receivers must be contacted through the
        # _airplay endpoint even when the binary selects its RAOP-compatible
        # flow internally; forcing Sonos onto the legacy _raop endpoint bypasses
        # that routing and can leave a healthy-looking session silent.
        protocol = "auto" if airplay_port else "raop"
        endpoint_port = airplay_port or raop_port
        args = [
            self.binary,
            "--protocol",
            protocol,
            "--volume",
            str(self.volume_percent),
            "--dacp",
            _dacp_id(),
            "--activeremote",
            str(uuid.uuid4().int % 2_000_000_000),
            "--cmdpipe",
            str(command_pipe),
            "--samplerate",
            "44100",
            "--bitdepth",
            "16",
            "--channels",
            "2",
            "--port",
            str(endpoint_port or 7000),
        ]
        if protocol != "raop" and airplay_port:
            args.extend(
                [
                    "--name",
                    _text(self.device.get("name")) or self.target,
                    "--hostname",
                    _text(self.device.get("server")) or f"{socket.gethostname()}.local.",
                ]
            )
        # A Sonos stereo pair passes the AirPlay stream through an internal
        # grouped-renderer pipeline. Give that pipeline enough queued audio to
        # avoid the silent/starved-renderer failure that otherwise still reports
        # a successful connection and START.
        if "sonos" in manufacturer:
            args.extend(["--latency", str(AIRPLAY_SONOS_BUFFER_DEPTH_MS)])
        raop_name = _text(self.device.get("raop_service_name"))
        if raop_name:
            args.extend(["--udn", raop_name])
        for prop in ("et", "md", "am", "pk", "pw", "cn"):
            value = _text(raop_properties.get(prop))
            if value:
                args.extend([f"--{prop}", value])
        txt = _serialize_txt(airplay_properties)
        if (
            airplay_properties
            and not (_text(airplay_properties.get("features")) or _text(airplay_properties.get("ft")))
            and _text(raop_properties.get("ft"))
        ):
            txt = f"{txt} ft={_text(raop_properties.get('ft'))}".strip()
        if txt:
            args.extend(["--txt", txt])
        if protocol != "raop" and airplay_port:
            # A singleton daemon owns UDP 319/320 and publishes one stable
            # grandmaster clock across cold sessions and warm track splices.
            # The CLI safely ignores this clock source for routes that resolve
            # to legacy RAOP timing.
            args.append("--ptp-shared")
        if source_ip := _source_ip_for_peer(self.device.get("host")):
            args.extend(["--if", source_ip])
        args.extend(["--debug", "3", _text(self.device.get("host"))])
        return args

    def _record_line(self, line: str) -> None:
        clean = _text(line)
        if not clean:
            return
        with contextlib.suppress(queue.Full):
            self.status_lines.put_nowait(clean)
        first_playing = False
        with self._condition:
            lowered = clean.lower()
            if "[status] connected" in lowered:
                self.connected = True
            if "[status] audio" in lowered:
                self.audio_present = True
            if "[status] playing " in lowered:
                first_playing = not self.playing
                self.playing = True
            if "[status] route " in lowered:
                fields = dict(part.split("=", 1) for part in clean.split() if "=" in part)
                self.route_protocol = _text(fields.get("protocol")).lower()
                self.route_flow = _text(fields.get("flow")).lower()
                self.route_timing = _text(fields.get("timing")).lower()
            if "[status] latency " in lowered:
                fields = dict(part.split("=", 1) for part in clean.split() if "=" in part)
                self.latency_lead_ms = _as_int(fields.get("lead_ms"), 0, 0, 30_000)
                self.warm_lead_ms = _as_int(fields.get("warm_lead_ms"), 0, 0, 30_000)
            if "[status] flushed" in lowered:
                fields = dict(part.split("=", 1) for part in clean.split() if "=" in part)
                self.flushed_head_unix_ms = _as_int(
                    fields.get("head_unix_ms"),
                    0,
                    0,
                    9_999_999_999_999,
                )
                self.flushed = True
            if "[status] clock_ready " in lowered:
                fields = dict(part.split("=", 1) for part in clean.split() if "=" in part)
                mode = _text(fields.get("mode")).lower()
                state = _text(fields.get("state")).lower()
                if not (state == "cold" and mode != "ntp"):
                    self.clock_ready_mode = mode
                    self.clock_ready_state = state
                    try:
                        self.clock_ready_at_unix_ms = int(fields.get("ready_at_unix_ms") or 0)
                    except (TypeError, ValueError):
                        self.clock_ready_at_unix_ms = 0
                    # ``probing`` includes a projected ready timestamp, but the
                    # receiver's PTP clock has not converged yet.  Treating that
                    # forecast as ready lets the native timeline become fixed
                    # while Sonos is still correcting its clock, which makes an
                    # otherwise shared start anchor audibly miss.  Warm groups
                    # already report ``ready`` and do not pay this cold-start
                    # wait on track replacement.
                    # NTP routes explicitly have no measurable readiness state;
                    # they must not wait for a PTP-only ``ready`` transition.
                    self.clock_ready_resolved = mode == "ntp" or state in {"ready", "stalled"}
            if "[status] started" in lowered:
                match = re.search(r"\bat_unix_ms=(\d+)", clean)
                if match:
                    self.start_ack_ms = int(match.group(1))
            if "[status] error" in lowered or "[error]" in lowered:
                self.error = clean
            self._condition.notify_all()
        if "[status] error" in lowered or "[error]" in lowered:
            logger.warning("[airplay_bridge] %s %s", self.target, clean)
        elif first_playing or any(
            marker in lowered
            for marker in (
                "[status] route",
                "[status] connected",
                "[status] audio ",
                "[status] latency ",
                "[status] clock_ready ",
                "[status] flushed",
                "[status] started ",
                "[status] eof",
            )
        ):
            logger.info("[airplay_bridge] %s %s", self.target, clean)
        else:
            logger.debug("[airplay_bridge] %s %s", self.target, clean)

    def _read_stream(self, stream: Any) -> None:
        try:
            for raw_line in iter(stream.readline, b""):
                self._record_line(bytes(raw_line).decode("utf-8", "ignore"))
        except Exception as exc:
            logger.debug("[airplay_bridge] status reader ended for %s: %s", self.target, exc)

    def _wait_for(self, predicate: Any, timeout_s: float) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout_s))
        with self._condition:
            while not predicate():
                if self.error:
                    return False
                process = self.process
                if process is not None and process.poll() is not None:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=min(0.25, remaining))
        return True

    def prepare(self, timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS) -> None:
        session_root = _runtime_root() / "sessions"
        session_root.mkdir(parents=True, exist_ok=True)
        self.run_dir = Path(tempfile.mkdtemp(prefix=f"{self.group_id[:8]}-", dir=session_root))
        command_pipe = self.run_dir / "commands.pipe"
        os.mkfifo(command_pipe, 0o600)
        self.command_fd = os.open(command_pipe, os.O_RDWR | os.O_NONBLOCK)
        args = self._build_args(command_pipe)
        self.process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **_safe_subprocess_options(),
        )
        assert self.process.stdout is not None
        assert self.process.stderr is not None
        threading.Thread(
            target=self._read_stream,
            args=(self.process.stdout,),
            name=f"airplay-stdout-{self.target[-6:]}",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._read_stream,
            args=(self.process.stderr,),
            name=f"airplay-stderr-{self.target[-6:]}",
            daemon=True,
        ).start()
        if not self._wait_for(lambda: self.connected, min(timeout_s, 12.0)):
            raise RuntimeError(self.error or f"AirPlay receiver {self.device.get('name')} did not connect.")

        self.send_metadata()
        self.send_command(f"VOLUME={self.volume_percent}")

    def send_metadata(self) -> None:
        self.send_command(
            "\n".join(
                (
                    f"TITLE={_command_value(self.title)}",
                    f"ARTIST={_command_value(self.artist)}",
                    f"ALBUM={_command_value(self.album)}",
                    f"DURATION={self.duration_seconds}",
                    f"ITEMID={_command_value(self.group_id)}",
                    "ACTION=SENDMETA",
                )
            )
        )

    def begin_audio(self, timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS) -> None:
        if self.process is None or self.process.poll() is not None or not self.connected:
            raise RuntimeError(f"AirPlay receiver {self.device.get('name')} is not connected.")
        if self.ffmpeg_process is not None and self.ffmpeg_process.poll() is None:
            return

        assert self.process.stdin is not None
        ffmpeg_args = [
            self.ffmpeg_binary,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
        ]
        if self.start_position_seconds > 0:
            ffmpeg_args.extend(["-ss", f"{self.start_position_seconds:.3f}"])
        ffmpeg_args.extend(
            [
                "-i",
                self.source_url,
                "-vn",
                "-sn",
                "-dn",
                "-ac",
                "2",
                "-ar",
                "44100",
                "-f",
                "s16le",
                "pipe:1",
            ]
        )
        self.ffmpeg_process = subprocess.Popen(
            ffmpeg_args,
            stdin=subprocess.DEVNULL,
            stdout=self.process.stdin,
            stderr=subprocess.PIPE,
            **_safe_subprocess_options(),
        )
        if not self._wait_for(lambda: self.audio_present, timeout_s):
            detail = self.error
            if not detail and self.ffmpeg_process.poll() not in (None, 0):
                stderr = self.ffmpeg_process.stderr
                detail = _text(stderr.read(2048) if stderr else "")
            raise RuntimeError(detail or f"AirPlay receiver {self.device.get('name')} did not buffer audio.")
        # Sonos can acknowledge a scheduled START yet hold its renderer silent
        # when the first metadata push arrived before PCM initialized the AirPlay
        # stream. Re-send identity and volume after [STATUS] audio, matching the
        # order that produces an audible renderer in Music Assistant's flow.
        self.send_metadata()
        self.send_command(f"VOLUME={self.volume_percent}")
        clock_ready = self._wait_for(
            lambda: self.clock_ready_resolved,
            min(AIRPLAY_CLOCK_READY_TIMEOUT_SECONDS, max(0.1, float(timeout_s))),
        )
        if not clock_ready:
            raise RuntimeError(
                f"AirPlay receiver {self.device.get('name')} did not stabilize its playback clock."
            )
        if self.clock_ready_state == "stalled" and self.clock_ready_mode != "ntp":
            raise RuntimeError(
                f"AirPlay receiver {self.device.get('name')} did not establish its playback clock."
            )

    def _terminate_ffmpeg(self) -> None:
        process = self.ffmpeg_process
        if process is None or process.poll() is not None:
            return
        with contextlib.suppress(Exception):
            process.terminate()
        with contextlib.suppress(Exception):
            process.wait(timeout=2.0)
        if process.poll() is None:
            with contextlib.suppress(Exception):
                process.kill()
            with contextlib.suppress(Exception):
                process.wait(timeout=1.0)

    def replace_audio(
        self,
        *,
        source_url: str,
        start_position_seconds: float,
        volume_percent: int,
        title: str,
        artist: str,
        album: str,
        duration_seconds: float,
        timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS,
    ) -> None:
        """Flush and refill one track without rebuilding the receiver session."""
        with self._replace_lock:
            process = self.process
            stdin = process.stdin if process is not None else None
            if (
                self._closed
                or process is None
                or process.poll() is not None
                or stdin is None
                or getattr(stdin, "closed", False)
                or not self.connected
            ):
                raise RuntimeError(f"AirPlay receiver {self.device.get('name')} cannot be reused.")

            # No old bytes may arrive between FLUSH and its acknowledgement.
            # Tater retains the CLI stdin writer while replacing only ffmpeg.
            self._terminate_ffmpeg()
            with self._condition:
                self.flushed = False
                self.flushed_head_unix_ms = 0
                self.audio_present = False
                self.playing = False
                self.start_ack_ms = 0
                self.error = ""
            self.send_command("ACTION=FLUSH")
            if not self._wait_for(
                lambda: self.flushed,
                min(
                    AIRPLAY_WARM_FLUSH_TIMEOUT_SECONDS,
                    max(0.1, float(timeout_s)),
                ),
            ):
                raise RuntimeError(
                    self.error
                    or f"AirPlay receiver {self.device.get('name')} did not acknowledge its warm flush."
                )

            # FLUSH re-arms the binary's one-shot audio status. Reset our copy
            # after the ack as well so only the replacement track can satisfy it.
            with self._condition:
                self.audio_present = False
                self.playing = False
                self.start_ack_ms = 0
            self.source_url = _text(source_url)
            self.start_position_seconds = max(0.0, float(start_position_seconds or 0.0))
            self.volume_percent = _as_int(volume_percent, self.volume_percent, 0, 100)
            self.title = _text(title) or "Tater Music"
            self.artist = _text(artist) or "Tater"
            self.album = _text(album) or "Tater Music"
            self.duration_seconds = max(0, int(float(duration_seconds or 0.0)))
            self.send_metadata()
            self.send_command(f"VOLUME={self.volume_percent}")
            self.begin_audio(timeout_s=max(5.0, min(30.0, float(timeout_s))))

    def send_command(self, command: str) -> None:
        if self.command_fd is None:
            raise RuntimeError("AirPlay command pipe is unavailable.")
        payload = (_text(command).rstrip("\n") + "\n").encode("utf-8")
        written = 0
        deadline = time.monotonic() + 2.0
        while written < len(payload):
            try:
                written += os.write(self.command_fd, payload[written:])
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Timed out writing to the AirPlay command pipe.")
                time.sleep(0.01)

    def start(self, start_unix_ms: int) -> int:
        with self._condition:
            self.start_ack_ms = 0
            self.playing = False
        self.send_metadata()
        self.send_command(f"VOLUME={self.volume_percent}")
        self.send_command(f"START_UNIX_MS={int(start_unix_ms)}\nACTION=START")
        if not self._wait_for(lambda: self.start_ack_ms > 0, AIRPLAY_START_ACK_TIMEOUT_SECONDS):
            raise RuntimeError(self.error or f"AirPlay receiver {self.device.get('name')} did not acknowledge start.")
        acknowledged_start_ms = int(self.start_ack_ms)
        start_delay_s = max(0.0, (acknowledged_start_ms - int(time.time() * 1000)) / 1000.0)
        if not self._wait_for(lambda: self.playing, start_delay_s + 3.0):
            raise RuntimeError(
                self.error
                or f"AirPlay receiver {self.device.get('name')} acknowledged start but did not begin playing."
            )
        return acknowledged_start_ms

    def set_volume(self, volume_percent: int) -> None:
        self.volume_percent = _as_int(volume_percent, self.volume_percent, 0, 100)
        self.send_command(f"VOLUME={self.volume_percent}")

    def stop(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
        with contextlib.suppress(Exception):
            self.send_command("ACTION=STOP")
        self._terminate_ffmpeg()
        if self.process is not None and self.process.stdin is not None:
            with contextlib.suppress(Exception):
                self.process.stdin.close()
        for process in (self.process,):
            if process is None or process.poll() is not None:
                continue
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                process.wait(timeout=2.0)
            if process.poll() is None:
                with contextlib.suppress(Exception):
                    process.kill()
        if self.command_fd is not None:
            with contextlib.suppress(Exception):
                os.close(self.command_fd)
            self.command_fd = None
        if self.run_dir is not None:
            with contextlib.suppress(Exception):
                shutil.rmtree(self.run_dir)
            self.run_dir = None


class _AirPlayGroup:
    def __init__(self, group_id: str, members: List[_AirPlayMember]) -> None:
        self.group_id = group_id
        self.members = list(members)
        self.created_at = time.time()
        self.start_unix_ms = 0

    def stop(self) -> None:
        with ThreadPoolExecutor(max_workers=max(1, len(self.members))) as executor:
            futures = [executor.submit(member.stop) for member in self.members]
            for future in futures:
                with contextlib.suppress(Exception):
                    future.result(timeout=4.0)


def _forget_group(group_id: str) -> Optional[_AirPlayGroup]:
    with _session_lock:
        group = _active_groups.pop(group_id, None)
        if group:
            for member in group.members:
                if _target_groups.get(member.target) == group_id:
                    _target_groups.pop(member.target, None)
        return group


def _stop_all_airplay_groups() -> Dict[str, Any]:
    with _session_lock:
        groups = list(_active_groups.values())
        _active_groups.clear()
        _target_groups.clear()
    warnings: List[str] = []
    sent_count = 0
    for group in groups:
        try:
            sent_count += len(group.members)
            group.stop()
        except Exception as exc:
            warnings.append(f"{group.group_id}: {exc}")
    return {"ok": not warnings, "sent_count": sent_count, "warnings": warnings}


def shutdown_airplay_bridge_runtime() -> Dict[str, Any]:
    """Stop active senders and Tater's owned shared PTP clock."""
    global _ptp_daemon_process
    global _ptp_daemon_external
    global _ptp_daemon_stop_requested
    global _ptp_daemon_last_ack

    group_result = _stop_all_airplay_groups()
    with _ptp_daemon_lock:
        _ptp_daemon_stop_requested = True
        process = _ptp_daemon_process
        external = bool(_ptp_daemon_external)
        _ptp_daemon_external = False
        _ptp_daemon_last_ack = ""
    if process is not None and process.poll() is None:
        with contextlib.suppress(Exception):
            process.terminate()
        try:
            process.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(Exception):
                process.kill()
            with contextlib.suppress(Exception):
                process.wait(timeout=1.0)
    with _ptp_daemon_lock:
        if _ptp_daemon_process is process:
            _ptp_daemon_process = None
    return {
        **group_result,
        "ptp_daemon_stopped": bool(process is not None),
        "external_ptp_daemon_preserved": external,
    }


def stop_airplay_targets(targets: Iterable[Any]) -> Dict[str, Any]:
    target_values = {airplay_target_value(target) for target in targets if airplay_target_value(target)}
    with _session_lock:
        group_ids = {_target_groups[target] for target in target_values if target in _target_groups}
    stopped = 0
    warnings: List[str] = []
    for group_id in group_ids:
        group = _forget_group(group_id)
        if not group:
            continue
        try:
            stopped += len(group.members)
            group.stop()
        except Exception as exc:
            warnings.append(f"{group_id}: {exc}")
    return {"ok": not warnings, "sent_count": stopped, "warnings": warnings}


def stop_airplay_group_sync(group_id: str) -> Dict[str, Any]:
    group = _forget_group(_text(group_id))
    if not group:
        return {"ok": True, "sent_count": 0, "warnings": []}
    try:
        sent_count = len(group.members)
        group.stop()
        return {"ok": True, "sent_count": sent_count, "warnings": []}
    except Exception as exc:
        return {"ok": False, "sent_count": 0, "warnings": [_text(exc)], "error": _text(exc)}


def prepare_airplay_group_sync(
    *,
    targets: List[str],
    source_url: str,
    start_position_seconds: float = 0.0,
    volume_percent: int = 75,
    target_volume_percent: Optional[Dict[str, Any]] = None,
    title: str = "Tater Music",
    artist: str = "Tater",
    album: str = "Tater Music",
    duration_seconds: float = 0.0,
    timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    clean_targets = [airplay_target_value(target) for target in targets if airplay_target_value(target)]
    clean_targets = list(dict.fromkeys(clean_targets))
    if not clean_targets:
        return {"ok": False, "sent_count": 0, "error": "No AirPlay bridge targets selected."}
    if not _text(source_url):
        return {"ok": False, "sent_count": 0, "error": "AirPlay playback URL is missing."}

    binary = ensure_airplay_cli()
    ffmpeg = _find_ffmpeg()
    if not ffmpeg:
        return {"ok": False, "sent_count": 0, "error": "ffmpeg is required for AirPlay playback."}
    stop_airplay_targets(clean_targets)
    devices = {airplay_target_value(row.get("id")): row for row in discover_airplay_devices(force=True)}
    native_airplay_devices = [
        devices.get(target)
        for target in clean_targets
        if isinstance(devices.get(target), dict) and devices.get(target, {}).get("airplay_port")
    ]
    ptp_daemon: Dict[str, Any] = {}
    if native_airplay_devices:
        first_device = native_airplay_devices[0] or {}
        try:
            ptp_daemon = ensure_airplay_ptp_daemon(
                binary=binary,
                source_ip=_source_ip_for_peer(first_device.get("host")),
            )
        except Exception as exc:
            return {
                "ok": False,
                "sent_count": 0,
                "error": f"AirPlay shared clock could not start: {_text(exc)}",
            }
    group_id = f"airplay-{uuid.uuid4().hex[:16]}"
    members: List[_AirPlayMember] = []
    failures: List[str] = []
    for target in clean_targets:
        device = devices.get(target)
        if not device or not bool(device.get("available", True)):
            failures.append(f"{target} (receiver is unavailable)")
            continue
        target_volume = _target_setting(target_volume_percent, target, volume_percent)
        members.append(
            _AirPlayMember(
                target=target,
                device=device,
                binary=binary,
                ffmpeg=ffmpeg,
                source_url=source_url,
                start_position_seconds=start_position_seconds,
                volume_percent=max(0, min(100, target_volume)),
                title=title,
                artist=artist,
                album=album,
                duration_seconds=duration_seconds,
                group_id=group_id,
            )
        )
    if not members:
        return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "No AirPlay receivers are available."}

    prepared: List[_AirPlayMember] = []
    with ThreadPoolExecutor(max_workers=len(members)) as executor:
        future_members = {
            executor.submit(member.prepare, max(5.0, min(30.0, float(timeout_s)))): member
            for member in members
        }
        for future in as_completed(future_members):
            member = future_members[future]
            try:
                future.result()
                prepared.append(member)
            except Exception as exc:
                failures.append(f"{member.target} ({exc})")
                member.stop()
    if not prepared:
        return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "AirPlay preparation failed."}

    group = _AirPlayGroup(group_id, prepared)
    with _session_lock:
        _active_groups[group_id] = group
        for member in prepared:
            _target_groups[member.target] = group_id
    result: Dict[str, Any] = {
        "ok": True,
        "group_id": group_id,
        "prepared_count": len(prepared),
        "prepared_targets": [member.target for member in prepared],
        "timing_modes": {
            member.target: member.route_timing or "unknown"
            for member in prepared
        },
        "routes": {
            member.target: {
                "protocol": member.route_protocol or "unknown",
                "flow": member.route_flow or "unknown",
                "timing": member.route_timing or "unknown",
            }
            for member in prepared
        },
    }
    if ptp_daemon:
        result["ptp_daemon"] = ptp_daemon
    if failures:
        result["warnings"] = failures
    return result


def prime_airplay_group_sync(
    *,
    group_id: str,
    timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Begin a paced PCM feed after every non-AirPlay member is prepared."""
    with _session_lock:
        group = _active_groups.get(_text(group_id))
    if not group:
        return {"ok": False, "primed_count": 0, "error": "AirPlay bridge group is unavailable."}

    failures: List[str] = []
    primed: List[_AirPlayMember] = []
    with ThreadPoolExecutor(max_workers=max(1, len(group.members))) as executor:
        future_members = {
            executor.submit(
                member.begin_audio,
                max(5.0, min(30.0, float(timeout_s))),
            ): member
            for member in group.members
        }
        for future in as_completed(future_members):
            member = future_members[future]
            try:
                future.result()
                primed.append(member)
            except Exception as exc:
                failures.append(f"{member.target} ({exc})")

    if failures or len(primed) != len(group.members):
        failed_group = _forget_group(group.group_id)
        if failed_group:
            failed_group.stop()
        return {
            "ok": False,
            "primed_count": len(primed),
            "error": "; ".join(failures) or "AirPlay audio priming failed.",
        }
    now_unix_ms = int(time.time() * 1000)
    minimum_start_lead_ms = max(
        AIRPLAY_SOLO_START_LEAD_MS,
        max((member.latency_lead_ms for member in primed), default=0)
        + AIRPLAY_WARM_SPLICE_MARGIN_MS,
    )
    return {
        "ok": True,
        "group_id": group.group_id,
        "primed_count": len(primed),
        "primed_targets": [member.target for member in primed],
        "minimum_start_lead_ms": minimum_start_lead_ms,
        "minimum_start_unix_ms": now_unix_ms + minimum_start_lead_ms,
        "clock_readiness": {
            member.target: {
                "mode": member.clock_ready_mode,
                "state": member.clock_ready_state,
                "ready_at_unix_ms": member.clock_ready_at_unix_ms,
            }
            for member in primed
        },
    }


def reuse_airplay_group_sync(
    *,
    group_id: str,
    targets: List[str],
    source_url: str,
    start_position_seconds: float = 0.0,
    volume_percent: int = 75,
    target_volume_percent: Optional[Dict[str, Any]] = None,
    target_sync_offset_ms: Optional[Dict[str, Any]] = None,
    reference_sync_offset_ms: int = 0,
    title: str = "Tater Music",
    artist: str = "Tater",
    album: str = "Tater Music",
    duration_seconds: float = 0.0,
    timeout_s: float = AIRPLAY_PREPARE_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Warm-flush an active group and refill it with a replacement track."""
    clean_targets = [airplay_target_value(target) for target in targets if airplay_target_value(target)]
    clean_targets = list(dict.fromkeys(clean_targets))
    with _session_lock:
        group = _active_groups.get(_text(group_id))
    if not group:
        return {
            "ok": False,
            "reusable": False,
            "primed_count": 0,
            "error": "AirPlay bridge group is unavailable.",
        }
    if set(clean_targets) != {member.target for member in group.members}:
        stale_group = _forget_group(group.group_id)
        if stale_group:
            stale_group.stop()
        return {
            "ok": False,
            "reusable": False,
            "primed_count": 0,
            "error": "The selected AirPlay receivers changed.",
        }
    if not _text(source_url):
        return {
            "ok": False,
            "reusable": False,
            "primed_count": 0,
            "error": "AirPlay playback URL is missing.",
        }

    failures: List[str] = []
    reused: List[_AirPlayMember] = []
    with ThreadPoolExecutor(max_workers=max(1, len(group.members))) as executor:
        future_members = {}
        for member in group.members:
            target_volume = _target_setting(target_volume_percent, member.target, volume_percent)
            future_members[
                executor.submit(
                    member.replace_audio,
                    source_url=source_url,
                    start_position_seconds=start_position_seconds,
                    volume_percent=max(0, min(100, target_volume)),
                    title=title,
                    artist=artist,
                    album=album,
                    duration_seconds=duration_seconds,
                    timeout_s=max(5.0, min(30.0, float(timeout_s))),
                )
            ] = member
        for future in as_completed(future_members):
            member = future_members[future]
            try:
                future.result()
                reused.append(member)
            except Exception as exc:
                failures.append(f"{member.target} ({exc})")

    if failures or len(reused) != len(group.members):
        failed_group = _forget_group(group.group_id)
        if failed_group:
            failed_group.stop()
        return {
            "ok": False,
            "reusable": False,
            "primed_count": len(reused),
            "error": "; ".join(failures) or "AirPlay warm replacement failed.",
        }

    now_unix_ms = int(time.time() * 1000)
    reference_offset = _as_int(reference_sync_offset_ms, 0, -1000, 1000)
    minimum_start_unix_ms = now_unix_ms + AIRPLAY_SOLO_START_LEAD_MS
    for member in reused:
        offset = _target_setting(target_sync_offset_ms, member.target, 0)
        if member.warm_lead_ms > 0:
            minimum_start_unix_ms = max(
                minimum_start_unix_ms,
                now_unix_ms
                + member.warm_lead_ms
                + AIRPLAY_WARM_SPLICE_MARGIN_MS
                - offset
                + reference_offset,
            )
        if member.flushed_head_unix_ms > 0:
            minimum_start_unix_ms = max(
                minimum_start_unix_ms,
                member.flushed_head_unix_ms
                + AIRPLAY_WARM_SPLICE_MARGIN_MS
                - offset
                + reference_offset,
            )

    group.start_unix_ms = 0
    return {
        "ok": True,
        "reusable": True,
        "reused": True,
        "group_id": group.group_id,
        "prepared_count": len(reused),
        "primed_count": len(reused),
        "prepared_targets": [member.target for member in reused],
        "primed_targets": [member.target for member in reused],
        "minimum_start_unix_ms": minimum_start_unix_ms,
        "minimum_start_lead_ms": max(0, minimum_start_unix_ms - now_unix_ms),
        "clock_readiness": {
            member.target: {
                "mode": member.clock_ready_mode,
                "state": member.clock_ready_state,
                "ready_at_unix_ms": member.clock_ready_at_unix_ms,
            }
            for member in reused
        },
        "warm_constraints": {
            member.target: {
                "warm_lead_ms": member.warm_lead_ms,
                "flushed_head_unix_ms": member.flushed_head_unix_ms,
            }
            for member in reused
        },
        "timing_modes": {
            member.target: member.route_timing or "unknown"
            for member in reused
        },
        "routes": {
            member.target: {
                "protocol": member.route_protocol or "unknown",
                "flow": member.route_flow or "unknown",
                "timing": member.route_timing or "unknown",
            }
            for member in reused
        },
    }


def commit_airplay_group_sync(
    *,
    group_id: str,
    start_unix_ms: int,
    reference_sync_offset_ms: int = 0,
    target_sync_offset_ms: Optional[Dict[str, Any]] = None,
    allow_reanchor: bool = False,
) -> Dict[str, Any]:
    with _session_lock:
        group = _active_groups.get(_text(group_id))
    if not group:
        return {"ok": False, "sent_count": 0, "error": "AirPlay bridge group is unavailable."}
    silent_members = [member.target for member in group.members if not member.audio_present]
    if silent_members:
        return {
            "ok": False,
            "sent_count": 0,
            "error": "AirPlay audio was not primed for " + ", ".join(silent_members) + ".",
        }
    base_start_ms = int(start_unix_ms)
    if base_start_ms <= int(time.time() * 1000) + 250:
        if not allow_reanchor:
            expired_group = _forget_group(group.group_id)
            if expired_group:
                expired_group.stop()
            return {
                "ok": False,
                "sent_count": 0,
                "error": "The shared AirPlay start instant expired before every player was ready.",
            }
        base_start_ms = int(time.time() * 1000) + AIRPLAY_SOLO_START_LEAD_MS
    reference_offset = _as_int(reference_sync_offset_ms, 0, -1000, 1000)

    starts: List[Dict[str, Any]] = []
    failures: List[str] = []
    tasks: Dict[Any, tuple[_AirPlayMember, int, int]] = {}
    with ThreadPoolExecutor(max_workers=max(1, len(group.members))) as executor:
        for member in group.members:
            offset = _target_setting(target_sync_offset_ms, member.target, 0)
            requested = base_start_ms + offset - reference_offset
            tasks[executor.submit(member.start, requested)] = (member, requested, offset)
        for future in as_completed(tasks):
            member, requested, offset = tasks[future]
            try:
                actual = int(future.result())
                starts.append(
                    {
                        "target": member.target,
                        "requested_unix_ms": requested,
                        "actual_unix_ms": actual,
                        "sync_offset_ms": offset,
                        "correction_ms": actual - requested,
                    }
                )
            except Exception as exc:
                failures.append(f"{member.target} ({exc})")

    # AirPlay-only groups can converge on a later instant if a receiver rejects
    # the first anchor. A native-satellite reference cannot be moved after its
    # commit, so mixed groups report the rare correction for calibration instead.
    if allow_reanchor and starts:
        corrected_base = max(
            row["actual_unix_ms"] - row["sync_offset_ms"] + reference_offset
            for row in starts
        )
        if corrected_base > base_start_ms + 2:
            return commit_airplay_group_sync(
                group_id=group_id,
                start_unix_ms=corrected_base + 150,
                reference_sync_offset_ms=reference_offset,
                target_sync_offset_ms=target_sync_offset_ms,
                allow_reanchor=False,
            )

    group.start_unix_ms = base_start_ms
    warnings = list(failures)
    corrected = [row for row in starts if abs(int(row.get("correction_ms") or 0)) > 10]
    if corrected:
        warnings.append(
            "An AirPlay receiver corrected the requested start time; use Test sync to verify its calibration."
        )
    timing_modes = sorted(
        {member.route_timing for member in group.members if member.route_timing}
    )
    result: Dict[str, Any] = {
        "ok": bool(starts),
        "sent_count": len(starts),
        "group_id": group_id,
        "start_unix_ms": base_start_ms,
        "members": sorted(starts, key=lambda row: _text(row.get("target"))),
        "timing_mode": timing_modes[0] if len(timing_modes) == 1 else "mixed",
    }
    if warnings:
        result["warnings"] = warnings
    if not starts:
        result["error"] = "; ".join(failures) or "AirPlay bridge start failed."
        group = _forget_group(group_id)
        if group:
            group.stop()
    return result


def set_airplay_target_volumes(values: Dict[str, Any]) -> Dict[str, Any]:
    sent_count = 0
    warnings: List[str] = []
    for raw_target, raw_volume in dict(values or {}).items():
        target = airplay_target_value(raw_target)
        with _session_lock:
            group = _active_groups.get(_target_groups.get(target, ""))
            member = next((item for item in group.members if item.target == target), None) if group else None
        if not member:
            warnings.append(f"{target} (no active AirPlay session)")
            continue
        try:
            member.set_volume(_as_int(raw_volume, 75, 0, 100))
            sent_count += 1
        except Exception as exc:
            warnings.append(f"{target} ({exc})")
    return {"ok": sent_count > 0, "sent_count": sent_count, "warnings": warnings}
