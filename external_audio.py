"""Reusable live external-audio input for synchronized Tater satellites.

Shairport Sync receives classic AirPlay/RAOP audio on Linux or macOS and sends
decoded 44.1 kHz stereo S16LE PCM to this module through standard output.  The
module keeps one shared PCM timeline and exposes it as an open-ended WAV stream
so every selected player starts from the same byte cursor.
"""

from __future__ import annotations

import contextlib
import base64
import logging
import os
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ElementTree
from collections import deque
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional
from urllib.parse import quote, urlencode


logger = logging.getLogger("external_audio")

SAMPLE_RATE = 44_100
CHANNELS = 2
SAMPLE_WIDTH = 2
FRAME_BYTES = CHANNELS * SAMPLE_WIDTH
DEFAULT_BUFFER_SECONDS = 45.0
DEFAULT_PREBUFFER_SECONDS = 3.0
DEFAULT_INPUT_IDLE_SECONDS = 8.0
EXTERNAL_NATIVE_START_LEAD_MS = 2500
PCM_IO_CHUNK_BYTES = 16 * 1024
DEFAULT_ROUTE_MAX_ATTEMPTS = 5
DEFAULT_RECEIVER_MAX_CONSECUTIVE_FAILURES = 3
DEFAULT_RECEIVER_RESTART_DELAYS = (5.0, 15.0)
SHAIRPORT_SYNC_VERSION = "5.2.1"


class ExternalAudioStreamError(RuntimeError):
    """Raised when a requested live stream is missing or no longer valid."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return _text(value).casefold() in {"1", "true", "yes", "on", "enabled"}


def _unique_text(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [part.strip() for part in values.split(",")]
    if not isinstance(values, (list, tuple, set)):
        values = [values]
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = _text(value)
        if token and token.casefold() not in seen:
            seen.add(token.casefold())
            result.append(token)
    return result


def _voice_core_selectors(values: Any) -> list[str]:
    """Return the canonical selectors expected by Voice Core media APIs."""
    selectors: list[str] = []
    for value in _unique_text(values):
        selector = value
        if selector.casefold().startswith("voice_core:"):
            selector = selector[len("voice_core:") :].strip()
        if selector and selector.casefold() not in {
            existing.casefold() for existing in selectors
        }:
            selectors.append(selector)
    return selectors


def _voice_session_owner_map(sessions: Any) -> Dict[str, str]:
    owners: Dict[str, str] = {}
    for raw in list(sessions or []):
        row = raw if isinstance(raw, dict) else {}
        session_id = _text(row.get("session_id"))
        if not session_id:
            continue
        for member in list(row.get("selectors") or []):
            selector = _text(member)
            if selector:
                owners[selector] = session_id
    return owners


def _percentage(value: Any, default: int = 100) -> int:
    try:
        result = int(round(float(value)))
    except (TypeError, ValueError):
        result = int(default)
    return max(0, min(100, result))


def _metadata_volume_percent(metadata: Any, default: int = 100) -> int:
    values = metadata if isinstance(metadata, dict) else {}
    raw = values.get("airplay_volume_percent")
    return _percentage(raw, default) if raw is not None else _percentage(default)


def _runtime_root() -> Path:
    configured = _text(os.getenv("TATER_RUNTIME_DIR"))
    root = Path(configured).expanduser() if configured else Path(__file__).resolve().parent / ".runtime"
    path = root.resolve() / "external_audio"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _receiver_launch_cwd(platform_name: Optional[str] = None) -> Optional[str]:
    """Return the receiver working directory without forcing ``fork`` on macOS.

    CPython can use the safer ``posix_spawn`` launcher on macOS only when
    ``cwd`` is ``None``. Linux keeps the runtime working directory.
    """
    platform = _text(platform_name) or sys.platform
    if platform == "darwin":
        return None
    return str(_runtime_root())


def _wav_stream_header() -> bytes:
    byte_rate = SAMPLE_RATE * FRAME_BYTES
    return b"".join(
        (
            b"RIFF",
            struct.pack("<I", 0xFFFFFFFF),
            b"WAVEfmt ",
            struct.pack(
                "<IHHIIHH",
                16,
                1,
                CHANNELS,
                SAMPLE_RATE,
                byte_rate,
                FRAME_BYTES,
                SAMPLE_WIDTH * 8,
            ),
            b"data",
            struct.pack("<I", 0xFFFFFFFF),
        )
    )


class _PcmTimeline:
    """A bounded PCM timeline addressed by monotonically increasing bytes."""

    def __init__(self, seconds: float = DEFAULT_BUFFER_SECONDS) -> None:
        self._max_bytes = max(FRAME_BYTES, int(seconds * SAMPLE_RATE * FRAME_BYTES))
        self._chunks: deque[tuple[int, bytes]] = deque()
        self._start = 0
        self._end = 0
        self._generation = 0
        self._condition = threading.Condition()

    def reset(self) -> int:
        with self._condition:
            self._chunks.clear()
            self._start = 0
            self._end = 0
            self._generation += 1
            self._condition.notify_all()
            return self._generation

    def close(self) -> None:
        with self._condition:
            self._generation += 1
            self._condition.notify_all()

    def write(self, data: bytes) -> int:
        clean_length = len(data) - (len(data) % FRAME_BYTES)
        if clean_length <= 0:
            return self._end
        payload = bytes(data[:clean_length])
        with self._condition:
            offset = self._end
            self._chunks.append((offset, payload))
            self._end += len(payload)
            minimum = max(0, self._end - self._max_bytes)
            while self._chunks and self._chunks[0][0] + len(self._chunks[0][1]) <= minimum:
                chunk_offset, chunk = self._chunks.popleft()
                self._start = chunk_offset + len(chunk)
            if self._chunks and self._start < minimum:
                chunk_offset, chunk = self._chunks.popleft()
                trim = minimum - chunk_offset
                trim -= trim % FRAME_BYTES
                self._chunks.appendleft((chunk_offset + trim, chunk[trim:]))
                self._start = chunk_offset + trim
            elif not self._chunks:
                self._start = self._end
            self._condition.notify_all()
            return self._end

    def snapshot(self, history_seconds: float = 0.0) -> tuple[int, int]:
        history_bytes = max(0, int(history_seconds * SAMPLE_RATE * FRAME_BYTES))
        history_bytes -= history_bytes % FRAME_BYTES
        with self._condition:
            cursor = max(self._start, self._end - history_bytes)
            cursor -= cursor % FRAME_BYTES
            return cursor, self._generation

    def available_bytes(self) -> int:
        with self._condition:
            return max(0, self._end - self._start)

    def read(
        self,
        cursor: int,
        generation: int,
        *,
        maximum: int = 64 * 1024,
        timeout: float = 1.0,
    ) -> tuple[bytes, int]:
        with self._condition:
            deadline = time.monotonic() + max(0.0, timeout)
            while cursor >= self._end and generation == self._generation:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return b"", cursor
                self._condition.wait(remaining)
            if generation != self._generation:
                raise ExternalAudioStreamError("The external audio stream ended.")
            if cursor < self._start:
                raise ExternalAudioStreamError("The external audio client fell behind the live buffer.")
            remaining = max(FRAME_BYTES, maximum - (maximum % FRAME_BYTES))
            parts: list[bytes] = []
            next_cursor = cursor
            for chunk_offset, chunk in self._chunks:
                chunk_end = chunk_offset + len(chunk)
                if chunk_end <= next_cursor:
                    continue
                local_start = max(0, next_cursor - chunk_offset)
                take = min(len(chunk) - local_start, remaining)
                take -= take % FRAME_BYTES
                if take <= 0:
                    break
                parts.append(chunk[local_start : local_start + take])
                next_cursor += take
                remaining -= take
                if remaining < FRAME_BYTES:
                    break
            return b"".join(parts), next_cursor


def _decode_metadata_code(value: Any) -> str:
    token = _text(value)
    try:
        return bytes.fromhex(token).decode("ascii", "replace")
    except (ValueError, TypeError):
        return token


def _metadata_item_values(source: bytes | str) -> Dict[str, str]:
    """Decode one Shairport Sync UDP or XML metadata item into Tater fields."""
    raw = source if isinstance(source, bytes) else source.encode("utf-8", "replace")
    if len(raw) >= 8 and not raw.lstrip().startswith(b"<"):
        raw_type, raw_code = struct.unpack("!II", raw[:8])
        item_type = raw_type.to_bytes(4, "big").decode("ascii", "replace")
        code = raw_code.to_bytes(4, "big").decode("ascii", "replace")
        value = raw[8:].decode("utf-8", "replace").strip("\x00\r\n ")
        if item_type == "ssnc" and code == "pvol":
            fields = [part.strip() for part in value.split(",")]
            try:
                airplay_db = float(fields[0])
            except (IndexError, TypeError, ValueError):
                return {}
            volume_percent = (
                0
                if airplay_db <= -144.0
                else max(0, min(100, round((airplay_db + 30.0) * 100.0 / 30.0)))
            )
            values = {
                "airplay_volume_db": f"{airplay_db:.2f}",
                "airplay_volume_percent": str(volume_percent),
            }
            if len(fields) > 1 and fields[1]:
                values["output_volume_db"] = fields[1][:32]
            return values
        key = {"minm": "title", "asar": "artist", "asal": "album"}.get(code)
        if not key:
            return {}
        return {key: value[:300]} if value else {}
    try:
        item = ElementTree.fromstring(raw)
    except ElementTree.ParseError:
        return {}
    code = _decode_metadata_code(item.findtext("code"))
    key = {"minm": "title", "asar": "artist", "asal": "album"}.get(code)
    data = item.find("data")
    if not key or data is None:
        return {}
    payload = (data.text or "").strip()
    if _text(data.attrib.get("encoding")).casefold() == "base64":
        try:
            value = base64.b64decode(payload, validate=False).decode("utf-8", "replace")
        except (ValueError, TypeError):
            return {}
    else:
        value = payload
    value = value.strip()
    return {key: value[:300]} if value else {}


def _config_string(value: Any) -> str:
    return _text(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _find_shairport_sync(configured: Any = "") -> str:
    candidates = [
        _text(configured),
        _text(os.getenv("TATER_SHAIRPORT_SYNC_PATH")),
        str(
            _runtime_root()
            / f"shairport-sync-v{SHAIRPORT_SYNC_VERSION}"
            / "bin"
            / "shairport-sync"
        ),
        _text(shutil.which("shairport-sync")),
        "/usr/local/bin/shairport-sync",
        "/opt/homebrew/bin/shairport-sync",
        "/usr/bin/shairport-sync",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return str(Path(candidate).resolve())
    return ""


class _ExternalAudioRuntime:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._config: Dict[str, Any] = {
            "enabled": False,
            "receiver_name": "Tater Music",
            "receiver_pin": "",
            "targets": [],
            "target_volume_percent": {},
            "target_sync_offset_ms": {},
            "volume_percent": 75,
            "prebuffer_seconds": DEFAULT_PREBUFFER_SECONDS,
            "input_idle_seconds": DEFAULT_INPUT_IDLE_SECONDS,
            "shairport_sync_path": "",
            "receiver_port": 0,
        }
        self._timeline = _PcmTimeline()
        self._udp_socket: Optional[socket.socket] = None
        self._udp_port = 0
        self._udp_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._receiver: Optional[subprocess.Popen[str]] = None
        self._receiver_path = ""
        self._receiver_started_at = 0.0
        self._receiver_network_port = 0
        self._receiver_restart_at = 0.0
        self._receiver_consecutive_failures = 0
        self._receiver_restart_paused = False
        self._receiver_error = ""
        self._receiver_logs: deque[str] = deque(maxlen=40)
        self._receiver_pcm_handle: Any = None
        self._receiver_log_handle: Any = None
        self._status = "disabled"
        self._metadata: Dict[str, str] = {}
        self._input_active = False
        self._last_audio_at = 0.0
        self._active_session: Dict[str, Any] = {}
        self._route_thread: Optional[threading.Thread] = None
        self._sender_volume_percent = 100
        self._pending_volume_percent: Optional[int] = None
        self._volume_thread: Optional[threading.Thread] = None
        self._chunks_received = 0
        self._bytes_received = 0

    @property
    def receiver_config_path(self) -> Path:
        return _runtime_root() / "shairport-sync.conf"

    def _normalized_config(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        target_volumes = raw.get("target_volume_percent")
        target_offsets = raw.get("target_sync_offset_ms")
        target_transports = raw.get("target_transport_mode")
        pin = "".join(char for char in _text(raw.get("receiver_pin")) if char.isdigit())
        return {
            "enabled": _as_bool(raw.get("enabled"), False),
            "receiver_name": _text(raw.get("receiver_name"))[:80] or "Tater Music",
            "receiver_pin": pin if len(pin) == 4 else "",
            "targets": _unique_text(raw.get("targets")),
            "target_volume_percent": dict(target_volumes) if isinstance(target_volumes, dict) else {},
            "target_sync_offset_ms": dict(target_offsets) if isinstance(target_offsets, dict) else {},
            "target_transport_mode": (
                dict(target_transports) if isinstance(target_transports, dict) else {}
            ),
            "volume_percent": max(0, min(100, int(raw.get("volume_percent") or 75))),
            "prebuffer_seconds": max(
                0.2,
                min(5.0, float(raw.get("prebuffer_seconds") or DEFAULT_PREBUFFER_SECONDS)),
            ),
            "input_idle_seconds": max(
                1.0,
                min(30.0, float(raw.get("input_idle_seconds") or DEFAULT_INPUT_IDLE_SECONDS)),
            ),
            "shairport_sync_path": _text(raw.get("shairport_sync_path")),
            "receiver_port": max(0, min(65524, int(raw.get("receiver_port") or 0))),
        }

    def configure(self, raw: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        next_config = self._normalized_config(raw if isinstance(raw, dict) else {})
        with self._lock:
            previous = self._config
            receiver_port_changed = previous.get("receiver_port") != next_config.get(
                "receiver_port"
            )
            receiver_changed = any(
                previous.get(key) != next_config.get(key)
                for key in (
                    "receiver_name",
                    "receiver_pin",
                    "shairport_sync_path",
                    "receiver_port",
                )
            )
            targets_changed = previous.get("targets") != next_config.get("targets")
            route_changed = any(
                previous.get(key) != next_config.get(key)
                for key in (
                    "targets",
                    "target_volume_percent",
                    "target_sync_offset_ms",
                    "target_transport_mode",
                    "volume_percent",
                )
            )
            self._config = next_config
            if not next_config["enabled"]:
                self._stop_active_session_locked("receiver_disabled")
                self._stop_receiver_locked()
                self._reset_receiver_restart_guard_locked()
                self._status = "disabled"
                return self.status()
            if not previous.get("enabled") or receiver_changed:
                self._reset_receiver_restart_guard_locked()
            self._ensure_service_threads_locked()
            if receiver_changed or targets_changed:
                if self._input_active:
                    self._stop_active_session_locked(
                        "targets_changed" if targets_changed else "receiver_changed"
                    )
                self._stop_receiver_locked()
                if receiver_port_changed:
                    self._receiver_network_port = int(next_config.get("receiver_port") or 0)
            if route_changed and self._input_active:
                self._stop_active_session_locked("targets_changed")
                self._input_active = False
                self._timeline.close()
            self._ensure_receiver_locked()
            return self.status()

    def _ensure_service_threads_locked(self) -> None:
        self._stop_event.clear()
        if self._udp_socket is None:
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp.bind(("127.0.0.1", 0))
            udp.settimeout(0.5)
            self._udp_socket = udp
            self._udp_port = int(udp.getsockname()[1])
        if self._udp_thread is None or not self._udp_thread.is_alive():
            self._udp_thread = threading.Thread(
                target=self._udp_loop,
                name="tater-external-audio-metadata",
                daemon=True,
            )
            self._udp_thread.start()
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name="tater-external-audio-monitor",
                daemon=True,
            )
            self._monitor_thread.start()

    def _receiver_config_text(self) -> str:
        receiver_port = int(
            self._receiver_network_port or self._config.get("receiver_port") or 0
        )
        name = _config_string(self._config.get("receiver_name"))
        pin = _config_string(self._config.get("receiver_pin"))
        password = f'  password = "{pin}";\n' if pin else ""
        return (
            "general = {\n"
            f'  name = "{name}";\n'
            f"{password}"
            '  service_type = "classic";\n'
            '  output_backend = "stdout";\n'
            f"  port = {receiver_port};\n"
            f"  udp_port_base = {receiver_port + 1};\n"
            "  udp_port_range = 10;\n"
            '  ignore_volume_control = "yes";\n'
            "  volume_range_db = 30;\n"
            '  volume_control_profile = "standard";\n'
            "  default_airplay_volume = 0.0;\n"
            "};\n"
            "sessioncontrol = {\n"
            "  session_timeout = 60;\n"
            '  allow_session_interruption = "yes";\n'
            "};\n"
            "stdout = {\n"
            f"  output_rate = {SAMPLE_RATE};\n"
            '  output_format = "S16_LE";\n'
            f"  output_channels = {CHANNELS};\n"
            "};\n"
            "metadata = {\n"
            '  enabled = "yes";\n'
            '  include_cover_art = "no";\n'
            '  socket_address = "127.0.0.1";\n'
            f"  socket_port = {self._udp_port};\n"
            "  socket_msglength = 65000;\n"
            "};\n"
        )

    def _receiver_command(self, binary: str) -> list[str]:
        command = [
            binary,
            "-c",
            str(self.receiver_config_path),
            "--service-type",
            "classic",
            "-o",
            "stdout",
        ]
        receiver_port = int(
            self._receiver_network_port or self._config.get("receiver_port") or 0
        )
        if receiver_port:
            command.extend(("-p", str(receiver_port)))
        if _as_bool(os.getenv("TATER_SHAIRPORT_DEBUG"), False):
            command.extend(("-v", "-v"))
        return command

    @staticmethod
    def _choose_receiver_port() -> int:
        """Reserve-check a TCP RAOP port and Shairport's ten UDP ports."""
        for _attempt in range(80):
            base = 47_000 + secrets.randbelow(14_500)
            sockets: list[socket.socket] = []
            try:
                for port in range(base, base + 11):
                    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp.bind(("0.0.0.0", port))
                    sockets.append(tcp)
                    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    udp.bind(("0.0.0.0", port))
                    sockets.append(udp)
                return base
            except OSError:
                continue
            finally:
                for reserved in sockets:
                    with contextlib.suppress(Exception):
                        reserved.close()
        raise RuntimeError("Could not find available ports for the AirPlay receiver.")

    def _receiver_is_listening_locked(self) -> bool:
        port = int(self._receiver_network_port or 0)
        if port <= 0:
            return False
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(0.15)
        try:
            return probe.connect_ex(("127.0.0.1", port)) == 0
        finally:
            probe.close()

    def _reset_receiver_restart_guard_locked(self) -> None:
        self._receiver_consecutive_failures = 0
        self._receiver_restart_paused = False
        self._receiver_restart_at = 0.0

    def _record_receiver_failure_locked(self, error: Any) -> None:
        self._receiver_consecutive_failures += 1
        attempts = self._receiver_consecutive_failures
        self._status = "error"
        self._receiver_error = _text(error)[:1000]
        if attempts >= DEFAULT_RECEIVER_MAX_CONSECUTIVE_FAILURES:
            self._receiver_restart_paused = True
            self._receiver_restart_at = 0.0
            self._receiver_error = (
                f"{self._receiver_error} Automatic restarts paused after {attempts} "
                "consecutive failures; turn AirPlay off and back on to retry."
            ).strip()
            logger.error("[external-audio] %s", self._receiver_error)
            return
        delay_index = min(attempts - 1, len(DEFAULT_RECEIVER_RESTART_DELAYS) - 1)
        delay = DEFAULT_RECEIVER_RESTART_DELAYS[delay_index]
        self._receiver_restart_at = time.monotonic() + delay
        logger.warning(
            "[external-audio] receiver launch failed (%s/%s); retrying in %.0fs: %s",
            attempts,
            DEFAULT_RECEIVER_MAX_CONSECUTIVE_FAILURES,
            delay,
            self._receiver_error,
        )

    def _ensure_receiver_locked(self) -> None:
        if not self._config["enabled"]:
            return
        if self._receiver is not None and self._receiver.poll() is None:
            if not self._input_active and (
                self._status != "starting" or self._receiver_is_listening_locked()
            ):
                self._status = "ready"
                self._receiver_consecutive_failures = 0
                self._receiver_restart_paused = False
            return
        if self._receiver_restart_paused:
            return
        if time.monotonic() < self._receiver_restart_at:
            return
        binary = _find_shairport_sync(self._config.get("shairport_sync_path"))
        self._receiver_path = binary
        if not binary:
            self._status = "dependency_missing"
            self._receiver_error = (
                "Shairport Sync is not installed. Install or package Shairport Sync "
                "5.2+ and set TATER_SHAIRPORT_SYNC_PATH."
            )
            self._receiver_restart_at = time.monotonic() + 10.0
            return
        configured_port = int(self._config.get("receiver_port") or 0)
        if configured_port:
            self._receiver_network_port = configured_port
        elif not self._receiver_network_port:
            try:
                self._receiver_network_port = self._choose_receiver_port()
            except Exception as exc:
                self._status = "error"
                self._receiver_error = _text(exc)
                self._receiver_restart_at = time.monotonic() + 5.0
                return
        try:
            self.receiver_config_path.write_text(
                self._receiver_config_text(),
                encoding="utf-8",
            )
            self._receiver = subprocess.Popen(
                self._receiver_command(binary),
                cwd=_receiver_launch_cwd(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                bufsize=0,
                close_fds=True,
                start_new_session=False,
            )
        except Exception as exc:
            self._receiver = None
            self._record_receiver_failure_locked(f"Could not start Shairport Sync: {exc}")
            return
        self._receiver_pcm_handle = self._receiver.stdout
        self._receiver_log_handle = self._receiver.stderr
        self._receiver_started_at = time.time()
        self._receiver_error = ""
        self._receiver_restart_at = 0.0
        self._status = "starting"
        process = self._receiver
        threading.Thread(
            target=self._receiver_pcm_loop,
            args=(process, self._receiver_pcm_handle),
            name="tater-shairport-pcm",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._receiver_output_loop,
            args=(process, self._receiver_log_handle),
            name="tater-shairport-log",
            daemon=True,
        ).start()
        logger.info(
            "[external-audio] Shairport Sync receiver started pid=%s port=%s",
            process.pid,
            self._receiver_network_port,
        )

    def _receiver_pcm_loop(self, process: subprocess.Popen[Any], stream: Any) -> None:
        if stream is None:
            return
        remainder = b""
        try:
            while process.poll() is None:
                read_once = getattr(stream, "read1", None)
                chunk = (
                    read_once(PCM_IO_CHUNK_BYTES)
                    if callable(read_once)
                    else stream.read(PCM_IO_CHUNK_BYTES)
                )
                if not chunk:
                    return
                payload = remainder + bytes(chunk)
                clean_length = len(payload) - (len(payload) % FRAME_BYTES)
                if clean_length:
                    self.ingest_pcm(payload[:clean_length])
                remainder = payload[clean_length:]
        except (OSError, ValueError) as exc:
            logger.debug("[external-audio] PCM reader stopped: %s", exc)

    def _receiver_output_loop(self, process: subprocess.Popen[Any], stream: Any) -> None:
        if stream is None:
            return
        try:
            for raw_line in stream:
                line = (
                    raw_line.decode("utf-8", "replace")
                    if isinstance(raw_line, bytes)
                    else _text(raw_line)
                ).strip()
                if not line:
                    continue
                with self._lock:
                    self._receiver_logs.append(line[-1000:])
        except Exception:
            return

    def _stop_receiver_locked(self) -> None:
        process = self._receiver
        self._receiver = None
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
                process.wait(timeout=3.0)
            if process.poll() is None:
                with contextlib.suppress(Exception):
                    process.kill()
        handles = (self._receiver_pcm_handle, self._receiver_log_handle)
        self._receiver_pcm_handle = None
        self._receiver_log_handle = None
        for handle in handles:
            if handle is not None:
                with contextlib.suppress(Exception):
                    handle.close()

    def _udp_loop(self) -> None:
        while not self._stop_event.is_set():
            udp = self._udp_socket
            if udp is None:
                return
            try:
                packet, _address = udp.recvfrom(65_535)
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                values = _metadata_item_values(packet)
                if values:
                    with self._lock:
                        self._metadata.update(values)
                        if "airplay_volume_percent" in values:
                            self._sender_volume_percent = _metadata_volume_percent(values)
                            self._schedule_volume_update_locked(
                                self._sender_volume_percent
                            )
            except Exception as exc:
                logger.debug("[external-audio] ignored metadata item: %s", exc)

    def _schedule_volume_update_locked(self, volume_percent: Any) -> None:
        session = self._active_session
        if not bool(session.get("routed")) or not isinstance(
            session.get("route_result"), dict
        ):
            return
        self._pending_volume_percent = _percentage(volume_percent)
        if self._volume_thread is not None and self._volume_thread.is_alive():
            return
        self._volume_thread = threading.Thread(
            target=self._volume_update_loop,
            name="tater-external-audio-volume",
            daemon=True,
        )
        self._volume_thread.start()

    @staticmethod
    def _native_member_volumes(targets: Any, volume_percent: Any) -> Dict[str, int]:
        from announcement_targets import split_announcement_targets
        from tater_voice import stereo_pairs

        volume = _percentage(volume_percent)
        grouped = split_announcement_targets(targets)
        result: Dict[str, int] = {}
        for selector in list(grouped.get("voice_core_selectors") or []):
            pair = (
                stereo_pairs.get_pair(selector)
                if stereo_pairs.is_stereo_selector(selector)
                else {}
            )
            if isinstance(pair, dict) and pair:
                for side in ("left", "right"):
                    member = _text(pair.get(f"{side}_selector"))
                    balance = _percentage(pair.get(f"{side}_volume_percent"), 100)
                    if member:
                        result[member] = int(round(volume * balance / 100.0))
                continue
            member = _text(selector)
            if member:
                result[member] = volume
        return result

    def _apply_route_volume(
        self,
        session_id: str,
        route_result: Dict[str, Any],
        targets: list[str],
        volume_percent: int,
    ) -> None:
        warnings: list[str] = []
        with self._lock:
            if _text(self._active_session.get("id")) != session_id:
                return
        try:
            from announcement_targets import split_announcement_targets

            grouped = split_announcement_targets(targets)
        except Exception as exc:
            logger.warning("[external-audio] volume target resolution failed: %s", exc)
            return

        sessions = list(route_result.get("voice_core_sessions") or [])
        if sessions:
            try:
                from tater_voice import native_satellite

                native_result = native_satellite.run_on_runtime_loop(
                    native_satellite.set_media_sessions_volume_if_matches(
                        sessions,
                        target_volume_percent=self._native_member_volumes(
                            targets,
                            volume_percent,
                        ),
                        volume_percent=volume_percent,
                    ),
                    timeout=8.0,
                )
                warnings.extend(
                    _text(row.get("error"))
                    for row in list((native_result or {}).get("sessions") or [])
                    if isinstance(row, dict) and _text(row.get("error"))
                )
                updated_native = [
                    f"{_text(row.get('selector'))}={_percentage(row.get('volume_percent'))}%"
                    for row in list((native_result or {}).get("sessions") or [])
                    if isinstance(row, dict) and bool(row.get("ok"))
                ]
                if updated_native:
                    logger.info(
                        "[external-audio] synchronized sender volume %s",
                        ", ".join(updated_native),
                    )
            except Exception as exc:
                warnings.append(str(exc))

        airplay_targets = [
            f"airplay:{player}"
            for player in list(grouped.get("airplay_players") or [])
            if _text(player)
        ]
        airplay_targets.extend(
            _text(target)
            for target in dict(route_result.get("sonos_airplay_routes") or {}).values()
            if _text(target)
        )
        airplay_targets = _unique_text(airplay_targets)
        if airplay_targets:
            try:
                from airplay_bridge import set_airplay_target_volumes

                airplay_result = set_airplay_target_volumes(
                    {
                        target: _percentage(volume_percent)
                        for target in airplay_targets
                    }
                )
                warnings.extend(
                    _text(value)
                    for value in list((airplay_result or {}).get("warnings") or [])
                    if _text(value)
                )
            except Exception as exc:
                warnings.append(str(exc))

        if warnings:
            logger.warning(
                "[external-audio] one or more volume targets did not update: %s",
                "; ".join(_unique_text(warnings)),
            )

    def _volume_update_loop(self) -> None:
        while True:
            with self._lock:
                volume_percent = self._pending_volume_percent
                self._pending_volume_percent = None
                session = self._active_session
                session_id = _text(session.get("id"))
                route_result = (
                    dict(session.get("route_result"))
                    if isinstance(session.get("route_result"), dict)
                    else {}
                )
                targets = list(self._config.get("targets") or [])
                if volume_percent is None or not session_id or not route_result:
                    self._volume_thread = None
                    return
            self._apply_route_volume(
                session_id,
                route_result,
                targets,
                _percentage(volume_percent),
            )

    def ingest_pcm(self, payload: bytes) -> None:
        clean_length = len(payload) - (len(payload) % FRAME_BYTES)
        if clean_length <= 0:
            return
        payload_le = bytes(payload[:clean_length])
        with self._lock:
            now = time.time()
            idle_limit = float(self._config.get("input_idle_seconds") or DEFAULT_INPUT_IDLE_SECONDS)
            if not self._input_active or now - self._last_audio_at > idle_limit:
                if self._input_active:
                    self._stop_active_session_locked("input_restarted")
                generation = self._timeline.reset()
                self._input_active = True
                self._active_session = {
                    "id": uuid.uuid4().hex,
                    "token": secrets.token_urlsafe(24),
                    "generation": generation,
                    "started_at": now,
                    "routed": False,
                    "routing": False,
                    "route_attempts": 0,
                    "route_retry_at": 0.0,
                    "cursor": 0,
                    "route_error": "",
                }
                self._status = "receiving"

            self._timeline.write(payload_le)
            self._last_audio_at = now
            self._chunks_received += 1
            self._bytes_received += len(payload_le)
            self._start_route_if_ready_locked()

    def _start_route_if_ready_locked(self) -> None:
        session = self._active_session
        if not self._input_active or not session or session.get("routing") or session.get("routed"):
            return
        attempts = int(session.get("route_attempts") or 0)
        if attempts >= DEFAULT_ROUTE_MAX_ATTEMPTS:
            return
        if time.time() < float(session.get("route_retry_at") or 0.0):
            return
        targets = list(self._config.get("targets") or [])
        if not targets:
            self._status = "waiting_for_targets"
            return
        required = int(float(self._config["prebuffer_seconds"]) * SAMPLE_RATE * FRAME_BYTES)
        if self._timeline.available_bytes() < required:
            self._status = "buffering"
            return
        cursor, generation = self._timeline.snapshot(history_seconds=self._config["prebuffer_seconds"])
        if generation != session.get("generation"):
            return
        session["cursor"] = cursor
        session["routing"] = True
        session["route_attempts"] = attempts + 1
        self._status = "routing"
        session_id = _text(session.get("id"))
        config = dict(self._config)
        config["targets"] = targets
        config["metadata"] = dict(self._metadata)
        self._route_thread = threading.Thread(
            target=self._route_session,
            args=(session_id, cursor, config),
            name="tater-external-audio-route",
            daemon=True,
        )
        self._route_thread.start()

    def _route_session(self, session_id: str, cursor: int, config: Dict[str, Any]) -> None:
        try:
            from media_playback import play_media_url_targets
            from speech_tts import _service_base_url_for_peer

            with self._lock:
                session = self._active_session
                if _text(session.get("id")) != session_id:
                    return
                token = _text(session.get("token"))
            query = urlencode({"cursor": cursor, "token": token})
            url = (
                f"{_service_base_url_for_peer().rstrip('/')}"
                f"/api/external-audio/v1/streams/{quote(session_id)}/live.wav?{query}"
            )
            metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
            sender_volume = _metadata_volume_percent(
                metadata,
                self._sender_volume_percent,
            )
            targets = list(config.get("targets") or [])
            result = play_media_url_targets(
                targets=targets,
                source_url=url,
                media_type="audio/wav",
                media_content_type="music",
                filename="external-audio-live.wav",
                title=_text(metadata.get("title")) or "AirPlay",
                artist=_text(metadata.get("artist")),
                album=_text(metadata.get("album")),
                volume_percent=sender_volume,
                target_volume_percent={target: sender_volume for target in targets},
                target_sync_offset_ms=dict(config.get("target_sync_offset_ms") or {}),
                target_transport_mode=dict(config.get("target_transport_mode") or {}),
                timeout_s=180.0,
                minimum_native_start_lead_ms=EXTERNAL_NATIVE_START_LEAD_MS,
                source_owner="external_audio",
            )
            if not result.get("ok"):
                raise RuntimeError(_text(result.get("error")) or "Satellite routing failed.")
            route_is_current = False
            with self._lock:
                if _text(self._active_session.get("id")) == session_id:
                    self._active_session["routing"] = False
                    self._active_session["routed"] = True
                    self._active_session["route_retry_at"] = 0.0
                    self._active_session["route_result"] = dict(result)
                    self._status = "playing"
                    self._schedule_volume_update_locked(self._sender_volume_percent)
                    route_is_current = True
            if not route_is_current:
                self._stop_route_result(result, "stale_route")
        except Exception as exc:
            logger.warning("[external-audio] route failed: %s", exc)
            with self._lock:
                if _text(self._active_session.get("id")) == session_id:
                    self._active_session["routing"] = False
                    self._active_session["route_error"] = _text(exc)[:500]
                    attempts = int(self._active_session.get("route_attempts") or 1)
                    retry_delay = min(30.0, float(2 ** min(attempts, 4)))
                    self._active_session["route_retry_at"] = time.time() + retry_delay
                    self._status = "error"

    def release_sessions(self, sessions: Any) -> Dict[str, Any]:
        """Relinquish External Audio sessions replaced by a newer source."""
        released = _voice_session_owner_map(sessions)
        if not released:
            return {"released_selectors": [], **self.status()}
        with self._lock:
            session = self._active_session
            route_result = (
                session.get("route_result")
                if isinstance(session.get("route_result"), dict)
                else {}
            )
            next_sessions: list[Dict[str, Any]] = []
            released_selectors: list[str] = []
            for raw in list(route_result.get("voice_core_sessions") or []):
                row = dict(raw) if isinstance(raw, dict) else {}
                session_id = _text(row.get("session_id"))
                kept: list[str] = []
                for member in list(row.get("selectors") or []):
                    selector = _text(member)
                    if selector and released.get(selector) == session_id:
                        released_selectors.append(selector)
                    elif selector:
                        kept.append(selector)
                if kept:
                    row["selectors"] = kept
                    next_sessions.append(row)
            if route_result:
                route_result = dict(route_result)
                route_result["voice_core_sessions"] = next_sessions
                session["route_result"] = route_result
            session["superseded_selectors"] = _unique_text(
                [
                    *list(session.get("superseded_selectors") or []),
                    *released_selectors,
                ]
            )
            if released_selectors and not _voice_session_owner_map(next_sessions):
                self._status = "receiving"
            status = self.status()
            status["released_selectors"] = released_selectors
            return status

    @staticmethod
    def _stop_route_result(route_result: Dict[str, Any], reason: str) -> None:
        expected_sessions = list(route_result.get("voice_core_sessions") or [])
        airplay_group_id = _text(route_result.get("airplay_bridge_group_id"))
        if expected_sessions:
            with contextlib.suppress(Exception):
                from media_playback import _voice_core_stop_media_sync

                _voice_core_stop_media_sync(
                    [],
                    expected_sessions=expected_sessions,
                    reason=f"external_audio_{reason}",
                )
        if airplay_group_id:
            with contextlib.suppress(Exception):
                from airplay_bridge import stop_airplay_group_sync

                stop_airplay_group_sync(airplay_group_id)

    def _stop_active_session_locked(self, reason: str) -> None:
        session = self._active_session
        route_result = (
            session.get("route_result")
            if isinstance(session.get("route_result"), dict)
            else {}
        )
        expected_sessions = list(route_result.get("voice_core_sessions") or [])
        airplay_group_id = _text(route_result.get("airplay_bridge_group_id"))
        was_routed = bool(session.get("routed") or session.get("routing"))
        self._active_session = {}
        self._pending_volume_percent = None
        self._input_active = False
        self._timeline.close()
        if was_routed and (expected_sessions or airplay_group_id):
            def stop_targets() -> None:
                self._stop_route_result(route_result, reason)

            threading.Thread(
                target=stop_targets,
                name="tater-external-audio-stop",
                daemon=True,
            ).start()
        logger.info("[external-audio] input session stopped reason=%s", reason)

    def stop_input(self) -> Dict[str, Any]:
        with self._lock:
            self._stop_active_session_locked("manual_stop")
            self._status = "ready" if self._receiver and self._receiver.poll() is None else "stopped"
            return self.status()

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(0.5):
            with self._lock:
                if not self._config.get("enabled"):
                    continue
                if (
                    self._input_active
                    and time.time() - self._last_audio_at
                    > float(self._config.get("input_idle_seconds") or DEFAULT_INPUT_IDLE_SECONDS)
                ):
                    self._stop_active_session_locked("input_idle")
                    self._status = "ready" if self._receiver and self._receiver.poll() is None else "stopped"
                process = self._receiver
                if process is not None and process.poll() is not None:
                    code = process.returncode
                    last_log = self._receiver_logs[-1] if self._receiver_logs else ""
                    self._stop_receiver_locked()
                    if not self._config.get("receiver_port"):
                        self._receiver_network_port = 0
                    self._record_receiver_failure_locked(
                        f"Shairport Sync exited with code {code}. {last_log}".strip()
                    )
                self._ensure_receiver_locked()

    def stream(self, session_id: Any, token: Any, cursor: Any) -> Iterator[bytes]:
        with self._lock:
            session = dict(self._active_session)
            if not session or _text(session.get("id")) != _text(session_id):
                raise ExternalAudioStreamError("The external audio stream was not found.")
            if not secrets.compare_digest(_text(session.get("token")), _text(token)):
                raise ExternalAudioStreamError("The external audio stream token is invalid.")
            try:
                clean_cursor = max(0, int(cursor))
            except Exception as exc:
                raise ExternalAudioStreamError("The external audio cursor is invalid.") from exc
            if clean_cursor % FRAME_BYTES:
                raise ExternalAudioStreamError("The external audio cursor is not frame-aligned.")
            generation = int(session.get("generation") or 0)

        def body() -> Iterator[bytes]:
            yield _wav_stream_header()
            next_cursor = clean_cursor
            while True:
                try:
                    chunk, next_cursor = self._timeline.read(
                        next_cursor,
                        generation,
                        maximum=PCM_IO_CHUNK_BYTES,
                        timeout=1.0,
                    )
                except ExternalAudioStreamError:
                    return
                if chunk:
                    yield chunk
                    continue
                with self._lock:
                    if _text(self._active_session.get("id")) != _text(session_id):
                        return

        return body()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            process_running = self._receiver is not None and self._receiver.poll() is None
            session = self._active_session
            return {
                "enabled": bool(self._config.get("enabled")),
                "status": self._status,
                "receiver_name": _text(self._config.get("receiver_name")),
                "receiver_available": bool(self._receiver_path),
                "receiver_running": process_running,
                "receiver_executable": self._receiver_path,
                "receiver_started_at": self._receiver_started_at,
                "receiver_port": self._receiver_network_port,
                "receiver_error": self._receiver_error,
                "receiver_consecutive_failures": self._receiver_consecutive_failures,
                "receiver_restart_paused": self._receiver_restart_paused,
                "metadata_port": self._udp_port,
                "targets": list(self._config.get("targets") or []),
                "input_active": self._input_active,
                "session_id": _text(session.get("id")),
                "session_started_at": float(session.get("started_at") or 0.0),
                "routed": bool(session.get("routed")),
                "routing": bool(session.get("routing")),
                "route_error": _text(session.get("route_error")),
                "superseded_selectors": list(session.get("superseded_selectors") or []),
                "metadata": dict(self._metadata),
                "sender_volume_percent": self._sender_volume_percent,
                "pcm_chunks_received": self._chunks_received,
                "pcm_bytes_received": self._bytes_received,
                "buffered_seconds": round(
                    self._timeline.available_bytes() / float(SAMPLE_RATE * FRAME_BYTES),
                    3,
                ),
                "adapter": "shairport_sync",
                "adapter_version": SHAIRPORT_SYNC_VERSION,
                "adapter_version_required": "5.2+",
            }

    def shutdown(self) -> None:
        with self._lock:
            self._stop_active_session_locked("runtime_shutdown")
            self._stop_receiver_locked()
            self._status = "disabled"
            self._config["enabled"] = False
            self._stop_event.set()
            udp = self._udp_socket
            self._udp_socket = None
        if udp is not None:
            with contextlib.suppress(Exception):
                udp.close()


_runtime = _ExternalAudioRuntime()


def configure_external_audio_runtime(config: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
    values = dict(config) if isinstance(config, dict) else {}
    values.update(kwargs)
    return _runtime.configure(values)


def get_external_audio_status() -> Dict[str, Any]:
    return _runtime.status()


def stream_external_audio_wav(session_id: Any, token: Any, cursor: Any) -> Iterator[bytes]:
    return _runtime.stream(session_id, token, cursor)


def stop_external_audio_input() -> Dict[str, Any]:
    return _runtime.stop_input()


def release_external_audio_sessions(sessions: Any) -> Dict[str, Any]:
    return _runtime.release_sessions(sessions)


def shutdown_external_audio_runtime() -> None:
    _runtime.shutdown()


def build_shairport_sync_config_for_test(
    *, receiver_port: int, metadata_port: int = 5555, **config: Any
) -> str:
    """Build the shared platform-neutral Shairport Sync configuration."""
    runtime = _ExternalAudioRuntime()
    runtime._config = runtime._normalized_config({"enabled": True, **config})
    runtime._receiver_network_port = int(receiver_port)
    runtime._udp_port = int(metadata_port)
    return runtime._receiver_config_text()


def build_shairport_sync_command_for_test(binary: str, *, receiver_port: int = 5000) -> list[str]:
    """Build the Shairport Sync command without starting background threads."""
    runtime = _ExternalAudioRuntime()
    runtime._receiver_network_port = int(receiver_port)
    return runtime._receiver_command(binary)


__all__ = [
    "ExternalAudioStreamError",
    "build_shairport_sync_command_for_test",
    "build_shairport_sync_config_for_test",
    "configure_external_audio_runtime",
    "get_external_audio_status",
    "release_external_audio_sessions",
    "shutdown_external_audio_runtime",
    "stop_external_audio_input",
    "stream_external_audio_wav",
]
