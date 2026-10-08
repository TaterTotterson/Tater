"""One-upstream, many-reader relay for synchronized mixed media playback.

On-demand music URLs often create a new encoder for every HTTP request.  A
mixed transport group must not receive independently produced streams. This
relay opens the source once, progressively spools it to a temporary file, and
gives every reader an identical view of that file.
"""

from __future__ import annotations

import contextlib
import logging
import math
import os
import secrets
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests


logger = logging.getLogger("shared_media_relay")

# At common compressed-audio bitrates, a 64 KiB requests.iter_content read can
# wait several seconds before releasing live audio. Keep relay chunks sub-second.
RELAY_IO_CHUNK_BYTES = 8 * 1024
RELAY_READY_BYTES = 4 * 1024
RELAY_READY_TIMEOUT_SECONDS = 20.0
RELAY_RETENTION_SECONDS = 10 * 60.0
RELAY_MAX_IDLE_SECONDS = 12 * 60 * 60.0


class SharedMediaRelayError(RuntimeError):
    """Raised when a shared relay cannot be created or opened."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _runtime_root() -> Path:
    configured = _text(os.getenv("TATER_RUNTIME_DIR"))
    root = (
        Path(configured).expanduser()
        if configured
        else Path(__file__).resolve().parents[1] / ".runtime"
    )
    path = root.resolve() / "shared_media_relay"
    path.mkdir(parents=True, exist_ok=True)
    return path


class _SharedMediaRelay:
    def __init__(
        self,
        source_url: str,
        *,
        media_type: str,
        filename: str,
        expected_duration_seconds: float = 0.0,
    ) -> None:
        self.id = uuid.uuid4().hex
        self.token = secrets.token_urlsafe(32)
        self.source_url = source_url
        self.media_type = (
            _text(media_type).split(";", 1)[0].strip().lower()
            or "application/octet-stream"
        )
        self.filename = Path(_text(filename) or "media.bin").name
        handle, path = tempfile.mkstemp(
            prefix=f"relay-{self.id[:12]}-",
            suffix=Path(self.filename).suffix or ".bin",
            dir=str(_runtime_root()),
        )
        os.close(handle)
        self.path = Path(path)
        self.created_at = time.time()
        self.last_access_at = self.created_at
        try:
            duration = float(expected_duration_seconds)
        except (TypeError, ValueError):
            duration = 0.0
        self.retention_seconds = min(
            RELAY_MAX_IDLE_SECONDS,
            max(
                RELAY_RETENTION_SECONDS,
                duration + RELAY_RETENTION_SECONDS if math.isfinite(duration) else 0.0,
            ),
        )
        self.completed_at = 0.0
        self.bytes_written = 0
        self.reader_count = 0
        self.complete = False
        self.error = ""
        self.response: Optional[requests.Response] = None
        self.condition = threading.Condition(threading.RLock())
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._copy_source,
            name=f"tater-media-relay-{self.id[:8]}",
            daemon=True,
        )

    def start(self) -> None:
        self.thread.start()

    def _copy_source(self) -> None:
        response: Optional[requests.Response] = None
        try:
            response = requests.get(
                self.source_url,
                headers={
                    "Accept": (
                        self.media_type
                        if self.media_type != "application/octet-stream"
                        else "*/*"
                    ),
                    "User-Agent": "Tater-Shared-Media-Relay/2.0",
                },
                stream=True,
                allow_redirects=True,
                timeout=(10, 60),
            )
            response.raise_for_status()
            with self.condition:
                self.response = response
                response_type = (
                    _text(response.headers.get("Content-Type"))
                    .split(";", 1)[0]
                    .strip()
                    .lower()
                )
                if response_type:
                    self.media_type = response_type
                self.condition.notify_all()

            with self.path.open("wb", buffering=0) as output:
                for chunk in response.iter_content(chunk_size=RELAY_IO_CHUNK_BYTES):
                    if self.stop_event.is_set():
                        break
                    payload = bytes(chunk or b"")
                    if not payload:
                        continue
                    output.write(payload)
                    with self.condition:
                        self.bytes_written += len(payload)
                        self.condition.notify_all()
        except Exception as exc:
            with self.condition:
                if not self.stop_event.is_set():
                    self.error = _text(exc) or exc.__class__.__name__
                self.condition.notify_all()
        finally:
            if response is not None:
                with contextlib.suppress(Exception):
                    response.close()
            with self.condition:
                self.response = None
                self.complete = True
                self.completed_at = time.time()
                self.condition.notify_all()
            logger.info(
                "[shared-media] source closed relay=%s bytes=%s error=%s",
                self.id[:12],
                self.bytes_written,
                self.error or "none",
            )

    def wait_until_ready(
        self,
        *,
        minimum_bytes: int = RELAY_READY_BYTES,
        timeout_s: float = RELAY_READY_TIMEOUT_SECONDS,
    ) -> None:
        required = max(1, int(minimum_bytes or RELAY_READY_BYTES))
        deadline = time.monotonic() + max(
            0.1,
            float(timeout_s or RELAY_READY_TIMEOUT_SECONDS),
        )
        with self.condition:
            while self.bytes_written < required and not self.complete and not self.error:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(timeout=min(0.5, remaining))
            if self.bytes_written > 0:
                return
            detail = self.error or "the source did not produce audio before the startup timeout"
        raise SharedMediaRelayError(f"Shared media source was not ready: {detail}")

    def describe(self) -> Dict[str, Any]:
        with self.condition:
            self.last_access_at = time.time()
            return {
                "path": self.path,
                "media_type": self.media_type,
                "filename": self.filename,
                "complete": self.complete and not self.error,
                "bytes_written": self.bytes_written,
            }

    def open(self) -> Iterator[bytes]:
        with self.condition:
            self.reader_count += 1
            self.last_access_at = time.time()

        def body() -> Iterator[bytes]:
            cursor = 0
            try:
                with self.path.open("rb", buffering=0) as source:
                    while True:
                        with self.condition:
                            while cursor >= self.bytes_written and not self.complete:
                                self.condition.wait(timeout=1.0)
                            available = self.bytes_written - cursor
                            complete = self.complete
                            error = self.error
                        if available > 0:
                            chunk = source.read(min(RELAY_IO_CHUNK_BYTES, available))
                            if chunk:
                                cursor += len(chunk)
                                yield chunk
                                continue
                        if complete:
                            if error and cursor <= 0:
                                raise SharedMediaRelayError(error)
                            return
            finally:
                with self.condition:
                    self.reader_count = max(0, self.reader_count - 1)
                    self.last_access_at = time.time()
                    self.condition.notify_all()

        return body()

    def stop(self) -> None:
        self.stop_event.set()
        with self.condition:
            response = self.response
            self.condition.notify_all()
        if response is not None:
            with contextlib.suppress(Exception):
                response.close()

    def remove_file(self) -> None:
        with contextlib.suppress(OSError):
            self.path.unlink()


_registry_lock = threading.RLock()
_relays: Dict[str, _SharedMediaRelay] = {}


def _prune_relays_locked(*, now: Optional[float] = None) -> None:
    current = float(now if now is not None else time.time())
    stale: list[_SharedMediaRelay] = []
    for relay_id, relay in list(_relays.items()):
        with relay.condition:
            completed_stale = (
                relay.complete
                and relay.reader_count <= 0
                and current - max(relay.completed_at, relay.last_access_at)
                >= relay.retention_seconds
            )
            abandoned = (
                relay.reader_count <= 0
                and current - relay.last_access_at >= RELAY_MAX_IDLE_SECONDS
            )
        if completed_stale or abandoned:
            _relays.pop(relay_id, None)
            stale.append(relay)
    for relay in stale:
        relay.stop()
        relay.remove_file()


def register_shared_media_relay(
    source_url: Any,
    *,
    media_type: Any = "application/octet-stream",
    filename: Any = "media.bin",
    minimum_ready_bytes: int = RELAY_READY_BYTES,
    ready_timeout_s: float = RELAY_READY_TIMEOUT_SECONDS,
    expected_duration_seconds: float = 0.0,
) -> Dict[str, Any]:
    url = _text(source_url)
    if not url.lower().startswith(("http://", "https://")):
        raise SharedMediaRelayError("Shared media relay requires an HTTP source URL.")

    relay = _SharedMediaRelay(
        url,
        media_type=_text(media_type),
        filename=_text(filename),
        expected_duration_seconds=expected_duration_seconds,
    )
    with _registry_lock:
        _prune_relays_locked()
        _relays[relay.id] = relay
    relay.start()
    try:
        relay.wait_until_ready(
            minimum_bytes=minimum_ready_bytes,
            timeout_s=ready_timeout_s,
        )
    except Exception:
        with _registry_lock:
            _relays.pop(relay.id, None)
        relay.stop()
        relay.thread.join(timeout=1.0)
        relay.remove_file()
        raise

    logger.info(
        "[shared-media] source ready relay=%s initial_bytes=%s",
        relay.id[:12],
        relay.bytes_written,
    )
    return {
        "relay_id": relay.id,
        "token": relay.token,
        "media_type": relay.media_type,
        "filename": relay.filename,
        "initial_bytes": relay.bytes_written,
        "complete": relay.complete and not relay.error,
    }


def open_shared_media_relay(
    relay_id: Any,
    token: Any,
) -> tuple[Iterator[bytes], str, str]:
    relay = _authorized_relay(relay_id, token)
    return relay.open(), relay.media_type, relay.filename


def describe_shared_media_relay(relay_id: Any, token: Any) -> Dict[str, Any]:
    return _authorized_relay(relay_id, token).describe()


def _authorized_relay(relay_id: Any, token: Any) -> _SharedMediaRelay:
    clean_id = _text(relay_id)
    clean_token = _text(token)
    with _registry_lock:
        _prune_relays_locked()
        relay = _relays.get(clean_id)
    if relay is None:
        raise SharedMediaRelayError("The shared media relay was not found or expired.")
    if not clean_token or not secrets.compare_digest(relay.token, clean_token):
        raise SharedMediaRelayError("The shared media relay token is invalid.")
    return relay


def shutdown_shared_media_relays() -> None:
    with _registry_lock:
        relays = list(_relays.values())
        _relays.clear()
    for relay in relays:
        relay.stop()
    for relay in relays:
        relay.thread.join(timeout=1.0)
        relay.remove_file()


__all__ = [
    "SharedMediaRelayError",
    "describe_shared_media_relay",
    "open_shared_media_relay",
    "register_shared_media_relay",
    "shutdown_shared_media_relays",
]
