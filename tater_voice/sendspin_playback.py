"""Small Sendspin v1 source for Tater replies, Music Core, and live input.

The native satellites expose the WebSocket player implemented by
``sendspin-cpp`` 0.8.x.  Tater only needs a deliberately small server surface
for handshake, clock replies, timestamped PCM streaming, and clean stream
teardown. Finite replies and music plus open-ended AirPlay input share the
same wire path without keeping the retired ``media.session`` protocol alive.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import inspect
import json
import logging
import os
import shutil
import struct
import subprocess
import sys
import time
import uuid
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import aiohttp

try:
    import audioop as _audioop
except Exception:  # Python 3.13 removed audioop
    _audioop = None


logger = logging.getLogger("sendspin_playback")

SENDSPIN_PORT = 8928
SENDSPIN_PATH = "/sendspin"
SENDSPIN_SAMPLE_RATE = 48_000
SENDSPIN_CHANNELS = 2
SENDSPIN_BIT_DEPTH = 16
SENDSPIN_AUDIO_MESSAGE = 4
SENDSPIN_CHUNK_FRAMES = 960  # 20 ms at 48 kHz
SENDSPIN_START_LEAD_US = 1_000_000
SENDSPIN_BUFFER_AHEAD_US = 800_000
SENDSPIN_HANDSHAKE_TIMEOUT_S = 20.0
SENDSPIN_END_MARGIN_S = 0.35
SENDSPIN_LIVE_INPUT_RATE = 44_100
SENDSPIN_LIVE_READ_BYTES = 16 * 1024
SENDSPIN_AIRPLAY_SAMPLE_RATE = 44_100
SENDSPIN_AIRPLAY_PRIME_SECONDS = 3.0


class SendspinPlaybackError(RuntimeError):
    """Raised when Sendspin audio cannot be delivered to every target."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _monotonic_us() -> int:
    return time.monotonic_ns() // 1_000


def _ffmpeg_binary() -> str:
    bundled = ""
    with contextlib.suppress(Exception):
        import imageio_ffmpeg

        bundled = _text(imageio_ffmpeg.get_ffmpeg_exe())
    candidates = (
        _text(os.getenv("TATER_FFMPEG_PATH") or os.getenv("FFMPEG_PATH")),
        bundled,
        _text(shutil.which("ffmpeg")),
        "/opt/homebrew/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "/usr/bin/ffmpeg",
    )
    for candidate in candidates:
        path = Path(candidate).expanduser() if candidate else None
        if path and path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
    return ""


@dataclass(frozen=True)
class _PreparedPcm:
    pcm: bytes
    frames: int
    duration_s: float


def _prepare_pcm_sync(
    audio_bytes: bytes,
    *,
    preserve_stereo: bool,
    left_delay_ms: int,
    right_delay_ms: int,
    left_volume_percent: int,
    right_volume_percent: int,
) -> _PreparedPcm:
    """Decode an asset and apply pair trim/delay directly to its two channels."""
    source = bytes(audio_bytes or b"")
    if not source:
        raise SendspinPlaybackError("Reply audio is empty.")
    ffmpeg = _ffmpeg_binary()
    if not ffmpeg:
        raise SendspinPlaybackError("ffmpeg is unavailable for Sendspin reply playback.")

    source_channels = 2 if preserve_stereo else 1
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        "pipe:0",
        "-map",
        "0:a:0",
        "-vn",
        "-map_metadata",
        "-1",
        "-ar",
        str(SENDSPIN_SAMPLE_RATE),
        "-ac",
        str(source_channels),
        "-c:a",
        "pcm_s16le",
        "-f",
        "s16le",
        "pipe:1",
    ]
    completed = subprocess.run(
        command,
        input=source,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=90.0,
    )
    decoded = bytes(completed.stdout or b"")
    if completed.returncode != 0 or not decoded:
        detail = bytes(completed.stderr or b"").decode("utf-8", errors="replace").strip()
        raise SendspinPlaybackError(
            detail or f"ffmpeg exited with status {completed.returncode}"
        )

    # An incomplete sample/frame at EOF cannot be scheduled meaningfully.
    frame_bytes = source_channels * 2
    decoded = decoded[: len(decoded) - (len(decoded) % frame_bytes)]
    samples = array("h")
    samples.frombytes(decoded)
    if sys.byteorder != "little":
        samples.byteswap()
    source_frames = len(samples) // source_channels
    if source_frames <= 0:
        raise SendspinPlaybackError("Decoded reply audio contains no PCM frames.")

    left_delay_frames = round(
        _as_int(left_delay_ms, 0, 0, 250) * SENDSPIN_SAMPLE_RATE / 1000
    )
    right_delay_frames = round(
        _as_int(right_delay_ms, 0, 0, 250) * SENDSPIN_SAMPLE_RATE / 1000
    )
    left_gain = _as_int(left_volume_percent, 100, 0, 100)
    right_gain = _as_int(right_volume_percent, 100, 0, 100)
    total_frames = source_frames + max(left_delay_frames, right_delay_frames)
    routed = array("h", [0]) * (total_frames * SENDSPIN_CHANNELS)

    for frame in range(source_frames):
        if source_channels == 1:
            left_sample = right_sample = samples[frame]
        else:
            source_index = frame * 2
            left_sample = samples[source_index]
            right_sample = samples[source_index + 1]
        if left_gain != 100:
            left_sample = int(left_sample * left_gain / 100)
        if right_gain != 100:
            right_sample = int(right_sample * right_gain / 100)
        routed[(frame + left_delay_frames) * 2] = left_sample
        routed[(frame + right_delay_frames) * 2 + 1] = right_sample

    if sys.byteorder != "little":
        routed.byteswap()
    pcm = routed.tobytes()
    return _PreparedPcm(
        pcm=pcm,
        frames=total_frames,
        duration_s=total_frames / float(SENDSPIN_SAMPLE_RATE),
    )


def _audio_packet(timestamp_us: int, pcm: bytes) -> bytes:
    return bytes((SENDSPIN_AUDIO_MESSAGE,)) + struct.pack(">q", int(timestamp_us)) + bytes(pcm)


def _scale_pcm_s16le(pcm: bytes, volume_percent: Any) -> bytes:
    volume = _as_int(volume_percent, 100, 0, 100)
    payload = bytes(pcm or b"")
    if volume >= 100 or not payload:
        return payload
    if _audioop is not None:
        return _audioop.mul(payload, 2, volume / 100.0)
    samples = array("h")
    samples.frombytes(payload)
    if sys.byteorder != "little":
        samples.byteswap()
    for index, sample in enumerate(samples):
        samples[index] = int(sample * volume / 100)
    if sys.byteorder != "little":
        samples.byteswap()
    return samples.tobytes()


class _PcmDelayLine:
    def __init__(self, delay_frames: int) -> None:
        frame_bytes = SENDSPIN_CHANNELS * (SENDSPIN_BIT_DEPTH // 8)
        self.buffer = bytearray(max(0, int(delay_frames)) * frame_bytes)

    def apply(self, pcm: bytes) -> bytes:
        payload = bytes(pcm or b"")
        if not self.buffer:
            return payload
        self.buffer.extend(payload)
        output = bytes(self.buffer[: len(payload)])
        del self.buffer[: len(payload)]
        return output


class _StreamingPcmResampler:
    """Stateful stereo S16LE conversion for a streaming PCM feed."""

    def __init__(
        self,
        input_rate: int = SENDSPIN_LIVE_INPUT_RATE,
        output_rate: int = SENDSPIN_SAMPLE_RATE,
    ) -> None:
        self.input_rate = max(1, int(input_rate))
        self.output_rate = max(1, int(output_rate))
        self._audioop_state: Any = None
        self._fallback_samples = array("h")
        self._fallback_position = 0.0

    def convert(self, pcm: bytes) -> bytes:
        frame_bytes = SENDSPIN_CHANNELS * (SENDSPIN_BIT_DEPTH // 8)
        payload = bytes(pcm or b"")
        payload = payload[: len(payload) - (len(payload) % frame_bytes)]
        if not payload:
            return b""
        if self.input_rate == self.output_rate:
            return payload
        if _audioop is not None:
            converted, self._audioop_state = _audioop.ratecv(
                payload,
                SENDSPIN_BIT_DEPTH // 8,
                SENDSPIN_CHANNELS,
                self.input_rate,
                self.output_rate,
                self._audioop_state,
            )
            return bytes(converted or b"")

        incoming = array("h")
        incoming.frombytes(payload)
        if sys.byteorder != "little":
            incoming.byteswap()
        self._fallback_samples.extend(incoming)
        source_frames = len(self._fallback_samples) // SENDSPIN_CHANNELS
        step = self.input_rate / float(self.output_rate)
        output = array("h")
        while int(self._fallback_position) + 1 < source_frames:
            frame = int(self._fallback_position)
            fraction = self._fallback_position - frame
            first = frame * SENDSPIN_CHANNELS
            second = first + SENDSPIN_CHANNELS
            for channel in range(SENDSPIN_CHANNELS):
                left = self._fallback_samples[first + channel]
                right = self._fallback_samples[second + channel]
                output.append(int(round(left + ((right - left) * fraction))))
            self._fallback_position += step
        consumed_frames = int(self._fallback_position)
        if consumed_frames > 0:
            del self._fallback_samples[: consumed_frames * SENDSPIN_CHANNELS]
            self._fallback_position -= consumed_frames
        if sys.byteorder != "little":
            output.byteswap()
        return output.tobytes()


class _FfmpegPcmReader:
    """Decode one finite HTTP media source into stereo 48 kHz S16LE PCM."""

    def __init__(self, source_url: str, *, start_position_seconds: float = 0.0) -> None:
        source = _text(source_url)
        if not source.lower().startswith(("http://", "https://")):
            raise SendspinPlaybackError("Sendspin music playback requires an HTTP media source.")
        ffmpeg = _ffmpeg_binary()
        if not ffmpeg:
            raise SendspinPlaybackError("ffmpeg is unavailable for Sendspin music playback.")
        command = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin"]
        position = max(0.0, float(start_position_seconds or 0.0))
        if position > 0:
            command.extend(("-ss", f"{position:.3f}"))
        command.extend(
            (
                "-i",
                source,
                "-map",
                "0:a:0",
                "-vn",
                "-sn",
                "-dn",
                "-map_metadata",
                "-1",
                "-ar",
                str(SENDSPIN_SAMPLE_RATE),
                "-ac",
                str(SENDSPIN_CHANNELS),
                "-c:a",
                "pcm_s16le",
                "-f",
                "s16le",
                "pipe:1",
            )
        )
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.closed = False

    def read(self, maximum: int, _timeout: float) -> Optional[bytes]:
        if self.closed:
            return None
        stdout = self.process.stdout
        if stdout is None:
            raise SendspinPlaybackError("ffmpeg did not expose its Sendspin PCM output.")
        chunk = stdout.read(max(4, int(maximum)))
        if chunk:
            return bytes(chunk)
        return_code = self.process.wait()
        if return_code != 0:
            detail = ""
            if self.process.stderr is not None:
                detail = self.process.stderr.read().decode("utf-8", errors="replace").strip()
            raise SendspinPlaybackError(
                detail or f"ffmpeg exited with status {return_code} while decoding Sendspin music."
            )
        return None

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.process.stdout is not None:
            with contextlib.suppress(Exception):
                self.process.stdout.close()
        if self.process.stderr is not None:
            with contextlib.suppress(Exception):
                self.process.stderr.close()
        if self.process.poll() is None:
            with contextlib.suppress(Exception):
                self.process.terminate()
            try:
                self.process.wait(timeout=2.0)
            except Exception:
                with contextlib.suppress(Exception):
                    self.process.kill()
                with contextlib.suppress(Exception):
                    self.process.wait(timeout=1.0)


def _websocket_url(host: str, *, port: int = SENDSPIN_PORT) -> str:
    token = _text(host)
    if not token:
        raise SendspinPlaybackError("A stereo-pair satellite has no reachable host address.")
    with contextlib.suppress(ValueError):
        address = ipaddress.ip_address(token.split("%", 1)[0])
        if address.version == 6:
            token = f"[{token}]"
    if any(character in token for character in ("/", "?", "#", "@")):
        raise SendspinPlaybackError(f"Invalid Sendspin satellite host: {host}")
    return f"ws://{token}:{int(port)}{SENDSPIN_PATH}"


class _SendspinPeer:
    def __init__(self, session: aiohttp.ClientSession, target: Dict[str, Any]) -> None:
        self.session = session
        self.selector = _text(target.get("selector"))
        self.host = _text(target.get("host"))
        self.port = _as_int(target.get("port"), SENDSPIN_PORT, 1, 65535)
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.receiver_task: Optional[asyncio.Task[None]] = None
        self.send_lock = asyncio.Lock()
        self.hello_event = asyncio.Event()
        self.state_event = asyncio.Event()
        self.time_event = asyncio.Event()
        self.client_hello: Dict[str, Any] = {}
        self.client_state = ""
        self.time_responses = 0
        self.error = ""
        self.closing = False

    async def open(self, *, server_id: str, server_name: str) -> None:
        url = _websocket_url(self.host, port=self.port)
        try:
            self.ws = await self.session.ws_connect(
                url,
                heartbeat=15.0,
                autoclose=True,
                autoping=True,
                max_msg_size=2 * 1024 * 1024,
            )
        except Exception as exc:
            raise SendspinPlaybackError(
                f"Could not connect to {self.selector or self.host} Sendspin player: {exc}"
            ) from exc
        self.receiver_task = asyncio.create_task(
            self._receive_loop(),
            name=f"sendspin-recv-{self.selector or self.host}",
        )
        await self.send_json(
            {
                "type": "server/hello",
                "payload": {
                    "server_id": server_id,
                    "name": server_name,
                    "version": 1,
                    "active_roles": ["player@v1"],
                    "connection_reason": "playback",
                },
            }
        )

    async def _receive_loop(self) -> None:
        try:
            assert self.ws is not None
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_json(message.data)
                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.closing:
                self.error = _text(exc) or exc.__class__.__name__
        finally:
            if not self.closing and not self.error:
                self.error = "the satellite closed its Sendspin connection"
            # Unblock startup immediately; wait_ready() reports the stored error.
            self.hello_event.set()
            self.state_event.set()
            self.time_event.set()

    async def _handle_json(self, raw_message: str) -> None:
        received_us = _monotonic_us()
        try:
            message = json.loads(raw_message)
        except Exception:
            return
        if not isinstance(message, dict):
            return
        message_type = _text(message.get("type"))
        payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
        if message_type == "client/hello":
            self.client_hello = dict(payload)
            self.hello_event.set()
            return
        if message_type == "client/state":
            self.client_state = _text(payload.get("state")).lower()
            if self.client_state == "synchronized":
                self.state_event.set()
            return
        if message_type == "client/time":
            try:
                client_transmitted = int(payload.get("client_transmitted"))
            except Exception:
                return
            transmitted_us = _monotonic_us()
            await self.send_json(
                {
                    "type": "server/time",
                    "payload": {
                        "client_transmitted": client_transmitted,
                        "server_received": received_us,
                        "server_transmitted": transmitted_us,
                    },
                }
            )
            self.time_responses += 1
            if self.time_responses >= 8:
                self.time_event.set()
            return
        if message_type == "client/goodbye":
            reason = _text(payload.get("reason")) or "unknown"
            self.error = f"the satellite left Sendspin ({reason})"
            self.hello_event.set()
            self.state_event.set()
            self.time_event.set()

    def _validate_hello(self) -> None:
        payload = self.client_hello
        if not payload:
            raise SendspinPlaybackError(
                f"{self.selector or self.host} did not send a Sendspin hello."
            )
        try:
            version = int(payload.get("version") or 0)
        except Exception:
            version = 0
        roles = {_text(value) for value in list(payload.get("supported_roles") or [])}
        support = (
            payload.get("player@v1_support")
            if isinstance(payload.get("player@v1_support"), dict)
            else {}
        )
        formats = support.get("supported_formats") if isinstance(support.get("supported_formats"), list) else []
        pcm_supported = any(
            isinstance(row, dict)
            and _text(row.get("codec")).lower() == "pcm"
            and _as_int(row.get("channels"), 0, 0, 255) == SENDSPIN_CHANNELS
            and _as_int(row.get("sample_rate"), 0, 0, 384_000) == SENDSPIN_SAMPLE_RATE
            and _as_int(row.get("bit_depth"), 0, 0, 64) == SENDSPIN_BIT_DEPTH
            for row in formats
        )
        if version != 1 or "player@v1" not in roles or not pcm_supported:
            raise SendspinPlaybackError(
                f"{self.selector or self.host} does not support Sendspin v1 PCM 48 kHz stereo."
            )

    async def wait_ready(self) -> None:
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    self.hello_event.wait(),
                    self.state_event.wait(),
                    self.time_event.wait(),
                ),
                timeout=SENDSPIN_HANDSHAKE_TIMEOUT_S,
            )
        except asyncio.TimeoutError as exc:
            raise SendspinPlaybackError(
                f"Timed out preparing {self.selector or self.host} for synchronized Sendspin playback."
            ) from exc
        if self.error:
            raise SendspinPlaybackError(f"{self.selector or self.host}: {self.error}")
        self._validate_hello()
        if self.client_state != "synchronized":
            raise SendspinPlaybackError(
                f"{self.selector or self.host} is busy with native audio."
            )

    async def send_json(self, message: Dict[str, Any]) -> None:
        if self.ws is None or self.ws.closed:
            raise SendspinPlaybackError(
                f"{self.selector or self.host} Sendspin connection is closed."
            )
        payload = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
        async with self.send_lock:
            await self.ws.send_str(payload)

    async def send_audio(self, timestamp_us: int, pcm: bytes) -> None:
        if self.ws is None or self.ws.closed:
            raise SendspinPlaybackError(
                f"{self.selector or self.host} Sendspin connection is closed."
            )
        packet = _audio_packet(timestamp_us, pcm)
        async with self.send_lock:
            await self.ws.send_bytes(packet)

    async def close(self) -> None:
        self.closing = True
        if self.ws is not None and not self.ws.closed:
            with contextlib.suppress(Exception):
                await self.ws.close()
        if self.receiver_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.receiver_task


async def _broadcast_json(peers: Iterable[_SendspinPeer], message: Dict[str, Any]) -> None:
    await asyncio.gather(*(peer.send_json(message) for peer in peers))


async def _broadcast_audio(
    peers: Iterable[_SendspinPeer], timestamp_us: int, pcm: bytes
) -> None:
    await asyncio.gather(*(peer.send_audio(timestamp_us, pcm) for peer in peers))


async def _stream_pair_pcm(
    targets: list[Dict[str, Any]],
    prepared: _PreparedPcm,
    *,
    pair_id: str,
    pair_name: str,
    started_event: asyncio.Event,
) -> Dict[str, Any]:
    if len(targets) != 2:
        raise SendspinPlaybackError("A Sendspin stereo reply requires exactly two satellites.")
    timeout = aiohttp.ClientTimeout(total=None, connect=6.0, sock_connect=6.0, sock_read=None)
    server_id = "tater-voice-core"
    peers: list[_SendspinPeer] = []
    stream_started = False
    async with aiohttp.ClientSession(timeout=timeout) as session:
        peers = [_SendspinPeer(session, target) for target in targets]
        try:
            await asyncio.gather(
                *(peer.open(server_id=server_id, server_name="Tater Voice Core") for peer in peers)
            )
            await asyncio.gather(*(peer.wait_ready() for peer in peers))
            # Give each ESP loop one turn after its eighth clock response so the
            # selected measurement is committed before timestamped audio arrives.
            await asyncio.sleep(0.1)

            group_id = _text(pair_id) or uuid.uuid4().hex[:12]
            await _broadcast_json(
                peers,
                {
                    "type": "group/update",
                    "payload": {
                        "playback_state": "playing",
                        "group_id": group_id,
                        "group_name": _text(pair_name) or "Tater Stereo Reply",
                    },
                },
            )
            await _broadcast_json(
                peers,
                {
                    "type": "stream/start",
                    "payload": {
                        "player": {
                            "codec": "pcm",
                            "sample_rate": SENDSPIN_SAMPLE_RATE,
                            "channels": SENDSPIN_CHANNELS,
                            "bit_depth": SENDSPIN_BIT_DEPTH,
                        }
                    },
                },
            )
            stream_started = True

            bytes_per_frame = SENDSPIN_CHANNELS * (SENDSPIN_BIT_DEPTH // 8)
            chunk_bytes = SENDSPIN_CHUNK_FRAMES * bytes_per_frame
            start_us = _monotonic_us() + SENDSPIN_START_LEAD_US
            first_packet = True
            for byte_offset in range(0, len(prepared.pcm), chunk_bytes):
                frame_offset = byte_offset // bytes_per_frame
                timestamp_us = start_us + round(
                    frame_offset * 1_000_000 / SENDSPIN_SAMPLE_RATE
                )
                send_at_us = timestamp_us - SENDSPIN_BUFFER_AHEAD_US
                delay_s = (send_at_us - _monotonic_us()) / 1_000_000.0
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                await _broadcast_audio(
                    peers,
                    timestamp_us,
                    prepared.pcm[byte_offset : byte_offset + chunk_bytes],
                )
                if first_packet:
                    first_packet = False
                    started_event.set()

            audible_end_us = start_us + round(
                prepared.frames * 1_000_000 / SENDSPIN_SAMPLE_RATE
            )
            remaining_s = (audible_end_us - _monotonic_us()) / 1_000_000.0
            if remaining_s > 0:
                await asyncio.sleep(remaining_s)
            await asyncio.sleep(SENDSPIN_END_MARGIN_S)
            await _broadcast_json(
                peers,
                {"type": "stream/end", "payload": {"roles": ["player"]}},
            )
            stream_started = False
            await _broadcast_json(
                peers,
                {
                    "type": "group/update",
                    "payload": {
                        "playback_state": "stopped",
                        "group_id": group_id,
                        "group_name": _text(pair_name) or "Tater Stereo Reply",
                    },
                },
            )
            selectors = [peer.selector for peer in peers]
            return {
                "ok": True,
                "sendspin_playback_started": True,
                "playback_completed": True,
                "group_id": group_id,
                "members": selectors,
                "duration_s": round(prepared.duration_s, 3),
                "sample_rate_hz": SENDSPIN_SAMPLE_RATE,
                "channels": SENDSPIN_CHANNELS,
            }
        finally:
            if stream_started:
                with contextlib.suppress(Exception):
                    await _broadcast_json(
                        peers,
                        {"type": "stream/end", "payload": {"roles": ["player"]}},
                    )
            started_event.set()
            await asyncio.gather(*(peer.close() for peer in peers), return_exceptions=True)


@dataclass
class _LiveStreamState:
    stream_id: str
    targets: list[Dict[str, Any]]
    started_event: asyncio.Event
    volume_percent: Dict[str, int]
    airplay_group_id: str = ""
    airplay_targets: tuple[str, ...] = ()
    airplay_sync_offset_ms: Optional[Dict[str, Any]] = None
    airplay_reference_sync_offset_ms: int = 0
    airplay_prepare_result: Optional[Dict[str, Any]] = None
    airplay_commit_result: Optional[Dict[str, Any]] = None
    on_finished: Optional[Callable[[str, str], Any]] = None
    task: Optional[asyncio.Task[Dict[str, Any]]] = None
    start_server_us: int = 0
    start_unix_ms: int = 0


_active_live_streams: Dict[str, _LiveStreamState] = {}
_live_target_owners: Dict[str, str] = {}


async def _stream_live_pcm(
    state: _LiveStreamState,
    read_pcm: Callable[[int, float], Optional[bytes]],
    *,
    group_name: str,
    input_sample_rate: int,
    start_lead_us: int,
    close_pcm: Optional[Callable[[], Any]] = None,
) -> Dict[str, Any]:
    targets = list(state.targets)
    airplay_group_id = _text(state.airplay_group_id)
    if not targets and not airplay_group_id:
        raise SendspinPlaybackError(
            "A live Sendspin stream requires at least one native or AirPlay player."
        )
    timeout = aiohttp.ClientTimeout(total=None, connect=6.0, sock_connect=6.0, sock_read=None)
    peers: list[_SendspinPeer] = []
    stream_started = False
    airplay_commit_task: Optional[asyncio.Task[Dict[str, Any]]] = None
    airplay_commit_result: Dict[str, Any] = {}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        peers = [_SendspinPeer(session, target) for target in targets]
        try:
            if peers:
                await asyncio.gather(
                    *(peer.open(server_id="tater-sendspin", server_name="Tater") for peer in peers)
                )
                await asyncio.gather(*(peer.wait_ready() for peer in peers))
                await asyncio.sleep(0.1)
                await _broadcast_json(
                    peers,
                    {
                        "type": "group/update",
                        "payload": {
                            "playback_state": "playing",
                            "group_id": state.stream_id,
                            "group_name": _text(group_name) or "Tater Audio",
                        },
                    },
                )
                await _broadcast_json(
                    peers,
                    {
                        "type": "stream/start",
                        "payload": {
                            "player": {
                                "codec": "pcm",
                                "sample_rate": SENDSPIN_SAMPLE_RATE,
                                "channels": SENDSPIN_CHANNELS,
                                "bit_depth": SENDSPIN_BIT_DEPTH,
                            }
                        },
                    },
                )
            stream_started = True

            frame_bytes = SENDSPIN_CHANNELS * (SENDSPIN_BIT_DEPTH // 8)
            chunk_bytes = SENDSPIN_CHUNK_FRAMES * frame_bytes
            pending = bytearray()
            resampler = _StreamingPcmResampler(input_sample_rate)
            airplay_resampler = _StreamingPcmResampler(
                SENDSPIN_SAMPLE_RATE,
                SENDSPIN_AIRPLAY_SAMPLE_RATE,
            )
            delay_lines = {
                peer.selector: _PcmDelayLine(
                    round(
                        _as_int(
                            next(
                                (
                                    target.get("delay_ms")
                                    for target in targets
                                    if _text(target.get("selector")) == peer.selector
                                ),
                                0,
                            ),
                            0,
                            0,
                            2000,
                        )
                        * SENDSPIN_SAMPLE_RATE
                        / 1000
                    )
                )
                for peer in peers
            }
            frames_sent = 0
            source_finished = False

            async def next_chunk() -> Optional[bytes]:
                nonlocal source_finished
                while len(pending) < chunk_bytes and not source_finished:
                    source = await asyncio.to_thread(
                        read_pcm,
                        SENDSPIN_LIVE_READ_BYTES,
                        0.5,
                    )
                    if source is None:
                        source_finished = True
                        break
                    if not source:
                        continue
                    pending.extend(resampler.convert(source))
                if source_finished and not pending:
                    return None
                if len(pending) < chunk_bytes:
                    pending.extend(b"\x00" * (chunk_bytes - len(pending)))
                chunk = bytes(pending[:chunk_bytes])
                del pending[:chunk_bytes]
                return chunk

            primed_chunks: list[bytes] = []
            airplay_ready: Dict[str, Any] = {}
            if airplay_group_id:
                from airplay_bridge import (
                    start_sendspin_airplay_bridge,
                    wait_sendspin_airplay_bridge_ready,
                    write_sendspin_airplay_bridge_pcm,
                )

                prime_frames = round(
                    SENDSPIN_AIRPLAY_PRIME_SECONDS * SENDSPIN_SAMPLE_RATE
                )
                while len(primed_chunks) * SENDSPIN_CHUNK_FRAMES < prime_frames:
                    chunk = await next_chunk()
                    if chunk is None:
                        break
                    primed_chunks.append(chunk)
                    airplay_pcm = airplay_resampler.convert(chunk)
                    bridge_write = await asyncio.to_thread(
                        write_sendspin_airplay_bridge_pcm,
                        airplay_group_id,
                        airplay_pcm,
                    )
                    if not bridge_write.get("ok"):
                        raise SendspinPlaybackError(
                            _text(bridge_write.get("error"))
                            or "An AirPlay bridge rejected Sendspin audio."
                        )
                if not primed_chunks:
                    raise SendspinPlaybackError(
                        "The Sendspin media source contained no audio."
                    )
                airplay_ready = await asyncio.to_thread(
                    wait_sendspin_airplay_bridge_ready,
                    airplay_group_id,
                    timeout_s=SENDSPIN_HANDSHAKE_TIMEOUT_S,
                )
                if not airplay_ready.get("ok"):
                    raise SendspinPlaybackError(
                        _text(airplay_ready.get("error"))
                        or "The AirPlay Sendspin bridge was not ready."
                    )

            monotonic_now_us = _monotonic_us()
            unix_now_us = time.time_ns() // 1_000
            requested_start_unix_ms = (
                unix_now_us + max(250_000, int(start_lead_us))
            ) // 1_000
            requested_start_unix_ms = max(
                requested_start_unix_ms,
                int(airplay_ready.get("minimum_start_unix_ms") or 0),
            )
            start_us = monotonic_now_us + max(
                250_000,
                requested_start_unix_ms * 1_000 - unix_now_us,
            )
            state.start_server_us = start_us
            state.start_unix_ms = (
                unix_now_us + (start_us - monotonic_now_us)
            ) // 1_000

            if airplay_group_id:
                airplay_commit_task = asyncio.create_task(
                    asyncio.to_thread(
                        start_sendspin_airplay_bridge,
                        group_id=airplay_group_id,
                        start_unix_ms=state.start_unix_ms,
                        reference_sync_offset_ms=state.airplay_reference_sync_offset_ms,
                        target_sync_offset_ms=state.airplay_sync_offset_ms,
                    ),
                    name=f"sendspin-airplay-commit-{state.stream_id}",
                )

            buffer_ahead_us = (
                max(SENDSPIN_BUFFER_AHEAD_US, 2_000_000)
                if airplay_group_id
                else SENDSPIN_BUFFER_AHEAD_US
            )

            async def publish_chunk(chunk: bytes, *, airplay_already_written: bool) -> None:
                nonlocal frames_sent
                timestamp_us = start_us + round(
                    frames_sent * 1_000_000 / SENDSPIN_SAMPLE_RATE
                )
                send_at_us = timestamp_us - buffer_ahead_us
                delay_s = (send_at_us - _monotonic_us()) / 1_000_000.0
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                if airplay_group_id and not airplay_already_written:
                    bridge_write = await asyncio.to_thread(
                        write_sendspin_airplay_bridge_pcm,
                        airplay_group_id,
                        airplay_resampler.convert(chunk),
                    )
                    if not bridge_write.get("ok"):
                        raise SendspinPlaybackError(
                            _text(bridge_write.get("error"))
                            or "An AirPlay bridge rejected Sendspin audio."
                        )
                if peers:
                    await asyncio.gather(
                        *(
                            peer.send_audio(
                                timestamp_us,
                                _scale_pcm_s16le(
                                    delay_lines[peer.selector].apply(chunk),
                                    state.volume_percent.get(peer.selector, 100),
                                ),
                            )
                            for peer in peers
                        )
                    )
                frames_sent += SENDSPIN_CHUNK_FRAMES

            for chunk in primed_chunks:
                await publish_chunk(chunk, airplay_already_written=True)
                if airplay_commit_task is not None and airplay_commit_task.done():
                    airplay_commit_result = airplay_commit_task.result()
                    if not airplay_commit_result.get("ok"):
                        raise SendspinPlaybackError(
                            _text(airplay_commit_result.get("error"))
                            or "The AirPlay Sendspin bridge did not start."
                        )
                    state.airplay_commit_result = dict(airplay_commit_result)
                    state.started_event.set()

            while True:
                chunk = await next_chunk()
                if chunk is None:
                    if frames_sent <= 0:
                        raise SendspinPlaybackError("The Sendspin media source contained no audio.")
                    break
                await publish_chunk(chunk, airplay_already_written=False)
                if airplay_commit_task is None:
                    state.started_event.set()
                elif airplay_commit_task.done() and not state.started_event.is_set():
                    airplay_commit_result = airplay_commit_task.result()
                    if not airplay_commit_result.get("ok"):
                        raise SendspinPlaybackError(
                            _text(airplay_commit_result.get("error"))
                            or "The AirPlay Sendspin bridge did not start."
                        )
                    state.airplay_commit_result = dict(airplay_commit_result)
                    state.started_event.set()

            if airplay_commit_task is not None:
                airplay_commit_result = await airplay_commit_task
                if not airplay_commit_result.get("ok"):
                    raise SendspinPlaybackError(
                        _text(airplay_commit_result.get("error"))
                        or "The AirPlay Sendspin bridge did not start."
                    )
                state.airplay_commit_result = dict(airplay_commit_result)
                state.started_event.set()
            audible_end_us = start_us + round(
                frames_sent * 1_000_000 / SENDSPIN_SAMPLE_RATE
            )
            remaining_s = (audible_end_us - _monotonic_us()) / 1_000_000.0
            if remaining_s > 0:
                await asyncio.sleep(remaining_s)
            await asyncio.sleep(SENDSPIN_END_MARGIN_S)
            result: Dict[str, Any] = {
                "ok": True,
                "sendspin_live_stream_started": True,
                "playback_completed": source_finished,
                "stream_id": state.stream_id,
                "group_id": state.stream_id,
                "members": [
                    *[peer.selector for peer in peers],
                    *list(state.airplay_targets),
                ],
                "duration_s": round(frames_sent / float(SENDSPIN_SAMPLE_RATE), 3),
                "sample_rate_hz": SENDSPIN_SAMPLE_RATE,
                "channels": SENDSPIN_CHANNELS,
            }
            if airplay_group_id:
                result.update(
                    {
                        "airplay_bridge_group_id": airplay_group_id,
                        "airplay_bridge_sent_count": int(
                            airplay_commit_result.get("sent_count") or 0
                        ),
                        "airplay_bridge_prepared_count": len(
                            state.airplay_targets
                        ),
                        "airplay_bridge_start_unix_ms": int(
                            airplay_commit_result.get("start_unix_ms")
                            or state.start_unix_ms
                        ),
                        "airplay_bridge_timing_mode": _text(
                            airplay_commit_result.get("timing_mode")
                        ),
                        "airplay_bridge_routes": dict(
                            (state.airplay_prepare_result or {}).get("routes") or {}
                        ),
                    }
                )
                bridge_warnings = [
                    _text(item)
                    for item in [
                        *list((state.airplay_prepare_result or {}).get("warnings") or []),
                        *list(airplay_commit_result.get("warnings") or []),
                    ]
                    if _text(item)
                ]
                if bridge_warnings:
                    result["warnings"] = bridge_warnings
            return result
        finally:
            if stream_started:
                with contextlib.suppress(Exception):
                    await _broadcast_json(
                        peers,
                        {"type": "stream/end", "payload": {"roles": ["player"]}},
                    )
                with contextlib.suppress(Exception):
                    await _broadcast_json(
                        peers,
                        {
                            "type": "group/update",
                            "payload": {
                                "playback_state": "stopped",
                                "group_id": state.stream_id,
                                "group_name": _text(group_name) or "Tater Audio",
                            },
                        },
                    )
            state.started_event.set()
            await asyncio.gather(*(peer.close() for peer in peers), return_exceptions=True)
            if airplay_group_id:
                with contextlib.suppress(Exception):
                    from airplay_bridge import stop_airplay_group_sync

                    await asyncio.to_thread(
                        stop_airplay_group_sync,
                        airplay_group_id,
                    )
            if callable(close_pcm):
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(close_pcm)


async def _stop_live_streams_for_targets(selectors: Iterable[Any]) -> None:
    await stop_live_streams_for_targets(selectors)


async def stop_live_streams_for_targets(selectors: Iterable[Any]) -> Dict[str, Any]:
    """Stop every active Sendspin stream that owns one of the given members."""
    stream_ids = {
        _live_target_owners.get(_text(selector), "")
        for selector in selectors
        if _text(selector)
    }
    active_ids = sorted(stream_id for stream_id in stream_ids if stream_id)
    for stream_id in active_ids:
        await stop_live_stream(stream_id)
    return {"ok": True, "stopped_count": len(active_ids), "stream_ids": active_ids}


async def start_live_pcm_stream(
    stream_id: str,
    targets: list[Dict[str, Any]],
    read_pcm: Callable[[int, float], Optional[bytes]],
    *,
    group_name: str = "Tater AirPlay",
    input_sample_rate: int = SENDSPIN_LIVE_INPUT_RATE,
    start_lead_ms: int = 1000,
    airplay_targets: Iterable[Any] | None = None,
    airplay_volume_percent: Dict[str, Any] | None = None,
    airplay_sync_offset_ms: Dict[str, Any] | None = None,
    airplay_reference_sync_offset_ms: int = 0,
    title: str = "",
    artist: str = "",
    album: str = "",
    duration_seconds: float = 0.0,
    on_finished: Optional[Callable[[str, str], Any]] = None,
    close_pcm: Optional[Callable[[], Any]] = None,
) -> Dict[str, Any]:
    """Start one PCM timeline; a ``None`` read marks a finite source's end."""
    clean_id = _text(stream_id)
    clean_targets = [dict(target) for target in targets if isinstance(target, dict)]
    from airplay_bridge import airplay_target_value

    clean_airplay_targets = list(
        dict.fromkeys(
            airplay_target_value(target)
            for target in list(airplay_targets or [])
            if airplay_target_value(target)
        )
    )
    if not clean_id:
        raise SendspinPlaybackError("A live Sendspin stream id is required.")
    if not clean_targets and not clean_airplay_targets:
        raise SendspinPlaybackError("No native or AirPlay Sendspin players were selected.")
    if not callable(read_pcm):
        raise SendspinPlaybackError("The live Sendspin PCM reader is unavailable.")

    owned_targets = [
        *(
            _text(target.get("selector"))
            for target in clean_targets
            if _text(target.get("selector"))
        ),
        *clean_airplay_targets,
    ]
    await _stop_live_streams_for_targets(owned_targets)
    if clean_id in _active_live_streams:
        await stop_live_stream(clean_id)

    volumes = {
        _text(target.get("selector")): _as_int(
            target.get("volume_percent"),
            100,
            0,
            100,
        )
        for target in clean_targets
        if _text(target.get("selector"))
    }
    airplay_volumes = {
        target: _as_int(
            dict(airplay_volume_percent or {}).get(target),
            100,
            0,
            100,
        )
        for target in clean_airplay_targets
    }
    airplay_prepare_result: Dict[str, Any] = {}
    airplay_group_id = ""
    if clean_airplay_targets:
        from airplay_bridge import prepare_sendspin_airplay_bridge

        airplay_prepare_result = await asyncio.to_thread(
            prepare_sendspin_airplay_bridge,
            targets=clean_airplay_targets,
            volume_percent=100,
            target_volume_percent=airplay_volumes,
            title=_text(title) or _text(group_name) or "Tater Music",
            artist=_text(artist) or "Tater",
            album=_text(album) or "Tater Music",
            duration_seconds=max(0.0, float(duration_seconds or 0.0)),
            pcm_sample_rate=SENDSPIN_AIRPLAY_SAMPLE_RATE,
            timeout_s=SENDSPIN_HANDSHAKE_TIMEOUT_S,
        )
        if not airplay_prepare_result.get("ok"):
            raise SendspinPlaybackError(
                _text(airplay_prepare_result.get("error"))
                or "The AirPlay Sendspin bridge could not be prepared."
            )
        airplay_group_id = _text(airplay_prepare_result.get("group_id"))
        clean_airplay_targets = [
            _text(target)
            for target in list(airplay_prepare_result.get("prepared_targets") or [])
            if _text(target)
        ]
        if not airplay_group_id or not clean_airplay_targets:
            if airplay_group_id:
                from airplay_bridge import stop_airplay_group_sync

                await asyncio.to_thread(stop_airplay_group_sync, airplay_group_id)
            raise SendspinPlaybackError(
                "The AirPlay Sendspin bridge returned no prepared players."
            )
        owned_targets = [
            *(
                _text(target.get("selector"))
                for target in clean_targets
                if _text(target.get("selector"))
            ),
            *clean_airplay_targets,
        ]
    state = _LiveStreamState(
        stream_id=clean_id,
        targets=clean_targets,
        started_event=asyncio.Event(),
        volume_percent=volumes,
        airplay_group_id=airplay_group_id,
        airplay_targets=tuple(clean_airplay_targets),
        airplay_sync_offset_ms=dict(airplay_sync_offset_ms or {}),
        airplay_reference_sync_offset_ms=_as_int(
            airplay_reference_sync_offset_ms,
            0,
            -1000,
            1000,
        ),
        airplay_prepare_result=dict(airplay_prepare_result),
        on_finished=on_finished,
    )
    task = asyncio.create_task(
        _stream_live_pcm(
            state,
            read_pcm,
            group_name=group_name,
            input_sample_rate=max(1, int(input_sample_rate)),
            start_lead_us=_as_int(start_lead_ms, 1000, 250, 5000) * 1000,
            close_pcm=close_pcm,
        ),
        name=f"sendspin-live-{clean_id}",
    )
    state.task = task
    _active_live_streams[clean_id] = state
    for selector in owned_targets:
        _live_target_owners[selector] = clean_id

    def _finished(finished: asyncio.Task[Dict[str, Any]]) -> None:
        if _active_live_streams.get(clean_id) is state:
            _active_live_streams.pop(clean_id, None)
        for selector in owned_targets:
            if _live_target_owners.get(selector) == clean_id:
                _live_target_owners.pop(selector, None)
        error = ""
        if not finished.cancelled():
            failure = finished.exception()
            if failure is not None:
                error = _text(failure) or failure.__class__.__name__
                logger.warning(
                    "[sendspin] live stream ended with an error stream=%s error=%s",
                    clean_id,
                    error,
                )
        callback = state.on_finished
        if callable(callback):
            with contextlib.suppress(Exception):
                callback_result = callback(clean_id, error)
                if inspect.isawaitable(callback_result):
                    asyncio.create_task(callback_result)

    task.add_done_callback(_finished)
    try:
        await asyncio.wait_for(
            state.started_event.wait(),
            timeout=SENDSPIN_HANDSHAKE_TIMEOUT_S + 10.0,
        )
    except asyncio.TimeoutError as exc:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        raise SendspinPlaybackError("Timed out starting the live Sendspin stream.") from exc
    await asyncio.sleep(0)
    if task.done():
        return task.result()
    result: Dict[str, Any] = {
        "ok": True,
        "sendspin_live_stream_started": True,
        "playback_completed": False,
        "stream_id": clean_id,
        "group_id": clean_id,
        "members": [*list(volumes), *clean_airplay_targets],
        "start_server_us": state.start_server_us,
        "start_unix_ms": state.start_unix_ms,
        "audible_start_server_us": state.start_server_us,
        "audible_start_unix_ms": state.start_unix_ms,
        "sample_rate_hz": SENDSPIN_SAMPLE_RATE,
        "channels": SENDSPIN_CHANNELS,
    }
    if airplay_group_id:
        commit_result = dict(state.airplay_commit_result or {})
        result.update(
            {
                "airplay_bridge_group_id": airplay_group_id,
                "airplay_bridge_sent_count": int(
                    commit_result.get("sent_count") or len(clean_airplay_targets)
                ),
                "airplay_bridge_prepared_count": len(clean_airplay_targets),
                "airplay_bridge_routes": dict(
                    airplay_prepare_result.get("routes") or {}
                ),
            }
        )
        if _text(commit_result.get("timing_mode")):
            result["airplay_bridge_timing_mode"] = _text(
                commit_result.get("timing_mode")
            )
        if commit_result.get("start_unix_ms") is not None:
            result["airplay_bridge_start_unix_ms"] = int(
                commit_result["start_unix_ms"]
            )
        bridge_warnings = [
            _text(item)
            for item in [
                *list(airplay_prepare_result.get("warnings") or []),
                *list(commit_result.get("warnings") or []),
            ]
            if _text(item)
        ]
        if bridge_warnings:
            result["warnings"] = bridge_warnings
    return result


async def start_media_url_stream(
    stream_id: str,
    targets: list[Dict[str, Any]],
    source_url: str,
    *,
    start_position_seconds: float = 0.0,
    group_name: str = "Tater Music",
    start_lead_ms: int = 1000,
    airplay_targets: Iterable[Any] | None = None,
    airplay_volume_percent: Dict[str, Any] | None = None,
    airplay_sync_offset_ms: Dict[str, Any] | None = None,
    airplay_reference_sync_offset_ms: int = 0,
    title: str = "",
    artist: str = "",
    album: str = "",
    duration_seconds: float = 0.0,
    on_finished: Optional[Callable[[str, str], Any]] = None,
) -> Dict[str, Any]:
    """Decode one finite media URL and publish it as a Sendspin PCM timeline."""
    reader = await asyncio.to_thread(
        _FfmpegPcmReader,
        source_url,
        start_position_seconds=max(0.0, float(start_position_seconds or 0.0)),
    )
    try:
        return await start_live_pcm_stream(
            stream_id,
            targets,
            reader.read,
            group_name=group_name,
            input_sample_rate=SENDSPIN_SAMPLE_RATE,
            start_lead_ms=start_lead_ms,
            airplay_targets=airplay_targets,
            airplay_volume_percent=airplay_volume_percent,
            airplay_sync_offset_ms=airplay_sync_offset_ms,
            airplay_reference_sync_offset_ms=airplay_reference_sync_offset_ms,
            title=title,
            artist=artist,
            album=album,
            duration_seconds=duration_seconds,
            on_finished=on_finished,
            close_pcm=reader.close,
        )
    except Exception:
        await asyncio.to_thread(reader.close)
        raise


async def set_live_stream_volumes(
    stream_id: str,
    volume_percent: Dict[str, Any],
) -> Dict[str, Any]:
    clean_id = _text(stream_id)
    state = _active_live_streams.get(clean_id)
    if state is None or state.task is None or state.task.done():
        return {"ok": False, "stream_id": clean_id, "error": "Sendspin stream is not active."}
    updated: Dict[str, int] = {}
    for selector, value in dict(volume_percent or {}).items():
        clean_selector = _text(selector)
        if clean_selector not in state.volume_percent:
            continue
        volume = _as_int(value, state.volume_percent[clean_selector], 0, 100)
        state.volume_percent[clean_selector] = volume
        updated[clean_selector] = volume
    airplay_updates = {
        _text(selector): _as_int(value, 100, 0, 100)
        for selector, value in dict(volume_percent or {}).items()
        if _text(selector) in state.airplay_targets
    }
    if state.airplay_group_id and airplay_updates:
        from airplay_bridge import set_airplay_target_volumes

        airplay_result = await asyncio.to_thread(
            set_airplay_target_volumes,
            airplay_updates,
        )
        if not airplay_result.get("ok"):
            return {
                "ok": False,
                "stream_id": clean_id,
                "volume_percent": {**updated, **airplay_updates},
                "error": _text(airplay_result.get("error"))
                or "; ".join(
                    _text(item)
                    for item in list(airplay_result.get("warnings") or [])
                    if _text(item)
                )
                or "AirPlay volume update failed.",
            }
        updated.update(airplay_updates)
    return {"ok": True, "stream_id": clean_id, "volume_percent": updated}


async def stop_live_stream(stream_id: str) -> Dict[str, Any]:
    clean_id = _text(stream_id)
    state = _active_live_streams.get(clean_id)
    if state is None or state.task is None:
        return {"ok": True, "stream_id": clean_id, "stopped": False}
    task = state.task
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task
    return {"ok": True, "stream_id": clean_id, "stopped": True}


_active_pair_tasks: Dict[str, asyncio.Task[Dict[str, Any]]] = {}


async def play_stereo_pair_audio(
    pair: Dict[str, Any],
    targets: list[Dict[str, Any]],
    audio_bytes: bytes,
    *,
    preserve_stereo: bool = False,
    wait_for_completion: bool = True,
    completion_timeout_s: float = 180.0,
) -> Dict[str, Any]:
    """Play one finite asset on both members of a saved Tater stereo pair."""
    pair_row = pair if isinstance(pair, dict) else {}
    pair_id = _text(pair_row.get("id") or pair_row.get("selector")) or uuid.uuid4().hex[:12]
    await _stop_live_streams_for_targets(
        _text(target.get("selector")) for target in targets
    )
    prepared = await asyncio.to_thread(
        _prepare_pcm_sync,
        bytes(audio_bytes or b""),
        preserve_stereo=bool(preserve_stereo),
        left_delay_ms=_as_int(pair_row.get("left_delay_ms"), 0, 0, 250),
        right_delay_ms=_as_int(pair_row.get("right_delay_ms"), 0, 0, 250),
        left_volume_percent=_as_int(pair_row.get("left_volume_percent"), 100, 0, 100),
        right_volume_percent=_as_int(pair_row.get("right_volume_percent"), 100, 0, 100),
    )

    previous = _active_pair_tasks.get(pair_id)
    if previous is not None and not previous.done():
        previous.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await previous

    started_event = asyncio.Event()
    task = asyncio.create_task(
        _stream_pair_pcm(
            targets,
            prepared,
            pair_id=pair_id,
            pair_name=_text(pair_row.get("name")),
            started_event=started_event,
        ),
        name=f"sendspin-pair-{pair_id}",
    )
    _active_pair_tasks[pair_id] = task

    def _finished(finished: asyncio.Task[Dict[str, Any]]) -> None:
        if _active_pair_tasks.get(pair_id) is finished:
            _active_pair_tasks.pop(pair_id, None)
        if finished.cancelled():
            return
        error = finished.exception()
        if error is not None and not wait_for_completion:
            logger.warning("[sendspin] background pair reply failed pair=%s error=%s", pair_id, error)

    task.add_done_callback(_finished)

    if not wait_for_completion:
        try:
            await asyncio.wait_for(started_event.wait(), timeout=SENDSPIN_HANDSHAKE_TIMEOUT_S + 5.0)
        except asyncio.TimeoutError:
            task.cancel()
            raise SendspinPlaybackError("Timed out starting the Sendspin stereo reply.")
        if task.done():
            return task.result()
        return {
            "ok": True,
            "sendspin_playback_started": True,
            "playback_completed": False,
            "group_id": pair_id,
            "members": [_text(target.get("selector")) for target in targets],
            "duration_s": round(prepared.duration_s, 3),
            "sample_rate_hz": SENDSPIN_SAMPLE_RATE,
            "channels": SENDSPIN_CHANNELS,
        }

    timeout_s = max(
        10.0,
        min(
            615.0,
            max(float(completion_timeout_s or 180.0), prepared.duration_s + 10.0),
        ),
    )
    try:
        return await asyncio.wait_for(task, timeout=timeout_s)
    except asyncio.TimeoutError as exc:
        raise SendspinPlaybackError("Timed out waiting for the Sendspin stereo reply to finish.") from exc
