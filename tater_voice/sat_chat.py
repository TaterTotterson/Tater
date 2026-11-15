from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import os
import struct
import time
import uuid
from collections import deque
from typing import Any, Dict, NamedTuple, Optional


SAMPLE_RATE_HZ = 16000
CHANNELS = 1
SAMPLE_FORMAT = "s16le"
VIDEO_FRAME_PREFIX = b"TSCV1"
H264_FRAME_PREFIX = b"TSCV2"
H264_HEADER_BYTES = 41
MAX_VIDEO_FRAME_BYTES = 2 * 1024 * 1024
MAX_H264_RELAY_FRAMES = 32
MAX_AUDIO_RELAY_FRAMES = 4

_lock = asyncio.Lock()
_calls: Dict[str, Dict[str, Any]] = {}
_endpoint_calls: Dict[str, str] = {}
_external_senders: Dict[str, Any] = {}
_external_event_senders: Dict[str, Any] = {}
_external_input_rates: Dict[str, int] = {}
_video_relays: Dict[tuple[str, str], Dict[str, Any]] = {}
_comparison_calls = 0
_logger = logging.getLogger(__name__)


class _MediaPacket(NamedTuple):
    data: bytes
    received_at: float
    input_gap_ms: float = 0.0
    source_gap_ms: Optional[float] = None
    sequence: Optional[int] = None
    stream_id: Optional[int] = None
    enqueued_at: Optional[float] = None


def _next_relay_mode() -> str:
    """Owner-local comparison opt-in; normal deployments stay on shared mode."""
    global _comparison_calls
    mode = os.environ.get("TATER_SAT_CHAT_RELAY_MODE", "").strip().lower()
    if mode == "compare":
        _comparison_calls += 1
        return "shared" if _comparison_calls % 2 else "split"
    return "split" if mode == "split" else "shared"


def _relay_tasks(relay: Dict[str, Any]) -> list[asyncio.Task]:
    task = relay.get("task")
    return [task] if isinstance(task, asyncio.Task) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _valid_h264_frame(data: bytes, *, balanced: bool = False) -> bool:
    if not data.startswith(H264_FRAME_PREFIX) or not (
        H264_HEADER_BYTES + 5 <= len(data) <= H264_HEADER_BYTES + 1024 * 1024
    ):
        return False
    width, height, fps, _sequence, flags, pts, stream_id = struct.unpack_from("<IIIIIQQ", data, 5)
    payload = data[H264_HEADER_BYTES:]
    return (
        (width, height, fps) in ({(768, 432, 30), (640, 480, 30)} if balanced else {(1280, 720, 30), (1280, 960, 30)})
        and flags in (0, 1)
        and pts < 2**63
        and stream_id != 0
        and (payload.startswith(b"\0\0\1") or payload.startswith(b"\0\0\0\1"))
    )


def _h264_receive_max_height(board: str, capabilities: Dict[str, Any]) -> int:
    # Cronos' first qualified v1 receiver supports its own 960-line capture
    # and 720p. Older Checkers v1 receivers only accept 720p, so an explicit
    # capability is required before sending them the taller Cronos stream.
    default = 960 if board == "cronos" else 720
    maximum = capabilities.get("sat_chat_h264_receive_max_height")
    return maximum if type(maximum) is int and maximum in (720, 960) else default


def _h264_pair_compatible(caller: Optional[Dict[str, Any]], target: Dict[str, Any]) -> bool:
    caller = caller or {}
    if not caller.get("video_h264") or not target.get("video_h264"):
        return False
    if caller.get("video_h264_balanced_only") or target.get("video_h264_balanced_only"):
        return False
    caller_height = 960 if caller.get("board") == "cronos" else 720
    target_height = 960 if target.get("board") == "cronos" else 720
    return (caller.get("video_h264_receive_max_height", 720) >= target_height
            and target.get("video_h264_receive_max_height", 720) >= caller_height)


def _video_receive_profile(participant: Optional[Dict[str, Any]]) -> str:
    board = _text((participant or {}).get("board")).lower().replace("_", "-")
    if board in {"checkers", "cronos", "crown", "athena"}:
        # The larger Echo displays use a 640x360 source at 15 FPS. This avoids
        # enlarging the former 320x180 call frame across an 848-1280px panel
        # while retaining a conservative cadence for software JPEG decoding.
        return "echo-balanced"
    if board == "rook":
        # Rook's VGA sensor and 352x198 call area get little benefit from the
        # larger profile, so keep its cheaper direct Bayer path at 20 FPS.
        return "echo-low"
    return ""


def _public_call(call: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(call, dict):
        return {}
    return {
        key: value
        for key, value in call.items()
        if key not in {"ended_notified"} and not str(key).startswith("_")
    }


def _endpoint_call(call: Dict[str, Any], endpoint: str) -> Dict[str, Any]:
    """Return call state plus the camera profile this endpoint should send.

    Profiles follow the receiver. Echo displays use the lower-cost 320x180
    stream, while mobile peers keep the native camera profile.
    """
    public = _public_call(call)
    caller = _text(call.get("caller"))
    target = _text(call.get("target"))
    peer = target if _text(endpoint) == caller else caller
    profiles = call.get("_video_profiles") if isinstance(call.get("_video_profiles"), dict) else {}
    profile = _text(profiles.get(peer))
    if profile:
        public["video_send_profile"] = profile
    return public


def _participant_from_client(selector: str, client: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not bool(client.get("connected")):
        return None
    capabilities = client.get("capabilities") if isinstance(client.get("capabilities"), dict) else {}
    if not _truthy(capabilities.get("sat_chat_audio")):
        return None
    board = _text(client.get("board")).lower().replace("_", "-")
    return {
        "id": selector,
        "selector": selector,
        "name": _text(client.get("device_name") or client.get("name") or selector),
        "room": _text(client.get("room")),
        "board": board,
        "connected": True,
        "audio": True,
        # Video remains false until the Echo camera helper can provide a
        # continuous stream. A snapshot capability is not a video-call claim.
        "video": _truthy(capabilities.get("sat_chat_video")),
        "video_h264": (board in {"checkers", "cronos", "crown"} or
                       board in {"athena", "rook"} and _truthy(capabilities.get("sat_chat_h264_balanced_only"))
                       and _truthy(capabilities.get("sat_chat_h264_balanced")))
                      and _truthy(capabilities.get("sat_chat_h264")) and capabilities.get("sat_chat_h264_version") == 1,
        "video_h264_receive_max_height": _h264_receive_max_height(board, capabilities),
        "video_h264_balanced": _truthy(capabilities.get("sat_chat_h264_balanced")),
        "video_h264_balanced_only": board in {"athena", "rook"} or _truthy(capabilities.get("sat_chat_h264_balanced_only")),
        "can_initiate": _truthy(capabilities.get("sat_chat_can_initiate")),
        "can_end": _truthy(capabilities.get("sat_chat_can_end")),
        "has_ui": _truthy(capabilities.get("sat_chat_ui")),
        "busy": selector in _endpoint_calls,
    }


async def _native_clients() -> Dict[str, Dict[str, Any]]:
    from . import native_satellite

    snapshot = await native_satellite.status(include_live_settings=False)
    clients = snapshot.get("clients") if isinstance(snapshot.get("clients"), dict) else {}
    return {str(key): value for key, value in clients.items() if isinstance(value, dict)}


async def participants(*, viewer: str = "") -> Dict[str, Any]:
    clients = await _native_clients()
    return await _directory_from_clients(clients, viewer=viewer)


async def _directory_from_clients(clients: Dict[str, Dict[str, Any]], *, viewer: str = "") -> Dict[str, Any]:
    viewer_token = _text(viewer)
    async with _lock:
        rows = [
            participant
            for selector, client in clients.items()
            if selector != viewer_token
            if (participant := _participant_from_client(selector, client)) is not None
        ]
        rows.sort(key=lambda row: (_text(row.get("room")).lower(), _text(row.get("name")).lower()))
        call_id = _endpoint_calls.get(viewer_token, "")
        active_call = _calls.get(call_id)
        call = _endpoint_call(active_call, viewer_token) if isinstance(active_call, dict) else {}
    return {
        "ok": True,
        "protocol": 1,
        "participant_count": len(rows),
        "participants": rows,
        "call": call,
        "media": {
            "audio": True,
            "video": True,
            "video_format": "jpeg",
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "channels": CHANNELS,
            "sample_format": SAMPLE_FORMAT,
        },
    }


async def _send(selector: str, message_type: str, payload: Dict[str, Any]) -> None:
    from . import native_satellite

    with contextlib.suppress(Exception):
        await native_satellite.send_command(selector, message_type, payload)


async def refresh_directories() -> None:
    clients = await _native_clients()
    targets = []
    for selector, client in clients.items():
        capabilities = client.get("capabilities") if isinstance(client.get("capabilities"), dict) else {}
        if bool(client.get("connected")) and _truthy(capabilities.get("sat_chat_audio")):
            targets.append(selector)
    for selector in targets:
        # Reuse this operation's snapshot. Native status includes effective
        # device/wake settings; rebuilding it for every recipient stalls the
        # same event loop that carries call audio and H.264 packets.
        directory = await _directory_from_clients(clients, viewer=selector)
        await _send(selector, "sat_chat.directory", directory)


async def start(caller: str, target: str, *, mode: str = "audio", caller_name: str = "") -> Dict[str, Any]:
    caller_token = _text(caller)
    target_token = _text(target)
    requested_mode = _text(mode).lower() or "audio"
    if not caller_token or not target_token or caller_token == target_token:
        raise ValueError("A caller and a different target are required.")
    if requested_mode not in {"audio", "video"}:
        raise ValueError("Sat Chat mode must be audio or video.")

    clients = await _native_clients()
    caller_client = clients.get(caller_token)
    target_client = clients.get(target_token)
    caller_participant = _participant_from_client(caller_token, caller_client or {})
    target_participant = _participant_from_client(target_token, target_client or {})
    external_caller = caller_token.startswith("spudlink:")
    if caller_participant is None and not external_caller:
        raise RuntimeError("The calling satellite is not connected or does not support Sat Chat.")
    if caller_participant is not None and not bool(caller_participant.get("can_initiate")):
        raise PermissionError("This satellite cannot start Sat Chat calls.")
    if target_participant is None:
        raise RuntimeError("The target satellite is not connected or does not support Sat Chat.")
    if requested_mode == "video" and not bool(target_participant.get("video")):
        raise ValueError("That satellite does not currently support Sat Chat video.")
    if requested_mode == "video" and caller_participant is not None and not bool(caller_participant.get("video")):
        raise ValueError("The calling satellite does not currently support Sat Chat video.")

    now = time.time()
    call_id = uuid.uuid4().hex
    state = "active"
    caller_label = _text(caller_name) or _text((caller_participant or {}).get("name")) or "Little Spud"
    call = {
        "id": call_id,
        "state": state,
        "mode": requested_mode,
        "caller": caller_token,
        "caller_name": caller_label,
        "target": target_token,
        "target_name": _text(target_participant.get("name")),
        "created_ts": now,
        "started_ts": now,
        "updated_ts": now,
        "_video_profiles": {
            caller_token: _video_receive_profile(caller_participant),
            target_token: _video_receive_profile(target_participant),
        },
    }
    if (caller_participant and caller_participant.get("video_h264") and target_participant.get("video_h264")
            and caller_participant.get("video_h264_balanced") and target_participant.get("video_h264_balanced")):
        call["_video_profiles"] = {caller_token: "echo-h264-balanced", target_token: "echo-h264-balanced"}
    elif _h264_pair_compatible(caller_participant, target_participant):
        call["_video_profiles"] = {caller_token: "echo-h264", target_token: "echo-h264"}
    async with _lock:
        if caller_token in _endpoint_calls or target_token in _endpoint_calls:
            raise RuntimeError("One of these Sat Chat participants is already busy.")
        call["_relay_mode"] = _next_relay_mode() if requested_mode == "video" else "shared"
        _calls[call_id] = call
        _endpoint_calls[caller_token] = call_id
        _endpoint_calls[target_token] = call_id
    _logger.info("[sat-chat] call=%s relay_mode=%s", call_id, call["_relay_mode"])
    await _send(target_token, "sat_chat.started", _endpoint_call(call, target_token))
    if not external_caller:
        await _send(caller_token, "sat_chat.started", _endpoint_call(call, caller_token))
    await refresh_directories()
    return {"ok": True, "call": _public_call(call)}


async def end(
    *,
    call_id: str = "",
    actor: str = "",
    reason: str = "ended",
    force: bool = False,
) -> Dict[str, Any]:
    call_token = _text(call_id)
    actor_token = _text(actor)
    media_tasks = []
    async with _lock:
        if not call_token and actor_token:
            call_token = _endpoint_calls.get(actor_token, "")
        call = _calls.get(call_token)
        if not isinstance(call, dict):
            return {"ok": True, "ended": False}
        endpoints = {_text(call.get("caller")), _text(call.get("target"))}
        if not force and actor_token not in endpoints:
            raise PermissionError("Only a Sat Chat participant can end this call.")
        if not force and actor_token.startswith("native:"):
            clients = await _native_clients()
            participant = _participant_from_client(actor_token, clients.get(actor_token) or {})
            if participant is not None and not bool(participant.get("can_end")):
                raise PermissionError("This satellite cannot end Sat Chat calls.")
        ended_ts = time.time()
        call["state"] = "ended"
        call["reason"] = _text(reason) or "ended"
        call["ended_ts"] = ended_ts
        call["updated_ts"] = ended_ts
        public = _public_call(call)
        _calls.pop(call_token, None)
        for endpoint in endpoints:
            if _endpoint_calls.get(endpoint) == call_token:
                _endpoint_calls.pop(endpoint, None)
        for key, relay in list(_video_relays.items()):
            if key[0] != call_token:
                continue
            _video_relays.pop(key, None)
            if isinstance(relay, dict):
                media_tasks.extend(_relay_tasks(relay))
    current_task = asyncio.current_task()
    for task in media_tasks:
        if task is not current_task:
            task.cancel()
    for endpoint in endpoints:
        if endpoint.startswith("native:"):
            await _send(endpoint, "sat_chat.ended", public)
        else:
            async with _lock:
                event_sender = _external_event_senders.get(endpoint)
            if callable(event_sender):
                with contextlib.suppress(Exception):
                    await event_sender({"ok": True, "type": "sat_chat.ended", "call": public})
    await refresh_directories()
    return {"ok": True, "ended": True, "call": public}


async def disconnected(endpoint: str) -> None:
    token = _text(endpoint)
    async with _lock:
        call_id = _endpoint_calls.get(token, "")
    if call_id:
        await end(call_id=call_id, actor=token, reason="disconnected", force=True)
    else:
        await refresh_directories()


async def route_audio(source: str, data: bytes, *, received_at: Optional[float] = None) -> bool:
    received_at = time.monotonic() if received_at is None else received_at
    source_token = _text(source)
    if not data:
        return False
    async with _lock:
        call_id = _endpoint_calls.get(source_token, "")
        call = _calls.get(call_id)
        if not isinstance(call, dict) or call.get("state") != "active":
            # Late/reserved video is not microphone PCM, even after call end.
            return data.startswith(b"TSCV")
        caller = _text(call.get("caller"))
        target = _text(call.get("target"))
        peer = target if source_token == caller else caller
        source_rate = int(_external_input_rates.get(source_token) or SAMPLE_RATE_HZ)
        mode = _text(call.get("mode")).lower() or "audio"
        profiles = call.get("_video_profiles") if isinstance(call.get("_video_profiles"), dict) else {}
        video_profile = _text(profiles.get(peer))
    if data.startswith(H264_FRAME_PREFIX):
        if mode == "video" and video_profile in {"echo-h264", "echo-h264-balanced"} and _valid_h264_frame(data, balanced=video_profile == "echo-h264-balanced"):
            await _queue_video_frame(call_id, source_token, peer, bytes(data), profile=video_profile, received_at=received_at)
        return True
    if data.startswith(b"TSCV") and not data.startswith(VIDEO_FRAME_PREFIX):
        return True
    is_video = data.startswith(VIDEO_FRAME_PREFIX)
    if is_video:
        jpeg_size = len(data) - len(VIDEO_FRAME_PREFIX)
        if mode != "video" or jpeg_size < 4 or jpeg_size > MAX_VIDEO_FRAME_BYTES or data[len(VIDEO_FRAME_PREFIX):len(VIDEO_FRAME_PREFIX) + 2] != b"\xff\xd8":
            return True
        await _queue_video_frame(call_id, source_token, peer, bytes(data), profile=video_profile, received_at=received_at)
        return True
    elif source_rate != SAMPLE_RATE_HZ:
        data = _resample_s16le(data, source_rate, SAMPLE_RATE_HZ)
    if not peer.startswith("native:"):
        async with _lock:
            sender = _external_senders.get(peer)
        if callable(sender):
            try:
                await sender(bytes(data))
            except Exception:
                await end(call_id=call_id, actor=source_token, reason="media_disconnected", force=True)
        # The frame belongs to Sat Chat even if the phone is still opening its
        # media socket. Never leak call audio into the assistant voice bridge.
        return True
    await _queue_audio_frame(call_id, source_token, peer, bytes(data), profile=video_profile, received_at=received_at)
    return True


def _new_media_relay(
    key: tuple[str, str], source: str, peer: str, *, profile: str = ""
) -> Dict[str, Any]:
    video_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_H264_RELAY_FRAMES)
    audio_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_AUDIO_RELAY_FRAMES)
    call = _calls.get(key[0]) or {}
    mode = call.get("_relay_mode") or (
        "split" if os.environ.get("TATER_SAT_CHAT_RELAY_MODE", "").strip().lower() == "split" else "shared"
    )
    relay: Dict[str, Any] = {
        "queue": video_queue,
        "audio_queue": audio_queue,
        "ready": asyncio.Event(),
        "started_at": time.monotonic(),
        "recent_video": deque(maxlen=120),
        "loop_stalls": deque(maxlen=16),
        "stats": {
            "source": _text(source),
            "peer": _text(peer),
            "profile": _text(profile),
            "relay_mode": mode,
            "received_frames": 0,
            "received_bytes": 0,
            "sent_frames": 0,
            "sent_bytes": 0,
            "dropped_gops": 0,
            "maximum_input_gap_ms": 0.0,
            "maximum_output_gap_ms": 0.0,
            "maximum_send_ms": 0.0,
            "maximum_frame_bytes": 0,
            "maximum_keyframe_bytes": 0,
            "maximum_queue_depth": 0,
            "maximum_queue_wait_ms": 0.0,
            "maximum_route_wait_ms": 0.0,
            "maximum_prepare_ms": 0.0,
            "maximum_source_gap_ms": 0.0,
            "maximum_event_loop_lag_ms": 0.0,
            "input_gaps_over_200_ms": 0,
            "output_gaps_over_200_ms": 0,
            "source_sequence_missing": 0,
            "source_sequence_duplicates": 0,
            "source_sequence_reversed": 0,
            "audio_received_frames": 0,
            "audio_sent_frames": 0,
            "audio_dropped_frames": 0,
            "audio_maximum_queue_depth": 0,
            "audio_maximum_send_ms": 0.0,
            "audio_maximum_queue_wait_ms": 0.0,
        },
    }
    task = asyncio.create_task(
        _media_relay_worker(
            key,
            source,
            peer,
            audio_queue,
            video_queue,
            relay["ready"],
            profile=_text(profile),
            relay=relay,
            mode=mode,
        )
    )
    relay["task"] = task
    return relay


def _relay_for_locked(
    key: tuple[str, str], source: str, peer: str, *, profile: str = ""
) -> Dict[str, Any]:
    relay = _video_relays.get(key)
    task = relay.get("task") if isinstance(relay, dict) else None
    if not isinstance(relay, dict) or not isinstance(task, asyncio.Task) or task.done():
        relay = _new_media_relay(key, source, peer, profile=profile)
        _video_relays[key] = relay
    elif profile and not _text(relay.get("stats", {}).get("profile")):
        relay["stats"]["profile"] = _text(profile)
    return relay


async def _queue_audio_frame(
    call_id: str, source: str, peer: str, data: bytes, *, profile: str = "",
    received_at: Optional[float] = None,
) -> None:
    key = (_text(call_id), _text(peer))
    async with _lock:
        relay = _relay_for_locked(key, source, peer, profile=profile)
        queue = relay["audio_queue"]
        stats = relay["stats"]
        stats["audio_received_frames"] = int(stats.get("audio_received_frames") or 0) + 1
        if queue.full():
            # PCM packets are independent 80 ms chunks. Prefer one short gap
            # over allowing a blocked peer to add seconds of conversational
            # latency or, previously, stop the source WebSocket reader too.
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
                stats["audio_dropped_frames"] = int(stats.get("audio_dropped_frames") or 0) + 1
        now = time.monotonic()
        queue.put_nowait(_MediaPacket(data, now if received_at is None else received_at, enqueued_at=now))
        stats["audio_maximum_queue_depth"] = max(
            int(stats.get("audio_maximum_queue_depth") or 0), queue.qsize()
        )
        relay["ready"].set()


async def _queue_video_frame(
    call_id: str, source: str, peer: str, data: bytes, *, profile: str = "",
    received_at: Optional[float] = None,
) -> None:
    key = (_text(call_id), _text(peer))
    async with _lock:
        relay = _relay_for_locked(key, source, peer, profile=profile)
        packet = _observe_video_input(relay, data, received_at=received_at)
        _enqueue_video_packet(relay, data, packet=packet)
        relay["ready"].set()


def _observe_video_input(
    relay: Dict[str, Any], data: bytes, *, received_at: Optional[float] = None
) -> _MediaPacket:
    enqueued = time.monotonic()
    now = enqueued if received_at is None else received_at
    stats = relay.get("stats")
    if not isinstance(stats, dict):
        return _MediaPacket(data, now, enqueued_at=enqueued)
    previous = stats.get("_input_at")
    input_gap = 0.0 if previous is None else (now - previous) * 1000.0
    if previous is not None:
        stats["maximum_input_gap_ms"] = max(
            float(stats.get("maximum_input_gap_ms") or 0.0), input_gap
        )
        if input_gap > 200.0:
            stats["input_gaps_over_200_ms"] += 1
    stats["_input_at"] = now
    stats["received_frames"] = int(stats.get("received_frames") or 0) + 1
    stats["received_bytes"] = int(stats.get("received_bytes") or 0) + len(data)
    stats["maximum_frame_bytes"] = max(int(stats.get("maximum_frame_bytes") or 0), len(data))
    sequence = stream_id = None
    source_gap = None
    if data.startswith(H264_FRAME_PREFIX) and len(data) >= H264_HEADER_BYTES:
        sequence, flags, pts, stream_id = struct.unpack_from("<IIQQ", data, 17)
        if stats.get("_source_stream_id") == stream_id:
            previous_pts = stats.get("_source_pts")
            if previous_pts is not None and pts >= previous_pts:
                source_gap = (pts - previous_pts) / 1000.0
                stats["maximum_source_gap_ms"] = max(stats["maximum_source_gap_ms"], source_gap)
            previous_sequence = stats.get("last_sequence")
            if previous_sequence is not None:
                delta = (sequence - previous_sequence) & 0xFFFFFFFF
                if delta == 0:
                    stats["source_sequence_duplicates"] += 1
                elif delta >= 0x80000000:
                    stats["source_sequence_reversed"] += 1
                elif delta > 1:
                    stats["source_sequence_missing"] += delta - 1
        stats["_source_stream_id"] = stream_id
        stats["_source_pts"] = pts
        stats["last_sequence"] = sequence
        if flags == 1:
            stats["maximum_keyframe_bytes"] = max(
                int(stats.get("maximum_keyframe_bytes") or 0), len(data)
            )
    return _MediaPacket(data, now, input_gap, source_gap, sequence, stream_id, enqueued)


def _enqueue_video_packet(
    relay: Dict[str, Any], data: bytes, *, packet: Optional[_MediaPacket] = None
) -> None:
    packet = packet if packet is not None else _MediaPacket(data, time.monotonic())
    queue = relay["queue"]
    stats = relay.get("stats") if isinstance(relay.get("stats"), dict) else {}
    h264 = data.startswith(H264_FRAME_PREFIX)
    if relay.get("h264") != h264:
        relay["h264"] = h264
        relay["waiting_keyframe"] = h264
        while not queue.empty():
            queue.get_nowait()
    if not h264:
        # JPEGs are independent: retain only the most recent waiting image.
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(packet)
        return
    keyframe = len(data) >= H264_HEADER_BYTES and struct.unpack_from("<I", data, 21)[0] == 1
    if relay.get("waiting_keyframe") and not keyframe:
        return
    if queue.full():
        # Never drop one predictive packet and forward its dependents. Bound
        # buffering to one GOP, then restart only at a fresh SPS/PPS + IDR.
        while not queue.empty():
            queue.get_nowait()
        stats["dropped_gops"] = int(stats.get("dropped_gops") or 0) + 1
        relay["waiting_keyframe"] = True
        if not keyframe:
            return
    queue.put_nowait(packet)
    stats["maximum_queue_depth"] = max(
        int(stats.get("maximum_queue_depth") or 0), queue.qsize()
    )
    relay["waiting_keyframe"] = False


def _next_media_packet(
    audio_queue: asyncio.Queue, video_queue: asyncio.Queue
) -> tuple[str, Optional[_MediaPacket]]:
    try:
        return "audio", audio_queue.get_nowait()
    except asyncio.QueueEmpty:
        pass
    try:
        return "video", video_queue.get_nowait()
    except asyncio.QueueEmpty:
        return "", None


async def _media_relay_worker(
    key: tuple[str, str],
    source: str,
    peer: str,
    audio_queue: asyncio.Queue,
    video_queue: asyncio.Queue,
    ready: asyncio.Event,
    *,
    profile: str = "",
    relay: Optional[Dict[str, Any]] = None,
    mode: str = "shared",
) -> None:
    children: list[asyncio.Task] = []
    try:
        if isinstance(relay, dict):
            children.append(asyncio.create_task(_relay_loop_monitor(relay)))
        if mode == "split":
            children.extend([
                asyncio.create_task(_independent_relay_worker("audio", peer, audio_queue, profile, relay)),
                asyncio.create_task(_independent_relay_worker("video", peer, video_queue, profile, relay)),
            ])
            await asyncio.gather(*children)
            return
        while True:
            kind, packet = _next_media_packet(audio_queue, video_queue)
            if packet is None:
                ready.clear()
                # Clear-before-recheck closes the enqueue race without one
                # get task per queue. The single worker is also the sole
                # WebSocket media writer for this peer, so audio can always
                # take priority over video without send-lock contention.
                kind, packet = _next_media_packet(audio_queue, video_queue)
                if packet is None:
                    await ready.wait()
                    continue
            await _forward_relay_packet(kind, packet, peer, profile, relay)
    except asyncio.CancelledError:
        raise
    except Exception:
        await end(call_id=key[0], actor=source, reason="media_disconnected", force=True)
    finally:
        for task in children:
            task.cancel()
        if children:
            await asyncio.gather(*children, return_exceptions=True)
        async with _lock:
            relay = _video_relays.get(key)
            if isinstance(relay, dict) and relay.get("task") is asyncio.current_task():
                _video_relays.pop(key, None)


async def _independent_relay_worker(
    kind: str, peer: str, queue: asyncio.Queue, profile: str, relay: Optional[Dict[str, Any]]
) -> None:
    while True:
        packet = await queue.get()
        await _forward_relay_packet(kind, packet, peer, profile, relay)


async def _relay_loop_monitor(relay: Dict[str, Any]) -> None:
    # Host-local scheduling only, not a source-clock/end-to-end latency claim.
    interval = 0.05
    trace = None
    if _truthy(os.environ.get("TATER_SAT_CHAT_STALL_TRACE")):
        from .relay_stall_trace import RelayStallTrace

        trace = RelayStallTrace(relay["started_at"])
        relay["stall_trace"] = trace
        trace.start()
    try:
        while True:
            due = time.monotonic() + interval
            if trace is not None:
                trace.heartbeat()
            await asyncio.sleep(interval)
            now = time.monotonic()
            if trace is not None:
                trace.heartbeat()
            lag_ms = max(0.0, now - due) * 1000.0
            stats = relay["stats"]
            stats["maximum_event_loop_lag_ms"] = max(stats["maximum_event_loop_lag_ms"], lag_ms)
            if lag_ms >= 50.0:
                relay["loop_stalls"].append({
                    "at_ms": round((now - relay["started_at"]) * 1000.0, 3),
                    "lag_ms": round(lag_ms, 3),
                })
    finally:
        if trace is not None:
            trace.stop()
            await asyncio.to_thread(trace.join)


async def _forward_relay_packet(
    kind: str, packet: _MediaPacket, peer: str, profile: str,
    relay: Optional[Dict[str, Any]],
) -> None:
    frame = packet.data
    dequeued = time.monotonic()
    if peer.startswith("native:"):
        from . import native_satellite

        if kind == "video" and profile == "echo-low":
            frame = await asyncio.to_thread(_resize_echo_video_frame, frame)
        elif kind == "video" and profile == "echo-balanced":
            frame = await asyncio.to_thread(
                _resize_echo_video_frame, frame, max_size=(640, 360), quality=62
            )
        sender = lambda payload: native_satellite.send_binary(peer, payload)
    else:
        async with _lock:
            sender = _external_senders.get(peer)
    if not callable(sender):
        return
    started = time.monotonic()
    await sender(frame)
    if kind == "video":
        _observe_video_output(relay, frame, started, packet=packet, dequeued=dequeued)
    else:
        _observe_audio_output(relay, started)
        if isinstance(relay, dict):
            stats = relay["stats"]
            stats["audio_maximum_queue_wait_ms"] = max(
                stats["audio_maximum_queue_wait_ms"],
                (dequeued - (packet.received_at if packet.enqueued_at is None else packet.enqueued_at)) * 1000.0,
            )


def _observe_audio_output(relay: Optional[Dict[str, Any]], started: float) -> None:
    if not isinstance(relay, dict):
        return
    stats = relay.get("stats")
    if not isinstance(stats, dict):
        return
    stats["audio_sent_frames"] = int(stats.get("audio_sent_frames") or 0) + 1
    stats["audio_maximum_send_ms"] = max(
        float(stats.get("audio_maximum_send_ms") or 0.0),
        (time.monotonic() - started) * 1000.0,
    )


def _observe_video_output(
    relay: Optional[Dict[str, Any]], frame: bytes, started: float, *,
    packet: _MediaPacket, dequeued: float,
) -> None:
    if not isinstance(relay, dict):
        return
    stats = relay.get("stats")
    if not isinstance(stats, dict):
        return
    finished = time.monotonic()
    previous = stats.get("_output_at")
    if previous is not None:
        output_gap = (started - previous) * 1000.0
        stats["maximum_output_gap_ms"] = max(
            float(stats.get("maximum_output_gap_ms") or 0.0), output_gap
        )
        if output_gap > 200.0:
            stats["output_gaps_over_200_ms"] += 1
    stats["_output_at"] = started
    stats["sent_frames"] = int(stats.get("sent_frames") or 0) + 1
    stats["sent_bytes"] = int(stats.get("sent_bytes") or 0) + len(frame)
    stats["maximum_send_ms"] = max(
        float(stats.get("maximum_send_ms") or 0.0), (finished - started) * 1000.0
    )
    enqueued = packet.received_at if packet.enqueued_at is None else packet.enqueued_at
    route_ms = (enqueued - packet.received_at) * 1000.0
    queue_ms = (dequeued - enqueued) * 1000.0
    prepare_ms = (started - dequeued) * 1000.0
    stats["maximum_queue_wait_ms"] = max(stats["maximum_queue_wait_ms"], queue_ms)
    stats["maximum_route_wait_ms"] = max(stats["maximum_route_wait_ms"], route_ms)
    stats["maximum_prepare_ms"] = max(stats["maximum_prepare_ms"], prepare_ms)
    relay["recent_video"].append({
        "sequence": packet.sequence,
        "stream_id": packet.stream_id,
        "received_at_ms": round((packet.received_at - relay["started_at"]) * 1000.0, 3),
        "sent_at_ms": round((started - relay["started_at"]) * 1000.0, 3),
        "input_gap_ms": round(packet.input_gap_ms, 3),
        "source_gap_ms": None if packet.source_gap_ms is None else round(packet.source_gap_ms, 3),
        "route_wait_ms": round(route_ms, 3),
        "queue_wait_ms": round(queue_ms, 3),
        "prepare_ms": round(prepare_ms, 3),
        "send_ms": round((finished - started) * 1000.0, 3),
    })


async def relay_diagnostics() -> Dict[str, Any]:
    """Return bounded active-call timing without retaining media contents."""
    async with _lock:
        rows = []
        for (call_id, peer), relay in _video_relays.items():
            if not isinstance(relay, dict):
                continue
            stats = relay.get("stats")
            queue = relay.get("queue")
            if not isinstance(stats, dict):
                continue
            public = {
                key: value
                for key, value in stats.items()
                if not str(key).startswith("_")
            }
            public["call_id"] = call_id
            public["peer"] = peer
            public["queue_depth"] = queue.qsize() if isinstance(queue, asyncio.Queue) else 0
            audio_queue = relay.get("audio_queue")
            public["audio_queue_depth"] = (
                audio_queue.qsize() if isinstance(audio_queue, asyncio.Queue) else 0
            )
            for field in public:
                if field.endswith("_ms"):
                    public[field] = round(float(public.get(field) or 0.0), 3)
            public["recent_video"] = list(relay["recent_video"])
            public["loop_stalls"] = list(relay["loop_stalls"])
            trace = relay.get("stall_trace")
            public["stall_stacks"] = trace.snapshot() if trace is not None else []
            rows.append(public)
        calls = [
            {"call_id": key, "relay_mode": call.get("_relay_mode", "shared")}
            for key, call in _calls.items()
        ]
    return {"ok": True, "relays": rows, "count": len(rows), "calls": calls}


def _resize_echo_video_frame(
    frame: bytes,
    *,
    max_size: tuple[int, int] = (480, 270),
    quality: int = 58,
) -> bytes:
    if not frame.startswith(VIDEO_FRAME_PREFIX):
        return frame
    try:
        from PIL import Image

        with Image.open(io.BytesIO(frame[len(VIDEO_FRAME_PREFIX):])) as source:
            source.load()
            if source.width <= max_size[0] and source.height <= max_size[1]:
                return frame
            image = source.convert("RGB")
            image.thumbnail(max_size, Image.Resampling.BILINEAR)
            encoded = io.BytesIO()
            image.save(encoded, format="JPEG", quality=quality, optimize=False)
        return VIDEO_FRAME_PREFIX + encoded.getvalue()
    except Exception:
        # Media routing must remain fail-open: a malformed or unusual JPEG is
        # still subject to the receiver's normal validation and must not end
        # an otherwise healthy Sat Chat call.
        return frame


def _resample_s16le(data: bytes, source_rate: int, target_rate: int) -> bytes:
    usable = len(data) - len(data) % 2
    if usable <= 0 or source_rate <= 0 or target_rate <= 0 or source_rate == target_rate:
        return bytes(data[:usable])
    samples = struct.unpack(f"<{usable // 2}h", data[:usable])
    output_count = max(1, int(round(len(samples) * target_rate / source_rate)))
    out = []
    for index in range(output_count):
        position = index * source_rate / target_rate
        left = min(len(samples) - 1, int(position))
        right = min(len(samples) - 1, left + 1)
        fraction = position - left
        out.append(int(round(samples[left] + (samples[right] - samples[left]) * fraction)))
    return struct.pack(f"<{len(out)}h", *out)


async def register_external_media(
    endpoint: str,
    sender: Any,
    *,
    event_sender: Any = None,
    input_rate: int = SAMPLE_RATE_HZ,
) -> Dict[str, Any]:
    token = _text(endpoint)
    if not token.startswith("spudlink:") or not callable(sender):
        raise ValueError("A valid Little Spud media endpoint is required.")
    async with _lock:
        call_id = _endpoint_calls.get(token, "")
        call = _calls.get(call_id)
        if not isinstance(call, dict) or call.get("state") != "active":
            raise RuntimeError("This Little Spud does not have an active Sat Chat call.")
        _external_senders[token] = sender
        if callable(event_sender):
            _external_event_senders[token] = event_sender
        _external_input_rates[token] = max(8000, min(48000, int(input_rate or SAMPLE_RATE_HZ)))
        public = _public_call(call)
    return {"ok": True, "call": public}


async def unregister_external_media(endpoint: str, sender: Any = None) -> bool:
    token = _text(endpoint)
    async with _lock:
        current = _external_senders.get(token)
        if sender is None or current is sender:
            _external_senders.pop(token, None)
            _external_event_senders.pop(token, None)
            _external_input_rates.pop(token, None)
            return True
    return False


async def handle_device_message(selector: str, message_type: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    action = _text(message_type)
    if action == "sat_chat.start":
        return await start(selector, _text(payload.get("target")), mode=_text(payload.get("mode")) or "audio")
    if action == "sat_chat.end":
        return await end(call_id=_text(payload.get("call_id") or payload.get("id")), actor=selector)
    return None


async def reset_for_tests() -> None:
    global _comparison_calls
    tasks = []
    async with _lock:
        _calls.clear()
        _endpoint_calls.clear()
        _external_senders.clear()
        _external_event_senders.clear()
        _external_input_rates.clear()
        tasks = [task for relay in _video_relays.values() if isinstance(relay, dict) for task in _relay_tasks(relay)]
        _video_relays.clear()
        _comparison_calls = 0
    for task in tasks:
        if isinstance(task, asyncio.Task):
            task.cancel()
    if tasks:
        await asyncio.gather(*[task for task in tasks if isinstance(task, asyncio.Task)], return_exceptions=True)
