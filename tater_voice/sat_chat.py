from __future__ import annotations

import asyncio
import contextlib
import io
import struct
import time
import uuid
from typing import Any, Dict, Optional


SAMPLE_RATE_HZ = 16000
CHANNELS = 1
SAMPLE_FORMAT = "s16le"
VIDEO_FRAME_PREFIX = b"TSCV1"
MAX_VIDEO_FRAME_BYTES = 2 * 1024 * 1024

_lock = asyncio.Lock()
_calls: Dict[str, Dict[str, Any]] = {}
_endpoint_calls: Dict[str, str] = {}
_external_senders: Dict[str, Any] = {}
_external_event_senders: Dict[str, Any] = {}
_external_input_rates: Dict[str, int] = {}
_video_relays: Dict[tuple[str, str], Dict[str, Any]] = {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _video_receive_profile(participant: Optional[Dict[str, Any]]) -> str:
    board = _text((participant or {}).get("board")).lower().replace("_", "-")
    if board in {"checkers", "crown", "athena"}:
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
        "can_initiate": _truthy(capabilities.get("sat_chat_can_initiate")),
        "can_end": _truthy(capabilities.get("sat_chat_can_end")),
        "has_ui": _truthy(capabilities.get("sat_chat_ui")),
        "busy": selector in _endpoint_calls,
    }


async def _native_clients() -> Dict[str, Dict[str, Any]]:
    from . import native_satellite

    snapshot = await native_satellite.status()
    clients = snapshot.get("clients") if isinstance(snapshot.get("clients"), dict) else {}
    return {str(key): value for key, value in clients.items() if isinstance(value, dict)}


async def participants(*, viewer: str = "") -> Dict[str, Any]:
    viewer_token = _text(viewer)
    clients = await _native_clients()
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
        directory = await participants(viewer=selector)
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
    async with _lock:
        if caller_token in _endpoint_calls or target_token in _endpoint_calls:
            raise RuntimeError("One of these Sat Chat participants is already busy.")
        _calls[call_id] = call
        _endpoint_calls[caller_token] = call_id
        _endpoint_calls[target_token] = call_id
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
    video_tasks = []
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
            task = relay.get("task") if isinstance(relay, dict) else None
            if isinstance(task, asyncio.Task):
                video_tasks.append(task)
    current_task = asyncio.current_task()
    for task in video_tasks:
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


async def route_audio(source: str, data: bytes) -> bool:
    source_token = _text(source)
    if not data:
        return False
    async with _lock:
        call_id = _endpoint_calls.get(source_token, "")
        call = _calls.get(call_id)
        if not isinstance(call, dict) or call.get("state") != "active":
            return False
        caller = _text(call.get("caller"))
        target = _text(call.get("target"))
        peer = target if source_token == caller else caller
        source_rate = int(_external_input_rates.get(source_token) or SAMPLE_RATE_HZ)
        mode = _text(call.get("mode")).lower() or "audio"
        profiles = call.get("_video_profiles") if isinstance(call.get("_video_profiles"), dict) else {}
        video_profile = _text(profiles.get(peer))
    is_video = data.startswith(VIDEO_FRAME_PREFIX)
    if is_video:
        jpeg_size = len(data) - len(VIDEO_FRAME_PREFIX)
        if mode != "video" or jpeg_size < 4 or jpeg_size > MAX_VIDEO_FRAME_BYTES or data[len(VIDEO_FRAME_PREFIX):len(VIDEO_FRAME_PREFIX) + 2] != b"\xff\xd8":
            return True
        await _queue_video_frame(call_id, source_token, peer, bytes(data), profile=video_profile)
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
    from . import native_satellite

    try:
        await native_satellite.send_binary(peer, data)
    except Exception:
        await end(call_id=call_id, actor=source_token, reason="peer_disconnected", force=True)
    return True


async def _queue_video_frame(call_id: str, source: str, peer: str, data: bytes, *, profile: str = "") -> None:
    key = (_text(call_id), _text(peer))
    async with _lock:
        relay = _video_relays.get(key)
        task = relay.get("task") if isinstance(relay, dict) else None
        if not isinstance(relay, dict) or not isinstance(task, asyncio.Task) or task.done():
            queue: asyncio.Queue = asyncio.Queue(maxsize=1)
            task = asyncio.create_task(_video_relay_worker(key, source, peer, queue, profile=_text(profile)))
            relay = {"queue": queue, "task": task}
            _video_relays[key] = relay
        queue = relay["queue"]
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(data)


async def _video_relay_worker(
    key: tuple[str, str],
    source: str,
    peer: str,
    queue: asyncio.Queue,
    *,
    profile: str = "",
) -> None:
    try:
        while True:
            frame = await queue.get()
            if peer.startswith("native:"):
                from . import native_satellite

                if profile == "echo-low":
                    frame = await asyncio.to_thread(_resize_echo_video_frame, frame)
                elif profile == "echo-balanced":
                    frame = await asyncio.to_thread(
                        _resize_echo_video_frame,
                        frame,
                        max_size=(640, 360),
                        quality=62,
                    )
                await native_satellite.send_binary(peer, frame)
                continue
            async with _lock:
                sender = _external_senders.get(peer)
            if callable(sender):
                await sender(frame)
    except asyncio.CancelledError:
        raise
    except Exception:
        await end(call_id=key[0], actor=source, reason="media_disconnected", force=True)
    finally:
        async with _lock:
            relay = _video_relays.get(key)
            if isinstance(relay, dict) and relay.get("task") is asyncio.current_task():
                _video_relays.pop(key, None)


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
    tasks = []
    async with _lock:
        _calls.clear()
        _endpoint_calls.clear()
        _external_senders.clear()
        _external_event_senders.clear()
        _external_input_rates.clear()
        tasks = [relay.get("task") for relay in _video_relays.values() if isinstance(relay, dict)]
        _video_relays.clear()
    for task in tasks:
        if isinstance(task, asyncio.Task):
            task.cancel()
    if tasks:
        await asyncio.gather(*[task for task in tasks if isinstance(task, asyncio.Task)], return_exceptions=True)
