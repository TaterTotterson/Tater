from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import inspect
import json
import os
import secrets
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from fastapi import WebSocket, WebSocketDisconnect

MAX_LOG_ROWS = 200
PROTOCOL_VERSION = 1
DEFAULT_STALE_AFTER_S = 15.0
PAIRING_CODE_LEN = 6
PAIRING_CODE_TTL_S = 600
SETUP_STATES = {"provisioning", "setup", "setup_mode", "pairing"}
TOOL_TTS_KINDS = {"tool", "tool_progress"}
NATIVE_AUDIO_QUEUE_MAX_CHUNKS = 120
NATIVE_AUDIO_QUEUE_DRAIN_TIMEOUT_S = 2.0
NATIVE_WEBSOCKET_TASK_CANCEL_TIMEOUT_S = 1.0
NATIVE_WEBSOCKET_BRIDGE_CLOSE_TIMEOUT_S = 3.0
NATIVE_WEBSOCKET_DISCONNECT_TIMEOUT_S = 1.0
NATIVE_MEDIA_DISCONNECT_GRACE_S = 6.0

VOICE_EVENT_STATE = {
    "RUN_START": "listening",
    "STT_START": "listening",
    "STT_VAD_END": "thinking",
    "INTENT_START": "thinking",
    "INTENT_END": "thinking",
    "TOOL_CALL_START": "tool_call",
    "TTS_START": "speaking",
    "TTS_END": "speaking",
    "RUN_END": "idle",
    "ERROR": "error",
}

_clients_lock = asyncio.Lock()
_clients: Dict[str, Dict[str, Any]] = {}
_client_loop: Optional[asyncio.AbstractEventLoop] = None
_client_loop_lock = threading.RLock()
_state_change_lock = threading.RLock()
_state_change_listeners: list[Callable[[str, str], Any]] = []
_pairing_lock = threading.RLock()
_pairing_sessions: Dict[str, Dict[str, Any]] = {}
_stereo_sessions: Dict[str, Dict[str, Any]] = {}
_stereo_adjust_tasks: Dict[str, asyncio.Task] = {}
_media_disconnect_tasks: Dict[str, asyncio.Task] = {}

NATIVE_SELECTOR_ALIASES_KEY = "tater:voice:native_selector_aliases:v1"
NATIVE_MEDIA_RENDER_LATENCY_KEY = "tater:voice:native_media_render_latency:v1"

STEREO_CLOCK_PROBE_COUNT = 5
STEREO_START_LEAD_MS = 750
STEREO_CLOCK_REFRESH_S = 60.0
STEREO_ADJUST_INTERVAL_S = 2.0
STEREO_ADJUST_THRESHOLD_FRAMES = 48
STEREO_ADJUST_MAX_FRAMES = 96
STEREO_ADJUST_SETTLE_MS = 4000
STEREO_STARTUP_ADJUST_WINDOW_S = 10.0
STEREO_STARTUP_ADJUST_THRESHOLD_FRAMES = 24
STEREO_STARTUP_ADJUST_MAX_FRAMES = 240
STEREO_STARTUP_ADJUST_SETTLE_MS = 2000
STEREO_PHASE_EMA_ALPHA = 0.25
STEREO_PHASE_STABLE_SAMPLES = 2
MEDIA_RENDER_START_GUARD_MS = 250
MEDIA_RENDER_LATENCY_EMA_ALPHA = 0.25
MEDIA_RENDER_LATENCY_MAX_FRAMES = 24_000
MEDIA_RENDER_LATENCY_LEARN_SAMPLES = 3


def _vp():
    from . import voice_pipeline as vp

    return vp


def bind_runtime_loop(loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.AbstractEventLoop:
    """Bind native client state to the server loop that owns its WebSockets."""
    global _client_loop
    target = loop
    if target is None:
        try:
            target = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError("Native satellite runtime loop must be bound from async startup") from exc
    if target.is_closed():
        raise RuntimeError("Cannot bind native satellite runtime to a closed event loop")
    with _client_loop_lock:
        owner = _client_loop
        if owner is not None and owner is not target and owner.is_running() and not owner.is_closed():
            raise RuntimeError("Native satellite runtime is already bound to a different event loop")
        _client_loop = target
    return target


def release_runtime_loop(loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """Release the server loop binding during application shutdown."""
    global _client_loop
    for task in list(_media_disconnect_tasks.values()):
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
    _media_disconnect_tasks.clear()
    with _client_loop_lock:
        if loop is None or _client_loop is loop:
            _client_loop = None


def add_state_change_listener(listener: Callable[[str, str], Any]) -> None:
    if not callable(listener):
        raise ValueError("listener must be callable")
    with _state_change_lock:
        if listener not in _state_change_listeners:
            _state_change_listeners.append(listener)


def remove_state_change_listener(listener: Callable[[str, str], Any]) -> None:
    with _state_change_lock:
        if listener in _state_change_listeners:
            _state_change_listeners.remove(listener)


def _notify_state_change(event: str, selector: str = "") -> None:
    event_token = _text(event)
    selector_token = _text(selector)
    if not event_token:
        return
    with _state_change_lock:
        listeners = list(_state_change_listeners)
    for listener in listeners:
        try:
            listener(event_token, selector_token)
        except Exception:
            continue


def _runtime_loop() -> Optional[asyncio.AbstractEventLoop]:
    with _client_loop_lock:
        loop = _client_loop
    if loop is not None and loop.is_running() and not loop.is_closed():
        return loop
    return None


def _close_awaitable(awaitable: Any) -> None:
    if inspect.iscoroutine(awaitable):
        awaitable.close()


def run_on_runtime_loop(awaitable: Any, *, timeout: float = 20.0) -> Any:
    """Run native client work on the loop that owns its locks and WebSockets."""
    owner = _runtime_loop()
    try:
        current = asyncio.get_running_loop()
    except RuntimeError:
        current = None

    if owner is current and current is not None:
        _close_awaitable(awaitable)
        raise RuntimeError("Cannot synchronously wait for native satellite work on its runtime loop")

    timeout_seconds = max(0.1, float(timeout or 20.0))
    if owner is not None:
        future = asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(awaitable, timeout=timeout_seconds),
            owner,
        )
        try:
            return future.result(timeout=timeout_seconds + 1.0)
        except TimeoutError:
            future.cancel()
            raise TimeoutError("Timed out waiting for native satellite action") from None

    if current is not None and current.is_running():
        _close_awaitable(awaitable)
        raise RuntimeError("Native satellite runtime loop has not been bound")
    return asyncio.run(asyncio.wait_for(awaitable, timeout=timeout_seconds))


def _text(value: Any) -> str:
    return str(value or "").strip()


def _hardware_id(value: Any) -> str:
    token = "".join(char for char in _text(value).lower() if char in "0123456789abcdef")
    return token if len(token) == 12 else ""


def _selector_mac_suffix(value: Any) -> str:
    token = _text(value).lower().rsplit("-", 1)[-1]
    return token if len(token) == 6 and all(char in "0123456789abcdef" for char in token) else ""


def _same_native_hardware(
    old_selector: Any,
    old_hardware_id: Any,
    new_selector: Any,
    new_hardware_id: Any,
) -> bool:
    old_hardware = _hardware_id(old_hardware_id)
    new_hardware = _hardware_id(new_hardware_id)
    if old_hardware and new_hardware:
        return old_hardware == new_hardware
    old_suffix = _selector_mac_suffix(old_selector)
    new_suffix = _selector_mac_suffix(new_selector)
    return bool(old_suffix and new_suffix and old_suffix == new_suffix)


def _load_selector_aliases() -> Dict[str, str]:
    try:
        raw = _vp().redis_client.get(NATIVE_SELECTOR_ALIASES_KEY)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(str(raw)) if raw else {}
    except Exception:
        parsed = {}
    return {
        _text(key): _text(value)
        for key, value in (parsed.items() if isinstance(parsed, dict) else [])
        if _text(key) and _text(value)
    }


def _save_selector_alias(old_selector: Any, new_selector: Any) -> None:
    old_token = _text(old_selector)
    new_token = _text(new_selector)
    if not old_token or not new_token or old_token == new_token:
        return
    aliases = _load_selector_aliases()
    aliases[old_token] = new_token
    with contextlib.suppress(Exception):
        _vp().redis_client.set(NATIVE_SELECTOR_ALIASES_KEY, json.dumps(aliases, ensure_ascii=False))


def _load_media_render_latencies() -> Dict[str, Dict[str, Any]]:
    try:
        raw = _vp().redis_client.get(NATIVE_MEDIA_RENDER_LATENCY_KEY)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(str(raw)) if raw else {}
    except Exception:
        parsed = {}
    return {
        _text(selector): {
            "frames": max(
                0,
                min(
                    MEDIA_RENDER_LATENCY_MAX_FRAMES,
                    _as_int(row.get("frames"), 0),
                ),
            ),
            "samples": max(0, _as_int(row.get("samples"), 0)),
            "updated_ts": float(row.get("updated_ts") or 0.0),
        }
        for selector, row in (parsed.items() if isinstance(parsed, dict) else [])
        if _text(selector) and isinstance(row, dict)
    }


def _learned_media_render_latency_frames(selector: Any) -> int:
    row = _load_media_render_latencies().get(_canonical_selector(selector), {})
    if not isinstance(row, dict) or _as_int(row.get("samples"), 0) <= 0:
        return 0
    return max(
        0,
        min(MEDIA_RENDER_LATENCY_MAX_FRAMES, _as_int(row.get("frames"), 0)),
    )


def _record_media_render_latency(selector: Any, observed_frames: int) -> int:
    token = _canonical_selector(selector)
    observed = max(0, min(MEDIA_RENDER_LATENCY_MAX_FRAMES, int(observed_frames)))
    if not token or observed <= 0:
        return 0
    profiles = _load_media_render_latencies()
    previous = profiles.get(token) if isinstance(profiles.get(token), dict) else {}
    previous_samples = max(0, _as_int(previous.get("samples"), 0))
    previous_frames = max(0, _as_int(previous.get("frames"), observed))
    learned = observed if previous_samples <= 0 else int(
        round(
            ((1.0 - MEDIA_RENDER_LATENCY_EMA_ALPHA) * previous_frames)
            + (MEDIA_RENDER_LATENCY_EMA_ALPHA * observed)
        )
    )
    profiles[token] = {
        "frames": max(0, min(MEDIA_RENDER_LATENCY_MAX_FRAMES, learned)),
        "samples": previous_samples + 1,
        "updated_ts": _now(),
    }
    with contextlib.suppress(Exception):
        _vp().redis_client.set(
            NATIVE_MEDIA_RENDER_LATENCY_KEY,
            json.dumps(profiles, ensure_ascii=False, separators=(",", ":")),
        )
    return learned


def _remove_selector_aliases(selector: Any) -> int:
    token = _text(selector)
    if not token:
        return 0
    aliases = _load_selector_aliases()
    remove_keys: set[str] = set()
    for alias in aliases:
        current = alias
        seen: set[str] = set()
        while current and current not in seen:
            if current == token:
                remove_keys.add(alias)
                break
            seen.add(current)
            current = _text(aliases.get(current))
    if not remove_keys:
        return 0
    cleaned = {key: value for key, value in aliases.items() if key not in remove_keys}
    try:
        _vp().redis_client.set(
            NATIVE_SELECTOR_ALIASES_KEY,
            json.dumps(cleaned, ensure_ascii=False),
        )
    except Exception:
        return 0
    return len(remove_keys)


def _canonical_selector(selector: Any) -> str:
    token = _text(selector)
    aliases = _load_selector_aliases()
    seen: set[str] = set()
    while token in aliases and token not in seen:
        seen.add(token)
        token = _text(aliases.get(token)) or token
    return token


def _default_name_for_board(board: Any) -> str:
    token = _text(board).lower().replace("_", "-")
    return {
        "satellite1": "Tater Sat1",
        "respeaker-xvf3800": "Tater ReSpeaker XVF3800",
        "s3-box": "Tater S3 Box",
    }.get(token, "Tater Voice PE" if token == "voice-pe" else "")


def _device_name_from_hello(payload: Dict[str, Any], fallback: Any = "") -> str:
    name = _text(payload.get("device_name") or payload.get("name"))
    board = _text(payload.get("board"))
    if name == "Tater Voice PE" and board and board != "voice-pe":
        name = _default_name_for_board(board) or name
    return name or _text(fallback)


def _lower(value: Any) -> str:
    return _text(value).lower()


def _tool_visual_requested(data: Dict[str, Any], tts_kind: str = "") -> bool:
    kind = _lower(tts_kind or data.get("tts_kind"))
    visual_mode = _lower(data.get("visual_mode"))
    state_after = _lower(data.get("state_after"))
    return kind in TOOL_TTS_KINDS or visual_mode == "tool_call" or state_after == "tool_call"


def _now() -> float:
    return time.time()


def _pairing_ttl_s() -> int:
    try:
        return max(60, min(3600, int(float(os.getenv("TATER_NATIVE_PAIRING_TTL_S", PAIRING_CODE_TTL_S)))))
    except Exception:
        return PAIRING_CODE_TTL_S


def _stale_after_s() -> float:
    try:
        return max(5.0, float(os.getenv("TATER_NATIVE_SATELLITE_STALE_S", DEFAULT_STALE_AFTER_S)))
    except Exception:
        return DEFAULT_STALE_AFTER_S


def _status_state(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    return _lower(payload.get("state") or payload.get("mode"))


def _is_setup_state(payload: Dict[str, Any]) -> bool:
    return _status_state(payload) in SETUP_STATES


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _lower(value)
    if not token:
        return bool(default)
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(
    value: Any,
    default: int = 0,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    if minimum is not None:
        parsed = max(int(minimum), parsed)
    if maximum is not None:
        parsed = min(int(maximum), parsed)
    return parsed


def _envelope(message_type: str, payload: Optional[Dict[str, Any]] = None, *, message_id: str = "", session_id: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "type": message_type,
        "id": message_id or uuid.uuid4().hex,
        "ts": _now(),
        "payload": payload if isinstance(payload, dict) else {},
    }
    if session_id:
        out["session_id"] = session_id
    return out


def _parse_json_text(text: Any) -> Dict[str, Any]:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    parsed = json.loads(str(text or "{}"))
    if not isinstance(parsed, dict):
        raise ValueError("message must be a JSON object")
    return parsed


def _message_payload(message: Dict[str, Any]) -> Dict[str, Any]:
    payload = message.get("payload")
    return payload if isinstance(payload, dict) else {}


def _message_type(message: Dict[str, Any]) -> str:
    return _text(message.get("type"))


def _credentials_path() -> Path:
    raw = _text(os.getenv("TATER_NATIVE_SATELLITE_CREDENTIALS_PATH"))
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".taterassistant" / "native_satellite_credentials.json"


def _allow_unpaired_open_connections() -> bool:
    return _as_bool(os.getenv("TATER_NATIVE_SATELLITE_ALLOW_UNPAIRED"), False)


def _load_credentials_unlocked() -> Dict[str, Any]:
    path = _credentials_path()
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except FileNotFoundError:
        return {"version": 1, "devices": {}}
    except Exception:
        return {"version": 1, "devices": {}}
    if not isinstance(data, dict):
        data = {}
    devices = data.get("devices")
    if not isinstance(devices, dict):
        devices = {}
    return {"version": 1, "devices": devices}


def _save_credentials_unlocked(data: Dict[str, Any]) -> None:
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "version": 1,
        "updated_ts": _now(),
        "devices": data.get("devices") if isinstance(data.get("devices"), dict) else {},
    }
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _token_hash(token: Any) -> str:
    value = _text(token)
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_pairing_code(value: Any) -> str:
    return "".join(ch for ch in _text(value) if ch.isdigit())


def _new_pairing_code() -> str:
    upper = 10 ** PAIRING_CODE_LEN
    return f"{secrets.randbelow(upper):0{PAIRING_CODE_LEN}d}"


def _display_pairing_code(code: str) -> str:
    token = _normalize_pairing_code(code)
    return " ".join(token[idx : idx + 3] for idx in range(0, len(token), 3))


def _new_device_token() -> str:
    return "tns_" + secrets.token_urlsafe(32)


def _pairing_public_unlocked(session: Dict[str, Any]) -> Dict[str, Any]:
    now_ts = _now()
    expires_ts = float(session.get("expires_ts") or 0.0)
    state = _text(session.get("state")) or "waiting"
    if state == "waiting" and expires_ts > 0 and now_ts >= expires_ts:
        session["state"] = "expired"
        state = "expired"
    return {
        "ok": True,
        "pairing_id": _text(session.get("id")),
        "code": _text(session.get("display_code")),
        "display_code": _text(session.get("display_code")),
        "expires_at": expires_ts,
        "expires_in_s": max(0, int(expires_ts - now_ts)) if expires_ts else 0,
        "state": state,
        "paired": state == "paired",
        "expired": state == "expired",
        "selector": _text(session.get("selector")),
        "device_id": _text(session.get("device_id")),
        "device_name": _text(session.get("device_name")),
    }


def _prune_pairing_sessions_unlocked() -> None:
    now_ts = _now()
    stale_cutoff = now_ts - 3600.0
    for session_id, session in list(_pairing_sessions.items()):
        if not isinstance(session, dict):
            _pairing_sessions.pop(session_id, None)
            continue
        expires_ts = float(session.get("expires_ts") or 0.0)
        if _text(session.get("state")) == "waiting" and expires_ts > 0 and now_ts >= expires_ts:
            session["state"] = "expired"
        if _text(session.get("state")) != "waiting" and expires_ts < stale_cutoff:
            _pairing_sessions.pop(session_id, None)


def start_pairing_session() -> Dict[str, Any]:
    ttl_s = _pairing_ttl_s()
    with _pairing_lock:
        _prune_pairing_sessions_unlocked()
        active_hashes = {
            _text(row.get("code_hash"))
            for row in _pairing_sessions.values()
            if isinstance(row, dict) and _text(row.get("state")) == "waiting"
        }
        code = _new_pairing_code()
        while _token_hash(code) in active_hashes:
            code = _new_pairing_code()
        session_id = uuid.uuid4().hex
        session = {
            "id": session_id,
            "code_hash": _token_hash(code),
            "display_code": _display_pairing_code(code),
            "created_ts": _now(),
            "expires_ts": _now() + ttl_s,
            "state": "waiting",
        }
        _pairing_sessions[session_id] = session
        return _pairing_public_unlocked(session)


def pairing_status(pairing_id: str) -> Dict[str, Any]:
    token = _text(pairing_id)
    if not token:
        raise ValueError("pairing_id is required")
    with _pairing_lock:
        _prune_pairing_sessions_unlocked()
        session = _pairing_sessions.get(token)
        if not isinstance(session, dict):
            return {"ok": True, "pairing_id": token, "state": "expired", "paired": False, "expired": True, "expires_in_s": 0}
        return _pairing_public_unlocked(session)


def _credential_row(selector: str, payload: Dict[str, Any], token_hash: str) -> Dict[str, Any]:
    return {
        "selector": selector,
        "device_id": _text(payload.get("device_id") or payload.get("id") or selector),
        "hardware_id": _hardware_id(payload.get("hardware_id")),
        "device_name": _device_name_from_hello(payload, selector),
        "board": _text(payload.get("board")),
        "firmware_version": _text(payload.get("firmware_version")),
        "room": _text(payload.get("room") or payload.get("area_name") or payload.get("room_name")),
        "token_hash": token_hash,
        "created_ts": _now(),
        "last_seen_ts": _now(),
    }


def _save_device_credential(selector: str, payload: Dict[str, Any], device_token: str) -> None:
    token_hash = _token_hash(device_token)
    if not token_hash:
        raise ValueError("device token is empty")
    with _pairing_lock:
        data = _load_credentials_unlocked()
        devices = data.get("devices")
        if not isinstance(devices, dict):
            devices = {}
            data["devices"] = devices
        existing = devices.get(selector) if isinstance(devices.get(selector), dict) else {}
        row = _credential_row(selector, payload, token_hash)
        if existing.get("created_ts"):
            row["created_ts"] = existing.get("created_ts")
        devices[selector] = row
        _save_credentials_unlocked(data)


def _remove_device_credentials(selector: Any) -> int:
    token = _text(selector)
    if not token:
        return 0
    with _pairing_lock:
        data = _load_credentials_unlocked()
        devices = data.get("devices") if isinstance(data.get("devices"), dict) else {}
        remove_keys = [
            key
            for key, row in devices.items()
            if _text(key) == token
            or (isinstance(row, dict) and _text(row.get("selector")) == token)
        ]
        if not remove_keys:
            return 0
        for key in remove_keys:
            devices.pop(key, None)
        data["devices"] = devices
        _save_credentials_unlocked(data)
        return len(remove_keys)


def _valid_device_credential(token: str, selector: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    supplied_hash = _token_hash(token)
    if not supplied_hash:
        return None
    device_id = _text(payload.get("device_id") or payload.get("id"))
    hardware_id = _hardware_id(payload.get("hardware_id"))
    with _pairing_lock:
        data = _load_credentials_unlocked()
        devices = data.get("devices") if isinstance(data.get("devices"), dict) else {}
        matched_key = ""
        matched_row: Optional[Dict[str, Any]] = None
        for key, row in list(devices.items()):
            if not isinstance(row, dict):
                continue
            row_hash = _text(row.get("token_hash"))
            if not row_hash or not hmac.compare_digest(row_hash, supplied_hash):
                continue
            row_selector = _text(row.get("selector") or key)
            row_device_id = _text(row.get("device_id"))
            if selector and row_selector and row_selector != selector:
                if (
                    not device_id
                    or row_device_id != device_id
                ) and not _same_native_hardware(
                    row_selector,
                    row.get("hardware_id"),
                    selector,
                    hardware_id,
                ):
                    continue
            matched_key = _text(key) or selector
            matched_row = dict(row)
            break
        if not matched_row:
            return None
        row = devices.get(matched_key)
        if isinstance(row, dict):
            row.update(
                {
                    "selector": selector or _text(row.get("selector")),
                    "device_id": device_id or _text(row.get("device_id")),
                    "hardware_id": hardware_id or _hardware_id(row.get("hardware_id")),
                    "device_name": _device_name_from_hello(payload, row.get("device_name")),
                    "board": _text(payload.get("board")) or _text(row.get("board")),
                    "firmware_version": _text(payload.get("firmware_version")) or _text(row.get("firmware_version")),
                    "last_seen_ts": _now(),
                }
            )
            if selector and matched_key != selector:
                devices.pop(matched_key, None)
                devices[selector] = row
            _save_credentials_unlocked(data)
            matched_row = dict(row)
        return matched_row


def _redeem_pairing_code(token: str, selector: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    code = _normalize_pairing_code(token)
    if len(code) != PAIRING_CODE_LEN:
        return None
    code_hash = _token_hash(code)
    with _pairing_lock:
        _prune_pairing_sessions_unlocked()
        session: Optional[Dict[str, Any]] = None
        for row in _pairing_sessions.values():
            if not isinstance(row, dict):
                continue
            if _text(row.get("state")) != "waiting":
                continue
            if hmac.compare_digest(_text(row.get("code_hash")), code_hash):
                session = row
                break
        if not session:
            return None
        device_token = _new_device_token()
        _save_device_credential(selector, payload, device_token)
        session["state"] = "paired"
        session["selector"] = selector
        session["device_id"] = _text(payload.get("device_id") or payload.get("id") or selector)
        session["device_name"] = _device_name_from_hello(payload, selector)
        session["paired_ts"] = _now()
        session["expires_ts"] = _now() + 30.0
        return {
            "mode": "paired",
            "pairing_id": _text(session.get("id")),
            "device_token": device_token,
            "selector": selector,
            "device_id": _text(session.get("device_id")),
            "device_name": _text(session.get("device_name")),
        }


def _event_name(event_type: Any) -> str:
    name = _text(getattr(event_type, "name", "")) or _text(event_type)
    if "." in name:
        name = name.rsplit(".", 1)[-1]
    if name.startswith("VOICE_ASSISTANT_"):
        name = name[len("VOICE_ASSISTANT_") :]
    return name.upper()


def _queue_command(queue: asyncio.Queue, message: Dict[str, Any]) -> None:
    try:
        queue.put_nowait(message)
        return
    except asyncio.QueueFull:
        with contextlib.suppress(Exception):
            queue.get_nowait()
    with contextlib.suppress(asyncio.QueueFull):
        queue.put_nowait(message)


def _capabilities(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("capabilities")
    if not isinstance(raw, dict):
        return {}
    capabilities: Dict[str, Any] = {}
    for key, value in raw.items():
        name = _text(key)
        if not name:
            continue
        if isinstance(value, str) and _lower(value) in {
            "0",
            "1",
            "true",
            "false",
            "yes",
            "no",
            "on",
            "off",
            "enabled",
            "disabled",
        }:
            capabilities[name] = _as_bool(value)
        elif isinstance(value, (bool, int, float, str)):
            capabilities[name] = value
    return capabilities


def _satellite_ducking_payload() -> Dict[str, int]:
    try:
        from speech_settings import get_speech_settings

        settings = get_speech_settings()
    except Exception:
        settings = {}

    return {
        "target_percent": _as_int(
            settings.get("satellite_ducking_target_percent"),
            20,
            minimum=0,
            maximum=100,
        ),
        "attack_ms": _as_int(
            settings.get("satellite_ducking_attack_ms"),
            150,
            minimum=0,
            maximum=10000,
        ),
        "release_ms": _as_int(
            settings.get("satellite_ducking_release_ms"),
            350,
            minimum=0,
            maximum=10000,
        ),
    }


def _live_settings_payload(selector: str = "", *, board: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    return native_live_settings.settings_snapshot(selector, board=board)


def _firmware_settings_payload(selector: str = "", *, board: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    return native_live_settings.firmware_settings_snapshot(selector, board=board)


async def _handle_wake_verifier_packet(
    selector: str,
    data: bytes,
    queue: asyncio.Queue,
    websocket: WebSocket,
) -> None:
    from . import wake_verifier

    result = await wake_verifier.verify_packet(data, selector=selector)
    async with _clients_lock:
        row = _clients.get(selector)
        if isinstance(row, dict) and row.get("websocket") is websocket:
            row["wake_verifier_count"] = int(row.get("wake_verifier_count") or 0) + 1
            if not bool(result.get("accepted")):
                row["wake_verifier_rejections"] = int(row.get("wake_verifier_rejections") or 0) + 1
            row["wake_verifier_last"] = dict(result)
            row["last_seen_ts"] = _now()
            row["last_message_type"] = "wake.verify"
    _vp().logger.info(
        "[wake-verifier] selector=%s request=%s mode=%s engine=%s selected_engine=%s accepted=%s available=%s score=%.3f stt_ms=%.1f total_ms=%.1f transcript=%r reason=%s",
        selector,
        result.get("request_id"),
        "enforce" if result.get("enforce") else "observe",
        _text(result.get("stt_engine")),
        _text(result.get("stt_engine_selected")),
        bool(result.get("accepted")),
        bool(result.get("available")),
        float(result.get("score") or 0.0),
        float(result.get("stt_ms") or 0.0),
        float(result.get("total_ms") or 0.0),
        _text(result.get("transcript")),
        _text(result.get("reason")),
    )
    _queue_command(queue, _envelope("wake.verify.result", result))
    try:
        await _vp().run_background(
            _vp()._voice_metrics_record_wake_verification,
            selector,
            result,
        )
    except Exception as exc:
        _vp().logger.debug("[wake-verifier] metrics persist skipped selector=%s: %s", selector, exc)


class _NativeVoiceAssistantEventType:
    VOICE_ASSISTANT_RUN_START = "RUN_START"
    RUN_START = "RUN_START"
    VOICE_ASSISTANT_RUN_END = "RUN_END"
    RUN_END = "RUN_END"
    VOICE_ASSISTANT_STT_START = "STT_START"
    STT_START = "STT_START"
    VOICE_ASSISTANT_STT_VAD_END = "STT_VAD_END"
    STT_VAD_END = "STT_VAD_END"
    VOICE_ASSISTANT_STT_END = "STT_END"
    STT_END = "STT_END"
    VOICE_ASSISTANT_INTENT_START = "INTENT_START"
    INTENT_START = "INTENT_START"
    VOICE_ASSISTANT_INTENT_END = "INTENT_END"
    INTENT_END = "INTENT_END"
    VOICE_ASSISTANT_TOOL_CALL_START = "TOOL_CALL_START"
    TOOL_CALL_START = "TOOL_CALL_START"
    VOICE_ASSISTANT_TOOL_CALL_END = "TOOL_CALL_END"
    TOOL_CALL_END = "TOOL_CALL_END"
    VOICE_ASSISTANT_TTS_START = "TTS_START"
    TTS_START = "TTS_START"
    VOICE_ASSISTANT_TTS_END = "TTS_END"
    TTS_END = "TTS_END"
    VOICE_ASSISTANT_ERROR = "ERROR"
    ERROR = "ERROR"


class _NativeVoiceAssistantModule:
    VoiceAssistantEventType = _NativeVoiceAssistantEventType


class _NativeVoiceAssistantClient:
    def __init__(
        self,
        selector: str,
        queue: asyncio.Queue,
        on_inactive: Optional[Callable[[], None]] = None,
    ):
        self.address = selector
        self._selector = selector
        self._queue = queue
        self._on_inactive = on_inactive
        self._callbacks: Dict[str, Optional[Callable[..., Any]]] = {}
        self._connected = True
        self._tts_kind = ""

    def is_connected(self) -> bool:
        return bool(self._connected)

    def disconnect(self) -> None:
        self._connected = False

    def callback(self, name: str) -> Optional[Callable[..., Any]]:
        cb = self._callbacks.get(name)
        return cb if callable(cb) else None

    def subscribe_voice_assistant(
        self,
        *,
        handle_start: Callable[..., Any],
        handle_stop: Callable[..., Any],
        handle_audio: Optional[Callable[..., Any]] = None,
        handle_announcement_finished: Optional[Callable[..., Any]] = None,
    ) -> Callable[[], None]:
        self._callbacks = {
            "handle_start": handle_start,
            "handle_stop": handle_stop,
            "handle_audio": handle_audio,
            "handle_announcement_finished": handle_announcement_finished,
        }

        def _unsubscribe() -> None:
            self._callbacks.clear()

        return _unsubscribe

    async def send_voice_assistant_event(self, event_type: Any, payload: Optional[Dict[str, Any]] = None) -> None:
        event = _event_name(event_type)
        data = payload if isinstance(payload, dict) else {}
        if event == "TTS_START":
            self._tts_kind = _text(data.get("tts_kind"))
        tts_kind = _text(data.get("tts_kind")) or self._tts_kind
        tool_visual = _tool_visual_requested(data, tts_kind)
        _queue_command(self._queue, _envelope("voice.event", {"event": event, "data": data}))

        state = VOICE_EVENT_STATE.get(event)
        if state == "speaking" and tool_visual:
            state = "tool_call"
        if state:
            _queue_command(self._queue, _envelope("state", {"state": state, "event": event}))

        url = _text(data.get("url"))
        if event == "TTS_END" and url:
            play_payload: Dict[str, Any] = {
                "url": url,
                "ducking": _satellite_ducking_payload(),
            }
            if tts_kind:
                play_payload["tts_kind"] = tts_kind
            if tool_visual:
                play_payload["state_after"] = "tool_call"
                play_payload["visual_mode"] = "tool_call"
            _queue_command(self._queue, _envelope("play.url", play_payload))
            self._tts_kind = ""
        elif event in {"RUN_END", "ERROR"}:
            self._tts_kind = ""
            if callable(self._on_inactive):
                with contextlib.suppress(Exception):
                    self._on_inactive()


class _NativeVoicePipelineBridge:
    def __init__(self, selector: str, queue: asyncio.Queue):
        self.selector = selector
        self.client = _NativeVoiceAssistantClient(selector, queue, self._mark_inactive)
        self.module = _NativeVoiceAssistantModule()
        self.unsubscribe: Optional[Callable[[], None]] = None
        self._audio_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=NATIVE_AUDIO_QUEUE_MAX_CHUNKS)
        self._audio_task: Optional[asyncio.Task] = None
        self._audio_drops = 0
        self._audio_drain_timeouts = 0
        self._audio_high_water = 0
        self._last_audio_drop_log_ts = 0.0
        self.active = False

    def _mark_inactive(self) -> None:
        self.active = False

    def _ensure_audio_task(self) -> None:
        if self._audio_task is None or self._audio_task.done():
            self._audio_task = asyncio.create_task(self._audio_worker())

    async def _audio_worker(self) -> None:
        while True:
            data = await self._audio_queue.get()
            try:
                await self._process_audio(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                _vp().logger.warning(
                    "[native-satellite] audio worker failed selector=%s depth=%s",
                    self.selector,
                    self._audio_queue.qsize(),
                    exc_info=True,
                )
            finally:
                self._audio_queue.task_done()

    async def _process_audio(self, data: bytes) -> None:
        cb = self.client.callback("handle_audio")
        if not callable(cb):
            return
        result = cb(bytes(data or b""))
        if hasattr(result, "__await__"):
            await result

    async def _wait_audio_drained(self) -> None:
        if self._audio_queue.empty():
            return
        try:
            await asyncio.wait_for(self._audio_queue.join(), timeout=NATIVE_AUDIO_QUEUE_DRAIN_TIMEOUT_S)
        except asyncio.TimeoutError:
            self._audio_drain_timeouts += 1
            _vp().logger.warning(
                "[native-satellite] audio queue drain timeout selector=%s depth=%s drops=%s timeouts=%s",
                self.selector,
                self._audio_queue.qsize(),
                self._audio_drops,
                self._audio_drain_timeouts,
            )

    def _clear_audio_queue(self) -> None:
        while True:
            try:
                self._audio_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._audio_queue.task_done()

    async def start(self) -> None:
        if self.unsubscribe is not None:
            return
        self.unsubscribe = await _vp()._esphome_subscribe_voice_assistant(
            self.selector,
            self.client,
            self.module,
            api_audio_supported=True,
        )
        self._ensure_audio_task()

    async def voice_start(self, payload: Dict[str, Any]) -> Optional[int]:
        cb = self.client.callback("handle_start")
        if not callable(cb):
            raise RuntimeError("Voice pipeline is not subscribed.")
        self._clear_audio_queue()
        self._ensure_audio_task()
        audio_settings = _audio_settings_from_payload(payload)
        flags = _as_int(payload.get("request_flags", payload.get("flags")), 0)
        wake_word = _text(
            payload.get("wake_word")
            or payload.get("wake_word_phrase")
            or payload.get("wake_phrase")
        )
        conversation_id = _text(payload.get("conversation_id") or payload.get("conversation"))
        result = cb(conversation_id, flags, audio_settings, wake_word or None)
        if hasattr(result, "__await__"):
            result = await result
        self.active = result is not None
        return result

    async def audio(self, data: bytes) -> None:
        payload = bytes(data or b"")
        if not payload:
            return
        self._ensure_audio_task()
        try:
            self._audio_queue.put_nowait(payload)
            depth = self._audio_queue.qsize()
            if depth > self._audio_high_water:
                self._audio_high_water = depth
            return
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._audio_queue.get_nowait()
                self._audio_queue.task_done()
            self._audio_drops += 1
            with contextlib.suppress(asyncio.QueueFull):
                self._audio_queue.put_nowait(payload)
            depth = self._audio_queue.qsize()
            if depth > self._audio_high_water:
                self._audio_high_water = depth
            now_ts = _now()
            if now_ts - self._last_audio_drop_log_ts >= 2.0:
                self._last_audio_drop_log_ts = now_ts
                _vp().logger.warning(
                    "[native-satellite] audio queue overflow selector=%s depth=%s drops=%s",
                    self.selector,
                    self._audio_queue.qsize(),
                    self._audio_drops,
                )

    def audio_stats(self) -> Dict[str, Any]:
        return {
            "queue_depth": self._audio_queue.qsize(),
            "queue_capacity": self._audio_queue.maxsize,
            "queue_high_water": self._audio_high_water,
            "queue_drops": self._audio_drops,
            "queue_drain_timeouts": self._audio_drain_timeouts,
        }

    async def voice_stop(self, payload: Dict[str, Any]) -> None:
        cb = self.client.callback("handle_stop")
        if not callable(cb):
            return
        await self._wait_audio_drained()
        abort = _as_bool(payload.get("abort"), False)
        result = cb(abort)
        if hasattr(result, "__await__"):
            await result
        self.active = False

    async def announcement_finished(self) -> None:
        cb = self.client.callback("handle_announcement_finished")
        if not callable(cb):
            return
        result = cb()
        if hasattr(result, "__await__"):
            await result

    async def close(self) -> None:
        if self.active:
            with contextlib.suppress(Exception):
                await self.voice_stop({"abort": True})
        if self.unsubscribe is not None:
            with contextlib.suppress(Exception):
                self.unsubscribe()
            self.unsubscribe = None
        if self._audio_task is not None:
            self._audio_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._audio_task
            self._audio_task = None
        self._clear_audio_queue()
        self.client.disconnect()
        self.active = False


def _audio_settings_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    body = payload if isinstance(payload, dict) else {}
    nested = body.get("audio_format")
    out: Dict[str, Any] = dict(nested) if isinstance(nested, dict) else {}
    aliases = (
        "rate",
        "sample_rate",
        "sample_rate_hz",
        "width",
        "sample_width",
        "sample_width_bytes",
        "channels",
        "num_channels",
    )
    for key in aliases:
        if key in body and body.get(key) is not None:
            out[key] = body.get(key)
    return out


def _voice_snapshot(selector: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"active": False}
    with contextlib.suppress(Exception):
        runtime = _vp()._selector_runtime(selector)
        session = runtime.get("session") if isinstance(runtime, dict) else None
        session_type = getattr(_vp(), "VoiceSessionRuntime", None)
        if session_type is not None and isinstance(session, session_type):
            out.update(
                {
                    "active": True,
                    "session_id": _text(getattr(session, "session_id", "")),
                    "conversation_id": _text(getattr(session, "conversation_id", "")),
                    "state": _text(getattr(session, "state", "")),
                    "wake_word": _text(getattr(session, "wake_word", "")),
                    "audio_chunks": int(getattr(session, "audio_chunks", 0) or 0),
                    "audio_bytes": int(getattr(session, "audio_bytes", 0) or 0),
                    "processing": bool(getattr(session, "processing", False)),
                    "awaiting_announcement": bool(runtime.get("awaiting_announcement")),
                }
            )
    return out


def _selector_from_hello(payload: Dict[str, Any], websocket: WebSocket) -> str:
    for key in ("device_id", "id", "selector"):
        value = _text(payload.get(key))
        if value:
            return f"native:{value}" if not value.startswith("native:") else value
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    fallback = _text(client_host) or uuid.uuid4().hex[:12]
    return f"native:{fallback}"


def _auth_enabled() -> bool:
    settings = _vp()._voice_settings()
    return _as_bool(settings.get("API_AUTH_ENABLED"), False)


def _expected_auth_token() -> str:
    settings = _vp()._voice_settings()
    return _text(settings.get("API_AUTH_KEY") or os.getenv("TATER_NATIVE_SATELLITE_TOKEN"))


def _auth_token_from_websocket(websocket: WebSocket) -> str:
    auth_header = _text(websocket.headers.get("authorization"))
    if auth_header.lower().startswith("bearer "):
        return auth_header.split(None, 1)[1].strip()
    for key in ("x-tater-token", "x-tater-satellite-token"):
        token = _text(websocket.headers.get(key))
        if token:
            return token
    return _text(websocket.query_params.get("token"))


async def _require_websocket_auth(websocket: WebSocket) -> Optional[str]:
    if not _auth_enabled():
        return None
    expected = _expected_auth_token()
    if not expected:
        return "API auth is enabled but no satellite/API token is configured."
    if _auth_token_from_websocket(websocket) != expected:
        return "Invalid or missing satellite token."
    return None


def _authorize_websocket_hello(websocket: WebSocket, selector: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    supplied = _auth_token_from_websocket(websocket)
    expected = _expected_auth_token()
    if expected and supplied and hmac.compare_digest(supplied, expected):
        return {"ok": True, "mode": "api_key"}

    credential = _valid_device_credential(supplied, selector, payload) if supplied else None
    if credential:
        return {"ok": True, "mode": "device_token", "credential": credential}

    pairing = _redeem_pairing_code(supplied, selector, payload) if supplied else None
    if pairing:
        return {"ok": True, **pairing}

    if _allow_unpaired_open_connections():
        return {"ok": True, "mode": "open"}

    if not supplied:
        return {"ok": False, "error": "Missing satellite pairing code or device token."}
    return {"ok": False, "error": "Invalid satellite token or expired pairing code."}


def _registry_metadata_from_hello(payload: Dict[str, Any], *, connected: bool) -> Dict[str, Any]:
    board = _text(payload.get("board"))
    room = _text(payload.get("room") or payload.get("area_name") or payload.get("room_name"))
    caps = _capabilities(payload)
    meta: Dict[str, Any] = {
        "native_selected": True,
        "native_connected": connected,
        "native_protocol": PROTOCOL_VERSION,
        "native_transport": "websocket",
        "native_last_seen_ts": _now(),
        "firmware_version": _text(payload.get("firmware_version")),
        "board": board,
        "device_id": _text(payload.get("device_id") or payload.get("id")),
        "hardware_id": _hardware_id(payload.get("hardware_id")),
        "capabilities": caps,
    }
    if room:
        meta["room"] = room
        meta["room_name"] = room
        meta["area_name"] = room
    return meta


def _upsert_registry_from_hello(selector: str, payload: Dict[str, Any], *, connected: bool) -> Dict[str, Any]:
    name = _device_name_from_hello(payload, selector)
    hardware_id = _hardware_id(payload.get("hardware_id"))
    with contextlib.suppress(Exception):
        current = _vp()._load_satellite_registry()
        for existing in current:
            old_selector = _text(existing.get("selector")) if isinstance(existing, dict) else ""
            old_metadata = existing.get("metadata") if isinstance(existing, dict) and isinstance(existing.get("metadata"), dict) else {}
            if (
                old_selector
                and old_selector != selector
                and _same_native_hardware(
                    old_selector,
                    old_metadata.get("hardware_id"),
                    selector,
                    hardware_id,
                )
            ):
                _save_selector_alias(old_selector, selector)
                from . import native_live_settings, stereo_pairs

                native_live_settings.migrate_selector(old_selector, selector)
                stereo_pairs.migrate_member_selector(old_selector, selector)
                _vp()._remove_satellite(old_selector)
    row = {
        "selector": selector,
        "host": "",
        "name": name,
        "source": "tater_native",
        "metadata": _registry_metadata_from_hello(payload, connected=connected),
        "last_seen_ts": _now(),
    }
    return _vp()._upsert_satellite(row)


def _client_snapshot(selector: str, row: Dict[str, Any]) -> Dict[str, Any]:
    hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
    payload = hello.get("payload") if isinstance(hello.get("payload"), dict) else {}
    queue = row.get("queue")
    logs = row.get("logs")
    connected = bool(row.get("connected"))
    auth = row.get("auth") if isinstance(row.get("auth"), dict) else {}
    voice = _voice_snapshot(selector)
    bridge = row.get("voice_bridge")
    if isinstance(bridge, _NativeVoicePipelineBridge):
        voice["audio_transport"] = bridge.audio_stats()
    return {
        "selector": selector,
        "connected": connected,
        "host": _text(row.get("client_host")),
        "device_id": _text(payload.get("device_id") or selector),
        "hardware_id": _hardware_id(payload.get("hardware_id")),
        "device_name": _device_name_from_hello(payload, row.get("name") or selector),
        "board": _text(payload.get("board")),
        "firmware_version": _text(payload.get("firmware_version")),
        "room": _text(payload.get("room") or payload.get("area_name") or payload.get("room_name")),
        "capabilities": _capabilities(payload),
        "connected_ts": float(row.get("connected_ts") or 0.0),
        "last_seen_ts": float(row.get("last_seen_ts") or 0.0),
        "last_disconnect_ts": float(row.get("last_disconnect_ts") or 0.0),
        "last_error": _text(row.get("last_error")),
        "last_message_type": _text(row.get("last_message_type")),
        "last_status": row.get("last_status") if isinstance(row.get("last_status"), dict) else {},
        "media_session": (
            dict(row.get("media_session"))
            if isinstance(row.get("media_session"), dict)
            else {"active": False}
        ),
        "audio_overlay": (
            dict(row.get("audio_overlay"))
            if isinstance(row.get("audio_overlay"), dict)
            else {"active": False}
        ),
        "live_settings": _live_settings_payload(selector, board=_text(payload.get("board"))),
        "auth": {
            "mode": _text(auth.get("mode")) or "open",
            "paired": _text(auth.get("mode")) in {"paired", "device_token"},
        },
        "log_count": len(logs) if isinstance(logs, deque) else 0,
        "queued_commands": queue.qsize() if isinstance(queue, asyncio.Queue) else 0,
        "binary_frames": int(row.get("binary_frames") or 0),
        "binary_bytes": int(row.get("binary_bytes") or 0),
        "wake_verifier": {
            "count": int(row.get("wake_verifier_count") or 0),
            "rejections": int(row.get("wake_verifier_rejections") or 0),
            "last": row.get("wake_verifier_last") if isinstance(row.get("wake_verifier_last"), dict) else {},
        },
        "voice": voice,
    }


async def status() -> Dict[str, Any]:
    now_ts = _now()
    stale_after = _stale_after_s()
    registry_updates: list[tuple[str, Dict[str, Any]]] = []
    disconnected_selectors: list[str] = []
    async with _clients_lock:
        for selector, row in _clients.items():
            if not isinstance(row, dict) or not bool(row.get("connected")):
                continue
            last_seen = float(row.get("last_seen_ts") or 0.0)
            if last_seen <= 0.0 or (now_ts - last_seen) <= stale_after:
                continue
            row["connected"] = False
            row["last_disconnect_ts"] = now_ts
            row["last_error"] = "stale heartbeat"
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            hello_payload = _message_payload(hello)
            if hello_payload:
                registry_updates.append((selector, hello_payload))
            disconnected_selectors.append(selector)
        clients = {
            selector: _client_snapshot(selector, row)
            for selector, row in _clients.items()
            if isinstance(row, dict)
        }
    for selector, hello_payload in registry_updates:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    for selector in disconnected_selectors:
        _schedule_media_disconnect_abort(selector, reason="stale heartbeat")
        _notify_state_change("disconnected", selector)
    return {"ok": True, "protocol": PROTOCOL_VERSION, "clients": clients, "count": len(clients)}


def status_snapshot_sync() -> Dict[str, Any]:
    clients = {
        selector: _client_snapshot(selector, row)
        for selector, row in _clients.items()
        if isinstance(row, dict)
    }
    return {"ok": True, "protocol": PROTOCOL_VERSION, "clients": clients, "count": len(clients)}


async def forget(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        raise ValueError("selector is required")
    if not token.startswith("native:"):
        raise ValueError("Only Tater Native satellites can be forgotten here.")

    pending_futures: list[asyncio.Future] = []
    async with _clients_lock:
        row = _clients.get(token)
        if isinstance(row, dict) and bool(row.get("connected")):
            raise RuntimeError(f"Connected native satellite cannot be forgotten: {token}")
        removed_runtime = _clients.pop(token, None) is not None
        pending = row.get("pending_requests") if isinstance(row, dict) else None
        if isinstance(pending, dict):
            pending_futures = [
                future
                for future in pending.values()
                if isinstance(future, asyncio.Future) and not future.done()
            ]
            pending.clear()

    for future in pending_futures:
        future.set_exception(RuntimeError(f"Native satellite forgotten: {token}"))

    removed_registry = bool(_vp()._remove_satellite(token))
    removed_credentials = _remove_device_credentials(token)
    removed_aliases = _remove_selector_aliases(token)
    removed = bool(
        removed_runtime
        or removed_registry
        or removed_credentials
        or removed_aliases
    )
    if removed:
        _notify_state_change("forgotten", token)
    return {
        "ok": True,
        "selector": token,
        "removed": removed,
        "runtime_removed": removed_runtime,
        "registry_removed": removed_registry,
        "credentials_removed": removed_credentials,
        "aliases_removed": removed_aliases,
    }


async def client_has_capability(selector: str, capability: str) -> bool:
    token = _canonical_selector(selector)
    cap = _text(capability)
    if not token or not cap:
        return False
    async with _clients_lock:
        row = _clients.get(token)
        hello = row.get("hello") if isinstance(row, dict) and isinstance(row.get("hello"), dict) else {}
        payload = _message_payload(hello)
    caps = _capabilities(payload)
    return bool(caps.get(cap))


async def client_media_session_active(selector: str) -> bool:
    token = _canonical_selector(selector)
    if not token:
        return False
    async with _clients_lock:
        row = _clients.get(token)
        session = row.get("media_session") if isinstance(row, dict) else {}
        return bool(isinstance(session, dict) and session.get("active"))


async def live_settings(selector: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    token = _canonical_selector(selector)
    board = ""
    async with _clients_lock:
        row = _clients.get(token) if token else {}
        hello = row.get("hello") if isinstance(row, dict) and isinstance(row.get("hello"), dict) else {}
        payload = _message_payload(hello)
        board = _text(payload.get("board"))
    return {
        "ok": True,
        "selector": token,
        "settings": native_live_settings.settings_snapshot(token, board=board),
        "fields": native_live_settings.settings_fields(token, board=board),
    }


async def logs(selector: str, *, after_seq: int = 0, limit: int = 100) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    max_rows = max(1, min(500, int(limit or 100)))
    async with _clients_lock:
        row = _clients.get(token) or {}
        log_rows = list(row.get("logs") or [])
    rows = [item for item in log_rows if int((item or {}).get("seq") or 0) > int(after_seq or 0)]
    return {"ok": True, "selector": token, "logs": rows[:max_rows], "count": len(rows[:max_rows])}


async def send_command(selector: str, message_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    command_type = _text(message_type)
    if not token:
        raise ValueError("selector is required")
    if not command_type:
        raise ValueError("type is required")
    message = _envelope(command_type, payload if isinstance(payload, dict) else {})
    setup_reset_requested = command_type in {"setup.reset", "provisioning.reset"}
    hello_payload: Dict[str, Any] = {}
    async with _clients_lock:
        row = _clients.get(token)
        if not isinstance(row, dict) or not bool(row.get("connected")):
            raise RuntimeError(f"Native satellite is not connected: {token}")
        queue = row.get("queue")
        if not isinstance(queue, asyncio.Queue):
            raise RuntimeError(f"Native satellite command queue unavailable: {token}")
        queue.put_nowait(message)
        if setup_reset_requested:
            row["connected"] = False
            row["last_disconnect_ts"] = _now()
            row["last_error"] = "setup mode requested"
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            hello_payload = _message_payload(hello)
    if hello_payload:
        _upsert_registry_from_hello(token, hello_payload, connected=False)
    if setup_reset_requested:
        _notify_state_change("disconnected", token)
    return {"ok": True, "selector": token, "message": message}


async def send_request(
    selector: str,
    message_type: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    timeout_s: float = 3.0,
) -> Dict[str, Any]:
    """Send a command and wait for the satellite's matching result message."""
    token = _canonical_selector(selector)
    command_type = _text(message_type)
    if not token:
        raise ValueError("selector is required")
    if not command_type:
        raise ValueError("type is required")

    message = _envelope(command_type, payload if isinstance(payload, dict) else {})
    request_id = _text(message.get("id"))
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()

    async with _clients_lock:
        row = _clients.get(token)
        if not isinstance(row, dict) or not bool(row.get("connected")):
            raise RuntimeError(f"Native satellite is not connected: {token}")
        queue = row.get("queue")
        if not isinstance(queue, asyncio.Queue):
            raise RuntimeError(f"Native satellite command queue unavailable: {token}")
        pending = row.setdefault("pending_requests", {})
        if not isinstance(pending, dict):
            pending = {}
            row["pending_requests"] = pending
        pending[request_id] = future
        try:
            queue.put_nowait(message)
        except Exception:
            pending.pop(request_id, None)
            raise

    try:
        result = await asyncio.wait_for(asyncio.shield(future), timeout=max(0.25, float(timeout_s or 3.0)))
        return result if isinstance(result, dict) else {}
    finally:
        async with _clients_lock:
            row = _clients.get(token)
            pending = row.get("pending_requests") if isinstance(row, dict) else None
            if isinstance(pending, dict):
                pending.pop(request_id, None)
        if not future.done():
            future.cancel()


def _monotonic_us() -> int:
    return int(time.monotonic_ns() // 1000)


async def _stereo_clock_probe(selector: str) -> Dict[str, Any]:
    token = _canonical_selector(selector)
    best: Dict[str, Any] = {}
    for _index in range(STEREO_CLOCK_PROBE_COUNT):
        server_send_us = _monotonic_us()
        try:
            result = await send_request(
                token,
                "audio.clock.sync",
                {"server_send_us": server_send_us},
                timeout_s=2.0,
            )
        except Exception:
            continue
        server_receive_us = _monotonic_us()
        if not _as_bool(result.get("ok"), False):
            continue
        satellite_receive_us = _as_int(result.get("satellite_receive_us"), 0)
        satellite_send_us = _as_int(result.get("satellite_send_us"), 0)
        if satellite_receive_us <= 0 or satellite_send_us < satellite_receive_us:
            continue
        satellite_processing_us = satellite_send_us - satellite_receive_us
        round_trip_us = max(
            0,
            (server_receive_us - server_send_us) - satellite_processing_us,
        )
        offset_us = int(
            round(
                (
                    (satellite_receive_us - server_send_us)
                    + (satellite_send_us - server_receive_us)
                )
                / 2.0
            )
        )
        sample = {
            "selector": token,
            "offset_us": offset_us,
            "round_trip_us": round_trip_us,
            "server_sample_us": int((server_send_us + server_receive_us) // 2),
            "satellite_sample_us": int((satellite_receive_us + satellite_send_us) // 2),
        }
        if not best or round_trip_us < int(best.get("round_trip_us") or 0):
            best = sample
        await asyncio.sleep(0)
    if not best:
        raise RuntimeError(f"Could not synchronize the playback clock for {token}.")
    return best


async def stereo_pair_compatibility(left_selector: str, right_selector: str) -> Dict[str, Any]:
    left = _canonical_selector(left_selector)
    right = _canonical_selector(right_selector)
    if not left.startswith("native:") or not right.startswith("native:"):
        return {"ok": False, "error": "Stereo pairs require two Tater Native satellites."}
    if left == right:
        return {"ok": False, "error": "Left and right satellites must be different."}

    missing: list[str] = []
    disconnected: list[str] = []
    async with _clients_lock:
        for selector in (left, right):
            row = _clients.get(selector)
            if not isinstance(row, dict) or not bool(row.get("connected")):
                disconnected.append(selector)
                continue
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            capabilities = _capabilities(_message_payload(hello))
            required = {
                "synchronized_media_sessions",
                "stereo_channel_selection",
                "media_playhead_telemetry",
                "media_drift_correction",
            }
            absent = sorted(name for name in required if not bool(capabilities.get(name)))
            try:
                session_version = int(float(capabilities.get("audio_session_version") or 0))
            except Exception:
                session_version = 0
            if session_version < 2:
                absent.append("audio_session_version_2")
            if absent:
                missing.append(f"{selector} ({', '.join(absent)})")
    if disconnected:
        return {
            "ok": False,
            "error": f"Both stereo satellites must be connected: {', '.join(disconnected)}.",
            "disconnected": disconnected,
        }
    if missing:
        return {
            "ok": False,
            "error": "Update satellite firmware before creating this stereo pair: " + "; ".join(missing),
            "missing_capabilities": missing,
        }
    return {"ok": True, "left_selector": left, "right_selector": right}


async def media_group_member_status(selectors: list[str]) -> Dict[str, Any]:
    members: list[str] = []
    for raw_selector in list(selectors or []):
        selector = _canonical_selector(raw_selector)
        if selector and selector not in members:
            members.append(selector)
    if not members or any(not selector.startswith("native:") for selector in members):
        return {
            "ok": False,
            "error": "Synchronized media groups require Tater Native satellites.",
            "ready_selectors": [],
            "unavailable": [],
        }

    ready: list[str] = []
    disconnected: list[str] = []
    incompatible: list[Dict[str, Any]] = []
    async with _clients_lock:
        for selector in members:
            row = _clients.get(selector)
            if not isinstance(row, dict) or not bool(row.get("connected")):
                disconnected.append(selector)
                continue
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            capabilities = _capabilities(_message_payload(hello))
            required = {
                "synchronized_media_sessions",
                "media_playhead_telemetry",
                "media_drift_correction",
            }
            absent = sorted(name for name in required if not bool(capabilities.get(name)))
            try:
                session_version = int(float(capabilities.get("audio_session_version") or 0))
            except Exception:
                session_version = 0
            if session_version < 2:
                absent.append("audio_session_version_2")
            if absent:
                incompatible.append(
                    {
                        "selector": selector,
                        "missing_capabilities": absent,
                        "reason": "missing " + ", ".join(absent),
                    }
                )
            else:
                ready.append(selector)
    unavailable = [
        {"selector": selector, "reason": "offline"}
        for selector in disconnected
    ]
    unavailable.extend(dict(row) for row in incompatible)
    return {
        "ok": bool(ready),
        "selectors": members,
        "ready_selectors": ready,
        "disconnected": disconnected,
        "incompatible": incompatible,
        "unavailable": unavailable,
    }


async def media_group_compatibility(selectors: list[str]) -> Dict[str, Any]:
    status = await media_group_member_status(selectors)
    members = list(status.get("selectors") or [])
    ready = list(status.get("ready_selectors") or [])
    disconnected = list(status.get("disconnected") or [])
    incompatible = [
        dict(row)
        for row in list(status.get("incompatible") or [])
        if isinstance(row, dict)
    ]
    if not members:
        return {
            "ok": False,
            "error": _text(status.get("error")) or "Synchronized media groups require Tater Native satellites.",
        }
    if disconnected:
        return {
            "ok": False,
            "error": "All synchronized satellites must be connected: " + ", ".join(disconnected) + ".",
            "disconnected": disconnected,
        }
    if incompatible:
        missing = [
            f"{_text(row.get('selector'))} ({', '.join(row.get('missing_capabilities') or [])})"
            for row in incompatible
        ]
        return {
            "ok": False,
            "error": "Update satellite firmware before using synchronized multi-room playback: "
            + "; ".join(missing),
            "missing_capabilities": missing,
        }
    return {"ok": len(ready) == len(members), "selectors": ready}


async def _stop_stereo_members(selectors: list[str], *, session_id: str = "") -> None:
    async def _stop(selector: str) -> None:
        with contextlib.suppress(Exception):
            await send_command(
                selector,
                "media.session.stop",
                {"session_id": _text(session_id), "reason": "stereo_group_stop"},
            )

    await asyncio.gather(*(_stop(selector) for selector in selectors))


async def prepare_stereo_media_session(
    pair: Dict[str, Any],
    *,
    session_id: str,
    media_url: str,
    volume_percent: int = 100,
    start_position_ms: int = 0,
    loop: bool = False,
    content_type: str = "music",
    title: str = "",
    artist: str = "",
    album: str = "",
    channel_mode: str = "stereo",
    wait_for_completion: bool = False,
    completion_timeout_s: float = 180.0,
) -> Dict[str, Any]:
    pair_row = pair if isinstance(pair, dict) else {}
    left = _canonical_selector(pair_row.get("left_selector"))
    right = _canonical_selector(pair_row.get("right_selector"))
    compatibility = await stereo_pair_compatibility(left, right)
    if not compatibility.get("ok"):
        raise RuntimeError(_text(compatibility.get("error")) or "Stereo pair is unavailable.")
    if not _text(session_id) or not _text(media_url):
        raise ValueError("session_id and media_url are required")

    group_id = _text(pair_row.get("id")) or uuid.uuid4().hex[:12]
    base_volume = max(0, min(100, _as_int(volume_percent, 100)))
    mono = _lower(channel_mode) in {"mono", "center", "tts"}
    member_rows = [
        {
            "selector": left,
            "channel": "mono" if mono else "left",
            "delay_ms": max(0, min(250, _as_int(pair_row.get("left_delay_ms"), 0))),
            "volume_percent": int(
                round(base_volume * max(0, min(100, _as_int(pair_row.get("left_volume_percent"), 100))) / 100.0)
            ),
        },
        {
            "selector": right,
            "channel": "mono" if mono else "right",
            "delay_ms": max(0, min(250, _as_int(pair_row.get("right_delay_ms"), 0))),
            "volume_percent": int(
                round(base_volume * max(0, min(100, _as_int(pair_row.get("right_volume_percent"), 100))) / 100.0)
            ),
        },
    ]
    result = await prepare_group_media_session(
        member_rows,
        group_id=group_id,
        group_selector=_text(pair_row.get("selector")),
        session_id=session_id,
        media_url=media_url,
        start_position_ms=start_position_ms,
        loop=loop,
        content_type=content_type,
        title=title,
        artist=artist,
        album=album,
        channel_mode="mono" if mono else "stereo",
        compatibility_checked=True,
        wait_for_completion=wait_for_completion,
        completion_timeout_s=completion_timeout_s,
    )
    result["stereo_session_started"] = True
    return result


async def prepare_group_media_session(
    members: list[Dict[str, Any]],
    *,
    group_id: str,
    session_id: str,
    media_url: str,
    group_selector: str = "",
    start_position_ms: int = 0,
    loop: bool = False,
    content_type: str = "music",
    title: str = "",
    artist: str = "",
    album: str = "",
    channel_mode: str = "mono",
    start_lead_ms: int = STEREO_START_LEAD_MS,
    compatibility_checked: bool = False,
    wait_for_completion: bool = False,
    completion_timeout_s: float = 180.0,
) -> Dict[str, Any]:
    member_rows: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_member in list(members or []):
        member = raw_member if isinstance(raw_member, dict) else {}
        selector = _canonical_selector(member.get("selector"))
        if not selector or selector in seen:
            continue
        seen.add(selector)
        member_rows.append(
            {
                "selector": selector,
                "channel": _lower(member.get("channel")) or "mono",
                "delay_ms": max(0, min(2000, _as_int(member.get("delay_ms"), 0))),
                "volume_percent": max(0, min(100, _as_int(member.get("volume_percent"), 100))),
            }
        )
    selectors = [member["selector"] for member in member_rows]
    if not compatibility_checked:
        compatibility = await media_group_compatibility(selectors)
        if not compatibility.get("ok"):
            raise RuntimeError(_text(compatibility.get("error")) or "The synchronized media group is unavailable.")
    async with _clients_lock:
        render_clock_support = {}
        capability_latency_frames: Dict[str, int] = {}
        capability_sample_rates: Dict[str, int] = {}
        for selector in selectors:
            row = _clients.get(selector) if isinstance(_clients.get(selector), dict) else {}
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            capabilities = _capabilities(_message_payload(hello))
            render_clock_support[selector] = bool(capabilities.get("media_render_clock"))
            capability_latency_frames[selector] = max(
                0,
                min(
                    MEDIA_RENDER_LATENCY_MAX_FRAMES,
                    _as_int(capabilities.get("media_output_latency_frames"), 0),
                ),
            )
            capability_sample_rates[selector] = max(
                1,
                _as_int(capabilities.get("media_sample_rate_hz"), 48000),
            )
    use_rendered_clock = bool(selectors) and all(render_clock_support.values())
    if any(render_clock_support.values()) and not use_rendered_clock:
        _vp().logger.warning(
            "[native-media] synchronized group uses the common source clock because "
            "rendered-clock support is mixed group=%s members=%s",
            _text(group_id) or "pending",
            ",".join(
                f"{selector}:{'rendered' if supported else 'source'}"
                for selector, supported in render_clock_support.items()
            ),
        )
    group_token = _text(group_id) or uuid.uuid4().hex[:12]
    group_session_id = _text(session_id)
    if not group_session_id or not _text(media_url):
        raise ValueError("session_id and media_url are required")

    media_content_type = _lower(content_type) or "music"
    transient_tts = media_content_type in {"tts", "speech", "announcement"}
    clocks = await asyncio.gather(*(_stereo_clock_probe(selector) for selector in selectors))
    clock_by_selector = {row["selector"]: row for row in clocks}

    async def _prepare(member: Dict[str, Any]) -> Dict[str, Any]:
        prepare_payload = {
            "session_id": group_session_id,
            "group_id": group_token,
            "media": {
                "url": media_url,
                "volume_percent": member["volume_percent"],
                "start_position_ms": max(0, _as_int(start_position_ms, 0)),
                "loop": bool(loop),
                "content_type": media_content_type,
                "title": _text(title),
                "artist": _text(artist),
                "album": _text(album),
            },
            "routing": {
                "channel": member["channel"],
                "pair_selector": _text(group_selector),
            },
        }
        if transient_tts:
            prepare_payload["visual_mode"] = "speaking"
            prepare_payload["state_after"] = "idle"
        seek_prepare_timeout_s = min(
            60.0,
            max(15.0, 8.0 + (max(0, _as_int(start_position_ms, 0)) / 15000.0)),
        )
        result = await send_request(
            member["selector"],
            "media.session.prepare",
            prepare_payload,
            timeout_s=seek_prepare_timeout_s,
        )
        if not _as_bool(result.get("ok"), False):
            raise RuntimeError(_text(result.get("error")) or f"{member['selector']} did not prepare audio.")
        return result

    try:
        prepared = await asyncio.gather(*(_prepare(member) for member in member_rows))
        prepared_by_selector = {
            member["selector"]: result
            for member, result in zip(member_rows, prepared)
            if isinstance(result, dict)
        }
        render_latency_frames: Dict[str, int] = {}
        render_sample_rates: Dict[str, int] = {}
        render_latency_us: Dict[str, int] = {}
        for member in member_rows:
            selector = member["selector"]
            prepared_row = prepared_by_selector.get(selector, {})
            sample_rate = max(
                1,
                _as_int(
                    prepared_row.get("sample_rate_hz"),
                    capability_sample_rates.get(selector, 48000),
                ),
            )
            reported_frames = max(
                0,
                min(
                    MEDIA_RENDER_LATENCY_MAX_FRAMES,
                    _as_int(prepared_row.get("output_latency_frames"), 0),
                ),
            )
            learned_frames = _learned_media_render_latency_frames(selector)
            latency_frames = (
                learned_frames
                or reported_frames
                or capability_latency_frames.get(selector, 0)
            ) if use_rendered_clock else 0
            render_sample_rates[selector] = sample_rate
            render_latency_frames[selector] = latency_frames
            render_latency_us[selector] = int(round(latency_frames * 1_000_000.0 / sample_rate))

        requested_lead_ms = max(
            250,
            min(5000, _as_int(start_lead_ms, STEREO_START_LEAD_MS)),
        )
        required_lead_ms = max(render_latency_us.values(), default=0) // 1000
        lead_ms = max(requested_lead_ms, required_lead_ms + MEDIA_RENDER_START_GUARD_MS)
        lead_ms = min(5000, lead_ms)
        start_server_us = _monotonic_us() + (lead_ms * 1000)
        start_unix_ms = int(
            round((time.time() * 1000.0) + ((start_server_us - _monotonic_us()) / 1000.0))
        )

        async def _commit(member: Dict[str, Any]) -> Dict[str, Any]:
            clock = clock_by_selector[member["selector"]]
            selector = member["selector"]
            start_at_us = (
                start_server_us
                + int(clock["offset_us"])
                + (int(member["delay_ms"]) * 1000)
                - render_latency_us.get(selector, 0)
            )
            result = await send_request(
                selector,
                "media.session.commit",
                {
                    "session_id": group_session_id,
                    "group_id": group_token,
                    "start_at_us": start_at_us,
                    "audible_start_at_us": (
                        start_server_us
                        + int(clock["offset_us"])
                        + (int(member["delay_ms"]) * 1000)
                    ),
                    "output_latency_frames": render_latency_frames.get(selector, 0),
                },
                timeout_s=3.0,
            )
            if not _as_bool(result.get("ok"), False):
                raise RuntimeError(_text(result.get("error")) or f"{member['selector']} rejected synchronized start.")
            return {**result, "start_at_us": start_at_us}

        committed = await asyncio.gather(*(_commit(member) for member in member_rows))
    except Exception:
        await _stop_stereo_members(selectors, session_id=group_session_id)
        raise

    completion_future = asyncio.get_running_loop().create_future()
    _stereo_sessions[group_token] = {
        "group_id": group_token,
        "pair_selector": _text(group_selector),
        "session_id": group_session_id,
        "selectors": selectors,
        "reference_selector": selectors[0],
        "left_selector": selectors[0],
        "right_selector": selectors[1] if len(selectors) > 1 else "",
        "clock_offsets_us": {selector: int(row.get("offset_us") or 0) for selector, row in clock_by_selector.items()},
        "clock_round_trip_us": {
            selector: int(row.get("round_trip_us") or 0) for selector, row in clock_by_selector.items()
        },
        "member_delays_ms": {member["selector"]: int(member.get("delay_ms") or 0) for member in member_rows},
        "render_latency_frames": dict(render_latency_frames),
        "render_sample_rates": dict(render_sample_rates),
        "audible_start_server_us": start_server_us,
        "start_position_frames": {
            selector: int(
                round(
                    max(0, _as_int(start_position_ms, 0))
                    * render_sample_rates.get(selector, 48000)
                    / 1000.0
                )
            )
            for selector in selectors
        },
        "actual_starts_us": {},
        "latency_learning_samples": {},
        "observed_render_latency_frames": {},
        "clock_sync_server_us": _monotonic_us(),
        "playheads": {},
        "phase_error_ema_frames": {},
        "phase_error_directions": {},
        "phase_error_stable_samples": {},
        "phase_sample_times_us": {},
        "last_phase_sample_server_us": 0,
        "last_adjust_server_us": 0,
        "use_rendered_clock": use_rendered_clock,
        "created_server_us": _monotonic_us(),
        "loop": bool(loop),
        "content_type": media_content_type,
        "channel_mode": _lower(channel_mode) or "mono",
        "completion_future": completion_future,
    }
    result = {
        "ok": True,
        "group_session_started": True,
        "group_id": group_token,
        "session_id": group_session_id,
        "members": member_rows,
        "prepared": prepared,
        "committed": committed,
        "start_server_us": start_server_us,
        "start_unix_ms": start_unix_ms,
        "audible_start_server_us": start_server_us,
        "audible_start_unix_ms": start_unix_ms,
        "render_latency_frames": dict(render_latency_frames),
        "render_latency_ms": {
            selector: round(render_latency_us.get(selector, 0) / 1000.0, 3)
            for selector in selectors
        },
        "start_lead_ms": lead_ms,
        "use_rendered_clock": use_rendered_clock,
        "clock_samples": clocks,
        "playback_completed": False,
    }
    if wait_for_completion:
        timeout_s = max(1.0, min(600.0, float(completion_timeout_s or 180.0)))
        try:
            completion = await asyncio.wait_for(asyncio.shield(completion_future), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            await _stop_stereo_members(selectors, session_id=group_session_id)
            stale = _stereo_sessions.pop(group_token, None)
            task = _stereo_adjust_tasks.pop(group_token, None)
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
            if isinstance(stale, dict):
                future = stale.get("completion_future")
                if isinstance(future, asyncio.Future) and not future.done():
                    future.cancel()
            raise RuntimeError("Timed out waiting for synchronized group playback to finish.") from exc
        result["playback_completed"] = True
        result["playback_ok"] = _as_bool((completion or {}).get("ok"), False)
        result["finished_members"] = list((completion or {}).get("members") or [])
    return result


def stereo_pair_media_active(pair: Dict[str, Any]) -> bool:
    pair_selector = _text((pair or {}).get("selector"))
    for session in _stereo_sessions.values():
        if (
            isinstance(session, dict)
            and _text(session.get("pair_selector")) == pair_selector
        ):
            return True
    return False


async def start_stereo_overlay(
    pair: Dict[str, Any],
    *,
    overlay_id: str,
    foreground_url: str,
    foreground_kind: str = "tts",
    foreground_volume_percent: int = 100,
    ducking: Optional[Dict[str, Any]] = None,
    start_server_us: int = 0,
    stop_media_when_finished: bool = False,
) -> Dict[str, Any]:
    pair_row = pair if isinstance(pair, dict) else {}
    left = _text(pair_row.get("left_selector"))
    right = _text(pair_row.get("right_selector"))
    compatibility = await stereo_pair_compatibility(left, right)
    if not compatibility.get("ok"):
        raise RuntimeError(_text(compatibility.get("error")) or "Stereo pair is unavailable.")

    pair_selector = _text(pair_row.get("selector"))
    session = next(
        (
            row
            for row in _stereo_sessions.values()
            if isinstance(row, dict) and _text(row.get("pair_selector")) == pair_selector
        ),
        None,
    )
    if not isinstance(session, dict):
        raise RuntimeError("The stereo pair does not have an active synchronized media session.")

    requested_start_server_us = _as_int(start_server_us, 0)
    minimum_start_server_us = _monotonic_us() + 100_000
    if requested_start_server_us >= minimum_start_server_us:
        offsets = (
            session.get("clock_offsets_us")
            if isinstance(session.get("clock_offsets_us"), dict)
            else {}
        )
        clocks = [
            {"selector": left, "offset_us": _as_int(offsets.get(left), 0)},
            {"selector": right, "offset_us": _as_int(offsets.get(right), 0)},
        ]
        synchronized_start_server_us = requested_start_server_us
    else:
        clocks = await asyncio.gather(_stereo_clock_probe(left), _stereo_clock_probe(right))
        synchronized_start_server_us = _monotonic_us() + (STEREO_START_LEAD_MS * 1000)
    duck = ducking if isinstance(ducking, dict) else {}
    base_volume = max(0, min(100, _as_int(foreground_volume_percent, 100)))
    member_settings = {
        left: {
            "delay_ms": max(0, min(250, _as_int(pair_row.get("left_delay_ms"), 0))),
            "volume_percent": int(
                round(base_volume * max(0, min(100, _as_int(pair_row.get("left_volume_percent"), 100))) / 100.0)
            ),
        },
        right: {
            "delay_ms": max(0, min(250, _as_int(pair_row.get("right_delay_ms"), 0))),
            "volume_percent": int(
                round(base_volume * max(0, min(100, _as_int(pair_row.get("right_volume_percent"), 100))) / 100.0)
            ),
        },
    }

    async def _start(clock: Dict[str, Any]) -> Dict[str, Any]:
        selector = _text(clock.get("selector"))
        settings = member_settings.get(selector) or {}
        start_at_us = (
            synchronized_start_server_us
            + int(clock.get("offset_us") or 0)
            + (_as_int(settings.get("delay_ms"), 0) * 1000)
        )
        result = await send_command(
            selector,
            "audio.overlay.start",
            {
                "overlay_id": _text(overlay_id),
                "foreground": {
                    "url": _text(foreground_url),
                    "kind": _text(foreground_kind) or "tts",
                    "volume_percent": _as_int(settings.get("volume_percent"), base_volume),
                },
                "ducking": dict(duck),
                "start_at_us": start_at_us,
                "group_id": _text(session.get("group_id")),
            },
        )
        return {**result, "start_at_us": start_at_us}

    members = await asyncio.gather(*(_start(clock) for clock in clocks))
    if stop_media_when_finished:
        session["stop_on_overlay_id"] = _text(overlay_id)
        session["overlay_finished_selectors"] = []
        session["stop_requested"] = False
    return {
        "ok": True,
        "stereo_overlay_started": True,
        "overlay_id": _text(overlay_id),
        "group_id": _text(session.get("group_id")),
        "start_server_us": synchronized_start_server_us,
        "stop_media_when_finished": bool(stop_media_when_finished),
        "members": members,
    }


def _session_members(session: Dict[str, Any]) -> list[str]:
    members: list[str] = []
    raw_members = session.get("selectors") if isinstance(session.get("selectors"), list) else []
    for raw_selector in [*raw_members, session.get("left_selector"), session.get("right_selector")]:
        selector = _text(raw_selector)
        if selector and selector not in members:
            members.append(selector)
    return members


def _media_group_ids_for_selector(selector: str) -> list[str]:
    token = _canonical_selector(selector)
    if not token:
        return []
    return [
        group_id
        for group_id, session in _stereo_sessions.items()
        if isinstance(session, dict) and token in set(_session_members(session))
    ]


def _cancel_media_disconnect_abort(selector: str) -> bool:
    token = _canonical_selector(selector)
    task = _media_disconnect_tasks.pop(token, None)
    if not isinstance(task, asyncio.Task) or task.done():
        return False
    task.cancel()
    _vp().logger.info(
        "[native-media] synchronized member rejoined during disconnect grace "
        "selector=%s groups=%s",
        token,
        ",".join(_media_group_ids_for_selector(token)) or "none",
    )
    return True


async def _abort_media_groups_after_disconnect_grace(
    selector: str,
    *,
    reason: str,
) -> None:
    token = _canonical_selector(selector)
    current_task = asyncio.current_task()
    try:
        await asyncio.sleep(NATIVE_MEDIA_DISCONNECT_GRACE_S)
        async with _clients_lock:
            row = _clients.get(token)
            reconnected = isinstance(row, dict) and bool(row.get("connected"))
        if reconnected:
            return
        aborted = await _abort_media_groups_for_disconnect(token, reason=reason)
        if aborted:
            _vp().logger.warning(
                "[native-media] synchronized member disconnect grace expired "
                "selector=%s grace_s=%.1f groups=%d",
                token,
                NATIVE_MEDIA_DISCONNECT_GRACE_S,
                aborted,
            )
    finally:
        if _media_disconnect_tasks.get(token) is current_task:
            _media_disconnect_tasks.pop(token, None)


def _schedule_media_disconnect_abort(selector: str, *, reason: str) -> bool:
    token = _canonical_selector(selector)
    group_ids = _media_group_ids_for_selector(token)
    if not token or not group_ids:
        return False
    previous = _media_disconnect_tasks.pop(token, None)
    if isinstance(previous, asyncio.Task) and not previous.done():
        previous.cancel()
    _media_disconnect_tasks[token] = asyncio.create_task(
        _abort_media_groups_after_disconnect_grace(token, reason=reason)
    )
    _vp().logger.warning(
        "[native-media] synchronized member disconnected; holding session for reconnect "
        "selector=%s groups=%s grace_s=%.1f",
        token,
        ",".join(group_ids),
        NATIVE_MEDIA_DISCONNECT_GRACE_S,
    )
    return True


async def _abort_media_groups_for_disconnect(selector: str, *, reason: str) -> int:
    """Stop a synchronized group instead of letting its remaining members drift alone."""
    token = _canonical_selector(selector)
    aborted: list[tuple[str, Dict[str, Any], list[str]]] = []
    for group_id, session in list(_stereo_sessions.items()):
        if not isinstance(session, dict) or token not in set(_session_members(session)):
            continue
        _stereo_sessions.pop(group_id, None)
        task = _stereo_adjust_tasks.pop(group_id, None)
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
        remaining = [member for member in _session_members(session) if member != token]
        completion_future = session.get("completion_future")
        if isinstance(completion_future, asyncio.Future) and not completion_future.done():
            completion_future.set_result(
                {
                    "ok": False,
                    "members": sorted(_session_members(session)),
                    "session_id": _text(session.get("session_id")),
                    "group_id": group_id,
                    "disconnected_selector": token,
                    "error": f"{token} disconnected during synchronized playback.",
                }
            )
        aborted.append((group_id, session, remaining))

    for group_id, session, remaining in aborted:
        if remaining:
            await _stop_stereo_members(
                remaining,
                session_id=_text(session.get("session_id")),
            )
        _vp().logger.warning(
            "[native-media] synchronized group aborted group=%s disconnected=%s reason=%s remaining=%s",
            group_id,
            token,
            _text(reason) or "disconnect",
            ",".join(remaining) or "none",
        )
    return len(aborted)


async def _refresh_stereo_clocks(session: Dict[str, Any]) -> None:
    selectors = _session_members(session)
    clocks = await asyncio.gather(
        *(_stereo_clock_probe(selector) for selector in selectors),
        return_exceptions=True,
    )
    offsets = dict(session.get("clock_offsets_us") or {})
    round_trips = dict(session.get("clock_round_trip_us") or {})
    updated = False
    for row in clocks:
        if not isinstance(row, dict):
            continue
        selector = _text(row.get("selector"))
        if not selector:
            continue
        offsets[selector] = int(row.get("offset_us") or 0)
        round_trips[selector] = int(row.get("round_trip_us") or 0)
        updated = True
    if updated:
        session["clock_offsets_us"] = offsets
        session["clock_round_trip_us"] = round_trips
        session["clock_sync_server_us"] = _monotonic_us()


async def _adjust_audible_timeline_session(
    group_id: str,
    session: Dict[str, Any],
    *,
    now_us: int,
) -> None:
    """Keep every rendered playhead on Tater's server-owned audible timeline."""
    selectors = _session_members(session)
    playheads = session.get("playheads") if isinstance(session.get("playheads"), dict) else {}
    if not selectors or any(
        not isinstance(playheads.get(selector), dict) for selector in selectors
    ):
        return
    if now_us - int(session.get("last_phase_sample_server_us") or 0) < int(
        STEREO_ADJUST_INTERVAL_S * 1_000_000
    ):
        return

    offsets = session.get("clock_offsets_us") if isinstance(session.get("clock_offsets_us"), dict) else {}
    member_delays = (
        session.get("member_delays_ms")
        if isinstance(session.get("member_delays_ms"), dict)
        else {}
    )
    start_positions = (
        session.get("start_position_frames")
        if isinstance(session.get("start_position_frames"), dict)
        else {}
    )
    audible_start_server_us = _as_int(session.get("audible_start_server_us"), 0)
    if audible_start_server_us <= 0:
        return

    phase_ema = session.setdefault("phase_error_ema_frames", {})
    phase_directions = session.setdefault("phase_error_directions", {})
    phase_stable_samples = session.setdefault("phase_error_stable_samples", {})
    phase_sample_times = session.setdefault("phase_sample_times_us", {})
    if not isinstance(phase_ema, dict):
        phase_ema = {}
        session["phase_error_ema_frames"] = phase_ema
    if not isinstance(phase_directions, dict):
        phase_directions = {}
        session["phase_error_directions"] = phase_directions
    if not isinstance(phase_stable_samples, dict):
        phase_stable_samples = {}
        session["phase_error_stable_samples"] = phase_stable_samples
    if not isinstance(phase_sample_times, dict):
        phase_sample_times = {}
        session["phase_sample_times_us"] = phase_sample_times

    startup = now_us - audible_start_server_us < int(
        STEREO_STARTUP_ADJUST_WINDOW_S * 1_000_000
    )
    threshold_frames = (
        STEREO_STARTUP_ADJUST_THRESHOLD_FRAMES
        if startup
        else STEREO_ADJUST_THRESHOLD_FRAMES
    )
    maximum_frames = (
        STEREO_STARTUP_ADJUST_MAX_FRAMES
        if startup
        else STEREO_ADJUST_MAX_FRAMES
    )
    settle_ms = (
        STEREO_STARTUP_ADJUST_SETTLE_MS
        if startup
        else STEREO_ADJUST_SETTLE_MS
    )
    corrections: Dict[str, int] = {}
    phase_errors: Dict[str, float] = {}
    raw_phase_errors: Dict[str, float] = {}
    sampled_phase = False
    for selector in selectors:
        row = playheads.get(selector) if isinstance(playheads.get(selector), dict) else {}
        if (
            not row
            or _as_bool(row.get("rebuffering"), False)
            or _text(row.get("session_id")) != _text(session.get("session_id"))
            or "rendered_frames" not in row
        ):
            continue
        satellite_time_us = _as_int(row.get("satellite_time_us"), 0)
        if satellite_time_us <= _as_int(phase_sample_times.get(selector), 0):
            continue
        phase_sample_times[selector] = satellite_time_us
        sampled_phase = True

        sample_rate = max(1, _as_int(row.get("sample_rate_hz"), 48000))
        event_server_us = satellite_time_us - _as_int(offsets.get(selector), 0)
        audible_member_start_us = (
            audible_start_server_us
            + (_as_int(member_delays.get(selector), 0) * 1000)
        )
        elapsed_us = max(0, event_server_us - audible_member_start_us)
        expected_frames = float(_as_int(start_positions.get(selector), 0)) + (
            float(elapsed_us) * float(sample_rate) / 1_000_000.0
        )
        rendered_frames = float(max(0, _as_int(row.get("rendered_frames"), 0)))
        phase_error_frames = expected_frames - rendered_frames
        try:
            previous_ema = float(phase_ema.get(selector, phase_error_frames))
        except (TypeError, ValueError):
            previous_ema = phase_error_frames
        smoothed_error_frames = (
            ((1.0 - STEREO_PHASE_EMA_ALPHA) * previous_ema)
            + (STEREO_PHASE_EMA_ALPHA * phase_error_frames)
        )
        phase_ema[selector] = smoothed_error_frames
        if abs(smoothed_error_frames) < threshold_frames:
            phase_directions[selector] = 0
            phase_stable_samples[selector] = 0
            continue
        direction = 1 if smoothed_error_frames > 0 else -1
        previous_direction = _as_int(phase_directions.get(selector), 0)
        stable_samples = (
            _as_int(phase_stable_samples.get(selector), 0) + 1
            if direction == previous_direction
            else 1
        )
        phase_directions[selector] = direction
        phase_stable_samples[selector] = stable_samples
        if stable_samples < STEREO_PHASE_STABLE_SAMPLES:
            continue
        correction = int(round(smoothed_error_frames))
        corrections[selector] = max(-maximum_frames, min(maximum_frames, correction))
        phase_errors[selector] = smoothed_error_frames
        raw_phase_errors[selector] = phase_error_frames

    if sampled_phase:
        session["last_phase_sample_server_us"] = now_us
    if not corrections:
        return

    async def _adjust(selector: str, correction: int) -> tuple[str, int, Dict[str, Any]]:
        row = _clients.get(selector) if isinstance(_clients.get(selector), dict) else {}
        hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
        supports_slew = bool(_capabilities(_message_payload(hello)).get("media_rate_slew"))
        result = await send_request(
            selector,
            "media.session.adjust",
            {
                "session_id": _text(session.get("session_id")),
                "group_id": group_id,
                "correction_frames": correction,
                "mode": "slew" if supports_slew else "legacy",
                "settle_ms": settle_ms if supports_slew else 0,
                "reference_selector": "tater:audible-timeline",
                "audible_start_server_us": audible_start_server_us,
            },
            timeout_s=2.0,
        )
        return selector, correction, result

    results = await asyncio.gather(
        *(_adjust(selector, correction) for selector, correction in corrections.items()),
        return_exceptions=True,
    )
    applied = {
        selector: correction
        for row in results
        if isinstance(row, tuple) and len(row) == 3
        for selector, correction, result in [row]
        if _as_bool(result.get("ok"), False)
    }
    if applied:
        session["last_adjust_server_us"] = now_us
        session["last_correction_frames"] = applied
        session["last_phase_error_frames"] = {
            selector: phase_errors[selector]
            for selector in applied
            if selector in phase_errors
        }
        session["last_raw_phase_error_frames"] = {
            selector: raw_phase_errors[selector]
            for selector in applied
            if selector in raw_phase_errors
        }


async def _adjust_stereo_session(group_id: str) -> None:
    try:
        session = _stereo_sessions.get(group_id)
        if not isinstance(session, dict):
            return
        now_us = _monotonic_us()
        last_clock_sync_us = int(session.get("clock_sync_server_us") or 0)
        if now_us - last_clock_sync_us >= int(STEREO_CLOCK_REFRESH_S * 1_000_000):
            await _refresh_stereo_clocks(session)
            now_us = _monotonic_us()

        playheads = session.get("playheads") if isinstance(session.get("playheads"), dict) else {}
        selectors = _session_members(session)
        if _as_bool(session.get("use_rendered_clock"), False):
            await _adjust_audible_timeline_session(
                group_id,
                session,
                now_us=now_us,
            )
            return
        if len(selectors) < 2:
            return
        reference = _text(session.get("reference_selector")) or selectors[0]
        reference_row = playheads.get(reference) if isinstance(playheads.get(reference), dict) else {}
        follower_rows = {
            selector: playheads.get(selector)
            for selector in selectors
            if selector != reference and isinstance(playheads.get(selector), dict)
        }
        if not reference_row or len(follower_rows) != len(selectors) - 1:
            return
        if _as_bool(reference_row.get("rebuffering"), False):
            return
        if any(
            _text(row.get("session_id")) != _text(session.get("session_id"))
            for row in [reference_row, *follower_rows.values()]
        ):
            return
        if now_us - int(session.get("last_phase_sample_server_us") or 0) < int(
            STEREO_ADJUST_INTERVAL_S * 1_000_000
        ):
            return
        use_rendered_clock = _as_bool(session.get("use_rendered_clock"), False)
        if use_rendered_clock and any(
            "rendered_frames" not in row for row in [reference_row, *follower_rows.values()]
        ):
            return

        offsets = session.get("clock_offsets_us") if isinstance(session.get("clock_offsets_us"), dict) else {}

        def _projected_source_frames(selector: str, row: Dict[str, Any]) -> float:
            sample_rate = max(1, _as_int(row.get("sample_rate_hz"), 48000))
            satellite_time_us = _as_int(row.get("satellite_time_us"), 0)
            server_event_us = satellite_time_us - _as_int(offsets.get(selector), 0)
            age_us = max(0, min(2_000_000, now_us - server_event_us))
            reported_frames = _as_int(
                row.get("rendered_frames") if use_rendered_clock else row.get("source_frames"),
                0,
            )
            try:
                playback_rate = float(row.get("playback_rate") or 1.0)
            except (TypeError, ValueError):
                playback_rate = 1.0
            playback_rate = max(0.98, min(1.02, playback_rate))
            return float(reported_frames) + (
                float(age_us) * float(sample_rate) * playback_rate / 1_000_000.0
            )

        reference_frames = _projected_source_frames(reference, reference_row)
        member_delays = (
            session.get("member_delays_ms")
            if isinstance(session.get("member_delays_ms"), dict)
            else {}
        )
        sample_rate = max(1, _as_int(reference_row.get("sample_rate_hz"), 48000))
        phase_ema = session.setdefault("phase_error_ema_frames", {})
        phase_directions = session.setdefault("phase_error_directions", {})
        phase_stable_samples = session.setdefault("phase_error_stable_samples", {})
        phase_sample_times = session.setdefault("phase_sample_times_us", {})
        if not isinstance(phase_ema, dict):
            phase_ema = {}
            session["phase_error_ema_frames"] = phase_ema
        if not isinstance(phase_directions, dict):
            phase_directions = {}
            session["phase_error_directions"] = phase_directions
        if not isinstance(phase_stable_samples, dict):
            phase_stable_samples = {}
            session["phase_error_stable_samples"] = phase_stable_samples
        if not isinstance(phase_sample_times, dict):
            phase_sample_times = {}
            session["phase_sample_times_us"] = phase_sample_times
        corrections: Dict[str, int] = {}
        phase_errors: Dict[str, float] = {}
        raw_phase_errors: Dict[str, float] = {}
        sampled_phase = False
        for follower, follower_row in follower_rows.items():
            if _as_bool(follower_row.get("rebuffering"), False):
                continue
            reference_sample_us = _as_int(reference_row.get("satellite_time_us"), 0)
            follower_sample_us = _as_int(follower_row.get("satellite_time_us"), 0)
            previous_sample = (
                phase_sample_times.get(follower)
                if isinstance(phase_sample_times.get(follower), dict)
                else {}
            )
            if previous_sample and (
                reference_sample_us <= _as_int(previous_sample.get("reference"), 0)
                or follower_sample_us <= _as_int(previous_sample.get("follower"), 0)
            ):
                continue
            phase_sample_times[follower] = {
                "reference": reference_sample_us,
                "follower": follower_sample_us,
            }
            sampled_phase = True
            follower_frames = _projected_source_frames(follower, follower_row)
            target_delta_frames = (
                (_as_int(member_delays.get(follower), 0) - _as_int(member_delays.get(reference), 0))
                * sample_rate
                / 1000.0
            )
            phase_error_frames = (reference_frames - follower_frames) - target_delta_frames
            try:
                previous_ema = float(phase_ema.get(follower, phase_error_frames))
            except (TypeError, ValueError):
                previous_ema = phase_error_frames
            smoothed_error_frames = (
                (1.0 - STEREO_PHASE_EMA_ALPHA) * previous_ema
                + STEREO_PHASE_EMA_ALPHA * phase_error_frames
            )
            phase_ema[follower] = smoothed_error_frames
            if abs(smoothed_error_frames) < STEREO_ADJUST_THRESHOLD_FRAMES:
                phase_directions[follower] = 0
                phase_stable_samples[follower] = 0
                continue
            direction = 1 if smoothed_error_frames > 0 else -1
            previous_direction = _as_int(phase_directions.get(follower), 0)
            stable_samples = (
                _as_int(phase_stable_samples.get(follower), 0) + 1
                if direction == previous_direction
                else 1
            )
            phase_directions[follower] = direction
            phase_stable_samples[follower] = stable_samples
            if stable_samples < STEREO_PHASE_STABLE_SAMPLES:
                continue
            correction = int(round(smoothed_error_frames))
            corrections[follower] = max(
                -STEREO_ADJUST_MAX_FRAMES,
                min(STEREO_ADJUST_MAX_FRAMES, correction),
            )
            phase_errors[follower] = smoothed_error_frames
            raw_phase_errors[follower] = phase_error_frames
        if sampled_phase:
            session["last_phase_sample_server_us"] = now_us
        if not corrections:
            return

        async def _adjust(follower: str, correction: int) -> tuple[str, int, Dict[str, Any]]:
            row = _clients.get(follower) if isinstance(_clients.get(follower), dict) else {}
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            supports_slew = bool(_capabilities(_message_payload(hello)).get("media_rate_slew"))
            result = await send_request(
                follower,
                "media.session.adjust",
                {
                    "session_id": _text(session.get("session_id")),
                    "group_id": group_id,
                    "correction_frames": correction,
                    "mode": "slew" if supports_slew else "legacy",
                    "settle_ms": STEREO_ADJUST_SETTLE_MS if supports_slew else 0,
                    "reference_selector": reference,
                },
                timeout_s=2.0,
            )
            return follower, correction, result

        results = await asyncio.gather(
            *(_adjust(selector, correction) for selector, correction in corrections.items()),
            return_exceptions=True,
        )
        applied = {
            selector: correction
            for row in results
            if isinstance(row, tuple) and len(row) == 3
            for selector, correction, result in [row]
            if _as_bool(result.get("ok"), False)
        }
        if applied:
            session["last_adjust_server_us"] = now_us
            session["last_correction_frames"] = applied
            session["last_phase_error_frames"] = {
                selector: phase_errors[selector] for selector in applied if selector in phase_errors
            }
            session["last_raw_phase_error_frames"] = {
                selector: raw_phase_errors[selector]
                for selector in applied
                if selector in raw_phase_errors
            }
    finally:
        _stereo_adjust_tasks.pop(group_id, None)


def _record_stereo_playhead(selector: str, payload: Dict[str, Any]) -> None:
    group_id = _text(payload.get("group_id"))
    session = _stereo_sessions.get(group_id)
    if not group_id or not isinstance(session, dict):
        return
    if selector not in set(_session_members(session)):
        return
    playheads = session.setdefault("playheads", {})
    if not isinstance(playheads, dict):
        playheads = {}
        session["playheads"] = playheads
    playheads[selector] = {**dict(payload), "received_server_us": _monotonic_us()}
    learning_samples = session.setdefault("latency_learning_samples", {})
    observed_latencies = session.setdefault("observed_render_latency_frames", {})
    actual_starts = session.get("actual_starts_us") if isinstance(session.get("actual_starts_us"), dict) else {}
    start_positions = session.get("start_position_frames") if isinstance(session.get("start_position_frames"), dict) else {}
    learned_sample_count = (
        _as_int(learning_samples.get(selector), 0)
        if isinstance(learning_samples, dict)
        else 0
    )
    actual_start_us = _as_int(actual_starts.get(selector), 0)
    satellite_time_us = _as_int(payload.get("satellite_time_us"), 0)
    sample_rate = max(1, _as_int(payload.get("sample_rate_hz"), 48000))
    if (
        _as_bool(session.get("use_rendered_clock"), False)
        and isinstance(learning_samples, dict)
        and isinstance(observed_latencies, dict)
        and learned_sample_count < MEDIA_RENDER_LATENCY_LEARN_SAMPLES
        and not _as_bool(payload.get("rebuffering"), False)
        and _as_int(session.get("last_adjust_server_us"), 0) <= 0
        and actual_start_us > 0
        and satellite_time_us > actual_start_us
        and "rendered_frames" in payload
    ):
        elapsed_frames = int(
            round((satellite_time_us - actual_start_us) * sample_rate / 1_000_000.0)
        )
        rendered_elapsed_frames = max(
            0,
            _as_int(payload.get("rendered_frames"), 0)
            - _as_int(start_positions.get(selector), 0),
        )
        observed_frames = elapsed_frames - rendered_elapsed_frames
        if 0 < observed_frames <= MEDIA_RENDER_LATENCY_MAX_FRAMES:
            previous_observed = _as_int(observed_latencies.get(selector), observed_frames)
            smoothed_observed = observed_frames if learned_sample_count <= 0 else int(
                round(
                    ((1.0 - MEDIA_RENDER_LATENCY_EMA_ALPHA) * previous_observed)
                    + (MEDIA_RENDER_LATENCY_EMA_ALPHA * observed_frames)
                )
            )
            observed_latencies[selector] = smoothed_observed
            learning_samples[selector] = learned_sample_count + 1
            _record_media_render_latency(selector, smoothed_observed)
    health_rows = session.setdefault("playhead_health", {})
    if not isinstance(health_rows, dict):
        health_rows = {}
        session["playhead_health"] = health_rows
    previous = health_rows.get(selector) if isinstance(health_rows.get(selector), dict) else {}
    current_health = {
        "rebuffering": _as_bool(payload.get("rebuffering"), False),
        "underrun_events": max(0, _as_int(payload.get("underrun_events"), 0)),
        "rejoin_count": max(0, _as_int(payload.get("rejoin_count"), 0)),
        "rejoin_frames": max(0, _as_int(payload.get("rejoin_frames"), 0)),
        "correction_frames": _as_int(payload.get("correction_frames"), 0),
        "buffered_frames": max(0, _as_int(payload.get("buffered_frames"), 0)),
        "output_latency_frames": max(
            0,
            _as_int(
                payload.get("output_latency_frames"),
                (observed_latencies.get(selector) or 0)
                if isinstance(observed_latencies, dict)
                else 0,
            ),
        ),
    }
    health_rows[selector] = current_health
    health_changed = (
        current_health["rebuffering"]
        or current_health["underrun_events"] > _as_int(previous.get("underrun_events"), 0)
        or current_health["rejoin_count"] > _as_int(previous.get("rejoin_count"), 0)
    )
    if health_changed:
        _vp().logger.warning(
            "[native-media] playback recovery selector=%s group=%s buffered_frames=%d "
            "rebuffering=%s underruns=%d rejoins=%d rejoin_frames=%d correction_frames=%d",
            selector,
            group_id,
            current_health["buffered_frames"],
            current_health["rebuffering"],
            current_health["underrun_events"],
            current_health["rejoin_count"],
            current_health["rejoin_frames"],
            current_health["correction_frames"],
        )
    # Render-clock sessions follow Tater's audible timeline even when AirPlay is
    # the only other destination. Source-clock compatibility sessions still
    # need a second native playhead because they compare members to each other.
    if (
        len(_session_members(session)) < 2
        and not _as_bool(session.get("use_rendered_clock"), False)
    ):
        return
    task = _stereo_adjust_tasks.get(group_id)
    if isinstance(task, asyncio.Task) and not task.done():
        return
    _stereo_adjust_tasks[group_id] = asyncio.create_task(_adjust_stereo_session(group_id))


def _record_stereo_started(selector: str, payload: Dict[str, Any]) -> None:
    group_id = _text(payload.get("group_id"))
    session = _stereo_sessions.get(group_id)
    if not group_id or not isinstance(session, dict) or selector not in set(_session_members(session)):
        return
    actual_starts = session.setdefault("actual_starts_us", {})
    if isinstance(actual_starts, dict):
        actual_starts[selector] = _as_int(payload.get("actual_start_us"), 0)


def _record_stereo_finished(selector: str, payload: Dict[str, Any]) -> None:
    session_id = _text(payload.get("session_id"))
    for group_id, session in list(_stereo_sessions.items()):
        if not isinstance(session, dict) or _text(session.get("session_id")) != session_id:
            continue
        finished = session.setdefault("finished_selectors", [])
        if not isinstance(finished, list):
            finished = []
            session["finished_selectors"] = finished
        if selector not in finished:
            finished.append(selector)
        finished_ok = session.setdefault("finished_ok", {})
        if not isinstance(finished_ok, dict):
            finished_ok = {}
            session["finished_ok"] = finished_ok
        finished_ok[selector] = _as_bool(payload.get("ok"), False)
        members = set(_session_members(session))
        if members and members.issubset(set(finished)):
            completion_future = session.get("completion_future")
            completion = {
                "ok": all(_as_bool(finished_ok.get(member), False) for member in members),
                "members": sorted(members),
                "session_id": session_id,
                "group_id": group_id,
            }
            if isinstance(completion_future, asyncio.Future) and not completion_future.done():
                completion_future.set_result(completion)
            _stereo_sessions.pop(group_id, None)
            task = _stereo_adjust_tasks.pop(group_id, None)
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()


def _record_stereo_overlay_finished(selector: str, payload: Dict[str, Any]) -> None:
    overlay_id = _text(payload.get("overlay_id"))
    if not overlay_id:
        return
    for session in list(_stereo_sessions.values()):
        if not isinstance(session, dict) or _text(session.get("stop_on_overlay_id")) != overlay_id:
            continue
        members = set(_session_members(session))
        if selector not in members:
            continue
        finished = session.setdefault("overlay_finished_selectors", [])
        if not isinstance(finished, list):
            finished = []
            session["overlay_finished_selectors"] = finished
        if selector not in finished:
            finished.append(selector)
        if members and members.issubset(set(finished)) and not _as_bool(
            session.get("stop_requested"),
            False,
        ):
            session["stop_requested"] = True
            asyncio.create_task(
                _stop_stereo_members(
                    sorted(members),
                    session_id=_text(session.get("session_id")),
                )
            )


async def push_live_settings(selector: str = "") -> Dict[str, Any]:
    token = _canonical_selector(selector)
    response_board = ""
    pushed: list[str] = []
    async with _clients_lock:
        targets = {token: _clients.get(token)} if token else dict(_clients)
        if token and isinstance(targets.get(token), dict):
            hello = targets[token].get("hello") if isinstance(targets[token].get("hello"), dict) else {}
            response_board = _text(_message_payload(hello).get("board"))
        for target_selector, row in targets.items():
            if not isinstance(row, dict) or not bool(row.get("connected")):
                continue
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            payload = _message_payload(hello)
            board = _text(payload.get("board"))
            queue = row.get("queue")
            if not isinstance(queue, asyncio.Queue):
                continue
            queued = _envelope("settings", _firmware_settings_payload(str(target_selector), board=board))
            _queue_command(queue, queued)
            pushed.append(str(target_selector))
    settings = _live_settings_payload(token, board=response_board)
    firmware_settings = _firmware_settings_payload(token, board=response_board)
    return {"ok": True, "settings": settings, "firmware_settings": firmware_settings, "pushed": pushed, "count": len(pushed)}


async def save_live_settings(values: Dict[str, Any], *, selector: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    token = _canonical_selector(selector)
    board = ""
    async with _clients_lock:
        row = _clients.get(token) if token else {}
        hello = row.get("hello") if isinstance(row, dict) and isinstance(row.get("hello"), dict) else {}
        board = _text(_message_payload(hello).get("board"))
    result = native_live_settings.save_settings(values or {}, selector=token, board=board)
    push_result = await push_live_settings(token)
    result["push"] = push_result
    _notify_state_change("settings", token)
    return result


async def _record_client(selector: str, websocket: WebSocket, hello: Dict[str, Any], auth: Optional[Dict[str, Any]] = None) -> asyncio.Queue:
    _cancel_media_disconnect_abort(selector)
    payload = _message_payload(hello)
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    voice_bridge = _NativeVoicePipelineBridge(selector, queue)
    now_ts = _now()
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    row = {
        "selector": selector,
        "name": _device_name_from_hello(payload, selector),
        "client_host": _text(client_host),
        "websocket": websocket,
        "queue": queue,
        "voice_bridge": voice_bridge,
        "hello": hello,
        "auth": {
            "mode": _text((auth or {}).get("mode")) or "open",
            "pairing_id": _text((auth or {}).get("pairing_id")),
        },
        "connected": True,
        "connected_ts": now_ts,
        "last_seen_ts": now_ts,
        "last_message_type": "hello",
        "last_status": {},
        "media_session": {"active": False},
        "audio_overlay": {"active": False},
        "logs": deque(maxlen=MAX_LOG_ROWS),
        "log_seq": 0,
        "binary_frames": 0,
        "binary_bytes": 0,
        "wake_verifier_count": 0,
        "wake_verifier_rejections": 0,
        "wake_verifier_last": {},
        "pending_requests": {},
    }
    _upsert_registry_from_hello(selector, payload, connected=True)
    aliases = _load_selector_aliases()
    async with _clients_lock:
        for old_selector, canonical in aliases.items():
            if canonical == selector and old_selector != selector:
                _clients.pop(old_selector, None)
        _clients[selector] = row
    _notify_state_change("connected", selector)
    return queue


async def reset_wake_verifier_runtime_stats() -> Dict[str, Any]:
    cleared = 0
    async with _clients_lock:
        for row in _clients.values():
            if not isinstance(row, dict):
                continue
            row["wake_verifier_count"] = 0
            row["wake_verifier_rejections"] = 0
            row["wake_verifier_last"] = {}
            cleared += 1
    _notify_state_change("wake_verifier_stats_reset", "")
    return {"ok": True, "cleared_clients": cleared}


async def _voice_bridge(selector: str) -> Optional[_NativeVoicePipelineBridge]:
    async with _clients_lock:
        row = _clients.get(selector)
        bridge = row.get("voice_bridge") if isinstance(row, dict) else None
    return bridge if isinstance(bridge, _NativeVoicePipelineBridge) else None


async def _voice_bridge_for_websocket(selector: str, websocket: WebSocket) -> Optional[_NativeVoicePipelineBridge]:
    async with _clients_lock:
        row = _clients.get(selector)
        if not isinstance(row, dict) or row.get("websocket") is not websocket:
            return None
        bridge = row.get("voice_bridge")
    return bridge if isinstance(bridge, _NativeVoicePipelineBridge) else None


async def _mark_disconnected(selector: str, reason: str, *, websocket: Optional[WebSocket] = None) -> bool:
    hello_payload: Dict[str, Any] = {}
    pending_futures: list[asyncio.Future] = []
    connection_changed = False
    async with _clients_lock:
        row = _clients.get(selector)
        if isinstance(row, dict):
            if websocket is not None and row.get("websocket") is not websocket:
                return False
            connection_changed = bool(row.get("connected"))
            row["connected"] = False
            row["last_disconnect_ts"] = _now()
            row["last_error"] = reason
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            hello_payload = _message_payload(hello)
            pending = row.get("pending_requests")
            if isinstance(pending, dict):
                pending_futures = [
                    future
                    for future in pending.values()
                    if isinstance(future, asyncio.Future) and not future.done()
                ]
                pending.clear()
    for future in pending_futures:
        future.set_exception(RuntimeError(f"Native satellite disconnected: {selector}"))
    if connection_changed:
        _schedule_media_disconnect_abort(selector, reason=reason)
    if hello_payload:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    if connection_changed:
        _notify_state_change("disconnected", selector)
    return bool(hello_payload)


async def _cancel_websocket_tasks(tasks: Any, *, selector: str, label: str) -> None:
    active = [task for task in tuple(tasks or ()) if isinstance(task, asyncio.Task) and not task.done()]
    for task in active:
        task.cancel()
    if not active:
        return
    done, pending = await asyncio.wait(
        active,
        timeout=NATIVE_WEBSOCKET_TASK_CANCEL_TIMEOUT_S,
    )
    for task in done:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.result()
    if pending:
        _vp().logger.warning(
            "[native-satellite] websocket %s cleanup timed out selector=%s pending=%s",
            label,
            selector or "-",
            len(pending),
        )


async def _handle_text_message(selector: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    msg_type = _message_type(message)
    payload = _message_payload(message)
    reply_to = _text(payload.get("reply_to")) if msg_type.endswith(".result") else ""
    setup_state_seen = msg_type == "status" and _is_setup_state(payload)
    setup_connection_changed = False
    hello_payload: Dict[str, Any] = {}
    response_future: Optional[asyncio.Future] = None
    async with _clients_lock:
        row = _clients.get(selector)
        if not isinstance(row, dict):
            return None
        row["last_seen_ts"] = _now()
        row["last_message_type"] = msg_type
        if msg_type == "status":
            row["last_status"] = payload
            if setup_state_seen:
                setup_connection_changed = bool(row.get("connected"))
                row["connected"] = False
                row["last_disconnect_ts"] = _now()
                row["last_error"] = f"setup state: {_status_state(payload)}"
                hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
                hello_payload = _message_payload(hello)
        elif msg_type == "media.session.started":
            row["media_session"] = {
                "active": True,
                "session_id": _text(payload.get("session_id")),
                "group_id": _text(payload.get("group_id")),
                "channel": _text(payload.get("channel")) or "stereo",
                "sample_rate_hz": _as_int(payload.get("sample_rate_hz"), 48000),
                "scheduled_start_us": _as_int(payload.get("scheduled_start_us"), 0),
                "actual_start_us": _as_int(payload.get("actual_start_us"), 0),
                "late_by_us": _as_int(payload.get("late_by_us"), 0),
                "started_ts": _now(),
            }
            _record_stereo_started(selector, payload)
        elif msg_type == "media.session.playhead":
            previous = row.get("media_session") if isinstance(row.get("media_session"), dict) else {}
            row["media_session"] = {
                **previous,
                "active": True,
                "session_id": _text(payload.get("session_id") or previous.get("session_id")),
                "group_id": _text(payload.get("group_id") or previous.get("group_id")),
                "channel": _text(payload.get("channel") or previous.get("channel")) or "stereo",
                "playhead": dict(payload),
                "playhead_ts": _now(),
            }
            _record_stereo_playhead(selector, payload)
        elif msg_type == "media.session.finished":
            previous = row.get("media_session") if isinstance(row.get("media_session"), dict) else {}
            row["media_session"] = {
                **previous,
                "active": False,
                "session_id": _text(payload.get("session_id") or previous.get("session_id")),
                "ok": _as_bool(payload.get("ok"), False),
                "finished_ts": _now(),
            }
            row["audio_overlay"] = {"active": False}
            _record_stereo_finished(selector, payload)
        elif msg_type == "audio.overlay.started":
            row["audio_overlay"] = {
                "active": True,
                "overlay_id": _text(payload.get("overlay_id")),
                "started_ts": _now(),
            }
        elif msg_type == "audio.overlay.finished":
            previous = row.get("audio_overlay") if isinstance(row.get("audio_overlay"), dict) else {}
            row["audio_overlay"] = {
                **previous,
                "active": False,
                "overlay_id": _text(payload.get("overlay_id") or previous.get("overlay_id")),
                "ok": _as_bool(payload.get("ok"), False),
                "finished_ts": _now(),
            }
            _record_stereo_overlay_finished(selector, payload)
        elif msg_type in {"log", "ota.status"}:
            logs_deque = row.get("logs")
            if isinstance(logs_deque, deque):
                row["log_seq"] = int(row.get("log_seq") or 0) + 1
                level = _text(payload.get("level") or "info")
                message = _text(payload.get("message"))
                if msg_type == "ota.status":
                    status_text = _text(payload.get("status"))
                    progress = _as_int(payload.get("progress"), -1)
                    level = "error" if status_text == "error" else "info"
                    if not message:
                        if progress >= 0:
                            message = f"OTA {status_text or 'status'}: {progress}%"
                        else:
                            message = f"OTA {status_text or 'status'}"
                logs_deque.append(
                    {
                        "seq": row["log_seq"],
                        "ts": _now(),
                        "level": level,
                        "message": message,
                        "type": msg_type,
                        "payload": payload,
                    }
                )
        if reply_to:
            pending = row.get("pending_requests")
            if isinstance(pending, dict):
                candidate = pending.pop(reply_to, None)
                if isinstance(candidate, asyncio.Future):
                    response_future = candidate
    if response_future is not None and not response_future.done():
        response_future.set_result(payload)
        return None
    if hello_payload:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    if setup_connection_changed:
        _notify_state_change("disconnected", selector)
    if msg_type == "ambient.observation.request":
        from . import reachy_ambient

        result = reachy_ambient.schedule(selector, payload)
        return _envelope(
            "ambient.observation.ack",
            result,
            message_id=_text(message.get("id")),
        )
    if msg_type == "music.reaction.request":
        from . import reachy_music

        result = reachy_music.schedule(selector, payload)
        return _envelope(
            "music.reaction.ack",
            result,
            message_id=_text(message.get("id")),
        )
    if msg_type in {"voice.start", "audio.start"}:
        from . import reachy_ambient
        from . import reachy_music

        reachy_ambient.cancel(selector)
        reachy_music.cancel(selector)
        bridge = await _voice_bridge(selector)
        if bridge is None:
            raise RuntimeError(f"Native satellite voice bridge unavailable: {selector}")
        result = await bridge.voice_start(payload)
        return _envelope(
            "voice.start.ack",
            {"ok": result is not None, "result": result},
            message_id=_text(message.get("id")),
        )

    if msg_type in {"voice.stop", "audio.stop"}:
        bridge = await _voice_bridge(selector)
        if bridge is not None:
            await bridge.voice_stop(payload)
        return _envelope("voice.stop.ack", {"ok": True}, message_id=_text(message.get("id")))

    if msg_type in {
        "announcement.finished",
        "playback.finished",
        "tts.finished",
        "audio.scene.finished",
        "audio.overlay.finished",
    }:
        bridge = await _voice_bridge(selector)
        if bridge is not None:
            await bridge.announcement_finished()
        if msg_type == "audio.scene.finished":
            ack_type = "audio.scene.finished.ack"
        elif msg_type == "audio.overlay.finished":
            ack_type = "audio.overlay.finished.ack"
        else:
            ack_type = "announcement.finished.ack"
        return _envelope(ack_type, {"ok": True}, message_id=_text(message.get("id")))

    if msg_type == "timer.event":
        from . import native_timers

        result = await native_timers.handle_device_event(selector, payload)
        return _envelope("timer.event.ack", result, message_id=_text(message.get("id")))

    if msg_type == "ping":
        return _envelope("pong", {"ok": True}, message_id=_text(message.get("id")))

    return None


async def handle_websocket(websocket: WebSocket) -> None:
    bind_runtime_loop()
    await websocket.accept()

    selector = ""
    command_sender: Optional[asyncio.Task] = None
    wake_verifier_tasks: set[asyncio.Task] = set()
    client_row: Optional[Dict[str, Any]] = None
    client_host = getattr(websocket.client, "host", "unknown") if websocket.client is not None else "unknown"
    send_lock = asyncio.Lock()

    async def send_json(message: Dict[str, Any]) -> None:
        async with send_lock:
            await websocket.send_json(message)

    try:
        first = await asyncio.wait_for(websocket.receive_text(), timeout=10.0)
        hello = _parse_json_text(first)
        if _message_type(hello) != "hello":
            _vp().logger.warning(
                "[native-satellite] rejected websocket from %s: first message was %s",
                getattr(websocket.client, "host", "unknown") if websocket.client is not None else "unknown",
                _message_type(hello) or "-",
            )
            await send_json(_envelope("error", {"ok": False, "error": "First message must be hello."}))
            await websocket.close(code=1002)
            return

        payload = _message_payload(hello)
        selector = _selector_from_hello(payload, websocket)
        auth = _authorize_websocket_hello(websocket, selector, payload)
        if not bool(auth.get("ok")):
            _vp().logger.warning(
                "[native-satellite] rejected websocket selector=%s device_id=%s host=%s token_present=%s reason=%s",
                selector,
                _text(payload.get("device_id") or payload.get("id")) or "-",
                getattr(websocket.client, "host", "unknown") if websocket.client is not None else "unknown",
                bool(_auth_token_from_websocket(websocket)),
                _text(auth.get("error")) or "Unauthorized.",
            )
            await send_json(_envelope("error", {"ok": False, "error": _text(auth.get("error")) or "Unauthorized."}))
            await websocket.close(code=1008)
            return
        queue = await _record_client(selector, websocket, hello, auth)
        async with _clients_lock:
            row = _clients.get(selector)
            client_row = row if isinstance(row, dict) and row.get("websocket") is websocket else None
        bridge = client_row.get("voice_bridge") if isinstance(client_row, dict) else None
        bridge = bridge if isinstance(bridge, _NativeVoicePipelineBridge) else None
        if bridge is not None:
            await bridge.start()
        _vp().logger.info(
            "[native-satellite] connected selector=%s board=%s firmware=%s room=%s auth=%s",
            selector,
            _text(payload.get("board")) or "-",
            _text(payload.get("firmware_version")) or "-",
            _text(payload.get("room")) or "-",
            _text(auth.get("mode")) or "open",
        )

        ack_payload = {
            "ok": True,
            "protocol": PROTOCOL_VERSION,
            "selector": selector,
            "server": "tater",
            "capabilities": {
                "settings": True,
                "state": True,
                "led": True,
                "play_url": True,
                "voice_stream": True,
                "pcm_binary": True,
                "timers": True,
                "ota": True,
            },
        }
        device_token = _text(auth.get("device_token"))
        if device_token:
            ack_payload["device_token"] = device_token

        await send_json(
            _envelope(
                "hello.ack",
                ack_payload,
                message_id=_text(hello.get("id")),
            )
        )
        await send_json(_envelope("state", {"state": "idle"}))
        await send_json(_envelope("settings", _firmware_settings_payload(selector, board=_text(payload.get("board")))))

        async def send_commands() -> None:
            while True:
                message = await queue.get()
                try:
                    await send_json(message)
                except Exception:
                    _vp().logger.warning(
                        "[native-satellite] command sender stopped selector=%s host=%s",
                        selector or "-",
                        client_host,
                        exc_info=True,
                    )
                    raise

        command_sender = asyncio.create_task(send_commands())
        while True:
            raw = await websocket.receive()
            raw_type = _text(raw.get("type"))
            if raw_type == "websocket.disconnect":
                _vp().logger.info(
                    "[native-satellite] websocket disconnect selector=%s host=%s code=%s reason=%s",
                    selector or "-",
                    client_host,
                    raw.get("code"),
                    _text(raw.get("reason")) or "-",
                )
                break
            if raw.get("text") is not None:
                try:
                    message = _parse_json_text(raw.get("text"))
                except Exception as exc:
                    await send_json(_envelope("error", {"ok": False, "error": f"Invalid JSON: {exc}"}))
                    continue
                try:
                    response = await _handle_text_message(selector, message)
                except Exception as exc:
                    await send_json(
                        _envelope(
                            "error",
                            {"ok": False, "error": str(exc) or type(exc).__name__},
                            message_id=_text(message.get("id")),
                        )
                    )
                    continue
                if response:
                    await send_json(response)
                continue
            binary = raw.get("bytes")
            if binary is not None:
                data = bytes(binary or b"")
                if isinstance(client_row, dict):
                    client_row["binary_frames"] = int(client_row.get("binary_frames") or 0) + 1
                    client_row["binary_bytes"] = int(client_row.get("binary_bytes") or 0) + len(data)
                    client_row["last_seen_ts"] = _now()
                    client_row["last_message_type"] = "binary"
                from . import wake_verifier

                if wake_verifier.is_wake_verifier_packet(data):
                    task = asyncio.create_task(
                        _handle_wake_verifier_packet(selector, data, queue, websocket)
                    )
                    wake_verifier_tasks.add(task)
                    task.add_done_callback(wake_verifier_tasks.discard)
                elif bridge is not None:
                    await bridge.audio(data)
    except WebSocketDisconnect:
        pass
    except asyncio.TimeoutError:
        _vp().logger.warning(
            "[native-satellite] websocket hello timeout host=%s selector=%s",
            client_host,
            selector or "-",
        )
        with contextlib.suppress(Exception):
            await websocket.close(code=1002)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _vp().logger.exception(
            "[native-satellite] websocket handler failed selector=%s host=%s",
            selector or "-",
            client_host,
        )
        with contextlib.suppress(Exception):
            await send_json(_envelope("error", {"ok": False, "error": str(exc) or type(exc).__name__}))
    finally:
        await _cancel_websocket_tasks(
            wake_verifier_tasks,
            selector=selector,
            label="wake verifier",
        )
        if command_sender is not None:
            await _cancel_websocket_tasks(
                (command_sender,),
                selector=selector,
                label="command sender",
            )
        if selector:
            bridge = None
            try:
                bridge = await asyncio.wait_for(
                    _voice_bridge_for_websocket(selector, websocket),
                    timeout=NATIVE_WEBSOCKET_DISCONNECT_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                _vp().logger.warning(
                    "[native-satellite] websocket voice lookup timed out selector=%s",
                    selector,
                )
            if bridge is not None:
                try:
                    await asyncio.wait_for(
                        bridge.close(),
                        timeout=NATIVE_WEBSOCKET_BRIDGE_CLOSE_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    _vp().logger.warning(
                        "[native-satellite] websocket voice cleanup timed out selector=%s",
                        selector,
                    )
                except Exception:
                    _vp().logger.warning(
                        "[native-satellite] websocket voice cleanup failed selector=%s",
                        selector,
                        exc_info=True,
                    )
            disconnected = False
            try:
                disconnected = await asyncio.wait_for(
                    _mark_disconnected(selector, "disconnect", websocket=websocket),
                    timeout=NATIVE_WEBSOCKET_DISCONNECT_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                _vp().logger.warning(
                    "[native-satellite] websocket registry cleanup timed out selector=%s",
                    selector,
                )
            if disconnected:
                _vp().logger.info("[native-satellite] disconnected selector=%s", selector)
