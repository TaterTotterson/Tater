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
_pairing_lock = threading.RLock()
_pairing_sessions: Dict[str, Dict[str, Any]] = {}


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
    with _client_loop_lock:
        if loop is None or _client_loop is loop:
            _client_loop = None


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


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


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
        "device_name": _text(payload.get("device_name") or payload.get("name") or selector),
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


def _valid_device_credential(token: str, selector: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    supplied_hash = _token_hash(token)
    if not supplied_hash:
        return None
    device_id = _text(payload.get("device_id") or payload.get("id"))
    with _pairing_lock:
        data = _load_credentials_unlocked()
        devices = data.get("devices") if isinstance(data.get("devices"), dict) else {}
        matched_key = ""
        matched_row: Optional[Dict[str, Any]] = None
        for key, row in devices.items():
            if not isinstance(row, dict):
                continue
            row_hash = _text(row.get("token_hash"))
            if not row_hash or not hmac.compare_digest(row_hash, supplied_hash):
                continue
            row_selector = _text(row.get("selector") or key)
            row_device_id = _text(row.get("device_id"))
            if selector and row_selector and row_selector != selector:
                if not device_id or row_device_id != device_id:
                    continue
            matched_key = _text(key) or selector
            matched_row = dict(row)
            break
        if not matched_row:
            return None
        row = devices.get(matched_key)
        if isinstance(row, dict):
            row["last_seen_ts"] = _now()
            _save_credentials_unlocked(data)
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
        session["device_name"] = _text(payload.get("device_name") or payload.get("name") or selector)
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


def _capabilities(payload: Dict[str, Any]) -> Dict[str, bool]:
    raw = payload.get("capabilities")
    if not isinstance(raw, dict):
        return {}
    return {
        _text(key): _as_bool(value)
        for key, value in raw.items()
        if _text(key)
    }


def _live_settings_payload(selector: str = "", *, board: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    return native_live_settings.settings_snapshot(selector, board=board)


def _firmware_settings_payload(selector: str = "", *, board: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    return native_live_settings.firmware_settings_snapshot(selector, board=board)


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
            play_payload: Dict[str, Any] = {"url": url}
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
        "capabilities": caps,
    }
    if room:
        meta["room"] = room
        meta["room_name"] = room
        meta["area_name"] = room
    return meta


def _upsert_registry_from_hello(selector: str, payload: Dict[str, Any], *, connected: bool) -> Dict[str, Any]:
    name = _text(payload.get("device_name") or payload.get("name") or selector)
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
        "device_name": _text(payload.get("device_name") or row.get("name") or selector),
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
        "live_settings": _live_settings_payload(selector, board=_text(payload.get("board"))),
        "auth": {
            "mode": _text(auth.get("mode")) or "open",
            "paired": _text(auth.get("mode")) in {"paired", "device_token"},
        },
        "log_count": len(logs) if isinstance(logs, deque) else 0,
        "queued_commands": queue.qsize() if isinstance(queue, asyncio.Queue) else 0,
        "binary_frames": int(row.get("binary_frames") or 0),
        "binary_bytes": int(row.get("binary_bytes") or 0),
        "voice": voice,
    }


async def status() -> Dict[str, Any]:
    now_ts = _now()
    stale_after = _stale_after_s()
    registry_updates: list[tuple[str, Dict[str, Any]]] = []
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
        clients = {
            selector: _client_snapshot(selector, row)
            for selector, row in _clients.items()
            if isinstance(row, dict)
        }
    for selector, hello_payload in registry_updates:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    return {"ok": True, "protocol": PROTOCOL_VERSION, "clients": clients, "count": len(clients)}


def status_snapshot_sync() -> Dict[str, Any]:
    clients = {
        selector: _client_snapshot(selector, row)
        for selector, row in _clients.items()
        if isinstance(row, dict)
    }
    return {"ok": True, "protocol": PROTOCOL_VERSION, "clients": clients, "count": len(clients)}


async def client_has_capability(selector: str, capability: str) -> bool:
    token = _text(selector)
    cap = _text(capability)
    if not token or not cap:
        return False
    async with _clients_lock:
        row = _clients.get(token)
        hello = row.get("hello") if isinstance(row, dict) and isinstance(row.get("hello"), dict) else {}
        payload = _message_payload(hello)
    caps = _capabilities(payload)
    return bool(caps.get(cap))


async def live_settings(selector: str = "") -> Dict[str, Any]:
    from . import native_live_settings

    token = _text(selector)
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
    token = _text(selector)
    max_rows = max(1, min(500, int(limit or 100)))
    async with _clients_lock:
        row = _clients.get(token) or {}
        log_rows = list(row.get("logs") or [])
    rows = [item for item in log_rows if int((item or {}).get("seq") or 0) > int(after_seq or 0)]
    return {"ok": True, "selector": token, "logs": rows[:max_rows], "count": len(rows[:max_rows])}


async def send_command(selector: str, message_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = _text(selector)
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
    return {"ok": True, "selector": token, "message": message}


async def push_live_settings(selector: str = "") -> Dict[str, Any]:
    token = _text(selector)
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

    token = _text(selector)
    board = ""
    async with _clients_lock:
        row = _clients.get(token) if token else {}
        hello = row.get("hello") if isinstance(row, dict) and isinstance(row.get("hello"), dict) else {}
        board = _text(_message_payload(hello).get("board"))
    result = native_live_settings.save_settings(values or {}, selector=selector, board=board)
    push_result = await push_live_settings(selector)
    result["push"] = push_result
    return result


async def _record_client(selector: str, websocket: WebSocket, hello: Dict[str, Any], auth: Optional[Dict[str, Any]] = None) -> asyncio.Queue:
    payload = _message_payload(hello)
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    voice_bridge = _NativeVoicePipelineBridge(selector, queue)
    now_ts = _now()
    client_host = getattr(websocket.client, "host", "") if websocket.client is not None else ""
    row = {
        "selector": selector,
        "name": _text(payload.get("device_name") or selector),
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
        "logs": deque(maxlen=MAX_LOG_ROWS),
        "log_seq": 0,
        "binary_frames": 0,
        "binary_bytes": 0,
    }
    async with _clients_lock:
        _clients[selector] = row
    _upsert_registry_from_hello(selector, payload, connected=True)
    return queue


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
    async with _clients_lock:
        row = _clients.get(selector)
        if isinstance(row, dict):
            if websocket is not None and row.get("websocket") is not websocket:
                return False
            row["connected"] = False
            row["last_disconnect_ts"] = _now()
            row["last_error"] = reason
            hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
            hello_payload = _message_payload(hello)
    if hello_payload:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    return bool(hello_payload)


async def _handle_text_message(selector: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    msg_type = _message_type(message)
    payload = _message_payload(message)
    setup_state_seen = msg_type == "status" and _is_setup_state(payload)
    hello_payload: Dict[str, Any] = {}
    async with _clients_lock:
        row = _clients.get(selector)
        if not isinstance(row, dict):
            return None
        row["last_seen_ts"] = _now()
        row["last_message_type"] = msg_type
        if msg_type == "status":
            row["last_status"] = payload
            if setup_state_seen:
                row["connected"] = False
                row["last_disconnect_ts"] = _now()
                row["last_error"] = f"setup state: {_status_state(payload)}"
                hello = row.get("hello") if isinstance(row.get("hello"), dict) else {}
                hello_payload = _message_payload(hello)
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
    if hello_payload:
        _upsert_registry_from_hello(selector, hello_payload, connected=False)
    if msg_type in {"voice.start", "audio.start"}:
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

    if msg_type in {"announcement.finished", "playback.finished", "tts.finished"}:
        bridge = await _voice_bridge(selector)
        if bridge is not None:
            await bridge.announcement_finished()
        return _envelope("announcement.finished.ack", {"ok": True}, message_id=_text(message.get("id")))

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
        with contextlib.suppress(Exception):
            from . import native_timers

            await native_timers.sync_selector(selector)
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
                if bridge is not None:
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
        if command_sender is not None:
            command_sender.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await command_sender
        if selector:
            bridge = await _voice_bridge_for_websocket(selector, websocket)
            if bridge is not None:
                await bridge.close()
            if await _mark_disconnected(selector, "disconnect", websocket=websocket):
                _vp().logger.info("[native-satellite] disconnected selector=%s", selector)
