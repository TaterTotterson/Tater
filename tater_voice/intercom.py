from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
import uuid
import wave
from io import BytesIO
from typing import Any, Dict, List, Optional

from announcement_targets import VOICE_CORE_TARGET_PREFIX, normalize_announcement_targets
from speech_tts import play_announcement_audio_targets
from tateros import integration_store as integration_store_module

from . import device_runtime

logger = logging.getLogger("voice_intercom")

DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S = 90.0
DEFAULT_INTERCOM_REPLY_TIMEOUT_S = 120.0
DEFAULT_INTERCOM_PLAYBACK_TIMEOUT_S = 180.0
HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"
INTERCOM_STOP_PHRASES = {
    "cancel broadcast",
    "cancel intercom",
    "close broadcast",
    "close intercom",
    "end broadcast",
    "end intercom",
    "stop broadcast",
    "stop intercom",
    "nevermind",
    "never mind",
}
INTERCOM_BROADCAST_WAKE_WORDS = {
    "tater intercom broadcast",
    "tater broadcast intercom",
    "intercom broadcast",
    "broadcast intercom",
    "push to intercom",
    "push to talk intercom",
}

_pending_lock = asyncio.Lock()
_pending_by_selector: Dict[str, Dict[str, Any]] = {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    fn = integration_store_module.integration_function("homeassistant", "load_homeassistant_config")
    if fn:
        return fn(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": HOMEASSISTANT_DEFAULT_BASE_URL, "token": ""}


def _lower(value: Any) -> str:
    return _text(value).lower()


def _now() -> float:
    return time.time()


def _norm(value: Any) -> str:
    token = _lower(value)
    token = re.sub(r"[^a-z0-9]+", " ", token)
    return re.sub(r"\s+", " ", token).strip()


def _selector_from_voice_core_target(target: Any) -> str:
    token = _text(target)
    if token.lower().startswith(VOICE_CORE_TARGET_PREFIX):
        return _text(token[len(VOICE_CORE_TARGET_PREFIX) :])
    return token


def _voice_core_target(selector: Any) -> str:
    token = _text(selector)
    return f"{VOICE_CORE_TARGET_PREFIX}{token}" if token else ""


def is_broadcast_wake_word(wake_word: Any) -> bool:
    return _norm(wake_word) in {_norm(item) for item in INTERCOM_BROADCAST_WAKE_WORDS}


def _metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return dict(meta)


def _merge_satellite_rows(base: Optional[Dict[str, Any]], overlay: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base or {})
    for key, value in dict(overlay or {}).items():
        if key == "metadata":
            old_meta = merged.get("metadata") if isinstance(merged.get("metadata"), dict) else {}
            new_meta = value if isinstance(value, dict) else {}
            merged["metadata"] = {**old_meta, **new_meta}
            continue
        if value not in ("", None):
            merged[key] = value
    return merged


def _registry_by_selector() -> Dict[str, Dict[str, Any]]:
    with contextlib.suppress(Exception):
        from . import runtime as esphome_runtime

        registry = esphome_runtime.load_satellite_registry()
        return {
            _text(row.get("selector")): dict(row)
            for row in (registry if isinstance(registry, list) else [])
            if isinstance(row, dict) and _text(row.get("selector"))
        }
    return {}


def _native_clients_by_selector() -> Dict[str, Dict[str, Any]]:
    try:
        from . import native_satellite

        status = native_satellite.status_snapshot_sync()
    except Exception:
        return {}
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    rows: Dict[str, Dict[str, Any]] = {}
    for selector, row in clients.items():
        if not isinstance(row, dict) or not bool(row.get("connected")):
            continue
        token = _text(selector) or _text(row.get("selector"))
        if token:
            rows[token] = dict(row)
    return rows


def _candidate_names(row: Dict[str, Any]) -> List[str]:
    meta = _metadata(row)
    device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
    values = [
        row.get("selector"),
        row.get("name"),
        row.get("host"),
        row.get("room"),
        row.get("area"),
        row.get("room_name"),
        row.get("area_name"),
        meta.get("area_name"),
        meta.get("room_name"),
        meta.get("room"),
        meta.get("area"),
        device_info.get("friendly_name"),
        device_info.get("name"),
        device_info.get("model"),
    ]
    out: List[str] = []
    seen = set()
    for value in values:
        token = _text(value)
        if not token:
            continue
        key = _norm(token)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def _candidate_label(row: Dict[str, Any], selector: str) -> str:
    meta = _metadata(row)
    device_info = row.get("device_info") if isinstance(row.get("device_info"), dict) else {}
    for value in (
        row.get("room"),
        row.get("area"),
        row.get("room_name"),
        row.get("area_name"),
        meta.get("area_name"),
        meta.get("room_name"),
        meta.get("room"),
        meta.get("area"),
        device_info.get("friendly_name"),
        row.get("name"),
        device_info.get("name"),
        row.get("host"),
        selector,
    ):
        token = _text(value)
        if token:
            return token
    return selector


def _source_label(selector: str) -> str:
    row = _client_or_registry_row(selector)
    return _candidate_label(row, selector) if row else selector


def _client_or_registry_row(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {}

    registry_row = _registry_by_selector().get(token, {})
    native_row = _native_clients_by_selector().get(token)
    if isinstance(native_row, dict):
        return _merge_satellite_rows(registry_row, native_row)

    status = device_runtime.status()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    row = clients.get(token)
    if isinstance(row, dict):
        return _merge_satellite_rows(registry_row, row)

    return dict(registry_row)


def _connected_client_row(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {}
    status = device_runtime.status()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    row = clients.get(token)
    if not isinstance(row, dict) or not bool(row.get("connected")):
        return {}
    return dict(row)


def _target_candidates() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    registry_rows = _registry_by_selector()
    native_rows = _native_clients_by_selector()

    for selector, row in native_rows.items():
        token = _text(selector) or _text(row.get("selector"))
        if not token or token in seen:
            continue
        seen.add(token)
        merged = _merge_satellite_rows(registry_rows.get(token, {}), row)
        merged["selector"] = token
        rows.append(merged)

    status = device_runtime.status()
    clients = status.get("clients") if isinstance(status.get("clients"), dict) else {}
    for selector, row in clients.items():
        if not isinstance(row, dict):
            continue
        token = _text(selector) or _text(row.get("selector"))
        if not token or token in seen:
            continue
        if not bool(row.get("connected")):
            continue
        seen.add(token)
        merged = _merge_satellite_rows(registry_rows.get(token, {}), row)
        merged["selector"] = token
        rows.append(merged)

    return rows


def _broadcast_targets(*, source_selector: Any = "") -> List[Dict[str, Any]]:
    source = _text(source_selector)
    rows: List[Dict[str, Any]] = []
    for row in _target_candidates():
        selector = _text(row.get("selector"))
        if not selector or selector == source:
            continue
        rows.append(row)
    rows.sort(key=lambda row: _lower(_candidate_label(row, _text(row.get("selector")))))
    return rows


def target_options(*, current_values: Any = None) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    for row in _target_candidates():
        selector = _text(row.get("selector"))
        if not selector:
            continue
        value = _voice_core_target(selector)
        if value in seen:
            continue
        seen.add(value)
        rows.append(
            {
                "value": value,
                "label": f"{_candidate_label(row, selector)} ({selector})",
            }
        )

    for value in normalize_announcement_targets(current_values):
        if not value.startswith(VOICE_CORE_TARGET_PREFIX) or value in seen:
            continue
        selector = _selector_from_voice_core_target(value)
        if selector:
            rows.append({"value": value, "label": f"{selector} (saved)"})
            seen.add(value)

    rows.sort(key=lambda item: _lower(item.get("label")))
    return rows


def resolve_target(target_query: Any, *, source_selector: Any = "") -> Dict[str, Any]:
    query = _text(target_query)
    if not query:
        return {"ok": False, "error": "target_required", "message": "No intercom target was provided."}

    query_lower = query.lower()
    explicit_target = query_lower.startswith(VOICE_CORE_TARGET_PREFIX) or query_lower.startswith("host:")
    normalized_targets = normalize_announcement_targets(query) if explicit_target else []
    if normalized_targets:
        first = normalized_targets[0]
        selector = _selector_from_voice_core_target(first)
        if selector:
            if _text(source_selector) and selector == _text(source_selector):
                return {
                    "ok": False,
                    "error": "same_source_target",
                    "message": "The intercom target is the same satellite you spoke from.",
                }
            if not _connected_client_row(selector):
                return {
                    "ok": False,
                    "error": "target_not_connected",
                    "message": f"I found {selector}, but that Tater satellite is not connected right now.",
                    "options": target_options(current_values=[_voice_core_target(selector)]),
                }
            row = _client_or_registry_row(selector)
            return {
                "ok": True,
                "target": _voice_core_target(selector),
                "selector": selector,
                "label": _candidate_label(row, selector) if row else selector,
            }

    query_norm = _norm(query)
    query_words = set(query_norm.split())
    best: Optional[Dict[str, Any]] = None
    best_score = 0

    for row in _target_candidates():
        selector = _text(row.get("selector"))
        if not selector or selector == _text(source_selector):
            continue
        names = _candidate_names(row)
        score = 0
        for name in names:
            name_norm = _norm(name)
            name_words = set(name_norm.split())
            if not name_norm:
                continue
            if query_norm == name_norm:
                score = max(score, 120)
            elif query_norm and query_norm in name_norm:
                score = max(score, 95)
            elif name_norm in query_norm:
                score = max(score, 85)
            elif query_words and query_words.issubset(name_words):
                score = max(score, 80)
            elif query_words and name_words and query_words.intersection(name_words):
                score = max(score, 45 + (10 * len(query_words.intersection(name_words))))
        if score > best_score:
            best_score = score
            best = row

    if not best or best_score < 60:
        return {
            "ok": False,
            "error": "target_not_found",
            "message": f"I couldn't find a connected Tater satellite matching {query!r}.",
            "options": target_options(),
        }

    selector = _text(best.get("selector"))
    return {
        "ok": True,
        "target": _voice_core_target(selector),
        "selector": selector,
        "label": _candidate_label(best, selector),
        "score": best_score,
    }


async def start_intercom(
    *,
    source_selector: Any,
    target_query: Any,
    timeout_s: float = DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S,
) -> Dict[str, Any]:
    source = _text(source_selector)
    if not source:
        return {"ok": False, "error": "source_required", "message": "I could not tell which satellite you spoke from."}

    resolved = resolve_target(target_query, source_selector=source)
    if not resolved.get("ok"):
        return resolved

    target_selector = _text(resolved.get("selector"))
    target_label = _text(resolved.get("label")) or target_selector
    session_id = uuid.uuid4().hex
    expires_ts = _now() + max(10.0, float(timeout_s or DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S))
    source_label = _source_label(source)

    async with _pending_lock:
        _prune_locked()
        _pending_by_selector[source] = {
            "session_id": session_id,
            "source_selector": source,
            "source_label": source_label,
            "target_selector": target_selector,
            "target": _voice_core_target(target_selector),
            "target_label": target_label,
            "created_ts": _now(),
            "expires_ts": expires_ts,
            "direction": "outbound",
        }

    logger.info(
        "[voice_intercom] started session=%s source=%s target=%s timeout_s=%.1f",
        session_id,
        source,
        target_selector,
        max(10.0, float(timeout_s or DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S)),
    )
    return {
        "ok": True,
        "session_id": session_id,
        "source_selector": source,
        "source_label": source_label,
        "target_selector": target_selector,
        "target": _voice_core_target(target_selector),
        "target_label": target_label,
        "expires_ts": expires_ts,
        "message": f"Intercom to {target_label} is open. Say the message you want to send.",
    }


async def cancel_for_selector(selector: Any) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {"ok": False, "removed": 0}
    removed = 0
    async with _pending_lock:
        if _pending_by_selector.pop(token, None):
            removed += 1
        for key, row in list(_pending_by_selector.items()):
            if _text(row.get("source_selector")) == token or _text(row.get("target_selector")) == token:
                _pending_by_selector.pop(key, None)
                removed += 1
    return {"ok": True, "selector": token, "removed": removed}


def _prune_locked() -> None:
    now = _now()
    for selector, row in list(_pending_by_selector.items()):
        if float(row.get("expires_ts") or 0.0) <= now:
            _pending_by_selector.pop(selector, None)


def _is_stop_phrase(transcript: str) -> bool:
    text = _norm(transcript)
    if not text:
        return False
    if text in {_norm(item) for item in INTERCOM_STOP_PHRASES}:
        return True
    return bool(re.search(r"\b(cancel|close|end|stop)\s+(the\s+)?intercom\b", text))


async def close_auto_reply_for_selector(selector: Any, *, conversation_id: Any = "") -> Dict[str, Any]:
    token = _text(selector)
    conv = _text(conversation_id)
    if not token:
        return {"ok": False, "removed": 0}

    removed = 0
    async with _pending_lock:
        _prune_locked()
        pending = _pending_by_selector.get(token)
        if isinstance(pending, dict) and bool(pending.get("auto_reopen")) and _text(pending.get("direction")) == "reply":
            pending_session = _text(pending.get("session_id"))
            if not conv or not pending_session or pending_session == conv:
                _pending_by_selector.pop(token, None)
                removed = 1

    if removed:
        logger.info("[voice_intercom] auto reply closed after no speech selector=%s conversation_id=%s", token, conv or "-")
    return {"ok": True, "selector": token, "removed": removed}


def _pcm_to_wav_bytes(audio_bytes: bytes, audio_format: Dict[str, Any]) -> bytes:
    pcm = bytes(audio_bytes or b"")
    if not pcm:
        return b""
    try:
        rate = int(audio_format.get("rate") or 16000)
        width = int(audio_format.get("width") or 2)
        channels = int(audio_format.get("channels") or 1)
    except Exception:
        rate, width, channels = 16000, 2, 1
    if width not in {1, 2, 3, 4}:
        width = 2
    if channels < 1 or channels > 8:
        channels = 1
    block_align = max(1, width * channels)
    usable = len(pcm) - (len(pcm) % block_align)
    if usable <= 0:
        return b""
    pcm = pcm[:usable]
    with BytesIO() as out:
        with wave.open(out, "wb") as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(width)
            wav_file.setframerate(rate)
            wav_file.writeframes(pcm)
        return out.getvalue()


async def _play_intercom_audio(
    *,
    pending: Dict[str, Any],
    transcript: str,
    wav_bytes: bytes,
) -> Dict[str, Any]:
    try:
        ha_config = load_homeassistant_config(required=False)
    except Exception:
        ha_config = {"base": "", "token": ""}

    source_label = _text(pending.get("source_label")) or _text(pending.get("source_selector")) or "another room"
    targets = [_text(item) for item in list(pending.get("targets") or []) if _text(item)]
    if not targets:
        target = _text(pending.get("target"))
        targets = [target] if target else []
    continue_conversation = bool(pending.get("continue_conversation", True))
    return await play_announcement_audio_targets(
        text=f"Intercom from {source_label}",
        wav_bytes=wav_bytes,
        ha_base=_text(ha_config.get("base")),
        token=_text(ha_config.get("token")),
        targets=targets,
        public_base_url="",
        backend="intercom",
        tts_kind="intercom",
        continue_conversation=continue_conversation,
        conversation_id=_text(pending.get("session_id")) if continue_conversation else "",
    )


async def handle_broadcast_voice_turn(
    *,
    selector: Any,
    transcript: Any,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    speech_s: Any = 0.0,
) -> Dict[str, Any]:
    source = _text(selector)
    text = _text(transcript)
    if _is_stop_phrase(text):
        return {
            "transcript": text,
            "response_text": "Broadcast cancelled.",
            "intercom": True,
            "intercom_broadcast": True,
            "intercom_cancelled": True,
        }

    targets = _broadcast_targets(source_selector=source)
    if not targets:
        return {
            "transcript": text,
            "response_text": "I could not find any other connected satellites to broadcast to.",
            "intercom": True,
            "intercom_broadcast": True,
            "intercom_sent": False,
            "error": "no_targets",
        }

    wav_bytes = _pcm_to_wav_bytes(audio_bytes, audio_format)
    speech_seconds = 0.0
    with contextlib.suppress(Exception):
        speech_seconds = float(speech_s or 0.0)
    if not wav_bytes or (not text and speech_seconds < 0.25):
        return {
            "transcript": text,
            "response_text": "I did not catch a broadcast message.",
            "intercom": True,
            "intercom_broadcast": True,
            "intercom_empty": True,
        }

    source_label = _source_label(source) if source else "another room"
    target_labels = [
        _candidate_label(row, _text(row.get("selector")))
        for row in targets
        if _text(row.get("selector"))
    ]
    pending = {
        "session_id": uuid.uuid4().hex,
        "source_selector": source,
        "source_label": source_label,
        "target_label": "all satellites",
        "targets": [_voice_core_target(row.get("selector")) for row in targets if _text(row.get("selector"))],
        "continue_conversation": False,
        "direction": "broadcast",
    }
    result = await _play_intercom_audio(pending=pending, transcript=text, wav_bytes=wav_bytes)
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    sent_count = int(result.get("sent_count") or 0) if isinstance(result, dict) else 0

    if ok:
        label_text = ", ".join(target_labels[:3])
        if len(target_labels) > 3:
            label_text = f"{label_text}, and {len(target_labels) - 3} more"
        return {
            "transcript": text,
            "response_text": f"Broadcast sent to {sent_count} satellite{'s' if sent_count != 1 else ''}.",
            "intercom": True,
            "intercom_broadcast": True,
            "intercom_sent": True,
            "target_count": sent_count,
            "target_labels": label_text,
        }

    error = _text(result.get("error")) if isinstance(result, dict) else "Broadcast playback failed."
    logger.warning("[voice_intercom] broadcast failed source=%s error=%s", source, error)
    return {
        "transcript": text,
        "response_text": "I could not send that broadcast.",
        "intercom": True,
        "intercom_broadcast": True,
        "intercom_sent": False,
        "error": error,
    }


async def handle_voice_turn(
    *,
    selector: Any,
    transcript: Any,
    audio_bytes: bytes,
    audio_format: Dict[str, Any],
    speech_s: Any = 0.0,
) -> Optional[Dict[str, Any]]:
    source = _text(selector)
    if not source:
        return None

    async with _pending_lock:
        _prune_locked()
        pending = _pending_by_selector.pop(source, None)

    if not isinstance(pending, dict):
        return None

    text = _text(transcript)
    if _is_stop_phrase(text):
        return {
            "transcript": text,
            "response_text": "Intercom closed.",
            "intercom": True,
            "intercom_cancelled": True,
        }

    wav_bytes = _pcm_to_wav_bytes(audio_bytes, audio_format)
    speech_seconds = 0.0
    with contextlib.suppress(Exception):
        speech_seconds = float(speech_s or 0.0)
    if not wav_bytes or (not text and speech_seconds < 0.25):
        if bool(pending.get("auto_reopen")) and _text(pending.get("direction")) == "reply":
            return {
                "transcript": text,
                "response_text": "I did not catch a reply. Intercom closed.",
                "intercom": True,
                "intercom_empty": True,
                "intercom_closed": True,
            }
        async with _pending_lock:
            _prune_locked()
            pending["expires_ts"] = _now() + DEFAULT_INTERCOM_CAPTURE_TIMEOUT_S
            _pending_by_selector[source] = pending
        return {
            "transcript": text,
            "response_text": "I did not catch an intercom message. Intercom is still open for a moment.",
            "intercom": True,
            "intercom_empty": True,
        }

    result = await _play_intercom_audio(pending=pending, transcript=text, wav_bytes=wav_bytes)
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    target_selector = _text(pending.get("target_selector"))
    target_label = _text(pending.get("target_label")) or target_selector
    source_label = _text(pending.get("source_label")) or source

    if ok and target_selector:
        async with _pending_lock:
            _prune_locked()
            _pending_by_selector[target_selector] = {
                "session_id": _text(pending.get("session_id")) or uuid.uuid4().hex,
                "source_selector": target_selector,
                "source_label": target_label,
                "target_selector": source,
                "target": _voice_core_target(source),
                "target_label": source_label,
                "created_ts": _now(),
                "expires_ts": _now() + DEFAULT_INTERCOM_REPLY_TIMEOUT_S,
                "direction": "reply",
                "auto_reopen": True,
            }

    if ok:
        return {
            "transcript": text,
            "response_text": f"Sent to {target_label}. Their mic will open for a reply.",
            "intercom": True,
            "intercom_sent": True,
            "target_selector": target_selector,
            "target_label": target_label,
        }

    error = _text(result.get("error")) if isinstance(result, dict) else "Intercom playback failed."
    logger.warning(
        "[voice_intercom] send failed source=%s target=%s error=%s",
        source,
        target_selector,
        error,
    )
    return {
        "transcript": text,
        "response_text": f"I could not send that intercom message to {target_label}.",
        "intercom": True,
        "intercom_sent": False,
        "error": error,
    }


async def status() -> Dict[str, Any]:
    async with _pending_lock:
        _prune_locked()
        rows = [dict(row) for row in _pending_by_selector.values()]
    rows.sort(key=lambda row: _text(row.get("source_label")).lower())
    return {"ok": True, "pending": rows, "count": len(rows)}
