from __future__ import annotations

import asyncio
import contextlib
import json
import sys
from typing import Any, Dict, List


def _vp():
    return sys.modules[__package__]


def _normalize_satellite_row(raw: Any) -> Dict[str, Any]:
    vp = _vp()
    row = raw if isinstance(raw, dict) else {}
    host = vp._lower(row.get("host"))
    selector = vp._text(row.get("selector"))
    if not selector and host:
        selector = f"host:{host}"

    return {
        "selector": selector,
        "host": host,
        "name": vp._text(row.get("name")),
        "source": vp._text(row.get("source")) or "manual",
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "last_seen_ts": vp._as_float(row.get("last_seen_ts"), 0.0),
    }


def _satellite_area_name(row: Any) -> str:
    vp = _vp()
    data = row if isinstance(row, dict) else {}
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    for key in ("area_name", "room_name", "room", "area"):
        value = vp._text(meta.get(key))
        if value:
            return value
    return ""


def _load_satellite_registry() -> List[Dict[str, Any]]:
    vp = _vp()
    with contextlib.suppress(Exception):
        raw = vp.redis_client.get(vp.REDIS_VOICE_SATELLITE_REGISTRY_KEY)
        if not raw:
            return []
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            return []
        rows: List[Dict[str, Any]] = []
        seen = set()
        for item in parsed:
            row = _normalize_satellite_row(item)
            selector = vp._text(row.get("selector"))
            if not selector or selector in seen:
                continue
            seen.add(selector)
            rows.append(row)
        return rows
    return []


def _save_satellite_registry(rows: List[Dict[str, Any]]) -> None:
    vp = _vp()
    clean: List[Dict[str, Any]] = []
    seen = set()
    for item in rows:
        row = _normalize_satellite_row(item)
        selector = vp._text(row.get("selector"))
        if not selector or selector in seen:
            continue
        seen.add(selector)
        clean.append(row)
    with contextlib.suppress(Exception):
        vp.redis_client.set(vp.REDIS_VOICE_SATELLITE_REGISTRY_KEY, json.dumps(clean, ensure_ascii=False))


def _upsert_satellite(row: Dict[str, Any]) -> Dict[str, Any]:
    vp = _vp()
    incoming = _normalize_satellite_row(row)
    selector = vp._text(incoming.get("selector"))
    if not selector:
        raise ValueError("satellite selector is required")

    current = _load_satellite_registry()
    merged: List[Dict[str, Any]] = []
    replaced = False
    for existing in current:
        if vp._text(existing.get("selector")) != selector:
            merged.append(existing)
            continue
        updated = dict(existing)
        for key, value in incoming.items():
            if key == "metadata":
                old_meta = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
                new_meta = value if isinstance(value, dict) else {}
                updated["metadata"] = {**old_meta, **new_meta}
                continue
            if value in ("", None) and key != "last_seen_ts":
                continue
            updated[key] = value
        updated["last_seen_ts"] = vp._now()
        merged.append(_normalize_satellite_row(updated))
        replaced = True

    if not replaced:
        incoming["last_seen_ts"] = vp._now()
        merged.append(incoming)

    _save_satellite_registry(merged)
    for row_out in merged:
        if vp._text(row_out.get("selector")) == selector:
            return row_out
    return incoming


def _set_satellite_selected(selector: str, selected: bool) -> None:
    vp = _vp()
    token = vp._text(selector)
    if not token:
        return
    rows = _load_satellite_registry()
    next_rows: List[Dict[str, Any]] = []
    found = False
    for row in rows:
        if vp._text(row.get("selector")) != token:
            next_rows.append(row)
            continue
        updated = dict(row)
        meta = dict(updated.get("metadata") or {})
        meta["esphome_selected"] = bool(selected)
        updated["metadata"] = meta
        updated["last_seen_ts"] = vp._now()
        next_rows.append(updated)
        found = True
    if not found:
        next_rows.append(
            {
                "selector": token,
                "host": vp._lower(token.split(":", 1)[1]) if token.startswith("host:") else "",
                "name": "",
                "source": "manual",
                "metadata": {"esphome_selected": bool(selected)},
                "last_seen_ts": vp._now(),
            }
        )
    _save_satellite_registry(next_rows)


def _satellite_lookup(selector: str) -> Dict[str, Any]:
    vp = _vp()
    token = vp._text(selector)
    if not token:
        return {}
    for row in _load_satellite_registry():
        if vp._text(row.get("selector")) == token:
            return dict(row)
    return {}


def _remove_satellite(selector: str) -> bool:
    vp = _vp()
    token = vp._text(selector)
    if not token:
        return False
    rows = _load_satellite_registry()
    next_rows = [row for row in rows if vp._text(row.get("selector")) != token]
    changed = len(next_rows) != len(rows)
    if changed:
        _save_satellite_registry(next_rows)
    return changed


def _selector_runtime(selector: str) -> Dict[str, Any]:
    vp = _vp()
    token = vp._text(selector)
    row = vp._voice_selector_runtime.get(token)
    if not isinstance(row, dict):
        row = {
            "lock": asyncio.Lock(),
            "session": None,
            "awaiting_announcement": False,
            "awaiting_session_id": "",
            "awaiting_announcement_kind": "",
            "announcement_future": None,
            "announcement_task": None,
            "audio_stall_task": None,
            "pending_followup_conversation_id": "",
            "pending_followup_until_ts": 0.0,
            "streamed_tts": None,
            "streamed_tts_dispatch_task": None,
            "service_running": False,
        }
        vp._voice_selector_runtime[token] = row
    if not hasattr(row.get("lock"), "acquire"):
        row["lock"] = asyncio.Lock()
    return row


def _cancel_announcement_wait(runtime: Dict[str, Any]) -> None:
    task = runtime.get("announcement_task")
    if isinstance(task, asyncio.Task):
        if task is asyncio.current_task():
            runtime["announcement_task"] = None
            return
        task.cancel()
    runtime["announcement_task"] = None


def _cancel_audio_stall_watch(runtime: Dict[str, Any]) -> None:
    task = runtime.get("audio_stall_task")
    if isinstance(task, asyncio.Task):
        if task is asyncio.current_task():
            runtime["audio_stall_task"] = None
            return
        task.cancel()
    runtime["audio_stall_task"] = None


def _cancel_streamed_tts_dispatch(runtime: Dict[str, Any]) -> None:
    task = runtime.get("streamed_tts_dispatch_task")
    if isinstance(task, asyncio.Task):
        if task is asyncio.current_task():
            runtime["streamed_tts_dispatch_task"] = None
            return
        task.cancel()
    runtime["streamed_tts_dispatch_task"] = None


def _clear_streamed_tts_state(runtime: Dict[str, Any]) -> None:
    state = runtime.get("streamed_tts")
    if isinstance(state, dict):
        task = state.get("prepare_task")
        if isinstance(task, asyncio.Task):
            if task is asyncio.current_task():
                state["prepare_task"] = None
            else:
                task.cancel()
    runtime["streamed_tts"] = None
    _cancel_streamed_tts_dispatch(runtime)


def _clear_pending_followup(runtime: Dict[str, Any]) -> None:
    runtime["pending_followup_conversation_id"] = ""
    runtime["pending_followup_until_ts"] = 0.0


def _arm_pending_followup(runtime: Dict[str, Any], conversation_id: str) -> None:
    vp = _vp()
    conv = vp._text(conversation_id)
    if not conv:
        _clear_pending_followup(runtime)
        return
    runtime["pending_followup_conversation_id"] = conv
    runtime["pending_followup_until_ts"] = vp._now() + float(vp.DEFAULT_CONTINUED_CHAT_REUSE_SECONDS)


def _claim_pending_followup(runtime: Dict[str, Any]) -> str:
    vp = _vp()
    conv = vp._text(runtime.get("pending_followup_conversation_id"))
    until_ts = vp._as_float(runtime.get("pending_followup_until_ts"), 0.0)
    if conv and vp._now() <= until_ts:
        _clear_pending_followup(runtime)
        return conv
    _clear_pending_followup(runtime)
    return ""


def _schedule_audio_stall_watch(
    selector: str,
    client: Any,
    module: Any,
    *,
    session_id: str,
) -> None:
    vp = _vp()
    token = vp._text(selector)
    sid = vp._text(session_id)
    if not token or not sid:
        return

    runtime = _selector_runtime(token)

    async def _watch() -> None:
        try:
            while True:
                await asyncio.sleep(max(0.05, float(vp.DEFAULT_AUDIO_STALL_POLL_S)))
                should_finalize = False
                finalize_reason = ""
                gap_s = 0.0
                voice_seen = False
                chunks = 0
                wake_started = False

                lock = runtime.get("lock")
                if lock is None or not hasattr(lock, "acquire"):
                    return

                async with lock:
                    session = runtime.get("session")
                    if not isinstance(session, vp.VoiceSessionRuntime):
                        return
                    if vp._text(session.session_id) != sid:
                        return
                    if bool(session.processing):
                        return

                    now_ts = vp._now()
                    wake_started = bool(vp._text(session.wake_word))
                    if now_ts < float(session.startup_gate_until_ts):
                        continue

                    chunks = int(session.audio_chunks)
                    last_ts = float(session.last_audio_ts or 0.0)
                    if last_ts <= 0.0:
                        elapsed = now_ts - float(session.started_ts or now_ts)
                        timeout_s = float(
                            vp.DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S if wake_started else vp.DEFAULT_BLANK_WAKE_TIMEOUT_S
                        )
                        if elapsed >= timeout_s:
                            should_finalize = True
                            finalize_reason = "audio_stall_no_audio" if wake_started else "blank_wake_timeout"
                        else:
                            continue

                    gap_s = max(0.0, now_ts - last_ts)

                    seg = None
                    if isinstance(session.eou_engine, vp.EouEngine):
                        seg = session.eou_engine.segmenter
                    voice_seen = bool(seg.voice_seen) if isinstance(seg, vp.SegmenterState) else bool(chunks > 0)

                    if voice_seen:
                        if gap_s >= float(vp.DEFAULT_AUDIO_STALL_TIMEOUT_S):
                            should_finalize = True
                            finalize_reason = "audio_stall_after_speech"
                    else:
                        timeout_s = float(
                            vp.DEFAULT_AUDIO_STALL_NO_SPEECH_TIMEOUT_S if wake_started else vp.DEFAULT_BLANK_WAKE_TIMEOUT_S
                        )
                        if gap_s >= timeout_s:
                            should_finalize = True
                            finalize_reason = "audio_stall_no_speech" if wake_started else "blank_wake_timeout"

                if should_finalize:
                    vp._native_debug(
                        f"esphome audio stall finalize selector={token} session_id={sid} "
                        f"reason={finalize_reason} gap_s={gap_s:.2f} voice_seen={voice_seen} chunks={chunks}"
                    )
                    with contextlib.suppress(Exception):
                        await vp._finalize_session(
                            token,
                            client,
                            module,
                            session_id=sid,
                            abort=False,
                            reason=finalize_reason,
                        )
                    return
        except asyncio.CancelledError:
            return
        except Exception as exc:
            vp._native_debug(f"audio stall watch failed selector={token} session_id={sid} error={exc}")

    _cancel_audio_stall_watch(runtime)
    runtime["audio_stall_task"] = asyncio.create_task(_watch())
